//! tailrace-console entrypoint (§3.1): flag parsing, startup migration +
//! admin seed + bootstrap token, then serve.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use console_auth::BootstrapAuthority;
use console_store::Store;
use tailrace_console::{build_router, state::AppState};
use tokio::net::TcpListener;

const DEFAULT_ADDR: &str = "0.0.0.0:8080";
const DEFAULT_DATA_DIR: &str = "./data";

struct Args {
    addr: String,
    data_dir: String,
}

fn usage() {
    eprintln!(
        "Usage: tailrace-console [--addr <HOST:PORT>] [--data-dir <DIR>] [--version]\n\
         \n\
         Options:\n\
         \x20 -a, --addr <HOST:PORT>   listen address (default {DEFAULT_ADDR})\n\
         \x20 -d, --data-dir <DIR>     metadata dir, holds console.db (default {DEFAULT_DATA_DIR})\n\
         \x20 -V, --version            print version and exit\n\
         \n\
         Env:\n\
         \x20 TAILRACE_BOOTSTRAP_TOKEN  pin the node self-registration bootstrap token\n\
         \x20                             (otherwise generated and printed once at startup)\n\
         \x20 TAILRACE_MASTER_KEY       credential master key, 32B base64 (§5.3; otherwise\n\
         \x20                             <data-dir>/master.key is generated, 0600, first boot)\n\
         \x20 RUST_LOG                   tracing filter (default info)"
    );
}

/// Hand-rolled flag parsing, mirroring the engine bin style (zero deps).
fn parse_args(argv: &[String]) -> Result<Args> {
    let mut args = Args {
        addr: DEFAULT_ADDR.into(),
        data_dir: DEFAULT_DATA_DIR.into(),
    };
    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i].as_str();
        let (flag, inline) = match arg.split_once('=') {
            Some((f, v)) => (f, Some(v.to_string())),
            None => (arg, None),
        };
        if matches!(flag, "-V" | "--version") {
            println!("tailrace-console {}", env!("CARGO_PKG_VERSION"));
            std::process::exit(0);
        }
        if matches!(flag, "-h" | "--help") {
            usage();
            std::process::exit(0);
        }
        let value = |i: &mut usize| -> Result<String> {
            if let Some(v) = inline.clone() {
                return Ok(v);
            }
            *i += 1;
            argv.get(*i)
                .cloned()
                .with_context(|| format!("missing value for {flag}"))
        };
        match flag {
            "-a" | "--addr" => args.addr = value(&mut i)?,
            "-d" | "--data-dir" => args.data_dir = value(&mut i)?,
            other => anyhow::bail!("unknown flag {other:?} (see --help)"),
        }
        i += 1;
    }
    Ok(args)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args(std::env::args().skip(1).collect::<Vec<_>>().as_slice())?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    std::fs::create_dir_all(&args.data_dir).context("create data dir")?;
    let data_dir = std::path::PathBuf::from(&args.data_dir);
    let db_path = data_dir.join("console.db");
    let store = Store::open(&db_path).with_context(|| format!("open {}", db_path.display()))?;

    // Credential master key (§5.3): TAILRACE_MASTER_KEY env (32B base64) >
    // <data-dir>/master.key (0600, generated on first boot). Fail-closed —
    // no machine-fingerprint derivation fallback; a key that cannot be
    // resolved/refuses to load stops the process.
    let cipher = match console_store::CredentialCipher::load(
        std::env::var(console_store::MASTER_KEY_ENV).ok().as_deref(),
        &data_dir,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[Tailrace] 凭据主密钥加载失败（fail-closed，§5.3，拒绝启动）: {e:#}");
            std::process::exit(1);
        }
    };

    // First boot: seed admin and print the one-time password (§4.1).
    if let Some((username, password)) = store.seed_admin_if_empty()? {
        eprintln!("──────────────────────────────────────────────────────────────");
        eprintln!("[Tailrace] 首次启动：初始管理员已创建");
        eprintln!("[Tailrace]   用户名：{username}");
        eprintln!("[Tailrace]   初始密码：{password}");
        eprintln!("[Tailrace]   （仅此一次显示，请妥善保存）");
        eprintln!("──────────────────────────────────────────────────────────────");
    }

    // Bootstrap token for node self-registration (§4.0 m2m).
    let (bootstrap, generated) = BootstrapAuthority::from_env_or_generate();
    if let Some(token) = generated {
        eprintln!("──────────────────────────────────────────────────────────────");
        eprintln!("[Tailrace] 节点自注册 bootstrap token（本次启动生成，仅此一次显示）：");
        eprintln!("[Tailrace]   {token}");
        eprintln!(
            "[Tailrace]   引擎侧 --bootstrap-token 使用它；固定请设 TAILRACE_BOOTSTRAP_TOKEN"
        );
        eprintln!("──────────────────────────────────────────────────────────────");
    }

    let addr: SocketAddr = args
        .addr
        .parse()
        .with_context(|| format!("invalid --addr {:?}", args.addr))?;
    let artifact_dir = data_dir.join("artifacts");
    std::fs::create_dir_all(&artifact_dir)
        .with_context(|| format!("create {}", artifact_dir.display()))?;
    let state = AppState::new_full(store, bootstrap, artifact_dir, cipher);
    let app = build_router(state);

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!(
        "tailrace-console {} listening on http://{addr}",
        env!("CARGO_PKG_VERSION")
    );
    tracing::info!(
        "data dir: {} | swagger: http://{addr}/swagger-ui | api doc: http://{addr}/api-docs/openapi.json",
        args.data_dir
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve")
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("ctrl-c received, shutting down");
}
