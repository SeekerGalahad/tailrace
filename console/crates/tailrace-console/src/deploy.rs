//! §4.4 SSH deploy chain — the eight-step state machine
//! (`PENDING→PREFLIGHT→UPLOAD→UNPACK→CONFIGURE→START→HEALTH_WAIT→REGISTER→REGISTERED`)
//! plus the process-action ride-alongs (start/stop/restart/rollback/uninstall)
//! and the failure-rollback path that is this stage's acceptance focus:
//! **any step failing after CONFIGURE rolls the `current` symlink back to the
//! previous release, restarts the old binary and re-confirms health**.
//!
//! Transport is russh (pure Rust; selection gated by `cargo deny check` on
//! the workspace graph). Authentication is public-key only — the console's
//! deploy key comes from `TAILRACE_SSH_KEY` (optional passphrase in
//! `TAILRACE_SSH_KEY_PASSPHRASE`); credentials never appear in request
//! bodies or the deploy log. Host keys are accepted on first use (deploy-LAN
//! positioning; a pinned known_hosts store is v1.x work, as is password
//! auth).
//!
//! Remote layout under the node's `install_dir` (POSIX sh; busybox-safe):
//!
//! ```text
//! <install_dir>/releases/<version>/   unpacked artifact (binary found by name)
//! <install_dir>/current -> releases/<version>   live release (symlink switch)
//! <install_dir>/prev     -> releases/<old>      rollback anchor (CONFIGURE)
//! <install_dir>/incoming-<sha8>.tar.gz          staged upload (removed post-UNPACK)
//! <install_dir>/engine.pid | engine.log | state/  process + --state-dir
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use console_store::Store;
use russh::keys::{load_secret_key, PrivateKeyWithHashAlg, PublicKeyOrCertificate};

use crate::state::AppState;

/// Env: console deploy key (OpenSSH private key path). Required for every
/// SSH operation; missing key fails PREFLIGHT with a config error.
pub const SSH_KEY_ENV: &str = "TAILRACE_SSH_KEY";
/// Env: optional passphrase for [`SSH_KEY_ENV`].
pub const SSH_KEY_PASSPHRASE_ENV: &str = "TAILRACE_SSH_KEY_PASSPHRASE";
/// Env: base URL engines use for `--control-plane` (they must reach the
/// console from their own network position — `http://host.docker.internal:8080`
/// in containerized setups). Default `http://127.0.0.1:8080`.
pub const CONSOLE_ADVERTISE_ENV: &str = "TAILRACE_CONSOLE_ADVERTISE_URL";
/// Env: HEALTH_WAIT + REGISTER combined deadline (seconds, default 90).
pub const HEALTH_TIMEOUT_ENV: &str = "TAILRACE_DEPLOY_HEALTH_TIMEOUT_SECS";

const DEFAULT_HEALTH_TIMEOUT_SECS: u64 = 90;
const SSH_CMD_TIMEOUT: Duration = Duration::from_secs(120);
/// Tarball upload deadline: generous for 512MB artifacts over slow links.
const SSH_UPLOAD_TIMEOUT: Duration = Duration::from_secs(600);
const HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// deploy_log.`action` values (DDL enum).
pub const ACTION_DEPLOY: &str = "DEPLOY";
pub const ACTION_UPGRADE: &str = "UPGRADE";
pub const ACTION_ROLLBACK: &str = "ROLLBACK";
pub const ACTION_START: &str = "START";
pub const ACTION_STOP: &str = "STOP";
pub const ACTION_RESTART: &str = "RESTART";
pub const ACTION_UNINSTALL: &str = "UNINSTALL";

// ---------------- SSH transport ----------------

/// Deploy-LAN client handler: accepts every host key (TOFU without the book).
struct TrustOnConnect;

impl russh::client::Handler for TrustOnConnect {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

/// One authenticated SSH connection to a managed node.
pub struct SshSession {
    handle: russh::client::Handle<TrustOnConnect>,
}

/// Outcome of one remote command.
#[derive(Debug)]
pub struct ExecOut {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

impl ExecOut {
    pub fn ok(&self) -> bool {
        self.code == 0
    }
}

impl SshSession {
    /// Connect + public-key auth using the console deploy key
    /// ([`SSH_KEY_ENV`]/[`SSH_KEY_PASSPHRASE_ENV`]).
    pub async fn connect(host: &str, port: i64, user: &str) -> anyhow::Result<Self> {
        let key_path = std::env::var(SSH_KEY_ENV)
            .map_err(|_| anyhow::anyhow!("{SSH_KEY_ENV} 未配置（console 部署私钥路径）"))?;
        let passphrase = std::env::var(SSH_KEY_PASSPHRASE_ENV).ok();
        let key = load_secret_key(PathBuf::from(&key_path), passphrase.as_deref())
            .map_err(|e| anyhow::anyhow!("部署私钥加载失败（{key_path}）: {e}"))?;

        let config = Arc::new(russh::client::Config {
            // A chain makes no SSH traffic for the whole HEALTH_WAIT/REGISTER
            // window (default 90s) — the connection must survive that pause:
            // keepalives hold it open, and the inactivity watchdog only
            // guards a genuinely dead peer (observed teardown mid-chain in
            // the docker e2e when it sat at the 10s connect timeout).
            keepalive_interval: Some(Duration::from_secs(15)),
            inactivity_timeout: Some(Duration::from_secs(300)),
            ..<_>::default()
        });
        let mut handle =
            russh::client::connect(config, (host, port.clamp(1, 65535) as u16), TrustOnConnect)
                .await
                .map_err(|e| anyhow::anyhow!("SSH 连接 {host}:{port} 失败: {e}"))?;
        let hash = handle.best_supported_rsa_hash().await?.flatten();
        let auth = handle
            .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), hash))
            .await
            .map_err(|e| anyhow::anyhow!("SSH 认证请求失败: {e}"))?;
        if !auth.success() {
            anyhow::bail!("SSH 公钥认证被拒绝（user={user} host={host}:{port}）");
        }
        Ok(Self { handle })
    }

    /// Run one command, collecting stdout/stderr up to exit status or a
    /// deadline. Remote-side timeout guard: the deadline only bounds this
    /// side's wait — a wedged command leaves the channel to the
    /// inactivity_timeout instead of leaking a blocked task.
    async fn exec_timed(&mut self, command: &str, timeout: Duration) -> anyhow::Result<ExecOut> {
        let mut channel = self
            .handle
            .channel_open_session()
            .await
            .map_err(|e| anyhow::anyhow!("SSH 通道打开失败: {e}"))?;
        channel
            .exec(true, command)
            .await
            .map_err(|e| anyhow::anyhow!("SSH exec 失败: {e}"))?;
        let mut out = String::new();
        let mut err = String::new();
        let mut code: Option<u32> = None;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let msg = tokio::time::timeout_at(deadline, channel.wait()).await?;
            match msg {
                Some(russh::ChannelMsg::Data { data }) => {
                    out.push_str(&String::from_utf8_lossy(&data));
                    if out.len() > 16 * 1024 {
                        out.truncate(16 * 1024);
                        out.push_str("…(truncated)");
                    }
                }
                Some(russh::ChannelMsg::ExtendedData { data, .. }) => {
                    err.push_str(&String::from_utf8_lossy(&data));
                    if err.len() > 16 * 1024 {
                        err.truncate(16 * 1024);
                        err.push_str("…(truncated)");
                    }
                }
                Some(russh::ChannelMsg::ExitStatus { exit_status }) => {
                    code = Some(exit_status);
                    // keep draining until Close/Eof so buffers flush
                }
                // without an exit status a Close/Eof means the channel was
                // torn down (timeout kill); keep draining otherwise
                Some(russh::ChannelMsg::Eof) | Some(russh::ChannelMsg::Close) if code.is_some() => {
                    break;
                }
                None => break,
                _ => {}
            }
        }
        Ok(ExecOut {
            // no exit status before the channel closed = killed/aborted
            code: code.unwrap_or(u32::MAX) as i32,
            stdout: out,
            stderr: err,
        })
    }

    pub async fn exec(&mut self, command: &str) -> anyhow::Result<ExecOut> {
        self.exec_timed(command, SSH_CMD_TIMEOUT).await
    }

    /// Stream local `bytes` into `cat > '<remote_path>'` and verify the
    /// remote sha256 matches `expect_sha256` (deploy verification anchor).
    async fn upload_and_verify(
        &mut self,
        remote_path: &str,
        bytes: &[u8],
        expect_sha256: &str,
    ) -> anyhow::Result<ExecOut> {
        let mut channel = self
            .handle
            .channel_open_session()
            .await
            .map_err(|e| anyhow::anyhow!("SSH 通道打开失败: {e}"))?;
        channel
            .exec(true, format!("cat > {}", shq(remote_path)))
            .await
            .map_err(|e| anyhow::anyhow!("SSH exec（上传）失败: {e}"))?;
        // 32KB chunks: below the channel window so writes never stall
        for chunk in bytes.chunks(32 * 1024) {
            tokio::time::timeout(SSH_UPLOAD_TIMEOUT, channel.data_bytes(chunk.to_vec()))
                .await
                .map_err(|_| anyhow::anyhow!("上传超时（{}B 已写）", bytes.len()))??;
        }
        channel
            .eof()
            .await
            .map_err(|e| anyhow::anyhow!("SSH eof 失败: {e}"))?;
        // drain to exit status
        let mut code = None;
        let deadline = tokio::time::Instant::now() + SSH_UPLOAD_TIMEOUT;
        loop {
            let msg = tokio::time::timeout_at(deadline, channel.wait()).await?;
            match msg {
                Some(russh::ChannelMsg::ExitStatus { exit_status }) => code = Some(exit_status),
                Some(russh::ChannelMsg::Close) | None => break,
                _ => {}
            }
        }
        if code != Some(0) {
            anyhow::bail!("远端写入 {remote_path} 失败（exit={code:?}）");
        }
        let check = self
            .exec(&format!("sha256sum {}", shq(remote_path)))
            .await?;
        let got = check
            .stdout
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        if !check.ok() || got != expect_sha256.to_ascii_lowercase() {
            anyhow::bail!(
                "上传校验失败：期望 sha256 {expect_sha256}，远端返回 {}（{}）",
                if got.is_empty() { "(空)" } else { &got },
                check.stderr.trim()
            );
        }
        Ok(ExecOut {
            code: 0,
            stdout: format!("sha256 {got} 校验一致（{}B）", bytes.len()),
            stderr: String::new(),
        })
    }

    pub async fn close(&mut self) {
        let _ = self
            .handle
            .disconnect(russh::Disconnect::ByApplication, "", "tailrace-console")
            .await;
    }
}

/// Single-quote for POSIX sh.
fn shq(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

// ---------------- chain inputs ----------------

/// Everything the eight-step machine needs for one node. Built by the
/// handlers from the node row + artifact row.
#[derive(Debug, Clone)]
pub struct ChainSpec {
    pub node_id: i64,
    pub node_key: String,
    pub name: String,
    pub host: String,
    pub ssh_port: i64,
    pub ssh_user: String,
    pub engine_port: i64,
    pub install_dir: String,
    /// Artifact (id, name, version, sha256) — bytes are read at UPLOAD
    /// time from `<artifact-dir>/<name>-<version>.tar.gz` (the upload
    /// handler's on-disk convention; `filename` is only the client-side
    /// original name and may not match the stored file).
    pub artifact_id: i64,
    pub artifact_name: String,
    pub artifact_version: String,
    pub artifact_sha256: String,
    /// Per-node token for a *fresh* deploy (`--bootstrap-token`); None on
    /// redeploy/upgrade — the engine re-uses the token persisted in its
    /// state dir.
    pub fresh_token: Option<String>,
    /// `clean` re-extracts the release dir; `wipe_state` removes the engine
    /// state dir (cursors/token) before START.
    pub clean: bool,
    pub wipe_state: bool,
    /// deploy_log action label: DEPLOY (fresh/redeploy) or UPGRADE.
    pub action: &'static str,
    pub operator: String,
}

/// Which side of the run the chain ended on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainOutcome {
    /// All eight steps OK, node REGISTERED on the new release.
    Registered,
    /// A step failed; rollback restored the previous release and health was
    /// re-confirmed (the acceptance-focus path).
    RolledBack,
    /// A step failed and rollback itself failed / had nothing to restore
    /// (fresh deploy) — node left FAILED, manual intervention needed.
    FailedNoRestore,
}

/// Step log writer: deploy_log row + node.deploy_status advance in one go.
fn log_step(store: &Store, spec: &ChainSpec, step: &str, status: &str, output: &str) {
    if let Err(e) = store.insert_deploy_log(
        spec.node_id,
        spec.action,
        step,
        status,
        output,
        Some(&spec.operator),
    ) {
        tracing::warn!(node = spec.node_id, step, error = %e, "deploy_log write failed");
    }
    // Step rows advance deploy_status through the machine positions;
    // ROLLBACK rows are remediation annotations — the terminal state
    // (FAILED/REGISTERED/UNINSTALLED) is always set explicitly by the chain
    // after them, so they must not stomp it.
    if status == "OK" && step != "ROLLBACK" {
        let _ = store.set_node_deploy_status(spec.node_id, step);
    }
}

fn fail(store: &Store, spec: &ChainSpec, step: &str, msg: &str) {
    tracing::warn!(node = spec.node_id, step, "deploy step failed: {msg}");
    log_step(store, spec, step, "FAIL", msg);
}

/// Health probe against `http://host:engine_port/healthz`.
async fn engine_healthy(state: &AppState, host: &str, port: i64) -> bool {
    let endpoint = console_proxy::NodeEndpoint::new(format!("http://{host}:{port}"));
    matches!(endpoint.healthz_ok(state.node_client()).await, Ok(true))
}

/// Wait until the node row shows ONLINE with `engine_version` equal to the
/// artifact version (the REGISTER / version-verification gate).
async fn wait_registered(
    state: &AppState,
    spec: &ChainSpec,
    deadline: tokio::time::Instant,
) -> anyhow::Result<()> {
    loop {
        if let Ok(Some(node)) = state.store().node(spec.node_id) {
            if node.status == "ONLINE" {
                match node.engine_version.as_deref() {
                    Some(v) if v == spec.artifact_version => return Ok(()),
                    Some(v) => {
                        anyhow::bail!(
                            "版本核对失败：节点上报 {v}，制品版本 {}",
                            spec.artifact_version
                        )
                    }
                    None => { /* registered but no version beat yet */ }
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("REGISTER 超时：节点未在窗口内完成注册心跳上报");
        }
        tokio::time::sleep(HEALTH_POLL_INTERVAL).await;
    }
}

/// The engine start command (POSIX sh). `token` only on fresh deploys.
fn start_command(spec: &ChainSpec, bin_rel: &str, token: Option<&str>) -> String {
    let control_plane = std::env::var(CONSOLE_ADVERTISE_ENV)
        .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());
    let mut cmd = format!(
        "cd {dir} || exit 1; rm -f engine.pid; ( nohup ./current/{bin} serve \
         --addr 0.0.0.0:{port} --advertise http://{host}:{port} \
         --control-plane {cp} --node-key {key} --name {name} \
         --state-dir {dir}/state --heartbeat-secs 15",
        dir = shq(&spec.install_dir),
        bin = bin_rel,
        port = spec.engine_port,
        host = shq(&spec.host),
        cp = shq(&control_plane),
        key = shq(&spec.node_key),
        name = shq(&spec.name),
    );
    if let Some(t) = token {
        cmd.push_str(&format!(" --bootstrap-token {}", shq(t)));
    }
    // double-fork `( nohup … & echo $! > pid )` + trailing `sleep 1`: the
    // exec channel closes the instant the command returns — without the
    // held-open second, sshd tears down the session before the child has
    // exec'd nohup and the engine dies without ever creating engine.log
    // (observed against OpenSSH in the docker e2e target).
    cmd.push_str(" >> engine.log 2>&1 < /dev/null & echo $! > engine.pid ); sleep 1");
    cmd
}

/// SIGTERM → bounded wait → SIGKILL via the pidfile (graceful final-flush
/// window for the engine; busybox-sh compatible).
fn stop_command(install_dir: &str) -> String {
    format!(
        "cd {dir} || exit 1; if [ -f engine.pid ]; then p=$(cat engine.pid); \
         kill $p 2>/dev/null || true; i=0; \
         while kill -0 $p 2>/dev/null && [ $i -lt 15 ]; do sleep 0.2; i=$((i+1)); done; \
         kill -9 $p 2>/dev/null || true; rm -f engine.pid; fi",
        dir = shq(install_dir)
    )
}

// ---------------- the eight-step machine ----------------

/// Run the chain for one node. Every step lands in deploy_log; on failure
/// after CONFIGURE the rollback path runs (restore prev → start old →
/// health re-confirm). The final node state is REGISTERED / FAILED.
pub async fn run_deploy_chain(state: AppState, spec: ChainSpec) -> ChainOutcome {
    let store = state.store().clone();
    let _ = store.set_node_deploy_status(spec.node_id, "PENDING");
    let health_deadline_secs = std::env::var(HEALTH_TIMEOUT_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_HEALTH_TIMEOUT_SECS);

    // ---- PREFLIGHT: SSH reachability + install dir + prev-release anchor
    let mut ssh = match SshSession::connect(&spec.host, spec.ssh_port, &spec.ssh_user).await {
        Ok(s) => s,
        Err(e) => {
            fail(&store, &spec, "PREFLIGHT", &format!("{e:#}"));
            let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
            return ChainOutcome::FailedNoRestore;
        }
    };
    let preflight_cmd = format!(
        "mkdir -p {dir}/releases && test -w {dir} && df -k {dir} | tail -1",
        dir = shq(&spec.install_dir)
    );
    let df_line = match ssh.exec(&preflight_cmd).await {
        Ok(o) if o.ok() => o,
        Ok(o) => {
            fail(
                &store,
                &spec,
                "PREFLIGHT",
                &format!(
                    "install dir 不可写或创建失败（exit={}）：{}",
                    o.code,
                    o.stderr.trim()
                ),
            );
            ssh.close().await;
            let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
            return ChainOutcome::FailedNoRestore;
        }
        Err(e) => {
            fail(&store, &spec, "PREFLIGHT", &format!("{e:#}"));
            ssh.close().await;
            let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
            return ChainOutcome::FailedNoRestore;
        }
    };
    let prev_release = ssh
        .exec(&format!(
            "readlink {}/current 2>/dev/null || true",
            shq(&spec.install_dir)
        ))
        .await
        .ok()
        .map(|o| o.stdout.trim().to_string())
        .filter(|s| !s.is_empty());
    log_step(
        &store,
        &spec,
        "PREFLIGHT",
        "OK",
        &format!(
            "ssh ok；磁盘 {}；{}prev_release={}",
            df_line.stdout.trim(),
            if spec.wipe_state { "wipe_state; " } else { "" },
            prev_release.as_deref().unwrap_or("(none)")
        ),
    );
    if spec.wipe_state {
        let _ = ssh
            .exec(&format!("rm -rf {}/state", shq(&spec.install_dir)))
            .await;
    }

    // current release state before any mutation (rollback eligibility)
    let had_prev = prev_release.is_some();

    // ---- UPLOAD: stage the tarball + sha256 verify
    let stored_name = format!("{}-{}.tar.gz", spec.artifact_name, spec.artifact_version);
    let artifact_path = state.artifact_dir().join(&stored_name);
    let bytes = match tokio::fs::read(&artifact_path).await {
        Ok(b) => b,
        Err(e) => {
            fail(
                &store,
                &spec,
                "UPLOAD",
                &format!("制品文件读取失败（{}）: {e}", artifact_path.display()),
            );
            ssh.close().await;
            let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
            return ChainOutcome::FailedNoRestore;
        }
    };
    let incoming = format!(
        "{}/incoming-{}.tar.gz",
        spec.install_dir,
        spec.artifact_sha256.get(..8).unwrap_or("artifact")
    );
    let up = match ssh
        .upload_and_verify(&incoming, &bytes, &spec.artifact_sha256)
        .await
    {
        Ok(o) => o.stdout,
        Err(e) => {
            fail(&store, &spec, "UPLOAD", &format!("{e:#}"));
            let _ = ssh.exec(&format!("rm -f {}", shq(&incoming))).await;
            ssh.close().await;
            let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
            return ChainOutcome::FailedNoRestore;
        }
    };
    log_step(&store, &spec, "UPLOAD", "OK", &up);

    // ---- UNPACK: extract to releases/<version>, locate the binary
    let release_dir = format!("{}/releases/{}", spec.install_dir, spec.artifact_version);
    if spec.artifact_version.contains('/') || spec.artifact_version.contains("..") {
        fail(&store, &spec, "UNPACK", "制品版本号含路径字符，拒绝解包");
        let _ = ssh.exec(&format!("rm -f {}", shq(&incoming))).await;
        ssh.close().await;
        let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
        return ChainOutcome::FailedNoRestore;
    }
    let clean_part = if spec.clean {
        format!("rm -rf {} && ", shq(&release_dir))
    } else {
        String::new()
    };
    let unpack_cmd = format!(
        "{clean}mkdir -p {dir} && tar -xzf {inc} -C {dir} && \
         find {dir} -name tailrace-node -type f | head -1",
        clean = clean_part,
        dir = shq(&release_dir),
        inc = shq(&incoming),
    );
    let bin_path = match ssh.exec(&unpack_cmd).await {
        Ok(o) if o.ok() && !o.stdout.trim().is_empty() => o.stdout.trim().to_string(),
        Ok(o) => {
            let reason = if o.stdout.trim().is_empty() {
                "包内未找到 tailrace-node 可执行文件".to_string()
            } else {
                format!("tar 解包失败（exit={}）：{}", o.code, o.stderr.trim())
            };
            fail(&store, &spec, "UNPACK", &reason);
            let _ = ssh
                .exec(&format!(
                    "rm -f {} && rm -rf {}",
                    shq(&incoming),
                    shq(&release_dir)
                ))
                .await;
            ssh.close().await;
            let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
            return ChainOutcome::FailedNoRestore;
        }
        Err(e) => {
            fail(&store, &spec, "UNPACK", &format!("{e:#}"));
            let _ = ssh
                .exec(&format!(
                    "rm -f {} && rm -rf {}",
                    shq(&incoming),
                    shq(&release_dir)
                ))
                .await;
            ssh.close().await;
            let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
            return ChainOutcome::FailedNoRestore;
        }
    };
    let _ = ssh.exec(&format!("chmod +x {}", shq(&bin_path))).await;
    // path under `current` = path under the release dir (current -> releases/<v>)
    let bin_rel = bin_path
        .strip_prefix(&format!("{}/", release_dir))
        .unwrap_or(&bin_path)
        .to_string();
    let _ = ssh.exec(&format!("rm -f {}", shq(&incoming))).await;
    log_step(
        &store,
        &spec,
        "UNPACK",
        "OK",
        &format!(
            "releases/{} ← {}（binary={bin_rel}）",
            spec.artifact_version,
            short(&spec.artifact_sha256)
        ),
    );

    // ---- CONFIGURE: snapshot prev, stop old engine, switch current
    // (from here on, failure => rollback)
    let stop = stop_command(&spec.install_dir);
    let switch_cmd = format!(
        "cd {dir} || exit 1; \
         if readlink current >/dev/null 2>&1; then ln -sfn \"$(readlink current)\" prev; else rm -f prev; fi; \
         {stop}; ln -sfn {rel} current && readlink current",
        dir = shq(&spec.install_dir),
        stop = stop,
        rel = shq(&format!("releases/{}", spec.artifact_version)),
    );
    match ssh.exec(&switch_cmd).await {
        Ok(o) if o.ok() => {
            log_step(
                &store,
                &spec,
                "CONFIGURE",
                "OK",
                &format!(
                    "旧进程已停；current → releases/{}（prev={}）",
                    spec.artifact_version,
                    prev_release.as_deref().unwrap_or("(none)")
                ),
            );
        }
        Ok(o) => {
            let msg = format!("CONFIGURE 失败（exit={}）：{}", o.code, o.stderr.trim());
            return rollback(state, spec, ssh, "CONFIGURE", &msg, had_prev).await;
        }
        Err(e) => {
            let msg = format!("CONFIGURE 失败：{e:#}");
            return rollback(state, spec, ssh, "CONFIGURE", &msg, had_prev).await;
        }
    }

    // ---- START: launch the new binary
    let start_cmd = start_command(&spec, &bin_rel, spec.fresh_token.as_deref());
    match ssh.exec(&start_cmd).await {
        Ok(o) if o.ok() => {
            log_step(
                &store,
                &spec,
                "START",
                "OK",
                "nohup 已拉起（pid 已写 engine.pid）",
            );
        }
        Ok(o) => {
            let msg = format!("START 失败（exit={}）：{}", o.code, o.stderr.trim());
            return rollback(state, spec, ssh, "START", &msg, had_prev).await;
        }
        Err(e) => {
            let msg = format!("START 失败：{e:#}");
            return rollback(state, spec, ssh, "START", &msg, had_prev).await;
        }
    }

    // ---- HEALTH_WAIT: poll /healthz on the new binary
    let deadline = tokio::time::Instant::now() + Duration::from_secs(health_deadline_secs);
    let mut healthy = false;
    while tokio::time::Instant::now() < deadline {
        if engine_healthy(&state, &spec.host, spec.engine_port).await {
            healthy = true;
            break;
        }
        tokio::time::sleep(HEALTH_POLL_INTERVAL).await;
    }
    if !healthy {
        // surface the process exit tail for the operator
        let tail = ssh
            .exec(&format!(
                "tail -c 600 {}/engine.log 2>/dev/null || true",
                shq(&spec.install_dir)
            ))
            .await
            .map(|o| o.stdout)
            .unwrap_or_default();
        let msg = format!(
            "HEALTH_WAIT 超时（{health_deadline_secs}s 内 /healthz 未就绪）；engine.log 尾部：{}",
            tail.trim()
        );
        return rollback(state, spec, ssh, "HEALTH_WAIT", &msg, had_prev).await;
    }
    log_step(&store, &spec, "HEALTH_WAIT", "OK", "/healthz 200");

    // ---- REGISTER: node self-registers + version verification
    if let Err(e) = wait_registered(&state, &spec, deadline).await {
        return rollback(state, spec, ssh, "REGISTER", &format!("{e:#}"), had_prev).await;
    }
    log_step(
        &store,
        &spec,
        "REGISTER",
        "OK",
        &format!(
            "节点已注册（engine_version={} == 制品 {}）",
            spec.artifact_version, spec.artifact_version
        ),
    );

    // ---- REGISTERED: terminal state
    let _ = store.set_node_artifact(spec.node_id, spec.artifact_id);
    let _ = store.set_node_deploy_status(spec.node_id, "REGISTERED");
    log_step(
        &store,
        &spec,
        "REGISTERED",
        "OK",
        &format!("八步完成：current → releases/{}", spec.artifact_version),
    );
    ssh.close().await;
    ChainOutcome::Registered
}

/// Failure rollback (§4.4 acceptance focus): restore the previous release,
/// restart the old binary, re-confirm health. `failed_step` names the eight-
/// step machine position that tripped (its FAIL row is written here).
async fn rollback(
    state: AppState,
    spec: ChainSpec,
    mut ssh: SshSession,
    failed_step: &str,
    fail_msg: &str,
    had_prev: bool,
) -> ChainOutcome {
    let store = state.store().clone();
    fail(&store, &spec, failed_step, fail_msg);

    log_step(
        &store,
        &spec,
        "ROLLBACK",
        "FAIL",
        "触发回滚（任一步失败→恢复旧二进制→健康确认）",
    );

    // stop whatever is running from the failed attempt
    let _ = ssh.exec(&stop_command(&spec.install_dir)).await;

    if !had_prev {
        // fresh deploy: no previous release to restore — remove the broken
        // current link and leave the node FAILED (nothing was served before).
        let _ = ssh
            .exec(&format!(
                "cd {dir} || exit 1; rm -f current prev engine.pid; rm -f {dir}/incoming-*.tar.gz",
                dir = shq(&spec.install_dir)
            ))
            .await;
        log_step(
            &store,
            &spec,
            "ROLLBACK",
            "FAIL",
            "无先前版本可回滚（首次部署失败）：已移除损坏的 current 链接，节点标记 FAILED",
        );
        let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
        ssh.close().await;
        return ChainOutcome::FailedNoRestore;
    }

    // restore prev -> current
    let restore = format!(
        "cd {dir} || exit 1; ln -sfn \"$(readlink prev)\" current && rm -f prev && readlink current",
        dir = shq(&spec.install_dir)
    );
    match ssh.exec(&restore).await {
        Ok(o) if o.ok() => {
            log_step(
                &store,
                &spec,
                "ROLLBACK",
                "OK",
                &format!("current 已回退 → {}", o.stdout.trim()),
            );
        }
        Ok(o) => {
            log_step(
                &store,
                &spec,
                "ROLLBACK",
                "FAIL",
                &format!("回退符号链接失败（exit={}）：{}", o.code, o.stderr.trim()),
            );
            let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
            ssh.close().await;
            return ChainOutcome::FailedNoRestore;
        }
        Err(e) => {
            log_step(
                &store,
                &spec,
                "ROLLBACK",
                "FAIL",
                &format!("回退命令执行失败：{e:#}"),
            );
            let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
            ssh.close().await;
            return ChainOutcome::FailedNoRestore;
        }
    }

    // locate the OLD binary under the restored current and restart it
    let old_bin = match ssh
        .exec(&format!(
            "find {}/current/ -name tailrace-node -type f 2>/dev/null | head -1",
            shq(&spec.install_dir)
        ))
        .await
    {
        Ok(o) if !o.stdout.trim().is_empty() => o
            .stdout
            .trim()
            .strip_prefix(&format!("{}/current/", spec.install_dir))
            .unwrap_or(o.stdout.trim())
            .to_string(),
        _ => {
            log_step(
                &store,
                &spec,
                "ROLLBACK",
                "FAIL",
                "回退目录中未找到 tailrace-node，无法恢复旧进程",
            );
            let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
            ssh.close().await;
            return ChainOutcome::FailedNoRestore;
        }
    };
    let start_old = start_command(&spec, &old_bin, None);
    if let Ok(o) = ssh.exec(&start_old).await {
        if o.ok() {
            log_step(
                &store,
                &spec,
                "ROLLBACK",
                "OK",
                "旧二进制已重启（无 --bootstrap-token，沿用持久化节点 token）",
            );
        } else {
            log_step(
                &store,
                &spec,
                "ROLLBACK",
                "FAIL",
                &format!("旧进程拉起失败（exit={}）：{}", o.code, o.stderr.trim()),
            );
            let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
            ssh.close().await;
            return ChainOutcome::FailedNoRestore;
        }
    }

    // health re-confirm on the restored binary
    let deadline = tokio::time::Instant::now()
        + Duration::from_secs(
            std::env::var(HEALTH_TIMEOUT_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(DEFAULT_HEALTH_TIMEOUT_SECS),
        );
    let mut healthy = false;
    while tokio::time::Instant::now() < deadline {
        if engine_healthy(&state, &spec.host, spec.engine_port).await {
            healthy = true;
            break;
        }
        tokio::time::sleep(HEALTH_POLL_INTERVAL).await;
    }
    let _ = store.set_node_deploy_status(spec.node_id, "FAILED");
    if healthy {
        log_step(
            &store,
            &spec,
            "ROLLBACK",
            "OK",
            "回滚完成：旧二进制健康确认（/healthz 200），部署标记 FAILED、服务恢复",
        );
        ssh.close().await;
        ChainOutcome::RolledBack
    } else {
        log_step(
            &store,
            &spec,
            "ROLLBACK",
            "FAIL",
            "回滚后健康确认失败：旧二进制未在窗口内恢复 /healthz，需人工介入",
        );
        ssh.close().await;
        ChainOutcome::FailedNoRestore
    }
}

fn short(sha: &str) -> &str {
    sha.get(..8).unwrap_or(sha)
}

// ---------------- process actions (§4.4 action endpoint) ----------------

/// Synchronous SSH-managed process actions. `upgrade` is NOT here — it rides
/// the full eight-step chain via [`run_deploy_chain`].
pub enum ProcessAction {
    Start,
    Stop,
    Restart,
    Rollback,
    Uninstall,
}

impl ProcessAction {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "start" => Some(Self::Start),
            "stop" => Some(Self::Stop),
            "restart" => Some(Self::Restart),
            "rollback" => Some(Self::Rollback),
            "uninstall" => Some(Self::Uninstall),
            _ => None,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Start => ACTION_START,
            Self::Stop => ACTION_STOP,
            Self::Restart => ACTION_RESTART,
            Self::Rollback => ACTION_ROLLBACK,
            Self::Uninstall => ACTION_UNINSTALL,
        }
    }
}

/// Run one process action over SSH; each phase lands in deploy_log under the
/// action's own label. Returns Err when SSH or the remote side fails.
pub async fn run_process_action(
    state: &AppState,
    spec: &ChainSpec,
    action: ProcessAction,
) -> anyhow::Result<()> {
    let store = state.store().clone();
    let label = action.label();
    let log = |step: &str, status: &str, output: &str| {
        let _ = store.insert_deploy_log(
            spec.node_id,
            label,
            step,
            status,
            output,
            Some(&spec.operator),
        );
    };
    let mut ssh = SshSession::connect(&spec.host, spec.ssh_port, &spec.ssh_user).await?;
    let dir = shq(&spec.install_dir);

    match action {
        ProcessAction::Start => {
            let bin = locate_current_binary(&mut ssh, &spec.install_dir).await?;
            let out = ssh.exec(&start_command(spec, &bin, None)).await?;
            if !out.ok() {
                anyhow::bail!("START 失败（exit={}）：{}", out.code, out.stderr.trim());
            }
            log("START", "OK", "引擎已通过 SSH 拉起");
            wait_health(state, spec, 15).await
        }
        ProcessAction::Stop => {
            let out = ssh.exec(&stop_command(&spec.install_dir)).await?;
            if !out.ok() {
                anyhow::bail!("STOP 失败（exit={}）：{}", out.code, out.stderr.trim());
            }
            log(
                "STOP",
                "OK",
                "SIGTERM→SIGKILL（pidfile）；90s 无心跳后节点判 OFFLINE",
            );
            Ok(())
        }
        ProcessAction::Restart => {
            let stop_out = ssh.exec(&stop_command(&spec.install_dir)).await?;
            if !stop_out.ok() {
                anyhow::bail!(
                    "RESTART(stop) 失败（exit={}）：{}",
                    stop_out.code,
                    stop_out.stderr.trim()
                );
            }
            let bin = locate_current_binary(&mut ssh, &spec.install_dir).await?;
            let out = ssh.exec(&start_command(spec, &bin, None)).await?;
            if !out.ok() {
                anyhow::bail!(
                    "RESTART(start) 失败（exit={}）：{}",
                    out.code,
                    out.stderr.trim()
                );
            }
            log(
                "RESTART",
                "OK",
                "stop → start 完成（at-least-once 重放窗口见引擎游标）",
            );
            wait_health(state, spec, 20).await
        }
        ProcessAction::Rollback => {
            // current ⇄ prev swap, then start + health
            let has_prev = ssh
                .exec(&format!(
                    "cd {dir} || exit 1; readlink prev 2>/dev/null || true"
                ))
                .await?
                .stdout
                .trim()
                .to_string();
            if has_prev.is_empty() {
                log("ROLLBACK", "FAIL", "prev 链接不存在，无可回滚版本");
                anyhow::bail!("prev 链接不存在，无可回滚版本（先完成一次成功部署）");
            }
            let _ = ssh.exec(&stop_command(&spec.install_dir)).await?;
            let swap = format!(
                "cd {dir} || exit 1; cur=$(readlink current); ln -sfn \"$(readlink prev)\" current && ln -sfn \"$cur\" prev && readlink current"
            );
            let out = ssh.exec(&swap).await?;
            if !out.ok() {
                log(
                    "ROLLBACK",
                    "FAIL",
                    &format!("版本对调失败（exit={}）：{}", out.code, out.stderr.trim()),
                );
                anyhow::bail!("版本对调失败（exit={}）：{}", out.code, out.stderr.trim());
            }
            let bin = locate_current_binary(&mut ssh, &spec.install_dir).await?;
            let out = ssh.exec(&start_command(spec, &bin, None)).await?;
            if !out.ok() {
                log(
                    "ROLLBACK",
                    "FAIL",
                    &format!(
                        "回退版本拉起失败（exit={}）：{}",
                        out.code,
                        out.stderr.trim()
                    ),
                );
                anyhow::bail!(
                    "回退版本拉起失败（exit={}）：{}",
                    out.code,
                    out.stderr.trim()
                );
            }
            log(
                "ROLLBACK",
                "OK",
                &format!("current ⇄ prev 对调完成 → {}", out.stdout.trim()),
            );
            wait_health(state, spec, 20).await?;
            let _ = store.set_node_deploy_status(spec.node_id, "REGISTERED");
            Ok(())
        }
        ProcessAction::Uninstall => {
            let _ = ssh.exec(&stop_command(&spec.install_dir)).await?;
            let out = ssh.exec(&format!("rm -rf {dir}")).await?;
            if !out.ok() {
                anyhow::bail!("UNINSTALL 失败（exit={}）：{}", out.code, out.stderr.trim());
            }
            log("UNINSTALL", "OK", "进程已停，安装目录已清除");
            let _ = store.set_node_deploy_status(spec.node_id, "UNINSTALLED");
            Ok(())
        }
    }
}

async fn locate_current_binary(ssh: &mut SshSession, install_dir: &str) -> anyhow::Result<String> {
    let out = ssh
        .exec(&format!(
            "find {}/current/ -name tailrace-node -type f 2>/dev/null | head -1",
            shq(install_dir)
        ))
        .await?;
    let bin = out.stdout.trim();
    if bin.is_empty() {
        anyhow::bail!("current 链接下未找到 tailrace-node（install_dir={install_dir}）");
    }
    // relative to `current` (the start command runs ./current/<rel>)
    Ok(bin
        .strip_prefix(&format!("{install_dir}/current/"))
        .unwrap_or(bin)
        .to_string())
}

async fn wait_health(state: &AppState, spec: &ChainSpec, timeout_secs: u64) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    while tokio::time::Instant::now() < deadline {
        if engine_healthy(state, &spec.host, spec.engine_port).await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(1000)).await;
    }
    anyhow::bail!("引擎 {timeout_secs}s 内未恢复 /healthz")
}
