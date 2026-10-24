//! §4.4 docker-target e2e (T35 acceptance ④): a container plays the remote
//! node, the REAL `tailrace-node` linux binary runs inside it as a true
//! process, and the console drives it over the eight-step SSH chain.
//!
//! Live gate (chaos.rs convention): `TAILRACE_TESTCONTAINERS=1` opts in;
//! additionally requires (a) a working docker daemon and (b) the linux
//! `tailrace-node` binary at `target/e2e/tailrace-node` (or
//! `TAILRACE_NODE_BIN`). One-off preparation on this dev machine:
//!
//! ```sh
//! gh release download v1.0.0 -p 'tailrace-node-1.0.0-aarch64-unknown-linux-gnu.tar.gz' \
//!   -O target/e2e/tailrace-node-linux.tar.gz
//! tar -xzf target/e2e/tailrace-node-linux.tar.gz -C target/e2e --strip-components 1 \
//!   tailrace-node-1.0.0-aarch64-unknown-linux-gnu/tailrace-node
//! docker pull debian:bookworm-slim   # glibc base (release binary is *-linux-gnu)
//! ```
//!
//! Coverage: (1) fresh hosts[] deploy reaches REGISTERED with the full step
//! trail; (2) **mid-failure rollback** — an upgrade whose "new binary" dies
//! on start makes HEALTH_WAIT fail, and the chain must restore the previous
//! release, restart the old process and re-confirm /healthz (deploy_status
//! FAILED, service back on 1.0.0).

mod common;

use std::collections::HashMap;
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

use axum::http::StatusCode;
use axum::Router;
use common::*;
use serde_json::{json, Value};

const IMAGE: &str = "tailrace-e2e-sshd:latest";
const INSTALL_DIR: &str = "/home/deploy/tailrace/engine";

fn sh(args: &[&str]) -> anyhow::Result<String> {
    let out = Command::new(args[0]).args(&args[1..]).output()?;
    if !out.status.success() {
        anyhow::bail!(
            "{:?} failed: {}{}",
            args,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral")
        .local_addr()
        .expect("local addr")
        .port()
}

/// tar.gz with `tailrace-node` at the archive root (artifact convention).
fn make_tar_gz(dir: &std::path::Path, out: &std::path::Path) -> anyhow::Result<()> {
    sh(&[
        "tar",
        "-czf",
        out.to_str().unwrap(),
        "-C",
        dir.to_str().unwrap(),
        "tailrace-node",
    ])?;
    Ok(())
}

fn multipart(boundary: &str, filename: &str, payload: &[u8], version: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        "Content-Disposition: form-data; name=\"name\"\r\n\r\ntailrace-node\r\n".as_bytes(),
    );
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"version\"\r\n\r\n{version}\r\n").as_bytes(),
    );
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!(
            "Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n\
                 Content-Type: application/gzip\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(payload);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

async fn upload_artifact(app: &TestApp, tarball: &[u8], version: &str) -> i64 {
    let boundary = "tailrace-e2e-boundary";
    let body = multipart(boundary, "node.tar.gz", tarball, version);
    let mut headers = HashMap::new();
    headers.insert(
        "content-type",
        format!("multipart/form-data; boundary={boundary}"),
    );
    let (s, b, _) = raw_request(app, "POST", "/api/artifacts", headers, body).await;
    assert_eq!(s, StatusCode::CREATED, "artifact upload {version}: {b}");
    b["id"].as_i64().unwrap()
}

async fn deploy_log(app: &TestApp, node_id: i64) -> Vec<Value> {
    let (s, b) = req(
        app,
        "GET",
        &format!("/api/nodes/{node_id}/deploy-log?limit=1000"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    b["items"].as_array().unwrap().clone()
}

async fn wait_status(app: &TestApp, node_id: i64, want: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(Some(n)) = app.store.node(node_id) {
            if n.deploy_status.as_deref() == Some(want) {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    false
}

async fn healthz_ok(port: u16) -> bool {
    let url = format!("http://127.0.0.1:{port}/healthz");
    match reqwest::get(&url).await {
        Ok(r) => r.status().is_success(),
        Err(_) => false,
    }
}

/// Teardown guard: `docker rm -f` runs even when an assertion panics
/// mid-flow (the L3 no-residue discipline, previously violated on failure).
struct RmOnDrop(String);
impl Drop for RmOnDrop {
    fn drop(&mut self) {
        let _ = sh(&["docker", "rm", "-f", &self.0]);
    }
}

fn gate_ready() -> bool {
    if std::env::var("TAILRACE_TESTCONTAINERS").ok().as_deref() != Some("1") {
        return false;
    }
    Command::new("docker")
        .args(["info"])
        .output()
        .is_ok_and(|o| o.status.success())
}

fn node_binary() -> anyhow::Result<std::path::PathBuf> {
    if let Ok(p) = std::env::var("TAILRACE_NODE_BIN") {
        let p = std::path::PathBuf::from(p);
        if p.is_file() {
            return Ok(p);
        }
    }
    let p =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../target/e2e/tailrace-node");
    if p.is_file() {
        return Ok(p);
    }
    anyhow::bail!(
        "linux tailrace-node binary missing: {} (set TAILRACE_NODE_BIN or fetch the release \
         artifact; see the module doc)",
        p.display()
    )
}

/// Build the sshd image (cached by tag) from debian:bookworm-slim + a
/// passwordless `deploy` user. glibc base — the release node binary is
/// *-linux-gnu.
fn ensure_sshd_image(workdir: &std::path::Path) -> anyhow::Result<()> {
    let listed = sh(&["docker", "images", "-q", IMAGE])?;
    if !listed.trim().is_empty() {
        return Ok(());
    }
    let dockerfile = workdir.join("Dockerfile");
    std::fs::write(
        &dockerfile,
        "FROM debian:bookworm-slim\n\
         RUN apt-get update && apt-get install -y --no-install-recommends \
         openssh-server tar \
         && rm -rf /var/lib/apt/lists/* \
         && useradd -m -s /bin/bash deploy \
         && mkdir -p /run/sshd /home/deploy/.ssh \
         && chown -R deploy:deploy /home/deploy/.ssh\n\
         CMD [\"/usr/sbin/sshd\", \"-D\", \"-e\"]\n",
    )?;
    sh(&["docker", "build", "-t", IMAGE, workdir.to_str().unwrap()])?;
    Ok(())
}

#[ignore = "live docker e2e: TAILRACE_TESTCONTAINERS=1 + docker + linux tailrace-node binary (module doc)"]
#[tokio::test]
async fn docker_e2e_ssh_deploy_and_midfailure_rollback() -> anyhow::Result<()> {
    if !gate_ready() {
        eprintln!("[live-gate] TAILRACE_TESTCONTAINERS=1 + docker required — skipping");
        return Ok(());
    }
    // ---------- local fixtures ----------
    let workdir = std::env::temp_dir().join(format!(
        "tailrace-ssh-e2e-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    ));
    std::fs::create_dir_all(workdir.join("good"))?;
    std::fs::create_dir_all(workdir.join("broken"))?;
    let bin = node_binary()?;
    std::fs::copy(&bin, workdir.join("good/tailrace-node"))?;
    sh(&[
        "chmod",
        "+x",
        workdir.join("good/tailrace-node").to_str().unwrap(),
    ])?;
    // the "new binary" that dies one second in — HEALTH_WAIT never sees 200
    std::fs::write(
        workdir.join("broken/tailrace-node"),
        "#!/bin/sh\nsleep 1\nexit 42\n",
    )?;
    sh(&[
        "chmod",
        "+x",
        workdir.join("broken/tailrace-node").to_str().unwrap(),
    ])?;
    let good_tar = workdir.join("good.tar.gz");
    let broken_tar = workdir.join("broken.tar.gz");
    make_tar_gz(&workdir.join("good"), &good_tar)?;
    make_tar_gz(&workdir.join("broken"), &broken_tar)?;

    // console deploy key (ed25519) + env wiring
    let key = workdir.join("id_ed25519");
    sh(&[
        "ssh-keygen",
        "-t",
        "ed25519",
        "-N",
        "",
        "-f",
        key.to_str().unwrap(),
    ])?;
    let pubkey = std::fs::read_to_string(key.with_extension("pub"))?
        .trim()
        .to_string();
    std::env::set_var("TAILRACE_SSH_KEY", key.to_str().unwrap());
    std::env::set_var("TAILRACE_DEPLOY_HEALTH_TIMEOUT_SECS", "40");

    // ---------- container as remote node ----------
    ensure_sshd_image(&workdir)?;
    let ssh_port = free_port();
    let engine_port = free_port();
    let console_port = free_port();
    let cname = format!(
        "tailrace-e2e-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
    );
    let _guard = RmOnDrop(cname.clone());
    let result = main_flow(
        &pubkey,
        ssh_port,
        engine_port,
        console_port,
        &cname,
        &good_tar,
        &broken_tar,
    )
    .await;
    drop(_guard); // explicit teardown on the success path; the assert below proves it
    let residue = sh(&[
        "docker",
        "ps",
        "-a",
        "--filter",
        &format!("name={cname}"),
        "-q",
    ])?;
    assert!(residue.trim().is_empty(), "container leaked: {cname}");
    std::fs::remove_dir_all(&workdir).ok();
    result
}

async fn main_flow(
    pubkey: &str,
    ssh_port: u16,
    engine_port: u16,
    console_port: u16,
    cname: &str,
    good_tar: &std::path::Path,
    broken_tar: &std::path::Path,
) -> anyhow::Result<()> {
    sh(&[
        "docker",
        "run",
        "-d",
        "--name",
        cname,
        "-p",
        &format!("{ssh_port}:22"),
        "-p",
        &format!("{engine_port}:{engine_port}"),
        IMAGE,
    ])?;
    // wait for sshd, then trust our key
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", ssh_port)).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    sh(&[
        "docker",
        "exec",
        cname,
        "sh",
        "-c",
        &format!(
            "echo '{pubkey}' > /home/deploy/.ssh/authorized_keys \
             && chmod 600 /home/deploy/.ssh/authorized_keys \
             && chown deploy:deploy /home/deploy/.ssh/authorized_keys"
        ),
    ])?;

    // ---------- real console on a TCP listener (engine registers back) ----------
    std::env::set_var(
        "TAILRACE_CONSOLE_ADVERTISE_URL",
        format!("http://host.docker.internal:{console_port}"),
    );
    let app = app().await;
    let router: Router = app.router.clone();
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", console_port)).await?;
    tokio::spawn(async move { axum::serve(listener, router).await });

    // ---------- scenario 1: fresh hosts[] deploy → REGISTERED ----------
    let good_id = upload_artifact(&app, &std::fs::read(good_tar)?, "1.0.0").await;
    let (s, b) = req(
        &app,
        "POST",
        "/api/nodes/deploy",
        Some(json!({
            "hosts": [{
                "host": "127.0.0.1", "name": "edge-docker-1", "sshUser": "deploy",
                "sshPort": ssh_port, "enginePort": engine_port, "installDir": INSTALL_DIR
            }],
            "artifactId": good_id
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "deploy accepted: {b}");
    let node_id = b["deploys"][0]["nodeId"].as_i64().unwrap();
    assert_eq!(b["deploys"][0]["status"], "PENDING");

    if !wait_status(&app, node_id, "REGISTERED", Duration::from_secs(120)).await {
        let dump = sh(&[
            "docker",
            "exec",
            cname,
            "sh",
            "-c",
            &format!(
                "ls -la {INSTALL_DIR}; readlink {INSTALL_DIR}/current 2>/dev/null; \
                 cat {INSTALL_DIR}/engine.pid 2>/dev/null; \
                 tail -c 800 {INSTALL_DIR}/engine.log 2>/dev/null; \
                 ps aux | grep -v grep | grep tailrace || true"
            ),
        ])
        .unwrap_or_else(|e| format!("(dump failed: {e})"));
        panic!(
            "fresh deploy must reach REGISTERED; log: {:#?}\ncontainer state:\n{dump}",
            deploy_log(&app, node_id).await
        );
    }
    let log = deploy_log(&app, node_id).await;
    let steps: Vec<&str> = log.iter().map(|i| i["step"].as_str().unwrap()).collect();
    assert_eq!(
        steps,
        vec![
            "PREFLIGHT",
            "UPLOAD",
            "UNPACK",
            "CONFIGURE",
            "START",
            "HEALTH_WAIT",
            "REGISTER",
            "REGISTERED"
        ],
        "eight-step trail (all OK): {log:#?}"
    );
    assert!(
        log.iter().all(|i| i["status"] == "OK"),
        "no FAIL rows in a successful deploy: {log:#?}"
    );
    let node = app.store.node(node_id)?.unwrap();
    assert_eq!(
        node.engine_version.as_deref(),
        Some("1.0.0"),
        "version echo"
    );
    assert_eq!(node.artifact_id, Some(good_id));
    assert_eq!(node.status, "ONLINE");
    assert!(healthz_ok(engine_port).await, "engine serving /healthz");
    // sha256 verification anchor visible in the UPLOAD output
    let upload_row = log.iter().find(|i| i["step"] == "UPLOAD").unwrap();
    assert!(upload_row["output"].as_str().unwrap().contains("sha256"));

    // ---------- scenario 2: mid-failure rollback (acceptance focus) ----------
    let broken_id = upload_artifact(&app, &std::fs::read(broken_tar)?, "9.9.9-broken").await;
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/nodes/{node_id}/action"),
        Some(json!({"action": "upgrade", "artifactId": broken_id})),
    )
    .await;
    assert_eq!(s, StatusCode::ACCEPTED, "upgrade accepted: {b}");
    assert_eq!(b["accepted"], true);

    assert!(
        wait_status(&app, node_id, "FAILED", Duration::from_secs(150)).await,
        "broken upgrade must fail; log: {:#?}",
        deploy_log(&app, node_id).await
    );
    let log2 = deploy_log(&app, node_id).await;
    let upgrade_rows: Vec<&Value> = log2.iter().skip(steps.len()).collect();
    let up_steps: Vec<&str> = upgrade_rows
        .iter()
        .map(|i| i["step"].as_str().unwrap())
        .collect();
    assert!(
        up_steps.contains(&"HEALTH_WAIT"),
        "failure at HEALTH_WAIT: {up_steps:?}"
    );
    let hw = upgrade_rows
        .iter()
        .find(|i| i["step"] == "HEALTH_WAIT")
        .unwrap();
    assert_eq!(hw["status"], "FAIL");
    assert!(
        up_steps.contains(&"ROLLBACK"),
        "rollback rows present: {up_steps:?}"
    );
    let rollback_ok = upgrade_rows
        .iter()
        .filter(|i| i["step"] == "ROLLBACK" && i["status"] == "OK")
        .count();
    assert!(
        rollback_ok >= 3,
        "rollback: symlink restore + restart + health all OK: {upgrade_rows:#?}"
    );
    let final_rollback = upgrade_rows
        .iter()
        .rev()
        .find(|i| i["step"] == "ROLLBACK")
        .unwrap();
    assert!(
        final_rollback["output"]
            .as_str()
            .unwrap()
            .contains("回滚完成"),
        "health re-confirm recorded: {final_rollback}"
    );

    // service truth: old binary back, healthy, still version 1.0.0
    assert!(
        healthz_ok(engine_port).await,
        "old binary re-serving /healthz"
    );
    let node = app.store.node(node_id)?.unwrap();
    assert_eq!(node.deploy_status.as_deref(), Some("FAILED"));
    assert_eq!(
        node.artifact_id,
        Some(good_id),
        "artifact pin untouched by failed upgrade"
    );
    assert_eq!(node.engine_version.as_deref(), Some("1.0.0"));
    let link = sh(&[
        "docker",
        "exec",
        cname,
        "readlink",
        &format!("{INSTALL_DIR}/current"),
    ])?;
    assert_eq!(
        link.trim(),
        "releases/1.0.0",
        "current symlink restored to the previous release"
    );

    println!(
        "docker e2e green: fresh deploy 8/8 OK + mid-failure rollback restored 1.0.0 \
         (container {cname}, ssh {ssh_port}, engine {engine_port})"
    );
    Ok(())
}
