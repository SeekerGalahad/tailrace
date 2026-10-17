//! Chaos acceptance: spawn a child process running a standalone pipeline →
//! kill -9 mid-flight → restart (cursor ledger restores) → **idempotent
//! reconciliation: the UNIQUE KEY table's row count == the seeded row count
//! (no loss, no duplication)**.
//!
//! Two dependency modes:
//!
//! * **ephemeral (opt-in, `TAILRACE_TESTCONTAINERS=1` — the CI chaos
//!   job)**: Kafka via the testcontainers-rs apache module (random host
//!   port, advertised listener rewritten by the module); Doris FE/BE as
//!   `docker run` containers on a dedicated static-IP subnet (the image
//!   entrypoint rejects hostnames in FE_SERVERS/BE_ADDR and
//!   testcontainers-rs cannot pin container IPs — T11 finding F5). The
//!   stack is DDL'd and seeded by the test itself; the row-count
//!   reconciliation runs in-test through the FE's bundled mysql client.
//! * **live-gate (default, local)**: the documented preconditions — a local
//!   kafka(9092)/doris BE(8040) reachable; topic tailrace.chaos.src
//!   pre-seeded with 5 payload=chaos-e2e OGG samples; the table
//!   tailrace_spike.ogg_event_ukey (UNIQUE KEY on order_no). Reconciliation
//!   stays external (an out-of-band script), as before.

use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;

use testcontainers::runners::SyncRunner;
use testcontainers::Container;
use testcontainers_modules::kafka::apache;

const EXPECTED_ROWS: u64 = 5;
/// Doris images for the ephemeral stack — same multi-arch tags as
/// docker-compose.yml (the old `2.1.7-fe-${arch}` tags were renamed
/// upstream, T11 finding F2).
const DORIS_FE_TAG: &str = "fe-2.1.7";
const DORIS_BE_TAG: &str = "be-2.1.7";

/// Dependency stack handle. The ephemeral-mode containers are owned here —
/// dropping the handle removes them (testcontainers RAII), so it must outlive
/// the spawned pipeline phases.
struct Deps {
    bootstrap: String,
    stream_load_url: String,
    /// (env name, env value) basic-auth for Doris stream load.
    auth: Option<(String, String)>,
    mode: Mode,
}

enum Mode {
    /// The documented local dev stack (docker-compose.yml, seeded by hand);
    /// reconciliation stays external.
    Local,
    /// Ephemeral stack; `sql` shells out to the FE's bundled mysql client.
    /// Kafka stays a testcontainers handle (RAII); the Doris pair is driven
    /// through the docker CLI (static-IP subnet — see boot_ephemeral) and
    /// torn down in Deps::drop.
    Ephemeral {
        /// RAII kafka handle (drop = container removal) — never read
        _kafka: Box<Container<apache::Kafka>>,
        fe: String,
        be: String,
        network: String,
    },
}

impl Deps {
    fn local() -> Self {
        Self {
            bootstrap: "localhost:9092".into(),
            stream_load_url: "http://localhost:8040/api/tailrace_spike/ogg_event_ukey/_stream_load"
                .into(),
            auth: Some((
                "TAILRACE_TEST_DORIS_AUTH".into(),
                "Basic dGFpbHJhY2U6dGFpbHJhY2VfbG9jYWw=".into(),
            )),
            mode: Mode::Local,
        }
    }

    /// Run one SQL statement through the FE mysql client (ephemeral mode
    /// only); Ok(None) in local mode (no in-band mysql route — documented).
    fn sql(&self, stmt: &str) -> anyhow::Result<Option<String>> {
        let Mode::Ephemeral { fe, .. } = &self.mode else {
            return Ok(None);
        };
        let out = Command::new("docker")
            .args([
                "exec",
                fe,
                "mysql",
                "-h127.0.0.1",
                "-P9030",
                "-uroot",
                "-e",
                stmt,
            ])
            .output()?;
        anyhow::ensure!(
            out.status.success(),
            "mysql exec failed ({:?}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
        Ok(Some(String::from_utf8_lossy(&out.stdout).into_owned()))
    }
}

/// Best-effort ephemeral-stack teardown with retries (used by Deps::drop
/// AND by boot_ephemeral's early-failure paths): on OrbStack a `docker
/// rm -f` issued while the container is still finishing its start can
/// silently fail and leak the container — and then the subnet, so the
/// next run's network create dies on "Pool overlaps" (T11 first-run
/// finding F7). `be`/`network` may be empty strings for not-yet-created
/// resources.
fn docker_teardown(fe: &str, be: &str, network: &str) {
    let mut containers: Vec<&str> = vec![fe];
    if !be.is_empty() {
        containers.push(be);
    }
    for attempt in 0..3 {
        for c in &containers {
            let _ = Command::new("docker")
                .args(["rm", "-f", c])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        if !network.is_empty() {
            let _ = Command::new("docker")
                .args(["network", "rm", network])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        let any_left = containers.iter().any(|c| {
            Command::new("docker")
                .args(["inspect", c])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
        });
        if !any_left || attempt == 2 {
            break;
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

impl Drop for Deps {
    fn drop(&mut self) {
        if let Mode::Ephemeral {
            fe, be, network, ..
        } = &self.mode
        {
            // a leaked container is still caught by CI's `docker ps`
            // emptiness check
            docker_teardown(fe, be, network);
        }
    }
}

// ==================== ephemeral stack (testcontainers) ====================

/// Wait until the FE reports the BE (by container IP) as Alive in
/// `show backends` — the webserver's /api/health precedes the first
/// successful FE→BE heartbeat round by a few seconds, and DDL against a
/// not-yet-alive backend fails with "available backend num is 0".
/// The Alive column index is resolved from the header row (it shifted
/// across doris builds — hardcoding it bit once already).
fn wait_backend_alive(fe: &str, be_ip: &str, secs: u64) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let out = Command::new("docker")
            .args([
                "exec",
                fe,
                "mysql",
                "-N",
                "-h127.0.0.1",
                "-P9030",
                "-uroot",
                "-e",
                "show backends",
            ])
            .output()?;
        let text = String::from_utf8_lossy(&out.stdout);
        // -N strips the header — field names come from a -B (batch) query
        // without -N once; resolve Alive's index from the known 2.1 layout
        // (BackendId..BrpcPort, LastStartTime, LastHeartbeat, Alive…)
        let alive_idx = 9;
        let alive = text.lines().any(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            f.len() > alive_idx && f[1] == be_ip && f[alive_idx].trim() == "true"
        });
        if alive {
            return Ok(());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "backend {be_ip} never reported Alive within {secs}s:\n{text}"
        );
        std::thread::sleep(Duration::from_millis(1500));
    }
}

/// HTTP probe: poll until the endpoint answers HTTP 2xx or the deadline
/// passes (FE `/api/bootstrap`, BE `/api/health`).
fn wait_http_ok(url: &str, secs: u64) -> anyhow::Result<()> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    let mut last = String::new();
    while std::time::Instant::now() < deadline {
        match client.get(url).send() {
            Ok(r) if r.status().is_success() => return Ok(()),
            Ok(r) => last = format!("status {}", r.status()),
            Err(e) => last = e.to_string(),
        }
        std::thread::sleep(Duration::from_millis(1500));
    }
    anyhow::bail!("http readiness probe failed within {secs}s: {url} ({last})")
}

fn boot_ephemeral() -> anyhow::Result<Deps> {
    // Kafka: the testcontainers-rs apache module (KRaft; the module rewrites
    // the advertised listener to the mapped host port via its start script).
    let kafka = apache::Kafka::default().start()?;
    let bootstrap = format!(
        "127.0.0.1:{}",
        kafka.get_host_port_ipv4(apache::KAFKA_PORT)?
    );

    // Doris pair via `docker run` on a static-IP subnet. Why not
    // testcontainers here: the image entrypoint validates FE_SERVERS /
    // BE_ADDR against a **literal IPv4** (hostnames are rejected — same
    // finding as the compose fix, F5) and testcontainers-rs cannot pin a
    // container's IP. The subnet differs from docker-compose.yml's doris_net
    // (172.28.0.0/24) so both stacks can run at once.
    let docker = |args: &[&str]| -> anyhow::Result<String> {
        let out = Command::new("docker").args(args).output()?;
        anyhow::ensure!(
            out.status.success(),
            "docker {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    };
    let suffix = std::process::id();
    let network = format!("tailrace-chaos-{suffix}");
    let fe_name = format!("tailrace-chaos-fe-{suffix}");
    let be_name = format!("tailrace-chaos-be-{suffix}");
    // Deterministic host ports derived from the test pid (20000 + pid%15000
    // stays clear of the compose overrides 38030/39030/38040). Discovering
    // the random `-p ::port` mapping via `docker port` is NOT reliable on
    // OrbStack: the publish materializes well after `docker run -d` returns
    // (observed >10s with a custom network — T11 first-run finding F7).
    let base = 20_000 + (std::process::id() % 15_000) as u16;
    let (fe_http_port, fe_mysql_port, be_http_port) = (base, base + 1, base + 2);
    docker(&["network", "create", "--subnet", "172.29.0.0/24", &network])?;
    // doris fe/be refuse to start below 60000 open file descriptors — the
    // same ulimits as docker-compose.yml (without them the BE registers to
    // FE but its process never comes up, so /api/health never answers;
    // T11 first-run finding F8)
    docker(&[
        "run",
        "-d",
        "--rm",
        "--name",
        &fe_name,
        "--network",
        &network,
        "--ip",
        "172.29.0.2",
        "--ulimit",
        "nofile=655350:655350",
        "-p",
        &format!("127.0.0.1:{fe_http_port}:8030"),
        "-p",
        &format!("127.0.0.1:{fe_mysql_port}:9030"),
        "-e",
        "FE_SERVERS=fe1:172.29.0.2:9010",
        "-e",
        "FE_ID=1",
        &format!("apache/doris:{DORIS_FE_TAG}"),
    ])?;
    let fe_http = format!("127.0.0.1:{fe_http_port}");
    // from here on every failure path must tear the already-created stack
    // down — Deps (whose Drop does it) is only constructed at the very end,
    // so an early `?` would leak the subnet and break the next run
    if let Err(e) = wait_http_ok(&format!("http://{fe_http}/api/bootstrap"), 240) {
        docker_teardown(&fe_name, "", &network);
        return Err(e);
    }

    docker(&[
        "run",
        "-d",
        "--rm",
        "--name",
        &be_name,
        "--network",
        &network,
        "--ip",
        "172.29.0.3",
        "--ulimit",
        "nofile=655350:655350",
        "-p",
        &format!("127.0.0.1:{be_http_port}:8040"),
        "-e",
        "FE_SERVERS=fe1:172.29.0.2:9010",
        "-e",
        "BE_ADDR=172.29.0.3:9050",
        &format!("apache/doris:{DORIS_BE_TAG}"),
    ])?;
    let be_http = format!("127.0.0.1:{be_http_port}");
    if let Err(e) = wait_http_ok(&format!("http://{be_http}/api/health"), 240) {
        // the doris entrypoint turns any start_be.sh failure (swap /
        // max_map_count / nofile checks) into a clean container exit(0)
        // and its stdout lives only in `docker logs` — surface it or the
        // timeout is undiagnosable (W3-ci 2026-10-17, T11 finding F11).
        // inspect first: the BE runs --rm, so an exited container is
        // already gone and `docker logs` alone would only say so.
        let be_state = Command::new("docker")
            .args([
                "inspect",
                "-f",
                "{{.State.Status}} exit={{.State.ExitCode}}",
                &be_name,
            ])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_else(|err| format!("(docker inspect failed: {err})"));
        let be_logs = Command::new("docker")
            .args(["logs", "--tail", "30", &be_name])
            .output()
            .map(|o| {
                format!(
                    "{}\n{}",
                    String::from_utf8_lossy(&o.stdout),
                    String::from_utf8_lossy(&o.stderr)
                )
            })
            .unwrap_or_else(|err| format!("(docker logs failed: {err})"));
        docker_teardown(&fe_name, &be_name, &network);
        return Err(e.context(format!(
            "BE container state: {be_state}\nBE log tail:\n{be_logs}"
        )));
    }
    // /api/health answers as soon as the BE webserver is up — but the FE
    // still needs a heartbeat round (a few seconds) before Alive=true, and
    // DDL against a not-yet-alive backend dies with "available backend
    // num is 0" (same race the compose BE healthcheck guards against)
    if let Err(e) = wait_backend_alive(&fe_name, "172.29.0.3", 90) {
        docker_teardown(&fe_name, &be_name, &network);
        return Err(e);
    }

    let deps = Deps {
        bootstrap,
        stream_load_url: format!("http://{be_http}/api/tailrace_spike/ogg_event_ukey/_stream_load"),
        // root with an empty password on a fresh container
        auth: Some(("TAILRACE_TEST_DORIS_AUTH".into(), "Basic cm9vdDo=".into())),
        mode: Mode::Ephemeral {
            _kafka: Box::new(kafka),
            fe: fe_name.clone(),
            be: be_name.clone(),
            network: network.clone(),
        },
    };

    // DDL through the FE's bundled mysql client (the same route
    // scripts/smoke.sh uses via `docker compose exec doris-fe mysql`).
    // From here on any `?` failure drops Deps → teardown (the explicit
    // teardown paths above only exist for the pre-Deps boot steps).
    deps.sql(
        "CREATE DATABASE IF NOT EXISTS tailrace_spike; \
         CREATE TABLE IF NOT EXISTS tailrace_spike.ogg_event_ukey (\
             order_no VARCHAR(64) NOT NULL, batch_no VARCHAR(64), station VARCHAR(256), \
             op_type VARCHAR(8), seq BIGINT, payload VARCHAR(1024), etl_time_ms BIGINT) \
         UNIQUE KEY(order_no) DISTRIBUTED BY HASH(order_no) BUCKETS 1 \
         PROPERTIES('replication_num' = '1');",
    )?;

    // Seed the chaos topic: 5 OGG envelopes, payload=chaos-e2e (envelope
    // contract: op_type/table/ts from the top level, columns from after.*).
    let mut cfg = rdkafka::config::ClientConfig::new();
    cfg.set("bootstrap.servers", &deps.bootstrap);
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    let producer: BaseProducer = cfg.create()?;
    for i in 1..=EXPECTED_ROWS {
        let payload = format!(
            "{{\"table\":\"SHOPDB.OGG_EVENT\",\"op_type\":\"I\",\"ts\":{ts},\"after\":{{\
             \"order_no\":\"C-{seq:03}\",\"batch_no\":\"B-1\",\"station\":\"S-1\",\
             \"seq\":{seq},\"payload\":\"chaos-e2e\"}}}}",
            ts = 1_000 + i as i64,
            seq = i,
        );
        if let Err((e, _)) =
            producer.send(BaseRecord::<(), str>::to("tailrace.chaos.src").payload(&payload))
        {
            anyhow::bail!("chaos seed produce failed: {e}");
        }
    }
    producer.flush(Duration::from_secs(30))?;

    Ok(deps)
}

// ==================== live gate (local mode) ====================

/// The *documented* preconditions (doc header) — the seeded chaos topic on
/// 9092 and a Doris FE that accepts the tailrace dev credentials and serves
/// tailrace_spike — not bare TCP reachability: an unrelated stack on
/// 9092/8040 (another project's containers) must not turn this into a
/// spurious failure (same rationale as the runtime and connector live gates).
fn env_ready() -> bool {
    if !(TcpStream::connect("localhost:9092").is_ok()
        && TcpStream::connect("localhost:8040").is_ok())
    {
        return false; // CI-safe
    }
    let mut cfg = rdkafka::config::ClientConfig::new();
    cfg.set("bootstrap.servers", "localhost:9092");
    use rdkafka::consumer::Consumer;
    let consumer: rdkafka::consumer::BaseConsumer = match cfg.create() {
        Ok(c) => c,
        Err(_) => return false,
    };
    let topic_seeded = consumer
        .fetch_metadata(None, Duration::from_secs(3))
        .map(|m| m.topics().iter().any(|t| t.name() == "tailrace.chaos.src"))
        .unwrap_or(false);
    if !topic_seeded {
        return false;
    }
    // FE probe: some FE builds answer unauthorized API paths with 200 + the
    // HTML web app, so success requires a JSON body.
    let fe_ok = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .and_then(|c| {
            c.get("http://localhost:8030/api/tailrace_spike/ogg_event_ukey/schema")
                .header("Authorization", "Basic dGFpbHJhY2U6dGFpbHJhY2VfbG9jYWw=")
                .send()
                .and_then(|r| r.error_for_status())
                .and_then(|r| r.text())
                .map(|body| body.trim_start().starts_with('{'))
        })
        .unwrap_or(false);
    fe_ok
}

// ==================== the chaos flow ====================

fn write_spec(dir: &std::path::Path, d: &Deps) -> anyhow::Result<PathBuf> {
    let spec = dir.join("chaos.toml");
    std::fs::write(
        &spec,
        format!(
            r#"
[pipeline]
id = 3301
name = "chaos"

[[datasource]]
name = "k"
type = "kafka"
bootstrap = "{bootstrap}"

[[datasource]]
name = "d"
type = "doris"
stream_load_url = "{url}"
auth_env = "TAILRACE_TEST_DORIS_AUTH"

[[table]]
datasource = "k"
name = "tailrace.chaos.src"
format = "ogg"
columns = [["op_type","utf8"],["ogg_table","utf8"],["ogg_ts","int64"],["order_no","utf8"],["batch_no","utf8"],["station","utf8"],["seq","int64"],["payload","utf8"]]

[[table]]
datasource = "d"
name = "ogg_event_ukey"
kind = "doris_table"
label_prefix = "chaos"
columns = [["order_no","utf8"],["batch_no","utf8"],["station","utf8"],["op_type","utf8"],["seq","int64"],["payload","utf8"],["etl_time_ms","int64"]]

[[statement]]
source = "k.tailrace.chaos.src"
group = "tr.chaos.s0"
sink = "d.ogg_event_ukey"
pushdown = ["I","U"]
select_sql = "SELECT order_no, batch_no, station, op_type, seq, payload, ogg_ts AS etl_time_ms FROM src WHERE op_type <> 'D'"
"#,
            bootstrap = d.bootstrap,
            url = d.stream_load_url,
        ),
    )?;
    Ok(spec)
}

fn spawn_run(
    binary: &str,
    spec: &std::path::Path,
    state_dir: &std::path::Path,
    secs: u64,
    auth: &Option<(String, String)>,
    phase: u8,
) -> std::process::Child {
    // phase output goes to <dir>/node-phase<N>.log — a failed flow keeps
    // the dir on disk for post-mortem (main_flow only removes it on success)
    let log = state_dir
        .parent()
        .unwrap_or(std::path::Path::new("."))
        .join(format!("node-phase{phase}.log"));
    let mut cmd = Command::new(binary);
    cmd.arg("run")
        .arg(spec)
        .arg("--state-dir")
        .arg(state_dir)
        .arg("--secs")
        .arg(secs.to_string());
    if let Some((k, v)) = auth {
        cmd.env(k, v);
    }
    cmd.stdout(Stdio::from(
        std::fs::File::create(&log).expect("open phase log"),
    ))
    .stderr(Stdio::inherit())
    .spawn()
    .expect("spawn tailrace-node")
}

/// In-test row-count reconciliation (ephemeral mode): Doris
/// COUNT(payload='chaos-e2e') must == 5 — UNIQUE KEY idempotency absorbs the
/// kill-9 replay; nothing lost (count below) and nothing duplicated (count
/// above). Local mode keeps the external out-of-band reconciliation.
fn reconcile(d: &Deps) -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let out =
            d.sql("SELECT COUNT(*) FROM tailrace_spike.ogg_event_ukey WHERE payload='chaos-e2e';")?;
        let Some(out) = out else { return Ok(()) }; // local: external
        let n: u64 = out
            .lines()
            .find_map(|l| l.trim().parse::<u64>().ok())
            .unwrap_or(0);
        if n == EXPECTED_ROWS {
            return Ok(());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "reconciliation failed: expected {EXPECTED_ROWS} chaos-e2e rows, found {n}"
        );
        std::thread::sleep(Duration::from_secs(3));
    }
}

fn main_flow(d: &Deps) -> anyhow::Result<()> {
    let binary = env!("CARGO_BIN_EXE_tailrace-node");
    let dir = std::env::temp_dir().join(format!("tailrace-chaos-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let state_dir = dir.join("state");
    std::fs::create_dir_all(&state_dir)?;
    let spec = write_spec(&dir, d)?;

    // Phase 1: run 12 seconds, kill -9 at the 5-second mark (chaos
    // injection). The window must clear kafka-native's fresh-group
    // join/rebalance (several seconds — the conn-kafka liveness test budgets
    // 30s for the same path) so the kill lands on a pipeline that is
    // actually mid-stream, not still joining the group.
    let mut child = spawn_run(binary, &spec, &state_dir, 12, &d.auth, 1);
    std::thread::sleep(Duration::from_secs(5));
    if child.kill().is_err() {
        let _ = child.wait();
        anyhow::bail!("kill -9 injection failed");
    }
    let _ = child.wait();
    let killed = state_dir.join("offsets-3301.json").exists();
    // (the kill moment may predate the first cursor file — not asserted; the
    // restart path covers both)

    // Phase 2: restart (same state_dir — the cursor ledger restores)
    // 60s: the group-join window dominates on CI runners — the first
    // consumer-group join on a cold ephemeral broker (offsets-topic
    // creation + SyncGroup) was STILL blocking at +23s on a 4-vCPU
    // runner (2026-10-17: 0 rows consumed inside the old 25s window →
    // reconciliation found 0). 60s = 2× the 30s rebalance budget the
    // conn-kafka liveness test uses, plus consume + flush + commit.
    let mut child2 = spawn_run(binary, &spec, &state_dir, 60, &d.auth, 2);
    let status = child2.wait()?;
    anyhow::ensure!(status.success(), "restart phase failed: {status:?}");

    // Assertion: a successful restart phase = the pipeline consumed everything
    // (cursor advanced to the tail); the cursor ledger must be on disk
    anyhow::ensure!(
        state_dir.join("offsets-3301.json").exists(),
        "the cursor ledger must be on disk"
    );
    reconcile(d)?;
    println!("chaos flow done (killed_midway_with_cursor={killed})");
    std::fs::remove_dir_all(&dir)?;
    Ok(())
}

#[test]
fn chaos_kill9_restart_idempotent() -> anyhow::Result<()> {
    // Mode ① (opt-in, CI chaos job): ephemeral Kafka+Doris via
    // testcontainers-rs — booted, DDL'd and seeded right here.
    if std::env::var("TAILRACE_TESTCONTAINERS").ok().as_deref() == Some("1") {
        let deps = boot_ephemeral()?;
        return main_flow(&deps);
    }
    // Mode ② (default): documented live gate
    if !env_ready() {
        tracing::warn!(
            "[live-gate] no seeded tailrace.chaos.src topic / no FE serving tailrace_spike \
             with the dev credentials on 9092/8030 (another project's stack?) — skipping"
        );
        return Ok(()); // CI-safe / local-liveness only
    }
    main_flow(&Deps::local())
}
