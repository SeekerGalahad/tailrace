//! /v1 protocol contract tests (upgrade plan §二 engine H3①): a real
//! in-process node, a file→file pipeline, and the whole plan-push → observe
//! → act main path asserted over HTTP — `docs/protocol/engine-v1.md` is the
//! canon these tests lock. Zero external dependencies (FileSource/FileSink
//! are pure local).

use std::time::Duration;

use tests_contract::*;

fn bearer() -> reqwest::blocking::Client {
    http()
}

/// auth contract: /healthz open, tokenless /v1 → 401, wrong token → 401,
/// right token (both header shapes) → 200.
#[test]
fn v1_auth_contract() -> anyhow::Result<()> {
    let (base, _state_dir) = spawn_node("auth");
    let c = bearer();
    // server up (healthz is unauthenticated by protocol)
    poll_until(Duration::from_secs(5), || {
        c.get(format!("http://{base}/healthz"))
            .send()
            .ok()
            .map(|r| r.status())
    })
    .filter(|s| s == &reqwest::StatusCode::OK)
    .expect("healthz answers 200");
    // tokenless /v1 rejected
    assert_eq!(
        c.get(format!("http://{base}/v1/pipelines"))
            .send()?
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    // wrong token rejected
    assert_eq!(
        c.get(format!("http://{base}/v1/pipelines"))
            .header("Authorization", "Bearer tr_b_wrong")
            .send()?
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    // right token passes on both header shapes
    assert_eq!(
        c.get(format!("http://{base}/v1/pipelines"))
            .header("Authorization", format!("Bearer {TEST_TOKEN}"))
            .send()?
            .status(),
        reqwest::StatusCode::OK
    );
    assert_eq!(
        c.get(format!("http://{base}/v1/pipelines"))
            .header("X-Node-Token", TEST_TOKEN)
            .send()?
            .status(),
        reqwest::StatusCode::OK
    );
    Ok(())
}

/// stream_load_url domain allowlist (engine H1): a plan pointing a Doris
/// stream load at a non-allowlisted host is rejected whole — the plan body
/// cannot aim the Basic credential at an arbitrary target.
#[test]
fn v1_stream_load_allowlist_contract() -> anyhow::Result<()> {
    let (base, _state_dir) = spawn_node("allow");
    let c = bearer();
    poll_until(Duration::from_secs(5), || {
        c.get(format!("http://{base}/healthz"))
            .send()
            .ok()
            .map(|r| r.status())
    })
    .filter(|s| s == &reqwest::StatusCode::OK)
    .expect("healthz answers 200");
    let plan = serde_json::json!({
        "pipelineId": 9301,
        "sqlContent": "INSERT INTO ${ds:2.shop.orders} SELECT order_no, qty FROM ${ds:1.local.orders_in}",
        "tables": [
            file_table("1.local.orders_in", &[("order_no", "utf8"), ("qty", "int64")]),
            {"reference": "2.shop.orders", "kind": "doris_table",
             "columns": [["order_no", "utf8"], ["qty", "int64"]],
             "stream_load_url": "http://attacker.example:8040/api/shop/orders/_stream_load",
             "auth_env": "TAILRACE_TEST_DORIS_AUTH"},
        ],
        "checkpointMs": 1000,
        "dispatchId": 7301,
    });
    let resp: serde_json::Value = c
        .put(format!("http://{base}/v1/pipelines/9301/plan"))
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .json(&plan)
        .send()?
        .json()?;
    assert_eq!(resp["ok"], serde_json::json!(false), "{resp}");
    let err = resp["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("allowlist") && err.contains("attacker.example"),
        "rejection names the offending host: {err}"
    );
    Ok(())
}

/// The main /v1 path: plan push (SQL-first) → list → stats growth → logs →
/// offsets ledger → pause (totals frozen) → resume (catch-up) → stop
/// (deregistered). file→file, fully in-process.
#[test]
fn v1_file_pipeline_lifecycle_contract() -> anyhow::Result<()> {
    let work = tmp_workdir("lifecycle");
    let _cwd = CwdGuard::enter(&work);
    let (base, state_dir) = spawn_node("lifecycle");
    let c = bearer();
    poll_until(Duration::from_secs(5), || {
        c.get(format!("http://{base}/healthz"))
            .send()
            .ok()
            .map(|r| r.status())
    })
    .filter(|s| s == &reqwest::StatusCode::OK)
    .expect("healthz answers 200");

    // seed 3 rows before the push
    let rows3: Vec<_> = (1..=3)
        .map(|i| serde_json::json!({"order_no": format!("O-{i:03}"), "qty": i}))
        .collect();
    append_jsonl("orders_in", &rows3);

    // plan push
    let resp: serde_json::Value = c
        .put(format!("http://{base}/v1/pipelines/9101/plan"))
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .json(&file_plan(9101, 7101))
        .send()?
        .json()?;
    assert_eq!(resp["ok"], serde_json::json!(true), "plan push: {resp}");
    assert_eq!(resp["statements"], serde_json::json!(1));

    // list shows it RUNNING
    let listed = poll_until(Duration::from_secs(5), || {
        let v: serde_json::Value = c
            .get(format!("http://{base}/v1/pipelines"))
            .header("Authorization", format!("Bearer {TEST_TOKEN}"))
            .send()
            .ok()?
            .json()
            .ok()?;
        (v["pipelines"]
            .as_array()?
            .iter()
            .any(|p| p["id"] == 9101 && p["status"] == "RUNNING"))
        .then_some(v)
    })
    .expect("pipeline listed RUNNING");

    // stats totals grow over the seed batch
    let (consumed, flushed) = poll_until(Duration::from_secs(10), || {
        let v: serde_json::Value = c
            .get(format!("http://{base}/v1/pipelines/9101/stats"))
            .header("Authorization", format!("Bearer {TEST_TOKEN}"))
            .send()
            .ok()?
            .json()
            .ok()?;
        let t = &v["stats"]["totals"];
        let (cn, fl) = (t["consumed"].as_u64()?, t["flushed"].as_u64()?);
        (cn >= 3 && fl >= 3).then_some((cn, fl))
    })
    .expect("totals consume+flush the seed batch");
    assert!(consumed >= 3 && flushed >= 3);

    // logs: structured entries with a global seq
    let logs: serde_json::Value = c
        .get(format!(
            "http://{base}/v1/pipelines/9101/logs?tail=1&limit=50"
        ))
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .send()?
        .json()?;
    assert!(logs["ok"] == serde_json::json!(true));
    let entries = logs["entries"].as_array().expect("entries");
    assert!(
        !entries.is_empty(),
        "PIPELINE_STARTED/FLUSH_OK trail exists"
    );
    assert!(entries.iter().all(|e| e["seq"].as_u64().is_some()));

    // offsets: the ledger is the source of truth (byte cursor advanced).
    // The ledger commit can land a beat after the stats counter, so poll.
    let offsets: serde_json::Value = poll_until(Duration::from_secs(10), || {
        let v: serde_json::Value = c
            .get(format!("http://{base}/v1/pipelines/9101/offsets"))
            .header("Authorization", format!("Bearer {TEST_TOKEN}"))
            .send()
            .ok()?
            .json()
            .ok()?;
        (v["ok"] == serde_json::json!(true)
            && v["deployed"] == serde_json::json!(true)
            && v["statements"][0]["ledger"]["cursor"]["File"]["byte_pos"]
                .as_u64()
                .unwrap_or(0)
                > 0)
        .then_some(v)
    })
    .expect("file cursor committed after the flush");
    let ledger = &offsets["statements"][0]["ledger"];
    assert!(
        ledger["cursor"]["File"]["byte_pos"].as_u64().unwrap_or(0) > 0,
        "file cursor committed after the flush: {offsets}"
    );

    // pause: totals freeze while rows keep arriving
    let paused: serde_json::Value = c
        .post(format!("http://{base}/v1/pipelines/9101/pause"))
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .send()?
        .json()?;
    assert_eq!(paused["status"], serde_json::json!("PAUSED"));
    let rows2: Vec<_> = (4..=5)
        .map(|i| serde_json::json!({"order_no": format!("O-{i:03}"), "qty": i}))
        .collect();
    append_jsonl("orders_in", &rows2);
    std::thread::sleep(Duration::from_millis(1500));
    let stats_paused: serde_json::Value = c
        .get(format!("http://{base}/v1/pipelines/9101/stats"))
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .send()?
        .json()?;
    assert_eq!(
        stats_paused["stats"]["totals"]["consumed"]
            .as_u64()
            .unwrap_or(0),
        consumed,
        "paused pulls nothing (kept the pre-pause totals)"
    );

    // resume: the paused-window rows catch up
    c.post(format!("http://{base}/v1/pipelines/9101/resume"))
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .send()?;
    let total5 = poll_until(Duration::from_secs(10), || {
        let v: serde_json::Value = c
            .get(format!("http://{base}/v1/pipelines/9101/stats"))
            .header("Authorization", format!("Bearer {TEST_TOKEN}"))
            .send()
            .ok()?
            .json()
            .ok()?;
        let t = v["stats"]["totals"]["consumed"].as_u64()?;
        (t >= 5).then_some(t)
    })
    .expect("resume catches the paused-window rows");
    assert!(total5 >= 5);

    // stop: deregistered, final flush landed
    let stopped: serde_json::Value = c
        .post(format!("http://{base}/v1/pipelines/9101/stop"))
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .send()?
        .json()?;
    assert_eq!(stopped["status"], serde_json::json!("STOPPED"));
    let final_lines = poll_until(Duration::from_secs(5), || {
        (line_count("orders_out") >= 5).then(|| line_count("orders_out"))
    })
    .expect("sink file holds all rows after the final flush");
    assert!(final_lines >= 5);
    let after: serde_json::Value = c
        .get(format!("http://{base}/v1/pipelines"))
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .send()?
        .json()?;
    assert!(
        !after["pipelines"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["id"] == 9101),
        "stopped pipeline deregistered (initial list was {listed})"
    );
    // ledger survives the stop (offsets still answer, deployed=false)
    let offsets2: serde_json::Value = c
        .get(format!("http://{base}/v1/pipelines/9101/offsets"))
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .send()?
        .json()?;
    assert_eq!(offsets2["deployed"], serde_json::json!(false));
    assert!(
        offsets2["statements"][0]["ledger"]["cursor"]["File"]["byte_pos"]
            .as_u64()
            .unwrap_or(0)
            > 0,
        "ledger outlives the registration"
    );
    let _ = state_dir;
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}

/// engine H2 contract: malformed rows no longer evaporate — under the
/// default policy they land in the bad_rows/ side file (raw + cursor +
/// ts), the DECODE_BAD event carries the DLQ path, totals count only good
/// rows into the sink.
#[test]
fn v1_bad_rows_dlq_contract() -> anyhow::Result<()> {
    let work = tmp_workdir("dlq");
    let _cwd = CwdGuard::enter(&work);
    let (base, state_dir) = spawn_node("dlq");
    let c = bearer();
    poll_until(Duration::from_secs(5), || {
        c.get(format!("http://{base}/healthz"))
            .send()
            .ok()
            .map(|r| r.status())
    })
    .filter(|s| s == &reqwest::StatusCode::OK)
    .expect("healthz answers 200");

    // 3 good rows + 2 malformed (not JSON) interleaved
    let good: Vec<_> = (1..=3)
        .map(|i| serde_json::json!({"order_no": format!("G-{i}"), "qty": i}))
        .collect();
    append_jsonl("orders_in", &good);
    use std::io::Write;
    {
        let mut f = std::fs::OpenOptions::new().append(true).open("orders_in")?;
        writeln!(f, "{{not json at all")?;
        writeln!(f, "also{{broken")?;
    }

    let resp: serde_json::Value = c
        .put(format!("http://{base}/v1/pipelines/9201/plan"))
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .json(&file_plan(9201, 7201))
        .send()?
        .json()?;
    assert_eq!(resp["ok"], serde_json::json!(true), "{resp}");

    let (consumed, bad, flushed) = poll_until(Duration::from_secs(10), || {
        let v: serde_json::Value = c
            .get(format!("http://{base}/v1/pipelines/9201/stats"))
            .header("Authorization", format!("Bearer {TEST_TOKEN}"))
            .send()
            .ok()?
            .json()
            .ok()?;
        let t = &v["stats"]["totals"];
        let cn = t["consumed"].as_u64()?;
        let bd = t["bad"].as_u64()?;
        let fl = t["flushed"].as_u64()?;
        (cn >= 5 && bd >= 2 && fl >= 3).then_some((cn, bd, fl))
    })
    .expect("stats see 5 consumed / 2 bad / 3 flushed");
    assert_eq!((consumed, bad, flushed), (5, 2, 3));

    // side file: one JSONL record per bad row, raw bytes recoverable
    let dlq_path = state_dir.join("bad_rows").join("p9201-s0.jsonl");
    let dlq_text = std::fs::read_to_string(&dlq_path)
        .unwrap_or_else(|e| panic!("DLQ side file {}: {e}", dlq_path.display()));
    let records: Vec<serde_json::Value> = dlq_text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("valid jsonl record"))
        .collect();
    assert_eq!(records.len(), 2, "{dlq_text}");
    assert_eq!(records[0]["pipeline"], serde_json::json!(9201));
    assert_eq!(records[0]["statement"], serde_json::json!(0));
    assert!(records[0]["tsMs"].as_u64().unwrap_or(0) > 0);
    assert!(
        records[0]["cursor"]["File"]["byte_pos"]
            .as_u64()
            .unwrap_or(0)
            > 0,
        "record carries the poll-time cursor"
    );
    use base64::Engine as _;
    for r in &records {
        let raw = base64::engine::general_purpose::STANDARD
            .decode(r["rawB64"].as_str().unwrap())
            .unwrap();
        assert!(
            String::from_utf8_lossy(&raw).starts_with("{not")
                || String::from_utf8_lossy(&raw).starts_with("also"),
            "raw bytes preserved verbatim: {}",
            String::from_utf8_lossy(&raw)
        );
    }

    // the event carries the DLQ path (not a raw sample)
    let logs: serde_json::Value = c
        .get(format!(
            "http://{base}/v1/pipelines/9201/logs?tail=1&limit=50"
        ))
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .send()?
        .json()?;
    let hit = logs["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["kind"] == "DECODE_BAD")
        .expect("DECODE_BAD event emitted")
        .clone();
    assert!(
        hit["message"].as_str().unwrap_or("").contains("bad_rows"),
        "event names the DLQ path: {hit}"
    );

    // sink holds exactly the good rows
    assert_eq!(line_count("orders_out"), 3);

    c.post(format!("http://{base}/v1/pipelines/9201/stop"))
        .header("Authorization", format!("Bearer {TEST_TOKEN}"))
        .send()?;
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}
