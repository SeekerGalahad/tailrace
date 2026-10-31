//! Executor main-loop determinism tests (upgrade plan §二 engine H3②):
//! pause/resume behavior, cursor commit points, bad-row policy branches and
//! dispatch idempotency — driven directly against the PipelineRegistry
//! (the same object the /v1 routes use), fully in-process, file→file.

use std::time::Duration;

use tests_contract::*;

/// A compiled statement set for a file→file copy (no SQL needed — deploy
/// takes the compiler product).
fn file_statements() -> Vec<tailrace_catalog::StatementSpec> {
    vec![tailrace_catalog::StatementSpec {
        source: "1.local.orders_in".into(),
        group: "tr.pX.s0".into(),
        sink: "1.local.orders_out".into(),
        pushdown: vec![],
        select_sql: "SELECT order_no, qty FROM src".into(),
        start_offset: None,
        dims: Vec::new(),
        partial_columns: None,
    }]
}

fn file_tables() -> Vec<tailrace_catalog::PlanTableObject> {
    let cols = || {
        vec![
            ("order_no".to_string(), "utf8".to_string()),
            ("qty".to_string(), "int64".to_string()),
        ]
    };
    vec![
        tailrace_catalog::PlanTableObject::new("1.local.orders_in", "file")
            .with_format("jsonl")
            .with_columns(cols()),
        tailrace_catalog::PlanTableObject::new("1.local.orders_out", "file")
            .with_format("jsonl")
            .with_columns(cols()),
    ]
}

fn good_rows(from: i64, to: i64) -> Vec<serde_json::Value> {
    (from..=to)
        .map(|i| serde_json::json!({"order_no": format!("O-{i:03}"), "qty": i}))
        .collect()
}

/// Cursor commit points: the ledger cursor only advances behind successful
/// flushes, survives stop (final flush + commit), and pause freezes it.
#[test]
fn pause_resume_and_cursor_commit_points() -> anyhow::Result<()> {
    let work = tmp_workdir("commit");
    let _cwd = CwdGuard::enter(&work);
    let state_dir = work.join("state");
    let reg = std::sync::Arc::new(tailrace_runtime::registry::PipelineRegistry::new(
        &state_dir,
    ));

    append_jsonl("orders_in", &good_rows(1, 3));
    reg.deploy(
        5101,
        1,
        file_statements(),
        file_tables(),
        Some(1000),
        None,
        None,
        false,
    )?;

    // wait until the ledger shows a committed cursor (flush happened)
    let ledger = poll_until(Duration::from_secs(10), || {
        reg.offsets(5101).ok().and_then(|v| {
            let pos = v["statements"][0]["ledger"]["cursor"]["File"]["byte_pos"]
                .as_u64()
                .unwrap_or(0);
            (pos > 0).then_some(v)
        })
    })
    .expect("cursor committed after the first flush");
    let committed = ledger["statements"][0]["ledger"]["cursor"]["File"]["byte_pos"]
        .as_u64()
        .unwrap();

    // pause → append → the ledger cursor must not move (nothing consumed)
    reg.pause(5101);
    append_jsonl("orders_in", &good_rows(4, 5));
    std::thread::sleep(Duration::from_millis(1500));
    let during = reg.offsets(5101)?["statements"][0]["ledger"]["cursor"]["File"]["byte_pos"]
        .as_u64()
        .unwrap();
    assert_eq!(
        during, committed,
        "paused: no new commits (source polls nothing)"
    );

    // resume → the ledger advances past the paused window; stop → final
    // commit lands exactly at EOF (every line ends with a newline)
    reg.resume(5101);
    let file_len = std::fs::metadata("orders_in")?.len();
    let final_pos = poll_until(Duration::from_secs(10), || {
        let pos = reg.offsets(5101).ok()?["statements"][0]["ledger"]["cursor"]["File"]["byte_pos"]
            .as_u64()
            .unwrap_or(0);
        (pos >= file_len).then_some(pos)
    })
    .expect("resume catches the paused-window rows");
    assert_eq!(final_pos, file_len, "cursor converges to the file end");

    reg.stop_pipeline(5101);
    // the ledger is the survivor: readable after deregistration
    let after = reg.offsets(5101)?;
    assert_eq!(after["deployed"], serde_json::json!(false));
    assert_eq!(
        after["statements"][0]["ledger"]["cursor"]["File"]["byte_pos"]
            .as_u64()
            .unwrap_or(0),
        file_len,
        "stop committed the final cursor"
    );
    assert_eq!(line_count("orders_out"), 5, "all rows flushed");
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}

/// Bad-row policy branches (engine H2): fail → FAILED terminal (with
/// autoRestart=false for a fast deterministic verdict), skip → counted but
/// no side file, dlq (default) → quarantined.
#[test]
fn bad_row_policy_branches() -> anyhow::Result<()> {
    let work = tmp_workdir("policy");
    let _cwd = CwdGuard::enter(&work);
    let state_dir = work.join("state");
    let reg = std::sync::Arc::new(tailrace_runtime::registry::PipelineRegistry::new(
        &state_dir,
    ));

    // ① fail: autoRestart=false → the first bad row fails the statement and
    // the verdict is the FAILED terminal state
    append_jsonl("orders_in", &good_rows(1, 2));
    use std::io::Write;
    {
        let mut f = std::fs::OpenOptions::new().append(true).open("orders_in")?;
        writeln!(f, "{{broken json")?;
    }
    let fail_opts = tailrace_catalog::PlanOptions {
        bad_row_policy: Some("fail".into()),
        recovery: Some(tailrace_catalog::RecoveryOptions {
            auto_restart: Some(false),
            ..Default::default()
        }),
        ..Default::default()
    };
    reg.deploy(
        5201,
        1,
        file_statements(),
        file_tables(),
        Some(1000),
        Some(fail_opts),
        None,
        false,
    )?;
    let failed = poll_until(Duration::from_secs(10), || {
        reg.snapshot()
            .into_iter()
            .find(|(id, _, st, _)| *id == 5201 && st == "FAILED")
            .and_then(|(_, _, _, err)| err)
    })
    .expect("badRowPolicy=fail ends in FAILED");
    assert!(
        failed.contains("badRowPolicy=fail"),
        "failure names the policy: {failed}"
    );
    assert!(
        !state_dir.join("bad_rows").join("p5201-s0.jsonl").exists(),
        "fail policy writes no DLQ"
    );
    reg.stop_pipeline(5201);

    // ② skip: counted, no side file, pipeline keeps RUNNING
    let skip_opts = tailrace_catalog::PlanOptions {
        bad_row_policy: Some("skip".into()),
        ..Default::default()
    };
    reg.deploy(
        5202,
        2,
        file_statements(),
        file_tables(),
        Some(1000),
        Some(skip_opts),
        None,
        false,
    )?;
    let (consumed, bad, flushed) = poll_until(Duration::from_secs(10), || {
        let s = reg.stats(5202)?;
        let t = &s.totals;
        let (cn, bd, fl) = (t.consumed, t.bad, t.flushed);
        (cn >= 3 && bd >= 1 && fl >= 2).then_some((cn, bd, fl))
    })
    .expect("skip policy counts the bad row and flushes the good ones");
    assert_eq!((consumed, bad, flushed), (3, 1, 2));
    assert!(
        !state_dir.join("bad_rows").join("p5202-s0.jsonl").exists(),
        "skip policy writes no DLQ"
    );
    reg.stop_pipeline(5202);

    // ③ dlq (the default — no options at all): quarantined with raw bytes
    let dlq_path = state_dir.join("bad_rows").join("p5203-s0.jsonl");
    reg.deploy(
        5203,
        3,
        file_statements(),
        file_tables(),
        Some(1000),
        None,
        None,
        false,
    )?;
    poll_until(Duration::from_secs(10), || dlq_path.exists().then_some(()))
        .expect("default policy quarantines bad rows");
    let text = std::fs::read_to_string(&dlq_path)?;
    assert_eq!(
        text.lines().filter(|l| !l.trim().is_empty()).count(),
        1,
        "{text}"
    );
    reg.stop_pipeline(5203);
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}

/// T-V2 replay storm, end to end through the registry's restart loop: a
/// batch of 2 good + 3 bad rows whose flush fails (the sink target is a
/// directory) is re-polled from the uncommitted cursor on every
/// auto-restart, so the same three bad rows are isolated once per attempt.
/// The side file must hold each bad row ONCE (row-key dedup — each attempt
/// builds a fresh writer, so the index is seeded from disk), while
/// `stats.totals.bad` and the `tailrace_dlq_rows_total` series keep
/// counting every attempt (3 × attempts): the audit face never loses the
/// storm, the table never duplicates. Repairing the sink lands the good
/// rows and commits the cursor.
#[test]
fn dlq_replay_storm_dedups_rows_but_counts_attempts() -> anyhow::Result<()> {
    let work = tmp_workdir("dlqstorm");
    let _cwd = CwdGuard::enter(&work);
    let state_dir = work.join("state");
    let reg = std::sync::Arc::new(tailrace_runtime::registry::PipelineRegistry::new(
        &state_dir,
    ));
    append_jsonl("orders_in", &good_rows(1, 2));
    use std::io::Write;
    {
        let mut f = std::fs::OpenOptions::new().append(true).open("orders_in")?;
        writeln!(f, "{{broken-1")?;
        writeln!(f, "not json at all")?;
        writeln!(f, "{{\"unterminated\": ")?;
    }
    // the sink target is a DIRECTORY → every jsonl append fails → the
    // statement errors after isolating the bad rows and the registry
    // restarts it from the never-committed cursor (= replay)
    std::fs::create_dir_all("orders_out")?;
    let opts = tailrace_catalog::PlanOptions {
        recovery: Some(tailrace_catalog::RecoveryOptions {
            // fast first retry; a wide second backoff is the repair window
            // (the third consecutive no-progress failure would be terminal)
            restart_backoff_secs: Some(vec![1, 8]),
            auto_restart_max: Some(5),
            ..Default::default()
        }),
        ..Default::default()
    };
    reg.deploy(
        5401,
        1,
        file_statements(),
        file_tables(),
        Some(1000),
        Some(opts),
        None,
        false,
    )?;
    let dlq_path = state_dir.join("bad_rows").join("p5401-s0.jsonl");
    let dlq_lines = || -> usize {
        std::fs::read_to_string(&dlq_path)
            .map(|t| t.lines().filter(|l| !l.trim().is_empty()).count())
            .unwrap_or(0)
    };
    // two isolation attempts observed (first run + restart #1): the attempt
    // counter has doubled while the side file has not grown
    let bad_after_two = poll_until(Duration::from_secs(15), || {
        let bad = reg.stats(5401)?.totals.bad;
        (bad >= 6).then_some(bad)
    })
    .expect("the replay re-isolates the bad rows on restart");
    assert_eq!(bad_after_two, 6, "3 bad rows × 2 attempts");
    assert_eq!(
        dlq_lines(),
        3,
        "replayed bad rows did not duplicate the side file"
    );
    // repair the sink inside the second backoff → the next attempt flushes
    std::fs::remove_dir_all("orders_out")?;
    let committed = poll_until(Duration::from_secs(20), || {
        let v = reg.offsets(5401).ok()?;
        let pos = v["statements"][0]["ledger"]["cursor"]["File"]["byte_pos"]
            .as_u64()
            .unwrap_or(0);
        (pos > 0).then_some(pos)
    })
    .expect("after the repair the cursor commits (pipeline not FAILED)");
    assert_eq!(
        committed,
        std::fs::metadata("orders_in")?.len(),
        "cursor at EOF once the storm is over"
    );
    assert_eq!(line_count("orders_out"), 2, "the good rows landed once");
    let stats = reg.stats(5401).expect("pipeline still registered");
    let attempts = stats.totals.bad / 3;
    // the repair may land inside attempt 2's own flush window (no restart
    // after it) or in the backoff (one more replayed attempt) — either way
    // the attempt count and the file part ways
    assert!(
        attempts >= 2 && stats.totals.bad == attempts * 3,
        "every attempt counted: bad={} (= 3 bad rows × {attempts} attempts)",
        stats.totals.bad
    );
    assert_eq!(
        dlq_lines(),
        3,
        "side file = unique bad rows, not isolation attempts"
    );
    // the metric surface says the same thing: the counter is the attempt
    // count, the file is the row set
    let metrics = tailrace_api::metrics::node_metrics_text(&reg);
    assert!(
        metrics.contains(&format!(
            "tailrace_dlq_rows_total{{pipeline=\"5401\",statement=\"0\"}} {}\n",
            stats.totals.bad
        )),
        "{metrics}"
    );
    // and the three rows are the three distinct bad lines
    let raws: std::collections::BTreeSet<Vec<u8>> = std::fs::read_to_string(&dlq_path)?
        .lines()
        .map(|l| {
            use base64::Engine as _;
            let v: serde_json::Value = serde_json::from_str(l).unwrap();
            base64::engine::general_purpose::STANDARD
                .decode(v["rawB64"].as_str().unwrap())
                .unwrap()
        })
        .collect();
    assert_eq!(
        raws,
        [
            b"{broken-1".to_vec(),
            b"not json at all".to_vec(),
            b"{\"unterminated\":".to_vec(),
        ]
        .into_iter()
        .collect()
    );
    reg.stop_pipeline(5401);
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}

/// deploy idempotency: a repeated dispatch_id while the pipeline runs is
/// rejected; a fresh dispatch replaces it.
#[test]
fn dispatch_id_idempotent_rejection() -> anyhow::Result<()> {
    let work = tmp_workdir("dispatch");
    let _cwd = CwdGuard::enter(&work);
    let state_dir = work.join("state");
    let reg = std::sync::Arc::new(tailrace_runtime::registry::PipelineRegistry::new(
        &state_dir,
    ));
    append_jsonl("orders_in", &good_rows(1, 2));
    reg.deploy(
        5301,
        42,
        file_statements(),
        file_tables(),
        Some(1000),
        None,
        None,
        false,
    )?;
    // same dispatch while running → rejected
    let dup = reg.deploy(
        5301,
        42,
        file_statements(),
        file_tables(),
        Some(1000),
        None,
        None,
        false,
    );
    let err = dup.err().map(|e| format!("{e:#}")).unwrap_or_default();
    assert!(
        !err.is_empty() && err.contains("duplicate push dispatch_id=42"),
        "{err}"
    );
    // a fresh dispatch replaces the pipeline (stop-old-start-new)
    reg.deploy(
        5301,
        43,
        file_statements(),
        file_tables(),
        Some(1000),
        None,
        None,
        false,
    )?;
    poll_until(Duration::from_secs(5), || {
        reg.snapshot()
            .into_iter()
            .find(|(id, _, st, _)| *id == 5301 && st == "RUNNING")
            .map(|_| ())
    })
    .expect("re-pushed pipeline RUNNING again");
    reg.stop_pipeline(5301);
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}
