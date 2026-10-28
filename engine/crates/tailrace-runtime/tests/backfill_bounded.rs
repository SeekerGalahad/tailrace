//! T-A2 bounded backfill semantics（动工即写，钉住改造不得扰动的语义）：
//!
//! ① `run_bounded(from, to)` 跑到 `to` 即 flush + commit + 退出——游标
//!    恰停在 `to`（行边界对齐的 to，poll 粒度无 overshoot），输出行数
//!    恰为 `to` 之前的行数；
//! ② bf-{job} 台账命名空间隔离：回填成功/失败均**不写主管道游标**——
//!    主 `offsets-{id}.json` 逐字节不变，回填游标只落在 bf- 目录；
//! ③ 失败场景（sink flush 失败）不提交回填游标（重放窗口语义保持），
//!    主管道游标同样不动；
//! ④ 不可达的 `to`（超出源末尾）由 idle/EOF 守卫退出，不挂死。
//!
//! 源用真 FileSource（File cursor = byte_pos，`from/to` 直接可表达），
//! 汇用真 FileSink（jsonl）；reference 走 cwd 相对路径（同 t5 /
//! tests-contract 的 CwdGuard 惯例——PlanCatalog 的 reference 末段即
//! 表名/路径，绝对路径含 `.` 会被 reference 解析吃掉扩展名）。

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use tailrace_catalog::{PlanCatalog, PlanTableObject, StatementSpec};
use tailrace_ir::Cursor;
use tailrace_runtime::dlq::BadRowPolicy;
use tailrace_runtime::{backfill_state_dir, default_registry, PipelineRunner, RunOptions};
use tailrace_state::{FileOffsetStore, OffsetStore as _};

// ==================== 共用夹具 ====================

/// Unique temp work dir per (tag, pid, nanos).
fn tmp_workdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "tailrace-bf-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).expect("create workdir");
    d
}

/// cwd 串行化守卫（FileSource/FileSink cwd 相对——与 t5 的 CwdGuard 同构）。
static CWD_LOCK: Mutex<()> = Mutex::new(());

struct CwdGuard {
    prev: PathBuf,
    _lock: MutexGuard<'static, ()>,
}

impl CwdGuard {
    fn enter(dir: &Path) -> Self {
        let lock = CWD_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::current_dir().expect("current_dir");
        std::env::set_current_dir(dir).unwrap_or_else(|e| {
            panic!("chdir into {}: {e}", dir.display());
        });
        Self { prev, _lock: lock }
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.prev);
    }
}

/// Seed an equal-length jsonl source file（qty 恒 7 保证每行等长，行边界
/// 字节偏移可预计算）。返回 (文件名, 单行字节数含 \n)。
fn seed_source(rows: usize) -> (&'static str, usize) {
    let mut text = String::new();
    for i in 0..rows {
        text.push_str(&format!("{{\"order_no\":\"ORD-{i:06}\",\"qty\":7}}\n"));
    }
    std::fs::write("orders_in", text).expect("seed source");
    let line_len = "{\"order_no\":\"ORD-000000\",\"qty\":7}\n".len();
    ("orders_in", line_len)
}

fn bf_statements(sink_name: &str) -> Vec<StatementSpec> {
    vec![StatementSpec {
        source: "1.local.orders_in".into(),
        group: "bf.s0".into(),
        sink: format!("1.local.{sink_name}"),
        pushdown: vec![],
        select_sql: "SELECT order_no, qty FROM src".into(),
        start_offset: None,
        dims: Vec::new(),
    }]
}

fn bf_tables(sink_name: &str) -> Vec<PlanTableObject> {
    let cols = vec![
        ("order_no".to_string(), "utf8".to_string()),
        ("qty".to_string(), "int64".to_string()),
    ];
    vec![
        PlanTableObject {
            reference: "1.local.orders_in".into(),
            kind: "file".into(),
            format: Some("json".into()),
            columns: cols.clone(),
            bootstrap: None,
            stream_load_url: None,
            auth_env: None,
            sequence_column: None,
        },
        PlanTableObject {
            reference: format!("1.local.{sink_name}"),
            kind: "file".into(),
            format: Some("jsonl".into()),
            columns: cols,
            bootstrap: None,
            stream_load_url: None,
            auth_env: None,
            sequence_column: None,
        },
    ]
}

/// Backfill-shaped options: poll one row per turn, flush every poll (the
/// cursor commit rides every flush — the tightest observable).
fn bf_options() -> RunOptions {
    RunOptions {
        duration: Duration::from_secs(60),
        flush_rows: 1,
        flush_interval: Duration::from_secs(1),
        flush_bytes: 0,
        poll_rows: 1,
        poll_slice: Duration::from_millis(10),
        ..Default::default()
    }
}

/// 主 ledger 预置游标（模拟主管道已推进到 byte_pos = far_ahead）并返回
/// 预置后的文件内容（逐字节对照基线）。
fn preset_main_ledger(state_dir: &Path, pipeline_id: u64, src: &str, far_ahead: u64) -> String {
    let mut store = FileOffsetStore::new(state_dir).expect("main ledger store");
    store
        .save(
            pipeline_id,
            0,
            &Cursor::File {
                path: src.into(),
                byte_pos: far_ahead,
            },
            Some("main-pipeline-flush-1"),
            None,
        )
        .expect("preset main cursor");
    std::fs::read_to_string(state_dir.join(format!("offsets-{pipeline_id}.json")))
        .expect("main ledger readable")
}

/// 输出 jsonl 的非空行数（FileSink 的提交面）。
fn out_rows(path: &str) -> usize {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count()
}

// ==================== T-A2 验收测试 ====================

/// 验收①②：run_bounded 到 `to` 即 flush + commit + 退出；bf 命名空间隔离
/// ——回填成功后主 ledger 逐字节不变，回填游标只落 bf- 目录。
#[test]
fn bounded_run_stops_at_to_and_never_touches_the_main_ledger() -> anyhow::Result<()> {
    let work = tmp_workdir("stop");
    let _cwd = CwdGuard::enter(&work);
    let state_dir = work.join("state");
    std::fs::create_dir_all(&state_dir)?;
    let (src, line_len) = seed_source(50);
    // to = 第 20 行末尾的字节边界（行边界对齐 → poll 粒度无 overshoot）
    let to = (line_len * 20) as i64;
    let pipeline_id = 6101u64;
    let baseline = preset_main_ledger(&state_dir, pipeline_id, src, 999_999);

    let bf_dir = backfill_state_dir(&state_dir, "20261028-01");
    assert_eq!(
        bf_dir,
        state_dir.join("bf-20261028-01"),
        "bf- namespace dir shape"
    );
    // double-prefix tolerance: a job id already carrying the marker is not
    // prefixed twice
    assert_eq!(
        backfill_state_dir(&state_dir, "bf-20261028-01"),
        bf_dir,
        "no bf-bf- double prefix"
    );

    let catalog = PlanCatalog::new(
        pipeline_id,
        bf_statements("orders_out"),
        bf_tables("orders_out"),
    );
    let mut runner = PipelineRunner::assemble(
        &catalog,
        &bf_dir,
        bf_options(),
        None,
        BadRowPolicy::default(),
        &default_registry(),
    )?;
    let stats = runner.run_bounded(0, to)?;
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].flushed_rows, 20, "exactly the rows below `to`");
    assert_eq!(out_rows("orders_out"), 20, "sink side: 20 rows committed");

    // ① the bounded cursor stopped exactly at `to` (row-aligned, no
    // overshoot), committed ONLY in the bf- namespace
    let bf_store = FileOffsetStore::new(&bf_dir)?;
    let (cur, _, _) = bf_store
        .load(pipeline_id, 0)?
        .expect("backfill cursor committed in the bf namespace");
    match cur {
        Cursor::File { byte_pos, .. } => assert_eq!(byte_pos as i64, to, "cursor parked at `to`"),
        other => panic!("file-shaped cursor expected, got {other:?}"),
    }
    // ② the main ledger is byte-identical — success committed nothing there
    let after = std::fs::read_to_string(state_dir.join(format!("offsets-{pipeline_id}.json")))?;
    assert_eq!(baseline, after, "main pipeline ledger byte-identical");
    let main_store = FileOffsetStore::new(&state_dir)?;
    let (main_cur, _, _) = main_store
        .load(pipeline_id, 0)?
        .expect("preset main cursor still there");
    assert_eq!(
        main_cur,
        Cursor::File {
            path: src.into(),
            byte_pos: 999_999
        },
        "main pipeline cursor value untouched by the backfill"
    );
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}

/// 验收③：回填失败（sink flush 失败）——错误上抛，主 ledger 逐字节不变，
/// bf 命名空间也无游标提交（flush 失败在 save 之前 → 重放窗口语义保持）。
#[test]
fn bounded_run_failure_never_touches_the_main_ledger() -> anyhow::Result<()> {
    let work = tmp_workdir("fail");
    let _cwd = CwdGuard::enter(&work);
    let state_dir = work.join("state");
    std::fs::create_dir_all(&state_dir)?;
    let (src, line_len) = seed_source(10);
    let to = (line_len * 10) as i64;
    let pipeline_id = 6102u64;
    let baseline = preset_main_ledger(&state_dir, pipeline_id, src, 888_888);
    // poison the sink: a directory where FileSink's jsonl flush tries to
    // append → every flush fails
    std::fs::create_dir_all(work.join("orders_bad"))?;

    let bf_dir = backfill_state_dir(&state_dir, "job-fail");
    let catalog = PlanCatalog::new(
        pipeline_id,
        bf_statements("orders_bad"),
        bf_tables("orders_bad"),
    );
    let mut runner = PipelineRunner::assemble(
        &catalog,
        &bf_dir,
        bf_options(),
        None,
        BadRowPolicy::default(),
        &default_registry(),
    )?;
    let err = runner
        .run_bounded(0, to)
        .expect_err("sink flush must fail (target is a directory)");
    // FileSink's jsonl flush propagates the raw io error (no path wrapper)
    assert!(
        err.to_string().contains("Is a directory"),
        "failure came from the poisoned sink flush: {err:#}"
    );
    // the main ledger is byte-identical — failure committed nothing there
    let after = std::fs::read_to_string(state_dir.join(format!("offsets-{pipeline_id}.json")))?;
    assert_eq!(baseline, after, "main pipeline ledger byte-identical");
    // and the bf ledger has no committed cursor (no flush succeeded)
    let bf_store = FileOffsetStore::new(&bf_dir)?;
    assert!(
        bf_store.load(pipeline_id, 0)?.is_none(),
        "no backfill cursor committed under a failing flush"
    );
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}

/// 验收④：不可达的 `to`（源早已读尽且游标未到 `to`）由 idle/EOF 守卫
/// 退出——不挂死，数据全量 flush + commit。
#[test]
fn bounded_run_unreachable_to_exits_via_idle_guard() -> anyhow::Result<()> {
    let work = tmp_workdir("idle");
    let _cwd = CwdGuard::enter(&work);
    let state_dir = work.join("state");
    std::fs::create_dir_all(&state_dir)?;
    let (src, line_len) = seed_source(5);
    let unreachable_to = (line_len * 5) as i64 + 1; // past EOF: never reached
    let pipeline_id = 6103u64;
    let _baseline = preset_main_ledger(&state_dir, pipeline_id, src, 777_777);

    let bf_dir = backfill_state_dir(&state_dir, "job-idle");
    let catalog = PlanCatalog::new(
        pipeline_id,
        bf_statements("orders_out"),
        bf_tables("orders_out"),
    );
    let mut runner = PipelineRunner::assemble(
        &catalog,
        &bf_dir,
        bf_options(),
        None,
        BadRowPolicy::default(),
        &default_registry(),
    )?;
    // returns (does not hang) — the idle/EOF guard ends the run
    let stats = runner.run_bounded(0, unreachable_to)?;
    assert_eq!(stats[0].flushed_rows, 5, "all source rows flushed");
    assert_eq!(out_rows("orders_out"), 5);
    // the guard exit still commits the cursor (EOF position)
    let bf_store = FileOffsetStore::new(&bf_dir)?;
    let (cur, _, _) = bf_store
        .load(pipeline_id, 0)?
        .expect("guard exit commits the cursor");
    match cur {
        Cursor::File { byte_pos, .. } => {
            assert_eq!(byte_pos as usize, line_len * 5, "cursor parked at EOF")
        }
        other => panic!("file-shaped cursor expected, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}
