//! T6 攒批三阈值验收（roadmap §四 T6）：行数/字节/时间**任一满足即 flush**
//! （Doris Flink connector `sink.buffer-flush.max-rows / max-bytes / interval`
//! 语义）。每个测试只留一个可触发的阈值（另两个放到测试时窗内不可达），
//! 经**真** FileSource/FileSink 与真参数面（`PlanOptions::tuning` → RunOptions
//! 的 clamp 接线）跑 serve 模式。
//!
//! 触发归属的证明：终局 flush 只发生在 stop 之后——在 RUNNING 期间通过台账
//! totals 观察到的任何 flush 必为阈值触发；再把另两个阈值压到不可达
//! （行数≪flush_rows、字节≪flush_bytes、耗时≪flush_interval），唯一可达的
//! 阈值即归属。
//!
//! 第四个测试采集默认阈值下的**批大小分布**（docs/BENCHMARKS.md §4.6 的
//! 数据来源：从 hub 的 FLUSH_OK 事件提取每次 flush 的行数）。

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use tailrace_catalog::{PlanOptions, PlanTableObject, StatementSpec, TuningOptions};
use tailrace_runtime::registry::PipelineRegistry;

// ==================== 共用夹具 ====================

/// Unique temp work dir per (tag, pid, nanos).
fn tmp_workdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "tailrace-t6-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).expect("create workdir");
    d
}

/// cwd 串行化守卫（FileSink cwd 相对——与 tests/t5 的 CwdGuard 同构）。
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

/// Poll `probe` every 100ms until it returns Some, or `timeout` elapses.
fn poll_until<T>(timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Some(v) = probe() {
            return Some(v);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Seed the source file with `rows` JSON lines（order_no 28 字符 + qty int64，
/// 行宽与宏基准种子同量级）。
fn seed_input(rows: usize) {
    use std::io::Write;
    let mut f = std::fs::File::create("input").expect("create input");
    for i in 0..rows {
        writeln!(f, r#"{{"order_no":"ORD-{i:024}","qty":{i}}}"#).expect("write line");
    }
}

/// 三阈值 tuning 参数面（None = 引擎默认）。
fn tuning(
    flush_rows: Option<u64>,
    flush_interval_ms: Option<u64>,
    flush_bytes: Option<u64>,
    poll_rows: Option<usize>,
) -> Option<PlanOptions> {
    Some(PlanOptions {
        tuning: Some(TuningOptions {
            flush_rows,
            flush_interval_ms,
            flush_bytes,
            poll_rows,
            poll_slice_ms: None,
            dim_max_rows: None,
            dim_max_bytes: None,
        }),
        ..Default::default()
    })
}

fn t6_statements() -> Vec<StatementSpec> {
    vec![StatementSpec {
        source: "1.local.input".into(),
        group: "tr.p5420.s0".into(),
        sink: "1.local.orders_out".into(),
        pushdown: vec![],
        select_sql: "SELECT order_no, qty FROM src".into(),
        start_offset: None,
        dims: Vec::new(),
    }]
}

fn t6_tables() -> Vec<PlanTableObject> {
    let cols = vec![
        ("order_no".to_string(), "utf8".to_string()),
        ("qty".to_string(), "int64".to_string()),
    ];
    vec![
        PlanTableObject {
            reference: "1.local.input".into(),
            kind: "file".into(),
            format: Some("json".into()),
            columns: cols.clone(),
            bootstrap: None,
            stream_load_url: None,
            auth_env: None,
            sequence_column: None,
            partial_columns: None,
        },
        PlanTableObject {
            reference: "1.local.orders_out".into(),
            kind: "file".into(),
            format: Some("jsonl".into()),
            columns: cols,
            bootstrap: None,
            stream_load_url: None,
            auth_env: None,
            sequence_column: None,
            partial_columns: None,
        },
    ]
}

/// 台账 totals（consumed, flushed, flushes）。
fn ledger_totals(reg: &PipelineRegistry, id: u64) -> Option<(u64, u64, u64)> {
    let v = reg.offsets(id).ok()?;
    let t = &v["statements"][0]["ledger"]["totals"];
    Some((
        t["consumed"].as_u64()?,
        t["flushed"].as_u64()?,
        t["flushes"].as_u64()?,
    ))
}

/// 活动计数器的 consumed（stats 端点数据源——原子计数器，不经台账落盘点，
/// 用于 stop 前确认源已拉干：台账在最后一次 flush 点就不再前进，而终局
/// flush 只提交已拉取的行）。
fn live_consumed(reg: &PipelineRegistry, id: u64) -> Option<u64> {
    Some(reg.stats(id)?.totals.consumed)
}

/// hub 里的 FLUSH_OK 每次批大小（含 final flush，按 seq 序）。
fn flush_sizes(reg: &PipelineRegistry, id: u64) -> Vec<(bool, u64)> {
    let (entries, _) = reg.log_hub().tail(id, 0, 10_000);
    let mut out = Vec::new();
    for e in entries {
        if e.kind != "FLUSH_OK" {
            continue;
        }
        let final_flush = e.message.starts_with("final flush");
        let rows = e
            .message
            .split("rows=")
            .nth(1)
            .and_then(|s| s.split([' ', ',']).next())
            .and_then(|s| s.parse::<u64>().ok())
            .expect("FLUSH_OK rows= parse");
        out.push((final_flush, rows));
    }
    out
}

/// 分布统计（min/p50/p90/p99/max）。
fn dist_stats(mut sizes: Vec<u64>) -> (usize, u64, u64, u64, u64, u64) {
    sizes.sort_unstable();
    let pick = |q: f64| -> u64 {
        let idx = (((q / 100.0) * sizes.len() as f64).ceil() as usize)
            .saturating_sub(1)
            .min(sizes.len() - 1);
        sizes[idx]
    };
    (
        sizes.len(),
        sizes[0],
        pick(50.0),
        pick(90.0),
        pick(99.0),
        sizes[sizes.len() - 1],
    )
}

// ==================== T6 验收测试 ====================

/// T6 验收①行数阈值：flush_rows=200、poll_rows=100（interval=600s、
/// bytes=1GiB 不可达）→ 500 行 → RUNNING 期间恰好 2 次 flush 各 200 行
/// （poll 2/4 后 pending=200 恰好过界），尾部 100 行留给终局 flush。
#[test]
fn t6_row_boundary_triggers_flush() -> anyhow::Result<()> {
    let work = tmp_workdir("rows");
    let _cwd = CwdGuard::enter(&work);
    seed_input(500);
    let reg = PipelineRegistry::new(&work.join("state"));
    reg.deploy(
        5421,
        1,
        t6_statements(),
        t6_tables(),
        None,
        tuning(Some(200), Some(600_000), Some(1 << 30), Some(100)),
        None,
        false,
    )?;
    // 台账 totals 只在 flush 点落账：第 2 次阈值 flush（poll 4 后）时
    // consumed=400——尾部 100 行在 stop 前无 flush 点，不进台账（真提交面
    // 由 stop 后的文件行数断言）。
    let (consumed, flushed, flushes) = poll_until(Duration::from_secs(30), || {
        ledger_totals(&reg, 5421).filter(|(c, f, fl)| *c == 400 && *f == 400 && *fl == 2)
    })
    .expect("row boundary: 2 live flushes of 200 rows each");
    assert_eq!((consumed, flushed, flushes), (400, 400, 2));
    // 台账在最后一次 flush 点定格——stop 前等源拉干（否则终局 flush 无行可
    // 提交，文件面就会少尾部 100 行）
    poll_until(Duration::from_secs(10), || {
        live_consumed(&reg, 5421).filter(|c| *c == 500)
    })
    .expect("source drained before stop");
    reg.stop_pipeline(5421);
    // 500 行全部落盘（阈值 flush 400 + 终局 100）
    let lines = std::fs::read_to_string("orders_out")?.lines().count();
    assert_eq!(lines, 500, "no loss: threshold flushes + final flush");
    Ok(())
}

/// T6 验收②字节阈值：flush_rows=1M、interval=600s 不可达，flush_bytes=1024
/// → 每 poll（100 行 arrow 内存 ≈3-4KiB ≥ 1KiB）即过界 → 300 行 →
/// RUNNING 期间 3 次 flush 各 100 行（行数 300≪1M、耗时≪600s → 字节边界
/// 是唯一可达的触发器）。
#[test]
fn t6_byte_boundary_triggers_flush() -> anyhow::Result<()> {
    let work = tmp_workdir("bytes");
    let _cwd = CwdGuard::enter(&work);
    seed_input(300);
    let reg = PipelineRegistry::new(&work.join("state"));
    reg.deploy(
        5422,
        1,
        t6_statements(),
        t6_tables(),
        None,
        tuning(Some(1_000_000), Some(600_000), Some(1024), Some(100)),
        None,
        false,
    )?;
    let (consumed, flushed, flushes) = poll_until(Duration::from_secs(30), || {
        ledger_totals(&reg, 5422).filter(|(c, f, fl)| *c == 300 && *f == 300 && *fl == 3)
    })
    .expect("byte boundary: 3 live flushes of 100 rows each");
    assert_eq!((consumed, flushed, flushes), (300, 300, 3));
    reg.stop_pipeline(5422);
    let lines = std::fs::read_to_string("orders_out")?.lines().count();
    assert_eq!(lines, 300);
    Ok(())
}

/// T6 验收③时间阈值：flush_rows=1M、bytes=1GiB 不可达，interval=1s →
/// 100 行一次 poll 拉满后源到 EOF → idle 轮询路径 1s 边界 flush 100 行
/// （RUNNING 期间，非终局 flush）。
#[test]
fn t6_time_boundary_triggers_flush() -> anyhow::Result<()> {
    let work = tmp_workdir("time");
    let _cwd = CwdGuard::enter(&work);
    seed_input(100);
    let reg = PipelineRegistry::new(&work.join("state"));
    reg.deploy(
        5423,
        1,
        t6_statements(),
        t6_tables(),
        None,
        tuning(Some(1_000_000), Some(1_000), Some(1 << 30), Some(100)),
        None,
        false,
    )?;
    let (consumed, flushed, flushes) = poll_until(Duration::from_secs(30), || {
        ledger_totals(&reg, 5423).filter(|(c, f, fl)| *c == 100 && *f == 100 && *fl == 1)
    })
    .expect("time boundary: 1 live flush of the trailing 100-row batch");
    assert_eq!((consumed, flushed, flushes), (100, 100, 1));
    reg.stop_pipeline(5423);
    let lines = std::fs::read_to_string("orders_out")?.lines().count();
    assert_eq!(lines, 100);
    Ok(())
}

/// T6 验收④批大小分布（BENCHMARKS.md §4.6 数据源）：100k 行、**默认阈值**
/// （tuning 全缺省 = 10k rows / 8MiB / 10s / poll 8000）→ 检查点在微批边界：
/// pending 越过 10k 时已在第 2 个 poll（2×8000=16000）→ 每批 16000 行；
/// 12.5 个 poll 消费完 → 6 次 16000 + 终局 4000。断言分布形状并打印分布行。
#[test]
fn t6_flush_size_distribution_default_thresholds() -> anyhow::Result<()> {
    let work = tmp_workdir("dist");
    let _cwd = CwdGuard::enter(&work);
    seed_input(100_000);
    let reg = PipelineRegistry::new(&work.join("state"));
    reg.deploy(
        5424,
        1,
        t6_statements(),
        t6_tables(),
        None,
        tuning(None, None, None, None), // engine defaults
        None,
        false,
    )?;
    // 等第 6 次（最后一次）阈值 flush 落账：flush 点时 consumed=96000（12 个
    // poll）；尾部 4000 行在 stop 前无 flush 点，不进台账（终局 flush 由
    // stop 后的 hub 事件与文件行数断言）。
    poll_until(Duration::from_secs(120), || {
        ledger_totals(&reg, 5424).filter(|(c, f, fl)| *c == 96_000 && *f == 96_000 && *fl == 6)
    })
    .expect("default thresholds: 6 threshold flushes of 16000 rows each");
    // stop 前等源拉干（第 13 个 poll 的 4000 行）——终局 flush 只提交已拉取的行
    poll_until(Duration::from_secs(30), || {
        live_consumed(&reg, 5424).filter(|c| *c == 100_000)
    })
    .expect("source drained before stop");
    reg.stop_pipeline(5424);
    let sizes = flush_sizes(&reg, 5424);
    let threshold_sizes: Vec<u64> = sizes.iter().filter(|(f, _)| !f).map(|(_, r)| *r).collect();
    let final_sizes: Vec<u64> = sizes.iter().filter(|(f, _)| *f).map(|(_, r)| *r).collect();
    assert_eq!(
        threshold_sizes,
        vec![16_000; 6],
        "row boundary checked per poll: 2×8000 crosses 10k → every flush is 16000"
    );
    assert_eq!(final_sizes, vec![4_000], "stop finalizes the 4000-row tail");
    assert_eq!(
        threshold_sizes.iter().sum::<u64>() + final_sizes.iter().sum::<u64>(),
        100_000,
        "conservation across flushes"
    );
    let (n, min, p50, p90, p99, max) = dist_stats(threshold_sizes.clone());
    println!(
        "T6 batch-size distribution (default thresholds, file→file 100k rows): \
         flushes={n} min={min} p50={p50} p90={p90} p99={p99} max={max} final={}",
        final_sizes.iter().sum::<u64>()
    );
    let lines = std::fs::read_to_string("orders_out")?.lines().count();
    assert_eq!(lines, 100_000);
    Ok(())
}

/// T6 补充：字节阈值塑形的批大小分布（默认 #[ignore]——为 BENCHMARKS.md
/// §4.6 采集对照数据；显式跑：
/// `cargo test -p tailrace-runtime --test t6_flush_thresholds -- --ignored --nocapture`）。
/// flush_bytes=128KiB、poll_rows=8000（每 poll arrow 内存 ≈320KiB ≥ 128KiB）
/// → 每个 poll 都过字节界 → 批被字节阈值塑形为 8000 行（而非默认的 16000）；
/// 尾部 4000 行（≈160KiB）同样过界 → 也是阈值 flush（无终局 flush）。
#[test]
#[ignore = "bench-data collection for BENCHMARKS.md §4.6 (slow: 100k rows)"]
fn t6_flush_size_distribution_byte_limited() -> anyhow::Result<()> {
    let work = tmp_workdir("distbytes");
    let _cwd = CwdGuard::enter(&work);
    seed_input(100_000);
    let reg = PipelineRegistry::new(&work.join("state"));
    reg.deploy(
        5425,
        1,
        t6_statements(),
        t6_tables(),
        None,
        tuning(Some(1_000_000), Some(600_000), Some(128 * 1024), Some(8000)),
        None,
        false,
    )?;
    poll_until(Duration::from_secs(120), || {
        ledger_totals(&reg, 5425).filter(|(c, f, fl)| *c == 100_000 && *f == 100_000 && *fl == 13)
    })
    .expect("byte-limited: 12 flushes of 8000 + the 4000-row tail (also over 128KiB)");
    reg.stop_pipeline(5425);
    let sizes = flush_sizes(&reg, 5425);
    let threshold_sizes: Vec<u64> = sizes.iter().filter(|(f, _)| !f).map(|(_, r)| *r).collect();
    let final_sizes: Vec<u64> = sizes.iter().filter(|(f, _)| *f).map(|(_, r)| *r).collect();
    assert_eq!(&threshold_sizes[..12], &[8000u64; 12][..]);
    assert_eq!(
        threshold_sizes[12], 4_000,
        "the 4000-row tail crosses 128KiB too"
    );
    assert!(
        final_sizes.is_empty(),
        "nothing left for the stop-time final flush"
    );
    let (n, min, p50, p90, p99, max) = dist_stats(threshold_sizes.clone());
    println!(
        "T6 batch-size distribution (byte-limited 128KiB, file→file 100k rows): \
         flushes={n} min={min} p50={p50} p90={p90} p99={p99} max={max} final=0"
    );
    let lines = std::fs::read_to_string("orders_out")?.lines().count();
    assert_eq!(lines, 100_000);
    Ok(())
}
