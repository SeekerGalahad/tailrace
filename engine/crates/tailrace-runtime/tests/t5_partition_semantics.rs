//! T5 分区并行·方案 a 的语义回归测试（**动工前先写**，钉住改造不得扰动的
//! 语义——与 roadmap §四 T5 验收一一对应）：
//!
//! ① 部分分区推进：只有部分分区有数据的 poll，flush 点**整体提交**游标
//!    （未动分区 offset 原样保留，新分区才加入 map）；
//! ② 重启不丢不重：stop（final flush + commit）→ 再 deploy，游标从台账恢复，
//!    totals 单调且继续增长，sink 侧唯一键恰好等于总行数（无重复投递、无丢失）；
//! ③ NO_PROGRESS_LIMIT=3：连续 3 次失败且游标指纹不动 → FAILED 终态；
//!    有进步（任一 flush 成功越过）则计数复位、不误杀。
//!
//! 源用 mock 多分区源（游标记账与 [`tailrace_conn_kafka`] 的 KafkaSource 同构：
//! poll 返回即推进当轮有数据分区的 next offset，cursor() 返回整个 offsets map）；
//! 成功路径的汇用**真** FileSink（cwd 相对，持 CwdGuard 串行化），失败注入用
//! mock 计数汇。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use datafusion::arrow::record_batch::RecordBatch;
use tailrace_catalog::{PlanTableObject, RecoveryOptions, StatementSpec, TableObject};
use tailrace_connector::{ConnectorRegistry, Sink, SinkAck, Source, SourceChunk};
use tailrace_ir::Cursor;
use tailrace_runtime::registry::PipelineRegistry;

// ==================== 共用夹具 ====================

/// Unique temp work dir per (tag, pid, nanos).
fn tmp_workdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "tailrace-t5-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).expect("create workdir");
    d
}

/// cwd 串行化守卫（FileSink cwd 相对——与 tests-contract 的 CwdGuard 同构）。
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

// ==================== mock 多分区源 + 计数汇 ====================

/// Mock 多分区源存储（topic → 每分区行字节）：registry 每次重启都会**重建**
/// 连接器实例，数据必须活得比实例久（像真 broker）。行 = JSON
/// {"order_no":"<topic>-p<P>-<I:06>","qty":I}。
fn mock_store() -> &'static Mutex<MockStore> {
    static S: OnceLock<Mutex<MockStore>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(MockStore::default()))
}

#[derive(Default)]
struct MockStore {
    /// topic → partition → rows
    sources: BTreeMap<String, Vec<Vec<Vec<u8>>>>,
    /// sink name → 已 flush 提交的累计行数
    sink_committed_rows: BTreeMap<String, u64>,
    /// sink name → 剩余「flush 必失败」次数（毒 sink 注入）
    sink_fail_flush: BTreeMap<String, u32>,
}

fn mockparts_seed(topic: &str, partition: usize, from: usize, rows: usize) {
    let mut g = mock_store().lock().unwrap();
    let parts = g.sources.entry(topic.to_string()).or_default();
    while parts.len() <= partition {
        parts.push(Vec::new());
    }
    for i in from..from + rows {
        parts[partition].push(
            format!("{{\"order_no\":\"{topic}-p{partition}-{i:06}\",\"qty\":{i}}}").into_bytes(),
        );
    }
}

/// 已提交行数（成功 flush 的累计——毒 sink 场景里失败尝试的 pending 不计）。
fn mocksink_committed_rows(name: &str) -> u64 {
    mock_store()
        .lock()
        .unwrap()
        .sink_committed_rows
        .get(name)
        .copied()
        .unwrap_or(0)
}

/// 让该 sink 的接下来 n 次 flush 失败（毒 sink 注入；n 足够大 = 永远失败）。
fn mocksink_fail_flush(name: &str, n: u32) {
    mock_store()
        .lock()
        .unwrap()
        .sink_fail_flush
        .insert(name.to_string(), n);
}

/// 多分区 mock 源：poll 跨分区 round-robin 拉取（一轮取一个分区一行），
/// 游标记账与 KafkaSource 完全同构——poll 返回即推进**当轮有数据分区**的
/// next offset，cursor() 返回整个 offsets map（无数据分区不动/不加入）。
struct MockPartsSource {
    topic: String,
    pos: BTreeMap<i32, i64>,
}

impl Source for MockPartsSource {
    fn open(&mut self, resume: &Cursor) -> anyhow::Result<()> {
        if let Cursor::Kafka { topic, offsets, .. } = resume {
            if *topic == self.topic && !offsets.is_empty() {
                self.pos = offsets.clone();
            }
        }
        Ok(())
    }

    fn poll(&mut self, max_rows: usize, _deadline: Duration) -> anyhow::Result<Vec<SourceChunk>> {
        if max_rows == 0 {
            return Ok(vec![]);
        }
        let guard = mock_store().lock().unwrap();
        let Some(parts) = guard.sources.get(&self.topic) else {
            return Ok(vec![]);
        };
        let mut out = Vec::new();
        'outer: loop {
            let mut progressed = false;
            for (p, rows) in parts.iter().enumerate() {
                let next = self.pos.get(&(p as i32)).copied().unwrap_or(0);
                if (next as usize) < rows.len() {
                    progressed = true;
                    self.pos.insert(p as i32, next + 1);
                    out.push(SourceChunk::kafka(
                        rows[next as usize].clone(),
                        p as i32,
                        next + 1,
                    ));
                    if out.len() >= max_rows {
                        break 'outer;
                    }
                }
            }
            if !progressed {
                break;
            }
        }
        drop(guard);
        Ok(out)
    }

    fn cursor(&self) -> Cursor {
        Cursor::Kafka {
            topic: self.topic.clone(),
            group: "mockparts".into(),
            offsets: self.pos.clone(),
        }
    }
}

/// 计数汇（毒数据判据测试专用）：write 只累计行数，flush 才提交；可注入 n
/// 次 flush 失败（失败尝试的 pending 随实例消亡——与真实 sink 的 flush 点
/// 提交语义同构）。
struct MockCountingSink {
    name: String,
    pending: u64,
    label_seq: u64,
}

impl Sink for MockCountingSink {
    fn write(&mut self, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
        self.pending += batches.iter().map(|b| b.num_rows() as u64).sum::<u64>();
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<SinkAck> {
        let mut g = mock_store().lock().unwrap();
        let left = g.sink_fail_flush.get(&self.name).copied().unwrap_or(0);
        if left > 0 {
            g.sink_fail_flush.insert(self.name.clone(), left - 1);
            anyhow::bail!("mock sink flush failure injected ({} left)", left - 1);
        }
        let rows = self.pending;
        *g.sink_committed_rows.entry(self.name.clone()).or_insert(0) += rows;
        self.pending = 0;
        self.label_seq += 1;
        Ok(SinkAck {
            rows,
            kind: "MOCK_FLUSH",
            idempotency_key: format!("mock.{}.{}", self.name, self.label_seq),
        })
    }
}

/// mockparts 源 + mocksink 汇，叠在默认 registry 上（成功路径可走真 FileSink）。
fn t5_registry() -> ConnectorRegistry {
    fn src_factory(
        obj: &TableObject,
        _spec: &StatementSpec,
        _rel: Option<&tailrace_catalog::ReliabilityOptions>,
    ) -> anyhow::Result<Box<dyn Source>> {
        Ok(Box::new(MockPartsSource {
            topic: obj.name.clone(),
            pos: BTreeMap::new(),
        }))
    }
    fn sink_factory(
        obj: &TableObject,
        _rel: Option<&tailrace_catalog::ReliabilityOptions>,
    ) -> anyhow::Result<Box<dyn Sink>> {
        Ok(Box::new(MockCountingSink {
            name: obj.name.clone(),
            pending: 0,
            label_seq: 0,
        }))
    }
    let mut reg = tailrace_runtime::default_registry();
    reg.register_source("mockparts", src_factory);
    reg.register_sink("mocksink", sink_factory);
    reg
}

fn t5_statements(topic: &str, sink_ref: &str) -> Vec<StatementSpec> {
    vec![StatementSpec {
        source: format!("1.kafka.{topic}"),
        group: "tr.p54.s0".into(),
        sink: sink_ref.into(),
        pushdown: vec![],
        select_sql: "SELECT order_no, qty FROM src".into(),
        start_offset: None,
        dims: Vec::new(),
        partial_columns: None,
    }]
}

fn t5_tables(topic: &str, sink_ref: &str, sink_kind: &str) -> Vec<PlanTableObject> {
    vec![
        PlanTableObject::new(format!("1.kafka.{topic}"), "mockparts")
            .with_format("json")
            .with_columns(vec![
                ("order_no".to_string(), "utf8".to_string()),
                ("qty".to_string(), "int64".to_string()),
            ]),
        PlanTableObject::new(sink_ref, sink_kind)
            .with_format("jsonl")
            .with_columns(vec![
                ("order_no".to_string(), "utf8".to_string()),
                ("qty".to_string(), "int64".to_string()),
            ]),
    ]
}

/// 读台账游标（offsets endpoint 的 JSON 形状）。
fn ledger_kafka_offsets(reg: &Arc<PipelineRegistry>, id: u64) -> Option<BTreeMap<String, i64>> {
    let v = reg.offsets(id).ok()?;
    let c = &v["statements"][0]["ledger"]["cursor"]["Kafka"]["offsets"];
    if c.is_null() {
        return None;
    }
    Some(
        c.as_object()?
            .iter()
            .map(|(k, v)| (k.clone(), v.as_i64().unwrap_or(0)))
            .collect(),
    )
}

/// 台账 totals（consumed, flushed, flushes）——registry 计数器的重启续种来源。
fn ledger_totals(reg: &Arc<PipelineRegistry>, id: u64) -> Option<(u64, u64, u64)> {
    let v = reg.offsets(id).ok()?;
    let t = &v["statements"][0]["ledger"]["totals"];
    Some((
        t["consumed"].as_u64()?,
        t["flushed"].as_u64()?,
        t["flushes"].as_u64()?,
    ))
}

/// 从 jsonl 输出文件提取 order_no 集合（真 FileSink 的提交面）。
fn orders_out_keys() -> Vec<String> {
    std::fs::read_to_string("orders_out")
        .expect("orders_out readable")
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let s = l.split("\"order_no\":\"").nth(1).expect("order_no field");
            s.split('"').next().expect("closing quote").to_string()
        })
        .collect()
}

// ==================== T5 验收测试 ====================

/// T5 验收①②：部分分区推进的游标提交 + 重启不丢不重（汇=真 FileSink）。
#[test]
fn t5_partial_partition_commit_and_restart_no_loss_no_dup() -> anyhow::Result<()> {
    let work = tmp_workdir("parts");
    let _cwd = CwdGuard::enter(&work); // FileSink cwd 相对（既有契约测试惯例）
    let state_dir = work.join("state");
    let reg = Arc::new(PipelineRegistry::with_connectors(&state_dir, t5_registry()));
    let topic = format!("t5_parts_{}", std::process::id());
    let stmts = || t5_statements(&topic, "1.local.orders_out");
    let tables = || t5_tables(&topic, "1.local.orders_out", "file");

    // —— run 1：只有分区 0/1 有数据（部分分区推进；p2 无数据，不在游标 map）——
    mockparts_seed(&topic, 0, 0, 3000);
    mockparts_seed(&topic, 1, 0, 2000);
    reg.deploy(5401, 1, stmts(), tables(), Some(1000), None, None, false)?;
    let run1_offsets = poll_until(Duration::from_secs(20), || {
        let m = ledger_kafka_offsets(&reg, 5401)?;
        (m.len() == 2
            && m.get("0").copied() == Some(3000)
            && m.get("1").copied() == Some(2000)
            && !m.contains_key("2"))
        .then_some(m)
    })
    .expect("run 1 commits the whole cursor map at the flush point: {0:3000, 1:2000}, p2 absent");
    assert_eq!(
        run1_offsets.get("2"),
        None,
        "partition with no data must not appear in the cursor map"
    );
    let t1 = ledger_totals(&reg, 5401).expect("run 1 totals committed");
    assert_eq!(
        (t1.0, t1.1),
        (5000, 5000),
        "consumed=flushed=5000 in run 1 (the cursor commits only behind a flush)"
    );
    reg.stop_pipeline(5401);

    // —— run 2（重启）：p0 追加 1000、p2 新出现 800 行、p1 无新数据 ——
    mockparts_seed(&topic, 0, 3000, 1000);
    mockparts_seed(&topic, 2, 0, 800);
    reg.deploy(5401, 2, stmts(), tables(), Some(1000), None, None, false)?;
    poll_until(Duration::from_secs(20), || {
        let m = ledger_kafka_offsets(&reg, 5401)?;
        (m.len() == 3
            && m.get("0").copied() == Some(4000)
            && m.get("1").copied() == Some(2000)
            && m.get("2").copied() == Some(800))
        .then_some(m)
    })
    .expect("restart resumes from the ledger: {0:4000, 1:2000 (frozen), 2:800}");
    let t2 = ledger_totals(&reg, 5401).expect("run 2 totals committed");
    // 台账 totals 跨 deploy 连续（deploy 边界从台账种子）：run1 提交 5000 +
    // run2 新增 1800 = 6800——若 run 2 重放了 run 1 的 5000 行会是 11800
    assert_eq!(
        (t2.0, t2.1),
        (6800, 6800),
        "totals cumulative across the restart: 5000 (seeded) + 1800 new, no replay of run 1"
    );
    assert!(
        t2.0 > t1.0 && t2.1 > t1.1 && t2.2 > t1.2,
        "totals monotonic: run1 {t1:?} → run2 {t2:?}"
    );
    reg.stop_pipeline(5401);

    // —— 不丢不重：真 FileSink 的提交面恰好 5800 行、5800 个唯一键 ——
    let keys = orders_out_keys();
    assert_eq!(
        keys.len(),
        6800,
        "no loss, no dup across the restart (5000 + 1800 rows)"
    );
    let mut uniq = keys;
    uniq.sort();
    uniq.dedup();
    assert_eq!(
        uniq.len(),
        6800,
        "every order_no unique (no replayed duplicates)"
    );
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}

/// T5 验收③a：NO_PROGRESS_LIMIT=3——sink flush 永远失败 → 游标永远不过界 →
/// 3 次无进步重启后 FAILED 终态，且台账零提交（未提交 = 无游标）。
#[test]
fn t5_no_progress_limit_three_failures_terminal() -> anyhow::Result<()> {
    let work = tmp_workdir("noprog");
    let state_dir = work.join("state");
    let reg = Arc::new(PipelineRegistry::with_connectors(&state_dir, t5_registry()));
    let topic = format!("t5_noprog_{}", std::process::id());
    mockparts_seed(&topic, 0, 0, 100);
    mocksink_fail_flush("mockout_noprog", u32::MAX); // 每次 flush 都失败
    let opts = tailrace_catalog::PlanOptions {
        recovery: Some(RecoveryOptions {
            restart_backoff_secs: Some(vec![0]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let stmts = t5_statements(&topic, "1.local.mockout_noprog");
    let tables = t5_tables(&topic, "1.local.mockout_noprog", "mocksink");
    reg.deploy(5402, 1, stmts, tables, Some(1000), Some(opts), None, false)?;
    let failed = poll_until(Duration::from_secs(30), || {
        reg.snapshot()
            .into_iter()
            .find(|(id, _, st, _)| *id == 5402 && st == "FAILED")
            .and_then(|(_, _, _, err)| err)
    })
    .expect("poison sink: FAILED terminal after 3 no-progress restarts");
    assert!(
        failed.contains("mock sink flush failure"),
        "statement error surfaces the failure: {failed}"
    );
    // 判决全文（NO_PROGRESS_LIMIT=3 措辞）在 STATEMENT_FAILED 事件里——
    // snapshot.lastError 只存语句层错误，判决在 hub 事件流（真实消费路径）
    let verdict = poll_until(Duration::from_secs(5), || {
        let (hub, _) = reg.log_hub().tail(5402, 0, 100);
        hub.iter()
            .find(|e| e.kind == "STATEMENT_FAILED")
            .map(|e| e.message.clone())
    })
    .expect("STATEMENT_FAILED hub entry present");
    assert!(
        verdict.contains("3 consecutive failures without cursor progress"),
        "verdict names the NO_PROGRESS_LIMIT=3 criterion: {verdict}"
    );
    // 台账零提交：flush 失败在 save 之前 → 无游标（重启重放窗口）
    assert!(
        ledger_kafka_offsets(&reg, 5402).is_none(),
        "no cursor committed under a failing flush"
    );
    assert_eq!(
        mocksink_committed_rows("mockout_noprog"),
        0,
        "nothing committed on the sink side"
    );
    reg.stop_pipeline(5402);
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}

/// T5 验收③b：有进步就复位——首次 flush 失败、重启后重放并成功 → 游标过界
/// → 计数复位，管道保持 RUNNING（不误杀），行恰好一次提交。
#[test]
fn t5_progress_resets_no_progress_counter() -> anyhow::Result<()> {
    let work = tmp_workdir("prog");
    let state_dir = work.join("state");
    let reg = Arc::new(PipelineRegistry::with_connectors(&state_dir, t5_registry()));
    let topic = format!("t5_prog_{}", std::process::id());
    mockparts_seed(&topic, 0, 0, 100);
    mocksink_fail_flush("mockout_prog", 1); // 只毒第一次 flush
    let opts = tailrace_catalog::PlanOptions {
        recovery: Some(RecoveryOptions {
            restart_backoff_secs: Some(vec![0]),
            ..Default::default()
        }),
        ..Default::default()
    };
    let stmts = t5_statements(&topic, "1.local.mockout_prog");
    let tables = t5_tables(&topic, "1.local.mockout_prog", "mocksink");
    reg.deploy(5403, 1, stmts, tables, Some(1000), Some(opts), None, false)?;
    // 重启后重放 100 行并成功 flush → 游标提交 → RUNNING（非 FAILED）
    poll_until(Duration::from_secs(30), || {
        ledger_kafka_offsets(&reg, 5403)
            .filter(|m| m.get("0").copied() == Some(100))
            .map(|_| ())
    })
    .expect("replayed batch flushes and commits the cursor");
    std::thread::sleep(Duration::from_millis(800)); // 若误判毒数据，FAILED 会在此窗口出现
    let st = reg
        .snapshot()
        .into_iter()
        .find(|(id, _, _, _)| *id == 5403)
        .expect("pipeline still registered");
    assert_eq!(st.2, "RUNNING", "progress resets the NO_PROGRESS counter");
    assert_eq!(
        mocksink_committed_rows("mockout_prog"),
        100,
        "exactly-once committed after replay (the failed attempt's pending died with it)"
    );
    let t = ledger_totals(&reg, 5403).expect("totals committed");
    // consumed 计数在 deploy 内跨重启尝试**连续累计**（计数器只在 deploy 边界
    // 从台账种子）：重放的 100 行再计一次 → consumed=200 是当前语义；flushed
    // 只在成功 flush 点入账 → 恰好 100（恰好一次提交）。
    assert_eq!(
        t.1, 100,
        "flushed totals count committed rows only (exactly-once on the sink)"
    );
    assert!(t.0 >= 100, "consumed totals include the replayed batch");
    reg.stop_pipeline(5403);
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}
