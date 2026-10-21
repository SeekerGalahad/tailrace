//! T15 分区并行·方案 b 的语义回归测试（**动工前先写**，钉住动游标边界前的
//! 现语义——与 roadmap §九 T15 验收对应）：
//!
//! ① 新增分区**不重启管道**即纳入 poll 面：单次 deploy 内，运行中出现的新分区
//!    数据被消费，flush 点整体提交的游标 map 长出新分区项（台账可查）；
//! ② 跨发现边界的重启不丢不重：发现后提交的游标（含新分区项）重启后被原样
//!    恢复，仅新增数据流入——真 FileSink 提交面唯一键恰好等于总行数；
//! ③ totals 跨「发现」与「重启」单调续涨（无重放）；
//! ④ 毒汇下运行中长分区：sink flush 永远失败时，新分区出现**不得**扰动
//!    NO_PROGRESS 判据——游标指纹只随台账（flush 点提交）移动，未提交的
//!    发现/消费不产生「进步」，3 次无进步重启后照常 FAILED 终态（发现项
//!    不会让毒数据管道「假装有进展」而逃过判决），台账游标冻结在毒前提交。
//!
//! 运行时侧不区分旧分区与新发现分区：源的 `cursor()` 返回整个 offsets map，
//! 注册表在 flush 点整体提交。mock 多分区源的记账与 [`tailrace_conn_kafka`]
//! 的 KafkaSource 同构（poll 返回即推进当轮有数据分区的 next offset；新分区
//! 出现即被纳入迭代面）。KafkaSource 真实的元数据发现路径（30s 差异对比 +
//! assign 模式 assignment 刷新）由 conn-kafka 的 testcontainers 测试钉住
//! （`kafka_source_discovers_new_partitions_without_reopen`），此处钉住的
//! 是 runtime/台账/NO_PROGRESS 判据侧的语义。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use datafusion::arrow::record_batch::RecordBatch;
use tailrace_catalog::{PlanTableObject, RecoveryOptions, StatementSpec, TableObject};
use tailrace_connector::{ConnectorRegistry, Sink, SinkAck, Source, SourceChunk};
use tailrace_ir::Cursor;
use tailrace_runtime::registry::PipelineRegistry;

// ==================== 共用夹具（与 t5_partition_semantics 同构） ====================

fn tmp_workdir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "tailrace-t15-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).expect("create workdir");
    d
}

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

// ==================== mock 多分区源（可在运行中长出新分区） ====================

fn mock_store() -> &'static Mutex<MockStore> {
    static S: OnceLock<Mutex<MockStore>> = OnceLock::new();
    S.get_or_init(|| Mutex::new(MockStore::default()))
}

#[derive(Default)]
struct MockStore {
    /// topic → partition → rows（分区数可随测试进程增长 = 动态分区发现的上游事实）
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
        parts.push(Vec::new()); // 新槽位 = 新发现的分区（首条数据前不可见）
    }
    for i in from..from + rows {
        parts[partition].push(
            format!("{{\"order_no\":\"{topic}-p{partition}-{i:06}\",\"qty\":{i}}}").into_bytes(),
        );
    }
}

/// 与 t5 的 MockPartsSource 同构：poll 跨分区 round-robin，游标记账 =
/// KafkaSource 语义（poll 返回即推进当轮有数据分区；新分区一旦有数据即加入
/// offsets map）。
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
            group: "mockparts15".into(),
            offsets: self.pos.clone(),
        }
    }
}

/// 让该 sink 的接下来 n 次 flush 失败（毒 sink 注入；u32::MAX = 永远失败）。
fn mocksink_fail_flush(name: &str, n: u32) {
    mock_store()
        .lock()
        .unwrap()
        .sink_fail_flush
        .insert(name.to_string(), n);
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

/// 计数汇（毒数据判据测试专用，与 t5 的 MockCountingSink 同构）：write 只
/// 累计行数，flush 才提交；可注入 n 次 flush 失败（失败尝试的 pending 随
/// 实例消亡——与真实 sink 的 flush 点提交语义同构）。
struct MockCountingSink {
    name: String,
    pending: u64,
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
        Ok(SinkAck {
            rows,
            kind: "MOCK_FLUSH",
            idempotency_key: "mock.0".into(),
        })
    }
}

fn t15_registry() -> ConnectorRegistry {
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
        }))
    }
    let mut reg = tailrace_runtime::default_registry();
    reg.register_source("mockparts", src_factory);
    reg.register_sink("mocksink", sink_factory);
    reg
}

fn t15_statements(topic: &str, sink_ref: &str) -> Vec<StatementSpec> {
    vec![StatementSpec {
        source: format!("1.kafka.{topic}"),
        group: "tr.p5501.s0".into(),
        sink: sink_ref.into(),
        pushdown: vec![],
        select_sql: "SELECT order_no, qty FROM src".into(),
        start_offset: None,
    }]
}

fn t15_tables(topic: &str, sink_ref: &str, sink_kind: &str) -> Vec<PlanTableObject> {
    vec![
        PlanTableObject {
            reference: format!("1.kafka.{topic}"),
            kind: "mockparts".into(),
            format: Some("json".into()),
            columns: vec![
                ("order_no".to_string(), "utf8".to_string()),
                ("qty".to_string(), "int64".to_string()),
            ],
            bootstrap: None,
            stream_load_url: None,
            auth_env: None,
            sequence_column: None,
        },
        PlanTableObject {
            reference: sink_ref.into(),
            kind: sink_kind.into(),
            format: Some("jsonl".into()),
            columns: vec![
                ("order_no".to_string(), "utf8".to_string()),
                ("qty".to_string(), "int64".to_string()),
            ],
            bootstrap: None,
            stream_load_url: None,
            auth_env: None,
            sequence_column: None,
        },
    ]
}

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

fn ledger_totals(reg: &Arc<PipelineRegistry>, id: u64) -> Option<(u64, u64, u64)> {
    let v = reg.offsets(id).ok()?;
    let t = &v["statements"][0]["ledger"]["totals"];
    Some((
        t["consumed"].as_u64()?,
        t["flushed"].as_u64()?,
        t["flushes"].as_u64()?,
    ))
}

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

/// T15 验收①②③：分区增长发生在**运行中**（同一 deploy 内）——新分区数据进入
/// poll 面、游标 map 长出新项且 flush 点整体提交；随后重启，游标从台账恢复
/// （含发现项），不丢不重，totals 全程单调。
#[test]
fn t15_new_partitions_join_mid_run_without_restart() -> anyhow::Result<()> {
    let work = tmp_workdir("disc");
    let _cwd = CwdGuard::enter(&work); // FileSink cwd 相对（既有契约测试惯例）
    let state_dir = work.join("state");
    let reg = Arc::new(PipelineRegistry::with_connectors(
        &state_dir,
        t15_registry(),
    ));
    let topic = format!("t15_disc_{}", std::process::id());

    // —— run 1：两分区起步（p0/p1）——
    mockparts_seed(&topic, 0, 0, 3000);
    mockparts_seed(&topic, 1, 0, 2000);
    reg.deploy(
        5501,
        1,
        t15_statements(&topic, "1.local.orders_out"),
        t15_tables(&topic, "1.local.orders_out", "file"),
        Some(1000),
        None,
        None,
        false,
    )?;
    poll_until(Duration::from_secs(20), || {
        let m = ledger_kafka_offsets(&reg, 5501)?;
        (m.len() == 2 && m.get("0").copied() == Some(3000) && m.get("1").copied() == Some(2000))
            .then_some(m)
    })
    .expect("two-partition baseline commits {0:3000, 1:2000}");
    let t1 = ledger_totals(&reg, 5501).expect("run 1 totals committed");

    // —— 分区增长发生在运行中（不 stop、不重新 deploy）：p2 新出现 + p0 追加 ——
    mockparts_seed(&topic, 2, 0, 800); // 新分区
    mockparts_seed(&topic, 0, 3000, 1000); // 旧分区追加
    poll_until(Duration::from_secs(20), || {
        let m = ledger_kafka_offsets(&reg, 5501)?;
        (m.len() == 3
            && m.get("0").copied() == Some(4000)
            && m.get("1").copied() == Some(2000)
            && m.get("2").copied() == Some(800))
        .then_some(m)
    })
    .expect("mid-run partition growth joins the poll face: {0:4000, 1:2000, 2:800} — same deploy");
    // 不重启的证明：注册表自 deploy 以来零 PIPELINE_RESTART 事件，状态仍 RUNNING
    let (hub, _) = reg.log_hub().tail(5501, 0, 200);
    assert!(
        hub.iter().all(|e| e.kind != "PIPELINE_RESTART"),
        "no restart happened during the partition growth (kinds: {:?})",
        hub.iter().map(|e| e.kind.as_str()).collect::<Vec<_>>()
    );
    let st = reg
        .snapshot()
        .into_iter()
        .find(|(id, _, _, _)| *id == 5501)
        .expect("pipeline registered");
    assert_eq!(st.2, "RUNNING", "still the same live pipeline");
    let t2 = ledger_totals(&reg, 5501).expect("post-growth totals committed");
    assert_eq!(
        (t2.0, t2.1),
        (6800, 6800),
        "totals after mid-run growth: 5000 + 1800 (p0 1000 + p2 800)"
    );
    assert!(
        t2.0 > t1.0 && t2.1 > t1.1 && t2.2 > t1.2,
        "totals monotonic across the growth: {t1:?} → {t2:?}"
    );
    reg.stop_pipeline(5501);

    // —— run 2（重启）：只有「发现的」p2 有新数据，p0/p1 冻结 ——
    mockparts_seed(&topic, 2, 800, 500);
    reg.deploy(
        5501,
        2,
        t15_statements(&topic, "1.local.orders_out"),
        t15_tables(&topic, "1.local.orders_out", "file"),
        Some(1000),
        None,
        None,
        false,
    )?;
    poll_until(Duration::from_secs(20), || {
        let m = ledger_kafka_offsets(&reg, 5501)?;
        (m.len() == 3
            && m.get("0").copied() == Some(4000)
            && m.get("1").copied() == Some(2000)
            && m.get("2").copied() == Some(1300))
        .then_some(m)
    })
    .expect("restart resumes the discovered partition from the ledger: {0:4000, 1:2000, 2:1300}");
    let t3 = ledger_totals(&reg, 5501).expect("run 2 totals committed");
    assert_eq!(
        (t3.0, t3.1),
        (7300, 7300),
        "totals cumulative across the discovery-restart boundary (6800 + 500), no replay"
    );
    assert!(
        t3.0 > t2.0 && t3.1 > t2.1 && t3.2 > t2.2,
        "totals monotonic across the restart: {t2:?} → {t3:?}"
    );
    reg.stop_pipeline(5501);

    // —— 不丢不重：真 FileSink 提交面 7300 行、7300 个唯一键 ——
    let keys = orders_out_keys();
    assert_eq!(
        keys.len(),
        7300,
        "no loss, no dup across mid-run growth + restart (5000 + 1800 + 500)"
    );
    let mut uniq = keys;
    uniq.sort();
    uniq.dedup();
    assert_eq!(uniq.len(), 7300, "every order_no unique");
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}

/// T15 验收④：毒汇下**运行中长分区**不得扰动 NO_PROGRESS 判据——游标指纹
/// 只随台账（flush 点整体提交）移动：未提交的发现/重放消费不产生「进步」，
/// 3 次无进步重启后照常 FAILED 终态；台账游标冻结在毒化前最后一次提交
/// （新分区 p2 的数据永不入账），sink 侧零提交。若发现项被误计为进步
/// （例如把「map 长出新项」当 flush 点提交），本测试会以 FAILED 不出现
/// 或台账游标含 p2 的形式失败。
#[test]
fn t15_growth_under_poison_sink_keeps_no_progress_verdict() -> anyhow::Result<()> {
    let work = tmp_workdir("poison");
    let state_dir = work.join("state");
    let reg = Arc::new(PipelineRegistry::with_connectors(
        &state_dir,
        t15_registry(),
    ));
    let topic = format!("t15_poison_{}", std::process::id());

    // —— 阶段 1（健康）：两分区数据提交，台账游标 {0,1} ——
    mockparts_seed(&topic, 0, 0, 300);
    mockparts_seed(&topic, 1, 0, 200);
    let opts = tailrace_catalog::PlanOptions {
        recovery: Some(RecoveryOptions {
            restart_backoff_secs: Some(vec![0]),
            ..Default::default()
        }),
        ..Default::default()
    };
    reg.deploy(
        5504,
        1,
        t15_statements(&topic, "1.local.mockout_t15poison"),
        t15_tables(&topic, "1.local.mockout_t15poison", "mocksink"),
        Some(1000),
        Some(opts),
        None,
        false,
    )?;
    poll_until(Duration::from_secs(20), || {
        let m = ledger_kafka_offsets(&reg, 5504)?;
        (m.len() == 2 && m.get("0").copied() == Some(300) && m.get("1").copied() == Some(200))
            .then_some(m)
    })
    .expect("healthy phase commits {0:300, 1:200}");
    assert_eq!(
        mocksink_committed_rows("mockout_t15poison"),
        500,
        "phase 1 committed exactly the two-partition data"
    );

    // —— 阶段 2（毒化 + 运行中长分区）：flush 永远失败，同时 p2 新出现——
    mocksink_fail_flush("mockout_t15poison", u32::MAX);
    mockparts_seed(&topic, 2, 0, 100); // 新分区数据（未提交，永不入账）
    mockparts_seed(&topic, 0, 300, 50); // 旧分区追加（同样未提交）
    let failed = poll_until(Duration::from_secs(30), || {
        reg.snapshot()
            .into_iter()
            .find(|(id, _, st, _)| *id == 5504 && st == "FAILED")
            .and_then(|(_, _, _, err)| err)
    })
    .expect("poison sink after mid-run growth: FAILED terminal after 3 no-progress restarts");
    assert!(
        failed.contains("mock sink flush failure"),
        "statement error surfaces the failure: {failed}"
    );
    let verdict = poll_until(Duration::from_secs(5), || {
        let (hub, _) = reg.log_hub().tail(5504, 0, 100);
        hub.iter()
            .find(|e| e.kind == "STATEMENT_FAILED")
            .map(|e| e.message.clone())
    })
    .expect("STATEMENT_FAILED hub entry present");
    assert!(
        verdict.contains("3 consecutive failures without cursor progress"),
        "mid-run partition growth must NOT reset the poison verdict: {verdict}"
    );
    // 台账冻结在毒化前提交：p2 从未入账（无 flush 点整体提交，无发现项逃逸）
    let frozen = ledger_kafka_offsets(&reg, 5504).expect("ledger holds the last commit");
    assert_eq!(
        (
            frozen.get("0").copied(),
            frozen.get("1").copied(),
            frozen.get("2").copied()
        ),
        (Some(300), Some(200), None),
        "cursor frozen at the last healthy commit — uncommitted growth never enters the ledger"
    );
    assert_eq!(
        mocksink_committed_rows("mockout_t15poison"),
        500,
        "nothing more committed on the sink side after poisoning"
    );
    reg.stop_pipeline(5504);
    let _ = std::fs::remove_dir_all(&work);
    Ok(())
}
