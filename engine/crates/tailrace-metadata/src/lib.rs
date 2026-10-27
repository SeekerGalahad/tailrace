//! tailrace-metadata: the engine-side metadata clients behind console
//! discover / infer-schema (roadmap T14, console API §4.2 v1 minimal set).
//!
//! Division of labor: this crate owns the *protocol* work — listing what a
//! datasource holds and turning samples / DDL into a column list — while the
//! console owns request validation, credential unsealing, caching and the
//! REST surface (same embed pattern as the console's local tailrace-parser
//! compile). Nothing here persists anything.
//!
//! Column types use the engine's established vocabulary — `"int64"` /
//! `"float64"` / `"bool"` / `"utf8"` — which maps 1:1 to Arrow via
//! `tailrace-runtime`'s `arrow_type` (anything unrecognized folds to Utf8),
//! and is exactly the `table_object.columns_json` wire shape.
//!
//! Kafka naming convention: a topic `a.b` maps to the two-part table-object
//! reference (db `a`, table `b`, split at the **first** dot); a dot-less
//! topic keeps an empty db (`""`). `db.table` joins back losslessly, which
//! is what infer-schema relies on to find the topic again.
//!
//! Sync stance: kafka operations block in librdkafka and are bridged through
//! `spawn_blocking`; the sqlx backends are natively async.

mod infer;
mod kafka;
mod sql;

use std::time::Duration;

/// Upper bound for one backend call. Kafka's own fetch/poll deadlines are
/// tighter; this wraps the async sql paths (an unreachable host must not
/// hang a console request).
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Connection spec the console derives from a datasource row (type +
/// connJson + unsealed credential halves). Built by the caller; this crate
/// never reads secrets itself.
#[derive(Debug, Clone)]
pub enum ConnSpec {
    Kafka {
        bootstrap: String,
    },
    /// Anything speaking the MySQL wire protocol — Doris FE (9030) or a
    /// MySQL server. SHOW DATABASES / SHOW TABLES / DESC over the **text**
    /// protocol (rdkafka-style lowest common denominator: no COM_STMT_PREPARE,
    /// which Doris FE support varies on).
    MySql {
        host: String,
        port: u16,
        database: Option<String>,
        username: String,
        password: String,
    },
    Postgres {
        host: String,
        port: u16,
        database: Option<String>,
        username: String,
        password: String,
    },
}

/// One discovered asset (Kafka topic or SQL table). `partitions` is filled
/// for Kafka only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredAsset {
    /// Logical db ("" for a dot-less Kafka topic).
    pub db_name: String,
    pub table_name: String,
    /// Kafka topic partition count; SQL backends leave `None`.
    pub partitions: Option<i32>,
}

/// Inference output: `(column, arrow-type-name)` pairs in declaration /
/// first-seen order, plus the decode `format` the samples suggest (Kafka
/// only; `None` = not applicable / leave as-is).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferredSchema {
    pub columns: Vec<(String, String)>,
    pub format: Option<String>,
}

/// List the assets a datasource holds (§4.2 discover). `None` db in a MySql
/// spec lists every non-system database; a configured db restricts to it.
pub async fn discover(spec: &ConnSpec) -> anyhow::Result<Vec<DiscoveredAsset>> {
    match spec {
        ConnSpec::Kafka { bootstrap } => {
            let bootstrap = bootstrap.to_string();
            let topics = tokio::task::spawn_blocking(move || kafka::list_topics(&bootstrap))
                .await
                .map_err(|e| anyhow::anyhow!("kafka listing task failed: {e}"))??;
            Ok(topics
                .into_iter()
                .map(|t| {
                    let (db, table) = split_topic(&t.name);
                    DiscoveredAsset {
                        db_name: db,
                        table_name: table,
                        partitions: Some(t.partitions),
                    }
                })
                .collect())
        }
        ConnSpec::MySql { .. } => tokio::time::timeout(CALL_TIMEOUT, sql::mysql_list_tables(spec))
            .await
            .map_err(|_| anyhow::anyhow!("mysql 元数据查询超时 (>{CALL_TIMEOUT:?})"))?
            .map(|pairs| {
                pairs
                    .into_iter()
                    .map(|(db, table)| DiscoveredAsset {
                        db_name: db,
                        table_name: table,
                        partitions: None,
                    })
                    .collect()
            }),
        ConnSpec::Postgres { .. } => tokio::time::timeout(CALL_TIMEOUT, sql::pg_list_tables(spec))
            .await
            .map_err(|_| anyhow::anyhow!("postgres 元数据查询超时 (>{CALL_TIMEOUT:?})"))?
            .map(|pairs| {
                pairs
                    .into_iter()
                    .map(|(db, table)| DiscoveredAsset {
                        db_name: db,
                        table_name: table,
                        partitions: None,
                    })
                    .collect()
            }),
    }
}

/// Infer a schema (§4.2 infer-schema — nothing persisted).
///
/// - Kafka: samples up to `sample_rows` tail messages and infers columns
///   with the same typed chain the JsonDecoder uses (str → utf8, i64 →
///   int64, f64 → float64, bool → bool; mixed columns widen — int+float →
///   float64, anything+str → utf8). `format` selects the walk (`ogg` =
///   envelope + `after.*`, `json`/`jsonl` = root keys) or is detected from
///   the first sample when absent.
/// - MySql / Postgres: DDL column types mapped through the same arrow
///   vocabulary (`DESC` / `information_schema.columns` respectively);
///   `format` is ignored and returned as `None`.
pub async fn infer_schema(
    spec: &ConnSpec,
    db: &str,
    table: &str,
    format: Option<&str>,
    sample_rows: usize,
) -> anyhow::Result<InferredSchema> {
    match spec {
        ConnSpec::Kafka { bootstrap } => {
            let topic = join_topic(db, table);
            match format {
                Some("csv") | Some("parquet") => anyhow::bail!(
                    "kafka 样本推断暂不支持 {format:?} 格式（无自描述样本），请手工登记列"
                ),
                Some("ogg") | Some("json") | Some("jsonl") | Some("debezium-json") | None => {}
                Some(other) => {
                    anyhow::bail!("未知 decode 格式 {other:?}（ogg/json/jsonl/debezium-json）")
                }
            }
            let bootstrap = bootstrap.to_string();
            let topic2 = topic.clone();
            let samples = tokio::task::spawn_blocking(move || {
                kafka::sample_tail(&bootstrap, &topic2, sample_rows.max(1))
            })
            .await
            .map_err(|e| anyhow::anyhow!("kafka 采样任务失败: {e}"))??;
            if samples.is_empty() {
                anyhow::bail!("主题 {topic:?} 无样本消息，无法推断 schema");
            }
            infer::infer_from_json_samples(&samples, format)
        }
        ConnSpec::MySql { .. } => {
            let cols = tokio::time::timeout(CALL_TIMEOUT, sql::mysql_describe(spec, db, table))
                .await
                .map_err(|_| anyhow::anyhow!("mysql DESC 超时 (>{CALL_TIMEOUT:?})"))??;
            Ok(InferredSchema {
                columns: cols
                    .into_iter()
                    .map(|(name, ty)| (name, infer::ddl_arrow_type(&ty).to_string()))
                    .collect(),
                format: None,
            })
        }
        ConnSpec::Postgres { .. } => {
            let cols = tokio::time::timeout(CALL_TIMEOUT, sql::pg_describe(spec, db, table))
                .await
                .map_err(|_| anyhow::anyhow!("postgres 列查询超时 (>{CALL_TIMEOUT:?})"))??;
            Ok(InferredSchema {
                columns: cols
                    .into_iter()
                    .map(|(name, ty)| (name, infer::pg_arrow_type(&ty).to_string()))
                    .collect(),
                format: None,
            })
        }
    }
}

/// `"a.b"` → (`"a"`, `"b"`); `"flat"` → (`""`, `"flat"`) (first-dot split;
/// the reference form `${ds:id.db.table}` is three-part, so the remainder
/// stays one identifier however many dots the topic carries).
pub(crate) fn split_topic(topic: &str) -> (String, String) {
    match topic.split_once('.') {
        Some((db, table)) => (db.to_string(), table.to_string()),
        None => (String::new(), topic.to_string()),
    }
}

/// Inverse of [`split_topic`].
pub(crate) fn join_topic(db: &str, table: &str) -> String {
    if db.is_empty() {
        table.to_string()
    } else {
        format!("{db}.{table}")
    }
}
