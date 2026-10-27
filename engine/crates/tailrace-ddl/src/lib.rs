//! tailrace-ddl: pure CREATE TABLE draft generator for Doris / StarRocks
//! (roadmap T-A1, blueprint §7.2).
//!
//! Division of labor: this crate only *composes text* — schema plus key
//! hints in, draft DDL plus annotation metadata out. No IO, no tokio, no
//! cluster probing: the console owns inference, BE-count probing and the
//! human-confirm step, then feeds the confirmed choices back in as hints.
//! Purity is what makes the snapshot tests (given schema → fixed DDL text)
//! meaningful — the whole §7.2 rule table lives here and nowhere else.
//!
//! Vocabulary boundary (until G2): columns carry the engine's established
//! four-type vocabulary — `utf8` / `int64` / `float64` / `bool` — mapped per
//! blueprint §7.1's retained rows. Anything else is an explicit error, not a
//! silent fold: a draft built on a folded type would lie about the table
//! (G2's hard-error direction, applied early to the one place that emits
//! DDL). For the same reason the partition heuristic is **name**-based —
//! the current vocabulary has no timestamp/date type to key on, so
//! time-shaped *names* stand in until G2 lands real types (then detection
//! switches to the column type; see [`partition_candidate`]).
//!
//! Every guess is annotated, never silent (§7.2 ③): key model, key column
//! and partition column each carry a recommendation reason or a 「猜测」
//! flag in [`DraftDdl`] — exactly what the console wizard highlights in its
//! confirm step. Nothing here executes anything; `tailrace-metadata`'s
//! `mysql_exec` is the exec side of the closed loop.

use anyhow::bail;

/// One column: `(name, vocabulary type)` — the exact `columns_json` wire
/// shape (`InferredSchema::columns` / console list-tables rows), so the
/// console passes inference output through untouched.
pub type Column = (String, String);

/// Target table identity plus columns in declaration order.
#[derive(Debug, Clone)]
pub struct Schema {
    pub db: String,
    pub table: String,
    /// `(column, vocabulary type)` pairs, declaration order as inferred /
    /// hand-registered. The generated DDL moves the key column to the front
    /// (Doris requires key columns to lead the column list); relative order
    /// of the rest is preserved.
    pub columns: Vec<Column>,
}

/// Doris key models the draft chooses between (§7.2 ②).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyModel {
    /// CDC stream (sequence/op_type evidence) — dedup on the key.
    Unique,
    /// Plain append, no dedup semantics.
    Duplicate,
}

impl KeyModel {
    /// The DDL keyword as it appears in the key clause (`UNIQUE KEY` /
    /// `DUPLICATE KEY`).
    pub fn sql(self) -> &'static str {
        match self {
            KeyModel::Unique => "UNIQUE KEY",
            KeyModel::Duplicate => "DUPLICATE KEY",
        }
    }

    /// Stable wire name for console payloads.
    pub fn name(self) -> &'static str {
        match self {
            KeyModel::Unique => "unique",
            KeyModel::Duplicate => "duplicate",
        }
    }
}

/// Caller-side hints: what the console knows beyond the raw schema — the
/// declared `sequence_column`, the human-confirmed choices fed back on
/// regenerate, and the probed replication setting.
#[derive(Debug, Clone, Default)]
pub struct KeyHints {
    /// Declared sequence column (CDC ordering). Its presence — like an
    /// `op_type` column in the schema — recommends [`KeyModel::Unique`].
    /// Must name an existing column (fail-fast, not silently dropped).
    pub sequence_column: Option<String>,
    /// Key column the human confirmed in the wizard. `None` → the id/`*_id`
    /// heuristic guesses and the draft flags it as such.
    pub key_column: Option<String>,
    /// Key-model override: what the human actually picked (may differ from
    /// the recommendation). `None` → the recommendation stands.
    pub key_model: Option<KeyModel>,
    /// Probed BE count for `"replication_num"`. `None` = unknown → omit the
    /// property (cluster default); `Some(1)` is the single-node downgrade
    /// without which CREATE TABLE fails on a 1-BE cluster.
    pub replication_num: Option<u32>,
}

/// Day-granularity RANGE dynamic-partition candidate (§7.2 ②: 时序数据标准
/// 做法) — offered when a time-shaped column is present, confirmed or
/// dropped by the human.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionCandidate {
    /// Column the RANGE partition rides on.
    pub column: String,
    /// Name-based guess — always `true` in this wave; goes `false` for
    /// genuinely-typed columns once G2 lands.
    pub guessed: bool,
}

/// Generator output: the exact DDL text plus every annotation the wizard
/// needs (§7.2 ③ — full text, key-column highlight, 「猜测」 flags).
#[derive(Debug, Clone)]
pub struct DraftDdl {
    /// Full `CREATE TABLE` statement — the text to preview and, after
    /// confirmation, to execute verbatim and audit.
    pub ddl: String,
    /// Key model the DDL encodes (the human's override if one was given,
    /// otherwise the recommendation).
    pub key_model: KeyModel,
    /// Why this model was recommended — shown next to the choice.
    pub key_model_reason: String,
    /// The key column the key clause / hash distribution ride on; the
    /// wizard highlights it.
    pub key_column: String,
    /// `true` = heuristic guess (no confirmed key column in the hints) —
    /// the 「这是猜测」 badge.
    pub key_column_guessed: bool,
    /// Partition candidate, when a time-shaped column was found.
    pub partition: Option<PartitionCandidate>,
    /// Human-facing caveats that do not change the DDL text (e.g. a declared
    /// `sequence_column` kept as a plain column under DUPLICATE KEY).
    pub notes: Vec<String>,
}

/// Generate a CREATE TABLE draft (§7.2 steps ①+② — pure; execution is step
/// ④ and lives elsewhere). Errors on anything that would make the draft a
/// lie: empty/invalid identifiers, duplicate columns, types outside the
/// four-type vocabulary, hints naming columns that do not exist.
pub fn generate_draft(schema: &Schema, hints: &KeyHints) -> anyhow::Result<DraftDdl> {
    if schema.db.trim().is_empty() {
        bail!("db 名为空，无法生成 DDL（无点 Kafka 主题需先指定目标库）");
    }
    if schema.table.trim().is_empty() {
        bail!("表名为空，无法生成 DDL");
    }
    if schema.columns.is_empty() {
        bail!("列清单为空，无法生成 DDL");
    }
    let mut seen = std::collections::HashSet::new();
    for (name, ty) in &schema.columns {
        if !seen.insert(name.as_str()) {
            bail!("列名重复：{name}");
        }
        if doris_type(ty).is_none() {
            bail!(
                "列 {name:?} 的类型 {ty:?} 不在受支持词汇内（utf8/int64/float64/bool）——\
                 G2 扩表前不静默折叠未知类型"
            );
        }
    }
    if let Some(seq) = &hints.sequence_column {
        if !schema.columns.iter().any(|(n, _)| n == seq) {
            bail!("sequence_column {seq:?} 不在列清单中");
        }
    }

    // Key model: CDC evidence (declared sequence column or an op_type
    // column) recommends UNIQUE — the dedup story; otherwise DUPLICATE.
    let cdc = hints.sequence_column.is_some() || has_op_type(&schema.columns);
    let recommended = if cdc {
        KeyModel::Unique
    } else {
        KeyModel::Duplicate
    };
    let key_model = hints.key_model.unwrap_or(recommended);
    let key_model_reason = if cdc {
        "检测到 CDC 证据（sequence_column 或 op_type 列），推荐 UNIQUE KEY 去重".to_string()
    } else {
        "未检测到 CDC 证据，推荐 DUPLICATE KEY（追加写）".to_string()
    };

    // Key column: confirmed choice wins; otherwise the naming heuristic
    // (which never consults sequence_column — that column orders updates,
    // it does not identify the row).
    let (key_column, key_column_guessed) = match &hints.key_column {
        Some(explicit) => {
            if !schema.columns.iter().any(|(n, _)| n == explicit) {
                bail!("指定的键列 {explicit:?} 不在列清单中");
            }
            (explicit.clone(), false)
        }
        None => (guess_key_column(&schema.columns).to_string(), true),
    };

    let partition = partition_candidate(&schema.columns);

    let mut notes = Vec::new();
    if key_model == KeyModel::Duplicate {
        if let Some(seq) = &hints.sequence_column {
            notes.push(format!(
                "sequence_column `{seq}` 仅作为普通列保留，DUPLICATE KEY 模式下无去重语义"
            ));
        }
    }

    // Doris requires key columns to lead the column list: key column first,
    // the rest in declaration order.
    let mut ordered: Vec<&Column> = Vec::with_capacity(schema.columns.len());
    ordered.extend(schema.columns.iter().filter(|(n, _)| n == &key_column));
    ordered.extend(schema.columns.iter().filter(|(n, _)| n != &key_column));

    let mut defs: Vec<String> = Vec::with_capacity(ordered.len());
    for (n, t) in &ordered {
        let ty = doris_type(t).expect("type validated in the pass above");
        defs.push(format!("    `{}` {}", quote_ident(n)?, ty));
    }
    let mut ddl = format!(
        "CREATE TABLE IF NOT EXISTS `{}`.`{}` (\n",
        quote_ident(&schema.db)?,
        quote_ident(&schema.table)?
    );
    ddl.push_str(&defs.join(",\n"));
    ddl.push_str("\n)");
    ddl.push_str(&format!(
        "\n{}(`{}`)",
        key_model.sql(),
        quote_ident(&key_column)?
    ));
    if let Some(p) = &partition {
        ddl.push_str(&format!(
            "\nPARTITION BY RANGE(`{}`) ()",
            quote_ident(&p.column)?
        ));
    }
    ddl.push_str(&format!(
        "\nDISTRIBUTED BY HASH(`{}`) BUCKETS AUTO",
        quote_ident(&key_column)?
    ));

    // Fixed property order keeps snapshots stable: MoW → dynamic partition →
    // replication.
    let mut props: Vec<String> = Vec::new();
    if key_model == KeyModel::Unique {
        props.push(r#""enable_unique_key_merge_on_write" = "true""#.to_string());
    }
    if partition.is_some() {
        props.extend(
            [
                r#""dynamic_partition.enable" = "true""#,
                r#""dynamic_partition.time_unit" = "DAY""#,
                r#""dynamic_partition.start" = "-30""#,
                r#""dynamic_partition.end" = "3""#,
                r#""dynamic_partition.prefix" = "p""#,
            ]
            .map(String::from),
        );
    }
    if let Some(n) = hints.replication_num {
        props.push(format!(r#""replication_num" = "{n}""#));
    }
    if !props.is_empty() {
        ddl.push_str("\nPROPERTIES (\n    ");
        ddl.push_str(&props.join(",\n    "));
        ddl.push_str("\n)");
    }
    ddl.push(';');

    Ok(DraftDdl {
        ddl,
        key_model,
        key_model_reason,
        key_column,
        key_column_guessed,
        partition,
        notes,
    })
}

/// Four-type vocabulary → Doris column types (§7.1's retained rows; G2 will
/// grow this table, until then unknown is `None` → explicit error).
fn doris_type(vocab: &str) -> Option<&'static str> {
    match vocab {
        "utf8" => Some("VARCHAR(65533)"),
        "int64" => Some("BIGINT"),
        "float64" => Some("DOUBLE"),
        "bool" => Some("BOOLEAN"),
        _ => None,
    }
}

/// The OGG envelope's CDC marker column (exact name, case-insensitive).
fn has_op_type(columns: &[Column]) -> bool {
    columns
        .iter()
        .any(|(n, _)| n.eq_ignore_ascii_case("op_type"))
}

/// Key-column heuristic (§7.2 ②): first column named `id` / `*_id` in
/// declaration order, else the first column. A guess by definition — the
/// caller flags it.
fn guess_key_column(columns: &[Column]) -> &str {
    for (name, _) in columns {
        let n = name.to_ascii_lowercase();
        if n == "id" || n.ends_with("_id") {
            return name.as_str();
        }
    }
    &columns[0].0
}

/// Partition heuristic: the first column whose name reads as a point in
/// time. Name-based only because the four-type vocabulary cannot carry a
/// timestamp/date type (such columns fold to utf8 upstream) — G2 replaces
/// the name match with a type match and the guess flag with evidence.
fn partition_candidate(columns: &[Column]) -> Option<PartitionCandidate> {
    const EXACT: [&str; 6] = ["ts", "time", "timestamp", "date", "dt", "day"];
    const SUFFIX: [&str; 5] = ["_ts", "_time", "_timestamp", "_date", "_at"];
    columns
        .iter()
        .find(|(n, _)| {
            let n = n.to_ascii_lowercase();
            EXACT.contains(&n.as_str())
                || SUFFIX.iter().any(|s| n.len() > s.len() && n.ends_with(s))
        })
        .map(|(n, _)| PartitionCandidate {
            column: n.clone(),
            guessed: true,
        })
}

/// Backtick-quote one identifier: backticks doubled, NUL and empty rejected
/// (same discipline as the metadata crate's SQL assembly).
fn quote_ident(name: &str) -> anyhow::Result<String> {
    if name.is_empty() || name.contains('\0') {
        bail!("非法标识符（空或含 NUL）");
    }
    Ok(name.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    //! Snapshot suite (§7.7 A1 row: 给定 schema → 固定 DDL 文本). Full-text
    //! assertions against the generator's exact output; the matrix runs
    //! 4-type vocabulary × UNIQUE/DUPLICATE × partitioned/plain.

    use super::*;

    fn schema(db: &str, table: &str, columns: &[(&str, &str)]) -> Schema {
        Schema {
            db: db.into(),
            table: table.into(),
            columns: columns
                .iter()
                .map(|(n, t)| (n.to_string(), t.to_string()))
                .collect(),
        }
    }

    // ① all four types × DUPLICATE × plain — no properties block at all.
    #[test]
    fn snapshot_duplicate_four_types_plain() {
        let s = schema(
            "shop",
            "events",
            &[
                ("id", "int64"),
                ("name", "utf8"),
                ("price", "float64"),
                ("active", "bool"),
            ],
        );
        let d = generate_draft(&s, &KeyHints::default()).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `shop`.`events` (
    `id` BIGINT,
    `name` VARCHAR(65533),
    `price` DOUBLE,
    `active` BOOLEAN
)
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS AUTO;"#
        );
        assert_eq!(d.key_model, KeyModel::Duplicate);
        assert_eq!(d.key_model.name(), "duplicate");
        assert_eq!(d.key_column, "id");
        assert!(
            d.key_column_guessed,
            "heuristic key must carry the guess flag"
        );
        assert!(d.partition.is_none());
        assert!(d.notes.is_empty());
    }

    // ② all four types × UNIQUE (op_type evidence) × plain — key column
    // hoisted ahead of op_type (Doris key columns lead the list), MoW on.
    #[test]
    fn snapshot_unique_op_type_plain() {
        let s = schema(
            "shop",
            "cdc_orders",
            &[
                ("op_type", "utf8"),
                ("order_id", "utf8"),
                ("amount", "float64"),
                ("paid", "bool"),
            ],
        );
        let d = generate_draft(&s, &KeyHints::default()).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `shop`.`cdc_orders` (
    `order_id` VARCHAR(65533),
    `op_type` VARCHAR(65533),
    `amount` DOUBLE,
    `paid` BOOLEAN
)
UNIQUE KEY(`order_id`)
DISTRIBUTED BY HASH(`order_id`) BUCKETS AUTO
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true"
);"#
        );
        assert_eq!(d.key_model, KeyModel::Unique);
        assert!(d.key_model_reason.contains("CDC"));
        assert_eq!(d.key_column, "order_id");
        assert!(d.key_column_guessed);
        assert!(d.partition.is_none());
    }

    // ③ OGG envelope × UNIQUE (declared sequence_column) × partitioned on
    // ogg_ts × single-node replication downgrade.
    #[test]
    fn snapshot_unique_ogg_partitioned_replication_1() {
        let s = schema(
            "shop",
            "ogg_orders",
            &[
                ("op_type", "utf8"),
                ("ogg_table", "utf8"),
                ("ogg_ts", "int64"),
                ("order_id", "utf8"),
                ("seq", "int64"),
                ("amount", "float64"),
            ],
        );
        let hints = KeyHints {
            sequence_column: Some("seq".into()),
            replication_num: Some(1),
            ..KeyHints::default()
        };
        let d = generate_draft(&s, &hints).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `shop`.`ogg_orders` (
    `order_id` VARCHAR(65533),
    `op_type` VARCHAR(65533),
    `ogg_table` VARCHAR(65533),
    `ogg_ts` BIGINT,
    `seq` BIGINT,
    `amount` DOUBLE
)
UNIQUE KEY(`order_id`)
PARTITION BY RANGE(`ogg_ts`) ()
DISTRIBUTED BY HASH(`order_id`) BUCKETS AUTO
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "replication_num" = "1"
);"#
        );
        assert_eq!(d.key_model, KeyModel::Unique);
        // sequence column orders updates, never becomes the key itself
        assert_eq!(d.key_column, "order_id");
        assert!(d.key_column_guessed);
        let p = d.partition.unwrap();
        assert_eq!(p.column, "ogg_ts");
        assert!(p.guessed);
    }

    // ④ four types × DUPLICATE × partitioned (utf8 event_time name match) —
    // *_id heuristic still picks the key; partition is a separate guess.
    #[test]
    fn snapshot_duplicate_partitioned() {
        let s = schema(
            "metrics",
            "samples",
            &[
                ("event_time", "utf8"),
                ("user_id", "int64"),
                ("score", "float64"),
                ("ok", "bool"),
            ],
        );
        let d = generate_draft(&s, &KeyHints::default()).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `metrics`.`samples` (
    `user_id` BIGINT,
    `event_time` VARCHAR(65533),
    `score` DOUBLE,
    `ok` BOOLEAN
)
DUPLICATE KEY(`user_id`)
PARTITION BY RANGE(`event_time`) ()
DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p"
);"#
        );
        assert_eq!(d.key_column, "user_id");
        assert!(d.key_column_guessed);
        assert_eq!(d.partition.unwrap().column, "event_time");
    }

    // ⑤ confirmed key column (wizard round two) — no guess badge; probed
    // multi-BE replication rides along.
    #[test]
    fn snapshot_confirmed_key_and_replication() {
        let s = schema(
            "shop",
            "users",
            &[("user_id", "int64"), ("nickname", "utf8"), ("vip", "bool")],
        );
        let hints = KeyHints {
            key_column: Some("user_id".into()),
            replication_num: Some(3),
            ..KeyHints::default()
        };
        let d = generate_draft(&s, &hints).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `shop`.`users` (
    `user_id` BIGINT,
    `nickname` VARCHAR(65533),
    `vip` BOOLEAN
)
DUPLICATE KEY(`user_id`)
DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO
PROPERTIES (
    "replication_num" = "3"
);"#
        );
        assert!(!d.key_column_guessed, "confirmed key is not a guess");
        assert_eq!(
            d.key_model_reason,
            "未检测到 CDC 证据，推荐 DUPLICATE KEY（追加写）"
        );
    }

    // ⑥ human overrides UNIQUE→DUPLICATE with a sequence column declared →
    // plain-column note, no MoW property, partition still offered.
    #[test]
    fn snapshot_duplicate_override_with_sequence_note() {
        let s = schema(
            "shop",
            "raw_cdc",
            &[
                ("op_type", "utf8"),
                ("row_id", "int64"),
                ("ogg_ts", "int64"),
                ("payload", "utf8"),
            ],
        );
        let hints = KeyHints {
            sequence_column: Some("ogg_ts".into()),
            key_model: Some(KeyModel::Duplicate),
            ..KeyHints::default()
        };
        let d = generate_draft(&s, &hints).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `shop`.`raw_cdc` (
    `row_id` BIGINT,
    `op_type` VARCHAR(65533),
    `ogg_ts` BIGINT,
    `payload` VARCHAR(65533)
)
DUPLICATE KEY(`row_id`)
PARTITION BY RANGE(`ogg_ts`) ()
DISTRIBUTED BY HASH(`row_id`) BUCKETS AUTO
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p"
);"#
        );
        assert_eq!(d.key_model, KeyModel::Duplicate);
        assert!(!d.ddl.contains("enable_unique_key_merge_on_write"));
        assert_eq!(
            d.notes,
            vec![
                "sequence_column `ogg_ts` 仅作为普通列保留，DUPLICATE KEY 模式下无去重语义"
                    .to_string()
            ]
        );
    }

    // ⑦ no id-shaped column at all → key falls back to the first column,
    // still flagged as a guess.
    #[test]
    fn snapshot_key_falls_back_to_first_column() {
        let s = schema(
            "shop",
            "plain",
            &[("region", "utf8"), ("qty", "int64"), ("ratio", "float64")],
        );
        let d = generate_draft(&s, &KeyHints::default()).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `shop`.`plain` (
    `region` VARCHAR(65533),
    `qty` BIGINT,
    `ratio` DOUBLE
)
DUPLICATE KEY(`region`)
DISTRIBUTED BY HASH(`region`) BUCKETS AUTO;"#
        );
        assert_eq!(d.key_column, "region");
        assert!(d.key_column_guessed);
    }

    // Identifier hygiene: backticks doubled, no breakout.
    #[test]
    fn snapshot_quoting_backtick_names() {
        let s = schema("sh`op", "ev`ents", &[("na`me", "utf8"), ("id", "int64")]);
        let d = generate_draft(&s, &KeyHints::default()).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `sh``op`.`ev``ents` (
    `id` BIGINT,
    `na``me` VARCHAR(65533)
)
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS AUTO;"#
        );
    }

    #[test]
    fn rejects_bad_inputs() {
        // empty db (dot-less Kafka topic) / empty table / no columns
        assert!(
            generate_draft(&schema("", "t", &[("id", "int64")]), &KeyHints::default()).is_err()
        );
        assert!(
            generate_draft(&schema("db", "", &[("id", "int64")]), &KeyHints::default()).is_err()
        );
        assert!(
            generate_draft(&schema("db", "t", &[]), &KeyHints::default()).is_err(),
            "empty column list is rejected"
        );
        // duplicate column
        let e = generate_draft(
            &schema("db", "t", &[("id", "int64"), ("id", "utf8")]),
            &KeyHints::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("列名重复"), "{e}");
        // outside the four-type vocabulary — explicit, no silent fold
        let e = generate_draft(
            &schema("db", "t", &[("id", "int64"), ("ts", "timestamp")]),
            &KeyHints::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            e.contains("timestamp") && e.contains("utf8/int64/float64/bool"),
            "{e}"
        );
        // hints naming unknown columns
        let e = generate_draft(
            &schema("db", "t", &[("id", "int64")]),
            &KeyHints {
                sequence_column: Some("nope".into()),
                ..KeyHints::default()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("sequence_column"), "{e}");
        let e = generate_draft(
            &schema("db", "t", &[("id", "int64")]),
            &KeyHints {
                key_column: Some("nope".into()),
                ..KeyHints::default()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("键列"), "{e}");
        // NUL smuggling in a name
        assert!(generate_draft(
            &schema("db", "t", &[("id\0", "int64")]),
            &KeyHints::default()
        )
        .is_err());
    }
}
