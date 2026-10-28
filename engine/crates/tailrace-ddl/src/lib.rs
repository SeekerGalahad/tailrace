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
//! switches to the column type; see [`partition_candidate`]). A candidate is
//! only offered when Doris would actually accept the clause (type and
//! key-model rules probed live on 2.1.7 — an unexecutable draft is a lie
//! too), and the human can drop it through [`KeyHints::partition`].
//!
//! Every guess is annotated, never silent (§7.2 ③): key model, key column
//! and partition column each carry a recommendation reason or a 「猜测」
//! flag in [`DraftDdl`] — exactly what the console wizard highlights in its
//! confirm step; a partition candidate that had to be skipped says why in
//! [`DraftDdl::notes`]. Nothing here executes anything; `tailrace-metadata`'s
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
    /// Partition decision fed back from the wizard. `None` → the heuristic
    /// offers its candidate (when one is executable) flagged as a guess;
    /// `Some(true)` → the human confirmed it (guess flag clears);
    /// `Some(false)` → dropped — no `PARTITION BY` clause and no
    /// `dynamic_partition.*` properties, whatever the heuristic found.
    pub partition: Option<bool>,
}

/// Day-granularity RANGE dynamic-partition candidate (§7.2 ②: 时序数据标准
/// 做法) — offered when a time-shaped column can carry the RANGE key,
/// confirmed or dropped by the human via [`KeyHints::partition`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionCandidate {
    /// Column the RANGE partition rides on.
    pub column: String,
    /// `true` = name-based heuristic the human has not confirmed yet;
    /// `false` once confirmed (`KeyHints::partition == Some(true)`) — or,
    /// after G2, when a genuine time type makes the pick evidence.
    pub guessed: bool,
}

/// Outcome of the partition heuristic — a candidate, a time-shaped column
/// that had to be skipped (with the reason, so the draft stays annotated),
/// or nothing time-shaped at all.
enum PartitionPick {
    Candidate(PartitionCandidate),
    Skipped(String),
    None,
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

    let mut notes = Vec::new();
    if key_model == KeyModel::Duplicate {
        if let Some(seq) = &hints.sequence_column {
            notes.push(format!(
                "sequence_column `{seq}` 仅作为普通列保留，DUPLICATE KEY 模式下无去重语义"
            ));
        }
    }

    // Partition: the heuristic runs after key model and key column because
    // both gate executability (MoW partition column must be the key). The
    // human's decision then overrides: dropped → no clause, no properties;
    // confirmed → the guess flag clears.
    let partition = match partition_candidate(&schema.columns, key_model, &key_column) {
        PartitionPick::Candidate(_) if hints.partition == Some(false) => None,
        PartitionPick::Candidate(c) => {
            // Doris derives DAY dynamic partitions on an integer column as
            // yyyyMMdd bounds (probed: p20261027 = [20261027, 20261028)) —
            // an epoch column lands outside every partition and the load is
            // refused. The vocabulary cannot tell the two apart; say so.
            notes.push(format!(
                "分区列 `{}` 为 BIGINT：按天动态分区生成 yyyyMMdd 整数区间（如 p20261027 = \
                 [20261027, 20261028)），该列的值须为 yyyyMMdd 形式的整数日期，epoch 秒/毫秒值会\
                 落在所有分区之外被拒载——不确定时请取消分区",
                c.column
            ));
            Some(PartitionCandidate {
                guessed: c.guessed && hints.partition != Some(true),
                ..c
            })
        }
        PartitionPick::Skipped(why) => {
            notes.push(why);
            None
        }
        PartitionPick::None => None,
    };

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

/// Partition heuristic: the first time-shaped column that Doris would accept
/// as a RANGE partition key. Name-based only because the four-type vocabulary
/// cannot carry a timestamp/date type (such columns fold to utf8 upstream) —
/// G2 replaces the name match with a type match and the guess flag with
/// evidence. Executability rules (each one an FE refusal probed live on
/// Doris 2.1.7, so the draft never walks into them):
/// - only `int64` → BIGINT can be a RANGE key: VARCHAR / BOOLEAN ("cannot be
///   a range partition key") and DOUBLE ("Floating point type column can not
///   be partition column") are refused;
/// - a merge-on-write UNIQUE KEY table's partition column must be a KEY
///   column ("Merge-on-Write table's partition column must be KEY column") —
///   with the single-column key clause emitted here, that is the key column
///   itself.
///
/// Time-shaped columns are scanned in declaration order; the first executable
/// one wins. If every one of them had to be skipped, the first one's reason
/// comes back as a note (a dropped guess is still annotated, never silent).
fn partition_candidate(columns: &[Column], key_model: KeyModel, key_column: &str) -> PartitionPick {
    const EXACT: [&str; 6] = ["ts", "time", "timestamp", "date", "dt", "day"];
    const SUFFIX: [&str; 5] = ["_ts", "_time", "_timestamp", "_date", "_at"];
    let time_shaped = |name: &str| {
        let n = name.to_ascii_lowercase();
        EXACT.contains(&n.as_str()) || SUFFIX.iter().any(|s| n.len() > s.len() && n.ends_with(s))
    };
    let mut first_skip: Option<String> = None;
    for (name, ty) in columns.iter().filter(|(n, _)| time_shaped(n)) {
        let skip = if ty != "int64" {
            format!(
                "时间形列 `{name}` 为 {ty}（{}），Doris RANGE 分区键仅接受整数/日期类型——未生成分区\
                 子句（G2 落地真实时间类型后可自动分区）",
                doris_type(ty).unwrap_or("?")
            )
        } else if key_model == KeyModel::Unique && name != key_column {
            format!(
                "时间形列 `{name}` 可作分区列，但 UNIQUE KEY（merge-on-write）表的分区列必须是键列——\
                 未生成分区子句（如需按天分区，键须含该列，请人工决策）"
            )
        } else {
            return PartitionPick::Candidate(PartitionCandidate {
                column: name.clone(),
                guessed: true,
            });
        };
        first_skip.get_or_insert(skip);
    }
    match first_skip {
        Some(why) => PartitionPick::Skipped(why),
        None => PartitionPick::None,
    }
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
    //! 4-type vocabulary × UNIQUE/DUPLICATE × partitioned/plain, and every
    //! partitioned shape asserted here was created successfully on a live
    //! Doris 2.1.7 (R6 review round) — the skipped shapes are the ones the
    //! FE refused.

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

    // ③ OGG envelope × UNIQUE (declared sequence_column) × single-node
    // replication downgrade. ogg_ts is time-shaped AND BIGINT, but a
    // merge-on-write table's partition column must be a key column (Doris
    // 2.1.7: "Merge-on-Write table's partition column must be KEY column")
    // — the candidate is skipped and the draft says why instead of emitting
    // a clause the FE refuses.
    #[test]
    fn snapshot_unique_ogg_replication_1_partition_skipped_non_key() {
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
DISTRIBUTED BY HASH(`order_id`) BUCKETS AUTO
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "replication_num" = "1"
);"#
        );
        assert_eq!(d.key_model, KeyModel::Unique);
        // sequence column orders updates, never becomes the key itself
        assert_eq!(d.key_column, "order_id");
        assert!(d.key_column_guessed);
        assert!(
            d.partition.is_none(),
            "MoW non-key time column is not a candidate"
        );
        assert_eq!(d.notes.len(), 1, "{:?}", d.notes);
        assert!(
            d.notes[0].contains("`ogg_ts`") && d.notes[0].contains("必须是键列"),
            "skip reason annotated: {}",
            d.notes[0]
        );
    }

    // ④ four types × DUPLICATE × utf8 event_time name match — the key guess
    // still rides *_id; the time-shaped column is VARCHAR, which Doris
    // refuses as a RANGE key ("Column[event_time] type[VARCHAR] cannot be a
    // range partition key"), so no clause and a note instead.
    #[test]
    fn snapshot_duplicate_partition_skipped_varchar_time_column() {
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
DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO;"#
        );
        assert_eq!(d.key_column, "user_id");
        assert!(d.key_column_guessed);
        assert!(d.partition.is_none());
        assert_eq!(d.notes.len(), 1, "{:?}", d.notes);
        assert!(
            d.notes[0].contains("`event_time`")
                && d.notes[0].contains("utf8（VARCHAR(65533)）")
                && d.notes[0].contains("仅接受整数/日期类型"),
            "{}",
            d.notes[0]
        );
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
    // plain-column note, no MoW property; the BIGINT time column IS an
    // executable candidate on a DUPLICATE table (probed: DUPLICATE + BIGINT
    // non-key RANGE key + dynamic DAY partitions create fine) — offered as a
    // guess, with the yyyyMMdd caveat spelled out.
    #[test]
    fn snapshot_duplicate_override_with_sequence_note_partitioned() {
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
        let p = d.partition.clone().unwrap();
        assert_eq!(p.column, "ogg_ts");
        assert!(p.guessed, "unconfirmed candidate keeps the guess flag");
        assert_eq!(d.notes.len(), 2, "{:?}", d.notes);
        assert_eq!(
            d.notes[0],
            "sequence_column `ogg_ts` 仅作为普通列保留，DUPLICATE KEY 模式下无去重语义"
        );
        assert!(
            d.notes[1].contains("`ogg_ts` 为 BIGINT") && d.notes[1].contains("yyyyMMdd"),
            "{}",
            d.notes[1]
        );
    }

    // ⑥b the human DROPS the candidate (`partition: Some(false)`) — same
    // schema as ⑥: no PARTITION clause, no dynamic_partition properties, no
    // partition caveat; the sequence note stands.
    #[test]
    fn snapshot_partition_dropped_by_hint() {
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
            partition: Some(false),
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
DISTRIBUTED BY HASH(`row_id`) BUCKETS AUTO;"#
        );
        assert!(
            d.partition.is_none(),
            "dropped candidate leaves the response"
        );
        assert_eq!(
            d.notes,
            vec![
                "sequence_column `ogg_ts` 仅作为普通列保留，DUPLICATE KEY 模式下无去重语义"
                    .to_string()
            ]
        );
    }

    // ⑥c the human CONFIRMS the candidate (`partition: Some(true)`) — DDL
    // byte-identical to ⑥, guess flag cleared (the key-column confirm
    // semantics, applied to the partition).
    #[test]
    fn snapshot_partition_confirmed_clears_guess() {
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
        let unconfirmed = generate_draft(
            &s,
            &KeyHints {
                sequence_column: Some("ogg_ts".into()),
                key_model: Some(KeyModel::Duplicate),
                ..KeyHints::default()
            },
        )
        .unwrap();
        let confirmed = generate_draft(
            &s,
            &KeyHints {
                sequence_column: Some("ogg_ts".into()),
                key_model: Some(KeyModel::Duplicate),
                partition: Some(true),
                ..KeyHints::default()
            },
        )
        .unwrap();
        assert_eq!(confirmed.ddl, unconfirmed.ddl);
        assert!(confirmed.ddl.contains("PARTITION BY RANGE(`ogg_ts`) ()"));
        assert_eq!(
            confirmed.partition,
            Some(PartitionCandidate {
                column: "ogg_ts".into(),
                guessed: false,
            })
        );
        assert_eq!(confirmed.notes, unconfirmed.notes);
    }

    // ⑥d UNIQUE × partition column == key column — the one MoW shape Doris
    // accepts (probed: UNIQUE KEY(ts) PARTITION BY RANGE(ts) + MoW + dynamic
    // DAY creates fine). Here the human confirmed the time column as the key.
    #[test]
    fn snapshot_unique_partitioned_on_key_column() {
        let s = schema(
            "iot",
            "readings",
            &[
                ("op_type", "utf8"),
                ("event_ts", "int64"),
                ("value", "float64"),
            ],
        );
        let hints = KeyHints {
            key_column: Some("event_ts".into()),
            ..KeyHints::default()
        };
        let d = generate_draft(&s, &hints).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `iot`.`readings` (
    `event_ts` BIGINT,
    `op_type` VARCHAR(65533),
    `value` DOUBLE
)
UNIQUE KEY(`event_ts`)
PARTITION BY RANGE(`event_ts`) ()
DISTRIBUTED BY HASH(`event_ts`) BUCKETS AUTO
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p"
);"#
        );
        assert_eq!(d.key_model, KeyModel::Unique);
        assert!(!d.key_column_guessed);
        assert_eq!(
            d.partition.as_ref().map(|p| p.column.as_str()),
            Some("event_ts")
        );
        assert_eq!(d.notes.len(), 1, "{:?}", d.notes);
        assert!(d.notes[0].contains("yyyyMMdd"));
    }

    // ⑥e several time-shaped columns: the unexecutable one (utf8) is passed
    // over for the first executable one (int64) — and because a candidate
    // was found, no skip note is emitted for the utf8 column.
    #[test]
    fn partition_picks_first_executable_time_column() {
        let s = schema(
            "m",
            "t",
            &[("event_time", "utf8"), ("dt", "int64"), ("v", "float64")],
        );
        let d = generate_draft(&s, &KeyHints::default()).unwrap();
        assert!(d.ddl.contains("PARTITION BY RANGE(`dt`) ()"), "{}", d.ddl);
        assert!(!d.ddl.contains("event_time`) ()"));
        assert_eq!(d.partition.as_ref().map(|p| p.column.as_str()), Some("dt"));
        assert_eq!(d.notes.len(), 1, "{:?}", d.notes);
        assert!(d.notes[0].contains("`dt` 为 BIGINT"));
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
