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
//! Vocabulary surface (G2 words landed, A3 widened the mapping, A4 opened
//! `variant`, A5 opened `list<float32>`): columns carry the 16-word
//! vocabulary; the generator maps the 13 scalar words — the four legacy ones
//! (`utf8` / `int64` / `float64` / `bool`) plus `string` / `int8` / `int16`
//! / `int32` / `float32` / `decimal(p,s)` / `timestamp(unit)` / `date` /
//! `binary` — per blueprint §7.1's mapping column, `variant` → `VARIANT`
//! (probed on 2.1.7 and 3.1.4 alike,
//! scripts/e2e/evidence/variant-probe-2026-10-30/: a VARIANT value column
//! creates under DUPLICATE and UNIQUE MoW, the FE refuses it as a key column,
//! and the engine's string-shaped emission is parsed into the variant
//! structure server-side), and `list<float32[,dim]>` → `ARRAY<FLOAT>` with
//! the declared dimension as a column COMMENT (probed on 3.1.4,
//! scripts/e2e/evidence/vector-probe-2026-10-30/: the value column creates
//! under DUPLICATE and UNIQUE MoW with the COMMENT kept verbatim, the FE
//! refuses ARRAY as a key column and as a partition column — Doris itself
//! has no array dimension, so the COMMENT is documentation and the decoder's
//! dim guard is the enforcement). `null` stays unmapped — Doris has no
//! all-null column type and inventing one (NULL_TYPE is internal) would make
//! the draft lie, so it keeps erroring until a use case names the target.
//! Anything unmapped is an explicit error, not a silent fold: a draft built
//! on a folded type would lie about the table. For the same reason the
//! partition heuristic is **type**-based since A3 — timestamp/date columns
//! are real time types now, so they take the RANGE key directly (probed on
//! 2.1.7: dynamic DAY partitions on a DATETIME/DATE column generate true
//! date bounds, no yyyyMMdd convention needed — that caveat stays
//! BIGINT-only), and a candidate is
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
//!
//! Sequence-column semantics (dogfooding #3, R8): under UNIQUE KEY a
//! declared `sequence_column` is emitted as the table-level
//! `function_column.sequence_col` property — without it Doris silently
//! ignores the engine's `sequence_column` stream-load header and the
//! ordering protection the declaration promises does not exist (probed live
//! on 2.1.7 and 3.1.4; the W5 e2e wave locks the load-side behaviour).

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
/// generator's vocabulary surface, hints naming columns that do not exist.
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
                "列 {name:?} 的类型 {ty:?} 不在 DDL 生成器支持面内\
                 （utf8/string/int8/int16/int32/int64/float32/float64/decimal(p,s)/\
                 timestamp(s|ms|us|ns)/date/bool/binary/variant/list<float32[,dim]>；\
                 null 无 Doris 对应列类型）——不静默折叠未知类型"
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
    // A4: a VARIANT column cannot carry the key (FE: "Variant type should
    // not be used in key column" on 3.1.4, "JSONB or VARIANT type …" on
    // 2.1.7 — probe A3). The heuristic already steps over variant columns;
    // this catches an explicit pick and the all-variant schema, where the
    // heuristic has nothing else to fall back on — a draft the FE refuses
    // is a lie, same rule as the partition executability checks.
    if schema
        .columns
        .iter()
        .any(|(n, t)| n == &key_column && t == "variant")
    {
        bail!(
            "键列 {key_column:?} 的类型为 variant：Doris 不接受 VARIANT 作键列\
             （FE 拒绝 \"Variant type should not be used in key column\"）——请改选标量列为键"
        );
    }
    // A5: same rule for the vector column (FE: "Array can only be used in
    // the non-key column of the duplicate table at present." — probe A3,
    // 3.1.4; the wording is legacy, the value-column case works under
    // UNIQUE too, probe A2).
    if let Some((_, ty)) = schema
        .columns
        .iter()
        .find(|(n, t)| n == &key_column && is_list_type(t))
    {
        bail!(
            "键列 {key_column:?} 的类型为 {ty}：Doris 不接受 ARRAY 作键列\
             （FE 拒绝 \"Array can only be used in the non-key column\"）——请改选标量列为键"
        );
    }

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
            // BIGINT keeps the yyyyMMdd caveat: Doris derives DAY dynamic
            // partitions on an integer column as yyyyMMdd bounds (probed:
            // p20261027 = [20261027, 20261028)) — an epoch column lands
            // outside every partition and the load is refused. A DATETIME/
            // DATE candidate needs no such caveat: its dynamic DAY bounds
            // are true dates (probed: p20261030 onward), which is exactly
            // why the time types take the RANGE key since A3.
            let is_integer = schema
                .columns
                .iter()
                .find(|(n, _)| n == &c.column)
                .map(|(_, ty)| ty == "int64")
                .unwrap_or(false);
            if is_integer {
                notes.push(format!(
                    "分区列 `{}` 为 BIGINT：按天动态分区生成 yyyyMMdd 整数区间（如 p20261027 = \
                     [20261027, 20261028)），该列的值须为 yyyyMMdd 形式的整数日期，epoch 秒/毫秒值会\
                     落在所有分区之外被拒载——不确定时请取消分区",
                    c.column
                ));
            }
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

    // Fixed property order keeps snapshots stable: MoW → sequence_col →
    // dynamic partition → replication.
    let mut props: Vec<String> = Vec::new();
    if key_model == KeyModel::Unique {
        props.push(r#""enable_unique_key_merge_on_write" = "true""#.to_string());
    }
    // Dogfooding #3 (R8): a UNIQUE table with a declared sequence column
    // must carry the table-level property. Without it the engine's
    // `sequence_column` stream-load header is silently ignored by Doris
    // (Status Success) and a low-seq write clobbers a high-seq row — the
    // exact ordering protection the declaration promises, probed live on
    // both stacks (probe2.out D2 vs C2: the property-less table accepts the
    // seq=5 overwrite, the attributed table drops it). The column itself
    // stays a plain declared column (Doris hides the sequence role); under
    // DUPLICATE KEY there is no dedup story, so the plain-column note in
    // `notes` remains the only effect.
    if key_model == KeyModel::Unique {
        if let Some(seq) = &hints.sequence_column {
            props.push(format!(
                r#""function_column.sequence_col" = "{}""#,
                quote_ident(seq)?
            ));
        }
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

/// Column-type vocabulary → Doris column DDL (blueprint §7.1 mapping column,
/// the A3 widening — the same probe-backed rows the snapshot suite pins).
/// The four legacy words keep their exact pre-A3 spellings; the new scalar
/// words join them. Parametric words carry their parameters: `decimal(p,s)`
/// round-trips p,s (bounds checked here, same 1..=38 / s≤p contract as
/// `ColumnType::parse` — kept local so this pure-text crate does not grow a
/// datafusion dependency through tailrace-ir), every `timestamp(unit)`
/// lands `DATETIME(6)` (Doris has no timezone and one time precision, so
/// the unit is a decode-side concern only). `binary` lands STRING with the
/// base64 convention as a column COMMENT — Doris has no native binary, and
/// a comment rides the table (SHOW CREATE TABLE keeps it visible) where a
/// generator note would only ride the wizard. `variant` lands `VARIANT`
/// (A4 — DESC prints it back as `variant`, probed on 2.1.7 and 3.1.4; the
/// column takes any JSON value and the engine's string-shaped emission is
/// parsed into the variant structure server-side, so no carrier convention
/// is needed on the DDL). `list<float32[,dim]>` lands `ARRAY<FLOAT>` (A5 —
/// DESC prints it back as `array<float>`, probed on 3.1.4), the declared
/// dimension riding as a column COMMENT in the binary style: Doris has no
/// array dimension (a 5-element array loads into a dim=3 column, probe B9),
/// so the comment documents the contract for whoever reads SHOW CREATE
/// TABLE while the decoder's dim guard enforces it on the way in. The
/// `list<…>` form is parsed locally like `decimal(…)` (same datafusion-free
/// rationale): element must be `float32`, dim — when present — a decimal
/// integer ≥ 1, the same contract `ColumnType::parse` checks. `null`
/// returns `None` (see the crate doc).
///
/// Public since A3: the console's evolve side (ALTER TABLE … ADD/MODIFY
/// COLUMN texts) reuses this one mapping instead of keeping a second
/// hand-synced copy — the pre-A3 copy had to be widened in lockstep, which
/// is exactly how the two sides drift.
pub fn doris_type(vocab: &str) -> Option<String> {
    match vocab {
        "utf8" => Some("VARCHAR(65533)".to_string()),
        "string" => Some("STRING".to_string()),
        "int8" => Some("TINYINT".to_string()),
        "int16" => Some("SMALLINT".to_string()),
        "int32" => Some("INT".to_string()),
        "int64" => Some("BIGINT".to_string()),
        "float32" => Some("FLOAT".to_string()),
        "float64" => Some("DOUBLE".to_string()),
        "date" => Some("DATE".to_string()),
        "bool" => Some("BOOLEAN".to_string()),
        "binary" => Some("STRING COMMENT 'base64-encoded binary'".to_string()),
        "variant" => Some("VARIANT".to_string()),
        _ => {
            if let Some(rest) = vocab.strip_prefix("decimal(") {
                let params = rest.strip_suffix(')')?;
                let (p, s) = params.split_once(',')?;
                let (p, s): (u32, u32) = (p.trim().parse().ok()?, s.trim().parse().ok()?);
                if !(1..=38).contains(&p) || s > p {
                    return None;
                }
                Some(format!("DECIMAL({p},{s})"))
            } else if let Some(unit) = vocab
                .strip_prefix("timestamp(")
                .and_then(|rest| rest.strip_suffix(')'))
            {
                match unit {
                    "s" | "ms" | "us" | "ns" => Some("DATETIME(6)".to_string()),
                    _ => None,
                }
            } else if let Some(inner) = vocab
                .strip_prefix("list<")
                .and_then(|rest| rest.strip_suffix('>'))
            {
                // `float32` alone, or `float32,dim` with dim ≥ 1
                let (elem, dim) = match inner.split_once(',') {
                    Some((e, d)) => (e.trim(), Some(d.trim())),
                    None => (inner.trim(), None),
                };
                if elem != "float32" {
                    return None;
                }
                match dim {
                    None => Some("ARRAY<FLOAT>".to_string()),
                    Some(d) => {
                        // local mirror of tailrace-ir's MAX_VECTOR_DIM cap
                        // (this crate deliberately does not depend on that
                        // one) — dims past it are rejected with the same
                        // explicit-error-no-fold contract (R9 review fix:
                        // an unbounded dim is an unbounded decoder
                        // pre-allocation)
                        const MAX_VECTOR_DIM: u32 = 65_536;
                        let d: u32 = d
                            .parse()
                            .ok()
                            .filter(|d| (1..=MAX_VECTOR_DIM).contains(d))?;
                        Some(format!("ARRAY<FLOAT> COMMENT 'vector dim={d}'"))
                    }
                }
            } else {
                None
            }
        }
    }
}

/// Whether a vocabulary word is the vector column (`list<…>`). Only called
/// on words [`doris_type`] already accepted, so the prefix is enough — the
/// generator never sees an unparsed `list<…>` past the validation pass.
fn is_list_type(vocab: &str) -> bool {
    vocab.starts_with("list<")
}

/// The OGG envelope's CDC marker column (exact name, case-insensitive).
fn has_op_type(columns: &[Column]) -> bool {
    columns
        .iter()
        .any(|(n, _)| n.eq_ignore_ascii_case("op_type"))
}

/// Key-column heuristic (§7.2 ②): first column named `id` / `*_id` in
/// declaration order, else the first column — skipping `variant` columns
/// (A4: the FE refuses VARIANT as a key column, probe A3) and `list<…>`
/// columns (A5: the FE refuses ARRAY as a key column, vector probe A3) at
/// both steps. A guess by definition — the caller flags it. When every
/// column is variant/list the first one comes back anyway and
/// [`generate_draft`]'s key-type checks turn that into the explicit error.
fn guess_key_column(columns: &[Column]) -> &str {
    let scalar = |(_, ty): &&Column| ty != "variant" && !is_list_type(ty);
    for (name, _) in columns.iter().filter(scalar) {
        let n = name.to_ascii_lowercase();
        if n == "id" || n.ends_with("_id") {
            return name.as_str();
        }
    }
    columns
        .iter()
        .find(scalar)
        .map(|(n, _)| n.as_str())
        .unwrap_or(&columns[0].0)
}

/// Whether a vocabulary word can carry a RANGE partition key (executability
/// rules probed live on Doris 2.1.7, re-probed for the A3 widening): the
/// real time types (`timestamp(any unit)` → DATETIME, `date` → DATE) take
/// the RANGE key natively — dynamic DAY partitions on them generate true
/// date bounds; `int64` works through the yyyyMMdd integer convention (see
/// the caveat note in [`generate_draft`]). Everything else is refused by
/// the FE: VARCHAR / STRING ("cannot be a range partition key"), BOOLEAN,
/// DOUBLE / FLOAT ("Floating point type column can not be partition
/// column"), and DECIMAL alike.
fn range_key_capable(vocab: &str) -> bool {
    vocab == "int64"
        || vocab == "date"
        || vocab
            .strip_prefix("timestamp(")
            .is_some_and(|rest| rest.ends_with(')'))
}

/// Partition heuristic: the first time-shaped column that Doris would accept
/// as a RANGE partition key. A3 switched the executability test from
/// name-only to name × **type** — the time-shaped *name* still picks the
/// candidates (a column named `event_ts` but typed utf8 is text about time,
/// not time), and among those the type decides executability:
/// - a merge-on-write UNIQUE KEY table's partition column must be a KEY
///   column ("Merge-on-Write table's partition column must be KEY column") —
///   with the single-column key clause emitted here, that is the key column
///   itself;
/// - non-range-key types are skipped with the reason as a note.
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
        let skip = if !range_key_capable(ty) {
            format!(
                "时间形列 `{name}` 为 {ty}（{}），Doris RANGE 分区键仅接受整数/日期时间类型——\
                 未生成分区子句（如需按该列分区请改为 timestamp/date/int64）",
                doris_type(ty).unwrap_or_else(|| "?".to_string())
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
    //! assertions against the generator's exact output; the matrix runs the
    //! generator's vocabulary × UNIQUE/DUPLICATE × partitioned/plain, and every
    //! partitioned shape asserted here was created successfully on a live
    //! Doris 2.1.7 (R6 review round; the A3 shapes — new column types,
    //! DATETIME/DATE RANGE keys, the binary COMMENT — re-probed 2026-10-30;
    //! the A4 variant shapes probed on 2.1.7 AND 3.1.4 the same day; the A5
    //! vector shapes probed on 3.1.4,
    //! scripts/e2e/evidence/vector-probe-2026-10-30/) — the skipped shapes
    //! are the ones the FE refused.

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
    // a clause the FE refuses. Dogfooding #3 (R8): the declared sequence
    // column rides as `function_column.sequence_col` — the table-level
    // property without which Doris silently ignores the engine's
    // `sequence_column` load header (the attr'd shape is the live-probed
    // C2 table of probe2.out, created on 2.1.7 and 3.1.4 alike).
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
    "function_column.sequence_col" = "seq",
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
                && d.notes[0].contains("仅接受整数/日期时间类型")
                && d.notes[0].contains("timestamp/date/int64"),
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
        // the two-state pair with ③: under DUPLICATE KEY the sequence
        // column never becomes a table property — no dedup story to order
        // (dogfooding #3's other edge: a property on a DUPLICATE table
        // would be refused by the FE outright)
        assert!(!d.ddl.contains("function_column.sequence_col"));
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

    // ⑧ A3 widening — every newly mapped word on one table (probed live on
    // 2.1.7 as probe_a3.new_vocab, 2026-10-30): the four int widths, FLOAT,
    // STRING, DECIMAL(18,2), DATETIME(6) and DATE all create; binary lands
    // STRING with the base64 convention as a column COMMENT (Doris has no
    // native binary). The timestamp column is time-shaped AND a real time
    // type, so it takes the RANGE key — and as a DATETIME candidate it
    // carries NO yyyyMMdd caveat (dynamic DAY partitions on a time column
    // generate true date bounds; that caveat is the BIGINT convention's).
    #[test]
    fn snapshot_new_vocabulary_partitioned_timestamp() {
        let s = schema(
            "shop",
            "typed",
            &[
                ("id", "int64"),
                ("rank", "int8"),
                ("grade", "int16"),
                ("qty", "int32"),
                ("score", "float32"),
                ("memo", "string"),
                ("amount", "decimal(18,2)"),
                ("created_at", "timestamp(ms)"),
                ("biz_day", "date"),
                ("payload", "binary"),
            ],
        );
        let d = generate_draft(&s, &KeyHints::default()).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `shop`.`typed` (
    `id` BIGINT,
    `rank` TINYINT,
    `grade` SMALLINT,
    `qty` INT,
    `score` FLOAT,
    `memo` STRING,
    `amount` DECIMAL(18,2),
    `created_at` DATETIME(6),
    `biz_day` DATE,
    `payload` STRING COMMENT 'base64-encoded binary'
)
DUPLICATE KEY(`id`)
PARTITION BY RANGE(`created_at`) ()
DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p"
);"#
        );
        assert_eq!(d.key_model, KeyModel::Duplicate);
        assert_eq!(d.key_column, "id");
        assert!(d
            .partition
            .as_ref()
            .is_some_and(|p| p.column == "created_at"));
        // a DATETIME candidate needs no yyyyMMdd warning — true date bounds
        assert!(
            d.notes.is_empty(),
            "time-typed candidate carries no integer-date caveat: {:?}",
            d.notes
        );
    }

    // ⑨ date as a non-key RANGE candidate on a DUPLICATE table (probed:
    // probe_a3.date_part — DUPLICATE + RANGE(d DATE) non-key + dynamic DAY
    // creates fine). Same shape as ⑥'s BIGINT candidate minus the yyyyMMdd
    // caveat.
    #[test]
    fn snapshot_date_partitioned_duplicate_non_key() {
        let s = schema(
            "shop",
            "daily",
            &[("user_id", "int64"), ("dt", "date"), ("v", "float64")],
        );
        let d = generate_draft(&s, &KeyHints::default()).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `shop`.`daily` (
    `user_id` BIGINT,
    `dt` DATE,
    `v` DOUBLE
)
DUPLICATE KEY(`user_id`)
PARTITION BY RANGE(`dt`) ()
DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p"
);"#
        );
        assert!(d.partition.as_ref().is_some_and(|p| p.column == "dt"));
        assert!(d.notes.is_empty(), "{:?}", d.notes);
    }

    // ⑩ UNIQUE (MoW) with the human-confirmed timestamp column as key — the
    // MoW-accepted partition shape on a real time type (probed:
    // probe_a3.mow_ts_key — UNIQUE KEY(event_ts DATETIME(6)) PARTITION BY
    // RANGE(event_ts) + MoW + dynamic DAY creates fine). DECIMAL rides along
    // as a value column.
    #[test]
    fn snapshot_unique_mow_timestamp_key_partitioned() {
        let s = schema(
            "shop",
            "cdc_amounts",
            &[
                ("op_type", "utf8"),
                ("order_id", "utf8"),
                ("event_ts", "timestamp(us)"),
                ("amount", "decimal(18,2)"),
            ],
        );
        let hints = KeyHints {
            key_column: Some("event_ts".into()),
            ..KeyHints::default()
        };
        let d = generate_draft(&s, &hints).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `shop`.`cdc_amounts` (
    `event_ts` DATETIME(6),
    `op_type` VARCHAR(65533),
    `order_id` VARCHAR(65533),
    `amount` DECIMAL(18,2)
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
        assert!(d.partition.as_ref().is_some_and(|p| p.column == "event_ts"));
        assert!(d.notes.is_empty(), "{:?}", d.notes);
    }

    // ⑪ type-aware pick, the A3 version of ⑥e: among time-shaped names the
    // utf8 one is skipped (text about time, not time) and the real timestamp
    // column wins — and because a candidate was found, no skip note.
    #[test]
    fn partition_prefers_time_typed_column_over_text_named() {
        let s = schema(
            "m",
            "t",
            &[
                ("logged_at", "utf8"),
                ("ts", "timestamp(s)"),
                ("v", "int64"),
            ],
        );
        let d = generate_draft(&s, &KeyHints::default()).unwrap();
        assert!(d.ddl.contains("PARTITION BY RANGE(`ts`) ()"), "{}", d.ddl);
        assert!(!d.ddl.contains("logged_at`) ()"));
        assert_eq!(d.partition.as_ref().map(|p| p.column.as_str()), Some("ts"));
        assert!(d.notes.is_empty(), "{:?}", d.notes);
    }

    // ⑫ A4 — variant value column on a DUPLICATE table (probe A1 shape,
    // scripts/e2e/evidence/variant-probe-2026-10-30/, created on 2.1.7 and
    // 3.1.4 alike): `variant` lands VARIANT, no COMMENT convention (the
    // server parses the engine's string-shaped emission into the variant
    // structure, probe B2). The key heuristic rides `id` as before.
    #[test]
    fn snapshot_variant_value_column_duplicate() {
        let s = schema(
            "shop",
            "events",
            &[("id", "int64"), ("payload", "variant"), ("memo", "utf8")],
        );
        let d = generate_draft(&s, &KeyHints::default()).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `shop`.`events` (
    `id` BIGINT,
    `payload` VARIANT,
    `memo` VARCHAR(65533)
)
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS AUTO;"#
        );
        assert_eq!(d.key_model, KeyModel::Duplicate);
        assert_eq!(d.key_column, "id");
        assert!(d.partition.is_none());
        assert!(d.notes.is_empty(), "{:?}", d.notes);
    }

    // ⑬ A4 — variant value column under UNIQUE KEY merge-on-write (probe A2
    // shape; probe C: partial_columns updates of OTHER columns preserve the
    // variant value, updates of the variant column replace it whole). The
    // CDC op_type evidence picks UNIQUE, the variant column is a plain value
    // column next to a decimal.
    #[test]
    fn snapshot_variant_unique_mow_cdc() {
        let s = schema(
            "shop",
            "cdc_orders",
            &[
                ("op_type", "utf8"),
                ("order_id", "utf8"),
                ("attrs", "variant"),
                ("amount", "decimal(18,2)"),
            ],
        );
        let hints = KeyHints {
            replication_num: Some(1),
            ..KeyHints::default()
        };
        let d = generate_draft(&s, &hints).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `shop`.`cdc_orders` (
    `order_id` VARCHAR(65533),
    `op_type` VARCHAR(65533),
    `attrs` VARIANT,
    `amount` DECIMAL(18,2)
)
UNIQUE KEY(`order_id`)
DISTRIBUTED BY HASH(`order_id`) BUCKETS AUTO
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "replication_num" = "1"
);"#
        );
        assert_eq!(d.key_model, KeyModel::Unique);
        assert_eq!(d.key_column, "order_id");
        assert!(d.notes.is_empty(), "{:?}", d.notes);
    }

    // ⑬b A4 — a variant column never carries the key (FE refuses it, probe
    // A3): the heuristic steps over variant columns at both the `*_id` step
    // and the first-column fallback; an explicit variant pick and an
    // all-variant schema are explicit errors, not FE-refused drafts.
    #[test]
    fn variant_never_takes_the_key() {
        // heuristic: the leading variant column is skipped for the *_id one
        let d = generate_draft(
            &schema(
                "m",
                "t",
                &[("meta", "variant"), ("user_id", "int64"), ("v", "float64")],
            ),
            &KeyHints::default(),
        )
        .unwrap();
        assert_eq!(d.key_column, "user_id");
        assert!(d.key_column_guessed);
        assert!(d.ddl.starts_with(
            "CREATE TABLE IF NOT EXISTS `m`.`t` (\n    `user_id` BIGINT,\n    `meta` VARIANT,"
        ));
        // heuristic: no id-shaped column → first NON-variant column
        let d = generate_draft(
            &schema("m", "t", &[("meta", "variant"), ("region", "utf8")]),
            &KeyHints::default(),
        )
        .unwrap();
        assert_eq!(d.key_column, "region");
        // explicit variant key → error naming the type
        let e = generate_draft(
            &schema("m", "t", &[("id", "int64"), ("meta", "variant")]),
            &KeyHints {
                key_column: Some("meta".into()),
                ..KeyHints::default()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(
            e.contains("meta") && e.contains("variant") && e.contains("键列"),
            "{e}"
        );
        // all-variant schema: nothing can carry the key
        let e = generate_draft(&schema("m", "t", &[("v", "variant")]), &KeyHints::default())
            .unwrap_err()
            .to_string();
        assert!(e.contains("variant") && e.contains("键列"), "{e}");
    }

    // ⑬c A4 — a time-shaped NAME typed variant is text-about-time at best:
    // not a RANGE key (same path as the utf8 case ④), skip reason spells
    // the Doris type.
    #[test]
    fn variant_time_shaped_name_is_not_a_partition_key() {
        let d = generate_draft(
            &schema("m", "t", &[("id", "int64"), ("event_ts", "variant")]),
            &KeyHints::default(),
        )
        .unwrap();
        assert!(d.partition.is_none());
        assert_eq!(d.notes.len(), 1, "{:?}", d.notes);
        assert!(
            d.notes[0].contains("`event_ts`") && d.notes[0].contains("variant（VARIANT）"),
            "{}",
            d.notes[0]
        );
    }

    // ⑭ A5 — vector columns on a DUPLICATE table (vector probe A1 shape,
    // scripts/e2e/evidence/vector-probe-2026-10-30/, created on 3.1.4 with
    // the COMMENT kept verbatim by SHOW CREATE TABLE): the dimensioned word
    // lands ARRAY<FLOAT> with the dim as a column COMMENT (binary's
    // convention — Doris has no array dimension, probe B9: a 5-element array
    // loads into a dim=3 column, so the comment documents the contract and
    // the decoder's dim guard enforces it); the bare word lands a plain
    // ARRAY<FLOAT> (any length legal). The key heuristic rides `doc_id`.
    #[test]
    fn snapshot_vector_columns_duplicate() {
        let s = schema(
            "kb",
            "chunks",
            &[
                ("doc_id", "int64"),
                ("emb", "list<float32,768>"),
                ("scores", "list<float32>"),
            ],
        );
        let d = generate_draft(&s, &KeyHints::default()).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `kb`.`chunks` (
    `doc_id` BIGINT,
    `emb` ARRAY<FLOAT> COMMENT 'vector dim=768',
    `scores` ARRAY<FLOAT>
)
DUPLICATE KEY(`doc_id`)
DISTRIBUTED BY HASH(`doc_id`) BUCKETS AUTO;"#
        );
        assert_eq!(d.key_model, KeyModel::Duplicate);
        assert_eq!(d.key_column, "doc_id");
        assert!(d.partition.is_none());
        assert!(d.notes.is_empty(), "{:?}", d.notes);
    }

    // ⑮ A5 — a vector value column under UNIQUE KEY merge-on-write (vector
    // probe A2 shape, created on 3.1.4): CDC op_type evidence picks UNIQUE,
    // the vector column is a plain value column next to a scalar, and the
    // single-node replication downgrade rides along.
    #[test]
    fn snapshot_vector_unique_mow_cdc() {
        let s = schema(
            "kb",
            "cdc_chunks",
            &[
                ("op_type", "utf8"),
                ("chunk_id", "utf8"),
                ("emb", "list<float32,3>"),
                ("score", "float64"),
            ],
        );
        let hints = KeyHints {
            replication_num: Some(1),
            ..KeyHints::default()
        };
        let d = generate_draft(&s, &hints).unwrap();
        assert_eq!(
            d.ddl,
            r#"CREATE TABLE IF NOT EXISTS `kb`.`cdc_chunks` (
    `chunk_id` VARCHAR(65533),
    `op_type` VARCHAR(65533),
    `emb` ARRAY<FLOAT> COMMENT 'vector dim=3',
    `score` DOUBLE
)
UNIQUE KEY(`chunk_id`)
DISTRIBUTED BY HASH(`chunk_id`) BUCKETS AUTO
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "replication_num" = "1"
);"#
        );
        assert_eq!(d.key_model, KeyModel::Unique);
        assert_eq!(d.key_column, "chunk_id");
        assert!(d.notes.is_empty(), "{:?}", d.notes);
    }

    // ⑮b A5 — a vector column never carries the key (FE refuses ARRAY as a
    // key column, vector probe A3): the heuristic steps over list columns
    // at both the `*_id` step and the first-column fallback; an explicit
    // list pick and an all-list schema are explicit errors, not
    // FE-refused drafts.
    #[test]
    fn vector_column_never_takes_the_key() {
        // heuristic: the leading list column is skipped for the *_id one
        let d = generate_draft(
            &schema(
                "m",
                "t",
                &[
                    ("emb", "list<float32,3>"),
                    ("doc_id", "int64"),
                    ("v", "float64"),
                ],
            ),
            &KeyHints::default(),
        )
        .unwrap();
        assert_eq!(d.key_column, "doc_id");
        assert!(d.key_column_guessed);
        assert!(
            d.ddl
                .starts_with("CREATE TABLE IF NOT EXISTS `m`.`t` (\n    `doc_id` BIGINT,\n    `emb` ARRAY<FLOAT>"),
            "{}",
            d.ddl
        );
        // heuristic: no id-shaped column → first NON-list column
        let d = generate_draft(
            &schema("m", "t", &[("emb", "list<float32>"), ("region", "utf8")]),
            &KeyHints::default(),
        )
        .unwrap();
        assert_eq!(d.key_column, "region");
        // explicit list key → error naming the type
        let e = generate_draft(
            &schema("m", "t", &[("id", "int64"), ("emb", "list<float32,3>")]),
            &KeyHints {
                key_column: Some("emb".into()),
                ..KeyHints::default()
            },
        )
        .unwrap_err()
        .to_string();
        assert!(
            e.contains("emb") && e.contains("list<float32,3>") && e.contains("ARRAY 作键列"),
            "{e}"
        );
        // all-list schema: nothing can carry the key
        let e = generate_draft(
            &schema("m", "t", &[("v", "list<float32>")]),
            &KeyHints::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            e.contains("list<float32>") && e.contains("ARRAY 作键列"),
            "{e}"
        );
    }

    // ⑮c A5 — a time-shaped NAME typed list is not a RANGE key either
    // (FE: "Complex type column can't be partition column: ARRAY<FLOAT>",
    // vector probe A4): same skip path as the utf8 case ④, reason spells
    // the Doris type.
    #[test]
    fn vector_time_shaped_name_is_not_a_partition_key() {
        let d = generate_draft(
            &schema(
                "m",
                "t",
                &[("id", "int64"), ("event_ts", "list<float32,2>")],
            ),
            &KeyHints::default(),
        )
        .unwrap();
        assert!(d.partition.is_none());
        assert_eq!(d.notes.len(), 1, "{:?}", d.notes);
        assert!(
            d.notes[0].contains("`event_ts`")
                && d.notes[0].contains("list<float32,2>（ARRAY<FLOAT> COMMENT 'vector dim=2'）"),
            "{}",
            d.notes[0]
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
        // outside the generator's vocabulary — explicit, no silent fold.
        // Two distinct refusals: a word the 16-vocabulary itself rejects
        // (external spelling), and a legal word whose Doris mapping is not
        // open (null has no Doris column type — variant opened in A4,
        // list<float32> in A5).
        let e = generate_draft(
            &schema("db", "t", &[("id", "int64"), ("name", "varchar(64)")]),
            &KeyHints::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            e.contains("varchar(64)") && e.contains("不在 DDL 生成器支持面内"),
            "{e}"
        );
        // A5: malformed list forms are refused like any external spelling —
        // wrong element, dim 0, missing dim, bare word, unclosed — the same
        // contract `ColumnType::parse` enforces (the vector probe B9 shows
        // Doris would not catch a bad dimension for us)
        for bad in [
            "list<int64>",
            "list<float32,0>",
            "list<float32,>",
            "list<float32,-1>",
            "list<float32,1.5>",
            "list<float32,3,4>",
            "list",
            "list<float32",
            "list<>",
        ] {
            let e = generate_draft(
                &schema("db", "t", &[("id", "int64"), ("emb", bad)]),
                &KeyHints::default(),
            )
            .unwrap_err()
            .to_string();
            assert!(
                e.contains(bad) && e.contains("不在 DDL 生成器支持面内"),
                "{bad:?}: {e}"
            );
        }
        let e = generate_draft(
            &schema("db", "t", &[("id", "int64"), ("nothing", "null")]),
            &KeyHints::default(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            e.contains("\"null\"") && e.contains("无 Doris 对应列类型"),
            "{e}"
        );
        // parametric bounds mirror the vocabulary contract (p ≤ 38, s ≤ p)
        assert!(generate_draft(
            &schema("db", "t", &[("id", "int64"), ("amt", "decimal(39,1)")]),
            &KeyHints::default(),
        )
        .is_err());
        assert!(generate_draft(
            &schema("db", "t", &[("id", "int64"), ("amt", "decimal(18,19)")]),
            &KeyHints::default(),
        )
        .is_err());
        // a bare / malformed timestamp word is not a vocabulary word either
        assert!(generate_draft(
            &schema("db", "t", &[("id", "int64"), ("ts", "timestamp")]),
            &KeyHints::default(),
        )
        .is_err());
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
