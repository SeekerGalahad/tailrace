//! Column type vocabulary (G2, blueprint §7.1): the 16-word wire contract for
//! `TableObject.columns` type strings — 4 legacy words kept byte-compatible
//! (`utf8` / `int64` / `float64` / `bool`) plus 12 new ones
//! (`string` / `int8` / `int16` / `int32` / `float32` / `decimal(p,s)` /
//! `timestamp(unit)` / `date` / `binary` / `list<float32>` / `variant` /
//! `null`).
//!
//! Canonical home is tailrace-ir (the wire-contract crate); tailrace-catalog
//! re-exports it as the stable public path `tailrace_catalog::ColumnType`
//! (blueprint G2: "arrow_type 迁至 tailrace-catalog::ColumnType::parse").
//! The reverse placement is impossible without a dependency cycle: catalog
//! already depends on tailrace-ir, so ir cannot depend on catalog.
//!
//! Unknown vocabulary: strict `ColumnType::parse` errors; the two legacy
//! `arrow_type` call sites fold through [`arrow_type_or_utf8`] instead — a
//! one-minor-version migration window (blueprint §7.6) that warns (column
//! name + the known vocabulary) and folds to Utf8 exactly like the pre-G2
//! `_ => DataType::Utf8` arm did. The window const flips to `false` in the
//! next minor, which turns the fold into a hard error propagated by `?` at
//! pipeline assembly.

use std::fmt;

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};

/// The 16-word column type vocabulary (blueprint §7.1 table, one variant per
/// row). Parameterized words carry their parameters structurally so a
/// parse→Display round trip is the canonical spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    /// `utf8` (legacy) — Arrow `Utf8`
    Utf8,
    /// `string` (new) — Arrow `LargeUtf8`; Doris STRING, never a key column
    String,
    Int8,
    Int16,
    Int32,
    /// `int64` (legacy) — Arrow `Int64`
    Int64,
    Float32,
    /// `float64` (legacy) — Arrow `Float64`
    Float64,
    /// `decimal(p,s)` (new) — Arrow `Decimal128(p,s)`; p ≤ 38 (blueprint)
    /// and s ≤ p (arrow Decimal128 validity), Doris DECIMAL(p,s)
    Decimal {
        precision: u8,
        scale: u8,
    },
    /// `timestamp(unit)` (new) — Arrow `Timestamp(unit, None)`; Doris has no
    /// timezone so every unit lands DATETIME(6) on the DDL side (W3/A3)
    Timestamp(TimeUnit),
    /// `date` (new) — Arrow `Date32`
    Date,
    /// `bool` (legacy) — Arrow `Boolean`
    Boolean,
    /// `binary` (new) — Arrow `Binary`; Doris STRING via base64 convention
    Binary,
    /// `list<float32>` / `list<float32,dim>` (new) — Arrow `List(Float32)`.
    /// The optional dimension is a declaration-only annotation (vector
    /// columns, Doris ARRAY<FLOAT> on the DDL side): arrow `List` cannot
    /// carry it, so `to_arrow` drops it while Display keeps it round-trip
    /// stable.
    ListFloat32 {
        dim: Option<u32>,
    },
    /// `variant` (new) — Arrow `Utf8`: the raw JSON text passes through
    /// unfolded and Doris VARIANT infers the sub-columns (blueprint §7.1
    /// note 4). A4: the decoder writes a nested JSON value back as its
    /// serialized text (not NULL) when the field carries
    /// [`VARIANT_FIELD_METADATA`] — see [`arrow_field_or_utf8`]; the sink's
    /// ordinary string escaping is the right wire shape, because Doris
    /// parses a JSON string literal into the variant structure (probed on
    /// 2.1.7 and 3.1.4, scripts/e2e/evidence/variant-probe-2026-10-30/).
    Variant,
    /// `null` (new) — Arrow `Null`: an all-null column typed explicitly
    /// instead of silently falling to Utf8
    Null,
}

impl ColumnType {
    /// Parse one vocabulary word. Strict: unknown or malformed input is an
    /// error (the pre-G2 behavior folded silently to Utf8 — that fold now
    /// lives only in [`arrow_type_or_utf8`], behind the migration window).
    ///
    /// Vocabulary is lowercase-exact (a wire contract has one canonical
    /// spelling; accepting `UTF8` would make the round trip non-unique).
    /// Whitespace inside the parametric forms is tolerated
    /// (`decimal(18, 2)`), the canonical Display form has none.
    pub fn parse(vocab: &str) -> anyhow::Result<Self> {
        Parser::new(vocab).parse_type()
    }

    /// Map to the arrow DataType (blueprint §7.1 mapping column).
    pub fn to_arrow(&self) -> DataType {
        match self {
            ColumnType::Utf8 | ColumnType::Variant => DataType::Utf8,
            ColumnType::String => DataType::LargeUtf8,
            ColumnType::Int8 => DataType::Int8,
            ColumnType::Int16 => DataType::Int16,
            ColumnType::Int32 => DataType::Int32,
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Float32 => DataType::Float32,
            ColumnType::Float64 => DataType::Float64,
            // arrow's Decimal128 carries scale as i8 (arrow-rs signature);
            // the vocabulary guarantees 0..=38 which is lossless in i8
            ColumnType::Decimal { precision, scale } => {
                DataType::Decimal128(*precision, *scale as i8)
            }
            ColumnType::Timestamp(unit) => DataType::Timestamp(*unit, None),
            ColumnType::Date => DataType::Date32,
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::Binary => DataType::Binary,
            ColumnType::ListFloat32 { .. } => DataType::List(std::sync::Arc::new(Field::new(
                "item",
                DataType::Float32,
                true,
            ))),
            ColumnType::Null => DataType::Null,
        }
    }
}

/// Canonical spelling (the round-trip anchor: `parse(t)?.to_string() == t`
/// for every canonical input).
impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnType::Utf8 => f.write_str("utf8"),
            ColumnType::String => f.write_str("string"),
            ColumnType::Int8 => f.write_str("int8"),
            ColumnType::Int16 => f.write_str("int16"),
            ColumnType::Int32 => f.write_str("int32"),
            ColumnType::Int64 => f.write_str("int64"),
            ColumnType::Float32 => f.write_str("float32"),
            ColumnType::Float64 => f.write_str("float64"),
            ColumnType::Decimal { precision, scale } => {
                write!(f, "decimal({precision},{scale})")
            }
            ColumnType::Timestamp(unit) => {
                let word = match unit {
                    TimeUnit::Second => "s",
                    TimeUnit::Millisecond => "ms",
                    TimeUnit::Microsecond => "us",
                    TimeUnit::Nanosecond => "ns",
                };
                write!(f, "timestamp({word})")
            }
            ColumnType::Date => f.write_str("date"),
            ColumnType::Boolean => f.write_str("bool"),
            ColumnType::Binary => f.write_str("binary"),
            ColumnType::ListFloat32 { dim: None } => f.write_str("list<float32>"),
            ColumnType::ListFloat32 { dim: Some(d) } => write!(f, "list<float32,{d}>"),
            ColumnType::Variant => f.write_str("variant"),
            ColumnType::Null => f.write_str("null"),
        }
    }
}

/// Migration window for unknown vocabulary (blueprint §7.6: one minor
/// version of warn-then-fold, then a hard error). Flipping this to `false`
/// turns [`arrow_type_or_utf8`]'s fold arm into the error pass-through —
/// callers already `?` the Result, so the flip itself is the whole change.
pub const UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW: bool = true;

/// The schema-construction entry the two legacy `arrow_type` sites migrated
/// to: strict parse, arrow mapping, and the migration-window fold for
/// unknown words — warn (column name + known vocabulary, so the operator can
/// fix the declaration) then Utf8, byte-identical to the pre-G2 fallback arm.
pub fn arrow_type_or_utf8(column: &str, vocab: &str) -> anyhow::Result<DataType> {
    match ColumnType::parse(vocab) {
        Ok(ct) => Ok(ct.to_arrow()),
        Err(err) if UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW => {
            tracing::warn!(
                column,
                vocabulary = vocab,
                %err,
                known = KNOWN_VOCABULARY,
                "unknown column type vocabulary folds to utf8 (G2 migration window, hard error next minor)"
            );
            Ok(DataType::Utf8)
        }
        Err(err) => Err(err.context(format!(
            "column {column}: unknown type vocabulary {vocab:?} (known: {KNOWN_VOCABULARY})"
        ))),
    }
}

/// The 16-word list embedded in every unknown-type diagnostic.
pub const KNOWN_VOCABULARY: &str = "utf8, string, int8, int16, int32, int64, float32, float64, \
     decimal(p,s), timestamp(s|ms|us|ns), date, bool, binary, list<float32[,dim]>, variant, null";

/// Arrow `Field` metadata key marking a `variant` column (A4). `variant`
/// and `utf8` share the arrow type (`Utf8` — the raw JSON text rides as a
/// string, see [`ColumnType::to_arrow`]), so the arrow `DataType` alone
/// cannot tell the decoder which Utf8 columns must take a nested JSON
/// value as its serialized text instead of NULL. The field metadata is
/// the arrow-native per-column annotation for exactly that; value is the
/// literal `"true"`. Every schema the engine builds from the vocabulary
/// goes through [`arrow_field_or_utf8`] so the plan's anchored `src` schema
/// and the decode `ColBatch` schema carry identical metadata (the
/// micro-batch injection refuses a schema that differs from the anchor,
/// metadata included).
pub const VARIANT_FIELD_METADATA: &str = "tailrace.variant";

/// The schema-construction entry for one declared column: [`arrow_type_or_utf8`]
/// for the data type (same migration-window fold for unknown words),
/// nullable, plus the [`VARIANT_FIELD_METADATA`] marker when the word is
/// `variant`. The two legacy `Field::new(name, arrow_type(..), true)`
/// sites (tailrace-ir plan building, tailrace-runtime assembly) migrated
/// here so the marker cannot be present on one side and absent on the
/// other.
pub fn arrow_field_or_utf8(column: &str, vocab: &str) -> anyhow::Result<Field> {
    let field = Field::new(column, arrow_type_or_utf8(column, vocab)?, true);
    Ok(
        if matches!(ColumnType::parse(vocab), Ok(ColumnType::Variant)) {
            field.with_metadata(std::collections::HashMap::from([(
                VARIANT_FIELD_METADATA.to_string(),
                "true".to_string(),
            )]))
        } else {
            field
        },
    )
}

/// Whether a field carries the `variant` marker (decoder-side read of
/// [`VARIANT_FIELD_METADATA`]). Cheap on the hot path: a non-variant field
/// has an empty metadata map and short-circuits on `is_empty`.
pub fn is_variant_field(field: &Field) -> bool {
    let md = field.metadata();
    !md.is_empty() && md.get(VARIANT_FIELD_METADATA).map(String::as_str) == Some("true")
}

// ==================== recursive-descent parser (zero dependencies) ====================
//
// Grammar (blueprint §7.1; ~120 lines, hand-rolled — no nom/regex):
//
//   type   := word | "decimal" "(" INT "," INT ")"
//           | "timestamp" "(" unit ")" | "list" "<" "float32" [ "," INT ] ">"
//   unit   := "s" | "ms" | "us" | "ns"
//
// Constraints checked at the grammar actions: 1 ≤ p ≤ 38, 0 ≤ s ≤ p
// (Decimal128), dim ≥ 1. Input must be fully consumed (no trailing junk).

struct Parser<'a> {
    src: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src,
            bytes: src.as_bytes(),
            pos: 0,
        }
    }

    fn skip_ws(&mut self) {
        while let Some(b) = self.bytes.get(self.pos) {
            if b.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    /// Consume one exact punctuation byte or fail.
    fn expect(&mut self, ch: u8) -> anyhow::Result<()> {
        self.skip_ws();
        match self.bytes.get(self.pos) {
            Some(&b) if b == ch => {
                self.pos += 1;
                Ok(())
            }
            _ => anyhow::bail!(
                "expected {:?} at byte {} in type {src:?}",
                ch as char,
                self.pos,
                src = self.src
            ),
        }
    }

    /// Consume one run of [a-z0-9] and return it verbatim (vocabulary words
    /// and units are lowercase; digits only appear inside int literals taken
    /// by `uint`, so ident here never needs a leading digit).
    fn ident(&mut self) -> anyhow::Result<&'a str> {
        self.skip_ws();
        let start = self.pos;
        while let Some(&b) = self.bytes.get(self.pos) {
            if b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' {
                self.pos += 1;
            } else {
                break;
            }
        }
        if start == self.pos {
            anyhow::bail!(
                "expected a type word at byte {start} in type {src:?}",
                src = self.src
            );
        }
        Ok(&self.src[start..self.pos])
    }

    /// Consume one unsigned integer (decimal digits only; a leading `+`/`-`
    /// or a float literal `1.5` is not a valid dimension/precision).
    fn uint(&mut self) -> anyhow::Result<u64> {
        self.skip_ws();
        let start = self.pos;
        while let Some(&b) = self.bytes.get(self.pos) {
            if b.is_ascii_digit() {
                self.pos += 1;
            } else {
                break;
            }
        }
        if start == self.pos {
            anyhow::bail!(
                "expected a number at byte {start} in type {src:?}",
                src = self.src
            );
        }
        self.src[start..self.pos]
            .parse()
            .map_err(|e| anyhow::anyhow!("number out of range in type {:?}: {e}", self.src))
    }

    fn parse_type(&mut self) -> anyhow::Result<ColumnType> {
        let word = self.ident()?;
        let ty = match word {
            "utf8" => ColumnType::Utf8,
            "string" => ColumnType::String,
            "int8" => ColumnType::Int8,
            "int16" => ColumnType::Int16,
            "int32" => ColumnType::Int32,
            "int64" => ColumnType::Int64,
            "float32" => ColumnType::Float32,
            "float64" => ColumnType::Float64,
            "bool" => ColumnType::Boolean,
            "date" => ColumnType::Date,
            "binary" => ColumnType::Binary,
            "variant" => ColumnType::Variant,
            "null" => ColumnType::Null,
            "decimal" => {
                self.expect(b'(')?;
                let precision = self.uint()?;
                self.expect(b',')?;
                let scale = self.uint()?;
                self.expect(b')')?;
                // blueprint §7.1: p ≤ 38 (Decimal128); arrow additionally
                // requires 1 ≤ p and s ≤ p for a valid Decimal128(p,s)
                if !(1..=38).contains(&precision) {
                    anyhow::bail!("decimal precision must be 1..=38 (got {precision})");
                }
                if scale > precision {
                    anyhow::bail!("decimal scale {scale} exceeds precision {precision}");
                }
                ColumnType::Decimal {
                    precision: precision as u8,
                    scale: scale as u8,
                }
            }
            "timestamp" => {
                self.expect(b'(')?;
                let unit = match self.ident()? {
                    "s" => TimeUnit::Second,
                    "ms" => TimeUnit::Millisecond,
                    "us" => TimeUnit::Microsecond,
                    "ns" => TimeUnit::Nanosecond,
                    other => {
                        anyhow::bail!("timestamp unit must be one of s|ms|us|ns (got {other:?})")
                    }
                };
                self.expect(b')')?;
                ColumnType::Timestamp(unit)
            }
            "list" => {
                self.expect(b'<')?;
                let elem = self.ident()?;
                if elem != "float32" {
                    anyhow::bail!("list element must be float32 (got {elem:?})");
                }
                let dim = if self.bytes.get(self.pos) == Some(&b',') {
                    self.expect(b',')?;
                    let d = self.uint()?;
                    if d == 0 {
                        anyhow::bail!("list dimension must be ≥ 1 (got 0)");
                    }
                    Some(d as u32)
                } else {
                    None
                };
                self.expect(b'>')?;
                ColumnType::ListFloat32 { dim }
            }
            other => {
                anyhow::bail!("unknown type word {other:?} (known vocabulary: {KNOWN_VOCABULARY})")
            }
        };
        self.skip_ws();
        if self.pos != self.bytes.len() {
            anyhow::bail!(
                "unexpected trailing input {:?} in type {src:?}",
                &self.src[self.pos..],
                src = self.src
            );
        }
        Ok(ty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// G2 hard acceptance (blueprint §7.7): every one of the 16 words parses
    /// and Display-round-trips to the canonical spelling. Parameterized
    /// words appear in their canonical and whitespace-tolerated forms.
    #[test]
    fn sixteen_word_roundtrip() {
        let cases = [
            ("utf8", ColumnType::Utf8),
            ("string", ColumnType::String),
            ("int8", ColumnType::Int8),
            ("int16", ColumnType::Int16),
            ("int32", ColumnType::Int32),
            ("int64", ColumnType::Int64),
            ("float32", ColumnType::Float32),
            ("float64", ColumnType::Float64),
            (
                "decimal(18,2)",
                ColumnType::Decimal {
                    precision: 18,
                    scale: 2,
                },
            ),
            (
                "timestamp(us)",
                ColumnType::Timestamp(TimeUnit::Microsecond),
            ),
            ("date", ColumnType::Date),
            ("bool", ColumnType::Boolean),
            ("binary", ColumnType::Binary),
            ("list<float32>", ColumnType::ListFloat32 { dim: None }),
            (
                "list<float32,768>",
                ColumnType::ListFloat32 { dim: Some(768) },
            ),
            ("variant", ColumnType::Variant),
            ("null", ColumnType::Null),
        ];
        for (word, expect) in cases {
            let parsed = ColumnType::parse(word).unwrap_or_else(|e| panic!("{word:?}: {e}"));
            assert_eq!(parsed, expect, "{word:?}");
            assert_eq!(parsed.to_string(), word, "Display must be canonical");
        }
    }

    /// Parameterized boundaries (blueprint §7.1: p ≤ 38; arrow Decimal128:
    /// s ≤ p; every timestamp unit; list dimension).
    #[test]
    fn parametric_boundaries() {
        // decimal corners
        assert_eq!(
            ColumnType::parse("decimal(38,38)").unwrap(),
            ColumnType::Decimal {
                precision: 38,
                scale: 38
            }
        );
        assert_eq!(
            ColumnType::parse("decimal(1,0)").unwrap(),
            ColumnType::Decimal {
                precision: 1,
                scale: 0
            }
        );
        assert!(ColumnType::parse("decimal(39,0)").is_err(), "p > 38");
        assert!(ColumnType::parse("decimal(0,0)").is_err(), "p < 1");
        assert!(
            ColumnType::parse("decimal(18,19)").is_err(),
            "scale > precision"
        );
        // whitespace inside the parametric form is tolerated
        assert_eq!(
            ColumnType::parse("decimal( 18 , 2 )").unwrap(),
            ColumnType::Decimal {
                precision: 18,
                scale: 2
            }
        );
        // all four timestamp units round-trip
        for (word, unit) in [
            ("s", TimeUnit::Second),
            ("ms", TimeUnit::Millisecond),
            ("us", TimeUnit::Microsecond),
            ("ns", TimeUnit::Nanosecond),
        ] {
            let w = format!("timestamp({word})");
            assert_eq!(ColumnType::parse(&w).unwrap(), ColumnType::Timestamp(unit));
            assert_eq!(ColumnType::parse(&w).unwrap().to_string(), w);
        }
        // list dimension: bare and declared
        assert_eq!(
            ColumnType::parse("list<float32,1>").unwrap(),
            ColumnType::ListFloat32 { dim: Some(1) }
        );
        assert!(ColumnType::parse("list<float32,0>").is_err(), "dim 0");
    }

    /// G2 hard acceptance: illegal inputs error (never a silent Utf8 fold).
    #[test]
    fn illegal_inputs_error() {
        let bad = [
            "",
            "   ",
            "utf8x",            // prefix of a word is not the word
            "UTF8",             // vocabulary is lowercase-exact
            "varchar",          // external word, not in the contract
            "int 64",           // split word
            "decimal(18)",      // missing scale
            "decimal(18,)",     // missing scale number
            "decimal(18",       // unclosed
            "decimal",          // bare parameterized word
            "decimal(-1,2)",    // negative precision is not a uint
            "decimal(18,2.5)",  // float scale is not a uint
            "timestamp",        // bare
            "timestamp(x)",     // unknown unit
            "timestamp(us",     // unclosed
            "date(1)",          // date takes no parameters
            "list",             // bare
            "list<int64>",      // element must be float32
            "list<float32",     // unclosed
            "list<float32,",    // missing dimension
            "list<float32,768", // unclosed
            "int64 junk",       // trailing input
            "decimal(18,2) x",  // trailing input after a valid word
            "nulll",            // typo
        ];
        for word in bad {
            assert!(ColumnType::parse(word).is_err(), "{word:?} must not parse");
        }
    }

    /// Legacy regression anchor: the 4 pre-G2 words map to exactly the arrow
    /// types the old `arrow_type` match arms produced (byte-for-byte — this
    /// is what "旧词汇语义不变" pins).
    #[test]
    fn legacy_four_words_arrow_anchor() {
        assert_eq!(
            arrow_type_or_utf8("c", "utf8").unwrap(),
            DataType::Utf8,
            "utf8 went through the pre-G2 fallback arm and still lands Utf8"
        );
        assert_eq!(arrow_type_or_utf8("c", "int64").unwrap(), DataType::Int64);
        assert_eq!(
            arrow_type_or_utf8("c", "float64").unwrap(),
            DataType::Float64
        );
        assert_eq!(arrow_type_or_utf8("c", "bool").unwrap(), DataType::Boolean);
    }

    /// A4: the field-construction entry marks exactly the `variant` word.
    /// utf8 — the same arrow type — carries no metadata (byte-identical to
    /// the pre-A4 `Field::new(name, type, true)`), so schemas without a
    /// variant column are unchanged; the unknown-word fold lands a plain
    /// Utf8 field too (the marker is for the declared word, never a guess).
    #[test]
    fn variant_field_marker() {
        let v = arrow_field_or_utf8("attrs", "variant").unwrap();
        assert_eq!(v.name(), "attrs");
        assert_eq!(v.data_type(), &DataType::Utf8);
        assert!(v.is_nullable());
        assert!(is_variant_field(&v));
        assert_eq!(
            v.metadata().get(VARIANT_FIELD_METADATA).map(String::as_str),
            Some("true")
        );
        for word in ["utf8", "string", "int64", "varchar(64)"] {
            let f = arrow_field_or_utf8("c", word).unwrap();
            assert!(!is_variant_field(&f), "{word} must not carry the marker");
            assert!(f.metadata().is_empty(), "{word} field keeps empty metadata");
        }
        assert_eq!(
            arrow_field_or_utf8("c", "utf8").unwrap(),
            Field::new("c", DataType::Utf8, true),
            "non-variant fields are byte-identical to the legacy construction"
        );
    }

    /// Blueprint §7.1 arrow mapping column, all 16 words.
    #[test]
    fn arrow_mapping_table() {
        let f32_list = DataType::List(std::sync::Arc::new(Field::new(
            "item",
            DataType::Float32,
            true,
        )));
        let cases = [
            (ColumnType::Utf8, DataType::Utf8),
            (ColumnType::String, DataType::LargeUtf8),
            (ColumnType::Int8, DataType::Int8),
            (ColumnType::Int16, DataType::Int16),
            (ColumnType::Int32, DataType::Int32),
            (ColumnType::Int64, DataType::Int64),
            (ColumnType::Float32, DataType::Float32),
            (ColumnType::Float64, DataType::Float64),
            (
                ColumnType::Decimal {
                    precision: 18,
                    scale: 2,
                },
                DataType::Decimal128(18, 2),
            ),
            (
                ColumnType::Timestamp(TimeUnit::Microsecond),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (ColumnType::Date, DataType::Date32),
            (ColumnType::Boolean, DataType::Boolean),
            (ColumnType::Binary, DataType::Binary),
            (ColumnType::ListFloat32 { dim: None }, f32_list.clone()),
            // the dimension is declaration-only: arrow List drops it
            (ColumnType::ListFloat32 { dim: Some(768) }, f32_list),
            (ColumnType::Variant, DataType::Utf8),
            (ColumnType::Null, DataType::Null),
        ];
        for (ct, expect) in cases {
            assert_eq!(ct.to_arrow(), expect, "{ct}");
        }
    }

    /// Migration window (blueprint §7.6): the fold behavior and the window
    /// const must agree — while open, an unknown word warns and lands Utf8
    /// (behavior-identical to the pre-G2 fallback, only the warn is new);
    /// once flipped false, the very same call errors. Both phases are
    /// pinned, so flipping the const turns this test into the hard-error
    /// guard without any edit.
    #[test]
    fn migration_window_folds_unknown_to_utf8() {
        if UNKNOWN_TYPE_UTF8_MIGRATION_WINDOW {
            assert_eq!(
                arrow_type_or_utf8("amount", "varchar(64)").unwrap(),
                DataType::Utf8,
                "unknown words fold to Utf8 while the migration window is open"
            );
        } else {
            assert!(
                arrow_type_or_utf8("amount", "varchar(64)").is_err(),
                "window closed — unknown words must hard-error at assembly"
            );
        }
    }

    /// Review fix (R8 acceptance "迁移窗 warn 有日志样例"): the migration
    /// window's warn actually REACHES a tracing subscriber — install a
    /// thread-default fmt subscriber over a captured buffer, fold one
    /// unknown word, and assert the WARN line carries the message, the
    /// column name and the offending vocabulary. This is the runnable,
    /// regression-proof form of the acceptance's "log sample" (no runtime
    /// artifact is committed).
    #[test]
    fn migration_window_warn_actually_logs() {
        use std::io::Write;
        use std::sync::{Arc, Mutex};
        use tracing_subscriber::util::SubscriberInitExt as _;

        #[derive(Clone)]
        struct Capture(Arc<Mutex<Vec<u8>>>);
        impl Write for Capture {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().write(buf)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
            type Writer = Capture;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let buf = Capture(Arc::new(Mutex::new(Vec::new())));
        let guard = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_target(false)
            .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
            .set_default();
        let folded = arrow_type_or_utf8("amount", "varchar(64)").unwrap();
        drop(guard);
        assert_eq!(folded, DataType::Utf8);
        let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(text.contains("WARN"), "expected a WARN event, got: {text}");
        assert!(
            text.contains("unknown column type vocabulary folds to utf8"),
            "the migration-window message must reach the subscriber, got: {text}"
        );
        assert!(
            text.contains("amount") && text.contains("varchar(64)"),
            "column name + offending vocabulary ride the event, got: {text}"
        );
    }
}
