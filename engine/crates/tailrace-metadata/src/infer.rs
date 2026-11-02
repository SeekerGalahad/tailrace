//! Pure inference: JSON samples → columns (mirrors the decode chain's typed
//! extraction) and DDL type names → the engine's 16-word type vocabulary.
//!
//! JSON-family inference (ogg / json / jsonl / debezium-json) stays on the
//! four evidence-bearing scalar words — a JSON sample carries no width or
//! precision, so `int64`/`float64` are the honest integer/float picks and
//! anything wider would be invented — plus `variant` for a field whose
//! non-null samples are all nested (object / array): since A4 the decoder
//! passes such a value through as JSON text and Doris VARIANT infers the
//! sub-columns, so the whole sub-document is carried instead of NULLed
//! (the pre-A4 `utf8` pick kept the column visible but every nested value
//! decoded NULL). A field that mixes nested and scalar samples still lands
//! `utf8` — the lossy-but-honest carrier for a column of mixed shapes.
//! DDL type names (MySQL / Doris `DESC`,
//! PG `information_schema`) DO carry width/precision, and A3 narrowed the
//! fold: `tinyint` keeps its width as `int8`, `decimal(18,2)` keeps its
//! parameters, `datetime`/`date` land `timestamp(us)`/`date` — before A3
//! all of those folded to `int64`/`utf8`, which made evolve's registered-vs-
//! live diff deaf to real type differences on them (console-api 裁决 8:
//! 「G2 扩词汇后收窄」). Doris DESC prints a `STRING` column back as `text`
//! (live-probed 2.1.7, 2026-10-30), so `text` maps to `string` — that keeps
//! the draft→DESC round trip stable for evolve; likewise a VARIANT column
//! prints back as `variant` (probed 2.1.7 and 3.1.4) and maps to `variant`,
//! and an ARRAY<FLOAT> column prints back as `array<float>` (A5, probed
//! 3.1.4) and maps to the bare `list<float32>`. JSON-sample inference does
//! NOT pick `list<float32>` on its own — a numeric array in a sample is
//! `variant` like any array: whether it is a vector (and of what dimension)
//! is the operator's declaration, not a guess from samples.

use std::collections::HashMap;

use serde_json::Value;

use crate::InferredSchema;

/// Value-level type lattice. Merge widens: Null absorbs into anything,
/// Int+Float → Float, Nested+Nested stays Nested, any other mix → Str (a
/// column of mixed shapes is only safely carried as text — the
/// JsonDecoder's `as_str` arm then takes string values and NULLs the rest,
/// which is the lossy-but-honest option).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ty {
    Null,
    Bool,
    Int,
    Float,
    Str,
    /// Object or array (A4): the decoder carries it whole as JSON text into
    /// a `variant` column; Doris VARIANT infers the sub-columns.
    Nested,
}

impl Ty {
    fn of(v: &Value) -> Ty {
        match v {
            Value::Null => Ty::Null,
            Value::Bool(_) => Ty::Bool,
            Value::Number(n) if n.is_i64() || n.is_u64() => Ty::Int,
            Value::Number(_) => Ty::Float,
            Value::String(_) => Ty::Str,
            Value::Array(_) | Value::Object(_) => Ty::Nested,
        }
    }

    fn merge(self, other: Ty) -> Ty {
        match (self, other) {
            (a, b) if a == b => a,
            (Ty::Null, x) | (x, Ty::Null) => x,
            (Ty::Int, Ty::Float) | (Ty::Float, Ty::Int) => Ty::Float,
            _ => Ty::Str,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Ty::Bool => "bool",
            Ty::Int => "int64",
            Ty::Float => "float64",
            Ty::Nested => "variant",
            // a never-non-null column has no evidence → the runtime default
            Ty::Null | Ty::Str => "utf8",
        }
    }
}

/// Observed columns with merged types; emitted sorted by name (a sample's
/// keys already arrive alphabetically from serde_json's BTreeMap, and
/// sorting the union keeps the result independent of message order).
#[derive(Default)]
struct Columns {
    types: HashMap<String, Ty>,
}

impl Columns {
    fn observe(&mut self, obj: &serde_json::Map<String, Value>) {
        for (k, v) in obj {
            let t = Ty::of(v);
            self.types
                .entry(k.clone())
                .and_modify(|existing| *existing = existing.merge(t))
                .or_insert(t);
        }
    }

    fn into_pairs(self) -> Vec<(String, String)> {
        let mut pairs: Vec<(String, String)> = self
            .types
            .into_iter()
            .map(|(k, t)| (k, t.name().to_string()))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs
    }
}

/// OGG envelope columns the OggDecoder emits first, in its order
/// (`op_type` / `ogg_table` / `ogg_ts`), before `after.*`.
const OGG_ENVELOPE: [(&str, &str); 3] = [
    ("op_type", "utf8"),
    ("ogg_table", "utf8"),
    ("ogg_ts", "int64"),
];

/// The three JSON-family sample shapes inference walks (A3 added
/// debezium-json — the decode chain's fourth format finally gets its
/// console discover counterpart, the Debezium decoder's own doc note).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fmt {
    Ogg,
    Json,
    Debezium,
}

/// Infer from raw JSON samples. `format` = explicit `ogg` / `json` /
/// `jsonl` / `debezium-json`, or `None` to detect: the first parseable
/// object sample with an `after` object means OGG (or Debezium when it
/// carries `op` without `op_type`), otherwise plain JSON. Columns come out
/// sorted by name (deterministic regardless of message order — contract
/// snapshots stay stable; `preserve_order` is not enabled workspace-wide
/// anyway), with the three OGG envelope slots first for OGG.
pub(crate) fn infer_from_json_samples(
    samples: &[Vec<u8>],
    format: Option<&str>,
) -> anyhow::Result<InferredSchema> {
    let parsed: Vec<Value> = samples
        .iter()
        .filter_map(|raw| serde_json::from_slice::<Value>(raw).ok())
        .collect();
    if parsed.is_empty() {
        anyhow::bail!(
            "样本 {} 条均非合法 JSON，无法推断（csv/parquet 请手工登记列）",
            samples.len()
        );
    }
    let fmt = match format {
        Some("ogg") => Fmt::Ogg,
        Some("json") | Some("jsonl") => Fmt::Json,
        Some("debezium-json") => Fmt::Debezium,
        _ => detect(&parsed),
    };
    let mut cols = Columns::default();
    let mut used = 0usize;
    for v in &parsed {
        let Some(root) = v.as_object() else {
            continue; // scalars/arrays at the root are not row shapes
        };
        match fmt {
            Fmt::Ogg => match root.get("after").and_then(Value::as_object) {
                Some(after) => cols.observe(after),
                None => continue, // OggDecoder marks such a message Bad
            },
            Fmt::Json => cols.observe(root),
            // Mirrors DebeziumJsonDecoder::walk sample-for-sample: unwrap
            // the schemas.enable=true wrapper, skip deletes (op=d), missing
            // / null `after` (tombstones), and never surface the envelope
            // keys (before/op/ts_ms/source) as columns — the decoder does
            // not decode them, so neither does inference.
            Fmt::Debezium => {
                let env = if root.get("schema").is_some() && root.get("payload").is_some() {
                    match root.get("payload") {
                        Some(p) if p.is_object() => p,
                        _ => continue, // degenerate wrapper / null payload
                    }
                } else {
                    v
                };
                let Some(env) = env.as_object() else {
                    continue;
                };
                if env.get("op").and_then(Value::as_str) == Some("d") {
                    continue;
                }
                match env.get("after").and_then(Value::as_object) {
                    Some(after) => cols.observe(after),
                    None => continue,
                }
            }
        }
        used += 1;
    }
    if used == 0 {
        anyhow::bail!(match fmt {
            Fmt::Ogg => "样本无 `after` 对象，不是 OGG 形态（或改用 format=json）",
            Fmt::Debezium => "样本无有效 Debezium 事件（均缺 `after` 或为 delete/tombstone）",
            Fmt::Json => "样本根节点不是 JSON 对象，无法推断列",
        });
    }
    let mut columns: Vec<(String, String)> = Vec::new();
    if fmt == Fmt::Ogg {
        columns.extend(
            OGG_ENVELOPE
                .iter()
                .map(|(n, t)| (n.to_string(), t.to_string())),
        );
        let envelope: Vec<&str> = OGG_ENVELOPE.iter().map(|(n, _)| *n).collect();
        columns.extend(
            cols.into_pairs()
                .into_iter()
                .filter(|(n, _)| !envelope.contains(&n.as_str())),
        );
    } else {
        columns.extend(cols.into_pairs());
    }
    Ok(InferredSchema {
        columns,
        format: Some(
            match fmt {
                Fmt::Ogg => "ogg",
                Fmt::Debezium => "debezium-json",
                Fmt::Json => "json",
            }
            .to_string(),
        ),
    })
}

/// Shape detection from the first object sample: an `after` object means a
/// CDC envelope — `op` without `op_type` is Debezium, `op_type` (or neither
/// marker, the pre-A3 tie) is OGG; no `after` at all is plain JSON. The
/// OGG-first tie keeps the pre-A3 detection byte-stable for envelopes that
/// carry `after` alone.
fn detect(parsed: &[Value]) -> Fmt {
    let Some(first) = parsed.iter().find(|v| v.is_object()) else {
        return Fmt::Json;
    };
    let after_is_object = first.get("after").map(Value::is_object).unwrap_or(false);
    if !after_is_object {
        return Fmt::Json;
    }
    let has_op = first.get("op").map(Value::is_string).unwrap_or(false);
    let has_op_type = first.get("op_type").map(Value::is_string).unwrap_or(false);
    if has_op && !has_op_type {
        Fmt::Debezium
    } else {
        Fmt::Ogg
    }
}

/// MySQL / Doris DDL type (as `DESC` prints it: `bigint`, `int(11)`,
/// `varchar(64)`, `decimalv3(10,2)`, `largeint`, `tinyint(1)`, Doris
/// `datetimev2(3)` / `datev2` / `text` …) → the type vocabulary. A3
/// narrowing: the integer widths survive (`tinyint`→`int8` …, so a
/// registered int8 column still reads back int8 from DESC and evolve's
/// diff stays quiet), `float`→`float32`, DECIMAL keeps its parameters
/// (within the vocabulary's 1..=38 / s≤p bounds — outside them the honest
/// carrier is still utf8, matching conn-jdbc's no-decimal stance), the
/// datetime family lands `timestamp(us)` (Doris DATETIME(6) is the DDL
/// side of every unit, so us is the round-trip-stable spelling), `date`/
/// `datev2` → `date`, and the `text` family → `string` (Doris prints its
/// STRING columns back as `text` — live-probed 2.1.7 — so string is what
/// keeps draft→DESC→diff stable; MySQL TEXT lands the same, a strictly
/// wider carrier than VARCHAR(65533)). A4: Doris prints a VARIANT column
/// back as `variant` (probed 2.1.7 and 3.1.4,
/// scripts/e2e/evidence/variant-probe-2026-10-30/ A4) → `variant`, the
/// same round-trip reasoning. A5: Doris prints an ARRAY<FLOAT> column back
/// as `array<float>` (probed 3.1.4,
/// scripts/e2e/evidence/vector-probe-2026-10-30/ A5) → `list<float32>`; the
/// declared dimension is a column COMMENT, not part of the type, so DESC
/// cannot give it back — a dimensioned registration reads back as the bare
/// word. Every other array element type stays utf8 (the engine has no other
/// list word). MySQL `json` / Doris `json`/`jsonb` stay utf8:
/// they are not VARIANT and the engine has no JSON-column word.
pub(crate) fn ddl_arrow_type(ddl: &str) -> String {
    let lower = ddl.trim().to_ascii_lowercase();
    let head: String = lower
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    // `decimal_params()` = the (p,s) of a decimal-ish type, when present
    // and vocabulary-legal (1..=38, s≤p)
    let decimal_params = || -> Option<String> {
        let open = lower.find('(')?;
        let close = lower.rfind(')')?;
        let (p, s) = lower[open + 1..close].split_once(',')?;
        let (p, s): (u32, u32) = (p.trim().parse().ok()?, s.trim().parse().ok()?);
        ((1..=38).contains(&p) && s <= p).then(|| format!("decimal({p},{s})"))
    };
    // `array_elem()` = the element type between `array<` and `>`
    let array_elem = || -> Option<&str> {
        lower
            .strip_prefix("array<")
            .and_then(|rest| rest.strip_suffix('>'))
            .map(str::trim)
    };
    match head.as_str() {
        "tinyint" => "int8".to_string(),
        "smallint" => "int16".to_string(),
        "mediumint" | "int" | "integer" => "int32".to_string(),
        "bigint" | "largeint" => "int64".to_string(),
        "boolean" | "bool" => "bool".to_string(),
        "float" => "float32".to_string(),
        "double" | "real" => "float64".to_string(),
        "decimal" | "decimalv3" => decimal_params().unwrap_or_else(|| "utf8".to_string()),
        "datetime" | "datetimev2" | "timestamp" => "timestamp(us)".to_string(),
        "date" | "datev2" => "date".to_string(),
        "text" | "tinytext" | "mediumtext" | "longtext" | "string" => "string".to_string(),
        "variant" => "variant".to_string(),
        "array" if array_elem() == Some("float") => "list<float32>".to_string(),
        _ => "utf8".to_string(),
    }
}

/// PostgreSQL `information_schema.columns.data_type` (`integer`, `bigint`,
/// `double precision`, `character varying`, `timestamp without time zone`
/// …) → the type vocabulary, same A3 narrowing as [`ddl_arrow_type`]:
/// widths survive, `real`→`float32`, time-of-day stays utf8 (a duration is
/// not a timestamp), `timestamptz` stays utf8 (Doris has no timezone for
/// it to land in), `numeric` stays utf8 (information_schema reports it
/// without precision — the parameters would be invented). PG prints
/// unbounded text as `text`, hence `string` like the MySQL side.
pub(crate) fn pg_arrow_type(pg: &str) -> String {
    match pg.trim().to_ascii_lowercase().as_str() {
        "smallint" => "int16".to_string(),
        "integer" => "int32".to_string(),
        "bigint" => "int64".to_string(),
        "boolean" => "bool".to_string(),
        "real" => "float32".to_string(),
        "double precision" => "float64".to_string(),
        "date" => "date".to_string(),
        "timestamp without time zone" => "timestamp(us)".to_string(),
        "text" => "string".to_string(),
        _ => "utf8".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raws(lines: &[&str]) -> Vec<Vec<u8>> {
        lines.iter().map(|l| l.as_bytes().to_vec()).collect()
    }

    #[test]
    fn json_widening_and_order() {
        let s = raws(&[
            r#"{"id": 1, "name": "a", "ratio": 1, "ok": true, "nothing": null}"#,
            r#"{"id": 2, "name": "b", "ratio": 2.5, "ok": false, "nothing": null, "late": "x"}"#,
            r#"{"id": "three", "name": "c", "ratio": 3, "ok": true}"#,
        ]);
        let out = infer_from_json_samples(&s, None).unwrap();
        assert_eq!(out.format.as_deref(), Some("json"));
        // alphabetical key order (serde_json canonical), types merged:
        assert_eq!(
            out.columns,
            vec![
                ("id".into(), "utf8".into()), // int + str → utf8
                ("late".into(), "utf8".into()),
                ("name".into(), "utf8".into()),
                ("nothing".into(), "utf8".into()), // never non-null → default
                ("ok".into(), "bool".into()),
                ("ratio".into(), "float64".into()), // int + float → float64
            ]
        );
    }

    /// A4: a field whose non-null samples are all nested (object / array)
    /// infers `variant` — the whole sub-document is carried through (the
    /// decoder's variant passthrough) and Doris VARIANT does the sub-column
    /// inference. Null samples absorb; a nested/scalar mix stays `utf8`
    /// (mixed shapes have no honest typed carrier). Both json and
    /// debezium-json `after.*` walks pick it the same way.
    #[test]
    fn json_nested_infers_variant() {
        let s = raws(&[
            r#"{"id":1,"attrs":{"a":{"b":1}},"tags":[1,2],"maybe":null,"mixed":{"k":1}}"#,
            r#"{"id":2,"attrs":{"a":{"b":2},"c":"x"},"tags":[],"maybe":{"late":true},"mixed":"text"}"#,
            r#"{"id":3,"attrs":null,"tags":[3]}"#,
        ]);
        let out = infer_from_json_samples(&s, Some("json")).unwrap();
        assert_eq!(
            out.columns,
            vec![
                ("attrs".into(), "variant".into()), // object + object + null
                ("id".into(), "int64".into()),
                ("maybe".into(), "variant".into()), // null + object
                ("mixed".into(), "utf8".into()),    // object + string → mixed → utf8
                ("tags".into(), "variant".into()),  // arrays (incl. empty)
            ]
        );
        let dbz =
            raws(&[r#"{"before":null,"after":{"id":1,"payload":{"x":[1]}},"op":"c","ts_ms":1}"#]);
        let out = infer_from_json_samples(&dbz, None).unwrap();
        assert_eq!(out.format.as_deref(), Some("debezium-json"));
        assert_eq!(
            out.columns,
            vec![
                ("id".into(), "int64".into()),
                ("payload".into(), "variant".into()),
            ]
        );
    }

    #[test]
    fn ogg_detected_envelope_first() {
        let s = raws(&[
            r#"{"table":"SHOP.ORDERS","op_type":"I","ts":1700000000000,"after":{"order_no":"A1","seq":7,"amount":1.5}}"#,
            r#"{"table":"SHOP.ORDERS","op_type":"U","ts":1700000000001,"after":{"order_no":"A2","seq":8,"amount":2,"note":"n"}}"#,
        ]);
        let out = infer_from_json_samples(&s, None).unwrap();
        assert_eq!(out.format.as_deref(), Some("ogg"));
        assert_eq!(
            out.columns,
            vec![
                ("op_type".into(), "utf8".into()),
                ("ogg_table".into(), "utf8".into()),
                ("ogg_ts".into(), "int64".into()),
                ("amount".into(), "float64".into()),
                ("note".into(), "utf8".into()),
                ("order_no".into(), "utf8".into()),
                ("seq".into(), "int64".into()),
            ]
        );
    }

    #[test]
    fn explicit_format_and_failures() {
        // explicit json on an OGG-shaped sample: root keys; `after` is a
        // nested object → variant (A4; pre-A4 it folded to utf8 and every
        // value decoded NULL)
        let s = raws(&[r#"{"op_type":"I","after":{"x":1}}"#]);
        let out = infer_from_json_samples(&s, Some("json")).unwrap();
        assert_eq!(
            out.columns,
            vec![
                ("after".into(), "variant".into()),
                ("op_type".into(), "utf8".into())
            ]
        );
        // explicit ogg without `after` → error naming the shape
        let e = infer_from_json_samples(&raws(&[r#"{"x":1}"#]), Some("ogg")).unwrap_err();
        assert!(e.to_string().contains("after"), "{e}");
        // non-JSON samples → error
        let e = infer_from_json_samples(&raws(&["a,b,c", "1,2,3"]), None).unwrap_err();
        assert!(e.to_string().contains("非合法 JSON"), "{e}");
        // root arrays only → error
        let e = infer_from_json_samples(&raws(&["[1,2]"]), None).unwrap_err();
        assert!(e.to_string().contains("根节点"), "{e}");
    }

    /// A3: debezium-json walks `after.*` exactly the way
    /// DebeziumJsonDecoder decodes it — envelope keys (before/op/ts_ms/
    /// source) never become columns, deletes and tombstones are skipped,
    /// and the schemas.enable=true wrapper is unwrapped. Detection picks
    /// Debezium when an `after` object carries `op` without `op_type`.
    #[test]
    fn debezium_explicit_detected_and_skips() {
        // explicit format: c/r/u observed, delete skipped, envelope invisible
        let s = raws(&[
            r#"{"before":null,"after":{"id":1,"qty":5,"price":1.5,"ok":true},"op":"c","ts_ms":1700000000000,"source":{"db":"shop"}}"#,
            r#"{"before":{"id":1},"after":{"id":1,"qty":6,"price":1.5,"ok":false},"op":"u","ts_ms":1700000000001}"#,
            r#"{"before":{"id":1},"after":null,"op":"d","ts_ms":1700000000002}"#,
            r#"{"schema":{"type":"struct"},"payload":{"after":{"id":2,"qty":1},"op":"r","ts_ms":1700000000003}}"#,
        ]);
        let out = infer_from_json_samples(&s, Some("debezium-json")).unwrap();
        assert_eq!(out.format.as_deref(), Some("debezium-json"));
        assert_eq!(
            out.columns,
            vec![
                ("id".into(), "int64".into()),
                ("ok".into(), "bool".into()),
                ("price".into(), "float64".into()),
                ("qty".into(), "int64".into()),
            ]
        );
        // detection (no explicit format): op + after without op_type → debezium
        let out = infer_from_json_samples(&s, None).unwrap();
        assert_eq!(out.format.as_deref(), Some("debezium-json"));
        assert_eq!(out.columns.len(), 4);
        // OGG tie unchanged: after without op → still OGG (pre-A3 anchor)
        let ogg_shaped = raws(&[r#"{"table":"T","ts":1,"after":{"x":1}}"#]);
        let out = infer_from_json_samples(&ogg_shaped, None).unwrap();
        assert_eq!(out.format.as_deref(), Some("ogg"));
        // all-delete / no-after samples → the dedicated error, not a fold
        let e = infer_from_json_samples(
            &raws(&[r#"{"before":null,"after":null,"op":"d"}"#]),
            Some("debezium-json"),
        )
        .unwrap_err();
        assert!(e.to_string().contains("Debezium"), "{e}");
    }

    #[test]
    fn ddl_type_mapping_mysql_and_doris() {
        // A3 narrowing: widths survive, decimals keep their params, the
        // datetime family lands timestamp(us), Doris STRING prints back as
        // `text` (live-probed 2.1.7) and maps to `string`.
        for (ddl, want) in [
            ("bigint", "int64"),
            ("BIGINT(20) UNSIGNED", "int64"),
            ("int(11)", "int32"),
            ("tinyint", "int8"),
            ("tinyint(1)", "int8"),
            ("smallint", "int16"),
            ("mediumint", "int32"),
            ("largeint", "int64"), // Doris
            ("boolean", "bool"),   // Doris
            ("bool", "bool"),
            ("double", "float64"),
            ("float", "float32"),
            ("decimal(10,2)", "decimal(10,2)"),
            ("decimalv3(18,2)", "decimal(18,2)"), // Doris
            ("decimal(39,0)", "utf8"),            // p > 38: no legal vocabulary word
            ("decimal", "utf8"),                  // bare: parameters would be invented
            ("varchar(64)", "utf8"),
            ("char(2)", "utf8"),
            ("text", "string"), // Doris STRING prints back as text (probed)
            ("longtext", "string"),
            ("string", "string"), // Doris / StarRocks spelling when present
            ("datetime", "timestamp(us)"),
            ("datetime(6)", "timestamp(us)"),
            ("datetimev2(3)", "timestamp(us)"), // Doris
            ("timestamp", "timestamp(us)"),     // MySQL TIMESTAMP (no tz payload read)
            ("datev2", "date"),                 // Doris
            ("date", "date"),
            ("variant", "variant"), // Doris VARIANT prints back as variant (probed 2.1.7 + 3.1.4)
            ("json", "utf8"),       // MySQL JSON / Doris JSON(B): not VARIANT, no vocabulary word
            ("jsonb", "utf8"),      // Doris
            // A5: Doris ARRAY<FLOAT> prints back as array<float> (probed
            // 3.1.4) → the bare vector word (the dim is a COMMENT, not the
            // type); any other element type has no vocabulary word
            ("array<float>", "list<float32>"),
            ("ARRAY<FLOAT>", "list<float32>"), // SHOW CREATE TABLE casing
            ("array< float >", "list<float32>"),
            ("array<int>", "utf8"),    // Doris
            ("array<double>", "utf8"), // no list<float64> word
            ("array<array<float>>", "utf8"),
            ("array", "utf8"),  // bare / malformed
            ("hll", "utf8"),    // Doris
            ("bitmap", "utf8"), // Doris
        ] {
            assert_eq!(ddl_arrow_type(ddl), want, "ddl {ddl:?}");
        }
    }

    #[test]
    fn pg_type_mapping() {
        // Same A3 narrowing as the MySQL side: widths, float32, date, and
        // us-precision timestamps; numeric/timestamptz stay utf8 (no legal
        // vocabulary word without inventing parameters / a timezone).
        for (pg, want) in [
            ("integer", "int32"),
            ("bigint", "int64"),
            ("smallint", "int16"),
            ("boolean", "bool"),
            ("real", "float32"),
            ("double precision", "float64"),
            ("numeric", "utf8"),
            ("character varying", "utf8"),
            ("text", "string"),
            ("timestamp without time zone", "timestamp(us)"),
            ("timestamp with time zone", "utf8"),
            ("date", "date"),
            ("jsonb", "utf8"),
            ("uuid", "utf8"),
        ] {
            assert_eq!(pg_arrow_type(pg), want, "pg {pg:?}");
        }
    }

    #[test]
    fn topic_split_join_roundtrip() {
        use crate::{join_topic, split_topic};
        assert_eq!(split_topic("shop.orders"), ("shop".into(), "orders".into()));
        assert_eq!(split_topic("a.b.c"), ("a".into(), "b.c".into()));
        assert_eq!(split_topic("flat"), ("".into(), "flat".into()));
        for t in ["shop.orders", "a.b.c", "flat"] {
            let (d, n) = split_topic(t);
            assert_eq!(join_topic(&d, &n), t);
        }
    }
}
