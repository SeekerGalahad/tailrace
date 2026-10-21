//! Pure inference: JSON samples → columns (mirrors the decode chain's typed
//! extraction) and DDL type names → the engine's arrow vocabulary.
//!
//! Vocabulary = `tailrace-runtime::arrow_type`'s accepted set — `int64`,
//! `float64`, `bool`, everything else `utf8` — so what infer-schema hands
//! back is exactly what the runtime builds its target schema from.

use std::collections::HashMap;

use serde_json::Value;

use crate::InferredSchema;

/// Value-level type lattice. Merge widens: Null absorbs into anything,
/// Int+Float → Float, any other mix → Str (a column of mixed shapes is only
/// safely carried as text — the JsonDecoder's `as_str` arm then takes
/// string values and NULLs the rest, which is the lossy-but-honest option).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ty {
    Null,
    Bool,
    Int,
    Float,
    Str,
}

impl Ty {
    fn of(v: &Value) -> Ty {
        match v {
            Value::Null => Ty::Null,
            Value::Bool(_) => Ty::Bool,
            Value::Number(n) if n.is_i64() || n.is_u64() => Ty::Int,
            Value::Number(_) => Ty::Float,
            Value::String(_) => Ty::Str,
            // nested shapes: the JsonDecoder cannot place them in a scalar
            // column (NULLs them); utf8 keeps the column visible
            Value::Array(_) | Value::Object(_) => Ty::Str,
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

/// Infer from raw JSON samples. `format` = explicit `ogg` / `json` /
/// `jsonl`, or `None` to detect: the first parseable sample with an `after`
/// object means OGG, otherwise plain JSON. Columns come out sorted by name
/// (deterministic regardless of message order — contract snapshots stay
/// stable; `preserve_order` is not enabled workspace-wide anyway), with the
/// three OGG envelope slots first for OGG.
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
    let ogg = match format {
        Some("ogg") => true,
        Some("json") | Some("jsonl") => false,
        _ => parsed
            .iter()
            .find(|v| v.is_object())
            .map(|v| v.get("after").map(Value::is_object).unwrap_or(false))
            .unwrap_or(false),
    };
    let mut cols = Columns::default();
    let mut used = 0usize;
    for v in &parsed {
        let Some(root) = v.as_object() else {
            continue; // scalars/arrays at the root are not row shapes
        };
        if ogg {
            match root.get("after").and_then(Value::as_object) {
                Some(after) => cols.observe(after),
                None => continue, // OggDecoder marks such a message Bad
            }
        } else {
            cols.observe(root);
        }
        used += 1;
    }
    if used == 0 {
        anyhow::bail!(if ogg {
            "样本无 `after` 对象，不是 OGG 形态（或改用 format=json）"
        } else {
            "样本根节点不是 JSON 对象，无法推断列"
        });
    }
    let mut columns: Vec<(String, String)> = Vec::new();
    if ogg {
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
        format: Some(if ogg { "ogg" } else { "json" }.to_string()),
    })
}

/// MySQL / Doris DDL type (as `DESC` prints it: `bigint`, `int(11)`,
/// `varchar(64)`, `decimalv3(10,2)`, `largeint`, `tinyint(1)` …) → arrow
/// vocabulary. Integers of every width → int64 (LARGEINT/BIGINT UNSIGNED
/// beyond i64 are the documented overflow edge); DECIMAL stays utf8 —
/// text preserves precision, matching conn-jdbc's no-decimal stance.
pub(crate) fn ddl_arrow_type(ddl: &str) -> &'static str {
    let lower = ddl.trim().to_ascii_lowercase();
    let head: String = lower
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    match head.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "largeint" => "int64",
        "boolean" | "bool" => "bool",
        "float" | "double" | "real" => "float64",
        _ => "utf8",
    }
}

/// PostgreSQL `information_schema.columns.data_type` (`integer`, `bigint`,
/// `double precision`, `character varying`, `timestamp without time zone`
/// …) → arrow vocabulary.
pub(crate) fn pg_arrow_type(pg: &str) -> &'static str {
    match pg.trim().to_ascii_lowercase().as_str() {
        "smallint" | "integer" | "bigint" => "int64",
        "boolean" => "bool",
        "real" | "double precision" => "float64",
        _ => "utf8",
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
        // explicit json on an OGG-shaped sample: root keys, after as utf8 (nested)
        let s = raws(&[r#"{"op_type":"I","after":{"x":1}}"#]);
        let out = infer_from_json_samples(&s, Some("json")).unwrap();
        assert_eq!(
            out.columns,
            vec![
                ("after".into(), "utf8".into()),
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

    #[test]
    fn ddl_type_mapping_mysql_and_doris() {
        for (ddl, want) in [
            ("bigint", "int64"),
            ("BIGINT(20) UNSIGNED", "int64"),
            ("int(11)", "int64"),
            ("tinyint(1)", "int64"),
            ("smallint", "int64"),
            ("mediumint", "int64"),
            ("largeint", "int64"), // Doris
            ("boolean", "bool"),   // Doris
            ("bool", "bool"),
            ("double", "float64"),
            ("float", "float64"),
            ("decimal(10,2)", "utf8"),
            ("decimalv3(10,2)", "utf8"), // Doris
            ("varchar(64)", "utf8"),
            ("char(2)", "utf8"),
            ("text", "utf8"),
            ("string", "utf8"), // Doris
            ("datetime", "utf8"),
            ("datetimev2(3)", "utf8"), // Doris
            ("datev2", "utf8"),        // Doris
            ("json", "utf8"),
            ("array<int>", "utf8"), // Doris
            ("hll", "utf8"),        // Doris
            ("bitmap", "utf8"),     // Doris
        ] {
            assert_eq!(ddl_arrow_type(ddl), want, "ddl {ddl:?}");
        }
    }

    #[test]
    fn pg_type_mapping() {
        for (pg, want) in [
            ("integer", "int64"),
            ("bigint", "int64"),
            ("smallint", "int64"),
            ("boolean", "bool"),
            ("real", "float64"),
            ("double precision", "float64"),
            ("numeric", "utf8"),
            ("character varying", "utf8"),
            ("text", "utf8"),
            ("timestamp without time zone", "utf8"),
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
