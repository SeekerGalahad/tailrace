//! Console-side SQL preparation (§4.5): the editor's `db.table` form → the
//! engine's `${ds:datasourceId.db.table}` reference form, then local
//! compilation via the embedded tailrace-parser (§4.6 matrix: console-side
//! parser for offline editor diagnostics; zero online-node dependency).

use serde_json::{Map, Value};

/// One compile diagnostic in the §4.5 response shape (`line` is the
/// 1-based statement position, 1 when not statement-scoped).
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct CompileDiag {
    pub line: i64,
    pub message: String,
}

/// One dim-join extracted by the compiler (T-D1): `reference` is the
/// `${ds:dim.<datasourceId>.<table>}` body after `dim.` (`dsId.table` — the
/// plan table-object key for the dimension), `name` the static `dim_<alias>`
/// MemTable the rewritten selectSql joins against.
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct DimJoinOut {
    pub reference: String,
    pub name: String,
}

/// One compiled statement (§4.5 response shape; the engine's StatementSpec
/// with an explicit index).
#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct StatementOut {
    pub index: usize,
    pub source: String,
    pub group: String,
    pub sink: String,
    pub pushdown: Vec<String>,
    pub select_sql: String,
    pub start_offset: Option<String>,
    /// Dimension-table joins (T-D1 broadcast join): empty for plain
    /// statements. The publish gate resolves each `reference` against
    /// registered kind=dim table objects; deploy carries their metadata with
    /// the plan (the engine preflight counts them — a missing entry rejects
    /// the whole plan).
    pub dims: Vec<DimJoinOut>,
    /// Partial-column declaration (T-A7): the INSERT column list, `null`
    /// when the statement has none (full-row writes). The publish gate
    /// requires a doris sink registered `keyModel=unique` whose columns
    /// cover the list; deploy projects it onto the sink's plan table entry
    /// (`partial_columns`) — the switch the engine's DorisSink reads.
    pub partial_columns: Option<Vec<String>>,
}

/// Rewrite `db.table` two-part identifiers whose `db` is mapped in
/// `ds_mapping` into `${ds:<id>.<db>.<table>}` placeholders (§4.5 publish
/// materialization). Rewriting is lenient by design: only *mapped* database
/// names are touched, so SELECT aliases (`o.id`), numeric literals (`1.5`)
/// and quoted identifiers pass through untouched; unmapped INSERT/FROM
/// targets then fail compilation with the parser's "not a ${ds:}
/// placeholder" diagnostic (the `db.table` ↔ dsMapping cross-check).
///
/// The scan skips string literals ('…') and double-quoted identifiers
/// ("…"), mirroring the parser's own lexical rules. `a.b` inside a longer
/// `a.b.c` chain is left alone (v1 has no three-part references; the parser
/// rejects them with its own diagnostic).
pub fn substitute_db_refs(sql: &str, ds_mapping: &Map<String, Value>) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        // string literals / quoted identifiers pass through verbatim
        if c == '\'' || c == '"' {
            let quote = c;
            out.push(c);
            i += 1;
            while i < chars.len() {
                out.push(chars[i]);
                if chars[i] == quote {
                    // '' inside a '…' literal is an escaped quote — only the
                    // closing one (not followed by itself) ends the region.
                    if quote == '\'' && i + 1 < chars.len() && chars[i + 1] == '\'' {
                        out.push(chars[i + 1]);
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }
        if is_ident_start(c) {
            let start = i;
            while i < chars.len() && is_ident_part(chars[i]) {
                i += 1;
            }
            let first: String = chars[start..i].iter().collect();
            // look ahead: '.' + identifier + a boundary that is not '.'
            // (so the middle of a three-part chain is untouched)
            if i < chars.len()
                && chars[i] == '.'
                && i + 1 < chars.len()
                && is_ident_start(chars[i + 1])
            {
                let mut j = i + 1;
                while j < chars.len() && is_ident_part(chars[j]) {
                    j += 1;
                }
                let second: String = chars[i + 1..j].iter().collect();
                let followed_by_dot = j < chars.len() && chars[j] == '.';
                if !followed_by_dot {
                    if let Some(ds_id) = ds_mapping.get(&first).and_then(Value::as_i64) {
                        out.push_str(&format!("${{ds:{ds_id}.{first}.{second}}}"));
                        i = j;
                        continue;
                    }
                }
                // not a mapped db.table — emit verbatim
                out.push_str(&first);
                out.push('.');
                out.push_str(&second);
                i = j;
                continue;
            }
            out.push_str(&first);
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

fn is_ident_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_ident_part(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Local compile (§4.5 POST /api/pipelines/compile and the publish/deploy
/// gates). Returns (statements, diagnostics); diagnostics non-empty means
/// `ok:false` (hard parse errors are folded into diagnostics so the editor
/// gets one uniform shape).
pub fn compile_sql(sql: &str, pipeline_id: u64) -> (Vec<StatementOut>, Vec<CompileDiag>) {
    match tailrace_parser::compile(sql, pipeline_id) {
        Ok((compiled, diags)) => {
            let statements = compiled
                .statements
                .iter()
                .enumerate()
                .map(|(index, s)| StatementOut {
                    index,
                    source: s.source.clone(),
                    group: s.group.clone(),
                    sink: s.sink.clone(),
                    pushdown: s.pushdown.clone(),
                    select_sql: s.select_sql.clone(),
                    start_offset: s.start_offset.clone(),
                    dims: s
                        .dims
                        .iter()
                        .map(|d| DimJoinOut {
                            reference: d.reference.clone(),
                            name: d.name.clone(),
                        })
                        .collect(),
                    partial_columns: s.partial_columns.clone(),
                })
                .collect();
            let diags = diags
                .iter()
                .map(|d| CompileDiag {
                    line: d.statement.map(|s| s as i64 + 1).unwrap_or(1),
                    message: d.message.clone(),
                })
                .collect();
            (statements, diags)
        }
        Err(e) => (
            Vec::new(),
            vec![CompileDiag {
                line: 1,
                message: format!("SQL 解析失败: {e:#}"),
            }],
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mapping() -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("shop".into(), json!(3));
        m.insert("ods".into(), json!(4));
        m
    }

    #[test]
    fn rewrites_only_mapped_two_part_refs() {
        let sql = "INSERT INTO ods.orders\nSELECT o.id, 1.5 AS score FROM shop.orders o WHERE op_type <> 'D'";
        let out = substitute_db_refs(sql, &mapping());
        assert_eq!(
            out,
            "INSERT INTO ${ds:4.ods.orders}\nSELECT o.id, 1.5 AS score FROM ${ds:3.shop.orders} o WHERE op_type <> 'D'",
            "aliases (o.id), numeric literals (1.5) and literals stay untouched"
        );
    }

    #[test]
    fn skips_strings_and_three_part_chains() {
        let sql = "SELECT 'shop.orders' AS x, t.shop.orders FROM shop.orders";
        let out = substitute_db_refs(sql, &mapping());
        assert!(out.contains("'shop.orders'"), "literal untouched: {out}");
        assert!(
            out.contains("t.shop.orders"),
            "three-part chain untouched: {out}"
        );
        assert!(
            out.contains("${ds:3.shop.orders}"),
            "from ref rewritten: {out}"
        );
    }

    #[test]
    fn compile_via_local_parser() {
        let sql = "INSERT INTO ${ds:4.ods.orders} SELECT id, payload FROM ${ds:3.shop.orders} WHERE op_type IN ('I','U')";
        let (stmts, diags) = compile_sql(sql, 42);
        assert!(diags.is_empty(), "{diags:?}");
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0].source, "3.shop.orders");
        assert_eq!(stmts[0].sink, "4.ods.orders");
        assert_eq!(stmts[0].group, "tr.p42.s0");
        assert_eq!(stmts[0].pushdown, vec!["I", "U"]);
        assert!(stmts[0].select_sql.contains("FROM src"));

        // non-INSERT → ok:false diagnostics with the §4.5 shape
        let (stmts, diags) = compile_sql("SELECT 1", 42);
        assert!(stmts.is_empty());
        assert!(!diags.is_empty());
        assert!(diags[0].message.contains("INSERT"), "{diags:?}");

        // unmapped db.table stays raw → parser rejects with a clear message
        let raw = substitute_db_refs(
            "INSERT INTO nosuch.orders SELECT id FROM shop.orders",
            &mapping(),
        );
        let (_, diags) = compile_sql(&raw, 42);
        assert!(
            !diags.is_empty(),
            "unmapped target must diagnose: {diags:?}"
        );
    }

    /// T-A7 switch, both states through the console compile: the INSERT
    /// column list rides `partialColumns` verbatim; without one the field is
    /// null and the statement shape is otherwise identical. The db.table
    /// rewrite leaves the list alone (plain identifiers, no `db.` prefix).
    #[test]
    fn compile_carries_the_partial_columns_switch() {
        let off = substitute_db_refs(
            "INSERT INTO ods.orders SELECT id, payload FROM shop.orders",
            &mapping(),
        );
        let (stmts, diags) = compile_sql(&off, 42);
        assert!(diags.is_empty(), "{diags:?}");
        assert_eq!(stmts[0].partial_columns, None, "no list = full-row writes");

        let on = substitute_db_refs(
            "INSERT INTO ods.orders (id, payload) SELECT id, payload FROM shop.orders",
            &mapping(),
        );
        assert!(on.contains("${ds:4.ods.orders} (id, payload)"), "{on}");
        let (stmts, diags) = compile_sql(&on, 42);
        assert!(diags.is_empty(), "{diags:?}");
        assert_eq!(
            stmts[0].partial_columns.as_deref(),
            Some(&["id".to_string(), "payload".into()][..])
        );
        assert_eq!(stmts[0].sink, "4.ods.orders");
        assert_eq!(stmts[0].source, "3.shop.orders");
    }
}
