//! tailrace-parser: SQL frontend — pipeline SQL → statement set
//! (StatementSpec, the runtime input).
//!
//! Compile pipeline (the SQL compiler lives in the engine):
//! ① `${ds:ref}` reference extraction → synthetic identifier substitution
//!    (`${ds:12.default.orders_ogg}` → `__ds_0`);
//! ② FlinkDialect multi-statement parsing (v1 narrows to an ANSI subset:
//!    INSERT INTO sink SELECT … FROM source);
//! ③ AST → statement set: FROM tables rewrite to `src` (the runtime's decoded
//!    batch registration name), WHERE op_type constant predicates extract into
//!    the decode-time pushdown whitelist, cursor groups follow the
//!    `tr.p{pipeline}.s{idx}` convention;
//! ④ Validator: catalog resolves references + source/sink direction checks.
//! **The IR never carries credentials** — references are catalog anchors only.

use datafusion::sql::sqlparser::ast::{Expr, ObjectName, Query, SetExpr, Statement, TableFactor};
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::parser::Parser;

use tailrace_catalog::{CatalogProvider, StatementSpec};

/// FlinkDialect (v1: ANSI subset; the extension point for Flink-specific
/// syntax differences).
#[derive(Debug, Default)]
pub struct FlinkDialect;

impl Dialect for FlinkDialect {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }
    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_'
    }
}

/// Compile diagnostic (the data source for editor inline errors).
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct Diagnostic {
    pub statement: Option<usize>,
    pub message: String,
}

/// Compile product: statement set.
pub struct Compiled {
    pub statements: Vec<StatementSpec>,
}

/// Compile entry (editor inline validation: no plan options, group names use
/// the engine default convention).
pub fn compile(sql: &str, pipeline_id: u64) -> anyhow::Result<(Compiled, Vec<Diagnostic>)> {
    compile_with_opts(sql, pipeline_id, None, None)
}

/// Compile entry (the plan-push canon): `group_prefix` overrides the statement
/// group convention (`{prefix}.s{index}`, default `tr.p{pipeline}.s{n}`);
/// `start_offset` passes through to statements (earliest/latest).
pub fn compile_with_opts(
    sql: &str,
    pipeline_id: u64,
    group_prefix: Option<&str>,
    start_offset: Option<&str>,
) -> anyhow::Result<(Compiled, Vec<Diagnostic>)> {
    let (substituted, refs) = substitute_refs(sql);
    let dialect = FlinkDialect;
    let ast = Parser::parse_sql(&dialect, &substituted)?;
    let mut diags = Vec::new();
    let mut statements = Vec::new();
    for (idx, stmt) in ast.iter().enumerate() {
        let Statement::Insert(ins) = stmt else {
            diags.push(Diagnostic {
                statement: Some(idx),
                message: format!(
                    "only INSERT statements are supported (statement {} is {:?})",
                    idx + 1,
                    stmt
                ),
            });
            continue;
        };
        let table_name = match &ins.table {
            datafusion::sql::sqlparser::ast::TableObject::TableName(n) => n,
            _ => anyhow::bail!("statement #{}: INSERT target must be a table name", idx),
        };
        let sink = ref_of(table_name, &refs)
            .map_err(|e| anyhow::anyhow!("statement #{} sink reference invalid: {e}", idx))?;
        let Some(query) = ins.source.as_deref() else {
            anyhow::bail!("statement #{}: INSERT missing SELECT source", idx);
        };
        let (source_ref, select_sql, pushdown) = rewrite_query(query, &refs)
            .map_err(|e| anyhow::anyhow!("statement #{} compile failed: {e}", idx))?;
        let group = match group_prefix.map(str::trim).filter(|p| !p.is_empty()) {
            Some(p) => format!("{p}.s{idx}"),
            None => format!("tr.p{pipeline_id}.s{idx}"),
        };
        statements.push(StatementSpec {
            source: source_ref,
            group,
            sink,
            pushdown,
            select_sql,
            start_offset: start_offset.map(str::to_owned),
        });
    }
    if statements.is_empty() {
        diags.push(Diagnostic {
            statement: None,
            message:
                "no valid statements (a pipeline needs >=1 INSERT INTO sink SELECT FROM source)"
                    .into(),
        });
    }
    Ok((Compiled { statements }, diags))
}

/// Validator: catalog reference resolution + source/sink direction capability
/// checks.
pub fn validate(catalog: &dyn CatalogProvider, compiled: &Compiled) -> Vec<Diagnostic> {
    let mut out = Vec::new();
    for (i, st) in compiled.statements.iter().enumerate() {
        match catalog.resolve(&st.source) {
            Ok(t) if matches!(t.kind.as_str(), "kafka" | "file" | "jdbc") => {}
            Ok(t) => out.push(Diagnostic {
                statement: Some(i),
                message: format!(
                    "source {} only supports kafka/file/jdbc (got {})",
                    st.source, t.kind
                ),
            }),
            Err(e) => out.push(Diagnostic {
                statement: Some(i),
                message: format!("source resolution failed: {e}"),
            }),
        }
        match catalog.resolve(&st.sink) {
            Ok(t) if matches!(t.kind.as_str(), "kafka" | "file" | "doris" | "starrocks") => {}
            Ok(t) => out.push(Diagnostic {
                statement: Some(i),
                message: format!(
                    "sink {} only supports kafka/file/doris (got {})",
                    st.sink, t.kind
                ),
            }),
            Err(e) => out.push(Diagnostic {
                statement: Some(i),
                message: format!("sink resolution failed: {e}"),
            }),
        }
    }
    out
}

// ==================== internals ====================

/// `${ds:...}` extraction and substitution (inside string literals untouched
/// — a v1 lexical scan state machine).
fn substitute_refs(sql: &str) -> (String, std::collections::BTreeMap<String, String>) {
    let mut out = String::with_capacity(sql.len());
    let mut refs = std::collections::BTreeMap::new();
    let bytes: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let mut in_str: Option<char> = None;
    while i < bytes.len() {
        let c = bytes[i];
        if let Some(q) = in_str {
            out.push(c);
            if c == q {
                in_str = None;
            }
            i += 1;
            continue;
        }
        if c == '\'' || c == '"' {
            in_str = Some(c);
            out.push(c);
            i += 1;
            continue;
        }
        if c == '$' && i + 1 < bytes.len() && bytes[i + 1] == '{' {
            // read to '}'
            if let Some(end) = bytes[i + 2..].iter().position(|&x| x == '}') {
                let inner: String = bytes[i + 2..i + 2 + end].iter().collect();
                if let Some(rest) = inner.strip_prefix("ds:") {
                    let placeholder = format!("__ds_{}", refs.len());
                    refs.insert(placeholder.clone(), rest.to_string());
                    out.push_str(&placeholder);
                    i += 2 + end + 1;
                    continue;
                }
            }
        }
        out.push(c);
        i += 1;
    }
    (out, refs)
}

fn ident_name(name: &ObjectName) -> Option<String> {
    let mut parts = name.0.iter();
    let first = parts.next()?;
    if parts.next().is_some() {
        return None; // multi-part qualified names are not reference placeholders
    }
    match first {
        datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(i) => Some(i.value.clone()),
    }
}

fn ref_of(
    name: &ObjectName,
    refs: &std::collections::BTreeMap<String, String>,
) -> anyhow::Result<String> {
    let ident = ident_name(name)
        .ok_or_else(|| anyhow::anyhow!("must be a single-part ${{ds:}} reference"))?;
    refs.get(&ident)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("{ident} is not a ${{ds:}} placeholder"))
}

/// FROM table rewritten to src + pushdown extraction; returns (source_ref,
/// select_sql, pushdown).
fn rewrite_query(
    query: &Query,
    refs: &std::collections::BTreeMap<String, String>,
) -> anyhow::Result<(String, String, Vec<String>)> {
    let mut q = query.clone();
    let select = match q.body.as_mut() {
        SetExpr::Select(s) => s,
        other => anyhow::bail!("only SELECT bodies are supported ({:?})", other),
    };
    if select.from.len() != 1 {
        anyhow::bail!(
            "v1 supports a single FROM source (got {})",
            select.from.len()
        );
    }
    let factor = &mut select.from[0].relation;
    let source_ref = match factor {
        TableFactor::Table { name, .. } => {
            let r = ref_of(name, refs)?;
            *name = ObjectName(vec![
                datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier("src".into()),
            ]);
            r
        }
        other => anyhow::bail!("FROM must be a table reference ({:?})", other),
    };
    let pushdown = select
        .selection
        .as_ref()
        .map(extract_pushdown)
        .unwrap_or_default();
    Ok((source_ref, format!("{}", q), pushdown))
}

/// op_type constant predicate extraction (IN ('I','U') / = 'X'; a table
/// alias prefix is accepted too).
fn extract_pushdown(expr: &Expr) -> Vec<String> {
    match expr {
        Expr::InList {
            expr,
            list,
            negated,
        } if !*negated => {
            let is_op = col_is_op_type(expr);
            let values: Option<Vec<String>> = list
                .iter()
                .map(|v| match v {
                    Expr::Value(datafusion::sql::sqlparser::ast::ValueWithSpan {
                        value: datafusion::sql::sqlparser::ast::Value::SingleQuotedString(s),
                        ..
                    }) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            if is_op {
                if let Some(vals) = values {
                    return vals;
                }
            }
            Vec::new()
        }
        Expr::BinaryOp {
            left,
            op: datafusion::sql::sqlparser::ast::BinaryOperator::Eq,
            right,
        } => {
            let (_col, val) = if col_is_op_type(left) {
                (left, right.as_ref())
            } else if col_is_op_type(right) {
                (right, left.as_ref())
            } else {
                return Vec::new();
            };
            match val {
                Expr::Value(datafusion::sql::sqlparser::ast::ValueWithSpan {
                    value: datafusion::sql::sqlparser::ast::Value::SingleQuotedString(s),
                    ..
                }) => vec![s.clone()],
                _ => Vec::new(),
            }
        }
        Expr::BinaryOp {
            left,
            op: datafusion::sql::sqlparser::ast::BinaryOperator::And,
            right,
        } => {
            // Extract only under AND (the union is a permissive superset:
            // extra rows admitted here are still filtered by WHERE at
            // execution time — semantics stay exact). OR and any other
            // operator never extract: a past bug matched `op: _` across all
            // operators, so `op_type='I' OR seq>5` produced a ["I"] whitelist
            // and decode-time silently dropped the U/D rows that seq>5 would
            // have admitted — a semantics-breaking silent data loss.
            let mut out = extract_pushdown(left);
            out.extend(extract_pushdown(right));
            out
        }
        _ => Vec::new(),
    }
}

fn col_is_op_type(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(i) => i.value == "op_type",
        Expr::CompoundIdentifier(parts) => {
            parts.last().map(|i| i.value == "op_type").unwrap_or(false)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = include_str!("../../../examples/pipeline.sql");

    #[test]
    fn refs_substitution_skips_strings() {
        let (sql, refs) = substitute_refs("SELECT * FROM ${ds:a.b.c} WHERE x = '${ds:no}'");
        assert!(sql.contains("__ds_0"));
        assert_eq!(
            refs.values().collect::<Vec<_>>(),
            ["a.b.c".to_string()].iter().collect::<Vec<_>>(),
            "references inside string literals are not extracted"
        );
    }

    /// Golden set #0: examples/pipeline.sql (two-statement example) compiled →
    /// statement set structure assertions.
    #[test]
    fn golden_examples_pipeline_sql() -> anyhow::Result<()> {
        let (compiled, diags) = compile(EXAMPLE, 42)?;
        assert!(diags.is_empty(), "{diags:?}");
        let sts = compiled.statements;
        assert_eq!(
            sts.len(),
            2,
            "two statements (notification = a plain second INSERT)"
        );
        // statement 0: data lands in the Doris ODS
        assert_eq!(sts[0].source, "12.default.orders_ogg");
        assert_eq!(sts[0].sink, "3.shop.orders");
        assert_eq!(sts[0].group, "tr.p42.s0");
        assert_eq!(
            sts[0].pushdown,
            vec!["I", "U"],
            "op_type IN pushdown extraction"
        );
        assert!(
            sts[0].select_sql.contains("FROM src"),
            "{}",
            sts[0].select_sql
        );
        // statement 1: readiness notification (a plain sink)
        assert_eq!(sts[1].source, "12.default.orders_ogg");
        assert_eq!(sts[1].sink, "15.default.orders_notify");
        assert_eq!(sts[1].group, "tr.p42.s1");
        assert!(
            sts[1].select_sql.contains("ODS_READY"),
            "{}",
            sts[1].select_sql
        );
        Ok(())
    }

    /// Consumer controls: groupPrefix overrides the group convention
    /// ({prefix}.s{index}), startOffset passes through; defaults fall back to
    /// the engine convention with start_offset=None.
    #[test]
    fn golden_group_prefix_and_start_offset() -> anyhow::Result<()> {
        let (plain, _) = compile(EXAMPLE, 42)?;
        assert!(plain.statements.iter().all(|s| s.start_offset.is_none()));

        let (overridden, _) =
            compile_with_opts(EXAMPLE, 42, Some("etl.orders.eu"), Some("latest"))?;
        let sts = overridden.statements;
        assert_eq!(sts[0].group, "etl.orders.eu.s0");
        assert_eq!(sts[1].group, "etl.orders.eu.s1");
        assert!(sts
            .iter()
            .all(|s| s.start_offset.as_deref() == Some("latest")));

        // blank prefix = unset (falls back to the default convention)
        let (fallback, _) = compile_with_opts(EXAMPLE, 42, Some("  "), None)?;
        assert_eq!(fallback.statements[0].group, "tr.p42.s0");
        Ok(())
    }

    #[test]
    fn reject_non_insert_and_multi_source() {
        let (_, diags) = compile("SELECT 1", 1).unwrap();
        assert!(!diags.is_empty());
        let err = compile(
            "INSERT INTO ${ds:a.b} SELECT * FROM ${ds:c.d}, ${ds:e.f}",
            1,
        )
        .is_err();
        assert!(err, "multi-source rejected in v1");
    }

    /// Regression lock: op_type predicates extract only under AND. A past bug
    /// extracted ["I"] from `op_type='I' OR seq>5`, making decode-time
    /// silently drop the U/D rows that seq>5 would have admitted
    /// (semantics-breaking silent data loss).
    #[test]
    fn pushdown_extracts_only_under_and() {
        // OR: no extraction (conservative — WHERE still filters at execution time)
        let (c, _) = compile(
            "INSERT INTO ${ds:a.b} SELECT x FROM ${ds:c.d} WHERE op_type = 'I' OR seq > 5",
            1,
        )
        .unwrap();
        assert!(
            c.statements[0].pushdown.is_empty(),
            "no extraction under OR (the bug wrongly produced [\"I\"]): {:?}",
            c.statements[0].pushdown
        );
        // OR of two op_type equalities: still conservatively no extraction
        // (the canonical form is IN)
        let (c, _) = compile(
            "INSERT INTO ${ds:a.b} SELECT x FROM ${ds:c.d} WHERE op_type = 'I' OR op_type = 'U'",
            1,
        )
        .unwrap();
        assert!(
            c.statements[0].pushdown.is_empty(),
            "OR-of-eq conservatively not extracted"
        );
        // AND with a non-key conjunct: only the op_type side extracts
        let (c, _) = compile(
            "INSERT INTO ${ds:a.b} SELECT x FROM ${ds:c.d} WHERE op_type IN ('I','U') AND seq > 5",
            1,
        )
        .unwrap();
        assert_eq!(
            c.statements[0].pushdown,
            vec!["I", "U"],
            "AND side extracts normally"
        );
        // NOT IN: no extraction (existing boundary, locked behavior)
        let (c, _) = compile(
            "INSERT INTO ${ds:a.b} SELECT x FROM ${ds:c.d} WHERE op_type NOT IN ('D')",
            1,
        )
        .unwrap();
        assert!(c.statements[0].pushdown.is_empty(), "NOT IN not extracted");
    }

    #[test]
    fn validator_catches_bad_refs_and_kinds() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("tailrace-par-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let f = dir.join("c.toml");
        std::fs::write(
            &f,
            r#"
[pipeline]
id = 7
name = "t"
[[datasource]]
name = "k"
type = "kafka"
bootstrap = "x"
[[table]]
datasource = "k"
name = "t1"
columns = [["a","utf8"]]
"#,
        )?;
        let cat = tailrace_catalog::FileCatalog::load(&f)?;
        // good reference + bad reference
        let (ok, _) = compile("INSERT INTO ${ds:k.t1} SELECT a FROM ${ds:k.t1}", 7)?;
        assert!(validate(&cat, &ok).is_empty());
        let (bad, _) = compile("INSERT INTO ${ds:k.no} SELECT a FROM ${ds:k.t1}", 7)?;
        let d = validate(&cat, &bad);
        assert!(
            d.iter()
                .any(|x| x.message.contains("sink resolution failed")),
            "{d:?}"
        );
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
