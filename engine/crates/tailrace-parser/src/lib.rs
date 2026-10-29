//! tailrace-parser: SQL frontend — pipeline SQL → statement set
//! (StatementSpec, the runtime input).
//!
//! Compile pipeline (the SQL compiler lives in the engine):
//! ① `${ds:ref}` reference extraction → synthetic identifier substitution
//!    (`${ds:12.default.orders_ogg}` → `__ds_0`);
//! ② FlinkDialect multi-statement parsing (v1 narrows to an ANSI subset:
//!    INSERT INTO sink SELECT … FROM source);
//! ③ AST → statement set: FROM tables rewrite to `src` (the runtime's decoded
//!    batch registration name), JOIN dimension tables rewrite to their static
//!    `dim_<alias>` MemTable names (T-D1 broadcast join — the dim object is
//!    loaded FULL-TABLE over JDBC at assembly; NOT a streaming join), WHERE
//!    op_type constant predicates extract into the decode-time pushdown
//!    whitelist, cursor groups follow the `tr.p{pipeline}.s{idx}` convention;
//! ④ Validator: catalog resolves references + source/sink direction checks.
//! **The IR never carries credentials** — references are catalog anchors only.

use datafusion::sql::sqlparser::ast::{
    Expr, JoinOperator, ObjectName, Query, SetExpr, Statement, TableFactor,
};
use datafusion::sql::sqlparser::dialect::Dialect;
use datafusion::sql::sqlparser::parser::Parser;

use tailrace_catalog::{CatalogProvider, DimJoin, StatementSpec};

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
        // A7 partial-column declaration: an explicit INSERT column list
        // (`INSERT INTO sink (k, a) SELECT …`) is the statement-level switch
        // — the sink writes exactly that subset and Doris keeps the unlisted
        // columns' row values (UNIQUE KEY merge-on-write partial update).
        // No list = full-row writes, the historical default.
        let partial_columns = insert_column_list(idx, &ins.columns)?;
        let Some(query) = ins.source.as_deref() else {
            anyhow::bail!("statement #{}: INSERT missing SELECT source", idx);
        };
        let (source_ref, select_sql, pushdown, dims) = rewrite_query(query, &refs)
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
            dims,
            partial_columns,
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
                    "source {} only supports kafka/file/jdbc (got {}{})",
                    st.source,
                    t.kind,
                    // JDBC vs JDBC_TABLE is the classic mix-up (R3-W1 review
                    // low#2): a MySQL/PostgreSQL source declared
                    // `kind = jdbc_table` folds to doris and lands here
                    if t.kind == "doris" {
                        " — jdbc_table is the Doris SINK kind; a MySQL/PostgreSQL source is kind=jdbc"
                    } else {
                        ""
                    }
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
                    "sink {} only supports kafka/file/doris/starrocks (got {}{})",
                    st.sink,
                    t.kind,
                    // mirror mix-up: jdbc is source-only
                    if t.kind == "jdbc" {
                        " — jdbc is source-only (Doris/StarRocks sinks are kind=doris/starrocks)"
                    } else {
                        ""
                    }
                ),
            }),
            Err(e) => out.push(Diagnostic {
                statement: Some(i),
                message: format!("sink resolution failed: {e}"),
            }),
        }
        // T-D1: dim references resolve to kind=dim table objects (broadcast
        // join side — full-table JDBC load at assembly, never a streaming
        // source/sink kind)
        for d in &st.dims {
            match catalog.resolve(&d.reference) {
                Ok(t) if t.kind == "dim" => {}
                Ok(t) => out.push(Diagnostic {
                    statement: Some(i),
                    message: format!(
                        "dim reference {} must be a kind=dim table object (got {}) — \
                         dimension tables are JDBC-backed and loaded full-table for the broadcast join",
                        d.reference, t.kind
                    ),
                }),
                Err(e) => out.push(Diagnostic {
                    statement: Some(i),
                    message: format!("dim resolution failed: {e}"),
                }),
            }
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
        // T33 (sqlparser 0.55→0.62): new dialect-specific variant (function
        // call in object-name position, ast/mod.rs:423). A ${ds:} placeholder
        // is a plain single-part identifier by construction — the function
        // form falls through to the same "not a reference placeholder" None
        // the multi-part case already yields.
        datafusion::sql::sqlparser::ast::ObjectNamePart::Function(_) => None,
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

/// The INSERT column list as the statement's partial-column declaration:
/// `None` when absent (full-row writes), else the plain column names in
/// declaration order. Duplicates are refused here, statement-indexed — the
/// doris sink would refuse them too (colliding JSON keys), but at assembly,
/// after deploy; the editor's compile is the right place to say it.
fn insert_column_list(idx: usize, columns: &[ObjectName]) -> anyhow::Result<Option<Vec<String>>> {
    if columns.is_empty() {
        return Ok(None);
    }
    let mut out: Vec<String> = Vec::with_capacity(columns.len());
    for c in columns {
        let name = ident_name(c).ok_or_else(|| {
            anyhow::anyhow!(
                "statement #{idx}: INSERT column list entries must be plain column names (got {c})"
            )
        })?;
        if out.contains(&name) {
            anyhow::bail!("statement #{idx}: duplicate column {name} in the INSERT column list");
        }
        out.push(name);
    }
    Ok(Some(out))
}

/// FROM table rewritten to src + dim JOINs rewritten to their static
/// `dim_<alias>` MemTable names; returns (source_ref, select_sql, pushdown,
/// dims).
fn rewrite_query(
    query: &Query,
    refs: &std::collections::BTreeMap<String, String>,
) -> anyhow::Result<(String, String, Vec<String>, Vec<DimJoin>)> {
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
            if let Some(rest) = r.strip_prefix("dim.") {
                anyhow::bail!(
                    "${{ds:dim.{rest}}} is a dimension reference — dim tables are JOIN-only \
                     (the FROM source stays a streaming source: kafka/file/jdbc)"
                );
            }
            *name = ObjectName(vec![
                datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier("src".into()),
            ]);
            r
        }
        other => anyhow::bail!("FROM must be a table reference ({:?})", other),
    };
    // T-D1 broadcast join: every JOIN must be a ${ds:dim.datasource.table}
    // reference; it is rewritten to the static dim_<alias> MemTable the
    // runtime loads full-table over JDBC at assembly. v1 has no second
    // streaming source — a non-dim JOIN reference is rejected here.
    //
    // Operator gate: only INNER / bare JOIN / LEFT [OUTER] JOIN widen the
    // stream with dim columns (the broadcast contract, engine-v1.md 维表打宽).
    // RIGHT/FULL re-emit every unmatched dim row to the sink on EVERY
    // micro-batch (the dim side is a static per-statement snapshot) and
    // CROSS multiplies |src|×|dim| per batch — per-batch output
    // amplification the unique-key guard does not prevent, so they are
    // rejected at compile time.
    let mut dims: Vec<DimJoin> = Vec::new();
    for (ji, join) in select.from[0].joins.iter_mut().enumerate() {
        if !matches!(
            join.join_operator,
            JoinOperator::Join(_)
                | JoinOperator::Inner(_)
                | JoinOperator::Left(_)
                | JoinOperator::LeftOuter(_)
        ) {
            anyhow::bail!(
                "JOIN #{ji} ({} against ${{ds:dim.*}} is not supported — dimension joins are \
                 broadcast-widening: use INNER JOIN or LEFT [OUTER] JOIN (RIGHT/FULL re-emit \
                 every unmatched dim row per micro-batch; CROSS multiplies |src|×|dim|)",
                join_operator_name(&join.join_operator)
            );
        }
        let TableFactor::Table { name, alias, .. } = &mut join.relation else {
            anyhow::bail!(
                "JOIN #{ji} must be a ${{ds:dim.datasource.table}} dimension table reference (got {:?})",
                join.relation
            );
        };
        let r = ref_of(name, refs).map_err(|e| {
            anyhow::anyhow!("JOIN #{ji}: {e} — only ${{ds:dim.datasource.table}} dimension joins are supported in v1")
        })?;
        let reference = r.strip_prefix("dim.").ok_or_else(|| {
            anyhow::anyhow!(
                "JOIN #{ji}: ${{ds:{r}}} is not a dimension reference — v1 joins only \
                 ${{ds:dim.datasource.table}} (a second streaming source is not supported)"
            )
        })?;
        let (ds, tbl) = reference.split_once('.').ok_or_else(|| {
            anyhow::anyhow!(
                "JOIN #{ji}: dim reference must be ${{ds:dim.datasource.table}} (got ${{ds:dim.{reference}}})"
            )
        })?;
        if ds.is_empty() || tbl.is_empty() || tbl.contains('.') {
            anyhow::bail!(
                "JOIN #{ji}: dim reference must be ${{ds:dim.datasource.table}} with exactly two non-empty parts (got ${{ds:dim.{reference}}})"
            );
        }
        // registered MemTable name: dim_<alias> (alias defaults to the
        // reference's table part when the SQL omits one)
        let alias_name = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| tbl.to_string());
        let dim_name = format!("dim_{alias_name}");
        if dims.iter().any(|d| d.name == dim_name) {
            anyhow::bail!(
                "JOIN #{ji}: duplicate dimension alias {alias_name:?} — {dim_name} would be registered twice; give each dim join a distinct alias"
            );
        }
        *name = ObjectName(vec![
            datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(dim_name.as_str().into()),
        ]);
        dims.push(DimJoin {
            reference: reference.to_string(),
            name: dim_name,
        });
    }
    let pushdown = select
        .selection
        .as_ref()
        .map(extract_pushdown)
        .unwrap_or_default();
    Ok((source_ref, format!("{}", q), pushdown, dims))
}

/// SQL surface name of a join operator (the dim-join operator gate's
/// diagnostic text).
fn join_operator_name(op: &JoinOperator) -> &'static str {
    match op {
        JoinOperator::Join(_) => "JOIN",
        JoinOperator::Inner(_) => "INNER JOIN",
        JoinOperator::Left(_) => "LEFT JOIN",
        JoinOperator::LeftOuter(_) => "LEFT OUTER JOIN",
        JoinOperator::Right(_) => "RIGHT JOIN",
        JoinOperator::RightOuter(_) => "RIGHT OUTER JOIN",
        JoinOperator::FullOuter(_) => "FULL OUTER JOIN",
        JoinOperator::CrossJoin(_) => "CROSS JOIN",
        JoinOperator::Semi(_) => "SEMI JOIN",
        JoinOperator::LeftSemi(_) => "LEFT SEMI JOIN",
        JoinOperator::RightSemi(_) => "RIGHT SEMI JOIN",
        JoinOperator::Anti(_) => "ANTI JOIN",
        JoinOperator::LeftAnti(_) => "LEFT ANTI JOIN",
        JoinOperator::RightAnti(_) => "RIGHT ANTI JOIN",
        JoinOperator::CrossApply => "CROSS APPLY",
        JoinOperator::OuterApply => "OUTER APPLY",
        JoinOperator::AsOf { .. } => "ASOF JOIN",
        JoinOperator::StraightJoin(_) => "STRAIGHT_JOIN",
        JoinOperator::ArrayJoin => "ARRAY JOIN",
        JoinOperator::LeftArrayJoin => "LEFT ARRAY JOIN",
        JoinOperator::InnerArrayJoin => "INNER ARRAY JOIN",
    }
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

    /// A7 statement-level switch, both states: an INSERT column list compiles
    /// to `partial_columns = Some(names in declaration order)`; a bare INSERT
    /// (the golden example, every pre-A7 pipeline) compiles to `None` — the
    /// statement shape is otherwise identical. Duplicates and non-plain
    /// entries are refused at compile, statement-indexed.
    #[test]
    fn insert_column_list_is_the_partial_columns_switch() -> anyhow::Result<()> {
        // off: the golden example carries no column list anywhere
        let (plain, _) = compile(EXAMPLE, 42)?;
        assert!(
            plain.statements.iter().all(|s| s.partial_columns.is_none()),
            "no INSERT column list = full-row writes"
        );
        // on: the list rides verbatim, in declaration order
        let (on, diags) = compile(
            "INSERT INTO ${ds:4.ods.orders} (order_no, seq, qty) \
             SELECT order_no, seq, qty FROM ${ds:3.shop.orders} WHERE op_type IN ('I','U')",
            42,
        )?;
        assert!(diags.is_empty(), "{diags:?}");
        assert_eq!(
            on.statements[0].partial_columns.as_deref(),
            Some(&["order_no".to_string(), "seq".into(), "qty".into()][..])
        );
        // the rest of the statement is unchanged by the list
        assert_eq!(on.statements[0].sink, "4.ods.orders");
        assert_eq!(on.statements[0].pushdown, vec!["I", "U"]);
        assert!(on.statements[0].select_sql.contains("FROM src"));
        // duplicate entry → refused, naming the statement and the column
        let e = match compile(
            "INSERT INTO ${ds:4.ods.orders} (order_no, order_no) SELECT a, b FROM ${ds:3.shop.orders}",
            42,
        ) {
            Err(e) => e.to_string(),
            Ok((_, d)) => panic!("duplicate column must be rejected (diagnostics: {d:?})"),
        };
        assert!(
            e.contains("statement #0") && e.contains("duplicate column order_no"),
            "{e}"
        );
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

    /// T-D1 (blueprint D narrow path): `JOIN ${ds:dim.datasource.table}`
    /// compiles into StatementSpec.dims + the rewritten `dim_<alias>` table
    /// name inside select_sql; FROM stays `src`. Without an explicit alias the
    /// reference's table part names the MemTable.
    #[test]
    fn dim_join_compiles_to_dims_and_dim_alias_table() -> anyhow::Result<()> {
        let sql = "INSERT INTO ${ds:3.shop.orders_wide} \
                   SELECT s.order_no, d.name FROM ${ds:12.default.orders_ogg} s \
                   LEFT JOIN ${ds:dim.mysql.dim_user} d ON s.uid = d.id";
        let (c, diags) = compile(sql, 42)?;
        assert!(diags.is_empty(), "{diags:?}");
        assert_eq!(c.statements.len(), 1);
        assert_eq!(c.statements[0].source, "12.default.orders_ogg");
        assert_eq!(c.statements[0].sink, "3.shop.orders_wide");
        assert_eq!(
            c.statements[0].dims,
            vec![tailrace_catalog::DimJoin {
                reference: "mysql.dim_user".into(),
                name: "dim_d".into(),
            }],
            "alias d → dim_d, reference = datasource.table after the dim. prefix"
        );
        let sel = &c.statements[0].select_sql;
        assert!(sel.contains("FROM src"), "{sel}");
        assert!(
            sel.contains("LEFT JOIN dim_d d ON s.uid = d.id"),
            "join rewritten to the static dim_d MemTable (alias preserved): {sel}"
        );

        // no alias: the reference's table part names the MemTable
        let (c2, _) = compile(
            "INSERT INTO ${ds:a.b} SELECT s.x, id FROM ${ds:c.d} s \
             INNER JOIN ${ds:dim.e.f} ON s.x = id",
            1,
        )?;
        assert_eq!(c2.statements[0].dims[0].name, "dim_f");
        assert!(
            c2.statements[0].select_sql.contains("INNER JOIN dim_f ON"),
            "{}",
            c2.statements[0].select_sql
        );
        Ok(())
    }

    /// T-D1 rejections: a second streaming source in JOIN position, dim in
    /// FROM position, malformed dim references, and duplicate dim aliases.
    #[test]
    fn dim_join_rejects_non_dim_and_malformed_refs() {
        let err = |sql: &str| match compile(sql, 1) {
            Err(e) => e.to_string(),
            Ok((_, d)) => panic!("must be rejected (diagnostics: {d:?})"),
        };
        // JOIN to a second streaming source: v1 is single-source
        let e =
            err("INSERT INTO ${ds:a.b} SELECT s.x FROM ${ds:c.d} s JOIN ${ds:e.f} g ON s.x = g.x");
        assert!(
            e.contains("not a dimension reference"),
            "second streaming source in JOIN must be rejected: {e}"
        );
        // dim reference in FROM position (streaming side stays streaming)
        let e = err("INSERT INTO ${ds:a.b} SELECT x FROM ${ds:dim.c.d}");
        assert!(
            e.contains("dimension reference") && e.contains("JOIN-only"),
            "dim in FROM must be rejected: {e}"
        );
        // malformed: missing table part / extra parts
        let e = err(
            "INSERT INTO ${ds:a.b} SELECT s.x FROM ${ds:c.d} s JOIN ${ds:dim.onlyds} d ON s.x = d.x",
        );
        assert!(
            e.contains("must be ${ds:dim.datasource.table}"),
            "one-part dim ref rejected: {e}"
        );
        let e = err(
            "INSERT INTO ${ds:a.b} SELECT s.x FROM ${ds:c.d} s JOIN ${ds:dim.e.f.g} d ON s.x = d.x",
        );
        assert!(
            e.contains("exactly two non-empty parts"),
            "three-part dim ref rejected: {e}"
        );
        // duplicate dim alias → duplicate MemTable registration
        let e = err("INSERT INTO ${ds:a.b} SELECT s.x FROM ${ds:c.d} s \
             JOIN ${ds:dim.e.f} d ON s.x = d.x JOIN ${ds:dim.g.h} d ON s.y = d.y");
        assert!(
            e.contains("duplicate dimension alias"),
            "duplicate alias rejected: {e}"
        );
        // a derived table in JOIN position is not a dim reference
        let e = err(
            "INSERT INTO ${ds:a.b} SELECT s.x FROM ${ds:c.d} s JOIN (SELECT 1 AS x) t ON s.x = t.x",
        );
        assert!(
            e.contains("dimension table reference"),
            "derived table in JOIN rejected: {e}"
        );
    }

    /// T-D1 operator gate: only INNER / bare JOIN / LEFT [OUTER] JOIN widen
    /// the stream with dim columns (broadcast-widening contract). RIGHT and
    /// FULL would re-emit every unmatched dim row per micro-batch and CROSS
    /// multiplies |src|×|dim| — per-batch output amplification the
    /// unique-key guard does not prevent, so each is rejected at compile
    /// time with a diagnostic naming the allowed forms.
    #[test]
    fn dim_join_rejects_right_full_cross_operators() {
        let err = |sql: &str| match compile(sql, 1) {
            Err(e) => e.to_string(),
            Ok((_, d)) => panic!("must be rejected (diagnostics: {d:?})"),
        };
        // RIGHT JOIN / RIGHT OUTER JOIN
        for sql in [
            "INSERT INTO ${ds:a.b} SELECT s.x, d.y FROM ${ds:c.d} s RIGHT JOIN ${ds:dim.e.f} d ON s.x = d.x",
            "INSERT INTO ${ds:a.b} SELECT s.x, d.y FROM ${ds:c.d} s RIGHT OUTER JOIN ${ds:dim.e.f} d ON s.x = d.x",
        ] {
            let e = err(sql);
            assert!(
                e.contains("RIGHT") && e.contains("INNER JOIN or LEFT"),
                "RIGHT dim join rejected with the allowed forms: {e}"
            );
        }
        // FULL OUTER JOIN
        let e = err(
            "INSERT INTO ${ds:a.b} SELECT s.x, d.y FROM ${ds:c.d} s FULL OUTER JOIN ${ds:dim.e.f} d ON s.x = d.x",
        );
        assert!(
            e.contains("FULL OUTER JOIN") && e.contains("broadcast-widening"),
            "FULL dim join rejected: {e}"
        );
        // CROSS JOIN (no ON)
        let e = err(
            "INSERT INTO ${ds:a.b} SELECT s.x, d.y FROM ${ds:c.d} s CROSS JOIN ${ds:dim.e.f} d",
        );
        assert!(
            e.contains("CROSS JOIN") && e.contains("multiplies"),
            "CROSS dim join rejected: {e}"
        );
        // the accepted forms still compile (bare JOIN = INNER by SQL semantics)
        for sql in [
            "INSERT INTO ${ds:a.b} SELECT s.x, d.y FROM ${ds:c.d} s JOIN ${ds:dim.e.f} d ON s.x = d.x",
            "INSERT INTO ${ds:a.b} SELECT s.x, d.y FROM ${ds:c.d} s LEFT JOIN ${ds:dim.e.f} d ON s.x = d.x",
            "INSERT INTO ${ds:a.b} SELECT s.x, d.y FROM ${ds:c.d} s LEFT OUTER JOIN ${ds:dim.e.f} d ON s.x = d.x",
        ] {
            let (c, diags) = compile(sql, 1).expect("widening operators accepted");
            assert!(diags.is_empty(), "{sql}: {diags:?}");
            assert_eq!(c.statements[0].dims.len(), 1, "{sql}");
        }
    }

    /// T-D1 validator: dim references must resolve to kind=dim table objects;
    /// a wrong kind (e.g. the jdbc streaming source kind) gets a diagnostic
    /// naming the broadcast-join contract.
    #[test]
    fn validator_checks_dim_refs_resolve_to_dim_kind() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("tailrace-par-dim-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let f = dir.join("d.toml");
        std::fs::write(
            &f,
            r#"
[pipeline]
id = 11
name = "t"
[[datasource]]
name = "mysql"
type = "mysql"
bootstrap = "mysql://root@localhost/test"
[[table]]
datasource = "mysql"
name = "dim_user"
kind = "dim"
columns = [["id","int64"],["name","utf8"]]
sequence_column = "id"
[[table]]
datasource = "mysql"
name = "plain_src"
kind = "jdbc"
columns = [["id","int64"]]
[[table]]
datasource = "mysql"
name = "wide_out"
kind = "file"
columns = [["id","int64"],["name","utf8"]]
"#,
        )?;
        let cat = tailrace_catalog::FileCatalog::load(&f)?;
        // good: dim ref resolves to kind=dim
        let (ok, _) = compile(
            "INSERT INTO ${ds:mysql.wide_out} SELECT s.id, d.name FROM ${ds:mysql.plain_src} s \
             LEFT JOIN ${ds:dim.mysql.dim_user} d ON s.id = d.id",
            11,
        )?;
        assert!(validate(&cat, &ok).is_empty(), "kind=dim dim ref passes");
        // bad: dim ref points at a kind=jdbc streaming source object
        let (bad, _) = compile(
            "INSERT INTO ${ds:mysql.wide_out} SELECT s.id, d.name FROM ${ds:mysql.plain_src} s \
             LEFT JOIN ${ds:dim.mysql.plain_src} d ON s.id = d.id",
            11,
        )?;
        let d = validate(&cat, &bad);
        assert!(
            d.iter()
                .any(|x| x.message.contains("must be a kind=dim table object")),
            "{d:?}"
        );
        // bad: dim ref does not resolve at all
        let (bad, _) = compile(
            "INSERT INTO ${ds:mysql.wide_out} SELECT s.id, d.name FROM ${ds:mysql.plain_src} s \
             LEFT JOIN ${ds:dim.mysql.no_such} d ON s.id = d.id",
            11,
        )?;
        let d = validate(&cat, &bad);
        assert!(
            d.iter()
                .any(|x| x.message.contains("dim resolution failed")),
            "{d:?}"
        );
        std::fs::remove_dir_all(&dir)?;
        Ok(())
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

    /// R3-W1 review low#2: a MySQL/PostgreSQL source mistakenly declared
    /// `kind = jdbc_table` folds to doris — the diagnostic must name the
    /// JDBC (source) / JDBC_TABLE (Doris sink) mix-up instead of a bare
    /// "got doris"; dually, jdbc used as a sink must be called source-only.
    #[test]
    fn validator_names_the_jdbc_jdbc_table_mixup() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("tailrace-par-jdbc-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let f = dir.join("j.toml");
        std::fs::write(
            &f,
            r#"
[pipeline]
id = 9
name = "t"
[[datasource]]
name = "d"
type = "mysql"
bootstrap = "mysql://root@localhost/test"
[[table]]
datasource = "d"
name = "orders"
kind = "JDBC_TABLE"
columns = [["id","int64"]]
[[table]]
datasource = "d"
name = "orders_jdbc"
kind = "jdbc"
columns = [["id","int64"]]
"#,
        )?;
        let cat = tailrace_catalog::FileCatalog::load(&f)?;
        // JDBC_TABLE (→ doris) as a SOURCE: the hint must name the mix-up
        let (c, _) = compile(
            "INSERT INTO ${ds:d.orders} SELECT id FROM ${ds:d.orders}",
            9,
        )?;
        let d = validate(&cat, &c);
        assert!(
            d.iter()
                .any(|x| x.message.contains("jdbc_table is the Doris SINK kind")),
            "{d:?}"
        );
        // jdbc as a SINK: must be called source-only
        let (c, _) = compile(
            "INSERT INTO ${ds:d.orders_jdbc} SELECT id FROM ${ds:d.orders_jdbc}",
            9,
        )?;
        let d = validate(&cat, &c);
        assert!(
            d.iter().any(|x| x.message.contains("jdbc is source-only")),
            "{d:?}"
        );
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
