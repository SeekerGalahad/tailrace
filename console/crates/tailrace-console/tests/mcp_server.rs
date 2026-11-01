//! MCP server (T-E4, blueprint §7.5) over the real router: initialize +
//! tools/list surface, the two write gates (process switch, confirm:true),
//! read tools and actions against the mock engine, session-based actor
//! labelling, JSON-RPC error handling, the rate limit and the audit trail
//! every call leaves behind.

mod common;

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use common::*;
use console_store::Store;
use serde_json::{json, Value};
use tailrace_console::mcp::McpState;
use tower::ServiceExt;

/// §7.5 table order + the R8 four + the R11 apply pair — `tools/list` must
/// expose exactly these sixteen (the pause and resume routes share the
/// `pause_resume_pipeline` slot; the DDL closed loop, backfill status and
/// the pipeline-as-code pair ride at the end).
const ALL_TOOLS: [&str; 16] = [
    "list_datasources",
    "list_pipelines",
    "get_pipeline",
    "get_pipeline_stats",
    "get_pipeline_logs",
    "compile_sql",
    "create_pipeline",
    "deploy_pipeline",
    "pause_resume_pipeline",
    "trigger_backfill",
    "ddl_draft",
    "ddl_execute",
    "ddl_evolve",
    "backfill_status",
    "apply",
    "apply_dry_run",
];
const WRITE_TOOLS: [&str; 7] = [
    "create_pipeline",
    "deploy_pipeline",
    "pause_resume_pipeline",
    "trigger_backfill",
    "ddl_execute",
    "ddl_evolve",
    "apply",
];

// ---------------- helpers ----------------

/// Raw `POST /mcp` → (status, headers, parsed body — Null when empty).
async fn post_mcp(
    app: &TestApp,
    headers: &[(&str, &str)],
    body: &str,
) -> (StatusCode, HeaderMap, Value) {
    let mut builder = Request::post("/mcp").header("content-type", "application/json");
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let resp = app
        .router
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    (status, headers, json_body(resp).await)
}

/// One JSON-RPC request (id 1); asserts HTTP 200 and returns the response.
async fn rpc(app: &TestApp, headers: &[(&str, &str)], method: &str, params: Value) -> Value {
    let request = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    let (status, _h, body) = post_mcp(app, headers, &request.to_string()).await;
    assert_eq!(status, StatusCode::OK, "{method}: {body}");
    assert_eq!(body["jsonrpc"], "2.0", "{method}: {body}");
    assert_eq!(body["id"], 1, "{method}: {body}");
    body
}

async fn tool(app: &TestApp, headers: &[(&str, &str)], name: &str, args: Value) -> Value {
    rpc(
        app,
        headers,
        "tools/call",
        json!({ "name": name, "arguments": args }),
    )
    .await
}

/// Successful tools/call → (httpStatus, REST body). Asserts isError:false
/// and text content presence.
fn tool_ok(resp: &Value) -> (u16, Value) {
    assert!(resp.get("error").is_none(), "protocol error: {resp}");
    let r = &resp["result"];
    assert_eq!(r["isError"], false, "tool reported isError: {r}");
    assert_eq!(r["content"][0]["type"], "text", "{r}");
    assert!(!r["content"][0]["text"].as_str().unwrap().is_empty(), "{r}");
    (
        r["structuredContent"]["httpStatus"].as_u64().unwrap() as u16,
        r["structuredContent"]["body"].clone(),
    )
}

/// Failed tools/call (REST non-2xx) → (httpStatus, REST error body).
fn tool_err(resp: &Value) -> (u16, Value) {
    assert!(resp.get("error").is_none(), "protocol error: {resp}");
    let r = &resp["result"];
    assert_eq!(r["isError"], true, "expected isError: {r}");
    (
        r["structuredContent"]["httpStatus"].as_u64().unwrap() as u16,
        r["structuredContent"]["body"].clone(),
    )
}

/// Audit rows for one action: (actor, entity_id, parsed detail) in
/// insertion order.
fn audit_rows(store: &Store, action: &str) -> Vec<(String, Option<String>, Value)> {
    store
        .with_conn(|c| {
            let mut stmt = c.prepare(
                "SELECT actor, entity_id, detail_json FROM audit_log WHERE action = ?1 ORDER BY id",
            )?;
            let mut out: Vec<(String, Option<String>, Value)> = Vec::new();
            let rows = stmt.query_map([action], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, Option<String>>(2)?,
                ))
            })?;
            for row in rows {
                let (actor, entity, detail) = row?;
                let detail = detail
                    .and_then(|d| serde_json::from_str(&d).ok())
                    .unwrap_or(Value::Null);
                out.push((actor, entity, detail));
            }
            Ok(out)
        })
        .unwrap()
}

// ---------------- initialize / tools/list ----------------

#[tokio::test]
async fn initialize_and_tools_list_expose_the_sixteen_tools() {
    let app = app_with_mcp(false).await;

    // initialize: negotiation, server info, session id, instructions
    let request = json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "protocolVersion": "2025-06-18", "capabilities": {},
                    "clientInfo": { "name": "mcp-test", "version": "0.1" } }
    });
    let (status, headers, body) = post_mcp(&app, &[], &request.to_string()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let r = &body["result"];
    assert_eq!(r["protocolVersion"], "2025-06-18");
    assert_eq!(r["serverInfo"]["name"], "tailrace-console");
    assert_eq!(r["serverInfo"]["version"], env!("CARGO_PKG_VERSION"));
    assert!(r["capabilities"]["tools"].is_object());
    assert!(r["instructions"].as_str().unwrap().contains("DISABLED"));
    let sid = headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .expect("Mcp-Session-Id header on initialize");
    assert_eq!(sid.len(), 32, "32 hex chars");
    assert!(sid.chars().all(|c| c.is_ascii_hexdigit()));

    // unknown requested revision → newest supported; known older → echoed
    let b = rpc(
        &app,
        &[],
        "initialize",
        json!({"protocolVersion": "1999-01-01", "clientInfo": {"name": "x"}}),
    )
    .await;
    assert_eq!(b["result"]["protocolVersion"], "2025-06-18");
    let b = rpc(
        &app,
        &[],
        "initialize",
        json!({"protocolVersion": "2025-03-26", "clientInfo": {"name": "x"}}),
    )
    .await;
    assert_eq!(b["result"]["protocolVersion"], "2025-03-26");

    // notifications (no id) → 202 Accepted, no body
    let (status, _h, body) = post_mcp(
        &app,
        &[],
        &json!({"jsonrpc": "2.0", "method": "notifications/initialized"}).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body, Value::Null);

    // ping
    let b = rpc(&app, &[], "ping", json!({})).await;
    assert_eq!(b["result"], json!({}));

    // tools/list: exactly the sixteen (§7.5 ten + R8 four + R11 apply
    // pair), in table order, well-formed; descriptions are route-prefixed
    // (the OpenAPI same-source generation)
    let b = rpc(&app, &[], "tools/list", json!({})).await;
    let tools = b["result"]["tools"].as_array().expect("tools array");
    assert_eq!(tools.len(), 16, "{tools:#?}");
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(names, ALL_TOOLS);
    assert!(
        b["result"].get("nextCursor").is_none(),
        "all sixteen in one page"
    );
    for t in tools {
        let name = t["name"].as_str().unwrap();
        let description = t["description"].as_str().unwrap();
        assert!(description.len() > 20, "{name}: non-trivial description");
        // the route prefix is the same-source fingerprint: the description
        // is generated from the OpenAPI document, not hand-written here
        let route = match name {
            "ddl_draft" => "POST /api/datasources/{id}/tables/{db}/{table}/ddl/draft",
            "ddl_execute" => "POST /api/datasources/{id}/tables/{db}/{table}/ddl/execute",
            "ddl_evolve" => "POST /api/datasources/{id}/tables/{db}/{table}/ddl/evolve",
            "backfill_status" => "GET /api/pipelines/{id}/backfill/jobs",
            "apply" => "POST /api/apply",
            "apply_dry_run" => "POST /api/apply/dry-run",
            _ => continue,
        };
        assert!(
            description.starts_with(route),
            "{name}: OpenAPI-sourced description is route-prefixed: {description}"
        );
        assert_eq!(t["inputSchema"]["type"], "object", "{name}: schema type");
        let is_write = WRITE_TOOLS.contains(&name);
        assert_eq!(
            t["annotations"]["readOnlyHint"], !is_write,
            "{name}: readOnlyHint"
        );
        let required = t["inputSchema"]["required"].as_array().cloned();
        assert_eq!(
            required.is_some_and(|r| r.contains(&json!("confirm"))),
            is_write,
            "{name}: confirm required iff write"
        );
        if is_write {
            assert_eq!(
                t["inputSchema"]["properties"]["confirm"]["const"], true,
                "{name}: confirm is const true"
            );
        }
    }

    // instructions flip with the switch
    let app_rw = app_with_mcp(true).await;
    let b = rpc(
        &app_rw,
        &[],
        "initialize",
        json!({"protocolVersion": "2025-06-18", "clientInfo": {"name": "x"}}),
    )
    .await;
    assert!(b["result"]["instructions"]
        .as_str()
        .unwrap()
        .contains("ENABLED"));
}

// ---------------- write gate 1: process switch ----------------

#[tokio::test]
async fn write_tools_are_rejected_while_the_switch_is_off() {
    let app = app_with_mcp(false).await;
    let hdr = [("x-mcp-client", "gate-test")];
    let calls = [
        (
            "create_pipeline",
            json!({"confirm": true, "name": "p", "sqlContent": "-- x"}),
        ),
        ("deploy_pipeline", json!({"confirm": true, "id": 1})),
        (
            "pause_resume_pipeline",
            json!({"confirm": true, "id": 1, "action": "pause"}),
        ),
        ("trigger_backfill", json!({"confirm": true, "id": 1})),
        (
            "ddl_execute",
            json!({"confirm": true, "datasourceId": 1, "db": "ods", "table": "orders",
                    "ddl": "CREATE TABLE …", "keyModel": "unique"}),
        ),
        (
            "ddl_evolve",
            json!({"confirm": true, "datasourceId": 1, "db": "ods", "table": "orders"}),
        ),
        (
            "apply",
            json!({"confirm": true, "pipelines": [
                {"name": "p", "sqlContent": "SELECT 1"}
            ]}),
        ),
    ];
    for (name, args) in &calls {
        let b = tool(&app, &hdr, name, args.clone()).await;
        assert_eq!(b["error"]["code"], -32001, "{name}: {b}");
        assert_eq!(b["error"]["data"]["code"], "WRITE_DISABLED", "{name}: {b}");
        assert_eq!(b["error"]["data"]["tool"], *name);
        assert!(
            b["error"]["message"]
                .as_str()
                .unwrap()
                .contains("--mcp-allow-write"),
            "{name}: message names the switch"
        );
        assert!(b.get("result").is_none(), "{name}: no result on error");
    }
    // nothing was created
    let (_, body) = tool_ok(&tool(&app, &hdr, "list_pipelines", json!({})).await);
    assert_eq!(body["total"], 0);
    // every rejected attempt is audited under the MCP actor — no mutation rows
    let rows = audit_rows(&app.store, "MCP_TOOL_CALL");
    let rejected: Vec<_> = rows
        .iter()
        .filter(|(_, _, d)| d["code"] == "WRITE_DISABLED")
        .collect();
    assert_eq!(rejected.len(), 7, "{rows:#?}");
    for (actor, entity, detail) in &rejected {
        assert_eq!(actor, "mcp:gate-test");
        assert!(
            WRITE_TOOLS.contains(&entity.as_deref().unwrap()),
            "{entity:?}"
        );
        assert_eq!(detail["outcome"], "rejected");
        assert_eq!(detail["tool"], entity.as_deref().unwrap());
    }
    // 7 rejected + 1 ok read = the complete MCP audit trail
    assert_eq!(rows.len(), 8, "{rows:#?}");
    assert!(audit_rows(&app.store, "PIPELINE_CREATE").is_empty());
    assert!(
        audit_rows(&app.store, "TABLE_DDL_EXECUTE").is_empty(),
        "the DDL write never reached its handler"
    );
    assert!(
        audit_rows(&app.store, "APPLY").is_empty(),
        "the declaration file never reached the apply handler"
    );
}

// ---------------- write gate 2: confirm:true ----------------

#[tokio::test]
async fn write_tools_require_confirm_true_even_with_the_switch_on() {
    let app = app_with_mcp(true).await;
    let hdr = [("x-mcp-client", "confirm-test")];
    let base = json!({"name": "mcp-draft", "sqlContent": PIPELINE_SQL});

    // absent / false / string "true" / number 1 → all CONFIRM_REQUIRED
    for confirm in [Value::Null, json!(false), json!("true"), json!(1)] {
        let mut args = base.clone();
        if !confirm.is_null() {
            args["confirm"] = confirm.clone();
        }
        let b = tool(&app, &hdr, "create_pipeline", args).await;
        assert_eq!(b["error"]["code"], -32602, "confirm={confirm}: {b}");
        assert_eq!(
            b["error"]["data"]["code"], "CONFIRM_REQUIRED",
            "confirm={confirm}: {b}"
        );
    }
    let (_, body) = tool_ok(&tool(&app, &hdr, "list_pipelines", json!({})).await);
    assert_eq!(body["total"], 0, "nothing created without confirm");

    // confirm:true → the REST create runs: 201 + id
    let mut args = base.clone();
    args["confirm"] = json!(true);
    let b = tool(&app, &hdr, "create_pipeline", args).await;
    let (status, body) = tool_ok(&b);
    assert_eq!(status, 201);
    let pid = body["id"].as_i64().expect("created id");
    // text content mirrors the REST body (`PipelineCreated {id}`, pretty-printed)
    let text = b["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains(&format!("\"id\": {pid}")),
        "text carries the body: {text}"
    );

    let (_, body) = tool_ok(&tool(&app, &hdr, "get_pipeline", json!({"id": pid})).await);
    assert_eq!(body["name"], "mcp-draft");
    assert_eq!(body["status"], "DRAFT");
    assert_eq!(body["sqlContent"], PIPELINE_SQL);

    // audit: MCP envelope row (ok, httpStatus) + the handler's own domain
    // row, same actor; the four confirm rejections are recorded as well
    let rows = audit_rows(&app.store, "MCP_TOOL_CALL");
    let ok_create = rows
        .iter()
        .find(|(_, e, d)| e.as_deref() == Some("create_pipeline") && d["outcome"] == "ok")
        .expect("ok create audit");
    assert_eq!(ok_create.0, "mcp:confirm-test");
    assert_eq!(ok_create.2["httpStatus"], 201);
    assert_eq!(ok_create.2["args"]["name"], "mcp-draft");
    assert_eq!(
        ok_create.2["args"]["confirm"], true,
        "the acknowledgement is part of the audit evidence"
    );
    assert_eq!(
        rows.iter()
            .filter(|(_, _, d)| d["code"] == "CONFIRM_REQUIRED")
            .count(),
        4
    );
    let domain = audit_rows(&app.store, "PIPELINE_CREATE");
    assert_eq!(domain.len(), 1, "handler's own audit row");
    assert_eq!(domain[0].0, "mcp:confirm-test");
    assert_eq!(domain[0].1.as_deref(), Some(pid.to_string().as_str()));
}

// ---------------- R8: DDL closed loop + backfill status ----------------

/// The R8 tool quartet: ddl_draft / backfill_status are read tools (no
/// confirm, no switch — draft is pure text composition, status is a list);
/// ddl_execute / ddl_evolve ride both write gates and leave the handler's
/// own domain audit row under the MCP actor. FE-less assertions only:
/// draft is pure, execute and evolve fail against the unreachable FE
/// (their 502 envelopes), which is exactly the side the gates live on.
/// The draft leg also crosses into dogfooding #3: a table registered with
/// a sequenceColumn surfaces the R8 `function_column.sequence_col`
/// attribute through the tool (and not under a duplicate override).
#[tokio::test]
async fn ddl_and_backfill_tools_round_trip_the_r8_surface() {
    let app = app_with_mcp(true).await;
    let hdr = [("x-mcp-client", "ddl-mcp")];
    let (_kafka, doris, _, _) = seed_datasources(&app).await;
    let (s, b) = req(
        &app,
        "POST",
        &format!("/api/datasources/{doris}/tables"),
        Some(json!({
            "dbName": "ods", "tableName": "orders_seq", "format": null,
            "columnsJson": [["order_no", "utf8"], ["qty", "int64"], ["seq", "int64"]],
            "sequenceColumn": "seq"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let tid = b["id"].as_i64().unwrap();

    // ddl_draft: read tool, no confirm needed — the generator's two states
    // ride through (CDC evidence recommends UNIQUE; the declared seq column
    // becomes the table-level attribute)
    let (status, body) = tool_ok(
        &tool(
            &app,
            &hdr,
            "ddl_draft",
            json!({"datasourceId": doris, "db": "ods", "table": "orders_seq"}),
        )
        .await,
    );
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["keyModel"], "unique", "{body}");
    let ddl = body["ddl"].as_str().unwrap();
    assert!(ddl.contains("UNIQUE KEY(`order_no`)"), "{ddl}");
    assert!(
        ddl.contains(r#""function_column.sequence_col" = "seq""#),
        "dogfooding #3: the attribute is part of the draft: {ddl}"
    );
    // the duplicate override drops the attribute (no dedup story)
    let (_, body) = tool_ok(
        &tool(
            &app,
            &hdr,
            "ddl_draft",
            json!({"datasourceId": doris, "db": "ods", "table": "orders_seq",
                    "keyModel": "duplicate", "keyColumn": "order_no"}),
        )
        .await,
    );
    let ddl = body["ddl"].as_str().unwrap();
    assert!(ddl.contains("DUPLICATE KEY(`order_no`)"), "{ddl}");
    assert!(!ddl.contains("function_column.sequence_col"), "{ddl}");
    // a kafka table is not a DDL target — the REST 400 rides as a tool error
    let (status, body) = tool_err(
        &tool(
            &app,
            &hdr,
            "ddl_draft",
            json!({"datasourceId": _kafka, "db": "shop", "table": "orders"}),
        )
        .await,
    );
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"]["message_key"], "err.ddl.datasource_kind");

    // ddl_execute without confirm → CONFIRM_REQUIRED, audited, no handler row
    let b = tool(
        &app,
        &hdr,
        "ddl_execute",
        json!({"datasourceId": doris, "db": "ods", "table": "orders_seq",
                "ddl": "CREATE TABLE `ods`.`orders` (… UNIQUE KEY …);", "keyModel": "unique"}),
    )
    .await;
    assert_eq!(b["error"]["data"]["code"], "CONFIRM_REQUIRED", "{b}");

    // ddl_execute with confirm: the FE is unreachable → the REST 502
    // DDL_FAILED surfaces as a tool error AND the attempt is audit
    // evidence (handler domain row ok:false under the MCP actor)
    let (status, body) = tool_err(
        &tool(
            &app,
            &hdr,
            "ddl_execute",
            json!({"confirm": true, "datasourceId": doris, "db": "ods", "table": "orders_seq",
                    "ddl": "CREATE TABLE IF NOT EXISTS `ods`.`orders_seq` (\n  `order_no` VARCHAR(65533)\n)\nUNIQUE KEY(`order_no`)\nDISTRIBUTED BY HASH(`order_no`) BUCKETS AUTO;",
                    "keyModel": "unique"}),
        )
        .await,
    );
    assert_eq!(status, 502, "{body}");
    assert_eq!(body["error"]["code"], "DDL_FAILED", "{body}");
    let rows = audit_rows(&app.store, "TABLE_DDL_EXECUTE");
    assert_eq!(rows.len(), 1, "the handler's own audit row: {rows:#?}");
    assert_eq!(rows[0].0, "mcp:ddl-mcp");
    assert_eq!(rows[0].1.as_deref(), Some(tid.to_string().as_str()));
    assert_eq!(rows[0].2["ok"], false);
    assert_eq!(rows[0].2["keyModel"], "unique");

    // ddl_evolve with confirm: same gates, the DESC against the dead FE is
    // the 502 UNAVAILABLE
    let (status, body) = tool_err(
        &tool(
            &app,
            &hdr,
            "ddl_evolve",
            json!({"confirm": true, "datasourceId": doris, "db": "ods", "table": "orders_seq"}),
        )
        .await,
    );
    assert_eq!(status, 502, "{body}");
    assert_eq!(body["error"]["code"], "UNAVAILABLE", "{body}");
    assert_eq!(
        audit_rows(&app.store, "TABLE_DDL_EVOLVE").len(),
        0,
        "a DESC failure executes nothing, audits nothing on the domain side"
    );

    // backfill_status: read tool over an unpublishable-but-existing
    // pipeline draft — the job list, empty until a run is accepted
    let (s, b) = req(
        &app,
        "POST",
        "/api/pipelines",
        Some(json!({"name": "mcp-bf", "sqlContent": PIPELINE_SQL,
                    "dsMapping": {"shop": _kafka, "ods": doris}, "checkpointMs": 10000})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let pid = b["id"].as_i64().unwrap();
    let (status, body) = tool_ok(&tool(&app, &hdr, "backfill_status", json!({"id": pid})).await);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["jobs"].as_array().unwrap().len(), 0, "{body}");

    // the MCP envelope rows carry the whole story: the reads ok (draft ×3
    // — unique, duplicate override, kafka refusal), the confirm rejection
    // rejected, execute+evolve error
    let rows = audit_rows(&app.store, "MCP_TOOL_CALL");
    let by = |tool: &str| {
        rows.iter()
            .filter(|(_, e, _)| e.as_deref() == Some(tool))
            .count()
    };
    assert_eq!(
        by("ddl_draft"),
        3,
        "unique + duplicate + kafka refusal: {rows:#?}"
    );
    assert_eq!(by("ddl_execute"), 2, "confirm rejection + 502: {rows:#?}");
    assert_eq!(by("ddl_evolve"), 1, "{rows:#?}");
    assert_eq!(by("backfill_status"), 1, "{rows:#?}");
}

// ---------------- R11: the pipeline-as-code apply pair ----------------

/// The T-G6 pair as tools. `apply_dry_run` is a read tool (no switch, no
/// confirm) with ZERO side effects: the structured diff comes back, and
/// the store and the audit trail are byte-untouched except for this call's
/// own MCP_TOOL_CALL envelope row. `apply` rides both write gates and, once
/// through, lands exactly the plan the dry run showed — its per-write
/// domain rows and the APPLY summary row carry the MCP actor. Same
/// handler, same `plan()`: the diff an MCP client previews is the diff
/// apply lands; `confirm` is lifted out before the handler sees the file
/// (the handler's own top-level key check would otherwise 400 it).
#[tokio::test]
async fn apply_tools_share_the_g6_diff_and_dry_run_writes_nothing() {
    let app = app_with_mcp(true).await;
    let hdr = [("x-mcp-client", "gitops-agent")];
    // the world the file diffs against: one kafka datasource registered
    // over REST (so `edge-kafka` reads `unchanged`), nothing else
    let (s, b) = req(
        &app,
        "POST",
        "/api/datasources",
        Some(json!({
            "name": "edge-kafka", "type": "kafka",
            "connJson": {"bootstrap": "127.0.0.1:19092"}
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let kafka = b["id"].as_i64().unwrap();
    let file = json!({
        "datasources": [
            {"name": "edge-kafka", "type": "kafka", "connJson": {"bootstrap": "127.0.0.1:19092"}}
        ],
        "tables": [
            {"datasource": "edge-kafka", "dbName": "shop", "tableName": "orders", "format": "ogg",
             "columnsJson": [["id", "int64"], ["payload", "utf8"]]}
        ],
        "pipelines": [
            {"name": "orders→ods",
             "sqlContent": "INSERT INTO ods.orders SELECT id FROM shop.orders",
             "dsMapping": {"shop": "edge-kafka"}}
        ]
    });
    let total_audit = || -> i64 {
        app.store
            .with_conn(|c| Ok(c.query_row("SELECT COUNT(*) FROM audit_log", [], |r| r.get(0))?))
            .unwrap()
    };

    // --- apply_dry_run: read tool, no confirm — the structured diff
    let before = total_audit();
    let (status, diff) = tool_ok(&tool(&app, &hdr, "apply_dry_run", file.clone()).await);
    assert_eq!(status, 200, "{diff}");
    assert_eq!(diff["dryRun"], true);
    assert_eq!(
        diff["summary"],
        json!({"create": 2, "update": 0, "unchanged": 1, "orphan": 0})
    );
    assert_eq!(diff["datasources"][0]["action"], "unchanged");
    assert_eq!(diff["datasources"][0]["id"], kafka);
    assert_eq!(diff["tables"][0]["action"], "create");
    assert_eq!(
        diff["tables"][0]["id"],
        Value::Null,
        "dry-run create has no id yet"
    );
    assert_eq!(diff["pipelines"][0]["action"], "create");
    // zero side effects: nothing created, nothing audited beyond the one
    // MCP envelope row of this call
    assert_eq!(
        total_audit(),
        before + 1,
        "exactly the MCP_TOOL_CALL row, no domain / APPLY rows"
    );
    let (_, body) = tool_ok(&tool(&app, &hdr, "list_pipelines", json!({})).await);
    assert_eq!(body["total"], 0, "dry run created nothing");
    let (s, b) = req(&app, "GET", "/api/datasources/tables", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["total"], 0, "dry run registered nothing");
    assert!(audit_rows(&app.store, "APPLY").is_empty());
    assert!(audit_rows(&app.store, "TABLE_CREATE").is_empty());
    assert!(audit_rows(&app.store, "PIPELINE_CREATE").is_empty());

    // --- apply without confirm → CONFIRM_REQUIRED, audited, nothing written
    let b = tool(&app, &hdr, "apply", file.clone()).await;
    assert_eq!(b["error"]["code"], -32602, "{b}");
    assert_eq!(b["error"]["data"]["code"], "CONFIRM_REQUIRED", "{b}");
    assert!(audit_rows(&app.store, "APPLY").is_empty());

    // --- apply with confirm: the same plan lands
    let mut args = file.clone();
    args["confirm"] = json!(true);
    let (status, result) = tool_ok(&tool(&app, &hdr, "apply", args).await);
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["dryRun"], false);
    assert_eq!(
        result["summary"], diff["summary"],
        "apply lands the diff the dry run showed"
    );
    let pid = result["pipelines"][0]["id"]
        .as_i64()
        .expect("created pipeline id");
    let tid = result["tables"][0]["id"]
        .as_i64()
        .expect("created table id");
    let (_, body) = tool_ok(&tool(&app, &hdr, "get_pipeline", json!({"id": pid})).await);
    assert_eq!(body["name"], "orders→ods");
    assert_eq!(
        body["dsMapping"],
        json!({"shop": kafka}),
        "the dsMapping NAME resolved to the store id"
    );

    // audit: the MCP envelope rows, the handler's per-write domain rows and
    // the APPLY summary row — all under the MCP actor
    let rows = audit_rows(&app.store, "MCP_TOOL_CALL");
    let by = |tool: &str, outcome: &str| {
        rows.iter()
            .filter(|(_, e, d)| e.as_deref() == Some(tool) && d["outcome"] == outcome)
            .count()
    };
    assert_eq!(by("apply_dry_run", "ok"), 1, "{rows:#?}");
    assert_eq!(
        by("apply", "rejected"),
        1,
        "the confirm rejection: {rows:#?}"
    );
    assert_eq!(by("apply", "ok"), 1, "{rows:#?}");
    assert!(rows.iter().all(|(a, _, _)| a == "mcp:gitops-agent"));
    let summary = audit_rows(&app.store, "APPLY");
    assert_eq!(summary.len(), 1, "one APPLY summary row: {summary:#?}");
    assert_eq!(summary[0].0, "mcp:gitops-agent");
    assert_eq!(summary[0].2["summary"]["create"], 2);
    for (action, id) in [("TABLE_CREATE", tid), ("PIPELINE_CREATE", pid)] {
        let domain = audit_rows(&app.store, action);
        assert_eq!(domain.len(), 1, "{action}: {domain:#?}");
        assert_eq!(
            domain[0].0, "mcp:gitops-agent",
            "{action} carries the MCP actor"
        );
        assert_eq!(domain[0].1.as_deref(), Some(id.to_string().as_str()));
        assert_eq!(domain[0].2["via"], "apply");
    }

    // --- re-applying the converged file: zero changes, no domain or
    //     summary rows (the CI-loop ruling), only this call's envelope row
    let before = total_audit();
    let mut again = file.clone();
    again["confirm"] = json!(true);
    let (_, result) = tool_ok(&tool(&app, &hdr, "apply", again).await);
    assert_eq!(
        result["summary"],
        json!({"create": 0, "update": 0, "unchanged": 3, "orphan": 0})
    );
    assert_eq!(
        total_audit(),
        before + 1,
        "a no-op apply audits only the envelope"
    );

    // --- a typo'd section is the handler's own 400 (`confirm` was lifted
    //     out, so it is NOT the unknown key), surfacing as a tool error
    let (status, body) = tool_err(
        &tool(
            &app,
            &hdr,
            "apply",
            json!({"confirm": true, "pipeline": []}),
        )
        .await,
    );
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"]["message_key"], "err.apply.unknown_key");
}

// ---------------- read tools + actions (mock engine) ----------------

#[tokio::test]
async fn read_tools_and_actions_round_trip_through_the_mock_engine() {
    let app = app_with_mcp(true).await;
    let (st, port) = spawn_mock_engine().await;
    let (node_id, _k, _t) = online_node(&app, port, "edge-mcp").await;
    let (kafka, doris, _kt, _dt) = seed_datasources(&app).await;
    let pid = published_pipeline(&app, kafka, doris).await;
    let hdr = [("x-mcp-client", "reader")];

    // list_datasources: all + type filter
    let (_, body) = tool_ok(&tool(&app, &hdr, "list_datasources", json!({})).await);
    assert_eq!(body["total"], 2);
    let (_, body) = tool_ok(&tool(&app, &hdr, "list_datasources", json!({"type": "kafka"})).await);
    assert_eq!(body["total"], 1);
    assert_eq!(body["items"][0]["type"], "kafka");

    // list_pipelines with paging args
    let (_, body) = tool_ok(
        &tool(
            &app,
            &hdr,
            "list_pipelines",
            json!({"page": 1, "pageSize": 5}),
        )
        .await,
    );
    assert_eq!(body["total"], 1);
    assert_eq!(body["page_size"], 5);
    assert_eq!(body["pipelines"][0]["id"], pid);

    // deploy via MCP (write + confirm) → plan pushed, pipeline RUNNING
    let (status, body) = tool_ok(
        &tool(
            &app,
            &hdr,
            "deploy_pipeline",
            json!({"confirm": true, "id": pid, "nodeId": node_id}),
        )
        .await,
    );
    assert_eq!(status, 200);
    assert_eq!(body["ok"], true);
    assert_eq!(st.plans.lock().unwrap().len(), 1);
    let (_, body) = tool_ok(&tool(&app, &hdr, "get_pipeline", json!({"id": pid})).await);
    assert_eq!(body["status"], "RUNNING");
    assert_eq!(body["sqlContent"], PIPELINE_SQL);
    assert_eq!(body["assignedNode"]["id"], node_id);

    // stats proxy
    let (_, body) = tool_ok(&tool(&app, &hdr, "get_pipeline_stats", json!({"id": pid})).await);
    assert_eq!(body["ok"], true);
    assert_eq!(body["stats"]["totals"]["consumed"], 128000);

    // logs snapshot: one tail pull; `lines` clamped to the 50..500 band
    let (_, body) = tool_ok(
        &tool(
            &app,
            &hdr,
            "get_pipeline_logs",
            json!({"id": pid, "lines": 10}),
        )
        .await,
    );
    assert_eq!(body["entries"].as_array().unwrap().len(), 3);
    assert_eq!(body["lastSeq"], 3);
    assert_eq!(
        st.log_queries.lock().unwrap().last().map(String::as_str),
        Some("limit=50&tail=1")
    );

    // compile: ok + the 400-with-diagnostics shape as a tool error
    let (_, body) = tool_ok(
        &tool(
            &app,
            &hdr,
            "compile_sql",
            json!({"sql": PIPELINE_SQL, "dsMapping": {"shop": kafka, "ods": doris}, "pipelineId": 42}),
        )
        .await,
    );
    assert_eq!(body["ok"], true);
    assert_eq!(body["statements"][0]["group"], "tr.p42.s0");
    let (status, body) =
        tool_err(&tool(&app, &hdr, "compile_sql", json!({"sql": "SELECT 1"})).await);
    assert_eq!(status, 400);
    assert_eq!(body["ok"], false);
    assert!(!body["diagnostics"].as_array().unwrap().is_empty());

    // pause / resume share the one tool; other actions are rejected
    let (_, body) = tool_ok(
        &tool(
            &app,
            &hdr,
            "pause_resume_pipeline",
            json!({"confirm": true, "id": pid, "action": "pause"}),
        )
        .await,
    );
    assert_eq!(body["status"], "PAUSED");
    let (_, body) = tool_ok(&tool(&app, &hdr, "get_pipeline", json!({"id": pid})).await);
    assert_eq!(body["status"], "PAUSED");
    let (_, body) = tool_ok(
        &tool(
            &app,
            &hdr,
            "pause_resume_pipeline",
            json!({"confirm": true, "id": pid, "action": "resume"}),
        )
        .await,
    );
    assert_eq!(body["status"], "RUNNING");
    assert_eq!(
        *st.actions.lock().unwrap(),
        vec!["pause".to_string(), "resume".to_string()]
    );
    let b = tool(
        &app,
        &hdr,
        "pause_resume_pipeline",
        json!({"confirm": true, "id": pid, "action": "stop"}),
    )
    .await;
    assert_eq!(b["error"]["code"], -32602, "stop is not an option: {b}");
    assert_eq!(
        st.actions.lock().unwrap().len(),
        2,
        "stop never reached the engine"
    );

    // REST 404s surface as tool errors carrying the §4.0 envelope
    let (status, body) = tool_err(&tool(&app, &hdr, "get_pipeline", json!({"id": 999})).await);
    assert_eq!(status, 404);
    assert_eq!(body["error"]["code"], "NOT_FOUND");
    let (status, body) = tool_err(&tool(&app, &hdr, "get_pipeline_logs", json!({"id": 999})).await);
    assert_eq!(status, 404);
    assert_eq!(body["error"]["code"], "NOT_FOUND");

    // trigger_backfill (T-A2 wired): this pipeline's source is kafka, so the
    // bounded-replay gate answers 422 BACKFILL_SOURCE_UNSUPPORTED — the MCP
    // surface rides the REST handler's envelopes (dryRun defaults true here)
    let (status, body) = tool_err(
        &tool(
            &app,
            &hdr,
            "trigger_backfill",
            json!({
                "confirm": true, "id": pid,
                "from": {"type": "offset", "value": 0},
                "to": {"type": "offset", "value": 100}
            }),
        )
        .await,
    );
    assert_eq!(status, 422);
    assert_eq!(body["error"]["code"], "BACKFILL_SOURCE_UNSUPPORTED");
    assert_eq!(
        body["error"]["message_key"], "err.backfill.source_unsupported",
        "dictionary envelope surfaces through the tool error: {body}"
    );

    // domain audit rows carry the MCP actor (§7.5 actor=mcp:<client>)
    let deploys = audit_rows(&app.store, "PIPELINE_DEPLOY");
    assert_eq!(deploys.len(), 1);
    assert_eq!(deploys[0].0, "mcp:reader");
    let actions = audit_rows(&app.store, "PIPELINE_ACTION");
    assert_eq!(actions.len(), 2);
    assert!(actions.iter().all(|(a, _, _)| a == "mcp:reader"));
    // every MCP call has its envelope row; all three outcomes showed up
    let rows = audit_rows(&app.store, "MCP_TOOL_CALL");
    assert!(!rows.is_empty());
    assert!(rows.iter().all(|(a, _, _)| a == "mcp:reader"));
    let outcomes: Vec<&str> = rows
        .iter()
        .map(|(_, _, d)| d["outcome"].as_str().unwrap_or_default())
        .collect();
    for want in ["ok", "error", "rejected"] {
        assert!(outcomes.contains(&want), "{want} missing: {outcomes:?}");
    }
}

// ---------------- client labelling / sessions ----------------

#[tokio::test]
async fn session_id_attributes_tool_calls_to_client_info() {
    let app = app_with_mcp(false).await;
    let init = json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "protocolVersion": "2025-06-18", "capabilities": {},
                    "clientInfo": { "name": "zcode-test", "version": "9.9" } }
    });
    let (status, headers, _b) = post_mcp(&app, &[("user-agent", "node")], &init.to_string()).await;
    assert_eq!(status, StatusCode::OK);
    let sid = headers
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_string();

    // with the session header → clientInfo.name
    tool_ok(
        &tool(
            &app,
            &[("mcp-session-id", sid.as_str()), ("user-agent", "node")],
            "list_pipelines",
            json!({}),
        )
        .await,
    );
    // X-MCP-Client → User-Agent → anonymous fallback chain
    tool_ok(
        &tool(
            &app,
            &[("x-mcp-client", "gateway-label"), ("user-agent", "node")],
            "list_pipelines",
            json!({}),
        )
        .await,
    );
    tool_ok(
        &tool(
            &app,
            &[("user-agent", "ua-bot/1.0")],
            "list_pipelines",
            json!({}),
        )
        .await,
    );
    tool_ok(&tool(&app, &[], "list_pipelines", json!({})).await);
    // unknown session id → header fallback (no 404, no crash)
    tool_ok(
        &tool(
            &app,
            &[("mcp-session-id", "deadbeef"), ("user-agent", "stale")],
            "list_pipelines",
            json!({}),
        )
        .await,
    );

    let actors: Vec<String> = audit_rows(&app.store, "MCP_TOOL_CALL")
        .into_iter()
        .map(|(a, _, _)| a)
        .collect();
    assert_eq!(
        actors,
        vec![
            "mcp:zcode-test",
            "mcp:gateway-label",
            "mcp:ua-bot/1.0",
            "mcp:anonymous",
            "mcp:stale",
        ]
    );

    // initialize itself is audited with the clientInfo name + session
    let inits = audit_rows(&app.store, "MCP_INITIALIZE");
    assert_eq!(inits.len(), 1);
    assert_eq!(inits[0].0, "mcp:zcode-test");
    assert_eq!(inits[0].1.as_deref(), Some(sid.as_str()));
    assert_eq!(inits[0].2["clientInfo"]["version"], "9.9");
    assert_eq!(inits[0].2["protocolVersion"], "2025-06-18");
}

// ---------------- protocol errors ----------------

#[tokio::test]
async fn protocol_errors_follow_json_rpc() {
    let app = app_with_mcp(true).await;

    // malformed JSON → 400 + parse error
    let (status, _h, body) = post_mcp(&app, &[], "{not json").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32700);
    assert_eq!(body["id"], Value::Null);

    // batch array → invalid request (single-message endpoint)
    let (status, _h, body) = post_mcp(&app, &[], "[]").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["error"]["code"], -32600);

    // wrong jsonrpc version / missing method
    let (_, _, body) = post_mcp(
        &app,
        &[],
        &json!({"jsonrpc": "1.0", "id": 7, "method": "ping"}).to_string(),
    )
    .await;
    assert_eq!(body["error"]["code"], -32600);
    assert_eq!(body["id"], 7);
    let (_, _, body) = post_mcp(&app, &[], &json!({"jsonrpc": "2.0", "id": 8}).to_string()).await;
    assert_eq!(body["error"]["code"], -32600);

    // unknown method / unknown tool
    let b = rpc(&app, &[], "resources/list", json!({})).await;
    assert_eq!(b["error"]["code"], -32601);
    let b = tool(&app, &[], "drop_everything", json!({})).await;
    assert_eq!(b["error"]["code"], -32602);
    assert_eq!(b["error"]["data"]["code"], "UNKNOWN_TOOL");

    // missing / mistyped / string-numeric arguments
    let b = tool(&app, &[], "get_pipeline", json!({})).await;
    assert_eq!(b["error"]["code"], -32602);
    assert!(
        b["error"]["message"].as_str().unwrap().contains("\"id\""),
        "{b}"
    );
    let b = tool(&app, &[], "get_pipeline", json!({"id": "abc"})).await;
    assert_eq!(b["error"]["code"], -32602);
    let b = tool(&app, &[], "get_pipeline", json!({"id": "999"})).await;
    assert_eq!(tool_err(&b).0, 404, "numeric strings accepted for ids: {b}");
    let b = rpc(
        &app,
        &[],
        "tools/call",
        json!({"name": "list_pipelines", "arguments": [1, 2]}),
    )
    .await;
    assert_eq!(b["error"]["code"], -32602);
    let b = rpc(&app, &[], "tools/call", json!({})).await;
    assert_eq!(b["error"]["code"], -32602);

    // GET /mcp (no SSE stream here) → 405
    let resp = app
        .router
        .clone()
        .oneshot(Request::get("/mcp").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

    // invalid-argument attempts are audited as rejected, too — and so are
    // the attempts that never reached a tool (unknown tool / missing name /
    // non-object arguments): §7.5's "every tool call — ok, failed or
    // rejected — lands in audit_log" invariant
    let rows = audit_rows(&app.store, "MCP_TOOL_CALL");
    let by_code = |code: &str| rows.iter().filter(|(_, _, d)| d["code"] == code).count();
    assert_eq!(by_code("INVALID_ARGUMENTS"), 2, "{rows:#?}");
    assert_eq!(
        by_code("UNKNOWN_TOOL"),
        1,
        "the drop_everything attempt is audit evidence: {rows:#?}"
    );
    assert_eq!(
        by_code("INVALID_PARAMS"),
        2,
        "missing tool name + non-object arguments audited: {rows:#?}"
    );
    // every rejected row carries outcome=rejected
    for (_, _, d) in rows.iter() {
        if d["code"].is_string() {
            assert_eq!(d["outcome"], "rejected", "{d}");
        }
    }
}

// ---------------- rate limit ----------------

#[tokio::test]
async fn rate_limit_rejects_with_429_past_the_budget() {
    let app = app_with_mcp_state(McpState::with_rate_limit(false, 2)).await;
    for i in 0..2 {
        let (status, _h, body) = post_mcp(
            &app,
            &[],
            &json!({"jsonrpc": "2.0", "id": i, "method": "ping"}).to_string(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let (status, _h, body) = post_mcp(
        &app,
        &[],
        &json!({"jsonrpc": "2.0", "id": 9, "method": "ping"}).to_string(),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_eq!(body["error"]["code"], -32002);
    assert_eq!(body["id"], Value::Null);
}
