//! T-G6 pipeline-as-code contract tests: the dry-run diff's three states
//! (create / update-per-field / orphan) and its read-only ruling, apply
//! idempotence (a second apply of the same file is a zero-change no-op
//! that leaves status/generation/audit byte-untouched), whole-file
//! validation refusals, credential handling (never echoed, still
//! idempotent), and the CLI thin shell agreeing with the API on the same
//! file against a served router.

mod common;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use common::*;
use serde_json::{json, Value};
use tailrace_console::apply_cli::{self, ApplyOpts};
use tower::ServiceExt;

fn audit_count(app: &TestApp) -> i64 {
    app.store
        .with_conn(|c| Ok(c.query_row("SELECT COUNT(*) FROM audit_log", [], |r| r.get(0))?))
        .unwrap()
}

fn audit_rows(app: &TestApp, action: &str) -> Vec<String> {
    app.store
        .with_conn(|c| {
            let mut stmt =
                c.prepare("SELECT detail_json FROM audit_log WHERE action = ?1 ORDER BY id")?;
            let rows = stmt.query_map([action], |r| r.get::<_, String>(0))?;
            Ok(rows.collect::<Result<Vec<_>, _>>()?)
        })
        .unwrap()
}

/// Audit rows whose detail carries `"via":"apply"` — the REST seeding in
/// these tests writes the same action names, so apply-attributed rows are
/// told apart by the marker apply adds.
fn apply_audit_rows(app: &TestApp, action: &str) -> Vec<String> {
    audit_rows(app, action)
        .into_iter()
        .filter(|d| d.contains(r#""via":"apply""#))
        .collect()
}

/// Seed via the REST surface (the world apply diffs against): kafka ds +
/// table (registered with columns), doris ds, one legacy pipeline that the
/// declaration file will not mention.
async fn seed_legacy(app: &TestApp) -> (i64, i64) {
    let (s, b) = req(
        app,
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
    let (s, b) = req(
        app,
        "POST",
        "/api/datasources",
        Some(json!({
            "name": "shop-doris", "type": "doris",
            "connJson": {
                "jdbcUrl": "jdbc:mysql://127.0.0.1:19030/shop",
                "streamLoadUrl": "http://127.0.0.1:18040"
            }
        })),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    let doris = b["id"].as_i64().unwrap();
    let (s, b) = req(
        app,
        "POST",
        &format!("/api/datasources/{kafka}/tables"),
        Some(json!({
            "dbName": "shop", "tableName": "orders", "format": "ogg",
            "columnsJson": [["id", "int64"], ["payload", "utf8"]]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let (s, b) = req(
        app,
        "POST",
        "/api/pipelines",
        Some(json!({"name": "legacy", "sqlContent": "SELECT 1"})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{b}");
    (kafka, doris)
}

/// The declaration file used across the diff tests: two datasources (one
/// drifted), two tables (one drifted by columns, one new), an empty
/// pipelines section (the seeded legacy pipeline becomes an orphan).
fn drifted_file() -> Value {
    json!({
        "datasources": [
            {"name": "edge-kafka", "type": "kafka", "connJson": {"bootstrap": "127.0.0.1:19092"}},
            {"name": "shop-doris", "type": "doris", "connJson": {
                "jdbcUrl": "jdbc:mysql://127.0.0.1:19030/shop",
                "streamLoadUrl": "http://127.0.0.1:18041"
            }}
        ],
        "tables": [
            {"datasource": "edge-kafka", "dbName": "shop", "tableName": "orders", "format": "ogg",
             "columnsJson": [["id", "int64"], ["payload", "utf8"], ["op_type", "utf8"]]},
            {"datasource": "edge-kafka", "dbName": "shop", "tableName": "events", "format": "json",
             "columnsJson": [["id", "int64"]]}
        ],
        "pipelines": []
    })
}

/// The dry-run diff is real: new objects read `create`, drifted objects
/// read `update` with the exact drifted FIELDS named (per-field, per
/// connJson sub-key), matching objects read `unchanged`, and console
/// objects a present-but-unnamed section drops are reported as orphans.
/// And it wrote nothing: store and audit are byte-identical after.
#[tokio::test]
async fn dry_run_diff_three_states_and_read_only() {
    let app = app().await;
    let (kafka, doris) = seed_legacy(&app).await;
    let before_audit = audit_count(&app);

    let (s, b) = req(&app, "POST", "/api/apply/dry-run", Some(drifted_file())).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["dryRun"], true);

    // --- datasources: unchanged + update (one drifted sub-key)
    let ds = &b["datasources"];
    assert_eq!(ds[0]["key"], "edge-kafka");
    assert_eq!(ds[0]["action"], "unchanged");
    assert_eq!(ds[0]["id"], kafka);
    assert_eq!(ds[1]["key"], "shop-doris");
    assert_eq!(ds[1]["action"], "update");
    assert_eq!(
        ds[1]["changes"],
        json!([{"field": "connJson.streamLoadUrl",
                "from": "http://127.0.0.1:18040", "to": "http://127.0.0.1:18041"}])
    );

    // --- tables: update (columnsJson) + create
    let tables = &b["tables"];
    assert_eq!(tables[0]["key"], "edge-kafka/shop.orders");
    assert_eq!(tables[0]["action"], "update");
    let fields: Vec<&str> = tables[0]["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["field"].as_str().unwrap())
        .collect();
    assert_eq!(
        fields,
        vec!["columnsJson"],
        "only the drifted field is named"
    );
    assert_eq!(
        tables[0]["changes"][0]["to"],
        json!([["id", "int64"], ["payload", "utf8"], ["op_type", "utf8"]])
    );
    assert_eq!(tables[1]["key"], "edge-kafka/shop.events");
    assert_eq!(tables[1]["action"], "create");
    assert_eq!(tables[1]["id"], Value::Null, "dry-run create has no id yet");

    // --- the empty pipelines section declares the full set → legacy orphan
    assert_eq!(b["pipelines"].as_array().unwrap().len(), 0);
    assert_eq!(
        b["orphans"],
        json!([{"kind": "pipeline", "key": "legacy", "id": 1}]),
        "present-but-empty section reports the un-managed pipeline"
    );

    // --- summary: 1 create, 2 update, 1 unchanged, 1 orphan
    assert_eq!(
        b["summary"],
        json!({"create": 1, "update": 2, "unchanged": 1, "orphan": 1})
    );

    // --- read-only ruling: nothing written, nothing audited
    assert_eq!(audit_count(&app), before_audit);
    let (s, b) = req(&app, "GET", &format!("/api/datasources/{doris}"), None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        b["connJson"]["streamLoadUrl"], "http://127.0.0.1:18040",
        "dry-run did not write the drifted value"
    );
    let (s, b) = req(&app, "GET", "/api/datasources/tables?datasourceId=0", None).await;
    assert_eq!(s, StatusCode::OK);
    let _ = b; // count check via the list below is enough
    let (s, b) = req(&app, "GET", "/api/datasources/tables", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["total"], 1, "the new table was not created");
}

/// Apply is an idempotent upsert: first run creates the declared
/// topology; the second run of the SAME file is a zero-change no-op
/// (no generation bump, no PENDING_CHANGES flip, no audit row); a third
/// run with drifted fields updates exactly those fields.
#[tokio::test]
async fn apply_idempotent_upsert() {
    let app = app().await;
    let (kafka, doris) = seed_legacy(&app).await;

    let mut file = json!({
        "datasources": [
            {"name": "edge-kafka", "type": "kafka", "connJson": {"bootstrap": "127.0.0.1:19092"}},
            {"name": "mysql-shop", "type": "mysql",
             "connJson": {"jdbcUrl": "jdbc:mysql://127.0.0.1:13306/shop"},
             "username": "loader", "password": "pw-1"}
        ],
        "tables": [
            {"datasource": "edge-kafka", "dbName": "shop", "tableName": "orders", "format": "ogg",
             "columnsJson": [["id", "int64"], ["payload", "utf8"]]},
            {"datasource": "mysql-shop", "dbName": "shop", "tableName": "dim_user",
             "kind": "dim", "sequenceColumn": "id",
             "columnsJson": [["id", "int64"], ["name", "utf8"]]},
            {"datasource": "shop-doris", "dbName": "ods", "tableName": "orders",
             "keyModel": "unique", "columnsJson": [["id", "int64"], ["payload", "utf8"]]}
        ],
        "pipelines": [
            {"name": "orders→ods",
             "sqlContent": "INSERT INTO ods.orders SELECT id, payload FROM shop.orders",
             "dsMapping": {"shop": "edge-kafka", "ods": "shop-doris"},
             "checkpointMs": 5000,
             "config": {"reliability": null, "recovery": null, "tuning": null,
                        "groupPrefix": null, "startOffset": "earliest"}}
        ]
    });

    // --- first apply: creates mysql ds + 2 tables + pipeline; kafka ds and
    //     its table match already. Orphans: the legacy pipeline AND the
    //     seeded shop-doris datasource (a present datasources section is the
    //     declared full set, so anything unnamed of that kind is reported).
    let (s, b) = req(&app, "POST", "/api/apply", Some(file.clone())).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["dryRun"], false);
    assert_eq!(
        b["summary"],
        json!({"create": 4, "update": 0, "unchanged": 2, "orphan": 2})
    );
    assert_eq!(
        b["orphans"],
        json!([
            {"kind": "datasource", "key": "shop-doris", "id": doris},
            {"kind": "pipeline", "key": "legacy", "id": 1},
        ]),
        "orphans are reported, never deleted"
    );
    let pipe_id = b["pipelines"][0]["id"].as_i64().unwrap();

    // dsMapping names resolved to the store's numeric ids
    let (s, b) = req(&app, "GET", &format!("/api/pipelines/{pipe_id}"), None).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(
        b["dsMapping"],
        json!({"shop": kafka, "ods": doris}),
        "file names landed as numeric ids, same as the REST create path"
    );
    assert_eq!(b["status"], "DRAFT");
    assert_eq!(b["checkpointMs"], 5000);

    // audit: 4 creates + 1 APPLY summary row
    let rows = audit_rows(&app, "APPLY");
    assert_eq!(rows.len(), 1, "one APPLY summary row per apply");
    assert_eq!(
        serde_json::from_str::<Value>(&rows[0]).unwrap()["summary"]["create"],
        json!(4)
    );
    assert_eq!(apply_audit_rows(&app, "DATASOURCE_CREATE").len(), 1);
    assert_eq!(apply_audit_rows(&app, "TABLE_CREATE").len(), 2);
    assert_eq!(apply_audit_rows(&app, "PIPELINE_CREATE").len(), 1);

    // --- second apply of the SAME file: zero changes, byte-untouched
    let before_audit = audit_count(&app);
    let (s, b) = req(&app, "POST", "/api/apply", Some(file.clone())).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(
        b["summary"],
        json!({"create": 0, "update": 0, "unchanged": 6, "orphan": 2}),
        "second apply is a no-op"
    );
    assert_eq!(
        audit_count(&app),
        before_audit,
        "a no-op apply writes no audit rows"
    );

    // generation / status stayed put (no blind re-writes)
    let (s, b) = req(&app, "GET", &format!("/api/datasources/{doris}"), None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["generation"], 1);
    let (s, b) = req(&app, "GET", &format!("/api/pipelines/{pipe_id}"), None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        b["status"], "DRAFT",
        "unchanged pipeline never flips to PENDING_CHANGES"
    );

    // credentials ride the same idempotence: the sealed row never changed,
    // so a re-declared identical credential stays "unchanged" (covered by
    // the zero-change summary above) and the plaintext is not at rest
    let sealed = app
        .store
        .with_conn(|c| {
            Ok(c.query_row(
                "SELECT nonce || ':' || ciphertext FROM datasource_credential dc \
                 JOIN datasource d ON d.id = dc.datasource_id WHERE d.name = 'mysql-shop'",
                [],
                |r| r.get::<_, String>(0),
            )?)
        })
        .unwrap();
    assert!(!sealed.contains("pw-1"), "credential sealed, not plaintext");
    assert!(!sealed.contains("loader"));

    // --- third apply with drift: exactly the drifted fields update
    file["datasources"][1]["connJson"]["jdbcUrl"] = json!("jdbc:mysql://127.0.0.2:13306/shop");
    file["tables"][0]["columnsJson"] = json!([
        ["id", "int64"],
        ["payload", "utf8"],
        ["ts", "timestamp(us)"]
    ]);
    file["pipelines"][0]["sqlContent"] =
        json!("INSERT INTO ods.orders SELECT id, payload, ts FROM shop.orders");
    let (s, b) = req(&app, "POST", "/api/apply", Some(file)).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(
        b["summary"],
        json!({"create": 0, "update": 3, "unchanged": 3, "orphan": 2})
    );
    assert_eq!(
        b["datasources"][1]["changes"][0]["field"],
        "connJson.jdbcUrl"
    );
    assert_eq!(b["tables"][0]["key"], "edge-kafka/shop.orders");
    assert_eq!(b["tables"][0]["changes"][0]["field"], "columnsJson");
    assert_eq!(b["pipelines"][0]["changes"][0]["field"], "sqlContent");
    // TABLE_UPDATE is a new action (the registration PUT only edits keyModel)
    let updates = apply_audit_rows(&app, "TABLE_UPDATE");
    assert_eq!(updates.len(), 1);
}

/// A declared credential that actually changes reads as an update whose
/// from/to are redacted on both sides; the stored row is re-sealed and the
/// plaintext never appears anywhere.
#[tokio::test]
async fn credential_diff_redacted_and_never_echoed() {
    let app = app().await;
    let file = json!({
        "datasources": [
            {"name": "mysql-shop", "type": "mysql",
             "connJson": {"jdbcUrl": "jdbc:mysql://127.0.0.1:13306/shop"},
             "username": "loader", "password": "pw-1"}
        ]
    });
    let (s, b) = req(&app, "POST", "/api/apply", Some(file.clone())).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["summary"]["create"], 1);

    // same credential again → unchanged (sealed row opened + compared)
    let (s, b) = req(&app, "POST", "/api/apply/dry-run", Some(file)).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(
        b["summary"],
        json!({"create": 0, "update": 0, "unchanged": 1, "orphan": 0})
    );

    // rotated password → update, redacted both sides
    let rotated = json!({
        "datasources": [
            {"name": "mysql-shop", "type": "mysql",
             "connJson": {"jdbcUrl": "jdbc:mysql://127.0.0.1:13306/shop"},
             "username": "loader", "password": "pw-2"}
        ]
    });
    let (s, b) = req(&app, "POST", "/api/apply/dry-run", Some(rotated)).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["summary"]["update"], 1);
    assert_eq!(
        b["datasources"][0]["changes"],
        json!([{"field": "credential", "from": "<redacted>", "to": "<redacted>"}])
    );
    let raw = serde_json::to_string(&b).unwrap();
    assert!(!raw.contains("pw-1") && !raw.contains("pw-2") && !raw.contains("loader"));
}

/// Validation refuses the WHOLE file before any write: unknown top-level
/// key, unknown datasource references (table + dsMapping), duplicate keys,
/// non-object connJson, and the unauthenticated path.
#[tokio::test]
async fn validation_refuses_whole_file() {
    let app = app().await;
    let (kafka, _) = seed_legacy(&app).await;
    let before_audit = audit_count(&app);

    // no session → 401
    let resp = app
        .router
        .clone()
        .oneshot(
            Request::post("/api/apply")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let cases: Vec<(Value, &str)> = vec![
        (json!({"pipeline": []}), "err.apply.unknown_key"),
        (
            json!({"tables": [
                {"datasource": "no-such", "dbName": "shop", "tableName": "orders"}
            ]}),
            "err.apply.table_datasource_unknown",
        ),
        (
            json!({"datasources": [
                {"name": "a", "type": "kafka", "connJson": {"bootstrap": "h:9092"}},
                {"name": "a", "type": "kafka", "connJson": {"bootstrap": "h:9093"}}
            ]}),
            "err.apply.duplicate",
        ),
        (
            json!({"datasources": [
                {"name": "a", "type": "kafka", "connJson": "h:9092"}
            ]}),
            "err.apply.conn_json_not_object",
        ),
        (
            json!({"datasources": [
                {"name": "a", "type": "oracle", "connJson": {}}
            ]}),
            "err.apply.datasource_type_unknown",
        ),
        (
            json!({"datasources": [
                {"name": "a", "type": "kafka", "connJson": {}}
            ]}),
            "err.apply.field_missing",
        ),
        (
            json!({"datasources": [{"name": "", "type": "kafka", "connJson": {"bootstrap": "h"}}]}),
            "err.apply.field_missing",
        ),
        (
            json!({"pipelines": [
                {"name": "p", "sqlContent": "SELECT 1", "dsMapping": {"shop": "no-such"}}
            ]}),
            "err.apply.mapping_datasource_unknown",
        ),
        (
            json!({"tables": [
                {"datasource": "edge-kafka", "dbName": "shop", "tableName": "t",
                 "kind": "dim", "sequenceColumn": "id"}
            ]}),
            "err.apply.table_kind_invalid",
        ),
        (
            json!({"tables": [
                {"datasource": "edge-kafka", "dbName": "shop", "tableName": "t",
                 "keyModel": "unique"}
            ]}),
            "err.apply.table_key_model_invalid",
        ),
    ];
    for (body, key) in cases {
        let (s, b) = req(&app, "POST", "/api/apply", Some(body)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{key}: {b}");
        assert_eq!(b["error"]["message_key"], key, "{b}");
    }
    // malformed JSON → the shared bad_body envelope
    let resp = app
        .router
        .clone()
        .oneshot(
            Request::post("/api/apply")
                .header("content-type", "application/json")
                .header("cookie", &app.cookie)
                .body(Body::from("{not json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let b: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(b["error"]["message_key"], "err.bad_body");

    // nothing was written by any refused file
    assert_eq!(audit_count(&app), before_audit);
    let (s, b) = req(&app, "GET", "/api/datasources", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["total"], 2);
    let _ = kafka;
}

/// Absent vs present sections: a file that only declares `pipelines` makes
/// no statement about datasources/tables — those keys are absent from the
/// response wire, their objects are never orphan-reported, and an apply of
/// such a file touches nothing but the declared kind.
#[tokio::test]
async fn absent_sections_are_not_managed() {
    let app = app().await;
    seed_legacy(&app).await;
    let before_audit = audit_count(&app);

    let (s, b) = req(
        &app,
        "POST",
        "/api/apply/dry-run",
        Some(json!({"pipelines": []})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert!(
        b.get("datasources").is_none(),
        "absent section absent from the wire: {b}"
    );
    assert!(b.get("tables").is_none());
    assert_eq!(
        b["pipelines"],
        json!([]),
        "declared section present even when empty"
    );
    // only the pipeline section is managed → only the legacy pipeline is an
    // orphan; the seeded datasource/table are not the file's business
    assert_eq!(
        b["orphans"],
        json!([{"kind": "pipeline", "key": "legacy", "id": 1}])
    );
    assert_eq!(
        b["summary"],
        json!({"create": 0, "update": 0, "unchanged": 0, "orphan": 1})
    );

    // apply of the same minimal file writes nothing (empty declared set,
    // nothing to create) and audits nothing
    let (s, b) = req(&app, "POST", "/api/apply", Some(json!({"pipelines": []}))).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(audit_count(&app), before_audit);
    let (s, b) = req(&app, "GET", "/api/datasources", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(b["total"], 2, "datasources untouched");
}

/// Cross-path reuse: the same declaration file through the CLI's real
/// HTTP client against a served router produces the same diff the API
/// returns, and a CLI apply converges the store exactly like the API
/// would (proven by a follow-up API dry-run reporting zero drift).
#[tokio::test]
async fn api_and_cli_agree_on_the_same_file() {
    let app = app().await;
    let (_, _) = seed_legacy(&app).await;

    // serve the real router on an ephemeral port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let router = app.router.clone();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    // the session token (login response body → Bearer, the CLI's channel)
    let token = app.cookie.trim_start_matches("tr_session=").to_string();

    let file = json!({
        "datasources": [
            {"name": "edge-kafka", "type": "kafka",
             "connJson": {"bootstrap": "127.0.0.1:19092"}}
        ],
        "tables": [
            {"datasource": "edge-kafka", "dbName": "shop", "tableName": "orders",
             "format": "ogg", "columnsJson": [["id", "int64"], ["payload", "utf8"]]},
            {"datasource": "edge-kafka", "dbName": "shop", "tableName": "events", "format": "json"}
        ],
        "pipelines": [
            {"name": "orders→ods",
             "sqlContent": "INSERT INTO ods.orders SELECT id FROM shop.orders",
             "dsMapping": {"shop": "edge-kafka"}}
        ]
    });

    // 1) API dry-run (oneshot)
    let (s, api_diff) = req(&app, "POST", "/api/apply/dry-run", Some(file.clone())).await;
    assert_eq!(s, StatusCode::OK, "{api_diff}");
    assert_eq!(
        api_diff["summary"],
        json!({"create": 2, "update": 0, "unchanged": 2, "orphan": 2})
    );

    // 2) CLI dry-run over real HTTP — same file, same diff (ids for dry-run
    //    creates are null on both paths, so the bodies compare equal)
    let path = std::env::temp_dir().join(format!(
        "tailrace-apply-cli-{}-{}/pipelines.json",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    ));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, serde_json::to_vec(&file).unwrap()).unwrap();
    let opts = ApplyOpts {
        file: path.clone(),
        dry_run: true,
        server: format!("http://127.0.0.1:{port}"),
        token: Some(token.clone()),
        user: None,
        password: None,
    };
    let (status, cli_diff) = apply_cli::execute(&opts).await.expect("cli dry-run");
    assert_eq!(status, 200, "{cli_diff}");
    assert_eq!(cli_diff, api_diff, "CLI and API agree on the same file");
    // the human report renders the diff
    let report = apply_cli::render(&cli_diff, &opts);
    assert!(report.contains("+ edge-kafka/shop.events"));
    assert!(report.contains("summary: create 2, update 0, unchanged 2, orphan 2"));
    assert!(report.contains("(dry-run: nothing was written)"));

    // 3) CLI apply (real write over HTTP), then the API confirms zero drift
    let mut apply_opts = opts.clone();
    apply_opts.dry_run = false;
    let (status, result) = apply_cli::execute(&apply_opts).await.expect("cli apply");
    assert_eq!(status, 200, "{result}");
    assert_eq!(result["summary"]["create"], 2);

    let (s, b) = req(&app, "POST", "/api/apply/dry-run", Some(file)).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(
        b["summary"],
        json!({"create": 0, "update": 0, "unchanged": 4, "orphan": 2}),
        "after the CLI apply the API sees zero drift — one convergence path"
    );
    let _ = std::fs::remove_file(&path);
}
