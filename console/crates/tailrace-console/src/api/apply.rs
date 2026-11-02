//! Pipeline-as-code (T-G6, R10 design ruling): one declarative JSON file
//! describing datasources / table objects / pipelines flows through ONE
//! server-side pair — `POST /api/apply/dry-run` computes a structured diff
//! against the store (read-only, unaudited), `POST /api/apply` performs the
//! idempotent upsert (create missing, update drifted fields, touch nothing
//! that already matches, DELETE NOTHING — v1 never prunes; console objects
//! the file does not name are reported as orphans instead). GitOps, CI and
//! the CLI thin shell (`tailrace-console apply -f`, see apply_cli.rs) all
//! hit this same entry, so the diff a human reviews is the diff the machine
//! applies.
//!
//! Identity is the business key the REST surface already makes UNIQUE —
//! datasource `name`, table `(datasource name, dbName, tableName)`, pipeline
//! `name` — never a numeric id: a file written against one console applies
//! unchanged to another. `table.datasource` and `dsMapping` values therefore
//! take datasource NAMES (numeric ids are still accepted and resolved).
//!
//! Per-object bodies ARE the REST create/update DTOs (`DatasourceUpsertBody`
//! / `TableRegisterBody` + `datasource` / `PipelineUpsertBody`), so the file
//! schema cannot drift from the endpoints — serde is the single source of
//! truth. JSON only in v1 (serde_yaml is unmaintained; YAML users convert
//! with `yq` — docs/ops/pipeline-file.md). The `generation` CAS fields of
//! the embedded bodies are ignored: apply resolves the current generation
//! server-side and refuses on a mid-apply race (409), never blind-writes.

use std::collections::{BTreeMap, HashMap, HashSet};

use axum::body::Bytes;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use axum::Json;
use console_auth::{ApiError, AuthContext};
use console_store::DatasourceUpdate;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use utoipa::ToSchema;

use crate::api::datasources::{
    credential_payload, parse_key_model, registry_type, required_conn_keys, table_kind_for,
    DatasourceUpsertBody, TableRegisterBody,
};
use crate::api::messages;
use crate::api::pipelines::{PipelineConfig, PipelineUpsertBody};
use crate::api::{audit, operator_of};
use crate::state::AppState;

// ---------------- DTOs ----------------

/// The declaration file (`POST /api/apply[/dry-run]` body). Every section is
/// optional, and ABSENT vs PRESENT is meaningful: an absent key means "this
/// file makes no statement about that kind" (nothing of the kind is touched,
/// diffed or orphan-reported); a present key — even an empty array — is the
/// declared full set of that kind, so every console object of that kind the
/// file does not name is reported as an orphan. Orphans are reported only:
/// v1 never deletes.
#[derive(Debug, Default, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApplyFile {
    pub datasources: Option<Vec<DatasourceUpsertBody>>,
    pub tables: Option<Vec<ApplyTable>>,
    pub pipelines: Option<Vec<PipelineUpsertBody>>,
}

/// One table-object declaration: the REST registration body plus the owning
/// datasource by NAME (the REST endpoint carries the id in its path).
#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApplyTable {
    pub datasource: String,
    #[serde(flatten)]
    pub body: TableRegisterBody,
}

/// One drifted field of an updated object: `field` (dotted for connJson
/// sub-keys, `connJson.jdbcUrl`), `from`/`to` as JSON values. Credential
/// values are never echoed — presence only (`<redacted>`).
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApplyChange {
    pub field: String,
    pub from: Value,
    pub to: Value,
}

/// One declared object and its verdict: `create` / `update` / `unchanged`.
/// `id` is the existing row for update/unchanged, the id the apply created,
/// or null for a dry-run create (nothing is written, no id exists yet).
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApplyItem {
    pub key: String,
    pub action: String,
    pub id: Option<i64>,
    pub changes: Vec<ApplyChange>,
}

/// A console object of a kind the file declares but does not name (present
/// section = full set): apply leaves it untouched and says so.
#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApplyOrphan {
    pub kind: String,
    pub key: String,
    pub id: i64,
}

#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct ApplySummary {
    pub create: u32,
    pub update: u32,
    pub unchanged: u32,
    pub orphan: u32,
}

/// The dry-run report and the post-apply result (same shape; `dryRun` tells
/// them apart). Each section is `None` (absent from the wire) when the file
/// did not declare that kind — the response then makes no statement about
/// it either, matching the file's absent-vs-present semantics; a declared
/// section is always present, even as an empty array (declared full set).
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApplyResponse {
    pub dry_run: bool,
    pub summary: ApplySummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datasources: Option<Vec<ApplyItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<ApplyItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipelines: Option<Vec<ApplyItem>>,
    pub orphans: Vec<ApplyOrphan>,
}

// ---------------- handlers ----------------

/// `POST /api/apply/dry-run` — structured diff of the declaration file
/// against the store. Zero side effects: nothing written, nothing audited
/// (same ruling as the backfill dryRun and the DDL draft).
#[utoipa::path(
    post,
    path = "/api/apply/dry-run",
    request_body = ApplyFile,
    responses(
        (status = 200, body = ApplyResponse, description = "diff report; nothing was written"),
        (status = 400, body = crate::api::ErrorResponse, description = "malformed file / unknown reference"),
    )
)]
pub async fn dry_run(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    // Session-scoped like every /api write: the dry run reads the same
    // store the apply writes, so it needs the same authority. The operator
    // is resolved only for symmetry — a dry run never audits.
    let _ = operator_of(&ctx, &headers);
    let file = match parse_file(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    match plan(&state, &file) {
        Ok(plan) => Json(plan.to_response(true)).into_response(),
        Err(r) => r,
    }
}

/// `POST /api/apply` — idempotent upsert of the declaration file. Objects
/// that already match are left byte-untouched (no status flip, no
/// generation bump, no audit row); only real drift writes. Never deletes:
/// removals from the file become orphan entries in the response. Audited
/// per actual write plus one APPLY summary row.
#[utoipa::path(
    post,
    path = "/api/apply",
    request_body = ApplyFile,
    responses(
        (status = 200, body = ApplyResponse, description = "what was created/updated; unchanged objects untouched"),
        (status = 400, body = crate::api::ErrorResponse, description = "malformed file / unknown reference — nothing written"),
        (status = 409, body = crate::api::ErrorResponse, description = "concurrent edit raced the apply (datasource generation / name)"),
    )
)]
pub async fn apply(
    State(state): State<AppState>,
    axum::Extension(ctx): axum::Extension<AuthContext>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let operator = operator_of(&ctx, &headers);
    let file = match parse_file(&body) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let mut plan = match plan(&state, &file) {
        Ok(v) => v,
        Err(r) => return r,
    };
    execute(&state, &mut plan, &operator).unwrap_or_else(|r| r)
}

// ---------------- file parsing ----------------

/// Parse the declaration file: top-level keys are checked by hand so a typo
/// (`"pipeline"`) is a precise 400 instead of serde's silently-ignored
/// unknown field — with the absent-vs-present semantics of the three
/// sections, silently ignoring a typo'd section would hide half the diff.
fn parse_file(body: &Bytes) -> Result<ApplyFile, Response> {
    let raw: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return Err(messages::bad_request("err.bad_body", &[&e.to_string()]).into_response())
        }
    };
    let Some(map) = raw.as_object() else {
        return Err(messages::bad_request(
            "err.bad_body",
            &["declaration file must be a JSON object"],
        )
        .into_response());
    };
    for key in map.keys() {
        if !matches!(key.as_str(), "datasources" | "tables" | "pipelines") {
            return Err(messages::bad_request("err.apply.unknown_key", &[key]).into_response());
        }
    }
    serde_json::from_value(raw)
        .map_err(|e| messages::bad_request("err.bad_body", &[&e.to_string()]).into_response())
}

// ---------------- planning (the shared diff) ----------------

/// Everything the plan needs to know about the console's current state.
struct Current {
    datasources: Vec<console_store::Datasource>,
    tables: Vec<console_store::TableObject>,
    pipelines: Vec<console_store::PipelineFull>,
}

impl Current {
    fn load(state: &AppState) -> Result<Self, Response> {
        let store = state.store();
        let datasources = store
            .list_datasources(None)
            .map_err(|e| ApiError::internal(&e).into_response())?;
        let tables = store
            .list_table_objects(None, None)
            .map_err(|e| ApiError::internal(&e).into_response())?;
        let mut pipelines = Vec::new();
        for p in store
            .list_pipelines()
            .map_err(|e| ApiError::internal(&e).into_response())?
        {
            // pipeline_full carries config_json (the detail DTO reads the
            // same accessor); a row vanishing mid-plan (concurrent delete)
            // is skipped, not fatal — the plan just treats the name as free.
            if let Some(full) = store.pipeline_full(p.id).ok().flatten() {
                pipelines.push(full);
            }
        }
        Ok(Self {
            datasources,
            tables,
            pipelines,
        })
    }

    fn ds_by_name(&self, name: &str) -> Option<&console_store::Datasource> {
        self.datasources.iter().find(|d| d.name == name)
    }

    fn ds_name_by_id(&self, id: i64) -> Option<&str> {
        self.datasources
            .iter()
            .find(|d| d.id == id)
            .map(|d| d.name.as_str())
    }
}

/// The computed plan — what dry-run reports and what apply executes. Built
/// against one `Current` snapshot; dry-run stops here, apply executes.
struct Plan {
    datasources: Vec<DsPlan>,
    tables: Vec<TablePlan>,
    pipelines: Vec<PipePlan>,
    orphans: Vec<ApplyOrphan>,
    summary: ApplySummary,
    /// Which sections the file declared (absent vs present decides both the
    /// orphan report and which sections the response carries at all).
    managed: (bool, bool, bool),
}

struct DsPlan {
    item: ApplyItem,
    name: String,
    ty: String,
    conn_json: String,
    /// Sealed credential halves when the file declares a credential that
    /// differs from the stored one; None = keep what is stored (§4.2 blank
    /// password semantics).
    credential: Option<(String, String)>,
    /// (id, generation) when the datasource already exists.
    existing: Option<(i64, i64)>,
}

struct TablePlan {
    item: ApplyItem,
    ds_name: String,
    db_name: String,
    table_name: String,
    kind: String,
    format: Option<String>,
    columns_json: Option<String>,
    sequence_column: Option<String>,
    key_model: Option<String>,
    existing_id: Option<i64>,
}

struct PipePlan {
    item: ApplyItem,
    name: String,
    sql_content: String,
    /// alias → datasource NAME (the file's portable form; ids are resolved
    /// at execute time, after the datasource section has been applied).
    mapping_names: Option<BTreeMap<String, String>>,
    checkpoint_ms: i64,
    preferred_node: Option<i64>,
    config_json: Option<String>,
    existing_id: Option<i64>,
}

/// `err.apply.field_missing` with the section, the entry (index or name)
/// and the missing field — the one generic shape every entry-level check
/// funnels into.
fn missing(section: &str, entry: &str, field: &str) -> Response {
    messages::bad_request("err.apply.field_missing", &[section, entry, field]).into_response()
}

fn duplicate(section: &str, key: &str) -> Response {
    messages::bad_request("err.apply.duplicate", &[section, key]).into_response()
}

/// Compute the full plan: validate every declared object (against file ∪
/// store), diff each against the store, and collect orphans per declared
/// section. All validation happens before any write — an unknown reference
/// anywhere refuses the whole file (both dry-run and apply), so apply never
/// lands half a broken topology.
fn plan(state: &AppState, file: &ApplyFile) -> Result<Plan, Response> {
    let current = Current::load(state)?;
    let store = state.store();
    let cipher = state.cipher();

    // File-declared datasource types: a table under a datasource that does
    // not exist yet still derives/validates its kind against what apply
    // would make that datasource, not against nothing.
    let mut file_ds_types: HashMap<String, String> = HashMap::new();

    // ---------------- datasources ----------------
    let mut datasources = Vec::new();
    let mut seen_ds: HashSet<String> = HashSet::new();
    for (i, body) in file
        .datasources
        .as_deref()
        .unwrap_or(&[])
        .iter()
        .enumerate()
    {
        let idx = i.to_string();
        let name = body.name.trim().to_string();
        if name.is_empty() {
            return Err(missing("datasources", &idx, "name"));
        }
        if !seen_ds.insert(name.clone()) {
            return Err(duplicate("datasources", &name));
        }
        let ty = registry_type(body.ty.trim())
            .ok_or_else(|| {
                messages::bad_request(
                    "err.apply.datasource_type_unknown",
                    &[&name, body.ty.trim()],
                )
                .into_response()
            })?
            .to_string();
        let conn = body.conn_json.clone().unwrap_or(Value::Null);
        if !conn.is_object() {
            return Err(
                messages::bad_request("err.apply.conn_json_not_object", &[&name]).into_response(),
            );
        }
        let missing_keys: Vec<&str> = required_conn_keys(&ty)
            .iter()
            .filter(|k| {
                conn.get(**k)
                    .and_then(Value::as_str)
                    .map(|v| v.trim())
                    .map(str::is_empty)
                    .unwrap_or(true)
            })
            .copied()
            .collect();
        if !missing_keys.is_empty() {
            let want: Vec<String> = missing_keys
                .iter()
                .map(|k| format!("connJson.{k}"))
                .collect();
            return Err(missing("datasources", &name, &want.join(", ")));
        }
        let conn_json = serde_json::to_string(&conn)
            .map_err(|e| ApiError::internal(&e.into()).into_response())?;
        file_ds_types.insert(name.clone(), ty.clone());

        let mut item = ApplyItem {
            key: name.clone(),
            action: "create".into(),
            id: None,
            changes: Vec::new(),
        };
        let mut credential_changed = false;
        if let Some(existing) = current.ds_by_name(&name) {
            item.id = Some(existing.id);
            if existing.ds_type != ty {
                item.changes.push(ApplyChange {
                    field: "type".into(),
                    from: json!(existing.ds_type),
                    to: json!(ty),
                });
            }
            // per top-level connJson key, so a one-field tweak reads as one
            // diff line instead of a whole-object swap
            let have: Map<String, Value> =
                serde_json::from_str(&existing.conn_json).unwrap_or_default();
            let mut keys: Vec<&str> = have.keys().map(String::as_str).collect();
            keys.extend(conn.as_object().unwrap().keys().map(String::as_str));
            keys.sort_unstable();
            keys.dedup();
            for k in keys {
                let from = have.get(k).cloned().unwrap_or(Value::Null);
                let to = conn.get(k).cloned().unwrap_or(Value::Null);
                if from != to {
                    item.changes.push(ApplyChange {
                        field: format!("connJson.{k}"),
                        from,
                        to,
                    });
                }
            }
            // credential: a declared (re)set counts as a change only when
            // it actually differs — the stored halves are opened and
            // compared against the same plaintext payload the CRUD path
            // seals, so re-applying a file that carries credentials stays
            // idempotent. Values are never echoed.
            if let Some(want_plain) = credential_payload(body) {
                credential_changed = !store
                    .credential(existing.id)
                    .ok()
                    .flatten()
                    .and_then(|c| cipher.open(&c.nonce, &c.ciphertext).ok())
                    .is_some_and(|have_plain| have_plain == want_plain);
                if credential_changed {
                    item.changes.push(ApplyChange {
                        field: "credential".into(),
                        from: json!("<redacted>"),
                        to: json!("<redacted>"),
                    });
                }
            }
            item.action = if item.changes.is_empty() {
                "unchanged".into()
            } else {
                "update".into()
            };
        } else {
            credential_changed = credential_payload(body).is_some();
        }
        let credential = if credential_changed {
            credential_payload(body).and_then(|p| cipher.seal(&p).ok())
        } else {
            None
        };
        let existing = current.ds_by_name(&name).map(|d| (d.id, d.generation));
        datasources.push(DsPlan {
            item,
            name,
            ty,
            conn_json,
            credential,
            existing,
        });
    }

    // ---------------- tables ----------------
    let mut tables = Vec::new();
    let mut seen_table: HashSet<String> = HashSet::new();
    for (i, entry) in file.tables.as_deref().unwrap_or(&[]).iter().enumerate() {
        let idx = i.to_string();
        let ds_name = entry.datasource.trim().to_string();
        if ds_name.is_empty() {
            return Err(missing("tables", &idx, "datasource"));
        }
        // resolve the owning datasource: file declaration first (what the
        // world looks like after this apply), then the store
        let ds_type = file_ds_types
            .get(&ds_name)
            .cloned()
            .or_else(|| current.ds_by_name(&ds_name).map(|d| d.ds_type.clone()));
        let Some(ds_type) = ds_type else {
            return Err(messages::bad_request(
                "err.apply.table_datasource_unknown",
                &[&entry.body.db_name, &ds_name],
            )
            .into_response());
        };
        let body = &entry.body;
        let db_name = body.db_name.trim().to_string();
        let table_name = body.table_name.trim().to_string();
        if db_name.is_empty() || table_name.is_empty() {
            return Err(missing("tables", &idx, "dbName / tableName"));
        }
        let key = format!("{ds_name}/{db_name}.{table_name}");
        if !seen_table.insert(key.clone()) {
            return Err(duplicate("tables", &key));
        }
        // kind: derived from the datasource type; the one accepted override
        // is `dim` (T-D1, mysql/postgresql only) — same gate as the REST
        // register endpoint
        let kind = match body
            .kind
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            None => table_kind_for(&ds_type).to_string(),
            Some("dim") => {
                if !matches!(ds_type.as_str(), "mysql" | "postgresql") {
                    return Err(messages::bad_request(
                        "err.apply.table_kind_invalid",
                        &[&key, "dim", &ds_type],
                    )
                    .into_response());
                }
                "dim".to_string()
            }
            Some(other) => {
                return Err(messages::bad_request(
                    "err.apply.table_kind_invalid",
                    &[&key, other, &ds_type],
                )
                .into_response())
            }
        };
        let sequence_column = body
            .sequence_column
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        if kind == "dim" && sequence_column.is_none() {
            return Err(
                messages::bad_request("err.apply.table_dim_sequence_required", &[&key])
                    .into_response(),
            );
        }
        let key_model = match body
            .key_model
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            None => None,
            Some(s) => {
                if !matches!(ds_type.as_str(), "doris" | "starrocks") {
                    return Err(messages::bad_request(
                        "err.apply.table_key_model_invalid",
                        &[&key, s, &ds_type],
                    )
                    .into_response());
                }
                let model = parse_key_model(s).ok_or_else(|| {
                    messages::bad_request("err.apply.table_key_model_invalid", &[&key, s, &ds_type])
                        .into_response()
                })?;
                Some(model.name().to_string())
            }
        };
        let columns_json = body
            .columns_json
            .as_ref()
            .map(|cols| serde_json::to_string(cols).unwrap_or_else(|_| "[]".into()));
        let format = body
            .format
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        let mut item = ApplyItem {
            key: key.clone(),
            action: "create".into(),
            id: None,
            changes: Vec::new(),
        };
        // the table can only already exist when its datasource does (the
        // store keys table rows by datasource id)
        let existing_row = current.ds_by_name(&ds_name).and_then(|d| {
            current.tables.iter().find(|t| {
                t.datasource_id == d.id && t.db_name == db_name && t.table_name == table_name
            })
        });
        if let Some(existing) = existing_row {
            item.id = Some(existing.id);
            if existing.kind != kind {
                item.changes.push(ApplyChange {
                    field: "kind".into(),
                    from: json!(existing.kind),
                    to: json!(kind),
                });
            }
            if existing.format != format {
                item.changes.push(ApplyChange {
                    field: "format".into(),
                    from: json!(existing.format),
                    to: json!(format),
                });
            }
            let have_cols: Option<Value> = store
                .table_object_columns_json(existing.id)
                .ok()
                .flatten()
                .flatten()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok());
            let want_cols: Option<Value> = columns_json
                .as_deref()
                .and_then(|s| serde_json::from_str::<Value>(s).ok());
            if have_cols != want_cols {
                item.changes.push(ApplyChange {
                    field: "columnsJson".into(),
                    from: have_cols.unwrap_or(Value::Null),
                    to: want_cols.unwrap_or(Value::Null),
                });
            }
            if existing.sequence_column != sequence_column {
                item.changes.push(ApplyChange {
                    field: "sequenceColumn".into(),
                    from: json!(existing.sequence_column),
                    to: json!(sequence_column),
                });
            }
            if existing.key_model != key_model {
                item.changes.push(ApplyChange {
                    field: "keyModel".into(),
                    from: json!(existing.key_model),
                    to: json!(key_model),
                });
            }
            item.action = if item.changes.is_empty() {
                "unchanged".into()
            } else {
                "update".into()
            };
        }
        tables.push(TablePlan {
            item,
            ds_name,
            db_name,
            table_name,
            kind,
            format,
            columns_json,
            sequence_column,
            key_model,
            existing_id: existing_row.map(|t| t.id),
        });
    }

    // ---------------- pipelines ----------------
    let mut pipelines = Vec::new();
    let mut seen_pipe: HashSet<String> = HashSet::new();
    for (i, body) in file.pipelines.as_deref().unwrap_or(&[]).iter().enumerate() {
        let idx = i.to_string();
        let name = body.name.trim().to_string();
        if name.is_empty() {
            return Err(missing("pipelines", &idx, "name"));
        }
        if !seen_pipe.insert(name.clone()) {
            return Err(duplicate("pipelines", &name));
        }
        // dsMapping: alias → datasource NAME (portable); numeric ids (the
        // REST wire form) resolve through the store. A value that matches
        // neither the file nor the store refuses the whole file.
        let mapping_names = match &body.ds_mapping {
            None => None,
            Some(m) => {
                let mut out = BTreeMap::new();
                for (alias, v) in m {
                    let unknown = || {
                        messages::bad_request(
                            "err.apply.mapping_datasource_unknown",
                            &[&name, alias, &v.to_string()],
                        )
                        .into_response()
                    };
                    let resolved = match v {
                        Value::Number(n) => n
                            .as_i64()
                            .and_then(|id| current.ds_name_by_id(id).map(str::to_string))
                            .ok_or_else(unknown)?,
                        Value::String(s) => {
                            let s = s.trim();
                            if file_ds_types.contains_key(s) {
                                s.to_string()
                            } else if let Some(ds) = current.ds_by_name(s) {
                                ds.name.clone()
                            } else if let Ok(id) = s.parse::<i64>() {
                                current
                                    .ds_name_by_id(id)
                                    .map(str::to_string)
                                    .ok_or_else(unknown)?
                            } else {
                                return Err(unknown());
                            }
                        }
                        _ => return Err(unknown()),
                    };
                    out.insert(alias.clone(), resolved);
                }
                Some(out)
            }
        };
        let checkpoint_ms = body.checkpoint_ms.unwrap_or(10000);
        let config_json = body
            .config
            .as_ref()
            .map(|c| serde_json::to_string(c).unwrap_or_else(|_| "{}".into()));

        let mut item = ApplyItem {
            key: name.clone(),
            action: "create".into(),
            id: None,
            changes: Vec::new(),
        };
        let existing = current.pipelines.iter().find(|f| f.base.name == name);
        if let Some(full) = existing {
            let p = &full.base;
            item.id = Some(p.id);
            if p.sql_content != body.sql_content {
                item.changes.push(ApplyChange {
                    field: "sqlContent".into(),
                    from: json!(p.sql_content),
                    to: json!(body.sql_content),
                });
            }
            // current ids → names for a like-for-like compare
            let have_mapping: Option<BTreeMap<String, String>> = p
                .ds_mapping_json
                .as_deref()
                .and_then(|s| serde_json::from_str::<Map<String, Value>>(s).ok())
                .map(|m| {
                    m.iter()
                        .map(|(alias, v)| {
                            let id = v
                                .as_i64()
                                .or_else(|| v.as_str().and_then(|s| s.parse().ok()));
                            let ds = id
                                .and_then(|id| current.ds_name_by_id(id).map(str::to_string))
                                .unwrap_or_else(|| match id {
                                    Some(id) => format!("#{id}"),
                                    None => v.to_string(),
                                });
                            (alias.clone(), ds)
                        })
                        .collect()
                });
            if have_mapping != mapping_names {
                item.changes.push(ApplyChange {
                    field: "dsMapping".into(),
                    from: serde_json::to_value(have_mapping.unwrap_or_default())
                        .unwrap_or(Value::Null),
                    to: serde_json::to_value(mapping_names.clone().unwrap_or_default())
                        .unwrap_or(Value::Null),
                });
            }
            if p.checkpoint_ms != checkpoint_ms {
                item.changes.push(ApplyChange {
                    field: "checkpointMs".into(),
                    from: json!(p.checkpoint_ms),
                    to: json!(checkpoint_ms),
                });
            }
            if p.preferred_node != body.preferred_node_id {
                item.changes.push(ApplyChange {
                    field: "preferredNodeId".into(),
                    from: json!(p.preferred_node),
                    to: json!(body.preferred_node_id),
                });
            }
            // both sides normalized through PipelineConfig, the same
            // round-trip the detail DTO performs
            let have_cfg = full
                .config_json
                .as_deref()
                .and_then(|s| serde_json::from_str::<PipelineConfig>(s).ok())
                .map(|c| serde_json::to_value(&c).unwrap_or(Value::Null));
            let want_cfg = body
                .config
                .as_ref()
                .map(|c| serde_json::to_value(c).unwrap_or(Value::Null));
            if have_cfg != want_cfg {
                item.changes.push(ApplyChange {
                    field: "config".into(),
                    from: have_cfg.unwrap_or(Value::Null),
                    to: want_cfg.unwrap_or(Value::Null),
                });
            }
            item.action = if item.changes.is_empty() {
                "unchanged".into()
            } else {
                "update".into()
            };
        }
        pipelines.push(PipePlan {
            item,
            name,
            sql_content: body.sql_content.clone(),
            mapping_names,
            checkpoint_ms,
            preferred_node: body.preferred_node_id,
            config_json,
            existing_id: existing.map(|f| f.base.id),
        });
    }

    // ---------------- orphans (present section = declared full set) ----------------
    let mut orphans = Vec::new();
    if file.datasources.is_some() {
        let named: HashSet<&str> = datasources.iter().map(|d| d.name.as_str()).collect();
        for ds in &current.datasources {
            if !named.contains(ds.name.as_str()) {
                orphans.push(ApplyOrphan {
                    kind: "datasource".into(),
                    key: ds.name.clone(),
                    id: ds.id,
                });
            }
        }
    }
    if file.tables.is_some() {
        let named: HashSet<&str> = tables.iter().map(|t| t.item.key.as_str()).collect();
        for t in &current.tables {
            let key = match current.ds_name_by_id(t.datasource_id) {
                Some(ds) => format!("{ds}/{}.{}", t.db_name, t.table_name),
                None => format!("#{}/{}.{}", t.datasource_id, t.db_name, t.table_name),
            };
            if !named.contains(key.as_str()) {
                orphans.push(ApplyOrphan {
                    kind: "table".into(),
                    key,
                    id: t.id,
                });
            }
        }
    }
    if file.pipelines.is_some() {
        let named: HashSet<&str> = pipelines.iter().map(|p| p.name.as_str()).collect();
        for full in &current.pipelines {
            if !named.contains(full.base.name.as_str()) {
                orphans.push(ApplyOrphan {
                    kind: "pipeline".into(),
                    key: full.base.name.clone(),
                    id: full.base.id,
                });
            }
        }
    }

    let mut summary = ApplySummary::default();
    for item in datasources
        .iter()
        .map(|d| &d.item)
        .chain(tables.iter().map(|t| &t.item))
        .chain(pipelines.iter().map(|p| &p.item))
    {
        match item.action.as_str() {
            "create" => summary.create += 1,
            "update" => summary.update += 1,
            _ => summary.unchanged += 1,
        }
    }
    summary.orphan = orphans.len() as u32;

    Ok(Plan {
        datasources,
        tables,
        pipelines,
        orphans,
        summary,
        managed: (
            file.datasources.is_some(),
            file.tables.is_some(),
            file.pipelines.is_some(),
        ),
    })
}

impl Plan {
    fn to_response(&self, dry_run: bool) -> ApplyResponse {
        // a section the file did not declare is absent from the wire; a
        // declared one is present even when empty (declared full set)
        let section = |managed: bool, items: Vec<ApplyItem>| managed.then_some(items);
        ApplyResponse {
            dry_run,
            summary: self.summary.clone(),
            datasources: section(
                self.managed.0,
                self.datasources.iter().map(|d| d.item.clone()).collect(),
            ),
            tables: section(
                self.managed.1,
                self.tables.iter().map(|t| t.item.clone()).collect(),
            ),
            pipelines: section(
                self.managed.2,
                self.pipelines.iter().map(|p| p.item.clone()).collect(),
            ),
            orphans: self.orphans.clone(),
        }
    }
}

// ---------------- execution (apply only) ----------------

/// Run the plan. Order is dependency-shaped: datasources → tables →
/// pipelines (tables and dsMapping reference datasource names, which must
/// exist by the time they resolve). Not one transaction across objects —
/// a mid-plan store failure can leave earlier writes standing; re-applying
/// the same file converges (apply is idempotent), which is the declared
/// recovery path. Objects already matching were never planned as writes,
/// so they stay byte-untouched: no status flip, no generation bump, no
/// audit row.
fn execute(state: &AppState, plan: &mut Plan, operator: &str) -> Result<Response, Response> {
    let store = state.store();

    for ds in &mut plan.datasources {
        match ds.existing {
            None => {
                let id = store
                    .insert_datasource(&ds.name, &ds.ty, &ds.conn_json)
                    .map_err(|e| conflict_or_internal(&e).into_response())?;
                if let Some((nonce, ciphertext)) = &ds.credential {
                    store
                        .upsert_credential(id, nonce, ciphertext)
                        .map_err(|e| ApiError::internal(&e).into_response())?;
                }
                ds.item.id = Some(id);
                audit(
                    store,
                    operator,
                    "DATASOURCE_CREATE",
                    "datasource",
                    Some(id.to_string()),
                    Some(json!({"name": ds.name, "type": ds.ty, "via": "apply"}).to_string()),
                );
            }
            Some((id, generation)) if ds.item.action == "update" => {
                match store.update_datasource(
                    id,
                    &ds.name,
                    &ds.ty,
                    &ds.conn_json,
                    generation,
                    ds.credential
                        .as_ref()
                        .map(|(a, b)| (a.as_str(), b.as_str())),
                ) {
                    Ok(DatasourceUpdate::Applied(gen)) => {
                        audit(
                            store,
                            operator,
                            "DATASOURCE_UPDATE",
                            "datasource",
                            Some(id.to_string()),
                            Some(
                                json!({
                                    "name": ds.name,
                                    "generation": gen,
                                    "fields": fields_of(&ds.item),
                                    "via": "apply",
                                })
                                .to_string(),
                            ),
                        );
                    }
                    // a concurrent edit raced the plan's snapshot — refuse
                    // rather than blind-write (the PUT CAS ruling, carried
                    // over to the declarative path)
                    Ok(_) => {
                        return Err(messages::respond(
                            axum::http::StatusCode::CONFLICT,
                            "CONFLICT",
                            "err.apply.concurrent_modification",
                            &[&ds.name],
                        ))
                    }
                    Err(e) => return Err(ApiError::internal(&e).into_response()),
                }
            }
            _ => {}
        }
    }

    // refresh name → id AFTER the datasource section so table/mapping
    // resolution sees freshly created rows
    let ds_id_by_name: HashMap<String, i64> = store
        .list_datasources(None)
        .map_err(|e| ApiError::internal(&e).into_response())?
        .into_iter()
        .map(|d| (d.name, d.id))
        .collect();

    for t in &mut plan.tables {
        let Some(ds_id) = ds_id_by_name.get(&t.ds_name).copied() else {
            // unreachable unless the datasource vanished mid-apply
            return Err(messages::bad_request(
                "err.apply.table_datasource_unknown",
                &[&format!("{}.{}", t.db_name, t.table_name), &t.ds_name],
            )
            .into_response());
        };
        match t.existing_id {
            None => {
                let id = store
                    .insert_table_object(
                        ds_id,
                        &t.db_name,
                        &t.table_name,
                        &t.kind,
                        t.format.as_deref(),
                        t.columns_json.as_deref(),
                        t.sequence_column.as_deref(),
                        t.key_model.as_deref(),
                    )
                    .map_err(|e| conflict_or_internal(&e).into_response())?;
                t.item.id = Some(id);
                audit(
                    store,
                    operator,
                    "TABLE_CREATE",
                    "table_object",
                    Some(id.to_string()),
                    Some(
                        json!({
                            "datasourceId": ds_id, "dbName": t.db_name, "tableName": t.table_name,
                            "kind": t.kind, "via": "apply",
                        })
                        .to_string(),
                    ),
                );
            }
            Some(id) if t.item.action == "update" => {
                store
                    .update_table_object(
                        id,
                        &t.kind,
                        t.format.as_deref(),
                        t.columns_json.as_deref(),
                        t.sequence_column.as_deref(),
                        t.key_model.as_deref(),
                    )
                    .map_err(|e| ApiError::internal(&e).into_response())?;
                audit(
                    store,
                    operator,
                    "TABLE_UPDATE",
                    "table_object",
                    Some(id.to_string()),
                    Some(
                        json!({
                            "datasourceId": ds_id, "dbName": t.db_name, "tableName": t.table_name,
                            "fields": fields_of(&t.item),
                            "via": "apply",
                        })
                        .to_string(),
                    ),
                );
            }
            _ => {}
        }
    }

    for p in &mut plan.pipelines {
        // names → ids for storage (the store's wire form)
        let mapping_json = p.mapping_names.as_ref().map(|m| {
            let map: Map<String, Value> = m
                .iter()
                .map(|(alias, name)| {
                    // unreachable i64::MIN sentinel: plan validated every
                    // name against file ∪ store, and the datasource section
                    // ran first
                    let id = ds_id_by_name.get(name).copied().unwrap_or(i64::MIN);
                    (alias.clone(), Value::from(id))
                })
                .collect();
            serde_json::to_string(&map).unwrap_or_else(|_| "{}".into())
        });
        match p.existing_id {
            None => match store.create_pipeline(
                &p.name,
                &p.sql_content,
                mapping_json.as_deref(),
                p.checkpoint_ms,
                p.preferred_node,
                p.config_json.as_deref(),
            ) {
                Ok(console_store::PipelineWrite::Created(id)) => {
                    p.item.id = Some(id);
                    audit(
                        store,
                        operator,
                        "PIPELINE_CREATE",
                        "pipeline",
                        Some(id.to_string()),
                        Some(json!({"name": p.name, "via": "apply"}).to_string()),
                    );
                }
                Ok(console_store::PipelineWrite::NameConflict) => {
                    return Err(conflict_or_internal(&anyhow::anyhow!(
                        "UNIQUE constraint failed: pipeline.name"
                    ))
                    .into_response())
                }
                Err(e) => return Err(ApiError::internal(&e).into_response()),
            },
            Some(id) if p.item.action == "update" => {
                match store.update_pipeline(
                    id,
                    &p.name,
                    &p.sql_content,
                    mapping_json.as_deref(),
                    p.checkpoint_ms,
                    p.preferred_node,
                    p.config_json.as_deref(),
                ) {
                    Ok(console_store::PipelineWrite::Created(_)) => {
                        audit(
                            store,
                            operator,
                            "PIPELINE_UPDATE",
                            "pipeline",
                            Some(id.to_string()),
                            Some(
                                json!({
                                    "name": p.name,
                                    "fields": fields_of(&p.item),
                                    "via": "apply",
                                })
                                .to_string(),
                            ),
                        );
                    }
                    Ok(console_store::PipelineWrite::NameConflict) => {
                        return Err(conflict_or_internal(&anyhow::anyhow!(
                            "UNIQUE constraint failed: pipeline.name"
                        ))
                        .into_response())
                    }
                    Err(e) => return Err(ApiError::internal(&e).into_response()),
                }
            }
            _ => {}
        }
    }

    // one summary row when the apply actually wrote something (a no-op
    // apply writes nothing and audits nothing — same ruling as refused
    // writes and dry runs; a CI loop re-applying an already-converged file
    // must not spam the trail)
    let mut created: Vec<&str> = Vec::new();
    let mut updated: Vec<&str> = Vec::new();
    for item in plan
        .datasources
        .iter()
        .map(|d| &d.item)
        .chain(plan.tables.iter().map(|t| &t.item))
        .chain(plan.pipelines.iter().map(|p| &p.item))
    {
        match item.action.as_str() {
            "create" => created.push(item.key.as_str()),
            "update" => updated.push(item.key.as_str()),
            _ => {}
        }
    }
    if !created.is_empty() || !updated.is_empty() {
        audit(
            store,
            operator,
            "APPLY",
            "apply",
            None,
            Some(
                json!({
                    "summary": {
                        "create": plan.summary.create,
                        "update": plan.summary.update,
                        "unchanged": plan.summary.unchanged,
                        "orphan": plan.summary.orphan,
                    },
                    "created": created,
                    "updated": updated,
                })
                .to_string(),
            ),
        );
    }

    Ok(Json(plan.to_response(false)).into_response())
}

fn fields_of(item: &ApplyItem) -> Vec<&str> {
    item.changes.iter().map(|c| c.field.as_str()).collect()
}

/// UNIQUE-constraint failures surface as 409 (a name raced us), the rest
/// as 500 — same split as the datasources CRUD handlers.
fn conflict_or_internal(e: &anyhow::Error) -> Response {
    let is_constraint = e
        .chain()
        .any(|c| c.to_string().contains("UNIQUE constraint failed"));
    if is_constraint {
        ApiError::new(
            axum::http::StatusCode::CONFLICT,
            "CONFLICT",
            "name already taken",
        )
        .into_response()
    } else {
        ApiError::internal(e).into_response()
    }
}
