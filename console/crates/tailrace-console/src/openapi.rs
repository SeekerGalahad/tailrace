//! OpenAPI document (A4: utoipa 6 embedded, served at /swagger-ui).

use utoipa::OpenApi;

use crate::api::artifacts::{ArtifactItem, ArtifactUploaded, ArtifactsResponse};
use crate::api::auth::{LoginRequest, LoginResponse, MeResponse, UserInfo};
use crate::api::datasources::{
    DatasourceCreated, DatasourceItem, DatasourceListResponse, DatasourceType,
    DatasourceTypesResponse, DatasourceUpsertBody, DdlDraftBody, DdlDraftResponse,
    DdlEvolveResponse, DdlExecuteBody, DdlExecuteResponse, DdlPartitionDto, DiscoverBody,
    DiscoverResponse, DiscoveredAssetDto, InferSchemaBody, InferSchemaResponse, TableKeyModelBody,
    TableKeyModelSet, TableListResponse, TableObjectItem, TableRegisterBody, TableRegistered,
    TestBody, TestResponse, TypeField,
};
use crate::api::nodes::{
    HeartbeatBody, HeartbeatResponse, NodeCreated, NodeEcho, RegisterBody, RegisterResponse,
};
use crate::api::nodes_admin::{
    ActionAccepted, DeployAccepted, DeployBody, DeployHost, DeployLogItem, DeployLogResponse,
    DeployResponse, EventsResponse, ManualRegisterBody, ManualRegistered, MetricsResponse,
    MigrateBody, NodeActionBody, NodeCapacity, NodeDeleteResponse, NodeDetail, NodeEventItem,
    NodeListItem, NodeMetricPointOut, NodesListResponse, NodesSummary,
};
use crate::api::pipelines::{
    BackfillAccepted, BackfillBody, BackfillBound, BackfillDryRunOut, BackfillJobItem,
    BackfillJobsResponse, CompileBody, CompileResponse, DeployBody as PipeDeployBody,
    PartialUpdatePipeline, PipelineConfig, PipelineCreated, PipelineDetailOut, PipelineListItem,
    PipelineUpsertBody, PipelinesListResponse, PublishResponse, SqlTemplate, TemplatesResponse,
    TrendPoint, TrendResponse,
};
use crate::api::{ErrorBody, ErrorResponse};
use crate::compile::{CompileDiag, DimJoinOut, StatementOut};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Tailrace Console API",
        version = env!("CARGO_PKG_VERSION"),
        description = "管理面 REST。认证：POST /api/auth/login 签发不透明 token，经 Cookie tr_session 或 Authorization: Bearer 携带；/api/nodes/register|heartbeat 为 m2m 免会话端点（bootstrap/节点 token 自证）。失败信封 {\"error\":{code,message}}；compile 的 400 按其自带 diagnostics 形态返回。"
    ),
    paths(
        // auth (§4.1)
        crate::api::auth::login,
        crate::api::auth::logout,
        crate::api::auth::me,
        // m2m (§4.3)
        crate::api::nodes::register,
        crate::api::nodes::heartbeat,
        // health
        crate::api::health::healthz,
        // datasources (§4.2)
        crate::api::datasources::list_types,
        crate::api::datasources::list,
        crate::api::datasources::create,
        crate::api::datasources::detail,
        crate::api::datasources::update,
        crate::api::datasources::remove,
        crate::api::datasources::test_unsaved,
        crate::api::datasources::test_by_id,
        crate::api::datasources::discover,
        crate::api::datasources::infer_schema,
        crate::api::datasources::register_table,
        crate::api::datasources::list_tables,
        crate::api::datasources::remove_table,
        // T-A7 key model on record (partial-update publish gate basis)
        crate::api::datasources::set_table_key_model,
        // T-A1 create-table closed loop (§7.2)
        crate::api::datasources::ddl_draft,
        crate::api::datasources::ddl_execute,
        crate::api::datasources::ddl_evolve,
        // node admin (§4.3/§4.4)
        crate::api::nodes_admin::list,
        crate::api::nodes_admin::detail,
        crate::api::nodes_admin::register_manual,
        crate::api::nodes_admin::revoke,
        crate::api::nodes_admin::remove,
        crate::api::nodes_admin::events,
        crate::api::nodes_admin::all_events,
        crate::api::nodes_admin::metrics,
        crate::api::nodes_admin::pipelines_runtime,
        crate::api::nodes_admin::deploy_log,
        crate::api::nodes_admin::deploy,
        crate::api::nodes_admin::action,
        crate::api::nodes_admin::migrate,
        // artifacts (§4.4)
        crate::api::artifacts::list,
        crate::api::artifacts::upload,
        crate::api::artifacts::remove,
        // pipelines (§4.5)
        crate::api::pipelines::list,
        crate::api::pipelines::create,
        crate::api::pipelines::detail,
        crate::api::pipelines::update,
        crate::api::pipelines::remove,
        crate::api::pipelines::compile,
        crate::api::pipelines::publish,
        crate::api::pipelines::deploy,
        crate::api::pipelines::recover,
        crate::api::pipelines::pause,
        crate::api::pipelines::resume,
        crate::api::pipelines::stop,
        crate::api::pipelines::stats,
        crate::api::pipelines::offsets,
        crate::api::pipelines::logs,
        crate::api::pipelines::metrics_trend,
        crate::api::pipelines::templates,
        // T-A2 backfill (§7.3): bounded replay + job progress surface
        crate::api::pipelines::backfill,
        crate::api::pipelines::backfill_jobs,
    ),
    components(schemas(
        // auth
        LoginRequest, LoginResponse, MeResponse, UserInfo,
        // m2m
        RegisterBody, RegisterResponse, NodeCreated,
        HeartbeatBody, HeartbeatResponse, NodeEcho,
        // datasources
        DatasourceTypesResponse, DatasourceType, TypeField,
        DatasourceListResponse, DatasourceItem, DatasourceUpsertBody, DatasourceCreated,
        TestBody, TestResponse, DiscoverBody,
        // T14 discover/infer-schema DTOs (T24③: the paths at
        // datasources::{discover, infer_schema} referenced these bodies —
        // without components entries every $ref dangled in swagger-ui)
        DiscoverResponse, DiscoveredAssetDto, InferSchemaBody, InferSchemaResponse,
        TableRegisterBody, TableRegistered, TableListResponse, TableObjectItem,
        // T-A7 key model PUT
        TableKeyModelBody, TableKeyModelSet,
        // T-A1 DDL closed-loop DTOs (draft / execute / evolve)
        DdlDraftBody, DdlDraftResponse, DdlPartitionDto, DdlExecuteBody, DdlExecuteResponse,
        DdlEvolveResponse,
        // T-A7 evolve mutex: the partial-update pipelines on the table
        PartialUpdatePipeline,
        // nodes admin
        NodesListResponse, NodeListItem, NodesSummary, NodeCapacity, NodeDetail,
        ManualRegisterBody, ManualRegistered, NodeDeleteResponse,
        EventsResponse, NodeEventItem, MetricsResponse, NodeMetricPointOut,
        DeployLogResponse, DeployLogItem, DeployBody, DeployHost, DeployResponse,
        DeployAccepted, NodeActionBody, ActionAccepted, MigrateBody,
        // artifacts
        ArtifactsResponse, ArtifactItem, ArtifactUploaded,
        // pipelines
        PipelinesListResponse, PipelineListItem, PipelineDetailOut, PipelineUpsertBody,
        PipelineConfig, PipelineCreated, CompileBody, CompileResponse, CompileDiag,
        StatementOut, DimJoinOut, PublishResponse, PipeDeployBody, TrendResponse, TrendPoint,
        TemplatesResponse, SqlTemplate,
        // T-A2 backfill DTOs (§7.3)
        BackfillBody, BackfillBound, BackfillDryRunOut, BackfillAccepted, BackfillJobItem,
        BackfillJobsResponse,
        // shared
        ErrorResponse, ErrorBody,
    )))]
pub struct ApiDoc;

#[cfg(test)]
mod tests {
    use super::ApiDoc;
    use utoipa::OpenApi;

    /// T24③ regression guard: every `#/components/schemas/…` $ref anywhere in
    /// the document must resolve to a declared schema. The T14 paths
    /// (discover / infer-schema) referenced response bodies that were missing
    /// from components — swagger-ui rendered dangling $refs. Walking ALL refs
    /// (request bodies, responses, nested) keeps the whole document honest.
    #[test]
    fn every_schema_ref_resolves() {
        let doc = serde_json::to_value(ApiDoc::openapi()).expect("openapi serializes");
        let schemas = doc
            .pointer("/components/schemas")
            .and_then(|v| v.as_object())
            .expect("components.schemas object")
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<String>>();
        assert!(
            schemas.contains("DiscoverResponse")
                && schemas.contains("DiscoveredAssetDto")
                && schemas.contains("InferSchemaBody")
                && schemas.contains("InferSchemaResponse"),
            "T24③: discover/infer-schema DTOs must be registered, got: {schemas:?}"
        );
        let mut dangling = Vec::new();
        fn walk(
            v: &serde_json::Value,
            schemas: &std::collections::BTreeSet<String>,
            out: &mut Vec<String>,
        ) {
            match v {
                serde_json::Value::Object(map) => {
                    if let Some(r) = map.get("$ref").and_then(|r| r.as_str()) {
                        if let Some(name) = r.strip_prefix("#/components/schemas/") {
                            if !schemas.contains(name) {
                                out.push(r.to_string());
                            }
                        }
                    }
                    for child in map.values() {
                        walk(child, schemas, out);
                    }
                }
                serde_json::Value::Array(items) => {
                    for child in items {
                        walk(child, schemas, out);
                    }
                }
                _ => {}
            }
        }
        walk(&doc, &schemas, &mut dangling);
        assert!(dangling.is_empty(), "dangling $refs: {dangling:?}");
    }

    /// T39 ⑤: the list-tables DTO (`TableObjectItem.columns_json`) exposes
    /// `columnsJson` in the published contract (camelCase on the wire,
    /// nullable), so the swagger document and the web client type agree on
    /// the column-completion hop.
    #[test]
    fn table_object_item_exposes_columns_json() {
        let doc = serde_json::to_value(ApiDoc::openapi()).expect("openapi serializes");
        let props = doc
            .pointer("/components/schemas/TableObjectItem/properties")
            .and_then(|v| v.as_object())
            .expect("TableObjectItem.properties");
        assert!(
            props.contains_key("columnsJson"),
            "TableObjectItem must publish columnsJson, got keys: {:?}",
            props.keys().collect::<Vec<_>>()
        );
        // the pre-existing fields are still there (additive change)
        for k in [
            "id",
            "datasourceId",
            "dbName",
            "tableName",
            "kind",
            "cursorSemantics",
            "source",
        ] {
            assert!(props.contains_key(k), "missing {k}");
        }
    }

    /// T-D1: the compile DTO carries `dims` (the dim-join references the
    /// publish gate resolves), and the list-tables DTO carries the declared
    /// `sequenceColumn` — both are additive wire changes pinned here so the
    /// swagger document and the web/MCP clients agree.
    #[test]
    fn dim_join_fields_are_published() {
        let doc = serde_json::to_value(ApiDoc::openapi()).expect("openapi serializes");
        let stmt = doc
            .pointer("/components/schemas/StatementOut/properties")
            .and_then(|v| v.as_object())
            .expect("StatementOut.properties");
        assert!(
            stmt.contains_key("dims"),
            "StatementOut must publish dims, got keys: {:?}",
            stmt.keys().collect::<Vec<_>>()
        );
        assert!(
            doc.pointer("/components/schemas/DimJoinOut/properties")
                .and_then(|v| v.as_object())
                .is_some_and(|p| p.contains_key("reference") && p.contains_key("name")),
            "DimJoinOut schema must be registered with reference+name"
        );
        let table = doc
            .pointer("/components/schemas/TableObjectItem/properties")
            .and_then(|v| v.as_object())
            .expect("TableObjectItem.properties");
        assert!(
            table.contains_key("sequenceColumn"),
            "TableObjectItem must publish sequenceColumn, got keys: {:?}",
            table.keys().collect::<Vec<_>>()
        );
    }

    /// T-A1: the three DDL closed-loop paths are published, execute declares
    /// its 422 confirmation gate, and the draft / evolve DTOs carry the
    /// annotation fields the wizard reads (`keyColumnGuessed` badge,
    /// `partialUpdateNotice` exclusivity text) — pinned so swagger and the
    /// web client cannot drift from the handlers.
    #[test]
    fn ddl_closed_loop_is_published() {
        let doc = serde_json::to_value(ApiDoc::openapi()).expect("openapi serializes");
        let base = "/api/datasources/{id}/tables/{db}/{table}/ddl";
        for leaf in ["draft", "execute", "evolve"] {
            assert!(
                doc.pointer(&format!("/paths/{}~1{leaf}/post", base.replace('/', "~1")))
                    .is_some(),
                "missing POST {base}/{leaf}"
            );
        }
        assert!(
            doc.pointer(&format!(
                "/paths/{}~1execute/post/responses/422",
                base.replace('/', "~1")
            ))
            .is_some(),
            "execute must declare the 422 key-model gate"
        );
        let draft = doc
            .pointer("/components/schemas/DdlDraftResponse/properties")
            .and_then(|v| v.as_object())
            .expect("DdlDraftResponse.properties");
        for k in [
            "ddl",
            "keyModel",
            "keyColumn",
            "keyColumnGuessed",
            "partition",
            "notes",
        ] {
            assert!(draft.contains_key(k), "DdlDraftResponse missing {k}");
        }
        let evolve = doc
            .pointer("/components/schemas/DdlEvolveResponse/properties")
            .and_then(|v| v.as_object())
            .expect("DdlEvolveResponse.properties");
        for k in ["changed", "statements", "added", "partialUpdateNotice"] {
            assert!(evolve.contains_key(k), "DdlEvolveResponse missing {k}");
        }
    }

    /// T-A2: the backfill path pair is published, POST declares its 422
    /// capability gate, and the dry-run DTO carries the blueprint §7.3
    /// preview fields (jobId null by contract — a dry run creates nothing).
    #[test]
    fn backfill_surface_is_published() {
        let doc = serde_json::to_value(ApiDoc::openapi()).expect("openapi serializes");
        assert!(
            doc.pointer("/paths/~1api~1pipelines~1{id}~1backfill/post")
                .is_some(),
            "missing POST /api/pipelines/{{id}}/backfill"
        );
        assert!(
            doc.pointer("/paths/~1api~1pipelines~1{id}~1backfill~1jobs/get")
                .is_some(),
            "missing GET /api/pipelines/{{id}}/backfill/jobs"
        );
        assert!(
            doc.pointer("/paths/~1api~1pipelines~1{id}~1backfill/post/responses/422")
                .is_some(),
            "backfill must declare the 422 capability gate"
        );
        let dry = doc
            .pointer("/components/schemas/BackfillDryRunOut/properties")
            .and_then(|v| v.as_object())
            .expect("BackfillDryRunOut.properties");
        for k in ["jobId", "estimatedRows", "labelPrefix"] {
            assert!(dry.contains_key(k), "BackfillDryRunOut missing {k}");
        }
        let accepted = doc
            .pointer("/components/schemas/BackfillAccepted/properties")
            .and_then(|v| v.as_object())
            .expect("BackfillAccepted.properties");
        for k in ["jobId", "state", "labelPrefix"] {
            assert!(accepted.contains_key(k), "BackfillAccepted missing {k}");
        }
    }

    /// T-A7: the partial-update control plane is published end to end — the
    /// compile DTO carries `partialColumns` (the statement-level switch),
    /// the table registration / list DTOs carry `keyModel` (the publish-gate
    /// basis), the key-model PUT exists, the evolve DTO carries the
    /// `partialUpdatePipelines` checklist, and deploy / evolve declare the
    /// mutex 409s — pinned so swagger and the clients cannot drift from the
    /// handlers.
    #[test]
    fn partial_update_surface_is_published() {
        let doc = serde_json::to_value(ApiDoc::openapi()).expect("openapi serializes");
        let props = |schema: &str| {
            doc.pointer(&format!("/components/schemas/{schema}/properties"))
                .and_then(|v| v.as_object())
                .cloned()
                .unwrap_or_else(|| panic!("{schema}.properties"))
        };
        assert!(
            props("StatementOut").contains_key("partialColumns"),
            "StatementOut must publish partialColumns"
        );
        assert!(
            props("TableObjectItem").contains_key("keyModel"),
            "TableObjectItem must publish keyModel"
        );
        assert!(
            props("TableRegisterBody").contains_key("keyModel"),
            "TableRegisterBody must accept keyModel"
        );
        assert!(
            props("TableKeyModelBody").contains_key("keyModel")
                && props("TableKeyModelSet").contains_key("keyModel"),
            "key-model PUT DTOs must be registered"
        );
        assert!(
            doc.pointer("/paths/~1api~1datasources~1tables~1{id}/put")
                .is_some(),
            "missing PUT /api/datasources/tables/{{id}}"
        );
        let evolve = props("DdlEvolveResponse");
        assert!(
            evolve.contains_key("partialUpdateNotice")
                && evolve.contains_key("partialUpdatePipelines"),
            "DdlEvolveResponse must carry the mutex notice + pipeline checklist, got {:?}",
            evolve.keys().collect::<Vec<_>>()
        );
        let pipe = props("PartialUpdatePipeline");
        for k in ["id", "name", "status", "deployed", "columns"] {
            assert!(pipe.contains_key(k), "PartialUpdatePipeline missing {k}");
        }
        // the mutex 409s are declared on both sides
        for (path, what) in [
            ("/paths/~1api~1pipelines~1{id}~1deploy/post/responses/409", "deploy"),
            ("/paths/~1api~1pipelines~1{id}~1recover/post/responses/409", "recover"),
            ("/paths/~1api~1pipelines~1{id}~1backfill/post/responses/409", "backfill"),
            (
                "/paths/~1api~1datasources~1{id}~1tables~1{db}~1{table}~1ddl~1evolve/post/responses/409",
                "evolve",
            ),
        ] {
            let desc = doc
                .pointer(&format!("{path}/description"))
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("{what} must declare a 409"));
            let expect = if what == "evolve" {
                "PARTIAL_UPDATE_ACTIVE"
            } else {
                "SCHEMA_CHANGE_IN_PROGRESS"
            };
            assert!(desc.contains(expect), "{what} 409 must name {expect}: {desc}");
        }
    }
}
