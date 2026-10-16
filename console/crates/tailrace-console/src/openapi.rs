//! OpenAPI document (A4: utoipa 6 embedded, served at /swagger-ui).

use utoipa::OpenApi;

use crate::api::artifacts::{ArtifactItem, ArtifactUploaded, ArtifactsResponse};
use crate::api::auth::{LoginRequest, LoginResponse, MeResponse, UserInfo};
use crate::api::datasources::{
    DatasourceCreated, DatasourceItem, DatasourceListResponse, DatasourceType,
    DatasourceTypesResponse, DatasourceUpsertBody, DiscoverBody, TableListResponse,
    TableObjectItem, TableRegisterBody, TableRegistered, TestBody, TestResponse, TypeField,
};
use crate::api::nodes::{
    HeartbeatBody, HeartbeatResponse, NodeCreated, NodeEcho, RegisterBody, RegisterResponse,
};
use crate::api::nodes_admin::{
    DeployBody, DeployLogItem, DeployLogResponse, EventsResponse, ManualRegisterBody,
    ManualRegistered, MetricsResponse, MigrateBody, NodeActionBody, NodeCapacity,
    NodeDeleteResponse, NodeDetail, NodeEventItem, NodeListItem, NodeMetricPointOut,
    NodesListResponse, NodesSummary,
};
use crate::api::pipelines::{
    CompileBody, CompileResponse, DeployBody as PipeDeployBody, PipelineConfig, PipelineCreated,
    PipelineDetailOut, PipelineListItem, PipelineUpsertBody, PipelinesListResponse,
    PublishResponse, SqlTemplate, TemplatesResponse, TrendPoint, TrendResponse,
};
use crate::api::{ErrorBody, ErrorResponse};
use crate::compile::{CompileDiag, StatementOut};

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
        TableRegisterBody, TableRegistered, TableListResponse, TableObjectItem,
        // nodes admin
        NodesListResponse, NodeListItem, NodesSummary, NodeCapacity, NodeDetail,
        ManualRegisterBody, ManualRegistered, NodeDeleteResponse,
        EventsResponse, NodeEventItem, MetricsResponse, NodeMetricPointOut,
        DeployLogResponse, DeployLogItem, DeployBody, NodeActionBody, MigrateBody,
        // artifacts
        ArtifactsResponse, ArtifactItem, ArtifactUploaded,
        // pipelines
        PipelinesListResponse, PipelineListItem, PipelineDetailOut, PipelineUpsertBody,
        PipelineConfig, PipelineCreated, CompileBody, CompileResponse, CompileDiag,
        StatementOut, PublishResponse, PipeDeployBody, TrendResponse, TrendPoint,
        TemplatesResponse, SqlTemplate,
        // shared
        ErrorResponse, ErrorBody,
    )))]
pub struct ApiDoc;
