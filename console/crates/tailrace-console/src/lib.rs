//! tailrace-console — management-plane binary library (design doc §3.1/§4).
//!
//! The crate is lib+bin so integration tests can exercise the real router
//! assembly. `main.rs` only parses flags, wires startup side effects (store
//! open + migration, admin seed, bootstrap token) and serves.

pub mod api;
pub mod compile;
pub mod deploy;
pub mod mcp;
pub mod metrics;
pub mod openapi;
pub mod spa;
pub mod state;

use axum::extract::DefaultBodyLimit;
use axum::middleware::from_fn_with_state;
use axum::routing::{delete, get, post};
use axum::Router;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use console_auth::{require_node_heartbeat, require_node_register, require_session};
use state::AppState;

/// Full application router: REST API + swagger + MCP + embedded SPA
/// fallback. The MCP write switch comes from `MCP_ALLOW_WRITE` here; main
/// resolves the `--mcp-allow-write` flag and calls [`build_router_with_mcp`].
pub fn build_router(state: AppState) -> Router {
    build_router_with_mcp(state, mcp::McpState::from_env())
}

/// [`build_router`] with an explicit MCP configuration (T-E4 §7.5).
pub fn build_router_with_mcp(state: AppState, mcp_state: mcp::McpState) -> Router {
    // Session-authenticated endpoints (§4.0: cookie tr_session / bearer).
    // Everything under /api except the m2m register/heartbeat pair and
    // login itself lives here.
    let session_scoped = Router::new()
        // auth domain
        .route("/api/auth/logout", post(api::auth::logout))
        .route("/api/auth/me", get(api::auth::me))
        // datasource domain (§4.2)
        .route("/api/datasource-types", get(api::datasources::list_types))
        .route(
            "/api/datasources",
            get(api::datasources::list).post(api::datasources::create),
        )
        .route(
            "/api/datasources/test",
            post(api::datasources::test_unsaved),
        )
        .route(
            "/api/datasources/tables",
            get(api::datasources::list_tables),
        )
        .route(
            "/api/datasources/tables/{id}",
            delete(api::datasources::remove_table),
        )
        .route(
            "/api/datasources/{id}",
            get(api::datasources::detail)
                .put(api::datasources::update)
                .delete(api::datasources::remove),
        )
        .route(
            "/api/datasources/{id}/test",
            post(api::datasources::test_by_id),
        )
        .route(
            "/api/datasources/{id}/discover",
            post(api::datasources::discover),
        )
        .route(
            "/api/datasources/{id}/infer-schema",
            post(api::datasources::infer_schema),
        )
        .route(
            "/api/datasources/{id}/tables",
            post(api::datasources::register_table),
        )
        // T-A1 create-table closed loop (§7.2): draft → confirm → execute,
        // plus additive evolve
        .route(
            "/api/datasources/{id}/tables/{db}/{table}/ddl/draft",
            post(api::datasources::ddl_draft),
        )
        .route(
            "/api/datasources/{id}/tables/{db}/{table}/ddl/execute",
            post(api::datasources::ddl_execute),
        )
        .route(
            "/api/datasources/{id}/tables/{db}/{table}/ddl/evolve",
            post(api::datasources::ddl_evolve),
        )
        // node admin domain (§4.3)
        .route("/api/nodes", get(api::nodes_admin::list))
        .route("/api/nodes/manual", post(api::nodes_admin::register_manual))
        .route("/api/nodes/events", get(api::nodes_admin::all_events))
        .route("/api/nodes/deploy", post(api::nodes_admin::deploy))
        .route(
            "/api/nodes/{id}",
            get(api::nodes_admin::detail).delete(api::nodes_admin::remove),
        )
        .route("/api/nodes/{id}/revoke", post(api::nodes_admin::revoke))
        .route("/api/nodes/{id}/events", get(api::nodes_admin::events))
        .route("/api/nodes/{id}/metrics", get(api::nodes_admin::metrics))
        .route("/api/nodes/{id}/action", post(api::nodes_admin::action))
        .route("/api/nodes/{id}/migrate", post(api::nodes_admin::migrate))
        .route(
            "/api/nodes/{id}/deploy-log",
            get(api::nodes_admin::deploy_log),
        )
        .route(
            "/api/nodes/{id}/pipelines-runtime",
            get(api::nodes_admin::pipelines_runtime),
        )
        // artifact domain (§4.4)
        .route(
            "/api/artifacts",
            get(api::artifacts::list).post(api::artifacts::upload),
        )
        .route("/api/artifacts/{id}", delete(api::artifacts::remove))
        // pipeline domain (§4.5)
        .route(
            "/api/pipelines",
            get(api::pipelines::list).post(api::pipelines::create),
        )
        .route("/api/pipelines/compile", post(api::pipelines::compile))
        .route("/api/pipelines/templates", get(api::pipelines::templates))
        .route(
            "/api/pipelines/{id}",
            get(api::pipelines::detail)
                .put(api::pipelines::update)
                .delete(api::pipelines::remove),
        )
        .route("/api/pipelines/{id}/publish", post(api::pipelines::publish))
        .route("/api/pipelines/{id}/deploy", post(api::pipelines::deploy))
        .route("/api/pipelines/{id}/pause", post(api::pipelines::pause))
        .route("/api/pipelines/{id}/resume", post(api::pipelines::resume))
        .route("/api/pipelines/{id}/stop", post(api::pipelines::stop))
        .route("/api/pipelines/{id}/recover", post(api::pipelines::recover))
        .route("/api/pipelines/{id}/stats", get(api::pipelines::stats))
        .route("/api/pipelines/{id}/offsets", get(api::pipelines::offsets))
        .route("/api/pipelines/{id}/logs", get(api::pipelines::logs))
        .route(
            "/api/pipelines/{id}/metrics/trend",
            get(api::pipelines::metrics_trend),
        )
        .layer(from_fn_with_state(
            state.clone(),
            require_session::<AppState>,
        ))
        // §4.0: uploads beyond 512MB are 413 (multipart needs a raised body
        // limit; the JSON default of axum stays for everything else).
        .layer(DefaultBodyLimit::max(512 * 1024 * 1024));

    // m2m endpoints — bootstrap/node token in the JSON body, no session
    // (§4.0). Register admits unknown node_keys with the global bootstrap
    // token; heartbeat only already-registered nodes.
    let node_register = Router::new()
        .route("/api/nodes/register", post(api::nodes::register))
        .layer(from_fn_with_state(
            state.clone(),
            require_node_register::<AppState>,
        ));
    let node_heartbeat = Router::new()
        .route("/api/nodes/heartbeat", post(api::nodes::heartbeat))
        .layer(from_fn_with_state(
            state.clone(),
            require_node_heartbeat::<AppState>,
        ));

    Router::new()
        .route("/healthz", get(api::health::healthz))
        // A6 (§7.4): Prometheus scrape — OPEN (exporter convention, like
        // /healthz): console-local db-write gauges + per-node /metrics
        // proxied and re-labelled with node="<name>".
        .route("/metrics", get(metrics::scrape))
        .route("/api/auth/login", post(api::auth::login))
        .merge(session_scoped)
        .merge(node_register)
        .merge(node_heartbeat)
        // E4 (§7.5): MCP JSON-RPC endpoint — OPEN like /metrics (MCP
        // convention: no console session; write tools gated by the process
        // switch + confirm:true, every call audited as mcp:<client>).
        .merge(mcp::router(mcp_state))
        .merge(
            SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", openapi::ApiDoc::openapi()),
        )
        .fallback(spa::fallback)
        .with_state(state)
}
