//! Row types for the console metadata store (design doc §5.2).

#[derive(Debug, Clone)]
pub struct User {
    pub id: i64,
    pub username: String,
    pub password_hash: String,
    pub display_name: Option<String>,
    pub role: String,
    pub disabled: bool,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct Datasource {
    pub id: i64,
    pub name: String,
    pub ds_type: String,
    pub conn_json: String,
    pub status: String,
    pub generation: i64,
    pub created_at: String,
    pub updated_at: String,
}

/// Encrypted credential halves for a datasource (§5.3: AES-256-GCM, nonce + ciphertext).
#[derive(Debug, Clone)]
pub struct DatasourceCredential {
    pub nonce: String,
    pub ciphertext: String,
}

#[derive(Debug, Clone)]
pub struct TableObject {
    pub id: i64,
    pub datasource_id: i64,
    pub db_name: String,
    pub table_name: String,
    pub kind: String,
    pub format: Option<String>,
    pub cursor_semantics: String,
    pub source: String,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: i64,
    pub node_key: String,
    pub name: String,
    pub api_url: Option<String>,
    pub status: String,
    pub host: Option<String>,
    pub ssh_port: Option<i64>,
    pub ssh_user: Option<String>,
    pub engine_port: i64,
    pub install_dir: String,
    pub artifact_id: Option<i64>,
    pub engine_version: Option<String>,
    pub deploy_status: Option<String>,
    pub register_mode: String,
    pub capacity_json: Option<String>,
    /// sha256 hex digest of the per-node token (audit H1: plaintext never at
    /// rest; NULL = revoked).
    pub bootstrap_token: Option<String>,
    pub last_heartbeat: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct Pipeline {
    pub id: i64,
    pub name: String,
    pub sql_content: String,
    pub spec_json: Option<String>,
    pub ds_mapping_json: Option<String>,
    pub status: String,
    pub assigned_node: Option<i64>,
    pub preferred_node: Option<i64>,
    pub version: i64,
    pub checkpoint_ms: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone)]
pub struct PipelineVersion {
    pub id: i64,
    pub pipeline_id: i64,
    pub version: i64,
    pub published_by: String,
}

/// One row for the `node_event` table (engine heartbeat events, §4.3).
#[derive(Debug, Clone)]
pub struct NewNodeEvent {
    pub node_key: String,
    pub pipeline_id: Option<i64>,
    pub level: String,
    pub kind: String,
    pub message: String,
    /// Unix seconds; `None` falls back to "now" at insert time.
    pub ts_unix: Option<i64>,
}

/// One row for `node_metrics` (heartbeat `resources`, §4.3).
#[derive(Debug, Clone, Default)]
pub struct NewNodeMetrics {
    pub rss_bytes: Option<i64>,
    pub mem_total_bytes: Option<i64>,
    pub cpu_pct: Option<f64>,
    pub rx_bps: Option<f64>,
    pub tx_bps: Option<f64>,
}

/// One row for `pipeline_metrics` (heartbeat `pipelineStates[].totals`, §4.3).
#[derive(Debug, Clone)]
pub struct NewPipelineMetrics {
    pub pipeline_id: i64,
    pub status: Option<String>,
    pub consumed: i64,
    pub flushed: i64,
    pub flushes: i64,
}

#[derive(Debug, Clone)]
pub struct Artifact {
    pub id: i64,
    pub name: String,
    pub version: String,
    pub filename: String,
    pub sha256: String,
    pub size_bytes: i64,
    pub uploaded_by: String,
    pub created_at: String,
    /// Joined count of nodes with `artifact_id` pointing here (§4.4).
    pub ref_count: i64,
}

/// A `node_event` row as served by the events endpoints (§4.3).
#[derive(Debug, Clone)]
pub struct NodeEventRow {
    pub ts: String,
    pub node_key: String,
    pub pipeline_id: Option<i64>,
    pub level: String,
    pub kind: String,
    pub message: String,
}

/// A 1-minute aggregate point of `node_metrics` (§4.3 metrics endpoint).
#[derive(Debug, Clone)]
pub struct NodeMetricPoint {
    pub ts: String,
    pub rss_bytes: Option<i64>,
    pub mem_total_bytes: Option<i64>,
    pub cpu_pct: Option<f64>,
    pub rx_bps: Option<f64>,
    pub tx_bps: Option<f64>,
}

/// Insert payload of [`Store::insert_deployed_node`] (§4.4 hosts[] batch).
#[derive(Debug)]
pub struct NewDeployedNode<'a> {
    pub node_key: &'a str,
    pub name: &'a str,
    pub host: &'a str,
    pub ssh_port: i64,
    pub ssh_user: &'a str,
    pub engine_port: i64,
    pub install_dir: &'a str,
    /// sha256 digest of the per-node token (plaintext never stored).
    pub bootstrap_token: &'a str,
}

/// A `deploy_log` row (§4.4 deploy-log endpoint).
#[derive(Debug, Clone)]
pub struct DeployLogRow {
    pub step: String,
    pub status: String,
    pub output: Option<String>,
    pub operator: Option<String>,
    pub ts: String,
}

/// A 5-minute aggregate point of `pipeline_metrics` (§4.5 trend endpoint).
#[derive(Debug, Clone)]
pub struct PipelineTrendPoint {
    pub ts: String,
    pub consumed: i64,
    pub flushed: i64,
}

#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub actor: String,
    pub action: String,
    pub entity_type: String,
    pub entity_id: Option<String>,
    pub detail_json: Option<String>,
}
