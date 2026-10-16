//! tailrace-catalog: CatalogProvider backends — the single entry point for
//! table objects and connection resolution.
//!
//! FileCatalog (standalone): one toml declaring datasources + table objects +
//! pipeline statements; **credentials never enter the file** (`*_env` names an
//! environment variable, read at the moment of use).
//! PlanCatalog (serve): table object metadata embedded in the plan body pushed
//! by the console.
//! ControlPlaneCatalog (managed): reads the console datasource table — a
//! skeleton until the credential-ticket contract lands; the trait is in place.

/// Plan-level pipeline configuration (options pass-through:
/// reliability/recovery/tuning).
pub mod plan_options;
pub use plan_options::{PlanOptions, RecoveryOptions, ReliabilityOptions, TuningOptions};

/// Table object, connection-side view (catalog resolution product; the caller
/// fetches credentials by env name just-in-time and discards them).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TableObject {
    /// Owning datasource name (namespace inside the catalog)
    pub datasource: String,
    /// Table object name (topic / table name)
    pub name: String,
    /// Canonical connector kind: kafka / file / doris / starrocks (see
    /// `normalize_kind` for the accepted aliases)
    #[serde(default = "default_kind")]
    pub kind: String,
    /// Decode format: ogg / json / jsonl / csv / parquet
    pub format: Option<String>,
    /// Column definitions [name, arrow type] (v1 flat; discover output is the
    /// same shape)
    pub columns: Vec<(String, String)>,
    /// kafka bootstrap (kafka-kind source/sink side)
    #[serde(default)]
    pub bootstrap: Option<String>,
    /// doris stream load URL (doris sink side; BE direct)
    #[serde(default)]
    pub stream_load_url: Option<String>,
    /// Environment variable name holding the Basic credential (value like
    /// "Basic xxx")
    #[serde(default)]
    pub auth_env: Option<String>,
    /// doris sink label prefix (idempotency anchor)
    #[serde(default)]
    pub label_prefix: Option<String>,
    /// Doris UNIQUE KEY sequence column name (out-of-order dedup basis;
    /// absent = not declared)
    #[serde(default)]
    pub sequence_column: Option<String>,
}

fn default_kind() -> String {
    "kafka".into()
}

// ==================== stream_load_url host allowlist (upgrade plan §二 engine H1) ====================

/// Hosts allowed by the default stream-load policy: loopback only. A plan
/// body cannot point a Doris stream load (and its Basic credential) at an
/// arbitrary remote host; LAN deployments must raise the allowlist
/// explicitly (`--stream-load-hosts` / `TAILRACE_STREAM_LOAD_HOSTS`).
pub fn default_stream_load_hosts() -> Vec<String> {
    ["localhost", "127.0.0.1", "::1", "0.0.0.0"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// Parse `host[:port]` out of an absolute http(s) URL (no external dep).
/// Returns None for anything that is not a well-formed http(s) URL.
pub fn url_host(url: &str) -> Option<String> {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))?;
    // cut path / query / fragment
    let authority = rest.split(['/', '?', '#']).next()?;
    // strip userinfo (user:pass@)
    let authority = authority
        .rsplit_once('@')
        .map(|(_, h)| h)
        .unwrap_or(authority);
    let host = if let Some(rest) = authority.strip_prefix('[') {
        // IPv6 literal [::1]:80
        rest.split(']').next()?
    } else {
        // strip :port only when the suffix is all digits
        match authority.rsplit_once(':') {
            Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => h,
            _ => authority,
        }
    };
    let host = host.trim().trim_end_matches('.').to_lowercase();
    (!host.is_empty()).then_some(host)
}

/// Allowlist verdict for one stream_load_url. `None` (allowlist not
/// configured) means the loopback default.
pub fn stream_load_url_allowed(
    url: &str,
    configured: Option<&std::collections::HashSet<String>>,
) -> bool {
    let Some(host) = url_host(url) else {
        return false; // unparsable / non-http scheme → reject
    };
    match configured {
        Some(set) => set.contains(&host),
        None => default_stream_load_hosts().contains(&host),
    }
}

/// Canonical connector kind (the ConnectorRegistry key). Legacy aliases fold
/// in: "topic"→"kafka", "doris_table"/"JDBC_TABLE"→"doris". Unknown kinds pass
/// through lowercased (the registry reports "unsupported kind" at build time).
pub fn normalize_kind(kind: &str) -> String {
    match kind.to_uppercase().as_str() {
        "TOPIC" | "KAFKA" => "kafka".into(),
        "FILE" => "file".into(),
        "DORIS_TABLE" | "JDBC_TABLE" | "DORIS" => "doris".into(),
        "STARROCKS" => "starrocks".into(),
        other => other.to_lowercase(),
    }
}

/// Pipeline statement (declarative form; the SQL compiler emits the same
/// shape).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StatementSpec {
    /// Source table object (datasource.name)
    pub source: String,
    /// Consumer group (cursor group convention `tr.p{pipeline}.s{n}`; with a
    /// plan-configured groupPrefix it is `{prefix}.s{n}`)
    pub group: String,
    /// Target table object
    pub sink: String,
    /// Decode-time early-filter whitelist (op_type)
    #[serde(default)]
    pub pushdown: Vec<String>,
    /// Transform (datafusion SQL, src = the decoded batch's registered name)
    pub select_sql: String,
    /// Consume start (earliest/latest; None=earliest. Only effective when no
    /// committed offset exists)
    #[serde(default)]
    pub start_offset: Option<String>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PipelineSpec {
    pub pipeline: PipelineMeta,
    pub datasource: Vec<DatasourceSpec>,
    pub table: Vec<TableSpec>,
    #[serde(default)]
    pub statement: Vec<StatementSpec>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PipelineMeta {
    pub id: u64,
    pub name: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct DatasourceSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(default)]
    pub bootstrap: Option<String>,
    #[serde(default)]
    pub stream_load_url: Option<String>,
    #[serde(default)]
    pub auth_env: Option<String>,
}

/// Table declaration (datasource-level connection params can be overridden at
/// the table level).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TableSpec {
    pub datasource: String,
    pub name: String,
    #[serde(default = "default_kind")]
    pub kind: String,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub columns: Vec<(String, String)>,
    #[serde(default)]
    pub bootstrap: Option<String>,
    #[serde(default)]
    pub stream_load_url: Option<String>,
    #[serde(default)]
    pub auth_env: Option<String>,
    #[serde(default)]
    pub label_prefix: Option<String>,
}

/// Catalog backend trait (all backends the same shape — the golden-set parity
/// anchor).
pub trait CatalogProvider: Send + Sync {
    /// Resolve a reference ("datasource.table") → table object.
    fn resolve(&self, reference: &str) -> anyhow::Result<TableObject>;
    /// Pipeline statements (standalone=declared in the file; managed=pushed
    /// by the console).
    fn statements(&self) -> anyhow::Result<Vec<StatementSpec>>;
    fn pipeline_id(&self) -> u64;
}

/// File catalog (standalone): full toml declaration.
pub struct FileCatalog {
    spec: PipelineSpec,
}

impl FileCatalog {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let spec: PipelineSpec = toml::from_str(&text)?;
        Ok(Self { spec })
    }
}

impl CatalogProvider for FileCatalog {
    fn resolve(&self, reference: &str) -> anyhow::Result<TableObject> {
        let (ds, name) = reference
            .split_once('.')
            .ok_or_else(|| anyhow::anyhow!("reference must be datasource.table: {reference}"))?;
        let ds_spec = self
            .spec
            .datasource
            .iter()
            .find(|d| d.name == ds)
            .ok_or_else(|| anyhow::anyhow!("datasource not found: {ds}"))?;
        let tbl = self
            .spec
            .table
            .iter()
            .find(|t| t.datasource == ds && t.name == name)
            .ok_or_else(|| anyhow::anyhow!("table object not found: {reference}"))?;
        Ok(TableObject {
            datasource: ds.into(),
            name: name.into(),
            kind: normalize_kind(&tbl.kind),
            format: tbl.format.clone(),
            columns: tbl.columns.clone(),
            bootstrap: tbl.bootstrap.clone().or_else(|| ds_spec.bootstrap.clone()),
            stream_load_url: tbl
                .stream_load_url
                .clone()
                .or_else(|| ds_spec.stream_load_url.clone()),
            auth_env: tbl.auth_env.clone().or_else(|| ds_spec.auth_env.clone()),
            label_prefix: tbl
                .label_prefix
                .clone()
                .map(|p| format!("{p}-{}", self.spec.pipeline.id))
                .clone(),
            sequence_column: None,
        })
    }

    fn statements(&self) -> anyhow::Result<Vec<StatementSpec>> {
        Ok(self.spec.statement.clone())
    }

    fn pipeline_id(&self) -> u64 {
        self.spec.pipeline.id
    }
}

/// Fetch a credential from the environment (just-in-time: the caller should
/// use and drop it immediately).
pub fn credential_from_env(env_name: &str) -> anyhow::Result<String> {
    // Only TAILRACE_-prefixed names: a plan body could otherwise point at an
    // arbitrary environment variable (e.g. PATH) and exfiltrate non-credential
    // values.
    if !env_name.starts_with("TAILRACE_") {
        anyhow::bail!(
            "auth_env only allows the TAILRACE_ prefix (got: {})",
            env_name
        );
    }
    std::env::var(env_name)
        .map_err(|_| anyhow::anyhow!("environment variable {env_name} not set (credentials never enter files; inject before running)"))
}

/// Console catalog (managed skeleton): reads the console datasource table —
/// the credential-ticket contract gets wired in later.
pub struct ControlPlaneCatalog {
    pub base_url: String,
    pub token: String,
}

impl ControlPlaneCatalog {
    /// base_url points at the console API namespace (e.g.
    /// http://console:8080/api); token = the session Cookie header.
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            token: token.into(),
        }
    }

    fn fetch_tables(&self) -> anyhow::Result<Vec<CpTable>> {
        let url = format!("{}/datasource/tables", self.base_url);
        let body = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(30))
            .build()?
            .get(&url)
            .header("Cookie", &self.token)
            .send()?
            .error_for_status()?
            .text()?;
        Ok(serde_json::from_str::<CpTablesResp>(&body)?.tables)
    }
}

#[derive(serde::Deserialize)]
struct CpTablesResp {
    tables: Vec<CpTable>,
}

#[derive(serde::Deserialize)]
struct CpTable {
    id: i64,
    #[serde(rename = "datasourceId")]
    datasource_id: i64,
    #[serde(rename = "dbName")]
    db_name: String,
    #[serde(rename = "tableName")]
    table_name: String,
    #[serde(rename = "tableKind")]
    table_kind: String,
    format: Option<String>,
    #[serde(rename = "arrowSchema", default)]
    arrow_schema: Option<String>,
}

impl CatalogProvider for ControlPlaneCatalog {
    fn resolve(&self, reference: &str) -> anyhow::Result<TableObject> {
        let (ds_id, rest) = reference.split_once('.').ok_or_else(|| {
            anyhow::anyhow!("managed reference must be {{datasourceId}}.{{table}}: {reference}")
        })?;
        let ds_id: i64 = ds_id
            .parse()
            .map_err(|_| anyhow::anyhow!("datasource id must be numeric: {ds_id}"))?;
        let (db, tbl) = rest.split_once('.').unwrap_or(("default", rest));
        let hit = self
            .fetch_tables()?
            .into_iter()
            .find(|t| t.datasource_id == ds_id && t.db_name == db && t.table_name == tbl)
            .ok_or_else(|| {
                anyhow::anyhow!("table object not in the console catalog: {reference}")
            })?;
        // The list endpoint does not return the big arrow_schema field — fetch
        // the detail individually (same rule as the console UI screens)
        let detail_url = format!("{}/datasource/tables/{}", self.base_url, hit.id);
        let detail = reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(30))
            .build()?
            .get(&detail_url)
            .header("Cookie", &self.token)
            .send()?
            .error_for_status()?
            .text()?;
        let arrow = serde_json::from_str::<serde_json::Value>(&detail)
            .ok()
            .and_then(|v| v.get("table").cloned())
            .and_then(|t| {
                t.get("arrowSchema")
                    .and_then(|a| a.as_str())
                    .map(|s| s.to_string())
            });
        Ok(TableObject {
            datasource: ds_id.to_string(),
            name: hit.table_name,
            kind: normalize_kind(&hit.table_kind),
            format: hit.format,
            columns: parse_arrow_columns(arrow.as_deref().or(hit.arrow_schema.as_deref())),
            bootstrap: None,
            stream_load_url: None,
            auth_env: None,
            label_prefix: None,
            sequence_column: None,
        })
    }
    fn statements(&self) -> anyhow::Result<Vec<StatementSpec>> {
        anyhow::bail!("managed statements are pushed via console PUT /v1/pipelines/{{id}}/plan (not pulled from a catalog)")
    }
    fn pipeline_id(&self) -> u64 {
        0
    }
}

fn parse_arrow_columns(arrow_json: Option<&str>) -> Vec<(String, String)> {
    let Some(json) = arrow_json else {
        return Vec::new();
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(json) else {
        return Vec::new();
    };
    v.get("fields")
        .and_then(|f| f.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|f| {
                    let name = f.get("name")?.as_str()?.to_string();
                    let ty = f
                        .get("type")
                        .and_then(|t| t.get("name"))
                        .and_then(|n| n.as_str())
                        .unwrap_or("utf8")
                        .to_string();
                    Some((name, ty))
                })
                .collect()
        })
        .unwrap_or_default()
}

// ==================== PlanCatalog (serve mode — the console-pushed plan body's catalog backend) ====================

/// Table object metadata embedded in a plan body (resolved by the console's
/// pushPlan from its datasource table directory and carried with the plan;
/// connection params and the credential env name pass through verbatim —
/// credential values never enter the plan body).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PlanTableObject {
    /// Reference key ("datasource.table", the same namespace as
    /// StatementSpec.source/sink)
    pub reference: String,
    pub kind: String,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub columns: Vec<(String, String)>,
    #[serde(default)]
    pub bootstrap: Option<String>,
    #[serde(default)]
    pub stream_load_url: Option<String>,
    #[serde(default)]
    pub auth_env: Option<String>,
    /// Doris UNIQUE KEY sequence column name (filled by the console from
    /// table metadata)
    #[serde(default)]
    pub sequence_column: Option<String>,
}

/// Plan catalog backend: statements + the table object list (HashMap indexed,
/// resolve = O(1)).
pub struct PlanCatalog {
    pipeline_id: u64,
    statements: Vec<StatementSpec>,
    tables: std::collections::HashMap<String, PlanTableObject>,
}

impl PlanCatalog {
    pub fn new(
        pipeline_id: u64,
        statements: Vec<StatementSpec>,
        tables: Vec<PlanTableObject>,
    ) -> Self {
        Self {
            pipeline_id,
            statements,
            tables: tables
                .into_iter()
                .map(|t| (t.reference.clone(), t))
                .collect(),
        }
    }

    /// Missing table object metadata for referenced tables (pre-deploy
    /// preflight — a missing table rejects the whole plan, no half-assembled
    /// pipeline starts).
    pub fn missing_tables(&self) -> Vec<String> {
        let mut refs = std::collections::BTreeSet::new();
        for s in &self.statements {
            refs.insert(&s.source);
            refs.insert(&s.sink);
        }
        refs.iter()
            .filter(|r| !self.tables.contains_key(r.as_str()))
            .map(|r| r.to_string())
            .collect()
    }
}

impl CatalogProvider for PlanCatalog {
    fn resolve(&self, reference: &str) -> anyhow::Result<TableObject> {
        let t = self
            .tables
            .get(reference)
            .ok_or_else(|| anyhow::anyhow!("plan table object missing: {reference} (the console did not send its metadata with the plan)"))?;
        // three-part reference (dsId.db.table) — rsplit takes the last part as
        // the table name, the middle as the database (kafka topic = doris
        // table name)
        let (_db, table) = reference
            .rsplit_once('.')
            .map(|(prefix, tbl)| {
                let db = prefix
                    .rsplit_once('.')
                    .map(|(_, d)| d.to_string())
                    .unwrap_or_default();
                (db, tbl.to_string())
            })
            .unwrap_or((String::new(), reference.to_string()));
        let ds = reference
            .split_once('.')
            .map(|(d, _)| d.to_string())
            .unwrap_or_default();
        Ok(TableObject {
            datasource: ds,
            name: table,
            kind: normalize_kind(&t.kind),
            format: t.format.clone(),
            columns: t.columns.clone(),
            bootstrap: t.bootstrap.clone(),
            stream_load_url: t.stream_load_url.clone(),
            auth_env: t.auth_env.clone(),
            label_prefix: Some(format!("p{}", self.pipeline_id)),
            sequence_column: t.sequence_column.clone(),
        })
    }

    fn statements(&self) -> anyhow::Result<Vec<StatementSpec>> {
        Ok(self.statements.clone())
    }

    fn pipeline_id(&self) -> u64 {
        self.pipeline_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_catalog_resolves_and_statements() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("tailrace-cat-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let f = dir.join("p.toml");
        std::fs::write(
            &f,
            r#"
[pipeline]
id = 42
name = "demo"

[[datasource]]
name = "k"
type = "kafka"
bootstrap = "localhost:9092"

[[table]]
datasource = "k"
name = "t1"
columns = [["op_type", "utf8"], ["seq", "int64"]]

[[statement]]
source = "k.t1"
group = "tr.p42.s0"
sink = "k.t1"
select_sql = "SELECT * FROM src"
"#,
        )?;
        let cat = FileCatalog::load(&f)?;
        assert_eq!(cat.pipeline_id(), 42);
        let t = cat.resolve("k.t1")?;
        assert_eq!(t.kind, "kafka", "default kind normalized topic→kafka");
        assert_eq!(t.bootstrap.as_deref(), Some("localhost:9092"));
        assert_eq!(cat.statements()?.len(), 1);
        assert!(cat.resolve("k.none").is_err());
        assert!(cat.resolve("bad-ref").is_err());
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn kind_aliases_normalize_to_registry_keys() {
        assert_eq!(normalize_kind("topic"), "kafka");
        assert_eq!(normalize_kind("TOPIC"), "kafka");
        assert_eq!(normalize_kind("kafka"), "kafka");
        assert_eq!(normalize_kind("file"), "file");
        assert_eq!(normalize_kind("doris_table"), "doris");
        assert_eq!(normalize_kind("JDBC_TABLE"), "doris");
        assert_eq!(normalize_kind("starrocks"), "starrocks");
        assert_eq!(
            normalize_kind("mqtt"),
            "mqtt",
            "unknown kinds pass through lowercased"
        );
    }

    #[test]
    fn credential_env_only_allows_tailrace_prefix() {
        assert!(
            credential_from_env("PATH").is_err(),
            "arbitrary env names rejected"
        );
        let err = credential_from_env("SOMETHING_ELSE")
            .unwrap_err()
            .to_string();
        assert!(err.contains("TAILRACE_"), "{err}");
        // TAILRACE_-prefixed but unset: distinct error, still Err
        assert!(credential_from_env("TAILRACE_UNSET_XYZ").is_err());
    }

    /// engine H1: stream_load_url host allowlist — the plan body cannot
    /// point a stream load (and its Basic credential) anywhere it likes.
    #[test]
    fn stream_load_host_allowlist() {
        use std::collections::HashSet;
        // host parsing
        assert_eq!(
            url_host("http://be:8040/api/db/t/_stream_load").as_deref(),
            Some("be")
        );
        assert_eq!(
            url_host("https://FE1.Example.COM/").as_deref(),
            Some("fe1.example.com")
        );
        assert_eq!(url_host("http://[::1]:8040/x").as_deref(), Some("::1"));
        assert_eq!(
            url_host("http://user:pass@be:8040/x").as_deref(),
            Some("be"),
            "userinfo never counts as the host"
        );
        assert_eq!(url_host("http://be.").as_deref(), Some("be"));
        assert_eq!(url_host("file:///etc/passwd"), None);
        assert_eq!(url_host("not a url"), None);
        // default policy: loopback only
        assert!(stream_load_url_allowed(
            "http://127.0.0.1:8040/api/_stream_load",
            None
        ));
        assert!(stream_load_url_allowed(
            "http://localhost:8040/api/_stream_load",
            None
        ));
        assert!(stream_load_url_allowed(
            "http://[::1]:8040/api/_stream_load",
            None
        ));
        assert!(
            !stream_load_url_allowed("http://be.internal:8040/api/_stream_load", None),
            "remote hosts rejected under the default loopback-only policy"
        );
        assert!(!stream_load_url_allowed(
            "http://169.254.169.254/latest/meta",
            None
        ));
        // configured policy: exactly the listed hosts (loopback no longer
        // implied)
        let cfg: HashSet<String> = ["be.internal", "be2.internal"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(stream_load_url_allowed(
            "http://be.internal:8040/api/_stream_load",
            Some(&cfg)
        ));
        assert!(!stream_load_url_allowed(
            "http://127.0.0.1:8040/x",
            Some(&cfg)
        ));
        assert!(!stream_load_url_allowed(
            "http://be3.internal:8040/x",
            Some(&cfg)
        ));
    }
}
