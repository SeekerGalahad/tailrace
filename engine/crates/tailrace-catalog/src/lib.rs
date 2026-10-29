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

/// G2 (R8, blueprint §7.1): the 16-word column type vocabulary for
/// `TableObject.columns` type strings. Canonical home is tailrace-ir (the
/// wire-contract crate — this crate already depends on it, and the reverse
/// placement would be a dependency cycle); re-exported here so the stable
/// public path is `tailrace_catalog::ColumnType::parse` (blueprint G2:
/// "arrow_type 迁至 tailrace-catalog::ColumnType::parse"). The two legacy
/// `arrow_type` sites (runtime + ir) map through
/// [`arrow_type_or_utf8`], which folds unknown words to Utf8 behind a
/// one-minor-version warn window (const in tailrace-ir) instead of erroring
/// on the spot.
pub use tailrace_ir::column_type::{arrow_type_or_utf8, ColumnType};

/// Option key canon (R8 G1): the names of the legacy top-level fields that
/// moved into `TableObject::options`. The catalog owns the SPELLING only
/// (one constant set, so the three consumers of `sequence_column` — jdbc
/// cursor, dim pagination key, doris dedup basis — cannot drift apart);
/// each connector owns the SEMANTICS through its own typed accessor
/// (`KafkaTableExt`, `DorisTableExt`, …).
pub mod option_keys {
    /// kafka broker list / file directory / jdbc URL — the per-kind
    /// "where to connect" string (kafka + file + dim/jdbc kinds)
    pub const BOOTSTRAP: &str = "bootstrap";
    /// doris stream load endpoint (BE direct, doris/starrocks sinks)
    pub const STREAM_LOAD_URL: &str = "stream_load_url";
    /// environment variable name holding the Basic credential
    pub const AUTH_ENV: &str = "auth_env";
    /// doris sink label prefix (idempotency anchor)
    pub const LABEL_PREFIX: &str = "label_prefix";
    /// key column: dim = full-table load pagination key, jdbc = incremental
    /// cursor column, doris = UNIQUE KEY out-of-order dedup basis
    pub const SEQUENCE_COLUMN: &str = "sequence_column";
    /// doris partial-column update subset (A7)
    pub const PARTIAL_COLUMNS: &str = "partial_columns";

    /// Every key that serializes dual-written (options + legacy top-level)
    /// during the one-minor-version serde window — the write side of the
    /// G1 compatibility contract (read side = the `Raw` fold below).
    pub const LEGACY: [&str; 6] = [
        BOOTSTRAP,
        STREAM_LOAD_URL,
        AUTH_ENV,
        LABEL_PREFIX,
        SEQUENCE_COLUMN,
        PARTIAL_COLUMNS,
    ];
}

/// Connector-specific table options (R8 G1): everything a connector needs
/// beyond the five universal fields. Key spelling canon =
/// [`option_keys`]; arbitrary extra keys ride along verbatim (the console
/// passes datasource connJson keys through), typed reads go through
/// connector-side accessors.
pub type TableOptions = std::collections::BTreeMap<String, serde_json::Value>;

/// Table object, connection-side view (catalog resolution product; the caller
/// fetches credentials by env name just-in-time and discards them).
///
/// Construction goes through [`TableObject::new`] (the single construction
/// site — R8 G1 collapsed the per-callsite struct literals); the five
/// universal fields stay typed, everything connector-specific rides
/// `options`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(from = "TableObjectRaw")]
pub struct TableObject {
    /// Owning datasource name (namespace inside the catalog)
    pub datasource: String,
    /// Table object name (topic / table name)
    pub name: String,
    /// Canonical connector kind: kafka / file / jdbc / doris / starrocks (see
    /// `normalize_kind` for the accepted aliases; jdbc is source-only,
    /// doris/starrocks are sink-side)
    pub kind: String,
    /// Decode format: ogg / json / jsonl / csv / parquet / debezium-json
    /// (debezium-json = Debezium CDC envelope consumed from Kafka, blueprint
    /// C narrow path; `after.*` is the row data, deletes/tombstones skip,
    /// schema/payload-wrapped and bare converter envelopes both accepted.
    /// The runtime dispatch arm HAS landed (`build_decoder` decodes it), so
    /// hand-declared table objects can select it; the console discover path
    /// still rejects the value (kafka infer-schema accepts ogg/json/jsonl
    /// only — a debezium-json table cannot be discovered, only written into
    /// a catalog by hand or via table registration, which does not validate
    /// format names)
    pub format: Option<String>,
    /// Column definitions [name, arrow type] (v1 flat; discover output is the
    /// same shape)
    pub columns: Vec<(String, String)>,
    /// Connector-specific options (R8 G1). Key canon: [`option_keys`]; the
    /// serde window folds the legacy top-level field names into this map on
    /// read (explicit `options` keys win over the legacy spelling).
    #[serde(default)]
    pub options: TableOptions,
}

impl TableObject {
    /// The single construction site (R8 G1): five universal fields +
    /// connector options through the `with_*` builders. Former struct
    /// literals at 17+ callsites collapsed here; `grep "TableObject {"`
    /// finds only this factory body and the struct definition.
    pub fn new(
        datasource: impl Into<String>,
        name: impl Into<String>,
        kind: impl Into<String>,
    ) -> Self {
        Self {
            datasource: datasource.into(),
            name: name.into(),
            kind: kind.into(),
            format: None,
            columns: Vec::new(),
            options: TableOptions::new(),
        }
    }
}

/// Shared options API of [`TableObject`] / [`PlanTableObject`] (same
/// builders + typed read primitives on both — the runtime reads plan tables,
/// the connectors read resolved objects; one spelling).
macro_rules! impl_table_options_api {
    ($ty:ty) => {
        impl $ty {
            /// Set the decode format (builder).
            pub fn with_format(mut self, format: impl Into<String>) -> Self {
                self.format = Some(format.into());
                self
            }

            /// Set (or clear) the decode format from an `Option` (builder).
            pub fn format_opt(mut self, format: Option<String>) -> Self {
                self.format = format;
                self
            }

            /// Set the column list (builder).
            pub fn with_columns(mut self, columns: Vec<(String, String)>) -> Self {
                self.columns = columns;
                self
            }

            /// Replace the whole options map (builder).
            pub fn with_options(mut self, options: TableOptions) -> Self {
                self.options = options;
                self
            }

            /// Set one connector option (builder); `value` anything
            /// JSON-shaped (`"str"`, `json!(["k","a"])`, …).
            pub fn with_option(mut self, key: &str, value: impl Into<serde_json::Value>) -> Self {
                self.options.insert(key.to_string(), value.into());
                self
            }

            /// Raw option value (generic primitive; connectors prefer their
            /// own typed accessors over this).
            pub fn option(&self, key: &str) -> Option<&serde_json::Value> {
                self.options.get(key)
            }

            /// String option read (the shared primitive behind the
            /// connector-side typed accessors).
            pub fn option_str(&self, key: &str) -> Option<&str> {
                self.options.get(key).and_then(serde_json::Value::as_str)
            }

            /// String-list option read (`partial_columns` shape).
            pub fn option_str_list(&self, key: &str) -> Option<Vec<String>> {
                self.options.get(key).and_then(|v| {
                    v.as_array().map(|a| {
                        a.iter()
                            .filter_map(|x| x.as_str().map(str::to_string))
                            .collect()
                    })
                })
            }
        }
    };
}

impl_table_options_api!(TableObject);

/// Deserialize mirror of [`TableObject`] accepting BOTH shapes — the G1
/// serde window's read side: the legacy top-level field names (`bootstrap`,
/// `stream_load_url`, …) fold into `options` (an explicit `options` key wins
/// over the legacy spelling of the same field); `null` legacy values mean
/// "unset" exactly like the pre-G1 `Option::None`. Works for every
/// self-describing format the catalog reads (TOML specs, JSON plan bodies).
#[derive(serde::Deserialize)]
pub(crate) struct TableObjectRaw {
    datasource: String,
    name: String,
    #[serde(default = "default_kind")]
    kind: String,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    columns: Vec<(String, String)>,
    #[serde(default)]
    options: TableOptions,
    // ---- legacy top-level fields (folded into options on conversion) ----
    #[serde(default)]
    bootstrap: Option<String>,
    #[serde(default)]
    stream_load_url: Option<String>,
    #[serde(default)]
    auth_env: Option<String>,
    #[serde(default)]
    label_prefix: Option<String>,
    #[serde(default)]
    sequence_column: Option<String>,
    #[serde(default)]
    partial_columns: Option<Vec<String>>,
}

/// Fold one legacy `Option<T>` field into `options` unless the map already
/// carries the key (explicit `options` spelling wins).
fn fold_legacy(
    options: &mut TableOptions,
    key: &str,
    legacy: Option<impl Into<serde_json::Value>>,
) {
    if let Some(v) = legacy {
        options.entry(key.to_string()).or_insert_with(|| v.into());
    }
}

impl From<TableObjectRaw> for TableObject {
    fn from(raw: TableObjectRaw) -> Self {
        let TableObjectRaw {
            datasource,
            name,
            kind,
            format,
            columns,
            mut options,
            bootstrap,
            stream_load_url,
            auth_env,
            label_prefix,
            sequence_column,
            partial_columns,
        } = raw;
        fold_legacy(&mut options, option_keys::BOOTSTRAP, bootstrap);
        fold_legacy(&mut options, option_keys::STREAM_LOAD_URL, stream_load_url);
        fold_legacy(&mut options, option_keys::AUTH_ENV, auth_env);
        fold_legacy(&mut options, option_keys::LABEL_PREFIX, label_prefix);
        fold_legacy(&mut options, option_keys::SEQUENCE_COLUMN, sequence_column);
        fold_legacy(&mut options, option_keys::PARTIAL_COLUMNS, partial_columns);
        Self {
            datasource,
            name,
            kind,
            format,
            columns,
            options,
        }
    }
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
/// in: "topic"→"kafka", "doris_table"/"JDBC_TABLE"→"doris", "JDBC"→"jdbc".
/// Mind the JDBC pair (R3-W1 review low#2): **"JDBC" is the MySQL/
/// PostgreSQL batch SOURCE kind; "JDBC_TABLE" is a Doris SINK alias** (it
/// folds to "doris" — a source declared `kind = jdbc_table` resolves to a
/// doris object and trips the "source only supports kafka/file/jdbc (got
/// doris)" diagnostic). **"DIM" is the dimension-table kind (T-D1)** — a
/// JDBC-backed table loaded full-table at assembly for the broadcast join;
/// never a streaming source/sink kind. Unknown kinds pass through
/// lowercased (the registry reports "unsupported kind" at build time).
pub fn normalize_kind(kind: &str) -> String {
    match kind.to_uppercase().as_str() {
        "TOPIC" | "KAFKA" => "kafka".into(),
        "FILE" => "file".into(),
        // JDBC_TABLE = Doris sink alias (legacy) sits next to JDBC = the
        // jdbc SOURCE on purpose — they are distinct kinds, not aliases of
        // each other (see the doc comment above)
        "DORIS_TABLE" | "JDBC_TABLE" | "DORIS" => "doris".into(),
        "STARROCKS" => "starrocks".into(),
        "JDBC" => "jdbc".into(),
        // T-D1: dimension table (broadcast join side). Explicit arm for the
        // doc anchor; the lowercase fallback would produce the same value.
        "DIM" => "dim".into(),
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
    /// plan-configured groupPrefix it is `{prefix}.s{statement index}`)
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
    /// Dimension table joins (T-D1 broadcast join, blueprint D narrow path):
    /// one entry per `JOIN ${ds:dim.<datasource>.<table>} [AS] <alias>` in
    /// select_sql. Each dim table is loaded FULL-TABLE over JDBC at assembly
    /// (compile time) and registered as the static `dim_<alias>` MemTable —
    /// a broadcast join, NOT a streaming join (no watermark, no state
    /// backend; explicit row/memory caps fail the assembly).
    #[serde(default)]
    pub dims: Vec<DimJoin>,
    /// Statement-level partial-column declaration (A7): the INSERT column
    /// list — `INSERT INTO sink (k, a) SELECT …` compiles to
    /// `Some(["k","a"])`, a bare `INSERT INTO sink SELECT …` to `None`
    /// (full-row writes, the historical default). This is the compile-time
    /// record of the switch; the RUNTIME reads the declaration from the sink
    /// table object (option key `partial_columns` — R8 G1 moved it into
    /// `TableObject.options`) — the console projects
    /// this statement field onto the sink's plan table entry at deploy, and
    /// a TOML catalog declares it on the `[[table]]` directly. The two must
    /// agree, which the console guarantees by deriving one from the other.
    #[serde(default)]
    pub partial_columns: Option<Vec<String>>,
}

/// One dimension-table join extracted by the SQL compiler (T-D1).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DimJoin {
    /// Catalog reference of the dim table object (datasource.table; the
    /// object must be `kind = "dim"`)
    pub reference: String,
    /// Table name the rewritten select_sql joins against (`dim_<alias>`;
    /// alias defaults to the reference's table part when the SQL omits one)
    pub name: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PipelineSpec {
    pub pipeline: PipelineMeta,
    pub datasource: Vec<DatasourceSpec>,
    /// `[[table]]` entries deserialize straight into [`TableObject`]
    /// (R8 G1: the former 12-field `TableSpec` was field-identical to it;
    /// legacy top-level connection fields fold into `options` on read)
    pub table: Vec<TableObject>,
    #[serde(default)]
    pub statement: Vec<StatementSpec>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct PipelineMeta {
    pub id: u64,
    pub name: String,
}

/// Datasource-level connection defaults (table-level `options` keys override
/// these at resolve time). The former top-level `bootstrap` /
/// `stream_load_url` / `auth_env` fields fold into `options` on read (same
/// serde window as [`TableObject`]).
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(from = "DatasourceSpecRaw")]
pub struct DatasourceSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    #[serde(default)]
    pub options: TableOptions,
}

/// Deserialize mirror of [`DatasourceSpec`] (legacy field fold, G1 window).
#[derive(serde::Deserialize)]
struct DatasourceSpecRaw {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    #[serde(default)]
    options: TableOptions,
    #[serde(default)]
    bootstrap: Option<String>,
    #[serde(default)]
    stream_load_url: Option<String>,
    #[serde(default)]
    auth_env: Option<String>,
}

impl From<DatasourceSpecRaw> for DatasourceSpec {
    fn from(raw: DatasourceSpecRaw) -> Self {
        let DatasourceSpecRaw {
            name,
            ty,
            mut options,
            bootstrap,
            stream_load_url,
            auth_env,
        } = raw;
        fold_legacy(&mut options, option_keys::BOOTSTRAP, bootstrap);
        fold_legacy(&mut options, option_keys::STREAM_LOAD_URL, stream_load_url);
        fold_legacy(&mut options, option_keys::AUTH_ENV, auth_env);
        Self { name, ty, options }
    }
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
        // connection defaults: table-level option keys override the
        // datasource-level ones (the pre-G1 `tbl.X.or(ds.X)` fallback,
        // generalized to the whole options map — a datasource can now carry
        // any connector default, not just the three legacy fields)
        let mut options = ds_spec.options.clone();
        for (k, v) in &tbl.options {
            options.insert(k.clone(), v.clone());
        }
        // standalone label prefix convention: `{declared}-{pipeline id}`
        // (serve mode uses `p{pipeline id}` — PlanCatalog::resolve)
        if let Some(p) = tbl.option_str(option_keys::LABEL_PREFIX) {
            options.insert(
                option_keys::LABEL_PREFIX.to_string(),
                serde_json::json!(format!("{p}-{}", self.spec.pipeline.id)),
            );
        }
        Ok(TableObject::new(ds, name, normalize_kind(&tbl.kind))
            .format_opt(tbl.format.clone())
            .with_columns(tbl.columns.clone())
            .with_options(options))
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
        Ok(TableObject::new(
            ds_id.to_string(),
            hit.table_name,
            normalize_kind(&hit.table_kind),
        )
        .format_opt(hit.format)
        .with_columns(parse_arrow_columns(
            arrow.as_deref().or(hit.arrow_schema.as_deref()),
        )))
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
///
/// R8 G1 serde window, both directions:
/// - **read**: legacy top-level field names (`bootstrap`,
///   `stream_load_url`, `auth_env`, `sequence_column`, `partial_columns`)
///   fold into `options` (pre-G1 plan bodies stay deployable);
/// - **write**: known legacy keys are dual-written (options map + legacy
///   top-level spelling) so a pre-G1 engine reading a post-G1 artifact
///   still finds every field — drop the top-level spelling after one minor
///   version.
#[derive(Debug, Clone)]
pub struct PlanTableObject {
    /// Reference key ("datasource.table", the same namespace as
    /// StatementSpec.source/sink)
    pub reference: String,
    pub kind: String,
    pub format: Option<String>,
    pub columns: Vec<(String, String)>,
    /// Connector-specific options (same canon as [`TableObject::options`]).
    pub options: TableOptions,
}

impl PlanTableObject {
    /// The single construction site for plan table entries (R8 G1; the
    /// runtime/contract tests formerly spelled 19 struct literals).
    pub fn new(reference: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            reference: reference.into(),
            kind: kind.into(),
            format: None,
            columns: Vec::new(),
            options: TableOptions::new(),
        }
    }
}

impl_table_options_api!(PlanTableObject);

/// Deserialize mirror of [`PlanTableObject`] (legacy field fold — the G1
/// window's read side; `null` legacy values mean "unset").
#[derive(serde::Deserialize)]
struct PlanTableObjectRaw {
    reference: String,
    kind: String,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    columns: Vec<(String, String)>,
    #[serde(default)]
    options: TableOptions,
    // ---- legacy top-level fields (folded into options on conversion) ----
    #[serde(default)]
    bootstrap: Option<String>,
    #[serde(default)]
    stream_load_url: Option<String>,
    #[serde(default)]
    auth_env: Option<String>,
    #[serde(default)]
    sequence_column: Option<String>,
    #[serde(default)]
    partial_columns: Option<Vec<String>>,
}

impl From<PlanTableObjectRaw> for PlanTableObject {
    fn from(raw: PlanTableObjectRaw) -> Self {
        let PlanTableObjectRaw {
            reference,
            kind,
            format,
            columns,
            mut options,
            bootstrap,
            stream_load_url,
            auth_env,
            sequence_column,
            partial_columns,
        } = raw;
        fold_legacy(&mut options, option_keys::BOOTSTRAP, bootstrap);
        fold_legacy(&mut options, option_keys::STREAM_LOAD_URL, stream_load_url);
        fold_legacy(&mut options, option_keys::AUTH_ENV, auth_env);
        fold_legacy(&mut options, option_keys::SEQUENCE_COLUMN, sequence_column);
        fold_legacy(&mut options, option_keys::PARTIAL_COLUMNS, partial_columns);
        Self {
            reference,
            kind,
            format,
            columns,
            options,
        }
    }
}

impl<'de> serde::Deserialize<'de> for PlanTableObject {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        PlanTableObjectRaw::deserialize(d).map(Self::from)
    }
}

impl serde::Serialize for PlanTableObject {
    /// Dual-write serialization (the G1 window's write side): the whole
    /// `options` map under the `options` key + each known legacy key ALSO
    /// at its pre-G1 top-level spelling. A pre-G1 engine ignores the
    /// `options` key it does not know and reads the top-level fields; a
    /// post-G1 engine reads `options` (the fold prefers it).
    /// `format`/`columns` stay top-level in both eras.
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let mut map = s.serialize_map(None)?;
        map.serialize_entry("reference", &self.reference)?;
        map.serialize_entry("kind", &self.kind)?;
        if let Some(f) = &self.format {
            map.serialize_entry("format", f)?;
        }
        map.serialize_entry("columns", &self.columns)?;
        map.serialize_entry("options", &self.options)?;
        for key in option_keys::LEGACY {
            if let Some(v) = self.options.get(key) {
                map.serialize_entry(key, v)?;
            }
        }
        map.end()
    }
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
    /// pipeline starts). Dim references (T-D1) are part of the statement's
    /// table-object needs — their metadata (kind=dim + bootstrap +
    /// sequence_column) must ride with the plan too.
    pub fn missing_tables(&self) -> Vec<String> {
        let mut refs = std::collections::BTreeSet::new();
        for s in &self.statements {
            refs.insert(&s.source);
            refs.insert(&s.sink);
            for d in &s.dims {
                refs.insert(&d.reference);
            }
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
        Ok(TableObject::new(ds, table, normalize_kind(&t.kind))
            .format_opt(t.format.clone())
            .with_columns(t.columns.clone())
            .with_options(t.options.clone())
            // serve-mode label prefix convention (`p{pipeline id}`;
            // standalone FileCatalog uses `{declared}-{pipeline id}`)
            .with_option(option_keys::LABEL_PREFIX, format!("p{}", self.pipeline_id)))
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

    /// G2: the stable public path `tailrace_catalog::ColumnType::parse` is
    /// live (canonical home tailrace-ir, re-exported here — blueprint G2
    /// wording). Representative words across the 16-word vocabulary plus the
    /// unknown-word fold through [`arrow_type_or_utf8`] (migration window:
    /// warn + Utf8, never a silent hard error mid-window). The full 16-word
    /// round-trip / illegal-input matrices live next to the parser in
    /// tailrace-ir's column_type tests.
    #[test]
    fn column_type_vocabulary_reexport_path() {
        use datafusion::arrow::datatypes::DataType;
        use datafusion::arrow::datatypes::TimeUnit;

        // legacy 4 words — the byte-compat anchor the two migrated
        // arrow_type sites rely on
        assert_eq!(
            ColumnType::parse("utf8").unwrap().to_arrow(),
            DataType::Utf8
        );
        assert_eq!(
            ColumnType::parse("int64").unwrap().to_arrow(),
            DataType::Int64
        );
        // new parameterized words reach catalog consumers unmodified
        assert_eq!(
            ColumnType::parse("decimal(18,2)").unwrap(),
            ColumnType::Decimal {
                precision: 18,
                scale: 2
            }
        );
        assert_eq!(
            ColumnType::parse("timestamp(us)").unwrap(),
            ColumnType::Timestamp(TimeUnit::Microsecond)
        );
        assert_eq!(
            ColumnType::parse("decimal(18,2)").unwrap().to_string(),
            "decimal(18,2)"
        );
        // strict parse: unknown words error
        assert!(ColumnType::parse("varchar(64)").is_err());
        // migration window fold (blueprint §7.6): same unknown word through
        // the schema-construction entry lands Utf8, not an error
        assert_eq!(
            arrow_type_or_utf8("note", "varchar(64)").unwrap(),
            DataType::Utf8
        );
    }

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
        assert_eq!(
            t.option_str(option_keys::BOOTSTRAP),
            Some("localhost:9092"),
            "datasource-level bootstrap folds into options and resolves"
        );
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
        assert_eq!(
            normalize_kind("JDBC"),
            "jdbc",
            "jdbc is the SOURCE kind — distinct from the JDBC_TABLE→doris sink alias"
        );
        assert_eq!(normalize_kind("starrocks"), "starrocks");
        // T-D1: the dimension kind (broadcast join side)
        assert_eq!(normalize_kind("DIM"), "dim");
        assert_eq!(normalize_kind("dim"), "dim");
        assert_eq!(
            normalize_kind("mqtt"),
            "mqtt",
            "unknown kinds pass through lowercased"
        );
    }

    /// T-D1: a FileCatalog dim table declaration carries kind=dim +
    /// sequence_column (the PK / unique column used as the full-table load
    /// pagination key) through resolve(); a statement's dims ride the
    /// StatementSpec (`dims` serde-defaults empty — old plan bodies and tomls
    /// stay valid).
    #[test]
    fn file_catalog_dim_table_and_statement_dims() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("tailrace-cat-dim-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let f = dir.join("dim.toml");
        std::fs::write(
            &f,
            r#"
[pipeline]
id = 43
name = "dim-demo"

[[datasource]]
name = "db"
type = "mysql"
bootstrap = "mysql://root@localhost/test"

[[table]]
datasource = "db"
name = "users"
kind = "dim"
columns = [["id", "int64"], ["name", "utf8"]]
sequence_column = "id"

[[statement]]
source = "k.orders"
group = "tr.p43.s0"
sink = "k.orders_wide"
select_sql = "SELECT s.uid, d.name FROM src s LEFT JOIN dim_d d ON s.uid = d.id"

[[statement.dims]]
reference = "db.users"
name = "dim_d"
"#,
        )?;
        let cat = FileCatalog::load(&f)?;
        let stmts = cat.statements()?;
        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0].dims,
            vec![DimJoin {
                reference: "db.users".into(),
                name: "dim_d".into()
            }],
            "toml-declared dims parse into StatementSpec.dims"
        );
        let dim = cat.resolve("db.users")?;
        assert_eq!(dim.kind, "dim", "kind=dim normalizes to the dim key");
        assert_eq!(
            dim.option_str(option_keys::BOOTSTRAP),
            Some("mysql://root@localhost/test")
        );
        assert_eq!(dim.option_str(option_keys::SEQUENCE_COLUMN), Some("id"));
        assert_eq!(dim.columns.len(), 2);
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    /// A7: the partial-column subset declared at plan level (toml table /
    /// plan-body table object) rides resolve() verbatim — None (the
    /// serde-default) keeps pre-A7 declarations meaning full-row writes.
    #[test]
    fn partial_columns_flow_through_catalogs() -> anyhow::Result<()> {
        // FileCatalog: toml `partial_columns = [...]` on the sink table
        let dir = std::env::temp_dir().join(format!("tailrace-cat-pu-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let f = dir.join("pu.toml");
        std::fs::write(
            &f,
            r#"
[pipeline]
id = 44
name = "partial-demo"

[[datasource]]
name = "d"
type = "doris"
stream_load_url = "http://127.0.0.1:8040/api/db/t/_stream_load"
auth_env = "TAILRACE_TEST_AUTH"

[[table]]
datasource = "d"
name = "orders"
kind = "doris"
columns = [["order_no", "utf8"], ["qty", "int64"], ["note", "utf8"]]
sequence_column = "seq"

[[table]]
datasource = "d"
name = "orders_partial"
kind = "doris"
columns = [["order_no", "utf8"], ["seq", "int64"], ["qty", "int64"], ["note", "utf8"]]
sequence_column = "seq"
partial_columns = ["order_no", "seq", "qty"]

[[statement]]
source = "d.orders"
group = "tr.p44.s0"
sink = "d.orders_partial"
select_sql = "SELECT order_no, seq, qty FROM src"
"#,
        )?;
        let cat = FileCatalog::load(&f)?;
        assert_eq!(
            cat.resolve("d.orders")?
                .option_str_list(option_keys::PARTIAL_COLUMNS),
            None,
            "no declaration = full-row writes (the default)"
        );
        assert_eq!(
            cat.resolve("d.orders_partial")?
                .option_str_list(option_keys::PARTIAL_COLUMNS),
            Some(vec![
                "order_no".to_string(),
                "seq".to_string(),
                "qty".to_string()
            ]),
            "the declared subset rides the table object options"
        );
        std::fs::remove_dir_all(&dir)?;

        // PlanCatalog: the console-pushed plan body carries the same option
        // (serde-default keeps pre-A7 plan bodies valid)
        let table = PlanTableObject::new("1.db.orders_partial", "doris")
            .with_columns(vec![("order_no".into(), "utf8".into())])
            .with_option(
                option_keys::PARTIAL_COLUMNS,
                serde_json::json!(["order_no"]),
            );
        let plan = PlanCatalog::new(9, cat.statements()?, vec![table]);
        assert_eq!(
            plan.resolve("1.db.orders_partial")?
                .option_str_list(option_keys::PARTIAL_COLUMNS),
            Some(vec!["order_no".to_string()])
        );
        Ok(())
    }

    /// T-D1: the plan preflight (`missing_tables`) counts dim references — a
    /// dim join whose table object metadata did not ride with the plan
    /// rejects the whole plan up front (not at first assembly).
    #[test]
    fn plan_preflight_counts_dim_references() {
        let stmt = StatementSpec {
            source: "1.local.input".into(),
            group: "tr.p7.s0".into(),
            sink: "1.local.out".into(),
            pushdown: vec![],
            select_sql: "SELECT s.uid, d.name FROM src s LEFT JOIN dim_d d ON s.uid = d.id".into(),
            start_offset: None,
            dims: vec![DimJoin {
                reference: "2.db.dim_user".into(),
                name: "dim_d".into(),
            }],
            partial_columns: None,
        };
        let table = |r: &str| PlanTableObject::new(r, "file");
        // source/sink present, dim metadata missing → the dim reference is
        // the reported gap
        let cat = PlanCatalog::new(
            7,
            vec![stmt.clone()],
            vec![table("1.local.input"), table("1.local.out")],
        );
        assert_eq!(cat.missing_tables(), vec!["2.db.dim_user".to_string()]);
        // all three present → no gap
        let cat = PlanCatalog::new(
            7,
            vec![stmt],
            vec![
                table("1.local.input"),
                table("1.local.out"),
                PlanTableObject::new("2.db.dim_user", "dim")
                    .with_option(option_keys::SEQUENCE_COLUMN, "id"),
            ],
        );
        assert!(cat.missing_tables().is_empty());
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

    // ==================== R8 G1: options decoupling — dual-write/dual-read equivalence ====================

    /// The canonical post-G1 plan table entry (options map) and the pre-G1
    /// one (legacy top-level fields) deserialize to the SAME PlanTableObject
    /// — the dual-read half of the G1 serde window. Explicit `options` keys
    /// win over a legacy spelling of the same field.
    #[test]
    fn plan_body_legacy_and_options_shapes_read_identically() {
        let legacy = serde_json::json!({
            "reference": "1.ods.orders",
            "kind": "doris",
            "format": "ogg",
            "columns": [["k", "utf8"], ["seq", "int64"]],
            "bootstrap": null,
            "stream_load_url": "http://127.0.0.1:8040/api/ods/orders/_stream_load",
            "auth_env": "TAILRACE_DS_1",
            "sequence_column": "seq",
            "partial_columns": ["k", "seq"]
        });
        let modern = serde_json::json!({
            "reference": "1.ods.orders",
            "kind": "doris",
            "format": "ogg",
            "columns": [["k", "utf8"], ["seq", "int64"]],
            "options": {
                "stream_load_url": "http://127.0.0.1:8040/api/ods/orders/_stream_load",
                "auth_env": "TAILRACE_DS_1",
                "sequence_column": "seq",
                "partial_columns": ["k", "seq"]
            }
        });
        let a: PlanTableObject = serde_json::from_value(legacy).expect("legacy shape parses");
        let b: PlanTableObject = serde_json::from_value(modern).expect("options shape parses");
        assert_eq!(a.reference, b.reference);
        assert_eq!(a.kind, b.kind);
        assert_eq!(a.format, b.format);
        assert_eq!(a.columns, b.columns);
        assert_eq!(a.options, b.options, "both shapes fold to one options map");
        assert_eq!(
            a.option_str(option_keys::STREAM_LOAD_URL),
            Some("http://127.0.0.1:8040/api/ods/orders/_stream_load")
        );
        assert_eq!(
            a.option_str_list(option_keys::PARTIAL_COLUMNS),
            Some(vec!["k".to_string(), "seq".to_string()])
        );

        // options spelling WINS over the legacy spelling of the same field
        // (one field spelled both ways in the same body)
        let mixed = serde_json::json!({
            "reference": "1.ods.orders",
            "kind": "doris",
            "bootstrap": "legacy-value",
            "options": { "bootstrap": "options-value" }
        });
        let m: PlanTableObject = serde_json::from_value(mixed).unwrap();
        assert_eq!(m.option_str(option_keys::BOOTSTRAP), Some("options-value"));
    }

    /// The dual-write half: a serialized post-G1 PlanTableObject carries the
    /// options map AND every known legacy key at its pre-G1 top-level
    /// spelling — a pre-G1 engine (which ignores the unknown `options` key)
    /// still reads every field, and re-parsing the artifact reproduces the
    /// same object (round-trip).
    #[test]
    fn plan_table_object_serializes_dual_write_and_round_trips() {
        let obj = PlanTableObject::new("4.ods.orders", "doris")
            .with_format("ogg")
            .with_columns(vec![("k".into(), "utf8".into())])
            .with_option(option_keys::STREAM_LOAD_URL, "http://127.0.0.1:8040/x")
            .with_option(option_keys::AUTH_ENV, "TAILRACE_DS_4")
            .with_option(option_keys::SEQUENCE_COLUMN, "seq")
            .with_option(
                option_keys::PARTIAL_COLUMNS,
                serde_json::json!(["k", "seq"]),
            )
            .with_option("groupCommit", "off_mode"); // arbitrary extra key rides along
        let v = serde_json::to_value(&obj).unwrap();
        // dual-written legacy spellings present at top level
        assert_eq!(
            v["stream_load_url"], "http://127.0.0.1:8040/x",
            "legacy top-level spelling survives (pre-G1 engines read this)"
        );
        assert_eq!(v["auth_env"], "TAILRACE_DS_4");
        assert_eq!(v["sequence_column"], "seq");
        assert_eq!(v["partial_columns"], serde_json::json!(["k", "seq"]));
        // options map carries everything, including keys that have no legacy
        // spelling
        assert_eq!(v["options"]["groupCommit"], "off_mode");
        // round-trip: serialize → deserialize reproduces the object
        let back: PlanTableObject = serde_json::from_value(v).unwrap();
        assert_eq!(back.reference, obj.reference);
        assert_eq!(back.kind, obj.kind);
        assert_eq!(back.format, obj.format);
        assert_eq!(back.columns, obj.columns);
        assert_eq!(back.options, obj.options);
    }

    /// The regression anchor (blueprint §7.7 G1 row): a pre-G1 plan body —
    /// legacy field names, `null`s where the console writes nulls — deploys
    /// through PlanCatalog::resolve with every connector option intact.
    #[test]
    fn legacy_plan_body_resolves_through_plan_catalog() -> anyhow::Result<()> {
        // byte shape of a real pre-G1 doris sink entry (console
        // build_plan_table output, T11-era): nulls included
        let body = r#"{
            "reference": "3.shop.orders",
            "kind": "doris_table",
            "format": null,
            "columns": [["order_no","utf8"],["qty","int64"]],
            "bootstrap": null,
            "stream_load_url": "http://localhost:8040/api/shop/orders/_stream_load",
            "auth_env": "TAILRACE_TEST",
            "sequence_column": null,
            "partial_columns": null
        }"#;
        let t: PlanTableObject = serde_json::from_str(body)?;
        let cat = PlanCatalog::new(31, Vec::new(), vec![t]);
        let obj = cat.resolve("3.shop.orders")?;
        assert_eq!(obj.kind, "doris", "doris_table alias normalizes");
        assert_eq!(
            obj.option_str(option_keys::STREAM_LOAD_URL),
            Some("http://localhost:8040/api/shop/orders/_stream_load")
        );
        assert_eq!(obj.option_str(option_keys::AUTH_ENV), Some("TAILRACE_TEST"));
        assert_eq!(
            obj.option_str(option_keys::LABEL_PREFIX),
            Some("p31"),
            "serve-mode label prefix is synthesized at resolve"
        );
        assert_eq!(obj.option_str(option_keys::SEQUENCE_COLUMN), None);
        assert_eq!(obj.option_str_list(option_keys::PARTIAL_COLUMNS), None);
        assert_eq!(obj.columns.len(), 2);
        Ok(())
    }

    /// Legacy TOML specs (standalone FileCatalog) keep deploying: top-level
    /// connection fields fold into options at both the datasource and the
    /// table level, table-level keys overriding datasource-level ones; the
    /// `options` table spelling reads identically.
    #[test]
    fn legacy_toml_table_fields_fold_into_options() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join(format!("tailrace-cat-g1-{}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        let f = dir.join("g1.toml");
        std::fs::write(
            &f,
            r#"
[pipeline]
id = 77
name = "fold"

[[datasource]]
name = "d"
type = "doris"
stream_load_url = "http://127.0.0.1:8040/api/a/b/_stream_load"
auth_env = "TAILRACE_TEST_AUTH"

[[table]]
datasource = "d"
name = "legacy_spelled"
kind = "doris"
columns = [["k", "utf8"]]
label_prefix = "mreg"
sequence_column = "k"
partial_columns = ["k"]

[[table]]
datasource = "d"
name = "options_spelled"
kind = "doris"
columns = [["k", "utf8"]]
options = { label_prefix = "opt", sequence_column = "k" }
"#,
        )?;
        let cat = FileCatalog::load(&f)?;
        let legacy = cat.resolve("d.legacy_spelled")?;
        // table-level fold + datasource fallback (none of these are on the
        // table) + the standalone `{prefix}-{pipeline}` label convention
        assert_eq!(
            legacy.option_str(option_keys::STREAM_LOAD_URL),
            Some("http://127.0.0.1:8040/api/a/b/_stream_load"),
            "datasource-level stream_load_url falls through"
        );
        assert_eq!(
            legacy.option_str(option_keys::AUTH_ENV),
            Some("TAILRACE_TEST_AUTH")
        );
        assert_eq!(
            legacy.option_str(option_keys::LABEL_PREFIX),
            Some("mreg-77")
        );
        assert_eq!(legacy.option_str(option_keys::SEQUENCE_COLUMN), Some("k"));
        assert_eq!(
            legacy.option_str_list(option_keys::PARTIAL_COLUMNS),
            Some(vec!["k".to_string()])
        );
        // the `options` table spelling reads the same way
        let spelled = cat.resolve("d.options_spelled")?;
        assert_eq!(
            spelled.option_str(option_keys::LABEL_PREFIX),
            Some("opt-77")
        );
        assert_eq!(spelled.option_str(option_keys::SEQUENCE_COLUMN), Some("k"));
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }
}
