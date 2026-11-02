//! `tailrace-console apply` — the T-G6 CLI thin shell: read a declaration
//! file, POST it to the server-side apply endpoints (the same pair GitOps /
//! CI / MCP would hit), print the diff or result for humans. No diff logic
//! lives here on purpose: the server computes the only diff, the CLI only
//! renders it, so the two can never disagree.
//!
//! Auth mirrors the API surface: a session token via `--token` /
//! `TAILRACE_CONSOLE_TOKEN` (Bearer), or `--user` + `--password` (env
//! `TAILRACE_CONSOLE_USER` / `TAILRACE_CONSOLE_PASSWORD`) which logs in
//! first. Plain HTTP — same stance as the console→node clients (the
//! workspace reqwest carries no TLS stack); the apply body carries
//! credentials, so point `--server` at a trusted network path.

use anyhow::{bail, Context, Result};
use serde_json::Value;

/// Default console address (the server's own default listen addr).
const DEFAULT_SERVER: &str = "http://127.0.0.1:8080";

/// Parsed `tailrace-console apply` flags.
#[derive(Debug, Clone)]
pub struct ApplyOpts {
    pub file: std::path::PathBuf,
    pub dry_run: bool,
    pub server: String,
    pub token: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
}

pub fn usage() {
    eprintln!(
        "Usage: tailrace-console apply -f <file.json> [--dry-run] [--server <URL>]\n\
         \n\
         Pipeline-as-code (T-G6): send a declaration file to the console's\n\
         apply endpoints and print the diff / result.\n\
         \n\
         Options:\n\
         \x20 -f, --file <PATH>     declaration file (JSON; see docs/ops/pipeline-file.md)\n\
         \x20     --dry-run         diff only, write nothing (POST /api/apply/dry-run)\n\
         \x20 -s, --server <URL>    console base URL (default {DEFAULT_SERVER})\n\
         \x20 -t, --token <TOKEN>   session token (Authorization: Bearer)\n\
         \x20 -u, --user <NAME>     login username (with --password)\n\
         \x20 -p, --password <PW>   login password\n\
         \n\
         Env:\n\
         \x20 TAILRACE_CONSOLE_TOKEN       session token (same as --token)\n\
         \x20 TAILRACE_CONSOLE_USER        login username\n\
         \x20 TAILRACE_CONSOLE_PASSWORD    login password"
    );
}

/// Hand-rolled flag parsing, mirroring the engine bin style (zero deps).
pub fn parse_args(argv: &[String]) -> Result<ApplyOpts> {
    let mut opts = ApplyOpts {
        file: std::path::PathBuf::new(),
        dry_run: false,
        server: DEFAULT_SERVER.into(),
        token: None,
        user: None,
        password: None,
    };
    let mut i = 0;
    while i < argv.len() {
        let arg = argv[i].as_str();
        let (flag, inline) = match arg.split_once('=') {
            Some((f, v)) => (f, Some(v.to_string())),
            None => (arg, None),
        };
        if matches!(flag, "-h" | "--help") {
            usage();
            std::process::exit(0);
        }
        let value = |i: &mut usize| -> Result<String> {
            if let Some(v) = inline.clone() {
                return Ok(v);
            }
            *i += 1;
            argv.get(*i)
                .cloned()
                .with_context(|| format!("missing value for {flag}"))
        };
        match flag {
            "-f" | "--file" => opts.file = std::path::PathBuf::from(value(&mut i)?),
            "--dry-run" | "-n" => opts.dry_run = true,
            "-s" | "--server" => opts.server = value(&mut i)?,
            "-t" | "--token" => opts.token = Some(value(&mut i)?),
            "-u" | "--user" => opts.user = Some(value(&mut i)?),
            "-p" | "--password" => opts.password = Some(value(&mut i)?),
            other => bail!("unknown flag {other:?} (see `tailrace-console apply --help`)"),
        }
        i += 1;
    }
    if opts.file.as_os_str().is_empty() {
        bail!("missing -f <file> (see `tailrace-console apply --help`)");
    }
    Ok(opts)
}

/// Resolve the auth token: explicit `--token` / env, else log in with
/// `--user`/`--password` (env fallbacks) and use the issued token.
async fn resolve_token(opts: &ApplyOpts, client: &reqwest::Client) -> Result<String> {
    if let Some(token) = opts
        .token
        .clone()
        .or_else(|| std::env::var("TAILRACE_CONSOLE_TOKEN").ok())
    {
        return Ok(token);
    }
    let user = opts
        .user
        .clone()
        .or_else(|| std::env::var("TAILRACE_CONSOLE_USER").ok());
    let password = opts
        .password
        .clone()
        .or_else(|| std::env::var("TAILRACE_CONSOLE_PASSWORD").ok());
    let (Some(user), Some(password)) = (user, password) else {
        bail!(
            "no credentials: pass --token (or TAILRACE_CONSOLE_TOKEN), or --user + --password\n\
             (or TAILRACE_CONSOLE_USER / TAILRACE_CONSOLE_PASSWORD)"
        );
    };
    let resp = client
        .post(format!("{}/api/auth/login", opts.server))
        .json(&serde_json::json!({"username": user, "password": password}))
        .send()
        .await
        .context("login request failed")?;
    let status = resp.status();
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    if !status.is_success() {
        bail!(
            "login failed ({}): {}",
            status.as_u16(),
            body.pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("no message")
        );
    }
    body.get("token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .context("login response carried no token")
}

/// POST the declaration file; returns the HTTP status and parsed response
/// body. Exposed (with `ApplyOpts`) so the contract tests drive the exact
/// client path against a served router — the "same file via API and CLI"
/// guarantee is tested, not asserted.
pub async fn execute(opts: &ApplyOpts) -> Result<(u16, Value)> {
    let bytes =
        std::fs::read(&opts.file).with_context(|| format!("read {}", opts.file.display()))?;
    let client = console_proxy::node_http_client().context("http client")?;
    let token = resolve_token(opts, &client).await?;
    let path = if opts.dry_run {
        "/api/apply/dry-run"
    } else {
        "/api/apply"
    };
    let resp = client
        .post(format!("{}{path}", opts.server))
        .bearer_auth(token)
        .header("content-type", "application/json")
        .body(bytes)
        .send()
        .await
        .with_context(|| format!("POST {}{path}", opts.server))?;
    let status = resp.status().as_u16();
    let text = resp.text().await.context("read response body")?;
    let body: Value = serde_json::from_str(&text).unwrap_or(Value::String(text));
    Ok((status, body))
}

/// Entry point from main: run, print, return the process exit code.
pub async fn run(argv: &[String]) -> i32 {
    let opts = match parse_args(argv) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("tailrace-console apply: {e:#}");
            return 2;
        }
    };
    match execute(&opts).await {
        Ok((status, body)) => {
            print!("{}", render(&body, &opts));
            if (200..300).contains(&status) {
                0
            } else {
                eprintln!(
                    "apply failed (HTTP {status}): {}",
                    body.pointer("/error/message")
                        .and_then(Value::as_str)
                        .unwrap_or("(no message)")
                );
                1
            }
        }
        Err(e) => {
            eprintln!("tailrace-console apply: {e:#}");
            1
        }
    }
}

// ---------------- rendering (pure) ----------------

/// Max length of a rendered diff value before truncation.
const VALUE_LIMIT: usize = 72;

fn render_value(v: &Value) -> String {
    let s = match v {
        Value::Null => "null".into(),
        other => serde_json::to_string(other).unwrap_or_else(|_| "?".into()),
    };
    if s.chars().count() <= VALUE_LIMIT {
        s
    } else {
        let head: String = s.chars().take(VALUE_LIMIT).collect();
        format!("{head}…")
    }
}

/// sqlContent diffs render as a length pair instead of the whole text:
/// the file is right next to the reviewer, dumping SQL into the summary
/// adds noise, not information.
fn render_change(field: &str, from: &Value, to: &Value) -> String {
    if field == "sqlContent" {
        let len = |v: &Value| v.as_str().map(str::chars).map(Iterator::count).unwrap_or(0);
        format!(
            "      sqlContent: <changed, {} -> {} chars>",
            len(from),
            len(to)
        )
    } else {
        format!(
            "      {field}: {} -> {}",
            render_value(from),
            render_value(to)
        )
    }
}

/// Human-readable diff / result report. Deterministic (same body → same
/// bytes); unit-tested so the output shape is pinned.
pub fn render(body: &Value, opts: &ApplyOpts) -> String {
    let mut out = String::new();
    let mode = if opts.dry_run { "dry-run" } else { "apply" };
    out.push_str(&format!(
        "tailrace-console apply ({mode}) — server {}, file {}\n",
        opts.server,
        opts.file.display()
    ));
    for section in ["datasources", "tables", "pipelines"] {
        let items = body.get(section).and_then(Value::as_array);
        if items.is_none() {
            continue; // section absent from the file: not managed, not reported
        }
        out.push_str(&format!("\n{section}:\n"));
        if let Some(items) = items {
            if items.is_empty() {
                out.push_str("  (none declared)\n");
            }
            for item in items {
                let action = item.get("action").and_then(Value::as_str).unwrap_or("?");
                let mark = match action {
                    "create" => "+",
                    "update" => "~",
                    _ => "=",
                };
                let key = item.get("key").and_then(Value::as_str).unwrap_or("?");
                let id = item.get("id").and_then(Value::as_i64);
                out.push_str(&format!(
                    "  {mark} {key:<34} {action}{}\n",
                    id.map(|i| format!("  (#{i})")).unwrap_or_default()
                ));
                if let Some(changes) = item.get("changes").and_then(Value::as_array) {
                    for change in changes {
                        out.push_str(&render_change(
                            change.get("field").and_then(Value::as_str).unwrap_or("?"),
                            change.get("from").unwrap_or(&Value::Null),
                            change.get("to").unwrap_or(&Value::Null),
                        ));
                        out.push('\n');
                    }
                }
            }
        }
    }
    if let Some(orphans) = body.get("orphans").and_then(Value::as_array) {
        if !orphans.is_empty() {
            out.push_str("\norphans (in the console, not in this file — apply never deletes):\n");
            for o in orphans {
                out.push_str(&format!(
                    "  - {}  {}  (#{})\n",
                    o.get("kind").and_then(Value::as_str).unwrap_or("?"),
                    o.get("key").and_then(Value::as_str).unwrap_or("?"),
                    o.get("id").and_then(Value::as_i64).unwrap_or(0)
                ));
            }
        }
    }
    let summary = body.pointer("/summary").cloned().unwrap_or(Value::Null);
    let count = |k: &str| summary.get(k).and_then(Value::as_u64).unwrap_or(0);
    out.push_str(&format!(
        "\nsummary: create {}, update {}, unchanged {}, orphan {}\n",
        count("create"),
        count("update"),
        count("unchanged"),
        count("orphan")
    ));
    if opts.dry_run {
        out.push_str("(dry-run: nothing was written)\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn opts(dry_run: bool) -> ApplyOpts {
        ApplyOpts {
            file: std::path::PathBuf::from("pipelines.json"),
            dry_run,
            server: "http://127.0.0.1:8080".into(),
            token: None,
            user: None,
            password: None,
        }
    }

    /// The report pins: three-state marks, field-level change lines (SQL as
    /// a length pair, connJson sub-key dotted, credential redacted), orphan
    /// listing, summary line, and the dry-run footer.
    #[test]
    fn render_pins_the_report_shape() {
        let body = json!({
            "dryRun": true,
            "summary": {"create": 1, "update": 1, "unchanged": 1, "orphan": 1},
            "datasources": [
                {"key": "edge-kafka", "action": "create", "id": null, "changes": []},
                {"key": "shop-doris", "action": "update", "id": 2, "changes": [
                    {"field": "connJson.jdbcUrl", "from": "jdbc:mysql://a", "to": "jdbc:mysql://b"},
                    {"field": "credential", "from": "<redacted>", "to": "<redacted>"}
                ]},
                {"key": "file-src", "action": "unchanged", "id": 3, "changes": []}
            ],
            "tables": [
                {"key": "edge-kafka/shop.orders", "action": "create", "id": null, "changes": []}
            ],
            "pipelines": [
                {"key": "orders→ods", "action": "update", "id": 7, "changes": [
                    {"field": "sqlContent", "from": "INSERT INTO ods.orders SELECT 1", "to": "INSERT INTO ods.orders SELECT 1, 2"}
                ]}
            ],
            "orphans": [
                {"kind": "pipeline", "key": "old-one", "id": 3}
            ]
        });
        let out = render(&body, &opts(true));
        assert!(out.contains(
            "tailrace-console apply (dry-run) — server http://127.0.0.1:8080, file pipelines.json"
        ));
        assert!(out.contains("+ edge-kafka"));
        assert!(out.contains("~ shop-doris"));
        assert!(out.contains("update  (#2)"));
        assert!(out.contains("connJson.jdbcUrl: \"jdbc:mysql://a\" -> \"jdbc:mysql://b\""));
        assert!(out.contains("credential: \"<redacted>\" -> \"<redacted>\""));
        assert!(out.contains("sqlContent: <changed, 31 -> 34 chars>"));
        assert!(out.contains("orphans (in the console, not in this file — apply never deletes):"));
        assert!(out.contains("- pipeline  old-one  (#3)"));
        assert!(out.contains("summary: create 1, update 1, unchanged 1, orphan 1"));
        assert!(out.contains("(dry-run: nothing was written)"));
        // a section the file did not declare is absent from the wire and
        // from the report; a declared-but-empty one says so explicitly
        let absent = json!({
            "dryRun": false,
            "summary": {"create": 0, "update": 0, "unchanged": 0, "orphan": 0},
            "orphans": []
        });
        let rendered = render(&absent, &opts(false));
        assert!(!rendered.contains("datasources:"));
        assert!(!rendered.contains("tables:"));
        assert!(!rendered.contains("pipelines:"));
        let declared_empty = json!({
            "dryRun": false,
            "summary": {"create": 0, "update": 0, "unchanged": 0, "orphan": 0},
            "datasources": [], "orphans": []
        });
        assert!(render(&declared_empty, &opts(false)).contains("datasources:\n  (none declared)"));
    }

    #[test]
    fn long_values_are_truncated() {
        let long = "x".repeat(200);
        assert!(render_change("connJson.bootstrap", &json!(long), &json!("h:9092")).contains('…'));
    }

    /// Flag parsing: the required file, inline `=` forms, dry-run toggle,
    /// and the missing-file refusal.
    #[test]
    fn flag_parsing() {
        let argv = ["apply", "-f", "a.json", "--dry-run", "--server=http://x:1"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        // argv here excludes the subcommand word; parse takes the rest
        let rest: Vec<String> = argv[1..].to_vec();
        let opts = parse_args(&rest).expect("parses");
        assert_eq!(opts.file, std::path::PathBuf::from("a.json"));
        assert!(opts.dry_run);
        assert_eq!(opts.server, "http://x:1");
        assert!(parse_args(&[]).is_err(), "missing -f refuses");
        let bad = ["--wat".to_string()];
        assert!(parse_args(&bad).is_err());
    }
}
