//! tailrace-node: single-binary entry (toml config + CLI).
//! run <pipeline.toml> [--state-dir DIR] [--secs N] [--serve ADDR]: run a
//! pipeline standalone (FileCatalog).
//! compile <pipeline.sql> [--pipeline-id N]: SQL compile diagnostics.
//! serve [--addr ADDR]: pure API service mode (engine node — pipelines arrive
//! via console plan push).

use std::path::PathBuf;
use std::time::Duration;

use tailrace_connector::ConnectorRegistry;

/// Connector wiring: explicit register calls gated by Cargo features — no
/// macro magic (inventory/linkme would add deps and cognitive load; with
/// ≤10 connectors, explicit registration stays obvious at a glance).
fn build_registry() -> ConnectorRegistry {
    // placeholder-only feature combos register nothing (engine M10) — the
    // `mut` is used only when at least one real connector feature is on
    #[allow(unused_mut)]
    let mut reg = ConnectorRegistry::new();
    #[cfg(feature = "connector-kafka")]
    tailrace_conn_kafka::register(&mut reg);
    #[cfg(feature = "connector-file")]
    tailrace_conn_file::register(&mut reg);
    #[cfg(feature = "connector-doris")]
    tailrace_conn_doris::register(&mut reg);
    // P2 connector crates (mqtt/jdbc/s3) are one-line placeholders with no
    // `register` — enabling their features links them but registers nothing
    // (engine M10: placeholder crates must not break the build).
    reg
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("run") => {
            let spec = args.get(2).unwrap_or_else(|| {
                eprintln!("usage: tailrace-node run <pipeline.toml> [--state-dir DIR] [--secs N] [--serve ADDR]");
                std::process::exit(2);
            });
            let state_dir = flag_value(&args, "--state-dir")
                .map(PathBuf::from)
                .unwrap_or_else(|| std::env::temp_dir().join("tailrace-offsets"));
            let secs: u64 = flag_value(&args, "--secs")
                .and_then(|v| v.parse().ok())
                .unwrap_or(30);
            let serve_addr = flag_value(&args, "--serve");

            // --serve: start the API service first (background thread), then
            // run the pipeline (foreground, blocking)
            if let Some(addr) = &serve_addr {
                let state = tailrace_api::StateHandle::new();
                // the API service reuses the --state-dir registry (plan-push
                // state lands in the operator-specified state_dir, same as
                // the serve subcommand path)
                let registry = std::sync::Arc::new(
                    tailrace_runtime::registry::PipelineRegistry::with_stream_load_hosts(
                        &state_dir,
                        build_registry(),
                        stream_load_hosts_from_env(),
                    ),
                );
                init_tracing(Some(registry.log_hub()));
                state.upsert_pipeline(tailrace_api::PipelineSnapshot {
                    id: 0,
                    statements: vec![format!("run {}", spec)],
                    status: "RUNNING",
                });
                let api_token =
                    resolve_api_token(&state_dir, flag_value(&args, "--api-token"), None);
                match tailrace_api::start_server(addr, state, registry, api_token) {
                    Ok(_) => println!("API service started: http://{addr} (/healthz open; /v1 requires the node token)"),
                    Err(e) => {
                        tracing::error!(error = %format!("{e:#}"), "API service start failed");
                        shutdown_otel();
                        std::process::exit(1);
                    }
                }
            } else {
                init_tracing(None);
            }

            let opts = tailrace_runtime::RunOptions {
                duration: Duration::from_secs(secs),
                ..Default::default()
            };
            match tailrace_runtime::run_standalone(&PathBuf::from(spec), &state_dir, opts) {
                Ok((id, stats)) => {
                    for (i, s) in stats.iter().enumerate() {
                        println!("statement#{i}: consumed={} appended={} skipped={} flushed_rows={} flushes={}",
                            s.consumed, s.appended, s.skipped, s.flushed_rows, s.flushes);
                    }
                    println!("pipeline#{id} finished");
                }
                Err(e) => {
                    tracing::error!(error = %format!("{e:#}"), "pipeline execution failed");
                    shutdown_otel();
                    std::process::exit(1);
                }
            }
            shutdown_otel();

            // with --serve the process stays alive after the pipeline (the
            // API service thread keeps running)
            if serve_addr.is_some() {
                println!("pipeline finished — API service keeps running (Ctrl+C to exit)");
                loop {
                    std::thread::sleep(Duration::from_secs(3600));
                }
            }
        }
        Some("serve") => {
            let addr = flag_value(&args, "--addr").unwrap_or_else(|| "127.0.0.1:18100".to_string());
            // --control-plane enables self-registration + heartbeat (without
            // it, behavior is unchanged)
            let control_plane = flag_value(&args, "--control-plane");
            let name = flag_value(&args, "--name").unwrap_or_else(|| "engine-node".to_string());
            let advertise = flag_value(&args, "--advertise").unwrap_or_else(|| addr.clone());
            let state_dir = flag_value(&args, "--state-dir")
                .map(PathBuf::from)
                .unwrap_or_else(|| std::env::temp_dir().join("tailrace-node-state"));
            let heartbeat_secs: u64 = flag_value(&args, "--heartbeat-secs")
                .and_then(|v| v.parse().ok())
                .unwrap_or(30);
            let flag_node_key = flag_value(&args, "--node-key");
            let bootstrap_token = flag_value(&args, "--bootstrap-token");
            let api_token_flag = flag_value(&args, "--api-token");
            let stream_load_hosts = flag_value(&args, "--stream-load-hosts")
                .and_then(|v| parse_host_set(&v))
                .or_else(stream_load_hosts_from_env);

            let state = tailrace_api::StateHandle::new();
            let registry = std::sync::Arc::new(
                tailrace_runtime::registry::PipelineRegistry::with_stream_load_hosts(
                    &state_dir,
                    build_registry(),
                    stream_load_hosts,
                ),
            );
            // the log hub must exist before the subscriber: the tracing
            // layer sinks pipeline-tagged WARN/ERROR events into it
            init_tracing(Some(registry.log_hub()));
            // engine H1: /v1 sits behind the shared node token; when the
            // console issues a fresh per-node token at register time the
            // heartbeat rotates this handle live
            let api_token =
                resolve_api_token(&state_dir, api_token_flag, bootstrap_token.as_deref());
            let worker = control_plane.as_ref().map(|cp| {
                println!(
                    "heartbeat enabled: control-plane={cp} advertise={advertise} state_dir={} secs={heartbeat_secs}",
                    state_dir.display()
                );
                tailrace_api::heartbeat::HeartbeatWorker::start(
                    tailrace_api::heartbeat::HeartbeatConfig {
                        control_plane: cp.clone(),
                        advertise: advertise.clone(),
                        name: name.clone(),
                        state_dir: state_dir.clone(),
                        heartbeat_secs,
                        engine_version: env!("CARGO_PKG_VERSION").to_string(),
                        flag_node_key: flag_node_key.clone(),
                        bootstrap_token: bootstrap_token.clone(),
                    },
                    state.clone(),
                    Some(std::sync::Arc::clone(&registry)),
                    Some(api_token.clone()),
                )
            });

            match tailrace_api::start_server(
                &addr,
                state,
                std::sync::Arc::clone(&registry),
                api_token,
            ) {
                Ok(h) => {
                    println!("tailrace-node serve mode: http://{addr} (/healthz open; /v1 requires the node token)");
                    if let Some(w) = worker {
                        // signal thread: SIGINT/SIGTERM → final heartbeat beat
                        // (NODE_STOP) → process exit
                        let w = std::sync::Arc::new(w);
                        let sig = w.clone();
                        std::thread::spawn(move || {
                            let rt = tokio::runtime::Builder::new_current_thread()
                                .enable_all()
                                .build()
                                .expect("signal runtime");
                            rt.block_on(async {
                                let mut term = tokio::signal::unix::signal(
                                    tokio::signal::unix::SignalKind::terminate(),
                                )
                                .expect("SIGTERM listener");
                                tokio::select! {
                                    _ = tokio::signal::ctrl_c() => {}
                                    _ = term.recv() => {}
                                }
                                sig.request_stop();
                            });
                        });
                        // main thread stays resident until the signal-driven
                        // final heartbeat completes. Do not exit on a
                        // join_timeout as a fallback — that makes the engine
                        // "self-terminate N seconds after startup".
                        while !w.is_finished() {
                            std::thread::sleep(Duration::from_millis(500));
                        }
                        // graceful-shutdown finalization: stop all pipelines
                        // explicitly before exit (final flush + cursor
                        // commit). Skipping it loses in-memory batches and
                        // widens the replay window to a whole batching period.
                        registry.shutdown_all();
                        println!("serve shut down gracefully, exiting");
                        shutdown_otel();
                        std::process::exit(0);
                    }
                    // Without --control-plane the shutdown is graceful too (a
                    // bare SIGTERM kill would lose in-memory batches). signal
                    // → shutdown_all (final flush + cursor commit) → exit
                    let reg_sig = std::sync::Arc::clone(&registry);
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("signal runtime");
                        rt.block_on(async {
                            let mut term = tokio::signal::unix::signal(
                                tokio::signal::unix::SignalKind::terminate(),
                            )
                            .expect("SIGTERM listener");
                            tokio::select! {
                                _ = tokio::signal::ctrl_c() => {}
                                _ = term.recv() => {}
                            }
                        });
                        reg_sig.shutdown_all();
                        println!("serve graceful shutdown (signal-triggered, final flush + cursor commit done), exiting");
                        shutdown_otel();
                        std::process::exit(0);
                    });
                    let _ = h.join();
                }
                Err(e) => {
                    tracing::error!(error = %format!("{e:#}"), "API service start failed");
                    shutdown_otel();
                    std::process::exit(1);
                }
            }
        }
        Some("compile") => {
            init_tracing(None);
            let sql_path = match args.get(2) {
                Some(p) => p.clone(),
                None => {
                    eprintln!("usage: tailrace-node compile <pipeline.sql> [--pipeline-id N]");
                    std::process::exit(2);
                }
            };
            let pipeline_id: u64 = flag_value(&args, "--pipeline-id")
                .and_then(|v| v.parse().ok())
                .unwrap_or(1);
            match std::fs::read_to_string(&sql_path)
                .map_err(|e| anyhow::anyhow!("{e}"))
                .and_then(|sql| tailrace_parser::compile(&sql, pipeline_id))
            {
                Ok((compiled, diags)) => {
                    for st in &compiled.statements {
                        println!(
                            "statement {} → {}  group={}  pushdown={:?}",
                            st.source, st.sink, st.group, st.pushdown
                        );
                        println!("  select: {}", st.select_sql.replace('\n', " "));
                    }
                    for d in &diags {
                        tracing::warn!(statement = ?d.statement, "{}", d.message);
                    }
                    if !diags.is_empty() {
                        shutdown_otel();
                        std::process::exit(3);
                    }
                }
                Err(e) => {
                    tracing::error!(error = %format!("{e:#}"), "compile failed");
                    shutdown_otel();
                    std::process::exit(1);
                }
            }
        }
        _ => {
            init_tracing(None);
            println!(
                "tailrace-node {} (subcommands: run <pipeline.toml> [--serve ADDR] | serve [--addr ADDR] | compile <pipeline.sql>)",
                env!("CARGO_PKG_VERSION")
            );
        }
    }
}

/// T2 observability bootstrap: fmt (stderr) + EnvFilter (RUST_LOG, default
/// info) + the PipelineLogHub layer — engine WARN/ERROR events tagged with a
/// pipeline_id land in the per-pipeline ring the console SSE chain reads —
/// plus the optional OTLP span-export layer (feature `otel`, runtime-activated
/// by OTEL_EXPORTER_OTLP_ENDPOINT). `hub` is None on the standalone run /
/// compile paths (no registry, no per-pipeline sink).
fn init_tracing(hub: Option<std::sync::Arc<tailrace_runtime::pipeline_log::PipelineLogHub>>) {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    // the OTLP layer must sit directly on the registry (it layers over one
    // concrete subscriber type); plain tracing remains the always-on floor
    // when the feature is off or no endpoint is configured
    #[cfg(feature = "otel")]
    let stack = tracing_subscriber::registry().with(init_otel_layer());
    #[cfg(not(feature = "otel"))]
    let stack = tracing_subscriber::registry();
    stack
        .with(filter)
        // stderr: the eprintln channel tracing replaces wrote to stderr, and
        // the node's stdout carries result output (statement#… lines) — logs
        // must not pollute it (the fmt default is stdout)
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(hub.map(tailrace_runtime::pipeline_log::PipelineLogLayer::new))
        .init();
}

/// OTLP exporter provider handle (shutdown flushes the batch exporter).
#[cfg(feature = "otel")]
static OTEL_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

/// Build the OTLP layer: None when the feature is off-by-env (no
/// OTEL_EXPORTER_OTLP_ENDPOINT) or the exporter fails to build — plain
/// tracing continues either way. The exporter is configured entirely by the
/// standard OTLP env vars (endpoint/protocol/headers); the blocking reqwest
/// client matches the engine's synchronous threads.
#[cfg(feature = "otel")]
fn init_otel_layer() -> Option<
    tracing_opentelemetry::OpenTelemetryLayer<
        tracing_subscriber::Registry,
        opentelemetry_sdk::trace::SdkTracer,
    >,
> {
    use opentelemetry::trace::TracerProvider as _;

    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
        return None; // compiled in, staying dark (feature default-off anyway)
    }
    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            // subscriber not up yet — a plain stderr notice is the only channel
            eprintln!("[otel] OTLP exporter init failed (continuing without OTLP): {e:?}");
            return None;
        }
    };
    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(format!("tailrace-node@{}", env!("CARGO_PKG_VERSION")))
                .build(),
        )
        .build();
    let tracer = provider.tracer("tailrace-node");
    let _ = OTEL_PROVIDER.set(provider);
    Some(tracing_opentelemetry::layer().with_tracer(tracer))
}

/// Best-effort OTLP flush before process exit (spans still in the batch
/// exporter are dropped otherwise). No-op without the feature.
#[cfg(feature = "otel")]
fn shutdown_otel() {
    if let Some(p) = OTEL_PROVIDER.get() {
        let _ = p.shutdown();
    }
}

#[cfg(not(feature = "otel"))]
fn shutdown_otel() {}

fn flag_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

/// Resolve the /v1 node token (engine H1): flag > env > adopted per-node
/// token file > --bootstrap-token > generated (persisted + printed once).
fn resolve_api_token(
    state_dir: &std::path::Path,
    api_token_flag: Option<String>,
    bootstrap_flag: Option<&str>,
) -> tailrace_api::auth::NodeApiToken {
    let (token, origin) = tailrace_api::auth::resolve_node_api_token(
        state_dir,
        api_token_flag.as_deref(),
        bootstrap_flag,
    );
    match origin {
        tailrace_api::auth::TokenOrigin::Generated => {} // already logged with the value
        _ => tracing::info!(origin = ?origin, "/v1 node token source (value not logged)"),
    }
    token
}

/// `TAILRACE_STREAM_LOAD_HOSTS` env → explicit allowlist (engine H1).
fn stream_load_hosts_from_env() -> Option<std::collections::HashSet<String>> {
    std::env::var("TAILRACE_STREAM_LOAD_HOSTS")
        .ok()
        .and_then(|v| parse_host_set(&v))
}

/// Comma/space/semicolon-separated host list (empty → None = loopback
/// default).
fn parse_host_set(value: &str) -> Option<std::collections::HashSet<String>> {
    let set: std::collections::HashSet<String> = value
        .split([',', ' ', ';'])
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(|h| h.trim_end_matches('.').to_lowercase())
        .collect();
    (!set.is_empty()).then_some(set)
}
