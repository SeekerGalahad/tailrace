//! T14 integration tests for the metadata clients.
//!
//! Gating: `TAILRACE_TESTCONTAINERS=1` boots ephemeral containers (same
//! live-gate convention as conn-kafka / conn-jdbc; tags pinned to the
//! preheated local images); without the flag everything skips.
//!
//! Coverage choices, stated honestly:
//! - **Kafka**: real ephemeral broker (apache native image) — discover lists
//!   auto-created topics with partition counts, infer-schema samples real
//!   produced JSON and OGG messages.
//! - **Doris FE (9030)**: NOT started locally — the BE container refuses to
//!   come up under this machine's swap constraints (R3-W1 known wall), and
//!   FE-only gives an empty catalog. Instead the MySQL-protocol code path
//!   Doris shares (`SHOW DATABASES` / `SHOW TABLES` / `DESC` over the same
//!   wire protocol and statements) runs against a real mysql:8.0 container,
//!   and Doris-specific DDL type names (`largeint`, `decimalv3`,
//!   `datetimev2`, `array<…>`, …) are pinned by the always-on unit table in
//!   src/infer.rs. A true Doris-against-FE run stays a CI/environment debt.
//! - **PostgreSQL**: real ephemeral postgres:15 (information_schema path).

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use testcontainers::core::ImageExt;
use testcontainers::runners::SyncRunner;
use testcontainers_modules::{kafka, mysql, postgres};

use tailrace_metadata::{discover, infer_schema, ConnSpec};

fn gated() -> bool {
    std::env::var("TAILRACE_TESTCONTAINERS").ok().as_deref() == Some("1")
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn mysql_spec(port: u16) -> ConnSpec {
    ConnSpec::MySql {
        host: "127.0.0.1".into(),
        port,
        database: None,
        username: "root".into(),
        password: "".into(),
    }
}

#[test]
fn kafka_discover_and_infer_live() -> anyhow::Result<()> {
    if !gated() {
        eprintln!("[skip] TAILRACE_TESTCONTAINERS!=1 — metadata live tests skipped");
        return Ok(());
    }
    let node = kafka::apache::Kafka::default().start()?;
    let bootstrap = format!(
        "127.0.0.1:{}",
        node.get_host_port_ipv4(kafka::apache::KAFKA_PORT)?
    );
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()?;
    // produce first: auto-create + seed both a json topic and an ogg topic
    let json_topic = format!("shop.orders.{}", std::process::id());
    let ogg_topic = format!("shop.orders_ogg.{}", std::process::id());
    let messages: Vec<(&str, &str)> = vec![
        (
            json_topic.as_str(),
            r#"{"order_no":"A1","seq":7,"amount":1.5,"ok":true}"#,
        ),
        (
            json_topic.as_str(),
            r#"{"order_no":"A2","seq":8,"amount":2,"note":"n"}"#,
        ),
        (
            ogg_topic.as_str(),
            r#"{"table":"SHOP.ORDERS","op_type":"I","ts":1700000000000,"after":{"order_no":"A1","seq":7,"amount":1.5}}"#,
        ),
    ];
    for (topic, payload) in messages {
        for _ in 0..20 {
            match producer.send(BaseRecord::to(topic).payload(payload).key("k")) {
                Ok(()) => break,
                Err((e, _)) => {
                    eprintln!("[retry] produce {topic}: {e}");
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        }
    }
    producer.flush(Duration::from_secs(15))?;

    let spec = ConnSpec::Kafka { bootstrap };
    // --- discover: both topics listed as (shop, orders*) with partitions,
    // internal (`__consumer_offsets` …) hidden
    let assets = rt().block_on(discover(&spec))?;
    let listed: Vec<&tailrace_metadata::DiscoveredAsset> = assets
        .iter()
        .filter(|a| a.table_name.starts_with("orders"))
        .collect();
    assert_eq!(listed.len(), 2, "assets: {assets:?}");
    let json_asset = listed
        .iter()
        .find(|a| a.table_name.starts_with("orders."))
        .expect("json topic present");
    assert_eq!(json_asset.db_name, "shop");
    assert_eq!(json_asset.partitions, Some(1));
    assert!(assets.iter().all(|a| !a.table_name.starts_with("__")));

    // --- infer json: first-dot split rebuilds the topic name (columns in
    // serde_json's canonical alphabetical order)
    let json_schema = rt().block_on(infer_schema(
        &spec,
        &json_asset.db_name,
        &json_asset.table_name,
        None,
        20,
    ))?;
    assert_eq!(json_schema.format.as_deref(), Some("json"));
    assert_eq!(
        json_schema.columns,
        vec![
            ("amount".to_string(), "float64".to_string()),
            ("note".to_string(), "utf8".to_string()),
            ("ok".to_string(), "bool".to_string()),
            ("order_no".to_string(), "utf8".to_string()),
            ("seq".to_string(), "int64".to_string()),
        ]
    );

    // --- infer ogg: detected from the `after` envelope, columns prefixed by
    // the three OggDecoder envelope slots
    let ogg_table = ogg_topic
        .split_once('.')
        .map(|(_, t)| t)
        .unwrap_or("")
        .to_string();
    let ogg_schema = rt().block_on(infer_schema(&spec, "shop", &ogg_table, None, 20))?;
    assert_eq!(ogg_schema.format.as_deref(), Some("ogg"));
    assert_eq!(
        ogg_schema.columns[..3].to_vec(),
        vec![
            ("op_type".to_string(), "utf8".to_string()),
            ("ogg_table".to_string(), "utf8".to_string()),
            ("ogg_ts".to_string(), "int64".to_string()),
        ],
        "envelope first: {:?}",
        ogg_schema.columns
    );
    assert!(
        ogg_schema
            .columns
            .iter()
            .any(|(n, t)| n == "seq" && t == "int64"),
        "{:?}",
        ogg_schema.columns
    );
    Ok(())
}

/// MySQL-protocol path (Doris FE shares it): SHOW DATABASES / SHOW TABLES /
/// DESC with an assortment of types.
#[test]
fn mysql_protocol_discover_and_describe_live() -> anyhow::Result<()> {
    if !gated() {
        eprintln!("[skip] TAILRACE_TESTCONTAINERS!=1 — metadata live tests skipped");
        return Ok(());
    }
    let node = mysql::Mysql::default().with_tag("8.0").start()?;
    let port = node.get_host_port_ipv4(3306)?;
    // schema + seed through the same driver stack
    let setup = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    setup.block_on(async {
        use sqlx_core::connection::Connection;
        use sqlx_core::executor::Executor;
        use sqlx_core::sql_str::AssertSqlSafe;
        let mut conn =
            sqlx_mysql::MySqlConnection::connect(&format!("mysql://root@127.0.0.1:{port}")).await?;
        conn.execute(AssertSqlSafe("CREATE DATABASE shop")).await?;
        conn.execute(AssertSqlSafe(
            "CREATE TABLE shop.orders (\
             id BIGINT PRIMARY KEY, order_no VARCHAR(64), amount DOUBLE, \
             qty INT, paid BOOLEAN, note TEXT, born DATETIME)",
        ))
        .await?;
        anyhow::Ok(())
    })?;

    // --- discover: (shop, orders) present; mysql's system schemas hidden
    let assets = rt().block_on(discover(&mysql_spec(port)))?;
    let hit = assets
        .iter()
        .find(|a| a.db_name == "shop" && a.table_name == "orders")
        .expect("shop.orders discovered");
    assert_eq!(hit.partitions, None);
    assert!(
        !assets.iter().any(
            |a| ["mysql", "sys", "information_schema", "performance_schema"]
                .contains(&a.db_name.as_str())
        ),
        "system schemas must be hidden: {assets:?}"
    );

    // --- infer: DDL types through the arrow vocabulary. NOTE `paid`:
    // MySQL has no true BOOLEAN — DESC prints `tinyint(1)` → int64 (a
    // tinyint holding 2..127 must not fold to bool); the `boolean`→bool
    // arm serves Doris DESC output (unit-pinned in src/infer.rs).
    let schema = rt().block_on(infer_schema(&mysql_spec(port), "shop", "orders", None, 0))?;
    assert_eq!(schema.format, None);
    assert_eq!(
        schema.columns,
        vec![
            ("id".to_string(), "int64".to_string()),
            ("order_no".to_string(), "utf8".to_string()),
            ("amount".to_string(), "float64".to_string()),
            ("qty".to_string(), "int64".to_string()),
            ("paid".to_string(), "int64".to_string()),
            ("note".to_string(), "utf8".to_string()),
            ("born".to_string(), "utf8".to_string()),
        ]
    );
    // unknown table → error
    let err = rt().block_on(infer_schema(&mysql_spec(port), "shop", "nope", None, 0));
    assert!(err.is_err());
    Ok(())
}

#[test]
fn postgres_discover_and_describe_live() -> anyhow::Result<()> {
    if !gated() {
        eprintln!("[skip] TAILRACE_TESTCONTAINERS!=1 — metadata live tests skipped");
        return Ok(());
    }
    let node = postgres::Postgres::default().with_tag("15").start()?;
    let port = node.get_host_port_ipv4(5432)?;
    // module default: POSTGRES_USER/POSTGRES_PASSWORD = postgres/postgres
    let setup = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    setup.block_on(async {
        use sqlx_core::connection::Connection;
        use sqlx_core::executor::Executor;
        use sqlx_core::sql_str::AssertSqlSafe;
        let mut conn = sqlx_postgres::PgConnection::connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}"
        ))
        .await?;
        conn.execute(AssertSqlSafe(
            "CREATE SCHEMA shop; CREATE TABLE shop.orders (\
             id BIGINT PRIMARY KEY, order_no TEXT, amount DOUBLE PRECISION, \
             qty INTEGER, paid BOOLEAN, note TEXT)",
        ))
        .await?;
        anyhow::Ok(())
    })?;

    let spec = ConnSpec::Postgres {
        host: "127.0.0.1".into(),
        port,
        database: None,
        username: "postgres".into(),
        password: "postgres".into(),
    };
    let assets = rt().block_on(discover(&spec))?;
    assert!(
        assets
            .iter()
            .any(|a| a.db_name == "shop" && a.table_name == "orders"),
        "{assets:?}"
    );
    assert!(
        !assets.iter().any(|a| a.db_name.starts_with("pg_")),
        "system schemas hidden: {assets:?}"
    );
    let schema = rt().block_on(infer_schema(&spec, "shop", "orders", None, 0))?;
    assert_eq!(
        schema.columns,
        vec![
            ("id".to_string(), "int64".to_string()),
            ("order_no".to_string(), "utf8".to_string()),
            ("amount".to_string(), "float64".to_string()),
            ("qty".to_string(), "int64".to_string()),
            ("paid".to_string(), "bool".to_string()),
            ("note".to_string(), "utf8".to_string()),
        ]
    );
    Ok(())
}
