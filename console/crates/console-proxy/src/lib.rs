//! console-proxy — console → engine-node HTTP layer (design doc §3.1 聚合代理,
//! §4.6 division matrix): the shared client factories, the typed /v1 node
//! endpoint wrapper, and the datasource connectivity probes.

mod client;
mod node;
mod probe;

pub use client::{node_http_client, node_stream_client};
pub use node::{NodeEndpoint, ProxyError};
pub use probe::probe_datasource;
