//! tailrace-connector: Source/Sink traits + explicit ConnectorRegistry.
//!
//! The traits are the production contract (pause semantics, half-batch
//! rollback hooks, delivery acknowledgement); each connector crate exposes a
//! `register(&mut ConnectorRegistry)` entry point and the binary wires them by
//! Cargo feature — no macro magic.

pub mod registry;
pub mod sink;
pub mod source;

pub use registry::{ConnectorRegistry, SinkFactory, SourceFactory};
pub use sink::{Sink, SinkAck};
pub use source::Source;

// Cursor/SourceChunk live in tailrace-ir; re-exported so connector crates
// depend on this crate alone for the full contract.
pub use tailrace_ir::{Cursor, SourceChunk};
