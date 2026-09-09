//! An embedded, append-only event log.
//!
//! `eventsdb` is an event store that lives in the process rather than behind
//! a socket. It keeps the guarantees an event store exists for — immutable
//! facts, per-stream ordering, decisions taken inside the write, schema
//! evolution without rewriting stored bytes — and drops the ones that require
//! a cluster.
//!
//! # The layers
//!
//! | Layer | What it answers |
//! |-------|-----------------|
//! | [`event`] | what a valid event is, and which fields the store owns |
//! | [`upcast`] | how a reader sees an old event's current shape |
//! | [`store`] | one stream: append, decide-and-append, read, head |
//! | [`log`] | the database: read across streams, subscribe, checkpoint |
//!
//! This crate is the contract and the in-memory backend. A durable backend is
//! a separate crate implementing the same two traits.
//!
//! # What single-writer buys
//!
//! Two properties this design has are not available to a store that runs as a
//! separate service, and both come from the same place — one writer, one
//! transaction:
//!
//! - **Positions have no holes.** The backend allocates a
//!   [`position::Position`] inside the transaction that commits it, so a
//!   reader never sees `n + 1` while `n` is still uncommitted. Following the
//!   log is a range read, with no gap detection and no grace window.
//! - **A projection can be exactly-once.** When the read model lives in the
//!   same database as the log, applying an event and advancing the consumer's
//!   checkpoint are one transaction. There is no dedupe table and no
//!   idempotence requirement on the projection author.
//!
//! Distributing the store forfeits both.

pub mod error;
pub mod event;
pub mod log;
pub mod mem;
pub mod position;
pub mod store;
pub mod transfer;
pub mod upcast;

pub use error::{Error, Result};
pub use event::{validate, CURRENT_SCHEMA_VERSION};
pub use log::{EventLog, Filter};
pub use mem::MemEventStore;
pub use position::{Committed, Position, Recorded};
pub use store::{Decision, EventStore};
pub use transfer::{ExportedEvent, ImportReport};
pub use upcast::{Current, UpcastChain, Upcaster};
