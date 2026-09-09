//! The SQLite backend for [`eventsdb_core`].
//!
//! One file, one writer thread, WAL. The connection is confined to its own
//! thread by `rusqlite-isle`, so writes to a stream are serialized by
//! construction rather than by a lock this crate holds — and that single
//! writer is what makes the global order gap-free as read (see
//! [`eventsdb_core::position::Position`]).
//!
//! ```no_run
//! use eventsdb_core::{EventLog, EventStore, Filter, Position};
//! use eventsdb_sqlite::SqliteEventLog;
//! use serde_json::json;
//!
//! # async fn example() -> eventsdb_core::Result<()> {
//! let log = SqliteEventLog::open("events.db").await?;
//!
//! let mut orders = log.stream_handle("order-1");
//! orders
//!     .append(json!({ "kind": "placed", "data": { "total": 40 } })
//!         .as_object()
//!         .unwrap()
//!         .clone())
//!     .await?;
//!
//! let caught_up = log.read_all(Position::BEGINNING, &Filter::all(), 100).await?;
//! assert_eq!(caught_up.len(), 1);
//! # Ok(())
//! # }
//! ```

mod log;
mod project;
mod row;
mod schema;
mod shared;
mod store;

pub use log::{OpenOptions, SqliteEventLog, DEFAULT_BUSY_TIMEOUT, DEFAULT_POLL_INTERVAL};
pub use project::{Projection, ProjectionRunner, DEFAULT_BATCH};
pub use schema::TARGET_USER_VERSION;
pub use store::SqliteEventStore;

/// Re-exported so a projection can name the transaction it is handed without
/// pinning its own `rusqlite` version against this crate's.
pub use rusqlite::Transaction;
