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
//!
//! # A tenant is a file
//!
//! There is no tenant column, no tenant argument, and no per-tenant anything
//! inside one log. A tenant is a second [`SqliteEventLog`] on a second file,
//! and the isolation that gives is total: its own global position sequence,
//! its own `checkpoints` namespace, its own retention ledger and export
//! receipts, its own writer. Nothing one tenant does is visible to another
//! through any coordinate, because they share none.
//!
//! That last point is why a column would be the wrong answer here. The
//! global position is dense and allocated by one writer, and a reader is
//! entitled to treat a gap as a fact about retention. Put two tenants in
//! one sequence and every gap one of them sees is the other's writes —
//! either the positions leak the existence and rate of a neighbour's
//! activity, or the sequence has to be per tenant, which is what a second
//! file already is. Marten's conjoined tenancy exists because a PostgreSQL
//! instance is expensive to duplicate; a SQLite file is not.
//!
//! What a file costs is a thread. Each log owns a writer thread and, by
//! default, two reader threads with their connections
//! ([`OpenOptions::readers`]), so a hundred tenants is a hundred writers and
//! two hundred readers. `readers` brings the second number down — `0` puts
//! reads back on the writer — and a tenant that is idle costs its threads'
//! stacks and nothing else. Whether that is acceptable at a given tenant count is
//! the question to answer before choosing files; the store does not answer
//! it, and does not offer a cheaper shape that would.

mod hatch;
mod log;
mod project;
mod retention;
mod row;
mod schema;
mod shared;
mod store;
mod transfer;
mod txn;

pub use hatch::RESERVED_TABLES;
pub use log::{OpenOptions, SqliteEventLog, DEFAULT_BUSY_TIMEOUT, DEFAULT_POLL_INTERVAL};
pub use project::{Projection, ProjectionRunner, DEFAULT_BATCH};
pub use retention::{Completeness, ExportReceipt, Guard, Plan, Report};
pub use schema::TARGET_USER_VERSION;
pub use store::SqliteEventStore;
pub use txn::TxnContext;

/// Re-exported so a projection can name the transaction it is handed, and a
/// hatch closure can bind parameters, without pinning its own `rusqlite`
/// version against this crate's.
pub use rusqlite;
pub use rusqlite::Transaction;
