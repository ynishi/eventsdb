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
//! # Instrumentation
//!
//! Behind the **`tracing`** feature, off by default:
//!
//! ```toml
//! eventsdb-sqlite = { version = "0.5", features = ["tracing"] }
//! ```
//!
//! With it off, `tracing` is not in the dependency graph and the call sites
//! compile to the code that was there before. With it on, a subscriber sees
//! what the paths this crate documents in numbers are actually doing on the
//! caller's own log: how long a decision held the write lock, how large a
//! batch the runner chose, whether a subscription woke on the channel or on
//! the interval. Nothing is emitted unless a subscriber is installed.
//!
//! **The payload is never a field.** `data` and `meta` are the caller's and
//! may hold anything — a tenant, a customer, a name. Neither appears in a
//! span or an event at any level. What does appear: coordinates (`position`,
//! `seq`, a cursor), counts, a consumer or projection name, and — at `debug`,
//! never above — a stream id and a kind. The one caller-authored text that is
//! emitted at all is the hatch's `sql`, which is at `trace` and is bounded;
//! see the last row of the table.
//!
//! | path | span, at `debug` | fields |
//! |---|---|---|
//! | [`EventStore::append`], [`append_many`], [`append_at`] | `eventsdb.append` | `stream`, `count`, `position` on exit |
//! | [`EventStore::append_if`] | `eventsdb.decide` | `stream`, `folded` (how many events the decision was shown), `appended` |
//! | [`EventStore::append_expecting`] | `eventsdb.append` | `stream`, `expected`, `outcome` (`appended` / `head_mismatch` / `failed`), `position` |
//! | [`ProjectionRunner::run_once`] | `eventsdb.project` | `name`, `batch`, `applied` and `cursor` on exit |
//! | [`SqliteEventLog::retain`] | `eventsdb.retain` | `plan` (the ledger's own string), `guard`, `removed`, `highest_removed` (`0` when nothing matched) |
//! | [`SqliteEventLog::archive_then_retain`] | `eventsdb.archive` | `plan`, `page`, `pages`, `exported`; the removal under it is `eventsdb.retain` |
//! | [`SqliteEventLog::open`] and the rest of the constructors | `eventsdb.open` | `path`, `user_version_before` and `user_version_after` the ladder |
//! | [`SqliteEventLog::query`] and its variants | `eventsdb.hatch` | `op = "query"`, `rows` on exit; the `sql` is a **`trace`** event inside the span, clipped to its first 256 characters and marked with `…` when there were more |
//! | [`SqliteEventLog::with_transaction`] | `eventsdb.hatch` | `op = "transaction"`, `appended` |
//!
//! Two things are events rather than spans:
//!
//! - **A wake**, at `trace`, one per round of [`EventLog::subscribe`] and
//!   [`SqliteEventLog::replay`]: `woken_by` is `commit` or `interval`, with
//!   the `cursor` it carried on from. This is the distinction the README's
//!   Benchmarks section quotes in microseconds against milliseconds, and it
//!   cannot be seen from outside the loop — both arrive as a batch.
//! - **A refusal**, at `warn`, carrying the error's own text: a fold over a
//!   log missing its front ([`eventsdb_core::Error::Truncated`]), a removal
//!   no export covers ([`eventsdb_core::Error::NotExported`]) or that would
//!   overrun a consumer ([`eventsdb_core::Error::ConsumerBehind`]), and a
//!   statement the hatch's authorizer turned down. These are the four where
//!   the store declines rather than fails, and an operator wants them without
//!   turning on `debug`.
//!
//! [`append_many`]: eventsdb_core::EventStore::append_many
//! [`append_at`]: eventsdb_core::EventStore::append_at
//! [`EventStore::append`]: eventsdb_core::EventStore::append
//! [`EventStore::append_if`]: eventsdb_core::EventStore::append_if
//! [`EventStore::append_expecting`]: eventsdb_core::EventStore::append_expecting
//! [`EventLog::subscribe`]: eventsdb_core::EventLog::subscribe

mod backup;
mod catalog;
mod hatch;
mod log;
mod project;
mod retention;
mod row;
mod schema;
mod shared;
mod store;
mod trace;
mod transfer;
mod txn;

pub use catalog::{ConsumerCheckpoint, ExportRecord, RetentionEntry, StreamInfo};
/// Re-exported so a caller binding named parameters into `query`, or handing
/// [`SqliteEventLog::archive_then_retain`] somewhere to put the bytes, does
/// not have to name `eventsdb-core` as a dependency of its own.
pub use eventsdb_core::{JsonLinesSink, LogSink, Params, Sink};
pub use hatch::{QueryOptions, READ_ONLY_PRAGMAS, RESERVED_TABLES};
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
