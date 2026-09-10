//! An embedded, append-only event log.
//!
//! This crate is a name, not a layer. Everything it exposes is defined
//! elsewhere and re-exported here, so that one dependency reaches the whole
//! store rather than two, and so that the bare name means what a reader of
//! `eventsdb-core` and `eventsdb-sqlite` already assumes it means.
//!
//! | you want | you get |
//! |----------|---------|
//! | the contract, the envelope, the two traits, the in-memory backend | this crate, as it comes |
//! | the durable backend | this crate with the `sqlite` feature, in the `sqlite` module |
//!
//! The backend is behind a feature because the contract is usable without it:
//! a caller writing upcasters, or testing against
//! [`MemEventStore`], has no reason to build SQLite.
//!
//! Depending on `eventsdb-core` and `eventsdb-sqlite` directly remains
//! correct, and is the same code. Nothing here wraps, adapts or renames what
//! those crates export.

pub use eventsdb_core::*;

/// The durable backend: one file, one writer thread, WAL — plus projections,
/// which need the same connection to be exactly-once.
///
/// This is `eventsdb-sqlite`, unchanged.
#[cfg(feature = "sqlite")]
pub use eventsdb_sqlite as sqlite;
