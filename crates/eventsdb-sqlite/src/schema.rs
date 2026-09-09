//! The table's shape, and the ladder that moves it forward.
//!
//! # Two migration axes, and they are not the same one
//!
//! | Axis | Subject | Mechanism | Marker |
//! |------|---------|-----------|--------|
//! | Event shape | `data`, `meta`, the meaning of a kind | upcaster chain, applied on read | `_schema_version` |
//! | Table shape | columns, indices, constraints | this ladder, applied once at open | `PRAGMA user_version` |
//!
//! An upcaster transforms an event's JSON and cannot create a column, so a
//! change like "add a global position" has no place to run on the first axis.
//! Conflating the two is how a schema change ends up with no defined home.
//!
//! Each step is one transaction and runs before any append is served. Steps
//! are append-only: a shipped step is never edited, because a database that
//! already ran it will not run it again.

use eventsdb_core::error::{Error, Result};
use rusqlite::{Connection, TransactionBehavior};

use crate::shared::classify;

/// The version this build expects. Bumped with every appended step.
pub const TARGET_USER_VERSION: i64 = 1;

/// Step 1: the log and the consumer checkpoints.
///
/// `position` is `INTEGER PRIMARY KEY AUTOINCREMENT`, which is two decisions:
///
/// - **`INTEGER PRIMARY KEY`** aliases SQLite's rowid, and rowid values
///   survive `VACUUM`. A separate `INTEGER NOT NULL UNIQUE` column would need
///   its own allocation code and would give up a monotonic counter the
///   database already keeps.
/// - **`AUTOINCREMENT`** stops a rowid from being reused after a delete.
///   Nothing deletes today, but retention is on the list, and the first time
///   a row is removed a reused position silently rewinds every consumer
///   cursor pointing past it. The cost is the `sqlite_sequence` table; the
///   alternative is a class of bug that cannot be detected after the fact.
///
/// `(stream, seq)` is demoted from primary key to a unique constraint, which
/// keeps the same guarantee and the same lookup.
///
/// `meta` and `data` are `NOT NULL` because they are filled in with `{}` on
/// the way in, so a reader never has to tell an empty object from a missing
/// one.
const STEP_1: &str = "
    CREATE TABLE events (
        position       INTEGER PRIMARY KEY AUTOINCREMENT,
        stream         TEXT    NOT NULL,
        seq            INTEGER NOT NULL,
        epoch_ms       INTEGER NOT NULL,
        kind           TEXT    NOT NULL,
        schema_version INTEGER NOT NULL,
        meta           TEXT    NOT NULL,
        data           TEXT    NOT NULL,
        UNIQUE (stream, seq)
    );
    CREATE INDEX events_stream_kind_seq ON events (stream, kind, seq);
    CREATE INDEX events_kind_position   ON events (kind, position);

    CREATE TABLE checkpoints (
        consumer   TEXT PRIMARY KEY,
        position   INTEGER NOT NULL,
        updated_ms INTEGER NOT NULL
    );
";

/// The ladder, in order. Index `i` moves `user_version` from `i` to `i + 1`.
///
/// A database that predates `position` would get its backfill as a step here:
/// add the column, then number the existing rows by `(epoch_ms, stream, seq)`
/// — wall clock first, because it approximates causal order across streams,
/// with `seq` breaking ties deterministically within one. No such database
/// exists yet, so no such step is written.
const LADDER: &[&str] = &[STEP_1];

/// Bring `conn` up to [`TARGET_USER_VERSION`], one transaction per step.
///
/// Runs once at open and before any handle is issued, so no append is ever
/// served against a half-migrated schema. A database from a *newer* build is
/// refused rather than used: this build does not know what the extra steps
/// did, and writing under that assumption is how a log gets corrupted.
pub fn migrate(conn: &mut Connection) -> Result<()> {
    let current: i64 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(classify)?;

    if current > TARGET_USER_VERSION {
        return Err(Error::storage(format!(
            "database is at user_version {current}, newer than this build's \
             {TARGET_USER_VERSION}; refusing to write to a schema this build does not know"
        )));
    }

    for (index, step) in LADDER.iter().enumerate() {
        let from = index as i64;
        if from < current {
            continue;
        }
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify)?;
        tx.execute_batch(step).map_err(classify)?;
        tx.pragma_update(None, "user_version", from + 1)
            .map_err(classify)?;
        tx.commit().map_err(classify)?;
    }

    Ok(())
}

/// Apply the connection-level settings the backend depends on.
///
/// WAL so readers do not block the writer. `synchronous = NORMAL` is the
/// usual WAL pairing: a crash can lose the tail of the last transaction but
/// cannot corrupt the database. `busy_timeout` is what makes contention
/// surface as a wait rather than an immediate failure — and it only covers a
/// lock taken up front, which is why every write here uses `IMMEDIATE`.
pub fn apply_pragmas(conn: &Connection, busy_timeout: std::time::Duration) -> rusqlite::Result<()> {
    conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
    conn.busy_timeout(busy_timeout)?;
    Ok(())
}
