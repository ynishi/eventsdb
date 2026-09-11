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

use std::time::{Duration, Instant};

use eventsdb_core::error::{Error, Result};
use rusqlite::{Connection, Transaction, TransactionBehavior};

use crate::hatch::RESERVED_TABLES;
use crate::shared::{classify, is_contention};

/// The version this build expects. Bumped with every appended step.
pub const TARGET_USER_VERSION: i64 = 5;

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

/// Step 2: the retention ledger.
///
/// Retention is the one operation that removes facts, so what it removed is
/// itself recorded. `highest_removed` is the watermark a reader compares its
/// cursor against: a consumer at or below it has lost events it never saw,
/// and is told rather than handed a short answer (see
/// [`eventsdb_core::Error::Truncated`]).
///
/// The ledger is append-only like the log, and deliberately survives the
/// events it describes — it is the only remaining evidence that they existed.
const STEP_2: &str = "
    CREATE TABLE retention (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        applied_ms      INTEGER NOT NULL,
        plan            TEXT    NOT NULL,
        removed_count   INTEGER NOT NULL,
        highest_removed INTEGER NOT NULL
    );
    CREATE INDEX events_epoch_ms ON events (epoch_ms);
";

/// Step 3: the per-stream sequence counter, so `seq` cannot rewind.
///
/// `seq` used to be derived from `MAX(seq)` over the surviving rows, which is
/// correct only while nothing is ever removed. Retention broke it: dropping a
/// stream — or ageing out the whole of one — leaves no rows to take the
/// maximum from, so the next append to that stream name was stamped `seq = 1`
/// again. `UNIQUE(stream, seq)` does not catch it, because the rows that
/// would have collided are gone.
///
/// That is the same reuse the `position` column's `AUTOINCREMENT` exists to
/// prevent, and the argument was simply never carried across to `seq`. A
/// downstream table keyed `(stream, seq)` would end up with two different
/// events under one key and no error anywhere.
///
/// So the counter is stored, and **retention does not touch this table**: it
/// outlives the events it counted, exactly as the retention ledger does.
///
/// The backfill takes each stream's current maximum, which is the right
/// starting point for every database written before this step.
const STEP_3: &str = "
    CREATE TABLE stream_seq (
        stream   TEXT PRIMARY KEY,
        next_seq INTEGER NOT NULL
    );
    INSERT INTO stream_seq (stream, next_seq)
        SELECT stream, MAX(seq) + 1 FROM events GROUP BY stream;
";

/// Step 4: a stored event cannot be updated, by anyone holding the file.
///
/// The authorizer in [`crate::hatch`] already refuses an `UPDATE`, but it
/// covers this crate's own connection and only while a hatch call is running.
/// It is a property of *this API*. Anything else that opens the file is
/// unaffected — `sqlite3 log.db "UPDATE events SET kind = ..."` included.
///
/// A trigger lives in the schema, so every connection that opens the file gets
/// it, whoever opened it. That is the difference worth buying: append-only
/// stops being something callers are asked to respect and becomes a property
/// of the data.
///
/// **It needs no exception anywhere**, because nothing in this crate updates
/// an `events` row on any path: appends and imports insert, retention deletes,
/// and the per-stream counter is a different table — one that is upserted, so
/// the same trigger could not have been put there.
///
/// Deletion is deliberately left out. Retention removes events as its whole
/// purpose, so a `no_delete` trigger would have to be dropped and recreated
/// inside the one transaction that is allowed to delete. That is safe as far
/// as concurrency goes — SQLite rolls DDL back with the transaction, and the
/// `IMMEDIATE` write lock keeps everyone else out meanwhile — but it would
/// leave the guard switched off inside precisely the code most able to get
/// removal wrong, which is not a guard worth a ladder step.
const STEP_4: &str = "
    CREATE TRIGGER trg_events_no_update
    BEFORE UPDATE ON events
    BEGIN
        SELECT RAISE(ABORT, 'events is append-only: a stored event cannot be updated');
    END;
";

/// Step 5: export receipts.
///
/// An export is read off a reader and handed to the caller as a `Vec`; where
/// it goes from there — a file, another log — is the caller's, and so is
/// whether it got there. What the store can record is that a range was
/// handed out (`taken_ms`) and that the caller said it landed (`landed_ms`).
/// Two moments rather than one, because the second cannot be inferred from
/// the first: a row with `landed_ms` still `NULL` is an export that was
/// taken and never confirmed, and the retention guard treats it as if it had
/// not happened.
///
/// `whole` records whether the export's filter selected every event in its
/// range. A filtered export preserves some of the history and cannot vouch
/// for the rest, so only whole ones count towards what retention may remove.
///
/// Append-only, like the retention ledger, and read by [`crate::retention`]
/// as a chain: the ranges are exclusive on `from_position`, so page *k+1*'s
/// `from_position` is page *k*'s `through`, and the highest `through`
/// reachable from the beginning without a gap is how far the history is
/// known to be preserved.
const STEP_5: &str = "
    CREATE TABLE exports (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        taken_ms      INTEGER NOT NULL,
        from_position INTEGER NOT NULL,
        through       INTEGER NOT NULL,
        count         INTEGER NOT NULL,
        whole         INTEGER NOT NULL,
        landed_ms     INTEGER
    );
";

/// The ladder, in order. Index `i` moves `user_version` from `i` to `i + 1`.
///
/// A database that predates `position` would get its backfill as a step here:
/// add the column, then number the existing rows by `(epoch_ms, stream, seq)`
/// — wall clock first, because it approximates causal order across streams,
/// with `seq` breaking ties deterministically within one. No such database
/// exists yet, so no such step is written.
const LADDER: &[&str] = &[STEP_1, STEP_2, STEP_3, STEP_4, STEP_5];

/// Refuse a file that already carries a table under one of this crate's names.
///
/// Only meaningful at `user_version` 0, and only called there: from version 1
/// on, those tables are the ladder's own work and refusing them would refuse
/// every database this crate has ever written.
///
/// `sqlite_sequence` is deliberately not checked, although it is in
/// [`RESERVED_TABLES`]. SQLite creates it for any table with an
/// `AUTOINCREMENT` column and leaves it behind when that table is dropped, so
/// a file can hold it at `user_version` 0 with nothing foreign in it at all —
/// and `STEP_1` does not create it either, it asks for `AUTOINCREMENT` and
/// lets SQLite reuse the one already there. Its presence says nothing about a
/// foreign log, so it refuses nothing.
fn refuse_a_foreign_table(tx: &Transaction<'_>) -> Result<()> {
    let mut stmt = tx
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .map_err(classify)?;
    let present: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .map_err(classify)?
        .collect::<rusqlite::Result<_>>()
        .map_err(classify)?;
    drop(stmt);

    // Case-insensitively, as SQLite matches a table name.
    let taken = present.iter().find(|name| {
        RESERVED_TABLES
            .iter()
            .filter(|reserved| !reserved.eq_ignore_ascii_case("sqlite_sequence"))
            .any(|reserved| reserved.eq_ignore_ascii_case(name))
    });

    match taken {
        None => Ok(()),
        Some(name) => Err(Error::Unsupported(format!(
            "the file holds a table named `{name}` that this crate did not create \
             (`user_version` is 0, so the ladder has never run here). Rename it, \
             and rename or drop any index whose name the ladder uses, then open \
             the file and bring the old rows in through `import`. The README's \
             \"A table this crate did not create\" section is the whole sequence"
        ))),
    }
}

/// Bring `conn` up to [`TARGET_USER_VERSION`], one transaction per step.
///
/// Runs once at open and before any handle is issued, so no append is ever
/// served against a half-migrated schema. A database from a *newer* build is
/// refused rather than used: this build does not know what the extra steps
/// did, and writing under that assumption is how a log gets corrupted. So is a
/// file that already holds one of this crate's table names at `user_version` 0
/// — see `refuse_a_foreign_table`.
pub fn migrate(conn: &mut Connection) -> Result<()> {
    for (index, step) in LADDER.iter().enumerate() {
        let from = index as i64;

        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(classify)?;

        // Read the version *inside* the step's own transaction. Reading it
        // once up front and then opening a transaction per step leaves a
        // window: two connections opening the same fresh file both see 0, and
        // the loser runs `CREATE TABLE` against a schema that already has it,
        // failing with "table events already exists" — an error that reads
        // like corruption and is not classified `Busy`, so nothing retries it.
        // Under `IMMEDIATE` only one of them holds the write lock here, and
        // the other sees the version the winner committed.
        //
        // With that closed, the remaining way a reserved name is already taken
        // at version 0 is a table this crate did not create, which
        // `refuse_a_foreign_table` answers below — so the SQLite message is
        // not something a caller can reach by either route.
        let current: i64 = tx
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(classify)?;

        if current > TARGET_USER_VERSION {
            return Err(Error::storage(format!(
                "database is at user_version {current}, newer than this build's \
                 {TARGET_USER_VERSION}; refusing to write to a schema this build does not know"
            )));
        }
        if current != from {
            // Already applied, by an earlier run or by another connection that
            // got here first. Dropping the transaction rolls back a read that
            // changed nothing.
            continue;
        }

        if from == 0 {
            refuse_a_foreign_table(&tx)?;
        }

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
///
/// `auto_vacuum = INCREMENTAL` is set here rather than in the ladder because
/// SQLite only honours a change to it before the first table exists — after
/// that it takes a full `VACUUM`, which rewrites the file under an exclusive
/// lock. Setting it at creation is what makes
/// [`crate::retention`]'s reclaim a bounded, incremental operation instead.
/// On a database created before this line, the pragma is silently ignored and
/// reclaim has nothing to do; that is a limitation, not a failure.
pub fn apply_pragmas(conn: &Connection, busy_timeout: Duration) -> rusqlite::Result<()> {
    // Set first, because the batch below is the first thing on this
    // connection that can lose a lock — and not enough on its own, because
    // the busy handler the timeout installs is not always asked.
    // `journal_mode = WAL` reads the header's version bytes under a shared
    // lock and, finding them still saying rollback, writes them
    // (`sqlite3BtreeSetVersion`). A read promoted to a write inside one
    // statement is the case the `sqlite3_busy_handler` documentation exempts:
    // invoking the handler could leave two connections each waiting for the
    // other, so SQLite returns `SQLITE_BUSY` straight away and expects the
    // loser to let go and try again. Two connections opening one fresh file
    // put one of them there, and it is handed the error without the timeout
    // being waited at all.
    //
    // So the loser does what SQLite expects of it, within the bound the
    // caller set: retry the batch until the timeout has passed, and then
    // report the last failure. Whole, because all three pragmas are
    // idempotent — and splitting them would change nothing, the header write
    // is refused on its own statement just the same. A file already in WAL
    // finds the version bytes already set and never reaches the write.
    conn.busy_timeout(busy_timeout)?;

    let started = Instant::now();
    let mut backoff = Duration::from_millis(1);
    loop {
        let error = match conn.execute_batch(
            "PRAGMA auto_vacuum = INCREMENTAL; \
             PRAGMA journal_mode = WAL; \
             PRAGMA synchronous = NORMAL;",
        ) {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };

        let left = busy_timeout.saturating_sub(started.elapsed());
        if left.is_zero() || !is_contention(&error) {
            return Err(error);
        }
        // Clamped to what is left of the timeout, so the caller's bound is
        // the bound rather than the bound plus one backoff.
        std::thread::sleep(backoff.min(left));
        backoff = (backoff * 2).min(Duration::from_millis(50));
    }
}
