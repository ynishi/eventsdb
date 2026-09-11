//! A consistent physical copy of the file, taken while the log is open.
//!
//! # The physical copy and the logical one are different answers
//!
//! [`eventsdb_core::EventLog::export`] is the logical copy and is deliberately
//! only the events: the read models a projection built, the `checkpoints` it
//! advanced, the retention ledger and the export receipts all stay behind.
//! That is what makes an export survive a change of backend, and it is also
//! what makes restoring from one mean rebuilding every projection and losing
//! the ledger — the only remaining evidence of what retention removed.
//!
//! This is the other answer: every page of the file, read models and reserved
//! tables included, in SQLite's own format. What `cp` would give, except that
//! `cp` on an open WAL database does not give it — the `-wal` file holds
//! committed pages the main file does not yet, and a copy taken between two
//! writes can carry half a transaction.
//!
//! # Why the online backup API and not `VACUUM INTO`
//!
//! Both are one read transaction over the whole file, so both are consistent
//! by construction. They were measured against a log this crate had created,
//! which stands at `user_version` 5 with `auto_vacuum` 2 (`INCREMENTAL`):
//!
//! ```text
//!   copy taken by                user_version  auto_vacuum  journal_mode  on a reader
//!   ───────────────────────────  ────────────  ───────────  ────────────  ───────────
//!   VACUUM INTO 'dest'                      5            2  delete        no
//!   sqlite3_backup_step(-1)                 5            2  wal           yes
//! ```
//!
//! The pragmas were the criterion, and they came out a tie: both carry the
//! `auto_vacuum = INCREMENTAL` that [`SqliteEventLog::reclaim`] depends on and
//! that SQLite accepts only before the first table exists, so on either copy
//! reclaim is the bounded operation it is here rather than a silent no-op.
//!
//! What decided it is the last column. A reader connection is opened
//! `SQLITE_OPEN_READ_ONLY` *and* pinned with `PRAGMA query_only = 1`, and
//! `VACUUM INTO` on one fails with `SQLITE_READONLY`, "attempt to write a
//! readonly database". The pragma is what refuses it rather than the flag —
//! measured both ways round, and a read-only connection without `query_only`
//! runs the statement fine. That the destination is a different file does not
//! enter into it. So `VACUUM INTO` could only be had by moving the copy to the
//! writer, where it would hold every append behind it for the duration, or by
//! switching the reader's guard off around it. The backup API asks the source
//! connection for nothing but reads and runs on the reader as it is
//! configured.
//!
//! The one step is [`Backup::step`] with `-1`, not `run_to_completion`:
//! rusqlite's `run_to_completion` asserts its `pages_per_step` is positive and
//! panics on `-1`, so the call that copies every remaining page in one step is
//! the raw one. It matters that it is one step. The backup API restarts a copy
//! that is interrupted by a write from another connection, and this crate has
//! a writer on another thread by design; a stepped copy that yielded between
//! steps is the shape that can restart for ever on a busy log. One step holds
//! one read transaction from the first page to the last and has nothing to
//! restart into.

use std::path::Path;

use eventsdb_core::error::{Error, Result};
use rusqlite::backup::{Backup, StepResult};
use rusqlite::Connection;

use crate::log::SqliteEventLog;
use crate::shared::{classify, map_isle};

impl SqliteEventLog {
    /// Copy the whole database to `path`, as one consistent snapshot.
    ///
    /// # What travels
    ///
    /// Everything in the file: the events, `stream_seq`, `checkpoints`, the
    /// retention ledger, the export receipts, every index, the append-only
    /// trigger, the read-model tables projections created through
    /// [`SqliteEventLog::runner`], and `user_version` and `auto_vacuum` with
    /// them. The copy is a database this crate created, so it opens with
    /// [`SqliteEventLog::open`], the migration ladder finds it already at
    /// [`crate::TARGET_USER_VERSION`], and a projection resumes from its
    /// checkpoint rather than rebuilding.
    ///
    /// # What this is not
    ///
    /// Not a **restore**: nothing here writes into the live file. Restoring is
    /// opening the copy — it is a log like any other, and appending to it
    /// continues from the position and the per-stream counters the source had.
    ///
    /// Not an **export**. [`eventsdb_core::EventLog::export`] is the logical
    /// copy: events only, backend-neutral, and it survives a change of storage
    /// format. This is the file, in the file format the file is in. See the
    /// module doc for the trade between them.
    ///
    /// # Which connection, and a write in flight
    ///
    /// It runs on a reader connection, like every other read here, so the copy
    /// does not queue behind the writer and the writer does not queue behind
    /// the copy. A reader sees committed data, which is what a backup is: a
    /// [`SqliteEventLog::with_transaction`] or a projection batch that holds
    /// the write lock while this runs is **not** refused and is **not**
    /// waited for. The copy holds what had committed when it started, and the
    /// transaction in flight lands in the source afterwards, where it belongs.
    ///
    /// # The destination must not exist
    ///
    /// The path is the caller's, as an export's is, and it is taken with an
    /// exclusive create: a path that already holds anything at all — a
    /// previous backup most of all — is refused with [`Error::Validation`]
    /// rather than overwritten. The backup API itself would overwrite it
    /// silently, which is a shape that loses data on a typo. A failed copy
    /// removes the file this call created, so retrying to the same path is not
    /// refused by a leftover of the attempt before it.
    ///
    /// The copy is not recorded anywhere. The `exports` table is for pages of
    /// events and does not fit one, and the file is its own record.
    ///
    /// ```no_run
    /// # use eventsdb_sqlite::SqliteEventLog;
    /// # async fn example() -> eventsdb_core::Result<()> {
    /// let log = SqliteEventLog::open("events.db").await?;
    /// log.backup_to("events-backup.db").await?;
    ///
    /// // Restoring is opening the copy.
    /// let restored = SqliteEventLog::open("events-backup.db").await?;
    /// # let _ = restored;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn backup_to(&self, path: impl AsRef<Path>) -> Result<()> {
        let destination = path.as_ref().to_path_buf();
        claim(&destination)?;

        let shared = self.shared_handle();
        let target = destination.clone();
        let outcome = match shared
            .reader()
            .call(move |conn: &mut Connection| Ok(copy_pages(conn, &target)))
            .await
        {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        };

        if outcome.is_err() {
            // What is at the destination now is the empty file this call made
            // or a half-written copy of the log, and neither is a backup.
            // Best effort: if the removal fails too, the path holds a partial
            // file and the next call refuses it, which is the honest outcome.
            let _ = std::fs::remove_file(&destination);
        }
        outcome
    }
}

/// Take the destination path, or refuse because something is already there.
///
/// `create_new` rather than a `try_exists` and then an open: the check and the
/// creation are one syscall, so two backups racing for one path cannot both
/// pass the check. The zero-byte file it leaves is a fresh empty database as
/// far as SQLite is concerned, which is exactly what the copy wants to open.
fn claim(destination: &Path) -> Result<()> {
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(Error::validation(format!(
                "`{}` already exists, so the backup was not taken. The destination \
                 is a path this call creates: overwriting one would destroy \
                 whatever is there, and a previous backup is the likeliest thing \
                 to be there. Name a path that is free, or remove this one first",
                destination.display()
            )))
        }
        Err(error) => Err(Error::storage(format!(
            "could not create `{}` for the backup: {error}",
            destination.display()
        ))),
    }
}

/// The copy itself, against the reader's open connection.
///
/// Opens the destination on this thread, so the whole copy is one job on the
/// isle and the destination connection is closed before the job returns.
fn copy_pages(source: &Connection, destination: &Path) -> Result<()> {
    let mut into = Connection::open(destination).map_err(classify)?;
    let backup = Backup::new(source, &mut into).map_err(classify)?;

    match backup.step(-1).map_err(classify)? {
        StepResult::Done => Ok(()),
        // The source is read-only and the destination was created by this
        // call, so neither lock is one anybody else should hold — but
        // "somebody does" is contention, and another attempt is worth making
        // once the destination the failure removed is free again.
        StepResult::Busy | StepResult::Locked => Err(Error::Busy(format!(
            "the backup to `{}` could not take a lock",
            destination.display()
        ))),
        // `StepResult::More` is the one left, and `-1` is "every remaining
        // page", so there is nothing more to ask for. Reported rather than
        // looped over: a second step would be a second read transaction, and
        // one transaction from first page to last is the whole reason this is
        // one step. The arm is a wildcard because `StepResult` is
        // `#[non_exhaustive]`, so a variant added upstream lands here too —
        // as an unfinished copy, which is what it would be.
        other => Err(Error::storage(format!(
            "the backup to `{}` asked for every remaining page and SQLite \
             answered {other:?}",
            destination.display()
        ))),
    }
}
