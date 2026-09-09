//! What the log and every stream handle it hands out have in common.

use std::sync::Arc;
use std::time::Duration;

use std::sync::atomic::{AtomicUsize, Ordering};

use eventsdb_core::error::Error;
use eventsdb_core::position::Position;
use eventsdb_core::upcast::UpcastChain;
use rusqlite::ErrorCode;
use rusqlite_isle::{AsyncIsle, IsleError};
use tokio::sync::watch;

/// How many times a write is re-submitted when it loses the lock race.
///
/// Only for contention. An `append_if` is never retried whatever this says —
/// its decision is `FnOnce`, and a second attempt would need a second
/// decision.
pub(crate) const MAX_BUSY_RETRIES: u32 = 4;

/// Backoff before retry `attempt` (1-based).
pub(crate) fn backoff(attempt: u32) -> Duration {
    Duration::from_millis(2u64.saturating_pow(attempt).saturating_mul(5))
}

pub(crate) struct Shared {
    pub isle: AsyncIsle,
    /// Read-only connections, for statements that do not need the write
    /// transaction.
    ///
    /// Without these every read queues behind every write on the one thread,
    /// which throws away the property WAL exists to provide. Measured before
    /// they existed: a read that takes 73µs against an idle log took **8.2
    /// seconds** while a write transaction was open, because it was not
    /// waiting for the database — it was waiting for the queue.
    ///
    /// Empty for an in-memory log: each `:memory:` open is its own distinct
    /// database, so a second connection would not see the first one's data at
    /// all. Reads fall back to the writer there, which is correct and, with
    /// nothing on disk, cheap.
    pub readers: Vec<AsyncIsle>,
    /// Round-robin cursor over `readers`.
    pub next_reader: AtomicUsize,
    /// Identity of the database, not a path to take apart. Two handles are on
    /// the same database exactly when these compare equal.
    pub database: String,
    pub chain: UpcastChain,
    /// The newest committed position, for waking in-process subscribers.
    pub notify: watch::Sender<Position>,
    /// How often a subscriber re-reads when nothing woke it. This is what
    /// covers a writer in *another* process: SQLite has no cross-process
    /// notification, so the only way to see such a write is to look.
    pub poll_interval: Duration,
    /// Projection names with a live runner on this log.
    ///
    /// A projection's name is the primary key of its row in `checkpoints`, so
    /// two runners answering the same name share one cursor without either
    /// knowing: each advances it past events the other has not folded, and
    /// both quietly stop being exactly-once. Holding the names makes the
    /// second one a refusal instead.
    pub live_runners: std::sync::Mutex<std::collections::HashSet<String>>,
}

impl Shared {
    /// The isle a read should go to: the next reader, or the writer when there
    /// are none.
    ///
    /// Reads that must see uncommitted work — anything inside a write
    /// transaction, which is every projection fold and every decide-then-append
    /// — must **not** come through here. A reader connection sees only what has
    /// committed, which is the right answer for a standalone read and the wrong
    /// one for a read the same transaction is about to write against.
    pub fn reader(&self) -> &AsyncIsle {
        if self.readers.is_empty() {
            return &self.isle;
        }
        let next = self.next_reader.fetch_add(1, Ordering::Relaxed);
        &self.readers[next % self.readers.len()]
    }

    /// Announce a commit to in-process subscribers.
    ///
    /// The compare and the store happen under the channel's own lock. Reading
    /// with `borrow()` and then calling `send_replace` is two steps, and two
    /// concurrent appends can interleave so the smaller value lands last,
    /// walking the published value backwards. Nothing reads it today —
    /// subscribers wait on `changed()` and re-query — but a value that can go
    /// backwards is a trap for whoever reads it next.
    ///
    /// It sends even with no subscribers attached, so one arriving later
    /// starts from the right place rather than waiting for the next write.
    pub fn publish(self: &Arc<Self>, position: Position) {
        self.notify.send_if_modified(|current| {
            if position > *current {
                *current = position;
                true
            } else {
                false
            }
        });
    }
}

/// Which of our error classes a rusqlite failure belongs to.
///
/// Busy and locked are contention: another call is worth making. Everything
/// else is the database failing, and repeating it would only fail again.
pub(crate) fn classify(error: rusqlite::Error) -> Error {
    if let rusqlite::Error::SqliteFailure(inner, _) = &error {
        match inner.code {
            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked => {
                return Error::Busy(error.to_string())
            }
            // The isle interrupts a statement that passed its deadline, and
            // SQLite reports that as `SQLITE_INTERRUPT`. It is the caller's own
            // deadline arriving, not the database failing.
            ErrorCode::OperationInterrupted => return Error::Timeout(error.to_string()),
            _ => {}
        }
    }
    Error::Storage(error.to_string())
}

/// Map a failure of the isle itself — not of the SQL it ran.
pub(crate) fn map_isle(error: IsleError) -> Error {
    match error {
        IsleError::Sqlite(inner) => classify(inner),
        IsleError::Timeout => Error::Timeout("the isle reported a deadline".to_string()),
        other if other_is_busy(&other) => Error::Busy(other.to_string()),
        IsleError::Closed => Error::Storage("store is closed".to_string()),
        other => Error::Storage(other.to_string()),
    }
}

fn other_is_busy(error: &IsleError) -> bool {
    error.is_busy()
}
