//! What the log and every stream handle it hands out have in common.

use std::sync::Arc;
use std::time::Duration;

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
}

impl Shared {
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
        if matches!(
            inner.code,
            ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked
        ) {
            return Error::Busy(error.to_string());
        }
    }
    Error::Storage(error.to_string())
}

/// Map a failure of the isle itself — not of the SQL it ran.
pub(crate) fn map_isle(error: IsleError) -> Error {
    match error {
        IsleError::Sqlite(inner) => classify(inner),
        other if other_is_busy(&other) => Error::Busy(other.to_string()),
        IsleError::Closed => Error::Storage("store is closed".to_string()),
        other => Error::Storage(other.to_string()),
    }
}

fn other_is_busy(error: &IsleError) -> bool {
    error.is_busy()
}
