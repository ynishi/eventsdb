//! Coordinates: where an event sits in its stream, and in the database.
//!
//! There are two monotonic `u64`s in this system and they have different
//! scopes. `seq` counts within one stream; [`Position`] counts across every
//! stream of one database. Passing one where the other belongs produces a
//! read of the wrong range — a subscription that silently skips or repeats —
//! rather than a failure anyone would notice, so `Position` is a newtype and
//! `seq` stays a bare `u64` it cannot be confused with.

use serde_json::{Map, Value};

use crate::upcast::Current;

/// A global coordinate: the order an event was committed in, across every
/// stream of one database.
///
/// Positions are dense and gap-free *as read*: a reader never observes
/// `n + 1` while `n` is still uncommitted, so a subscription is a plain
/// `position > cursor` range read with no grace window.
///
/// The mechanism is narrower than "one writer", and worth stating precisely
/// because the narrower version is also stronger. The position is allocated
/// **inside the transaction that commits it**, and that transaction holds
/// SQLite's write lock from `BEGIN`, because every write the backend makes is
/// `IMMEDIATE`. Allocation order and commit order therefore cannot diverge —
/// and that argument does not depend on there being one connection. It holds
/// for two processes on one file, and it is measured: 120 appends through two
/// separately-opened logs come back as exactly `1..=120`, in order.
///
/// What a second connection *does* cost is the wake-up, not the order — see
/// [`crate::log::EventLog::subscribe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Position(u64);

impl Position {
    /// Before the first event. Where a consumer with no checkpoint starts.
    pub const BEGINNING: Position = Position(0);

    pub const fn new(value: u64) -> Self {
        Position(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// The value as SQLite stores it, or `None` when it does not fit.
    ///
    /// A position is a rowid, so every position the store ever assigns is in
    /// range. [`Position::new`] is public, though, and a `u64` above
    /// `i64::MAX` would bind as a negative number: `position > -1` reads the
    /// whole log rather than nothing, and a checkpoint written from one would
    /// silently replay everything through the exactly-once path. Callers that
    /// bind a position use this and refuse rather than wrap.
    pub fn as_stored(self) -> Option<i64> {
        i64::try_from(self.0).ok()
    }
}

impl std::fmt::Display for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// What a write returned: the coordinates the store assigned it.
///
/// A caller never reads back to learn where its event landed, and never
/// supplies these fields — they are the store's to give.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Committed {
    /// Per-stream sequence, from 1.
    pub seq: u64,
    /// Wall clock at append time.
    pub epoch_ms: u64,
    /// Global coordinate, or `None` from a backend that keeps one stream and
    /// therefore has no database-wide order to place the event in.
    pub position: Option<Position>,
}

/// An event read back from the log, with where it sits.
///
/// The `event` has already been through the upcaster chain, so a reader sees
/// the current shape whatever version the bytes were written under.
#[derive(Debug, Clone)]
pub struct Recorded {
    pub position: Position,
    pub stream: String,
    pub event: Current,
}

impl Recorded {
    /// The event's `kind`, as it reads after upcasting.
    pub fn kind(&self) -> &str {
        self.event.kind()
    }

    /// The per-stream sequence.
    pub fn seq(&self) -> u64 {
        self.event.seq()
    }

    /// The underlying object.
    pub fn into_inner(self) -> Map<String, Value> {
        self.event.into_inner()
    }
}
