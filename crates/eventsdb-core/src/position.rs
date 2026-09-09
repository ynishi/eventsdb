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
/// Positions are dense and gap-free *as read*: the backend allocates inside
/// the same transaction that commits, under a single writer, so a reader
/// never observes `n + 1` while `n` is still uncommitted. A subscription is
/// therefore a plain `position > cursor` range read with no grace window.
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
