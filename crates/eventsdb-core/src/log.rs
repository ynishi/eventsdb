//! The database-level SPI: one level above a stream.
//!
//! [`crate::store::EventStore`] is scoped to a single stream and stays that
//! way. Everything that is a question about the database as a whole lives
//! here: reading across streams, following the log as it grows, remembering
//! how far a consumer got — and moving the whole thing somewhere else.
//!
//! That last one is on the trait rather than on a backend on purpose. A log
//! that cannot be moved is a log its owner cannot leave, so being movable is a
//! claim this crate makes, not a convenience one implementation happens to
//! offer. A caller generic over `EventLog` can therefore write a migration.

use async_trait::async_trait;
use futures_core::stream::BoxStream;

use crate::error::{Error, Result};
use crate::position::{Position, Recorded};
use crate::store::EventStore;
use crate::transfer::{ExportedEvent, ImportReport};

/// Which events a cross-stream read or a subscription wants.
///
/// Both filters are matched on the **stored** kind and the stored stream
/// name, before the upcaster chain runs — the same rule
/// [`EventStore::read_kinds`] follows, for the same reason.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// Kinds to include. `None` includes every kind; an empty vector selects
    /// nothing, which is the honest reading of "include these" given none.
    pub kinds: Option<Vec<String>>,
    /// Streams to include. `None` reads every stream; an empty vector selects
    /// nothing, the same reading `kinds` gets.
    ///
    /// **A set, not one name.** Restricting to a single stream is the common
    /// case and [`Filter::stream`] still spells it, but the question a caller
    /// actually has is often about a *group* — the streams of one session, of
    /// one tenant, of one run. Given only a single-stream filter that has to
    /// be answered with one read per stream and a merge in the caller, which
    /// loses the position order the log exists to provide.
    pub streams: Option<Vec<String>>,
}

impl Filter {
    pub fn all() -> Self {
        Filter::default()
    }

    pub fn kinds<I, S>(kinds: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Filter {
            kinds: Some(kinds.into_iter().map(Into::into).collect()),
            streams: None,
        }
    }

    /// Restrict to one stream.
    ///
    /// Replaces whatever set was there, rather than adding to it: reading it
    /// as "and also this one" would make `.stream("a").stream("b")` mean
    /// something no reader would guess from the singular name.
    pub fn stream(mut self, stream: impl Into<String>) -> Self {
        self.streams = Some(vec![stream.into()]);
        self
    }

    /// Restrict to a set of streams.
    ///
    /// An empty set selects nothing, which is what "include these" given none
    /// says. A caller assembling a set from somewhere that can legitimately
    /// come back empty should check before asking.
    pub fn streams<I, S>(mut self, streams: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.streams = Some(streams.into_iter().map(Into::into).collect());
        self
    }

    /// Whether this filter can match anything at all. An empty list of either
    /// kind cannot, and a backend can skip the query entirely.
    pub fn selects_nothing(&self) -> bool {
        self.kinds.as_ref().is_some_and(|kinds| kinds.is_empty())
            || self
                .streams
                .as_ref()
                .is_some_and(|streams| streams.is_empty())
    }
}

#[async_trait]
pub trait EventLog: Send + Sync {
    /// A handle on one stream. Everything below this is the per-stream SPI,
    /// unchanged.
    async fn stream(&self, id: &str) -> Result<Box<dyn EventStore>>;

    /// Events with `position > from`, across every stream, in position order,
    /// at most `limit`.
    ///
    /// Exclusive on `from` so a cursor can be fed straight back in:
    /// [`Position::BEGINNING`] reads from the start, and the position of the
    /// last event handled reads the next batch.
    async fn read_all(
        &self,
        from: Position,
        filter: &Filter,
        limit: usize,
    ) -> Result<Vec<Recorded>>;

    /// The newest position in the log, or [`Position::BEGINNING`] if it is
    /// empty.
    async fn head_position(&self) -> Result<Position>;

    /// Catch up from `from`, then stay live.
    ///
    /// There is no seam a consumer has to handle: the live tail is the same
    /// range read, resumed. Ordering is by position and nothing is skipped —
    /// see [`Position`] for why that holds without gap detection.
    ///
    /// How the live half learns of a write is the backend's business. The
    /// SQLite backend wakes subscribers on **the same log** directly, and
    /// falls back to polling for anything else, because SQLite has no
    /// notification a writer elsewhere could send.
    ///
    /// "Anything else" includes a second log opened on the same file in this
    /// same process — the wake-up channel belongs to the log, not to the
    /// database. Nothing is lost either way; the difference is latency, and it
    /// is about three orders of magnitude [measured: 552µs woken directly
    /// against 552ms on a 600ms poll, `tests/two_logs.rs`]. Open the file once
    /// per process and share the log if that matters.
    fn subscribe(
        &self,
        from: Position,
        filter: Filter,
    ) -> Result<BoxStream<'static, Result<Recorded>>>;

    /// How far `consumer` has got, or [`Position::BEGINNING`] if it has never
    /// reported.
    async fn checkpoint_load(&self, consumer: &str) -> Result<Position>;

    /// Record how far `consumer` has got.
    ///
    /// Callers that need the checkpoint to move in the same transaction as
    /// the work it accounts for must not use this — it is its own write. That
    /// is what a projection runner is for.
    async fn checkpoint_save(&self, consumer: &str, at: Position) -> Result<()>;

    /// Read events out in position order, **as they are stored**.
    ///
    /// The one read that does not run the upcaster chain. Every other read
    /// wants the current shape; a transfer wants the bytes, so the receiving
    /// log can hold exactly what this one held and run its own chain over
    /// them. Upcasting on the way out would bake this build's reading of an
    /// old event into the copy and lose the original.
    ///
    /// Page with `from` and `limit`, feeding the last returned position back
    /// in; a short batch is the end. See [`crate::transfer`] for what travels
    /// and what the receiving log reassigns.
    ///
    /// The default declines, because a log with no stored form has nothing to
    /// hand over that another log could hold.
    async fn export(
        &self,
        from: Position,
        filter: &Filter,
        limit: usize,
    ) -> Result<Vec<ExportedEvent>> {
        let _ = (from, filter, limit);
        Err(Error::Unsupported(
            "this log cannot hand over its stored events".to_string(),
        ))
    }

    /// Write exported events into this log, in the order given.
    ///
    /// `seq` and `position` are this log's to assign; everything else travels
    /// unchanged, `epoch_ms` and `_schema_version` included. Keeping the
    /// version is what leaves an old event within reach of the upcaster
    /// written for it, and
    /// [`ImportReport::reproduced_coordinates`] reports whether the batch
    /// landed where it came from, so a migration can check rather than assume.
    ///
    /// The default declines rather than appending one at a time. A backend
    /// with no transaction could only offer a partial import, and a transfer
    /// that stopped half way is worse than one that refused: from the outside
    /// there is no way to tell how far it got.
    async fn import(&self, events: Vec<ExportedEvent>) -> Result<ImportReport> {
        let _ = events;
        Err(Error::Unsupported(
            "this log cannot take in exported events as one write".to_string(),
        ))
    }
}
