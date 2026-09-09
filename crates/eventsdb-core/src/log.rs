//! The database-level SPI: one level above a stream.
//!
//! [`crate::store::EventStore`] is scoped to a single stream and stays that
//! way. Everything that is a question about the database as a whole lives
//! here: reading across streams, following the log as it grows, and
//! remembering how far a consumer got.

use async_trait::async_trait;
use futures_core::stream::BoxStream;

use crate::error::Result;
use crate::position::{Position, Recorded};
use crate::store::EventStore;

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
    /// Restrict to one stream. `None` reads every stream.
    pub stream: Option<String>,
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
            stream: None,
        }
    }

    pub fn stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    /// Whether this filter can match anything at all. An empty kind list
    /// cannot, and a backend can skip the query entirely.
    pub fn selects_nothing(&self) -> bool {
        self.kinds.as_ref().is_some_and(|kinds| kinds.is_empty())
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
    /// SQLite backend wakes in-process subscribers directly and falls back to
    /// polling for writers in another process, because SQLite has no
    /// cross-process notification.
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
}
