//! Projections: read models built from the log, exactly once.
//!
//! # Why this lives with the backend rather than above it
//!
//! The whole value of a projection here is that applying an event and
//! advancing the consumer's checkpoint happen in **one** transaction. That is
//! only expressible if the projection writes through the same connection the
//! log is read on — so a projection's `apply` takes a [`Transaction`], which
//! is a backend type. A version of this that sat on the abstract
//! [`eventsdb_core::EventLog`] would hand the projection events that had
//! already been read and committed, and the checkpoint would be a second
//! write that could fail on its own. That is at-least-once, and it is exactly
//! what every consumer of a networked event store has to live with.
//!
//! So the coupling is not an oversight. It is the property.
//!
//! # What a runner guarantees
//!
//! - **Exactly-once.** A batch either applies and moves the cursor, or does
//!   neither. There is no dedupe table and no idempotence requirement on the
//!   projection author.
//! - **In order.** Events arrive in `position` order, which is the order they
//!   were committed in, with nothing skipped.
//! - **Resumable.** The cursor is in the same database as the log, so a
//!   process that stops mid-catch-up resumes where it stopped.

use std::sync::Arc;

use eventsdb_core::error::{Error, Result};
use eventsdb_core::log::Filter;
use eventsdb_core::position::{Position, Recorded};
use rusqlite::{Connection, Transaction, TransactionBehavior};

use crate::log::{load_checkpoint, save_checkpoint, select_recorded};
use crate::shared::{classify, map_isle, Shared};

/// How many events a runner applies per transaction unless told otherwise.
pub const DEFAULT_BATCH: usize = 256;

/// A read model built from the log.
///
/// Every method that writes is handed the transaction the log is being read
/// on. Writing anywhere else — another connection, another file, a network
/// call — gives up the one guarantee this is for.
pub trait Projection: Send + 'static {
    /// The consumer name the cursor is stored under. Stable across restarts,
    /// because it *is* the identity of the cursor.
    fn name(&self) -> &str;

    /// Which kinds this projection folds. `None` sees every kind.
    ///
    /// Narrowing here is not only a filter: it is what lets the read use the
    /// `(kind, position)` index instead of walking the log.
    fn kinds(&self) -> Option<Vec<String>> {
        None
    }

    /// Create the read model's tables. Idempotent — it runs on every
    /// [`ProjectionRunner::init`] and on every rebuild.
    fn init(&mut self, tx: &Transaction<'_>) -> Result<()> {
        let _ = tx;
        Ok(())
    }

    /// Empty the read model, for a rebuild. Whatever `init` created, this
    /// returns to its initial state.
    fn reset(&mut self, tx: &Transaction<'_>) -> Result<()>;

    /// Fold one event into the read model.
    fn apply(&mut self, tx: &Transaction<'_>, event: &Recorded) -> Result<()>;
}

/// Drives a [`Projection`] over a log.
pub struct ProjectionRunner<P: Projection> {
    shared: Arc<Shared>,
    /// `None` only after a call failed at the isle boundary and the
    /// projection could not be handed back — see [`ProjectionRunner::lost`].
    projection: Option<P>,
    batch: usize,
}

impl<P: Projection> ProjectionRunner<P> {
    pub(crate) fn new(shared: Arc<Shared>, projection: P) -> Self {
        ProjectionRunner {
            shared,
            projection: Some(projection),
            batch: DEFAULT_BATCH,
        }
    }

    /// How many events one transaction covers.
    ///
    /// Larger batches mean fewer commits and a longer write lock; smaller
    /// ones mean the opposite. Nothing about correctness changes.
    pub fn with_batch(mut self, batch: usize) -> Self {
        self.batch = batch.max(1);
        self
    }

    /// Give the projection back once the runner is done with it.
    pub fn into_inner(mut self) -> Result<P> {
        self.projection.take().ok_or_else(Self::lost)
    }

    fn lost() -> Error {
        Error::storage(
            "the runner's projection was dropped by a failed call and cannot be used again",
        )
    }

    /// Create the read model's tables.
    pub async fn init(&mut self) -> Result<()> {
        self.in_transaction(|projection, tx, _| projection.init(tx))
            .await
    }

    /// Apply at most one batch. Returns how many events were applied — `0`
    /// means the projection is caught up.
    pub async fn run_once(&mut self) -> Result<usize> {
        let batch = self.batch;
        self.in_transaction(move |projection, tx, chain| {
            let name = projection.name().to_string();
            let filter = Filter {
                kinds: projection.kinds(),
                stream: None,
            };

            let cursor = load_checkpoint(tx, &name)?;
            let events = select_recorded(tx, chain, cursor, &filter, batch)?;
            if events.is_empty() {
                return Ok(0);
            }

            let mut last = cursor;
            for event in &events {
                projection.apply(tx, event)?;
                last = event.position;
            }
            // Same transaction as the applies above. This is the exactly-once
            // claim, and it is one line.
            save_checkpoint(tx, &name, last)?;
            Ok(events.len())
        })
        .await
    }

    /// Run batches until the projection is caught up. Returns the total
    /// applied.
    ///
    /// Each batch is its own transaction, so a failure part-way keeps
    /// everything the earlier batches did and leaves the cursor where the
    /// last successful one put it.
    pub async fn catch_up(&mut self) -> Result<usize> {
        let mut total = 0;
        loop {
            let applied = self.run_once().await?;
            if applied == 0 {
                return Ok(total);
            }
            total += applied;
        }
    }

    /// Empty the read model, rewind the cursor, and replay from the start.
    ///
    /// The reset and the rewind share one transaction, so the model is never
    /// left emptied against a cursor that says it is up to date. The replay
    /// that follows is batched, so a reader looking during a rebuild can see a
    /// partially rebuilt model — which is the price of not holding a write
    /// lock over the whole log.
    pub async fn rebuild(&mut self) -> Result<usize> {
        self.in_transaction(|projection, tx, _| {
            let name = projection.name().to_string();
            projection.reset(tx)?;
            projection.init(tx)?;
            save_checkpoint(tx, &name, Position::BEGINNING)
        })
        .await?;
        self.catch_up().await
    }

    /// Where this projection's cursor currently sits.
    pub async fn position(&mut self) -> Result<Position> {
        self.in_transaction(|projection, tx, _| {
            let name = projection.name().to_string();
            load_checkpoint(tx, &name)
        })
        .await
    }

    /// Run `body` inside one `IMMEDIATE` transaction on the isle, with the
    /// projection moved across and handed back.
    ///
    /// The projection has to travel into the closure because the isle's job
    /// is `'static` — it cannot borrow from `self`. It comes back in the
    /// return value, which is why every method here goes through this one
    /// place rather than repeating the dance.
    async fn in_transaction<T, Body>(&mut self, body: Body) -> Result<T>
    where
        T: Send + 'static,
        Body: FnOnce(&mut P, &Transaction<'_>, &eventsdb_core::upcast::UpcastChain) -> Result<T>
            + Send
            + 'static,
    {
        let mut projection = self.projection.take().ok_or_else(Self::lost)?;
        let chain = self.shared.chain.clone();

        let job = move |conn: &mut Connection| {
            let outcome = (|| {
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(classify)?;
                let value = body(&mut projection, &tx, &chain)?;
                tx.commit().map_err(classify)?;
                Ok(value)
            })();
            Ok((projection, outcome))
        };

        match self.shared.isle.call(job).await {
            Ok((projection, outcome)) => {
                self.projection = Some(projection);
                outcome
            }
            Err(isle) => Err(map_isle(isle)),
        }
    }
}
