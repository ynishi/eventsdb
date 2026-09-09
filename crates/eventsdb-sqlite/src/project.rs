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

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use eventsdb_core::error::{Error, Result};
use eventsdb_core::log::Filter;
use eventsdb_core::position::{Position, Recorded};
use rusqlite::{Connection, Transaction, TransactionBehavior};

use crate::hatch::{guarded, map_denial};
use crate::log::{head_position_in, load_checkpoint, save_checkpoint, select_recorded};
use crate::retention::require_complete_from;
use crate::shared::{classify, map_isle, Shared};
use crate::txn::{Trusted, Untrusted};

/// How many events a runner applies per transaction unless told otherwise.
pub const DEFAULT_BATCH: usize = 256;

/// The runner's own view of its transaction.
///
/// It derefs to the [`Transaction`] for the runner's own bookkeeping — reading
/// and writing `checkpoints`, which is a reserved table and therefore needs
/// the guard's trust — and [`RunnerTxn::callback`] is the one way into the
/// projection, which drops that trust for the duration of the call.
///
/// The projection is a public trait, so its body is caller code. Before this
/// existed it was handed a transaction with no authorizer installed at all: a
/// second, unguarded door onto `events`, `checkpoints` and `retention`,
/// sitting beside the one the hatch carefully closes.
pub(crate) struct RunnerTxn<'t> {
    pub(crate) tx: &'t Transaction<'t>,
    pub(crate) trusted: &'t AtomicBool,
}

impl<'t> std::ops::Deref for RunnerTxn<'t> {
    type Target = Transaction<'t>;

    fn deref(&self) -> &Self::Target {
        self.tx
    }
}

impl RunnerTxn<'_> {
    /// Call into the projection with the guard's trust dropped, so its body is
    /// held to the same rules as a hatch closure: its own tables, yes; the
    /// log's, no.
    fn callback<R>(&self, body: impl FnOnce(&Transaction<'_>) -> Result<R>) -> Result<R> {
        let _untrusted = Untrusted::lower(self.trusted);
        body(self.tx)
    }
}

/// A read model built from the log.
///
/// Every method that writes is handed the transaction the log is being read
/// on. Writing anywhere else — another connection, another file, a network
/// call — gives up the one guarantee this is for.
///
/// That transaction is guarded: your own tables are yours, and the log's are
/// refused, exactly as in [`crate::SqliteEventLog::with_transaction`]. A fold
/// is not a place to append events or move another consumer's cursor from, and
/// a public trait handed an unguarded transaction is a second door onto the
/// invariants the rest of the crate maintains.
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

    /// Whether this projection is still correct when part of the history it
    /// would fold has been removed by retention.
    ///
    /// `false` by default, and that default is the important one: a total —
    /// a balance, a count, a sum — computed over a log missing its front is
    /// simply wrong, and nothing about the result says so. The runner refuses
    /// rather than produce it (see [`crate::retention`]).
    ///
    /// Return `true` only for a projection whose answer does not depend on
    /// the removed range: a "last 30 days" view, a latest-value-per-stream
    /// table, anything that would overwrite rather than accumulate.
    fn tolerates_truncation(&self) -> bool {
        false
    }
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
    /// Larger batches mean fewer commits and a longer write lock; smaller ones
    /// mean the opposite. Nothing about correctness changes.
    ///
    /// The lock matters because the batch read stays on the writer — it has to
    /// share a transaction with the completeness check, or retention could
    /// remove events between the check and the batch it vouched for. So this is
    /// the knob that bounds how long a fold holds the write lock, and the
    /// reason the default is a number rather than "everything" [measured: a
    /// 256-event batch took 175 ms while a concurrent read took 538 µs,
    /// `tests/lock_hold.rs`, debug build].
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
        self.in_transaction(|projection, tx, _| tx.callback(|raw| projection.init(raw)))
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
            // Inside the same transaction as the read, so retention cannot
            // land between the check and the batch it vouches for.
            if !projection.tolerates_truncation() {
                require_complete_from(tx, cursor)?;
            }

            // Read before the batch, so "the batch was short, therefore
            // everything up to here was scanned" stays true whatever else the
            // database does.
            let head = head_position_in(tx)?;
            let events = select_recorded(tx, chain, cursor, &filter, batch)?;
            let full = events.len() == batch;
            let applied = events.len();

            let mut last = cursor;
            for event in &events {
                tx.callback(|raw| projection.apply(raw, event))?;
                last = event.position;
            }

            // How far the projection has *seen*, which is not how far it has
            // applied. A projection that names its kinds declines everything
            // else, and a cursor that only moved on matches would park at the
            // last match for ever: it would report itself behind when it is
            // not, block retention through `Guard::RegisteredConsumers`, and
            // re-scan the same prefix on every poll.
            //
            // A short batch means the filtered range is exhausted, so
            // everything up to `head` has been offered and declined. A full
            // one says nothing about what lies beyond it, so the cursor stops
            // at the last applied event.
            let seen_through = if full { last } else { last.max(head) };
            if seen_through > cursor {
                // Same transaction as the applies above. This is the
                // exactly-once claim, and it is one line.
                save_checkpoint(tx, &name, seen_through)?;
            }
            Ok(applied)
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
    /// A rebuild on a truncated log is refused before anything is emptied:
    /// replaying what is left would silently produce a different model from
    /// the one being replaced, and destroying the old one first would leave
    /// nothing to compare against.
    pub async fn rebuild(&mut self) -> Result<usize> {
        self.in_transaction(|projection, tx, _| {
            if !projection.tolerates_truncation() {
                require_complete_from(tx, Position::BEGINNING)?;
            }
            let name = projection.name().to_string();
            tx.callback(|raw| projection.reset(raw))?;
            tx.callback(|raw| projection.init(raw))?;
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
        Body: FnOnce(&mut P, &RunnerTxn<'_>, &eventsdb_core::upcast::UpcastChain) -> Result<T>
            + Send
            + 'static,
    {
        let mut projection = self.projection.take().ok_or_else(Self::lost)?;
        let chain = self.shared.chain.clone();
        let trusted = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&trusted);

        let job = move |conn: &mut Connection| {
            // The runner reads and writes `checkpoints`, which is a reserved
            // table — so it runs trusted, and drops that trust around every
            // call into the projection (`RunnerTxn::callback`). Before this,
            // `apply` was handed a transaction with no authorizer on it at
            // all: a public trait with an unguarded second door onto `events`,
            // `checkpoints` and `retention`.
            let result = guarded(conn, trusted, move |conn| {
                let _trusted = Trusted::raise(&flag);
                let outcome = (|| {
                    let tx = conn
                        .transaction_with_behavior(TransactionBehavior::Immediate)
                        .map_err(classify)?;
                    let runner_tx = RunnerTxn {
                        tx: &tx,
                        trusted: &flag,
                    };
                    let value = body(&mut projection, &runner_tx, &chain)?;
                    tx.commit().map_err(classify)?;
                    Ok(value)
                })();
                Ok((projection, outcome))
            });

            // `guarded` catches a panic, and a projection that travelled into
            // one cannot come back — the runner is poisoned, as it already was.
            Ok(match result {
                Ok((projection, outcome)) => (Some(projection), outcome.map_err(map_denial)),
                Err(error) => (None, Err(error)),
            })
        };

        match self.shared.isle.call(job).await {
            Ok((projection, outcome)) => {
                self.projection = projection;
                outcome
            }
            Err(isle) => Err(map_isle(isle)),
        }
    }
}
