//! The per-stream SPI.
//!
//! An [`EventStore`] is one stream's append-only log behind a trait, so a
//! durable backend takes the same calls the in-memory one does. The SPI is
//! scoped to a **single** stream: there is no `stream` parameter, because the
//! stream is the unit of serialization and a handle *is* one. Questions about
//! the database as a whole belong to [`crate::log::EventLog`], one level up.
//!
//! # Append-only is the shape, not a runtime check
//!
//! The trait has no `update`, `delete` or `overwrite`. Immutability is
//! guaranteed by what the trait cannot express, which is a stronger guarantee
//! than a flag someone can pass.
//!
//! # Appends land; decisions are taken inside the write
//!
//! [`EventStore::append`] records a fact, and the store decides where it
//! lands. It is serialized per stream by the backend, so two handles on one
//! stream both write and the log interleaves in arrival order. An ordinary
//! append is never refused for an out-of-date view of the head: that would be
//! asking a fact to prove it knew the future.
//!
//! A *command* with an invariant is the other case, and it comes in two
//! shapes. **Which one you want depends on where the decision was made, not
//! on whether the check is atomic** — both check inside the write.
//!
//! - The decision runs **at** the write: [`EventStore::append_if`]. The
//!   backend reads the stream, calls the caller's decision and appends what it
//!   returns, all inside the same serialized write, so "reserve n only if the
//!   balance covers it" is checked against the stream as it is at that
//!   instant. Prefer this wherever it fits: it folds rather than comparing, so
//!   it raises no false conflicts; a decision with nothing to do returns
//!   `None` and is idempotent for free; and there is no retry loop because
//!   there is nothing to retry.
//! - The decision was made **before** the call, somewhere this process cannot
//!   reach — an HTTP client holding an `ETag`, a form somebody filled in, a
//!   message that sat in a queue. `Decision` is a `FnOnce` running under the
//!   lock, so it cannot represent that. [`EventStore::append_expecting`] can:
//!   it compares one number, which is exactly as much as a caller who left the
//!   process still knows.
//!
//! An earlier version of this paragraph said a compare-and-swap "could only
//! detect afterwards". That is wrong and worth correcting rather than quietly
//! deleting: a CAS whose head read is in the same transaction as its insert
//! detects at the same instant `append_if`'s decision does. What it cannot do
//! is *fold*. The version that really does detect too late is the one whose
//! lookup sits outside the transaction, which is not what this offers.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::position::Committed;
use crate::upcast::Current;

/// What to write, decided against the stream under the backend's lock.
///
/// `FnOnce`: it is called exactly once, so a contended `append_if` is not
/// retried by the backend — a second attempt would need a second decision,
/// and there is only one. Contention surfaces as [`Error::Busy`] instead,
/// which is the class that says another *call* is worth making.
pub type Decision = Box<dyn FnOnce(&[Current]) -> Option<Map<String, Value>> + Send>;

/// What a caller believes a stream's head to be, as of when it last looked.
///
/// A named type rather than an `Option<u64>` or a zero sentinel. The two cases
/// are not "some head" and "no head" — they are two different claims, and the
/// empty one is the one everybody gets wrong. Rails Event Store spells it
/// `-1`, others spell it `0`, and both produce off-by-one bug reports;
/// Equinox hides the number entirely rather than let it into domain code.
/// Naming the empty case is the mitigation available to a store that has to
/// expose it at all.
///
/// # Two variants are the surface
///
/// Stores that run as a service tend to offer four: these two, an `Any` that
/// checks nothing, and a `StreamExists` that checks only that the stream was
/// written to. `Any` is spelled here by not calling
/// [`EventStore::append_expecting`]. `StreamExists` is absent on purpose.
/// KurrentDB added it so an append could refuse a *soft-deleted* stream — a
/// stream that was written, then marked gone. That third state has nothing to
/// stand on here: the trait has no delete, retention leaves the counter
/// alone, and a stream has either been written or it has not. Past the
/// existence check `StreamExists` is `Any`, so it also adds nothing to
/// concurrency that [`Expected::Seq`] does not already give.
///
/// The question it is usually reached for — "did the command that creates
/// this stream run?" — is a fact about the domain, and it goes where domain
/// facts go: a key under `meta` on the creating event, read through
/// [`crate::log::Filter`]. See [`crate::event`] on why lifecycle lives there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Expected {
    /// Nothing has ever been appended to this stream.
    ///
    /// **Not the same as "the stream reads empty".** Retention can empty a
    /// stream whose counter stands at 50, and a caller that meant "this is a
    /// new order" must not be told yes about an order that was archived. The
    /// check is against what the stream's counter records, not against the
    /// rows that survive.
    Unwritten,

    /// The last event appended to this stream has this `seq`.
    ///
    /// A `seq`, not a [`crate::position::Position`]: the claim is about one
    /// stream, and the two coordinates have different scopes.
    Seq(u64),
}

#[async_trait]
pub trait EventStore: Send + Sync {
    /// Which stream this handle is.
    fn stream_id(&self) -> &str;

    /// Validate, stamp and append an event, returning its coordinates.
    ///
    /// A rejected event leaves no trace and consumes no sequence number.
    async fn append(&mut self, event: Map<String, Value>) -> Result<Committed>;

    /// Append `events` as one write, in the order given.
    ///
    /// For facts that are one fact: two records of a single occurrence must
    /// not be separable by a reader, because a stream where the first is
    /// visible without the second is a stream that never existed.
    ///
    /// **All or nothing on a backend that can do it.** The default below is
    /// the most a backend with no transaction can offer — it appends one at a
    /// time, so a failure part-way leaves what already landed. Both shipped
    /// backends override it.
    async fn append_many(&mut self, events: Vec<Map<String, Value>>) -> Result<Vec<Committed>> {
        let mut committed = Vec::with_capacity(events.len());
        for event in events {
            committed.push(self.append(event).await?);
        }
        Ok(committed)
    }

    /// Decide *inside* the store's serialization: read the stream, ask
    /// `decide` what to write, and append its answer in the same write.
    ///
    /// `kinds` filters what the decision is shown, exactly as it filters
    /// [`EventStore::read_kinds`] (`None` = the whole stream), and the events
    /// arrive in `seq` order. What the decision *writes* is unfiltered — the
    /// event it returns is appended whatever its kind. Returning `None`
    /// records nothing and leaves the stream untouched.
    async fn append_if(
        &mut self,
        kinds: Option<&[&str]>,
        decide: Decision,
    ) -> Result<Option<Committed>>;

    /// Record an event with a time it already has, rather than the wall clock
    /// of this call.
    ///
    /// The backfill counterpart to [`EventStore::append`]: history from a
    /// system that has its own notion of when, brought in without discarding
    /// that timeline. Identical to `append` in every other respect — the
    /// envelope is validated, `seq` and the position are this store's, the
    /// schema version is the author's.
    ///
    /// **Use `append` for ordinary writes.** For moving an *eventsdb* log,
    /// neither this nor `append` is the verb — `import` is, because it also
    /// carries the schema version each event was written under, which this
    /// cannot.
    ///
    /// See [`crate::event`] for what the time coordinate means and why the
    /// verb rather than a field is what says which moment it is. In short:
    /// positions order the log, the coordinate never does, and this call does
    /// not enforce that the time it is given is at or after the stream's head.
    /// Backfilling into an empty stream in chronological order keeps that
    /// property by construction.
    async fn append_at(&mut self, epoch_ms: u64, event: Map<String, Value>) -> Result<Committed> {
        let _ = (epoch_ms, event);
        Err(Error::Unsupported(
            "this store cannot record an event at a time other than now".to_string(),
        ))
    }

    /// Append `event` only if the stream's head is `expected`, refusing with
    /// [`Error::HeadMismatch`] if it is not.
    ///
    /// For a decision taken **before** this call, somewhere this process
    /// cannot reach: an HTTP client holding an `ETag`, a form somebody filled
    /// in, a message that sat in a queue. The caller read the stream at some
    /// head, went away, and is now saying "apply this only if nothing moved".
    ///
    /// [`EventStore::append_if`] is the other case and the better one wherever
    /// it applies — a decision folded from the stream *at* the write, which
    /// this cannot express because it compares one number and never looks at
    /// the events. Reach for this only when the decision could not have run
    /// under the lock.
    ///
    /// **The comparison happens inside the write.** The head is read in the
    /// same transaction that inserts, so a writer arriving between them
    /// serializes behind the write lock rather than slipping past the check. A
    /// refused append leaves no trace and consumes no sequence number, exactly
    /// as a rejected [`EventStore::append`] does.
    ///
    /// The default declines. A backend that cannot make the read and the
    /// insert one write has no honest answer here, and an imitation that
    /// checked separately would be worse than none — that gap between lookup
    /// and save is the documented weakness of the version-only form elsewhere.
    async fn append_expecting(
        &mut self,
        expected: Expected,
        event: Map<String, Value>,
    ) -> Result<Committed> {
        let _ = (expected, event);
        Err(Error::Unsupported(
            "this store cannot make a head check and an append one write".to_string(),
        ))
    }

    /// Queue an append and return without waiting for it to land.
    ///
    /// For a fact that has to be recorded from somewhere that cannot await:
    /// a `Drop` closing a session is the case this exists for, and there
    /// blocking is not allowed either, so neither `.await` nor
    /// `block_on` is available. Hence a plain `fn` — an `async fn` would need
    /// an executor the caller does not have.
    ///
    /// **`&self`, not `&mut self`.** A `Drop` has whatever reference it has,
    /// and demanding a unique one is the difference between reachable from
    /// there and not. Every other write on this trait takes `&mut self`
    /// because it returns coordinates the caller is expected to use; this one
    /// returns nothing to hold.
    ///
    /// **The envelope is checked here; the write is not reported.** A
    /// malformed event is refused synchronously, before the call returns.
    /// After that there is no channel back: a storage failure is dropped and
    /// nothing is woken, because the caller has already gone. Use it for the
    /// boundary record whose absence a reader would notice — the close of a
    /// session that would otherwise read as still open — not for a fact
    /// nothing else in the system knows.
    ///
    /// **Ordering is the backend's to keep.** The queued write must land
    /// before anything submitted after it; a backend that spawns a task which
    /// later calls [`EventStore::append`] has left the queue and races every
    /// other writer, which is worse than declining.
    ///
    /// The default declines rather than dropping the event quietly. A store
    /// with nowhere to queue it has no way to keep that ordering, and silence
    /// here would leave a stream looking open for ever with nothing to say
    /// why.
    fn detach_append(&self, event: Map<String, Value>) -> Result<()> {
        let _ = event;
        Err(Error::Unsupported(
            "this store cannot accept a write it is not awaited for".to_string(),
        ))
    }

    /// Events of `kinds` with `seq >= from_seq`, at most `limit`, in `seq`
    /// order and already upcasted.
    ///
    /// `None` reads every kind. `Some(kinds)` reads only those, and `limit`
    /// counts what came back rather than what was skipped. An empty slice
    /// selects nothing.
    ///
    /// **The kind selected on is the stored one**, because the selection
    /// happens in the backend, before the upcaster chain runs. A step that
    /// renames a kind therefore obliges every filtered read of it to name
    /// both spellings — which is a cost the chain's author pays knowingly,
    /// and the reason a rename is not free.
    async fn read_kinds(
        &self,
        kinds: Option<&[&str]>,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<Current>>;

    /// Every event with `seq >= from_seq`, at most `limit`.
    async fn read(&self, from_seq: u64, limit: usize) -> Result<Vec<Current>> {
        self.read_kinds(None, from_seq, limit).await
    }

    /// The last `n` events, in `seq` order.
    ///
    /// A read *from the end*, which the range reads cannot express: they
    /// start at a `seq` and count forward, so "the last five" could only be
    /// asked for by reading the whole stream and throwing the front away. The
    /// default does exactly that, and both shipped backends override it.
    async fn read_last(&self, n: usize) -> Result<Vec<Current>> {
        let mut all = self.read(0, usize::MAX).await?;
        if all.len() > n {
            all.drain(..all.len() - n);
        }
        Ok(all)
    }

    /// The highest `seq`, or `None` for an empty stream.
    ///
    /// Fallible on purpose: a transient failure must not read as an empty
    /// stream, or a caller deciding open-vs-resume takes the wrong branch.
    async fn head(&self) -> Result<Option<u64>>;

    /// Number of recorded events.
    async fn len(&self) -> Result<usize>;

    /// Whether nothing has been recorded yet.
    async fn is_empty(&self) -> Result<bool> {
        Ok(self.head().await?.is_none())
    }

    /// Which database this stream lives in, or `None` for a backend that is
    /// not one.
    ///
    /// Two handles answer with the same string exactly when they are the same
    /// database. It is an identity, not a path a caller should take apart.
    fn database(&self) -> Option<&str> {
        None
    }

    /// Answer a caller's own read-only SQL over the log.
    ///
    /// **Queries read the stored shape, not the upcasted one**: every other
    /// read goes through the chain, but SQL runs against the bytes as they
    /// were written, because the chain is Rust and the query is SQLite's. A
    /// caller reading across a schema change reads the versions it finds.
    ///
    /// **The limit is yours, and so is knowing whether it cut.** There is no
    /// `truncated` flag here because the `LIMIT` is in your text, not in a
    /// parameter this store owns: ask for `n + 1` rows and compare, which is
    /// the whole of what such a flag would tell you. Owning the limit instead
    /// would mean wrapping your statement to attach one, and this call
    /// deliberately never rewrites or parses what it is given — the read-only
    /// check above is SQLite's answer about your text, not a reading of it.
    ///
    /// The default refuses, because a store that is not a database has no
    /// answer to give.
    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Map<String, Value>>> {
        let _ = (sql, params);
        Err(Error::Unsupported(
            "this store is not a database and cannot answer SQL".to_string(),
        ))
    }

    /// [`EventStore::query`] with a bound on how long the statement may run.
    ///
    /// **Not the same thing as `busy_timeout`.** That one bounds *waiting for
    /// a lock*; this bounds a statement that took its lock immediately and is
    /// simply expensive — a recursive CTE with a runaway bound, a join with no
    /// usable index. Nothing else stops one, and a caller that hands SQL to
    /// somebody else (a shell, a script, a user) cannot know in advance which
    /// kind it is getting.
    ///
    /// A backend serving SQL from the same place it serves writes has the
    /// stronger reason: one expensive statement there stalls every append
    /// until it finishes.
    ///
    /// The deadline is reported as [`Error::Timeout`] — the caller's own bound
    /// arriving, not the database failing — so retrying a narrower query is
    /// the sensible next move.
    ///
    /// The default declines rather than falling back to [`EventStore::query`]:
    /// running an unbounded statement for a caller who asked for a bound is
    /// the one answer that is worse than none.
    async fn query_timeout(
        &self,
        sql: &str,
        params: Vec<Value>,
        timeout: Duration,
    ) -> Result<Vec<Map<String, Value>>> {
        let _ = (sql, params, timeout);
        Err(Error::Unsupported(
            "this store cannot bound how long a statement runs".to_string(),
        ))
    }
}
