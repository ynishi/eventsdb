//! [`TxnContext`] — the transaction, plus the log's own stamped operations
//! on it.
//!
//! # Why the context and not a raw handle
//!
//! The hatch used to hand out a bare [`Transaction`], which meant a caller
//! could write their own tables but could not append: the authorizer denies
//! `INSERT INTO events`, deliberately, so that an append cannot skip
//! validation, sequencing and ordering. That left "append this event **and**
//! write this row, together or not at all" impossible to express — and it is
//! the ordinary case, not an exotic one.
//!
//! Allowing raw inserts would have solved it by giving up the reason the
//! authorizer exists. Instead the context carries the stamping path itself:
//! [`TxnContext::append`] is the same `next_seq` / `stamp` / `insert_stamped`
//! sequence [`crate::SqliteEventStore::append`] runs, minus the commit. The
//! only route to `events` is still a stamped one.
//!
//! Two ecosystems converged on this shape independently: sea-orm's
//! `DatabaseTransaction` implements the same `ConnectionTrait` its ordinary
//! operations take, and Marten's `IDocumentSession` is a unit of work holding
//! document writes and event appends together. The alternative — pass the raw
//! handle only — is what `sqlx` does, and is the one its users complain about.
//!
//! # What it does not have
//!
//! No `commit` and no `rollback`. The unit of work has one commit point, and
//! it belongs to [`crate::SqliteEventLog::with_transaction`]: a second one
//! inside the closure would split the atomicity the whole thing is for.
//! `Deref` yields `&Transaction`, and `Transaction::commit` takes `self` by
//! value, so this is currently prevented by the types — **that is intent, not
//! an accident. A `DerefMut` or a `tx_mut()` accessor would silently open it.**
//!
//! # Notifying subscribers happens after the commit, not during
//!
//! Positions appended here are buffered and published once the transaction
//! commits. Waking a subscriber at a position a rollback then erases would
//! walk it past a hole it can never fill, and the watch only moves forward, so
//! the mistake would not correct itself.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use eventsdb_core::error::Result;
use eventsdb_core::event::{now_ms, restamp, stamp, validate};
use eventsdb_core::position::{Committed, Position};
use eventsdb_core::transfer::ExportedEvent;
use eventsdb_core::upcast::{apply_chain, Current, UpcastChain};
use rusqlite::Transaction;
use serde_json::{Map, Value};

use crate::store::{insert_stamped, next_seq, select_stream, set_next_seq};

/// Lifts the authorizer's refusal for exactly as long as the crate's own
/// stamped statements are running.
///
/// A `Drop` guard rather than a set/clear pair on purpose: a panic between the
/// two would leave the flag raised, and the caller's next raw statement could
/// then write `events` — forging precisely what the hatch exists to prevent.
/// Restores the previous value rather than clearing, so the guards nest. The
/// projection runner raises trust for its own work and lowers it again around
/// the projection's callbacks, and an append inside such a callback must put
/// back what it found.
pub(crate) struct Trusted<'a>(&'a AtomicBool, bool);

impl<'a> Trusted<'a> {
    pub(crate) fn raise(flag: &'a AtomicBool) -> Self {
        Trusted(flag, flag.swap(true, Ordering::SeqCst))
    }
}

impl Drop for Trusted<'_> {
    fn drop(&mut self) {
        self.0.store(self.1, Ordering::SeqCst);
    }
}

/// The inverse: drop trust for the duration of a callback the crate does not
/// control, and put it back afterwards.
pub(crate) struct Untrusted<'a>(&'a AtomicBool, bool);

impl<'a> Untrusted<'a> {
    pub(crate) fn lower(flag: &'a AtomicBool) -> Self {
        Untrusted(flag, flag.swap(false, Ordering::SeqCst))
    }
}

impl Drop for Untrusted<'_> {
    fn drop(&mut self) {
        self.0.store(self.1, Ordering::SeqCst);
    }
}

/// The open transaction, and what the log can do inside it.
///
/// `Deref` gives the raw handle for your own tables; the inherent methods are
/// the only way to reach the log's.
pub struct TxnContext<'t> {
    tx: &'t Transaction<'t>,
    chain: UpcastChain,
    trusted: Arc<AtomicBool>,
    published: RefCell<Vec<Position>>,
}

impl<'t> std::ops::Deref for TxnContext<'t> {
    type Target = Transaction<'t>;

    fn deref(&self) -> &Self::Target {
        self.tx
    }
}

impl<'t> TxnContext<'t> {
    pub(crate) fn new(
        tx: &'t Transaction<'t>,
        chain: UpcastChain,
        trusted: Arc<AtomicBool>,
    ) -> Self {
        TxnContext {
            tx,
            chain,
            trusted,
            published: RefCell::new(Vec::new()),
        }
    }

    /// Validate, stamp and insert one event, returning where it landed.
    ///
    /// The same rules as [`eventsdb_core::EventStore::append`] — a rejected
    /// event leaves no trace and consumes no sequence number — with one
    /// difference: it lands when the enclosing transaction commits, not
    /// before. Return `Err` from the closure and it is as if it never
    /// happened.
    ///
    /// The returned [`Committed`] is real and immediate, not a placeholder
    /// filled in at commit time. That is worth stating because the closest
    /// precedent gets it wrong: Marten defers the write, so an inline
    /// projection there reads `Sequence` as `0` and has to be told to use a
    /// slower mode. Here the transaction is already open, so a derived row
    /// written later in the same closure can be keyed on the actual position.
    pub fn append(&self, stream: &str, event: Map<String, Value>) -> Result<Committed> {
        self.append_at(stream, now_ms(), event)
    }

    /// The backfill counterpart to [`TxnContext::append`]: record an event
    /// with a time it already has, instead of the wall clock of this call.
    ///
    /// Identical to `append` in every other respect — the envelope is
    /// validated, `seq` comes from the stream counter, `position` from the
    /// rowid, `_schema_version` is stamped current. The only difference is
    /// which clock `epoch_ms` comes from.
    ///
    /// **Use `append` for ordinary writes, and reach for this only on
    /// backfill paths**: bringing history in from a system that already has
    /// its own notion of when, without discarding that timeline. For moving
    /// an *eventsdb* log, use [`TxnContext::import`] instead — it preserves
    /// the schema version too, which this cannot.
    ///
    /// # `epoch_ms` is a time coordinate, not an ordering key
    ///
    /// Every event carries a time coordinate (`epoch_ms`) and log positions
    /// (`seq`, `position`). `append` sets the coordinate to now; this one lets
    /// the caller substitute the moment the change happened where it came
    /// from. Either way, **the positions order the log and the coordinate
    /// never does.** Reads are `ORDER BY seq` or `ORDER BY position`; nothing
    /// sorts by time.
    ///
    /// That is also why supplying it surrenders nothing. `seq` and `position`
    /// are **allocations** whose uniqueness and monotonicity the store
    /// enforces — `stream_seq` and `AUTOINCREMENT` exist for no other reason —
    /// and handing those over would give up what the store guarantees.
    /// `epoch_ms` is a recorded observation with one consumer,
    /// [`crate::Plan::OlderThan`].
    ///
    /// # Non-monotonic time, and what it does to age
    ///
    /// This does not enforce that the supplied time is at or after the
    /// stream's current head: it may be earlier, equal or later, and nothing
    /// detects it. A stream written only through `append` is non-decreasing by
    /// construction, because the wall clock only moves forward.
    ///
    /// So `OlderThan` stops being a position prefix once backfilled history is
    /// present — old events can sit at high positions.
    /// [`crate::Plan::Streams`] already produces that shape and the retention
    /// watermark already handles it, so it is not a new failure class, but it
    /// is a different answer than the same call would give on an
    /// `append`-only log.
    ///
    /// **Backfilling into an empty stream in chronological order keeps the
    /// non-decreasing property by construction**, which is the usual migration
    /// shape and the one to prefer. Mixing backfill into a stream that already
    /// has appended events on a different clock is what leaves age queries
    /// answering in ways that do not match intuition.
    pub fn append_at(
        &self,
        stream: &str,
        epoch_ms: u64,
        event: Map<String, Value>,
    ) -> Result<Committed> {
        validate(&event)?;
        let seq = next_seq(self.tx, stream)?;
        let stamped = stamp(event, seq, epoch_ms)?;

        let committed = {
            let _trusted = Trusted::raise(&self.trusted);
            let committed = insert_stamped(self.tx, stream, &stamped)?;
            set_next_seq(self.tx, stream, seq + 1)?;
            committed
        };

        if let Some(position) = committed.position {
            self.published.borrow_mut().push(position);
        }
        Ok(committed)
    }

    /// Append several events to one stream, as
    /// [`eventsdb_core::EventStore::append_many`] does: one clock reading for
    /// the batch, contiguous sequence numbers.
    ///
    /// Appending to *different* streams is just two calls — there is no
    /// grouping rule and no ordering restriction, because each call takes its
    /// own `seq` from the stored counter and its own position from the rowid.
    pub fn append_many(
        &self,
        stream: &str,
        events: Vec<Map<String, Value>>,
    ) -> Result<Vec<Committed>> {
        // One clock reading for the batch: these are records of one
        // occurrence, and times differing by the cost of the loop would
        // suggest an ordering that is not there.
        let epoch_ms = now_ms();
        self.append_many_at(stream, events.into_iter().map(|e| (epoch_ms, e)).collect())
    }

    /// The batch form of [`TxnContext::append_at`]: one run of sequence
    /// numbers, each event keeping its own time.
    pub fn append_many_at(
        &self,
        stream: &str,
        events: Vec<(u64, Map<String, Value>)>,
    ) -> Result<Vec<Committed>> {
        for (_, event) in &events {
            validate(event)?;
        }
        if events.is_empty() {
            return Ok(Vec::new());
        }

        let base_seq = next_seq(self.tx, stream)?;
        let mut out = Vec::with_capacity(events.len());

        {
            let _trusted = Trusted::raise(&self.trusted);
            for (offset, (epoch_ms, event)) in events.into_iter().enumerate() {
                let stamped = stamp(event, base_seq + offset as u64, epoch_ms)?;
                out.push(insert_stamped(self.tx, stream, &stamped)?);
            }
            set_next_seq(self.tx, stream, base_seq + out.len() as u64)?;
        }

        self.published
            .borrow_mut()
            .extend(out.iter().filter_map(|c| c.position));
        Ok(out)
    }

    /// Take an event from another log, keeping everything it carries except
    /// its coordinates.
    ///
    /// `epoch_ms` **and `_schema_version`** survive; `seq` and `position` are
    /// reassigned by this store. The schema version is the important half:
    /// re-stamping an old event as current would put it out of reach of the
    /// upcaster written for it, and the imported log would then read as
    /// something it never was.
    ///
    /// Returns where it landed, so a caller importing into an empty store can
    /// check that against the position the event carried.
    pub fn import(&self, exported: &ExportedEvent) -> Result<Committed> {
        Ok(self
            .import_many(std::slice::from_ref(exported))?
            .pop()
            .expect("one event in, one out"))
    }

    /// Import a run of events, in the order given.
    ///
    /// The stream counter is read once per stream and written once per stream,
    /// rather than once per event. That is the whole of the difference, and it
    /// is most of the cost: importing 5 000 events one at a time took 45 ms
    /// against a 5.5 ms export, because each one did its own counter read and
    /// write [benched: `transfer` group, release].
    ///
    /// Events may name any streams, in any order — the counters are tracked
    /// per stream as the run is walked, so an interleaved export imports as it
    /// was exported.
    pub fn import_many(&self, exported: &[ExportedEvent]) -> Result<Vec<Committed>> {
        if exported.is_empty() {
            return Ok(Vec::new());
        }

        // The next sequence for each stream this run touches, read on first
        // sight and kept in step from there.
        let mut next: HashMap<&str, u64> = HashMap::new();
        let mut out = Vec::with_capacity(exported.len());

        {
            let _trusted = Trusted::raise(&self.trusted);
            for event in exported {
                let stream = event.stream.as_str();
                let seq = match next.get(stream) {
                    Some(seq) => *seq,
                    None => next_seq(self.tx, stream)?,
                };
                let restamped = restamp(event.event.clone(), seq)?;
                out.push(insert_stamped(self.tx, stream, &restamped)?);
                next.insert(stream, seq + 1);
            }
            // Once per stream, at the end.
            for (stream, seq) in &next {
                set_next_seq(self.tx, stream, *seq)?;
            }
        }

        self.published
            .borrow_mut()
            .extend(out.iter().filter_map(|c| c.position));
        Ok(out)
    }

    /// Read a stream as it stands *inside this transaction* — including what
    /// this closure has already appended.
    ///
    /// This is what makes a decision expressible here: read the stream, decide
    /// against what you find, append the answer, all before anyone else can
    /// write.
    pub fn read(
        &self,
        stream: &str,
        kinds: Option<&[&str]>,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<Current>> {
        let kinds = kinds.map(|k| k.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let stored = select_stream(self.tx, stream, kinds.as_deref(), from_seq, limit)?;
        apply_chain(&self.chain, stored)
            .into_iter()
            .map(Current::from_upcasted)
            .collect()
    }

    /// The highest `seq` `stream` currently holds, or `None` when it holds
    /// nothing.
    ///
    /// From the events, not from the counter: after retention empties a
    /// stream the counter carries on — that is what stops `seq` from being
    /// reused — but the stream really is empty, and this answers about the
    /// stream.
    pub fn head(&self, stream: &str) -> Result<Option<u64>> {
        self.tx
            .query_row(
                "SELECT MAX(seq) FROM events WHERE stream = ?1",
                [stream],
                |row| row.get::<_, Option<i64>>(0),
            )
            .map(|max| max.map(|seq| seq as u64))
            .map_err(crate::shared::classify)
    }

    /// The positions this closure appended, for publishing after the commit.
    pub(crate) fn into_published(self) -> Vec<Position> {
        self.published.into_inner()
    }
}
