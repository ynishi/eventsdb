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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use eventsdb_core::error::Result;
use eventsdb_core::event::{now_ms, stamp, validate};
use eventsdb_core::position::{Committed, Position};
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
struct Trusted<'a>(&'a AtomicBool);

impl<'a> Trusted<'a> {
    fn raise(flag: &'a AtomicBool) -> Self {
        flag.store(true, Ordering::SeqCst);
        Trusted(flag)
    }
}

impl Drop for Trusted<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
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
    /// The same rules as [`crate::SqliteEventStore::append`] — a rejected
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
        validate(&event)?;
        let seq = next_seq(self.tx, stream)?;
        let stamped = stamp(event, seq, now_ms())?;

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

    /// Append several events to one stream, as [`crate::SqliteEventStore::append_many`]
    /// does: one clock reading for the batch, contiguous sequence numbers.
    ///
    /// Appending to *different* streams is just two calls — there is no
    /// grouping rule and no ordering restriction, because each call takes its
    /// own `seq` from the stored counter and its own position from the rowid.
    pub fn append_many(
        &self,
        stream: &str,
        events: Vec<Map<String, Value>>,
    ) -> Result<Vec<Committed>> {
        for event in &events {
            validate(event)?;
        }
        if events.is_empty() {
            return Ok(Vec::new());
        }

        let base_seq = next_seq(self.tx, stream)?;
        let epoch_ms = now_ms();
        let mut out = Vec::with_capacity(events.len());

        {
            let _trusted = Trusted::raise(&self.trusted);
            for (offset, event) in events.into_iter().enumerate() {
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
