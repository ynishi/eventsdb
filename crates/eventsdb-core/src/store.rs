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
//! A *command* with an invariant — "reserve n only if the balance covers it"
//! — is the other case, and it is [`EventStore::append_if`]: the backend
//! reads the stream, calls the caller's decision and appends what it returns,
//! all inside the same serialized write. The check therefore runs against the
//! stream as it is at that instant, not against a head someone cached
//! earlier, which a compare-and-swap could only detect afterwards.

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
    /// The default refuses, because a store that is not a database has no
    /// answer to give.
    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Map<String, Value>>> {
        let _ = (sql, params);
        Err(Error::Unsupported(
            "this store is not a database and cannot answer SQL".to_string(),
        ))
    }
}
