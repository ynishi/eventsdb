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
//!
//! # Three read axes
//!
//! A cross-stream read or a subscription narrows the log along three axes,
//! and only three. Each is a column or a key the caller wrote, matched on the
//! **stored** shape before the upcaster chain runs:
//!
//! ```text
//!              position ───────────────────────────────────────▶
//!
//!   streams  │ orders/17 │ orders/17 │ orders/18 │ orders/17 │ orders/19 │
//!   kinds    │ placed    │ paid      │ placed    │ closed    │ placed    │
//!   meta     │ tenant=a  │ tenant=a  │ tenant=b  │ tenant=a  │ tenant=a  │
//!            │           │           │           │ closed=1  │           │
//!
//!   Filter { streams: [orders/17, orders/18] }     ─▶ 1st 2nd 3rd 4th
//!   Filter { kinds: [placed] }                     ─▶ 1st     3rd     5th
//!   Filter { meta: [(tenant, a)] }                 ─▶ 1st 2nd     4th 5th
//!   Filter { kinds: [closed], meta: [(tenant, a)] } ─▶             4th
//! ```
//!
//! `streams` and `kinds` are sets: a member matches. `meta` is a list of
//! `(key, value)` pairs, every pair must match, and a key an event does not
//! carry matches nothing — absence is not a value. Axes combine by AND.
//!
//! What the `meta` axis is: equality on a scalar the caller wrote, the same
//! operation `kind IN (...)` is. What it is not: a query language. There is
//! no range, no prefix, no `OR` across keys, and no reading inside `data`.
//! A caller with that question builds a projection, which is what a
//! projection is for; the axis exists so that the *properties a projection
//! keys on* are cheap to read from the log directly, not so that the log
//! becomes the projection. Why the properties live under `meta` at all,
//! lifecycle included, is [`crate::event`]'s to say.
//!
//! Matching happens on the stored shape for the reason `kinds` states: an
//! upcaster runs on the way *out*, so a key it would add is not in the row
//! to be matched. A caller that renames a `meta` key has two names to filter
//! by until the old rows are gone — which is a fact about the log, and the
//! filter reports it rather than hiding it.
//!
//! # A read is a page; a stream is the page loop
//!
//! [`EventLog::read_all`] returns a `Vec` bounded by `limit`, and is
//! exclusive on `from` so the position of the last event handled is the
//! next call's `from`. That is the whole of the read contract: a page, and a
//! cursor the caller owns. It is deliberately not a cursor the *log* owns —
//! an open cursor on an embedded database is an open read transaction, and
//! what that costs is the backend's to state, not this trait's to hide.
//!
//! Anything that reads more than a page is that call in a loop, and the
//! loop is the same whether it ends when the range runs dry or waits there
//! for more: [`EventLog::subscribe`] is the waiting form on this trait, and a
//! backend may offer the ending form beside it. Neither holds anything
//! between pages that `read_all` did not hold during one.

use async_trait::async_trait;
use futures_core::stream::BoxStream;
use serde_json::Value;

use crate::error::{Error, Result};
use crate::event::FIELD_META;
use crate::position::{Position, Recorded};
use crate::store::EventStore;
use crate::transfer::{ExportedEvent, ImportReport};

/// Which events a cross-stream read or a subscription wants.
///
/// Every axis is matched on the **stored** shape — the stored kind, the
/// stored stream name, the stored `meta` — before the upcaster chain runs,
/// the same rule [`EventStore::read_kinds`] follows, for the same reason.
/// The module doc has the three axes side by side.
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
    /// `meta` keys and the value each must hold. `None` and an empty vector
    /// both place no condition — there is no "these keys" to be given none
    /// of, so the two readings coincide here where they diverge above.
    ///
    /// **Pairs, not a set.** Every pair must hold, so two pairs on one key
    /// with different values select nothing, which is what "both" means. A
    /// value is a string, a number or a boolean — the scalars `meta` admits
    /// on the way in — and anything else is refused by the backend rather
    /// than matched against nothing: a `null` here could only ever mean
    /// "select nothing", and that already has a spelling.
    pub meta: Option<Vec<(String, Value)>>,
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
            ..Filter::default()
        }
    }

    /// Require `key` under `meta` to hold `value`.
    ///
    /// Adds to whatever pairs were there — every pair must hold — so
    /// `.meta("tenant", "a").meta("closed", true)` reads as "both", which is
    /// the reading the plural axis has. Contrast [`Filter::stream`], which
    /// replaces: one name is a singular claim, a property list is not.
    pub fn meta(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.meta
            .get_or_insert_with(Vec::new)
            .push((key.into(), value.into()));
        self
    }

    /// Whether every `meta` value is a scalar this filter can match.
    ///
    /// For a backend to call before it builds a query. The check is here
    /// rather than in [`Filter::meta`] so that a filter assembled by hand
    /// from its public fields is held to the same rule as one built through
    /// the method.
    pub fn validate(&self) -> Result<()> {
        for (key, value) in self.meta.iter().flatten() {
            match value {
                Value::String(_) | Value::Number(_) | Value::Bool(_) => {}
                Value::Null => {
                    return Err(Error::validation(format!(
                        "a filter on `{FIELD_META}.{key}` cannot match `null`: a key the \
                         event does not carry matches nothing already, and an empty \
                         `kinds` or `streams` is how to select nothing on purpose"
                    )))
                }
                other => {
                    return Err(Error::validation(format!(
                        "a filter on `{FIELD_META}.{key}` must be a string, number or \
                         boolean, found {}; `{FIELD_META}` holds scalars, so there is \
                         nothing structured there to match",
                        crate::event::type_name(other)
                    )))
                }
            }
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn meta_pairs_accumulate_and_validate_as_scalars() {
        let filter = Filter::all().meta("tenant", "a").meta("closed", true);
        assert_eq!(
            filter.meta,
            Some(vec![
                ("tenant".to_string(), json!("a")),
                ("closed".to_string(), json!(true)),
            ])
        );
        assert!(filter.validate().is_ok());
        assert!(
            !filter.selects_nothing(),
            "a meta pair is a condition, not a set"
        );
    }

    #[test]
    fn a_null_or_structured_meta_value_fails_validation() {
        for value in [json!(null), json!([1]), json!({ "id": 1 })] {
            let err = Filter::all().meta("k", value).validate().unwrap_err();
            assert!(matches!(err, Error::Validation(_)), "{err}");
        }
    }

    #[test]
    fn an_empty_meta_list_is_no_condition() {
        let filter = Filter {
            meta: Some(Vec::new()),
            ..Filter::default()
        };
        assert!(!filter.selects_nothing());
        assert!(filter.validate().is_ok());
    }
}
