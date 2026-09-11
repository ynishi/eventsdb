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
//! # Four read axes
//!
//! A cross-stream read or a subscription narrows the log along four axes,
//! and only four. Each is a column or a key the caller wrote, matched on the
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
//!   Filter { stream_prefix: orders/1 }             ─▶ 1st 2nd 3rd 4th 5th
//!   Filter { kinds: [closed], meta: [(tenant, a)] } ─▶             4th
//! ```
//!
//! `streams` and `kinds` are sets: a member matches. `meta` is a list of
//! `(key, value)` pairs, every pair must match, and a key an event does not
//! carry matches nothing — absence is not a value. `stream_prefix` is a
//! range on the stream name, and it is a second predicate on the same column
//! `streams` is: the two narrow each other rather than replace each other.
//! Axes combine by AND.
//!
//! What the `meta` axis is: equality on a scalar the caller wrote, the same
//! operation `kind IN (...)` is. What it is not: a query language. On this
//! axis there is no range, no prefix, no `OR` across keys, and no reading
//! inside `data`.
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
///
/// The module doc has the four axes side by side.
///
/// `#[non_exhaustive]`, so an axis can be added without breaking a caller.
/// Start from [`Filter::all`] or [`Filter::kinds`] and narrow with the
/// methods, or set the public fields on a default; only the struct literal
/// is reserved.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
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
    /// A prefix every stream name must start with.
    ///
    /// **A byte-range on a name the caller chose.** The store does not
    /// interpret the name: no separator is assumed and no category is
    /// parsed, so `stream_prefix("session-")` and `stream_prefix("sess")`
    /// are both legitimate and mean what they say. A name matches when the
    /// prefix's bytes are its first bytes — the ordering a backend comparing
    /// UTF-8 bytewise gives, which is what [`stream_prefix_bound`] turns
    /// into a range.
    ///
    /// **`None` and `Some("")` both place no condition**: every name starts
    /// with nothing, so an empty prefix is every stream rather than none of
    /// them, and [`Filter::selects_nothing`] says so.
    ///
    /// **AND with `streams`, not instead of it.** Both are predicates on the
    /// stream name, so a filter carrying both reads the members of the set
    /// that start with the prefix. Refusing the pair as contradictory would
    /// make `Filter` the one place in this API that judges a caller's
    /// predicate for usefulness.
    ///
    /// One prefix, not a list: a caller wanting two runs two reads. What the
    /// axis is *for* is the group a set cannot name — a stream is a period,
    /// so the sessions of one month are `session-2026-09-01`,
    /// `session-2026-09-02`, … and the streams that belong together are not
    /// a set anybody holds.
    pub stream_prefix: Option<String>,
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

    /// Restrict to streams whose name starts with `prefix`.
    ///
    /// Replaces whatever prefix was there — two prefixes on one filter would
    /// be a question with one answer, and the empty set has a spelling
    /// already. See the field for what a prefix is, how it meets `streams`,
    /// and why an empty one is every stream.
    pub fn stream_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.stream_prefix = Some(prefix.into());
        self
    }

    /// Whether this filter can match anything at all. An empty list of either
    /// kind cannot, and a backend can skip the query entirely.
    ///
    /// A prefix never makes it true: the empty prefix is every stream, and a
    /// prefix no stream carries is a question with an empty answer rather
    /// than a filter that cannot be asked.
    pub fn selects_nothing(&self) -> bool {
        self.kinds.as_ref().is_some_and(|kinds| kinds.is_empty())
            || self
                .streams
                .as_ref()
                .is_some_and(|streams| streams.is_empty())
    }
}

/// The exclusive upper bound of a stream-name prefix: the least string that
/// sorts above every name starting with `prefix`, or `None` when there is no
/// such string and the range is open at the top.
///
/// `stream >= prefix AND stream < bound` is then exactly "starts with
/// `prefix`" — for a backend that compares names as bytes, which is what
/// SQLite's default `BINARY` collation does to `TEXT`.
///
/// **The bound is the prefix with its last *character* replaced by the next
/// Unicode scalar value, not its last byte incremented.** Incrementing the
/// last byte can leave the bound invalid UTF-8, and a [`String`] is the only
/// thing this function can hand back for a backend to bind as text. The two
/// agree on where the boundary falls, because UTF-8 preserves code-point
/// order under bytewise comparison and no character's encoding is a prefix
/// of another's: a name sorts below the bound exactly when the character
/// where it first differs does.
///
/// A last character of [`char::MAX`] has no successor, so it is dropped and
/// the character before it carries — the carry a byte-level `0xFF` takes,
/// exact for the same reason: nothing sorts between `char::MAX` and the
/// carried bound, so the range loses no name and gains none. A prefix made
/// only of `char::MAX`, and the empty prefix, have no bound at all and leave
/// the range open above.
pub fn stream_prefix_bound(prefix: &str) -> Option<String> {
    for (at, ch) in prefix.char_indices().rev() {
        if let Some(next) = next_scalar(ch) {
            let mut bound = String::with_capacity(at + next.len_utf8());
            bound.push_str(&prefix[..at]);
            bound.push(next);
            return Some(bound);
        }
    }
    None
}

/// The next Unicode scalar value after `ch`, stepping over the surrogate
/// range, which is not one. `None` at [`char::MAX`], which has no successor.
fn next_scalar(ch: char) -> Option<char> {
    match ch as u32 {
        0x10FFFF => None,
        0xD7FF => char::from_u32(0xE000),
        other => char::from_u32(other + 1),
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

    /// A prefix is a range, and a range is never "select nothing" — not even
    /// the empty one, which is every stream. Nor is there anything about a
    /// prefix for `validate` to refuse: any string is a legitimate range.
    #[test]
    fn a_prefix_is_a_condition_never_an_empty_selection() {
        for prefix in ["", "session-", "\u{10FFFF}"] {
            let filter = Filter::all().stream_prefix(prefix);
            assert_eq!(filter.stream_prefix.as_deref(), Some(prefix));
            assert!(!filter.selects_nothing(), "prefix {prefix:?}");
            assert!(filter.validate().is_ok(), "prefix {prefix:?}");
        }

        // The set still decides: an empty set selects nothing whatever the
        // prefix says, because the two are ANDed.
        let filter = Filter::all()
            .streams(Vec::<String>::new())
            .stream_prefix("a");
        assert!(filter.selects_nothing());
    }

    /// `stream_prefix` replaces, and leaves the other axes alone.
    #[test]
    fn stream_prefix_replaces_and_composes() {
        let filter = Filter::kinds(["placed"])
            .streams(["session-1"])
            .stream_prefix("sess")
            .stream_prefix("session-");
        assert_eq!(filter.stream_prefix.as_deref(), Some("session-"));
        assert_eq!(filter.streams, Some(vec!["session-1".to_string()]));
        assert_eq!(filter.kinds, Some(vec!["placed".to_string()]));
    }

    /// The bound is the last character's successor, so it is still a `String`
    /// a backend can bind as text.
    #[test]
    fn the_bound_is_the_next_scalar_value_after_the_last_character() {
        assert_eq!(stream_prefix_bound("session-").as_deref(), Some("session."));
        assert_eq!(stream_prefix_bound("a").as_deref(), Some("b"));
        // Multi-byte: the last character moves, the ones before it do not.
        assert_eq!(stream_prefix_bound("会話-").as_deref(), Some("会話."));
        assert_eq!(stream_prefix_bound("会話").as_deref(), Some("会該"));
        // The surrogate range is not a scalar value and is stepped over.
        assert_eq!(stream_prefix_bound("\u{D7FF}").as_deref(), Some("\u{E000}"));
    }

    /// Both ends of the carry: a last character at [`char::MAX`] is dropped
    /// and the one before it moves, and a prefix that is nothing but
    /// `char::MAX` has no bound at all.
    #[test]
    fn char_max_carries_and_an_all_max_prefix_has_no_bound() {
        assert_eq!(
            stream_prefix_bound("a\u{10FFFF}").as_deref(),
            Some("b"),
            "the carry, the same one 0xFF takes at the byte level"
        );
        assert_eq!(
            stream_prefix_bound("a\u{10FFFF}\u{10FFFF}").as_deref(),
            Some("b")
        );
        assert_eq!(stream_prefix_bound("\u{10FFFF}"), None);
        assert_eq!(stream_prefix_bound("\u{10FFFF}\u{10FFFF}"), None);
        assert_eq!(stream_prefix_bound(""), None, "every name starts with it");
    }

    /// The property the bound exists for, checked bytewise the way a backend
    /// comparing `TEXT` under `BINARY` collation does: for every candidate
    /// name, `prefix <= name < bound` holds exactly when the name starts with
    /// the prefix.
    #[test]
    fn the_range_is_exactly_the_names_that_start_with_the_prefix() {
        let names = [
            "",
            "a",
            "ab",
            "b",
            "session",
            "session-",
            "session-2026-09-01",
            "session.",
            "sessions-x",
            "sessio",
            "会話",
            "会話-1",
            "会該",
            "a\u{10FFFF}",
            "a\u{10FFFF}z",
            "a\u{10FFFF}\u{10FFFF}",
            "az",
            "b\u{10FFFF}",
        ];
        for prefix in [
            "",
            "a",
            "session-",
            "sess",
            "会話",
            "\u{10FFFF}",
            "a\u{10FFFF}",
        ] {
            let bound = stream_prefix_bound(prefix);
            for name in names {
                let in_range = name.as_bytes() >= prefix.as_bytes()
                    && bound
                        .as_ref()
                        .is_none_or(|b| name.as_bytes() < b.as_bytes());
                assert_eq!(
                    in_range,
                    name.starts_with(prefix),
                    "prefix {prefix:?}, bound {bound:?}, name {name:?}"
                );
            }
        }
    }
}
