//! The one shape every instrumented path in this crate uses.
//!
//! Everything here exists twice — once against `tracing`, once as a shim that
//! compiles to nothing — so that a call site is written once and neither
//! reads nor cares which build it is in. That is the point: `#[cfg]` at the
//! call sites would put the feature into every function body, and a body that
//! differs between builds is a body that drifts between them.
//!
//! **The payload is never a field.** `data` and `meta` are the caller's and
//! may hold anything; the rule is stated once, in the crate root's
//! "Instrumentation" section, and nothing here takes a `Map<String, Value>`.
//!
//! The pieces:
//!
//! - [`span!`] — a `debug`-level span, or the shim's unit value.
//! - [`Span::record`] — a field filled in on the way out, when the value is
//!   only known then. The arguments are evaluated in both builds, so they are
//!   counts and coordinates, never formatting.
//! - [`instrument`] — a span attached to a **future**, which is what keeps a
//!   guard from being held across an `.await`. A span is entered directly
//!   ([`Span::in_scope`]) only around a synchronous emit, where there is no
//!   await point to hold it over.
//! - [`refusal`] — the `warn` a refused call leaves behind, carrying the
//!   error's own text.
//! - [`woke`] and [`statement`] — the two `trace` events, as functions
//!   because their arguments are computed in both builds.

use eventsdb_core::error::{Error, Result};

#[cfg(feature = "tracing")]
pub(crate) use tracing::Span;

/// The shim's span: a value with the same surface and no behaviour.
///
/// `Clone` and deliberately not `Copy`: [`tracing::Span`] is `Clone` alone, so
/// a `Copy` shim would make every `span.clone()` at a call site a clippy
/// warning in the build where the span is not a real one — which is the call
/// sites differing between builds by another name.
#[cfg(not(feature = "tracing"))]
#[derive(Clone)]
pub(crate) struct Span;

#[cfg(not(feature = "tracing"))]
impl Span {
    pub(crate) const fn none() -> Self {
        Span
    }

    /// Takes what [`tracing::Span::record`] takes, and drops it.
    pub(crate) fn record<T>(&self, _field: &'static str, _value: T) -> &Self {
        self
    }

    /// Takes what [`tracing::Span::in_scope`] takes, and just runs it.
    pub(crate) fn in_scope<T>(&self, body: impl FnOnce() -> T) -> T {
        body()
    }
}

/// A `debug`-level span, named and with its fields, or nothing.
///
/// The arguments are `tracing`'s: `span!("eventsdb.append", stream = %id,
/// count = 1, position = tracing::field::Empty)`. With the feature off the
/// tokens are never expanded, so nothing in them is evaluated — which is why
/// a value known only on the way out is recorded through [`Span::record`]
/// rather than computed here.
#[cfg(feature = "tracing")]
macro_rules! span {
    ($($field:tt)*) => { ::tracing::debug_span!($($field)*) };
}

#[cfg(not(feature = "tracing"))]
macro_rules! span {
    ($($field:tt)*) => {
        $crate::trace::Span::none()
    };
}

pub(crate) use span;

/// A subscription or a follow woke, and which of the two things woke it.
///
/// At `trace` because a busy log emits one of these per commit per
/// subscriber, and a function rather than a macro because the argument is
/// computed either way: a macro that dropped its tokens with the feature off
/// would leave the local that holds the answer unread.
#[cfg(feature = "tracing")]
pub(crate) fn woke(woken_by: &'static str, cursor: u64) {
    tracing::trace!(woken_by, cursor, "woke");
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn woke(_woken_by: &'static str, _cursor: u64) {}

/// The statement the hatch is about to run, at `trace` and nowhere else.
///
/// A literal in SQL is the caller's data — a customer id in a `WHERE` clause
/// is the obvious case — so this sits a level below everything else the hatch
/// emits and is bounded at [`SQL_LIMIT`] characters. A subscriber at `debug`
/// sees the span and its row count and never the text.
#[cfg(feature = "tracing")]
pub(crate) fn statement(sql: &str) {
    tracing::trace!(sql = %clipped(sql), "hatch statement");
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn statement(_sql: &str) {}

/// Run `future` inside `span`.
///
/// The only way a span covers asynchronous work here. Entering one by hand
/// and holding the guard across an `.await` would attribute whatever the
/// executor runs next to this span; an instrumented future enters on each
/// poll and exits on each yield, which is the arrangement `tracing` documents
/// for exactly this reason.
#[cfg(feature = "tracing")]
pub(crate) fn instrument<F>(span: Span, future: F) -> impl std::future::Future<Output = F::Output>
where
    F: std::future::Future,
{
    tracing::Instrument::instrument(future, span)
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn instrument<F>(_span: Span, future: F) -> impl std::future::Future<Output = F::Output>
where
    F: std::future::Future,
{
    future
}

/// Report a refusal at `warn`, and hand the outcome back untouched.
///
/// A refusal is the store declining to do what it was asked because doing it
/// would produce a wrong answer — the history is gone, no export covers it, a
/// consumer has not caught up, or the hatch was asked to write. Those are the
/// four an operator wants in a log without turning on `debug`, so they are
/// the four this recognises; everything else travels as it always did.
///
/// Wrapped around the outcome rather than raised at the point of refusal
/// because that point is inside a closure running on the SQLite thread, where
/// the span this belongs under is not current.
pub(crate) fn refusal<T>(outcome: Result<T>) -> Result<T> {
    if let Err(error) = &outcome {
        refused(error);
    }
    outcome
}

#[cfg(feature = "tracing")]
fn refused(error: &Error) {
    let refusal = match error {
        Error::Truncated { .. } => "truncated",
        Error::NotExported { .. } => "not_exported",
        Error::ConsumerBehind { .. } => "consumer_behind",
        Error::Unsupported(message) if message.starts_with("not authorized inside the hatch") => {
            "denied"
        }
        _ => return,
    };
    // The error's own text, which is the whole message: it names the
    // coordinates and, for `ConsumerBehind`, the consumer. None of the three
    // carries an event's `data` or `meta`.
    tracing::warn!(refusal, error = %error, "refused");
}

#[cfg(not(feature = "tracing"))]
fn refused(_error: &Error) {}

/// `PRAGMA user_version`, read only when somebody is instrumented to care.
///
/// The ladder's before-and-after is the one field in this crate that costs a
/// query to answer, so the query is part of the instrumentation rather than
/// part of opening a log: with the feature off this is a constant and the
/// pragma is never read.
#[cfg(feature = "tracing")]
pub(crate) fn user_version(conn: &rusqlite::Connection) -> i64 {
    conn.pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap_or(-1)
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn user_version(_conn: &rusqlite::Connection) -> i64 {
    -1
}

/// `PRAGMA freelist_count`, read only when somebody is instrumented to care.
///
/// What `eventsdb.reclaim` reports is this number before the vacuum minus
/// this number after it, and each is a pragma read on the writer — so, as
/// with [`user_version`], the read is part of the instrumentation. With the
/// feature off both sides are this constant and the difference is `0`, which
/// `Span::record` on the shim drops anyway.
#[cfg(feature = "tracing")]
pub(crate) fn freelist_count(conn: &rusqlite::Connection) -> i64 {
    conn.pragma_query_value(None, "freelist_count", |row| row.get(0))
        .unwrap_or(-1)
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn freelist_count(_conn: &rusqlite::Connection) -> i64 {
    -1
}

/// Whether an index by this name is already in `sqlite_master`, read only
/// when somebody is instrumented to care.
///
/// `CREATE INDEX IF NOT EXISTS` reports nothing about which of its two
/// outcomes happened, and `eventsdb.index` wants to say which — a caller who
/// waited on the writer for a while wants to know whether it was the scan or
/// the queue. One row of `sqlite_master`, which is a table of a few dozen
/// rows, and not read at all with the feature off: the answer is then
/// `false`, and the field it would have fed is dropped by the shim.
#[cfg(feature = "tracing")]
pub(crate) fn index_exists(conn: &rusqlite::Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
        [name],
        |row| row.get::<_, i64>(0),
    )
    .map(|count| count > 0)
    .unwrap_or(false)
}

#[cfg(not(feature = "tracing"))]
pub(crate) fn index_exists(_conn: &rusqlite::Connection, _name: &str) -> bool {
    false
}

/// How much of a statement the `sql` field carries.
///
/// The hatch's SQL is the caller's text and a literal in it is the caller's
/// data, which is why it is only ever emitted at `trace` — and why what is
/// emitted is bounded. Long enough that the shape of a statement is legible,
/// short enough that a generated `IN (...)` of ten thousand holes does not
/// land in a log line.
#[cfg(feature = "tracing")]
const SQL_LIMIT: usize = 256;

/// The first [`SQL_LIMIT`] characters of `sql`, marked when there were more.
///
/// By characters rather than bytes: slicing a `&str` at a byte that is not a
/// boundary panics, and SQL is caller text.
#[cfg(feature = "tracing")]
fn clipped(sql: &str) -> String {
    let mut out: String = sql.chars().take(SQL_LIMIT).collect();
    if out.chars().count() < sql.chars().count() {
        out.push('…');
    }
    out
}
