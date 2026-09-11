//! The error a store call can fail with.
//!
//! The variants are split by what a caller can *do* about them, not by where
//! they were raised. `Busy` says another call is worth making; `Validation`
//! says the event was wrong and no retry will change that; `Storage` says
//! somewhere the bytes had to go or come from failed; `Unsupported` says the
//! request was well-formed and this backend has no answer for it.

use thiserror::Error;

/// The result of every fallible store operation.
pub type Result<T> = std::result::Result<T, Error>;

/// `#[non_exhaustive]` because this list is not finished.
///
/// Several open questions end in "add a variant", and each of those is a
/// breaking change to every caller that matches exhaustively — unless the
/// attribute is there first. It costs a `_` arm today and buys the right to
/// name a new failure later without a major version.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The event did not satisfy the envelope contract. The message names the
    /// offending key and says what belongs there instead, because a caller
    /// that has to guess will guess the same way twice.
    #[error("invalid event: {0}")]
    Validation(String),

    /// The write could not take the lock in time. Retrying is meaningful:
    /// this is contention, not a defect in the request.
    #[error("store busy: {0}")]
    Busy(String),

    /// A deadline the caller set was reached, and the statement was
    /// interrupted.
    ///
    /// Deliberately not [`Error::Busy`]. Busy means someone else holds the
    /// lock, and the same call may well succeed next time; this means the work
    /// itself was longer than the caller allowed, and repeating it unchanged
    /// will take just as long. Nothing retries it automatically.
    #[error("exceeded the deadline: {0}")]
    Timeout(String),

    /// Somewhere the bytes had to go or come from failed: the database could
    /// not read, could not write, could not open — or a [`crate::Sink`] the
    /// caller supplied refused the page it was handed.
    ///
    /// Retry-or-call-someone, which is what puts those together: this module
    /// divides by what a caller can do, not by which component raised it.
    /// Contrast [`Error::Corruption`], which is the same call site's other
    /// outcome and wants the opposite response.
    #[error("storage failure: {0}")]
    Storage(String),

    /// The read succeeded and the bytes do not mean what they should.
    ///
    /// Split from [`Error::Storage`] because this module's rule is that
    /// variants divide by what a caller can *do*, and these two divide
    /// cleanly: a failing disk is worth retrying and worth paging someone
    /// about; bytes that do not decode are worth stopping for. Retrying reads
    /// the same bytes again.
    ///
    /// Two things arrive here, and the second is the common one:
    ///
    /// - a stored `meta` or `data` column that will not parse as JSON — out-of-
    ///   band tampering, an interrupted maintenance job, a version skew;
    /// - an event that does not read as an event *after the upcaster chain
    ///   ran*. That is almost always a bug in the chain rather than in the
    ///   file: a step dropped a field the next one needs. It was reported as a
    ///   storage failure before, which sent readers to look at the disk.
    ///
    /// Either way a dropped row must never read as an empty stream — a fold
    /// over a silently truncated log produces a wrong state rather than an
    /// obvious failure.
    #[error("stored data does not decode: {0}")]
    Corruption(String),

    /// The request is well-formed and this backend cannot serve it: an
    /// in-memory store asked to write a second stream, a store that is not a
    /// database asked to answer SQL.
    #[error("unsupported by this backend: {0}")]
    Unsupported(String),

    /// The history the caller asked for is partly gone: retention removed
    /// events at or below `removed_up_to`, and a fold starting at `requested`
    /// would be missing them.
    ///
    /// Loud on purpose. Retention is the one operation that can make a
    /// correct-looking read wrong, so a consumer whose cursor sits behind the
    /// watermark is told rather than handed a short answer it cannot tell
    /// from a complete one.
    #[error(
        "history at or below position {removed_up_to} has been removed; \
         reading from {requested} would be incomplete"
    )]
    Truncated { requested: u64, removed_up_to: u64 },

    /// The stream had moved on: the head the caller expected is not the head
    /// the write found, so nothing was appended.
    ///
    /// Both coordinates are given because the caller's next move needs both —
    /// `expected` is where it left off and `actual` is what to read up to, so
    /// it can fold only what it missed rather than the stream from the start.
    ///
    /// Deliberately not [`Error::Busy`]: nothing is contended, and repeating
    /// the same call unchanged will fail the same way. The caller has to
    /// decide again against a state it has not seen.
    #[error("stream head is {actual:?}, not the expected {expected:?}")]
    HeadMismatch {
        expected: crate::store::Expected,
        actual: crate::store::Expected,
    },

    /// Retention would have removed events a registered consumer has not seen.
    #[error(
        "retention would remove up to position {up_to}, past consumer `{consumer}` at {cursor}"
    )]
    ConsumerBehind {
        consumer: String,
        cursor: u64,
        up_to: u64,
    },

    /// Retention would have removed events no confirmed export covers.
    ///
    /// `exported_through` is how far the chain of landed, unfiltered exports
    /// reaches from the beginning; the plan wanted to remove up to `up_to`.
    /// The gap between them is history that would exist nowhere afterwards.
    #[error(
        "retention would remove up to position {up_to}, but confirmed exports \
         reach only {exported_through}"
    )]
    NotExported { up_to: u64, exported_through: u64 },
}

impl Error {
    /// Whether another attempt at the same call is worth making.
    ///
    /// Contention only. A [`Error::Timeout`] is not busy: the work was longer
    /// than the caller allowed, and repeating it unchanged will be too.
    pub fn is_busy(&self) -> bool {
        matches!(self, Error::Busy(_))
    }

    /// Whether a deadline was reached.
    pub fn is_timeout(&self) -> bool {
        matches!(self, Error::Timeout(_))
    }

    /// Public so a backend crate builds the same refusals this one does.
    pub fn validation(message: impl Into<String>) -> Self {
        Error::Validation(message.into())
    }

    /// Public for the same reason as [`Error::validation`].
    pub fn storage(message: impl Into<String>) -> Self {
        Error::Storage(message.into())
    }

    /// Public for the same reason as [`Error::validation`].
    pub fn corruption(message: impl Into<String>) -> Self {
        Error::Corruption(message.into())
    }

    /// Whether the bytes read back are the problem, rather than the reading.
    pub fn is_corruption(&self) -> bool {
        matches!(self, Error::Corruption(_))
    }
}
