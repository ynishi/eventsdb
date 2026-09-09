//! The error a store call can fail with.
//!
//! The variants are split by what a caller can *do* about them, not by where
//! they were raised. `Busy` says another call is worth making; `Validation`
//! says the event was wrong and no retry will change that; `Storage` says the
//! database itself failed; `Unsupported` says the request was well-formed and
//! this backend has no answer for it.

use thiserror::Error;

/// The result of every fallible store operation.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
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

    /// The database failed, or a stored row could not be decoded. A dropped
    /// row must never read as an empty stream — a fold over a truncated log
    /// produces a wrong state rather than an obvious failure.
    #[error("storage failure: {0}")]
    Storage(String),

    /// The request is well-formed and this backend cannot serve it: an
    /// in-memory store asked to write a second stream, a store that is not a
    /// database asked to answer SQL.
    #[error("unsupported by this backend: {0}")]
    Unsupported(String),
}

impl Error {
    /// Whether another attempt at the same call is worth making.
    pub fn is_busy(&self) -> bool {
        matches!(self, Error::Busy(_))
    }

    /// Public so a backend crate builds the same refusals this one does.
    pub fn validation(message: impl Into<String>) -> Self {
        Error::Validation(message.into())
    }

    /// Public for the same reason as [`Error::validation`].
    pub fn storage(message: impl Into<String>) -> Self {
        Error::Storage(message.into())
    }
}
