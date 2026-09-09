//! Removing events, and being honest about having done it.
//!
//! # Retention is the one operation that can make a correct read wrong
//!
//! Everything else in this store is append-only, so a reader that asks the
//! right question gets a right answer. Deleting breaks that: a fold that
//! starts before the deleted range comes back short, and nothing in the shape
//! of the result says so. A count is a count whether or not the rows it
//! counted still exist.
//!
//! So removal is not just a `DELETE`. Every application records what it took
//! in a ledger that outlives the events, and the highest position it removed
//! is a **watermark**. A consumer whose cursor sits at or below the watermark
//! is refused with [`Error::Truncated`] rather than served a short answer it
//! cannot distinguish from a complete one.
//!
//! # Positions go sparse, and that is safe
//!
//! Removing a stream's events leaves holes in the global order. This does not
//! reintroduce the problem [`eventsdb_core::Position`] is careful about: that
//! one is *temporal* — position `n + 1` visible while `n` is still
//! uncommitted — and it is prevented by allocating inside the committing
//! transaction under a single writer. A permanently absent position is
//! different in kind. Nothing waits for a specific position; a subscription
//! reads `position > cursor` in order and simply does not see what is gone.
//!
//! What would break is a **reused** position, because a cursor past it would
//! silently skip the new event that took its number. That is what
//! `AUTOINCREMENT` on the `position` column prevents, and it is the reason
//! that decision was made before there was anything to delete.
//!
//! # Deleting does not shrink the file by itself
//!
//! SQLite keeps freed pages for reuse. [`SqliteEventLog::reclaim`] returns
//! them to the filesystem incrementally — which works because
//! `auto_vacuum = INCREMENTAL` is set at creation. See
//! [`crate::schema::apply_pragmas`].

use eventsdb_core::error::{Error, Result};
use eventsdb_core::event::now_ms;
use eventsdb_core::position::Position;
use rusqlite::{Connection, TransactionBehavior};

use crate::log::SqliteEventLog;
use crate::shared::{classify, map_isle};

/// What to remove.
#[derive(Debug, Clone)]
pub enum Plan {
    /// Every event at or below `position`. The prefix-truncation shape, for a
    /// log read as one sequence.
    Before(Position),

    /// Every event whose time coordinate is before `epoch_ms`. The age shape.
    ///
    /// **Which moment that coordinate names depends on how the event was
    /// written** — the wall clock of an ordinary append, the source's time for
    /// a backfill, the original for a transfer (see
    /// [`eventsdb_core::event`]). On an append-only log the three coincide and
    /// this is a position prefix. On a log with backfilled or imported
    /// history it is not: old events can sit at high positions, so this
    /// removes a scattered set, exactly as [`Plan::Streams`] does.
    ///
    /// That is safe — the watermark handles a scattered removal, and §
    /// "Positions go sparse" above says why — but it is a different answer
    /// than the same call gives on a log written only by `append`.
    OlderThan(u64),

    /// Every event of these streams. The shape that fits a log of many short
    /// streams — a finished session, a closed chapter — where retention means
    /// dropping whole ones rather than trimming a front.
    Streams(Vec<String>),
}

impl Plan {
    /// The `WHERE` fragment and its parameters.
    ///
    /// Fallible because both numeric plans bind a `u64` into a column SQLite
    /// stores as `i64`. Wrapping would make `OlderThan(u64::MAX)` — "remove
    /// everything older than the far future" — match `epoch_ms < -1`, remove
    /// nothing, and report success. A wrong answer with no error is worse than
    /// a refusal.
    fn predicate(&self) -> Result<(String, Vec<Box<dyn rusqlite::ToSql>>)> {
        match self {
            Plan::Before(position) => Ok((
                "position <= ?1".to_string(),
                vec![Box::new(crate::log::stored_position(*position)?)],
            )),
            Plan::OlderThan(epoch_ms) => {
                let cutoff = i64::try_from(*epoch_ms).map_err(|_| {
                    Error::validation(format!(
                        "cutoff {epoch_ms} is beyond the range SQLite stores as a timestamp"
                    ))
                })?;
                Ok(("epoch_ms < ?1".to_string(), vec![Box::new(cutoff)]))
            }
            Plan::Streams(streams) => {
                if streams.is_empty() {
                    // Selects nothing, rather than everything — the reading a
                    // caller that passed an empty list meant.
                    return Ok(("0".to_string(), Vec::new()));
                }
                crate::log::check_placeholders(streams.len(), "streams")?;
                let holes: Vec<String> = (1..=streams.len()).map(|i| format!("?{i}")).collect();
                let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::with_capacity(streams.len());
                for stream in streams {
                    params.push(Box::new(stream.clone()));
                }
                Ok((format!("stream IN ({})", holes.join(", ")), params))
            }
        }
    }

    /// How this application is described in the ledger. Short by design: the
    /// ledger records that a removal happened and how far it reached, not a
    /// replayable command.
    fn describe(&self) -> String {
        match self {
            Plan::Before(position) => format!("before:{position}"),
            Plan::OlderThan(epoch_ms) => format!("older_than:{epoch_ms}"),
            Plan::Streams(streams) => format!("streams:{}", streams.len()),
        }
    }
}

/// Who is allowed to be left behind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Guard {
    /// Refuse if any consumer with a stored checkpoint has not passed the
    /// events this plan would remove. The default, and the one that makes the
    /// safe path the easy one.
    ///
    /// It can only see consumers that have **saved a checkpoint**. One that
    /// has never reported is invisible here, and a caller relying on this
    /// guard should make its consumers check in before their first batch.
    #[default]
    RegisteredConsumers,

    /// Remove regardless. For a caller that knows the consumers are gone, or
    /// that has accepted the loss.
    Force,
}

/// What an application of a [`Plan`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub removed: usize,
    /// The highest position removed, or `None` when nothing matched.
    pub highest_removed: Option<Position>,
    pub streams_affected: usize,
}

impl Report {
    pub fn nothing() -> Self {
        Report {
            removed: 0,
            highest_removed: None,
            streams_affected: 0,
        }
    }
}

/// Whether the history from a given position is all still there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completeness {
    Complete,
    /// Events at or below `removed_up_to` are gone, so a fold from the
    /// requested position would be missing some of its input.
    Incomplete {
        removed_up_to: Position,
    },
}

impl Completeness {
    pub fn is_complete(self) -> bool {
        matches!(self, Completeness::Complete)
    }
}

/// The highest position any retention has removed, or
/// [`Position::BEGINNING`] when none has run.
///
/// Takes `&Connection` so a projection runner can read it inside the same
/// transaction as the batch it is about to fold.
pub(crate) fn watermark(conn: &Connection) -> Result<Position> {
    conn.query_row(
        "SELECT COALESCE(MAX(highest_removed), 0) FROM retention",
        [],
        |row| row.get::<_, i64>(0),
    )
    .map(|highest| Position::new(highest as u64))
    .map_err(classify)
}

/// Refuse when `from` sits at or below the watermark.
pub(crate) fn require_complete_from(conn: &Connection, from: Position) -> Result<()> {
    let watermark = watermark(conn)?;
    if from < watermark {
        return Err(Error::Truncated {
            requested: from.get(),
            removed_up_to: watermark.get(),
        });
    }
    Ok(())
}

impl SqliteEventLog {
    /// Apply `plan`, subject to `guard`.
    ///
    /// The count, the guard check, the delete and the ledger entry are one
    /// `IMMEDIATE` transaction, so a concurrent append cannot land inside the
    /// range between deciding and deleting, and the ledger can never disagree
    /// with what is actually gone.
    pub async fn retain(&self, plan: Plan, guard: Guard) -> Result<Report> {
        let shared = self.shared_handle();
        let described = plan.describe();

        let job = move |conn: &mut Connection| {
            Ok((|| {
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(classify)?;

                let (predicate, params) = plan.predicate()?;

                // What would go, decided under the write lock.
                let (removed, highest, streams): (i64, Option<i64>, i64) = tx
                    .query_row(
                        &format!(
                            "SELECT COUNT(*), MAX(position), COUNT(DISTINCT stream) \
                             FROM events WHERE {predicate}"
                        ),
                        rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .map_err(classify)?;

                let Some(highest) = highest else {
                    return Ok(Report::nothing());
                };

                if guard == Guard::RegisteredConsumers {
                    // A consumer at or above the highest removed position has
                    // already seen everything this plan takes. One below it
                    // has not, and never will — so it is named rather than
                    // quietly overrun.
                    let behind: Option<(String, i64)> = tx
                        .query_row(
                            "SELECT consumer, position FROM checkpoints \
                             WHERE position < ?1 ORDER BY position LIMIT 1",
                            [highest],
                            |row| Ok((row.get(0)?, row.get(1)?)),
                        )
                        .map(Some)
                        .or_else(|error| match error {
                            rusqlite::Error::QueryReturnedNoRows => Ok(None),
                            other => Err(classify(other)),
                        })?;

                    if let Some((consumer, cursor)) = behind {
                        return Err(Error::ConsumerBehind {
                            consumer,
                            cursor: cursor as u64,
                            up_to: highest as u64,
                        });
                    }
                }

                tx.execute(
                    &format!("DELETE FROM events WHERE {predicate}"),
                    rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
                )
                .map_err(classify)?;

                // The ledger outlives what it describes. It is the only
                // remaining evidence those events existed, so it is written in
                // the same transaction that removes them.
                tx.execute(
                    "INSERT INTO retention (applied_ms, plan, removed_count, highest_removed) \
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![now_ms() as i64, described, removed, highest],
                )
                .map_err(classify)?;

                tx.commit().map_err(classify)?;

                Ok(Report {
                    removed: removed as usize,
                    highest_removed: Some(Position::new(highest as u64)),
                    streams_affected: streams as usize,
                })
            })())
        };

        match shared.isle.call(job).await {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }

    /// The highest position retention has removed, or
    /// [`Position::BEGINNING`] when none has run.
    pub async fn removed_watermark(&self) -> Result<Position> {
        let shared = self.shared_handle();
        match shared
            .isle
            .call(|conn: &mut Connection| Ok(watermark(conn)))
            .await
        {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }

    /// Whether a fold starting at `from` would see everything it needs.
    ///
    /// Reads themselves do not refuse — a range read returns the range that
    /// exists, which is an honest answer to what it was asked. This is the
    /// question to ask before *interpreting* such a read as a complete
    /// history, and it is what a projection runner asks on the caller's
    /// behalf.
    pub async fn completeness_from(&self, from: Position) -> Result<Completeness> {
        let watermark = self.removed_watermark().await?;
        Ok(if from < watermark {
            Completeness::Incomplete {
                removed_up_to: watermark,
            }
        } else {
            Completeness::Complete
        })
    }

    /// Return freed pages to the filesystem.
    ///
    /// Deleting rows does not shrink the file; SQLite keeps the pages for
    /// reuse. This gives them back incrementally rather than through a full
    /// `VACUUM`, which would rewrite the whole database under an exclusive
    /// lock. It depends on `auto_vacuum = INCREMENTAL` having been set when
    /// the database was created — on an older file it is a no-op.
    pub async fn reclaim(&self) -> Result<()> {
        let shared = self.shared_handle();
        match shared
            .isle
            .call(|conn: &mut Connection| {
                Ok(conn
                    .execute_batch("PRAGMA incremental_vacuum;")
                    .map_err(classify))
            })
            .await
        {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }
}
