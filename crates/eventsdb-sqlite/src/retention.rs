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
//!
//! # Removing only what is known to exist elsewhere
//!
//! Retention deletes; it does not archive. An export is a `Vec` handed to the
//! caller, and where it goes — a file, another log, an object store — is the
//! caller's, along with the format and the retry. The store cannot make
//! "export, then delete" one transaction, because the second half of the
//! export happens somewhere the transaction does not reach.
//!
//! What it can do is refuse to delete what nobody has said is safe. The
//! shape is the one Kafka's tiered storage and KurrentDB's archiving use —
//! a segment is eligible for deletion only after it has been uploaded — with
//! the difference that the upload is the caller's, so the store learns of it
//! in two steps rather than performing it:
//!
//! ```text
//!   export_recorded(from, filter, limit)
//!        │  reads the page off a reader, hands it back,
//!        │  and writes an `exports` row: taken, not yet landed
//!        ▼
//!   ┌─────────────────────────────────────────────┐
//!   │ exports │ from │ through │ whole │ landed_ms │
//!   │   #7    │  0   │   256   │  yes  │   NULL    │  ◀── taken
//!   └─────────────────────────────────────────────┘
//!        │  the caller writes the page wherever it goes …
//!        ▼
//!   confirm_export(7)          … and says so
//!        │
//!        ▼
//!   │   #7    │  0   │   256   │  yes  │  1757…   │  ◀── landed
//!
//!   retain(plan, Guard::Exported)
//!        │  chains the landed, whole rows from position 0 —
//!        │  #7 reaches 256, #8 (256‥512) landed reaches 512, …
//!        │  — and refuses if the plan would remove past the chain's end
//!        ▼
//!   Err(NotExported { up_to, exported_through })   or   the delete
//! ```
//!
//! A row that was taken and never confirmed is a page that may or may not
//! exist anywhere; the guard treats it as if it did not. A filtered export
//! (`whole = 0`) preserved some of its range and cannot vouch for the rest,
//! so it never extends the chain. A gap in the chain — page *k+1* landed,
//! page *k* not — stops it at *k*: what lies beyond may be preserved, but not
//! contiguously with what came before, and a history with a hole in it is
//! the thing this module exists to refuse.
//!
//! Which of the guards to use is policy, and policy stays with the caller:
//! [`Guard::Exported`] is offered, not defaulted. What the store no longer
//! allows is for "I exported it first" to be a thing the caller merely
//! remembers.

use eventsdb_core::error::{Error, Result};
use eventsdb_core::event::now_ms;
use eventsdb_core::log::Filter;
use eventsdb_core::position::Position;
use eventsdb_core::transfer::ExportedEvent;
use rusqlite::{Connection, TransactionBehavior};

use crate::log::SqliteEventLog;
use crate::shared::{classify, map_isle};

/// What to remove.
///
/// `#[non_exhaustive]` for the same reason as [`Error`]: the shapes worth
/// removing by are not a closed set — an archive-then-remove plan is the
/// obvious next one — and adding a variant should not be a breaking change.
#[derive(Debug, Clone)]
#[non_exhaustive]
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
///
/// `#[non_exhaustive]`: a `match` on it needs a wildcard arm, so a guard can
/// be added without breaking a caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
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

    /// Refuse unless confirmed exports cover everything this plan would
    /// remove, and — as [`Guard::RegisteredConsumers`] — refuse to overrun a
    /// stored checkpoint.
    ///
    /// "Cover" is the chain of landed, whole export receipts from the
    /// beginning of the log (see the module doc): the plan may remove up to
    /// the chain's end and no further, and a plan that would is refused with
    /// [`Error::NotExported`]. An export taken and never confirmed, a filtered
    /// one, or a page missing from the chain all leave the end where it was.
    Exported,
}

/// What [`SqliteEventLog::export_recorded`] wrote down about one page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportReceipt {
    /// The row's identity, for [`SqliteEventLog::confirm_export`].
    pub id: u64,
    /// The `from` the page was read with — exclusive, as `read_all`'s is.
    pub from: Position,
    /// The position of the last event in the page, or `from` when the page
    /// was empty.
    pub through: Position,
    pub count: usize,
    /// Whether the filter selected every event in the range. Only a whole
    /// export can vouch for the history it covers.
    pub whole: bool,
}

/// How far the chain of landed, whole exports reaches from the beginning.
///
/// Takes `&Connection` so `retain` can ask inside the transaction that
/// decides what to remove.
pub(crate) fn exported_through(conn: &Connection) -> Result<Position> {
    let mut stmt = conn
        .prepare(
            "SELECT from_position, through FROM exports \
             WHERE landed_ms IS NOT NULL AND whole = 1 \
             ORDER BY from_position, through",
        )
        .map_err(classify)?;
    let rows = stmt
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .map_err(classify)?;

    let mut reach: i64 = 0;
    for row in rows {
        let (from, through) = row.map_err(classify)?;
        // A page that starts at or before the reach and ends past it extends
        // the chain; one that starts beyond it is past a gap and does not.
        if from <= reach && through > reach {
            reach = through;
        }
    }
    Ok(Position::new(reach as u64))
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
///
/// `#[non_exhaustive]`: a `match` on it needs a wildcard arm. The common
/// question has a method, [`Completeness::is_complete`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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

                if guard == Guard::Exported {
                    // The chain of confirmed exports has to reach at least as
                    // far as this plan would remove. Checked under the same
                    // lock as the delete, so a confirmation cannot land in
                    // between and make the refusal stale.
                    let reach = exported_through(&tx)?;
                    if (highest as u64) > reach.get() {
                        return Err(Error::NotExported {
                            up_to: highest as u64,
                            exported_through: reach.get(),
                        });
                    }
                }

                if guard != Guard::Force {
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

    /// [`EventLog::export`](eventsdb_core::EventLog::export), with a receipt.
    ///
    /// The page is read the same way — off a reader, as stored, exclusive on
    /// `from` — and then a row is written saying it was handed out: the
    /// range, the count, and whether `filter` selected everything in it. The
    /// row is *taken*, not *landed*; where the page goes is the caller's, and
    /// [`SqliteEventLog::confirm_export`] is how the caller says it arrived.
    /// Until then [`Guard::Exported`] does not count it.
    ///
    /// The receipt's `through` is the last event's position, which is also
    /// the `from` of the next page — so paging with receipts is the same loop
    /// as paging without, with one confirmation per page.
    pub async fn export_recorded(
        &self,
        from: Position,
        filter: &Filter,
        limit: usize,
    ) -> Result<(Vec<ExportedEvent>, ExportReceipt)> {
        let events = crate::transfer::export(self, from, filter, limit).await?;
        // `export` fills the witness on every record it produces, so the
        // fallback is the empty page's, not a record's missing one.
        let through = events.last().and_then(|last| last.position).unwrap_or(from);
        let count = events.len();
        // Every axis, so an axis added to `Filter` cannot quietly widen what
        // counts as whole. `meta` and `stream_prefix` are compared against
        // what places no condition rather than against `None`, which is the
        // reading each of those axes documents: an empty pair list and an
        // empty prefix both select everything.
        let whole = filter.kinds.is_none()
            && filter.streams.is_none()
            && filter.meta.as_ref().is_none_or(|pairs| pairs.is_empty())
            && filter
                .stream_prefix
                .as_ref()
                .is_none_or(|prefix| prefix.is_empty());

        let shared = self.shared_handle();
        let taken_ms = now_ms() as i64;
        let from_stored = crate::log::stored_position(from)?;
        let through_stored = crate::log::stored_position(through)?;
        let id = match shared
            .isle
            .call(move |conn: &mut Connection| {
                Ok(conn
                    .execute(
                        "INSERT INTO exports \
                         (taken_ms, from_position, through, count, whole, landed_ms) \
                         VALUES (?1, ?2, ?3, ?4, ?5, NULL)",
                        rusqlite::params![
                            taken_ms,
                            from_stored,
                            through_stored,
                            count as i64,
                            i64::from(whole)
                        ],
                    )
                    .map(|_| conn.last_insert_rowid())
                    .map_err(classify))
            })
            .await
        {
            Ok(inner) => inner?,
            Err(isle) => return Err(map_isle(isle)),
        };

        Ok((
            events,
            ExportReceipt {
                id: id as u64,
                from,
                through,
                count,
                whole,
            },
        ))
    }

    /// Say that the page behind `receipt` has landed where it was going.
    ///
    /// Idempotent: confirming twice is one confirmation. An id no receipt
    /// has is refused as validation — it is a caller mixing up receipts, and
    /// quietly accepting it would let [`Guard::Exported`] vouch for a page
    /// nobody took.
    pub async fn confirm_export(&self, receipt: u64) -> Result<()> {
        let shared = self.shared_handle();
        let id = i64::try_from(receipt)
            .map_err(|_| Error::validation(format!("no export receipt {receipt}")))?;
        let landed_ms = now_ms() as i64;
        match shared
            .isle
            .call(move |conn: &mut Connection| {
                Ok((|| {
                    let changed = conn
                        .execute(
                            "UPDATE exports SET landed_ms = ?1 \
                             WHERE id = ?2 AND landed_ms IS NULL",
                            rusqlite::params![landed_ms, id],
                        )
                        .map_err(classify)?;
                    if changed == 1 {
                        return Ok(());
                    }
                    let exists: bool = conn
                        .query_row("SELECT 1 FROM exports WHERE id = ?1", [id], |_| Ok(true))
                        .or_else(|error| match error {
                            rusqlite::Error::QueryReturnedNoRows => Ok(false),
                            other => Err(classify(other)),
                        })?;
                    if exists {
                        Ok(())
                    } else {
                        Err(Error::validation(format!("no export receipt {receipt}")))
                    }
                })())
            })
            .await
        {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }

    /// How far confirmed, whole exports reach from the beginning of the log
    /// — the most a plan under [`Guard::Exported`] may remove.
    pub async fn exported_through(&self) -> Result<Position> {
        let shared = self.shared_handle();
        match shared
            .isle
            .call(|conn: &mut Connection| Ok(exported_through(conn)))
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
