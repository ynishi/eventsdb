//! What is in the log: the five listings.
//!
//! # Every other read takes a name the caller already holds
//!
//! `stream_handle` takes a stream id, `checkpoint_load` a consumer name,
//! `confirm_export` a receipt id. None of them answers *which ones are
//! there*, and the five tables that know — `stream_seq`, `events`,
//! `checkpoints`, `retention`, `exports` — were reachable only through the
//! escape hatch. Which made the contract for "list my streams" the column
//! layout of a reserved table, something the migration ladder is free to
//! change at any step.
//!
//! So: five methods, one per table, rather than one `catalog()` returning all
//! five at once. A catalogue cannot page and these have to — a log of many
//! short streams has many streams, and a long-running one has many ledger
//! rows. [`SqliteEventLog::streams`] states the paging rule the other four
//! follow.
//!
//! # Retention leaves each table in a different state, and the listings say so
//!
//! ```text
//!   retain(Plan::Streams(["session-1"]))
//!        │
//!        ├─ events        rows gone ────────▶ kinds():  a kind whose every
//!        │                                              event went does not list
//!        ├─ stream_seq    untouched ────────▶ streams(): session-1 still lists,
//!        │                                              head_seq intact, no events
//!        ├─ checkpoints   untouched ────────▶ checkpoints(): unchanged
//!        ├─ retention     one row appended ─▶ retention_ledger(): the removal
//!        └─ exports       untouched ────────▶ export_receipts(): unchanged
//! ```
//!
//! The counter is the truth of a stream's existence — that is
//! [`eventsdb_core::store::Expected::Unwritten`]'s reading, and it is why an
//! emptied stream still lists rather than being smoothed away: a caller about
//! to reuse the name needs to know the counter stands at 50. A kind has no
//! counter, so "was this kind ever written" is not a question this store can
//! answer once the events are gone, and the listing says that rather than
//! guessing.
//!
//! # Reads, off a reader
//!
//! None of these needs the writer and none needs to see uncommitted work, so
//! each goes to `Shared::reader` — the same connection `read_all` uses, for
//! the same reason: a listing must not queue behind a write.

use eventsdb_core::error::Result;
use eventsdb_core::log::stream_prefix_bound;
use eventsdb_core::position::Position;
use rusqlite::Connection;

use crate::log::SqliteEventLog;
use crate::retention::ExportReceipt;
use crate::shared::{classify, map_isle};
use crate::store::clamp_limit;

/// One stream, and where its counter stands.
///
/// Public fields and no `#[non_exhaustive]`, which is the shape
/// [`crate::Report`] and [`ExportReceipt`] already have: a row read back out
/// of a reserved table is a record a caller only ever *receives*, so the
/// marker would buy nothing a caller could use and would cost the
/// destructuring these are read with. What could add a field to it is a
/// column added by the migration ladder, and a ladder step is a version bump
/// on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    pub stream: String,
    /// The `seq` of the last event appended to this stream: the stored
    /// `next_seq` minus one, which is the same arithmetic
    /// [`eventsdb_core::EventStore::append_if`] does to answer with
    /// [`eventsdb_core::store::Expected::Seq`].
    ///
    /// **Not a count of what is there now.** Retention does not touch
    /// `stream_seq`, so a stream whose every event has been removed keeps the
    /// head it reached; `head_seq` is where the stream got to, and the next
    /// append is `head_seq + 1`.
    ///
    /// **`0` is not reachable through this API**, and the arithmetic is
    /// still spelled out rather than assumed away. A counter is stored only
    /// *after* an append, so the lowest `next_seq` this crate writes is 2,
    /// and the ladder's backfill takes `MAX(seq) + 1` over rows that exist;
    /// writing the counter by hand is refused wherever a caller could try it,
    /// inside [`SqliteEventLog::with_transaction`] and a projection's `apply`
    /// alike. What is left is a `sqlite3` session on the file, which the
    /// authorizer does not reach — and a row it left at `next_seq` 1 reads
    /// here as the 0 it is, which is
    /// [`eventsdb_core::store::Expected::Unwritten`]'s reading of the same
    /// counter, rather than being hidden or wrapped around.
    pub head_seq: u64,
}

/// Where one consumer sits, as [`eventsdb_core::EventLog::checkpoint_save`]
/// last left it.
///
/// Public fields and no `#[non_exhaustive]`; see [`StreamInfo`] for why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerCheckpoint {
    pub consumer: String,
    /// The position the consumer reported, exclusive as every cursor here is:
    /// it has handled everything at or below this.
    pub position: Position,
    /// Wall clock of the last report, milliseconds since the Unix epoch. The
    /// store's clock at the moment of the save, so it dates the report and
    /// orders nothing — see [`eventsdb_core::event`].
    pub updated_ms: u64,
}

/// One application of a [`crate::Plan`], as the ledger recorded it.
///
/// Public fields and no `#[non_exhaustive]`; see [`StreamInfo`] for why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionEntry {
    /// The ledger row's id, and the cursor
    /// [`SqliteEventLog::retention_ledger`] pages by.
    pub id: u64,
    /// Wall clock of the application, milliseconds since the Unix epoch.
    pub applied_ms: u64,
    /// How the application described itself — `before:120`, `older_than:…`,
    /// `streams:3`, or `archive-then-remove: <plan>` for one that preserved
    /// first.
    ///
    /// **A description, not a replayable command**, and the store does not
    /// interpret it: it is written as a string and handed back as the string
    /// it is. A caller that parses it is reading a rendering that belongs to
    /// [`crate::Plan`], not a contract this listing makes.
    pub plan: String,
    /// How many events that application removed.
    pub removed_count: usize,
    /// The highest position it removed. Always a real position: a plan that
    /// matched nothing returns [`crate::Report::nothing`] and writes no row.
    pub highest_removed: Position,
}

/// One export receipt, with the two moments the table keeps for it.
///
/// A wrapper around [`ExportReceipt`] rather than two more fields on it, for
/// two reasons. `ExportReceipt` has public fields and no
/// `#[non_exhaustive]`, so adding to it is a breaking change — but that is
/// the smaller half. The larger one is that the two timestamps are not facts
/// the *returning* path has: [`SqliteEventLog::export_recorded`] hands back a
/// receipt at the moment it writes the row, where `taken_ms` is "now" and
/// `landed_ms` is structurally always `None`. They become facts later, which
/// is exactly when this listing reads them.
///
/// So `ExportReceipt` stays what a page hands back — the range and whether it
/// was whole — and this is that plus what has happened to it since. A caller
/// holding one gets the other through the [`ExportRecord::receipt`] field;
/// the `id` is the same id, and it is what
/// [`SqliteEventLog::confirm_export`] takes.
///
/// Public fields and no `#[non_exhaustive]`; see [`StreamInfo`] for why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportRecord {
    /// The range, the count and the whole flag — the same value
    /// [`SqliteEventLog::export_recorded`] returned when the page was taken.
    pub receipt: ExportReceipt,
    /// Wall clock of the moment the page was handed out, milliseconds since
    /// the Unix epoch.
    pub taken_ms: u64,
    /// Wall clock of the moment the caller said it landed, or `None` for a
    /// page taken and never confirmed.
    ///
    /// [`crate::Guard::Exported`] treats `None` as if the export had not
    /// happened: where the page went is the caller's, and until the caller
    /// says so the store knows only that it handed one out.
    pub landed_ms: Option<u64>,
}

impl ExportRecord {
    /// Whether the caller has confirmed this page, which is the whole of what
    /// [`crate::Guard::Exported`] counts.
    pub fn landed(&self) -> bool {
        self.landed_ms.is_some()
    }
}

impl SqliteEventLog {
    /// Every stream this log has a counter for, ordered by name.
    ///
    /// # The paging rule, which the other four listings share
    ///
    /// `after` is an **exclusive cursor**, and it is the ordering key of the
    /// last row of the previous page — the name here, the id in the ledger —
    /// handed straight back in. `None` starts at the beginning. `limit`
    /// bounds the page, and a page shorter than `limit` is the end of the
    /// listing as of that read. A `limit` of 0 is an empty page and no query
    /// at all, which is what [`eventsdb_core::EventLog::read_all`] does with
    /// one.
    ///
    /// That is `read_all`'s contract with a different key, and the parameter
    /// order is `read_all`'s too: the cursor, what narrows it, the bound.
    /// Each listing reads off a reader connection, so it does not queue
    /// behind a write, and holds nothing between pages.
    ///
    /// # The prefix is the read's prefix
    ///
    /// `prefix` is [`eventsdb_core::log::Filter::stream_prefix`]'s axis,
    /// through the same [`stream_prefix_bound`], so the range this walks is
    /// the range a read walks. `None` and `Some("")` both place no
    /// condition, which is that axis's documented reading: every name starts
    /// with the empty prefix.
    ///
    /// A parameter rather than a `streams_with_prefix` twin, and rather than
    /// a `&Filter`. A `Filter` carries four axes and three of them are about
    /// *events* — `kinds` and `meta` are columns of a row, and a stream that
    /// retention has emptied has no row to carry either. A listing that took
    /// a `Filter` and honoured only the stream half would be a filter whose
    /// other half silently did nothing, on precisely the streams this listing
    /// exists to show.
    ///
    /// # What retention does to this answer
    ///
    /// Nothing, and that is the point. Retention removes events and never
    /// touches `stream_seq`, so **a stream whose every event has been removed
    /// still lists**, with its [`StreamInfo::head_seq`] intact and no events
    /// under it. The counter is the truth of a stream's existence — see
    /// [`eventsdb_core::store::Expected::Unwritten`] — and a caller about to
    /// reuse the name needs it. The difference is not visible to a read: a
    /// `read_all` under the same prefix cannot report a stream that has no
    /// rows.
    pub async fn streams(
        &self,
        after: Option<&str>,
        prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<StreamInfo>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let after = after.map(str::to_string);
        let prefix = prefix
            .filter(|prefix| !prefix.is_empty())
            .map(str::to_string);

        self.listing(move |conn| {
            let mut clauses: Vec<String> = Vec::new();
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

            if let Some(after) = after {
                params.push(Box::new(after));
                clauses.push(format!("stream > ?{}", params.len()));
            }
            if let Some(prefix) = prefix {
                let bound = stream_prefix_bound(&prefix);
                params.push(Box::new(prefix));
                clauses.push(format!("stream >= ?{}", params.len()));
                // `None` where the prefix has no successor: the range is open
                // at the top, exactly as it is for a read.
                if let Some(bound) = bound {
                    params.push(Box::new(bound));
                    clauses.push(format!("stream < ?{}", params.len()));
                }
            }

            let sql = format!(
                "SELECT stream, next_seq FROM stream_seq{} ORDER BY stream LIMIT ?{}",
                where_clause(&clauses),
                params.len() + 1
            );
            params.push(Box::new(clamp_limit(limit)));

            collect(conn, &sql, params, |row| {
                Ok(StreamInfo {
                    stream: row.get(0)?,
                    // Saturating rather than wrapping. Nothing this API
                    // permits stores a counter below 2, but a `sqlite3`
                    // session on the file meets no authorizer, and
                    // `next_seq - 1` on a 0 must read as 0 rather than as
                    // `u64::MAX`. See `StreamInfo::head_seq`.
                    head_seq: (row.get::<_, i64>(1)?.max(0) as u64).saturating_sub(1),
                })
            })
        })
        .await
    }

    /// Every kind the log currently holds an event of, ordered by kind.
    ///
    /// Pages exactly as [`SqliteEventLog::streams`] does; `after` is a kind
    /// name. `SELECT DISTINCT kind`, walking `events_kind_position` — the
    /// index whose leading column is `kind`.
    ///
    /// The limit is what bounds that walk, and is why this listing takes one
    /// even though the set of kinds is a vocabulary rather than something
    /// that grows with the log: SQLite has no seek-to-the-next-distinct-value
    /// plan, so `DISTINCT` reads an index entry per *event* of every kind it
    /// returns, and the `LIMIT` stops it at the page's last kind.
    ///
    /// # What retention does to this answer
    ///
    /// **A kind whose every event has been removed does not list.** Kinds
    /// have no counter — nothing in the schema records that a kind was ever
    /// written, only the events carrying it — so "was this ever a kind here"
    /// is not a question the store can answer after retention, and this
    /// listing does not pretend otherwise. A kind with one surviving event
    /// lists like any other. Contrast [`SqliteEventLog::streams`], where the
    /// counter outlives the events and the emptied stream stays.
    ///
    /// No count per kind: that is the first aggregate this API would offer,
    /// and [`SqliteEventLog::query`] answers it already.
    pub async fn kinds(&self, after: Option<&str>, limit: usize) -> Result<Vec<String>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let after = after.map(str::to_string);

        self.listing(move |conn| {
            let mut clauses: Vec<String> = Vec::new();
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

            if let Some(after) = after {
                params.push(Box::new(after));
                clauses.push(format!("kind > ?{}", params.len()));
            }

            let sql = format!(
                "SELECT DISTINCT kind FROM events{} ORDER BY kind LIMIT ?{}",
                where_clause(&clauses),
                params.len() + 1
            );
            params.push(Box::new(clamp_limit(limit)));

            collect(conn, &sql, params, |row| row.get(0))
        })
        .await
    }

    /// Every consumer that has saved a checkpoint, ordered by name.
    ///
    /// Pages exactly as [`SqliteEventLog::streams`] does; `after` is a
    /// consumer name. This is the read [`crate::Guard::RegisteredConsumers`]
    /// makes on the caller's behalf before it refuses a plan, exposed — so
    /// that the situation can be looked at before a retention call is made
    /// rather than learned from the error it raises.
    ///
    /// It has that guard's blind spot, for the same reason: **a consumer that
    /// has never checked in does not appear**, because nothing here knows it
    /// exists. A caller relying on either should make its consumers save a
    /// checkpoint before their first batch.
    ///
    /// # What retention does to this answer
    ///
    /// Nothing. A checkpoint outlives the events it points past, and a
    /// removal never takes one — which is what leaves the guard something to
    /// refuse with next time.
    pub async fn checkpoints(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ConsumerCheckpoint>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let after = after.map(str::to_string);

        self.listing(move |conn| {
            let mut clauses: Vec<String> = Vec::new();
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

            if let Some(after) = after {
                params.push(Box::new(after));
                clauses.push(format!("consumer > ?{}", params.len()));
            }

            let sql = format!(
                "SELECT consumer, position, updated_ms FROM checkpoints{} \
                 ORDER BY consumer LIMIT ?{}",
                where_clause(&clauses),
                params.len() + 1
            );
            params.push(Box::new(clamp_limit(limit)));

            collect(conn, &sql, params, |row| {
                Ok(ConsumerCheckpoint {
                    consumer: row.get(0)?,
                    position: position_of(row.get::<_, i64>(1)?),
                    updated_ms: row.get::<_, i64>(2)?.max(0) as u64,
                })
            })
        })
        .await
    }

    /// Every application of a retention plan, oldest first.
    ///
    /// Pages exactly as [`SqliteEventLog::streams`] does; `after` is a
    /// [`RetentionEntry::id`], and the order is by that id, which is the
    /// order the applications happened in.
    ///
    /// One row per application, written in the same transaction as the delete
    /// it describes — so the ledger can never disagree with what is actually
    /// gone, and a plan that matched nothing leaves no row at all.
    /// [`RetentionEntry::plan`] is the stored description, handed back as the
    /// string it is.
    ///
    /// # What retention does to this answer
    ///
    /// It adds to it. The ledger is append-only and **deliberately outlives
    /// the events it describes** — it is the only remaining evidence that
    /// they existed, which is why nothing removes from it, retention
    /// included.
    pub async fn retention_ledger(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RetentionEntry>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        self.listing(move |conn| {
            let mut clauses: Vec<String> = Vec::new();
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

            if let Some(after) = after {
                params.push(Box::new(after_id(after)));
                clauses.push(format!("id > ?{}", params.len()));
            }

            let sql = format!(
                "SELECT id, applied_ms, plan, removed_count, highest_removed FROM retention{} \
                 ORDER BY id LIMIT ?{}",
                where_clause(&clauses),
                params.len() + 1
            );
            params.push(Box::new(clamp_limit(limit)));

            collect(conn, &sql, params, |row| {
                Ok(RetentionEntry {
                    id: row.get::<_, i64>(0)?.max(0) as u64,
                    applied_ms: row.get::<_, i64>(1)?.max(0) as u64,
                    plan: row.get(2)?,
                    removed_count: row.get::<_, i64>(3)?.max(0) as usize,
                    highest_removed: position_of(row.get::<_, i64>(4)?),
                })
            })
        })
        .await
    }

    /// Every export receipt, oldest first.
    ///
    /// Pages exactly as [`SqliteEventLog::streams`] does; `after` is an
    /// [`ExportReceipt::id`], and the order is by that id, which is the order
    /// the pages were handed out in.
    ///
    /// Both states are here and are told apart by
    /// [`ExportRecord::landed_ms`]: a row with `None` was taken and never
    /// confirmed, and [`crate::Guard::Exported`] counts it as if it had not
    /// happened. So is a row whose [`ExportReceipt::whole`] is `false` — a
    /// filtered export preserved some of its range and cannot vouch for the
    /// rest. Which of these rows form the chain the guard walks is
    /// [`SqliteEventLog::exported_through`]'s answer, and this is the
    /// evidence behind it.
    ///
    /// # What retention does to this answer
    ///
    /// Nothing. The receipts are append-only, like the ledger, and a receipt
    /// outlives the events it covers — a removal under
    /// [`crate::Guard::Exported`] is the removal a receipt permitted, so
    /// taking it away would erase the reason.
    pub async fn export_receipts(
        &self,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<ExportRecord>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        self.listing(move |conn| {
            let mut clauses: Vec<String> = Vec::new();
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

            if let Some(after) = after {
                params.push(Box::new(after_id(after)));
                clauses.push(format!("id > ?{}", params.len()));
            }

            let sql = format!(
                "SELECT id, taken_ms, from_position, through, count, whole, landed_ms \
                 FROM exports{} ORDER BY id LIMIT ?{}",
                where_clause(&clauses),
                params.len() + 1
            );
            params.push(Box::new(clamp_limit(limit)));

            collect(conn, &sql, params, |row| {
                Ok(ExportRecord {
                    receipt: ExportReceipt {
                        id: row.get::<_, i64>(0)?.max(0) as u64,
                        from: position_of(row.get::<_, i64>(2)?),
                        through: position_of(row.get::<_, i64>(3)?),
                        count: row.get::<_, i64>(4)?.max(0) as usize,
                        whole: row.get::<_, i64>(5)? != 0,
                    },
                    taken_ms: row.get::<_, i64>(1)?.max(0) as u64,
                    landed_ms: row
                        .get::<_, Option<i64>>(6)?
                        .map(|landed| landed.max(0) as u64),
                })
            })
        })
        .await
    }

    /// Run one listing's job on a reader connection.
    ///
    /// The five differ only in their statement and their row shape, and this
    /// is everything they have in common: a reader, and the isle's failure
    /// mapped the way every other read here maps it.
    async fn listing<T, F>(&self, job: F) -> Result<Vec<T>>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<Vec<T>> + Send + 'static,
    {
        let shared = self.shared_handle();
        match shared
            .reader()
            .call(move |conn: &mut Connection| Ok(job(conn)))
            .await
        {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }
}

/// `WHERE a AND b`, or nothing at all when there is no clause.
///
/// A listing with no cursor and no prefix has no predicate, and a `WHERE 1`
/// standing in for one would be a term for the planner to cost.
fn where_clause(clauses: &[String]) -> String {
    if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    }
}

/// Prepare, run, and collect — the body every listing above ends with.
fn collect<T>(
    conn: &Connection,
    sql: &str,
    params: Vec<Box<dyn rusqlite::ToSql>>,
    read: impl Fn(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>> {
    let mut stmt = conn.prepare(sql).map_err(classify)?;
    let rows = stmt
        .query_map(
            rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
            read,
        )
        .map_err(classify)?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(classify)?);
    }
    Ok(out)
}

/// A stored `INTEGER` column as a [`Position`].
///
/// Negative is not reachable through this crate — every position it writes is
/// a rowid — and reading one as a huge `u64` would be worse than reading it
/// as the beginning.
fn position_of(stored: i64) -> Position {
    Position::new(stored.max(0) as u64)
}

/// A `u64` row id as SQLite compares it.
///
/// An id this store assigned is a rowid and fits. One above the range names a
/// row above every row there can be, so clamping answers with the empty page
/// the cursor asked for. Contrast `log::stored_position`, which refuses
/// instead: a position out of range would bind *negative* and turn
/// "read after the end" into "read everything", which clamping upwards cannot
/// do.
fn after_id(after: u64) -> i64 {
    i64::try_from(after).unwrap_or(i64::MAX)
}
