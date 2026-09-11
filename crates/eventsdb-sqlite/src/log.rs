//! The database-level backend: reads across streams, the live tail, and
//! consumer checkpoints.
//!
//! # A replay and a subscription are one loop
//!
//! Every read across streams is a page: `read_all` with a `from` and a
//! `limit`, served by one query on one borrowed reader. The two streaming
//! shapes this log offers are that page in a loop, with the cursor fed back
//! in, and they differ in exactly one place — what happens when a page comes
//! back short:
//!
//! ```text
//!                  ┌──────────────────────────────────┐
//!                  │  read_all(cursor, filter, BATCH)  │ ◀── one reader,
//!                  │  borrowed for this query only     │     one query
//!                  └────────────────┬─────────────────┘
//!                                   │ yield each; cursor = last position
//!                                   ▼
//!                       page full? ──yes──▶ read again, now
//!                                   │
//!                                   no  (range exhausted as of this read)
//!                                   │
//!                 ┌─────────────────┴──────────────────┐
//!             replay                             subscribe
//!               end                     cursor = max(cursor, head);
//!                                       wait for a commit or the poll
//!                                       interval; read again
//! ```
//!
//! Between pages the stream holds nothing: no connection, no statement, no
//! read transaction. That is the property the shape is chosen for. A cursor
//! left open on a SQLite statement keeps a read transaction open, and a read
//! transaction that never ends stops every checkpoint from resetting the WAL
//! (`sqlite.org/wal.html`, "checkpoint starvation"); a replay that paused
//! half way would have been holding one of the two readers and a growing
//! WAL file for as long as it paused. Paging gives that up for nothing —
//! the log is append-only and positions are dense, so a new event can only
//! land *after* the cursor, and page boundaries cannot duplicate or skip.
//!
//! The same loop, on the writer rather than a reader, is what a projection
//! runs to rebuild; the fold in `append_if` and `TxnContext::read` stay off
//! it because they must see uncommitted state.
//!
//! # The `meta` predicate and its index are one expression
//!
//! `meta` is stored as a JSON `TEXT` column, so a key is reached with
//! `json_extract(meta, '$."key"')`. SQLite uses an index on an expression
//! only when the expression in the query is *textually identical* to the one
//! in the index (`sqlite.org/expridx.html`). So one function spells the
//! expression, and both the `WHERE` clause and `CREATE INDEX` call it:
//!
//! ```text
//!   Filter { meta: [("tenant", "a")] }
//!        │
//!        ▼  meta_expr("tenant")  ──▶  json_extract(meta, '$."tenant"')
//!        │                                     │
//!        ├──▶ SELECT … WHERE … AND json_extract(meta, '$."tenant"') = ?
//!        │                                     │
//!        └──▶ CREATE INDEX events_meta_tenant     (same text)
//!                 ON events (json_extract(meta, '$."tenant"'), position)
//! ```
//!
//! The key is always quoted in the path, so a key containing `.` or `[`
//! names a key rather than a path. `position` is the second column so the
//! index also serves the `ORDER BY position` every read carries.
//!
//! # What `json_extract` hands back
//!
//! Not the JSON type, the SQL type: a string comes back `TEXT`, a number
//! `INTEGER` or `REAL`, JSON `true`/`false` as `1`/`0`, and both JSON `null`
//! and a missing key as SQL `NULL` (`sqlite.org/json1.html`). The predicate
//! binds against that:
//!
//! | filter value   | bound as         | matches                          |
//! |----------------|------------------|----------------------------------|
//! | string         | `TEXT`           | the same string                  |
//! | number         | `INTEGER`/`REAL` | the same number                  |
//! | `true`/`false` | `1`/`0`          | the same boolean                 |
//! | `null`         | refused          | — absence is not a value         |
//!
//! `NULL = NULL` is not true in SQL, which is what makes "a key the event does
//! not carry matches nothing" fall out of the comparison rather than need a
//! second clause. A stored `"1"` and a filter on `1` do not meet either: the
//! expression has no column affinity, so `TEXT` and `INTEGER` stay what they
//! are and compare unequal.
//!
//! # A stream prefix is a range, not a `LIKE`
//!
//! [`Filter::stream_prefix()`] becomes two bound comparisons on the `stream`
//! column, and [`eventsdb_core::log::stream_prefix_bound`] is the one place
//! that says what the upper bound is and why it is exact. Here is why the
//! predicate takes that shape rather than another:
//!
//! ```text
//!   stream_prefix("session-")
//!        │
//!        ├──▶ AND stream >= 'session-'      the prefix itself
//!        └──▶ AND stream <  'session.'      the bound, last char + 1
//! ```
//!
//! Stream names are Rust `String`s bound as `TEXT`, and SQLite compares
//! `TEXT` under `BINARY` collation — `memcmp` over the stored UTF-8 bytes —
//! unless a column or an expression says otherwise, and `events.stream`
//! does not. So the comparison SQLite makes is bytewise over UTF-8, which is
//! the comparison the bound is computed for.
//!
//! The bound is bound as `TEXT`, like the prefix, and that is why it is a
//! character successor rather than an incremented last byte. A byte-level
//! increment can produce bytes that are not UTF-8, which leaves nothing to
//! bind: as a `TEXT` parameter it is not a `String`, and as a `BLOB` it would
//! be worse than wrong — SQLite orders values by storage class before it
//! compares them, so every `BLOB` sorts above every `TEXT` and
//! `stream < ?bound` would stop excluding anything.
//!
//! A range and not `LIKE 'session-%'`, because two indices lead on `stream`
//! — `events_stream_kind_seq` and the one `UNIQUE (stream, seq)` creates —
//! and a range on a leading column is what SQLite can seek with. `LIKE`
//! reaches that plan only under `case_sensitive_like`, a connection-wide
//! pragma this crate does not set and the hatch refuses to set, so a `LIKE`
//! here would be a predicate whose plan depended on state outside the query.
//! Which index a given read actually gets is the planner's, and
//! `tests/stream_prefix.rs` asks it rather than asserting from here. An
//! empty prefix places no clause at all: every name starts with it, and a
//! clause that excludes nothing would still be a term for the planner to
//! cost.
//!
//! # Indexing is on request, not on every key
//!
//! The shipped indices (the migration ladder in `schema.rs` is the list)
//! cover the envelope columns every caller filters by. Which `meta` keys a
//! caller filters by is the caller's vocabulary, and indexing all of them
//! would index the vocabulary the store was told not to know. So [`SqliteEventLog::index_meta`] creates the
//! expression index above for one key, idempotently; the hatch's allowance
//! for `CREATE INDEX` remains for a shape this does not cover. Dropping a
//! shipped index is still refused there.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use eventsdb_core::error::{Error, Result};
use eventsdb_core::event::now_ms;
use eventsdb_core::log::{stream_prefix_bound, EventLog, Filter};
use eventsdb_core::position::{Position, Recorded};
use eventsdb_core::store::EventStore;
use eventsdb_core::transfer::{ExportedEvent, ImportReport};
use eventsdb_core::upcast::{apply_chain, Current, UpcastChain};
use futures_core::stream::BoxStream;
use rusqlite::Connection;
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver};
use serde_json::{Map, Value};
use tokio::sync::watch;

use crate::project::{Projection, ProjectionRunner};
use crate::row;
use crate::schema;
use crate::shared::{classify, map_isle, Shared};
use crate::store::{clamp_limit, SqliteEventStore};

/// Default wait before a subscriber looks again when nothing woke it.
///
/// This is the whole of the cross-process story: SQLite has no
/// `LISTEN`/`NOTIFY`, so a write from another process is invisible until
/// someone reads. In-process writes do not wait for it.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Default wait for a contended lock before a call fails as busy.
pub const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Default number of read-only connections beside the writer.
///
/// Two rather than one so a slow read does not queue behind another slow read,
/// and not many more because each is a thread and reads are usually short.
pub const DEFAULT_READERS: usize = 2;

/// How many events a replay or a subscription reads per page.
const BATCH: usize = 256;

/// Distinguishes in-memory databases from one another, so
/// [`EventStore::database`] stays an identity rather than a shared label.
static MEMORY_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct SqliteEventLog {
    shared: Arc<Shared>,
    /// Owns the isle's lifecycle. Held so the SQLite thread outlives every
    /// handle this log hands out.
    ///
    /// Behind a `Mutex` so [`SqliteEventLog::shutdown`] can take it through a
    /// shared reference: a log behind an `Arc` cannot be consumed, and joining
    /// the thread is what makes detached appends land.
    driver: std::sync::Mutex<Option<AsyncIsleDriver>>,
    /// The read-only connections' lifecycles, joined alongside the writer.
    reader_drivers: std::sync::Mutex<Vec<AsyncIsleDriver>>,
}

/// Open options, so the two timings above can be set without a second
/// constructor for each combination.
///
/// `#[non_exhaustive]`, so an option can be added without breaking a caller.
/// Start from [`OpenOptions::default`] and set what differs through the
/// methods — `OpenOptions::default().readers(0)` — or assign the public
/// fields; only the struct literal is reserved.
#[non_exhaustive]
pub struct OpenOptions {
    pub busy_timeout: Duration,
    pub poll_interval: Duration,
    pub upcasters: UpcastChain,
    /// How many read-only connections to open beside the writer.
    ///
    /// Reads are served from these, so a statement does not queue behind a
    /// write. Under WAL a reader does not block on a writer, and without them
    /// this store gave that away: a read measured at 73µs idle took 8.2
    /// seconds while one write transaction was open — waiting for the thread,
    /// not for the database.
    ///
    /// `0` puts everything back on the writer. Ignored for an in-memory log,
    /// where a second connection would be a second, empty database.
    pub readers: usize,
}

impl Default for OpenOptions {
    fn default() -> Self {
        OpenOptions {
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
            poll_interval: DEFAULT_POLL_INTERVAL,
            upcasters: UpcastChain::new(),
            readers: DEFAULT_READERS,
        }
    }
}

impl OpenOptions {
    /// How long a statement waits on a lock before it fails as `Busy`.
    pub fn busy_timeout(mut self, busy_timeout: Duration) -> Self {
        self.busy_timeout = busy_timeout;
        self
    }

    /// How often a subscription looks for writes made through another log.
    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// The upcaster chain every read runs through. Replaces, not appends.
    pub fn upcasters(mut self, upcasters: UpcastChain) -> Self {
        self.upcasters = upcasters;
        self
    }

    /// How many read-only connections to open beside the writer; see the
    /// field.
    pub fn readers(mut self, readers: usize) -> Self {
        self.readers = readers;
        self
    }
}

impl SqliteEventLog {
    /// Open (or create) a log at `path`.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, OpenOptions::default()).await
    }

    /// Opening the same file twice is safe but not free.
    ///
    /// The global order survives it — allocation happens inside the committing
    /// transaction and `IMMEDIATE` serialises those across connections, and
    /// `tests/two_logs.rs` measures 120 interleaved appends coming back as
    /// exactly `1..=120`. Projections and the retention guard are unaffected
    /// too: they work through tables, which are shared.
    ///
    /// Two things do not survive it. Subscribers are woken per **log**, so a
    /// write through one log reaches the other's subscribers only on the poll
    /// interval — measured at roughly a thousand times the latency. And the
    /// upcaster chain is per log: two logs opened with different chains read
    /// the same stored bytes differently, and nothing detects that. Open once
    /// per process and share the handle where you can.
    pub async fn open_with(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let busy_timeout = options.busy_timeout;

        let (isle, driver) =
            AsyncIsle::spawn(&path, move |conn| schema::apply_pragmas(conn, busy_timeout))
                .await
                .map_err(map_isle)?;

        // Canonicalised *after* the open, because the file may not have
        // existed before it. `database` is an identity — two handles answer
        // with the same string exactly when they are on the same database —
        // and `./a.db` next to `a.db` would otherwise read as two.
        let database = std::fs::canonicalize(&path)
            .unwrap_or_else(|_| path.clone())
            .display()
            .to_string();

        // After the writer, so the WAL files it creates already exist — a
        // read-only connection cannot create them.
        let mut readers = Vec::with_capacity(options.readers);
        let mut reader_drivers = Vec::with_capacity(options.readers);
        for _ in 0..options.readers {
            let (reader, reader_driver) = AsyncIsle::builder()
                .open_flags(
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                        | rusqlite::OpenFlags::SQLITE_OPEN_URI,
                )
                .busy_timeout(busy_timeout)
                .spawn(&path, move |conn| {
                    // Belt to the open flag's braces: a statement that would
                    // write is refused by SQLite rather than by us noticing.
                    conn.execute_batch("PRAGMA query_only = 1;")
                })
                .await
                .map_err(map_isle)?;
            readers.push(reader);
            reader_drivers.push(reader_driver);
        }

        Self::finish(isle, driver, readers, reader_drivers, database, options).await
    }

    /// A log with no file behind it. Durable in every other respect — the
    /// same schema, the same transactions — and gone when it is dropped.
    pub async fn open_in_memory() -> Result<Self> {
        Self::open_in_memory_with(OpenOptions::default()).await
    }

    pub async fn open_in_memory_with(options: OpenOptions) -> Result<Self> {
        let busy_timeout = options.busy_timeout;
        let (isle, driver) =
            AsyncIsle::open_in_memory(move |conn| schema::apply_pragmas(conn, busy_timeout))
                .await
                .map_err(map_isle)?;

        let database = format!("memory:{}", MEMORY_COUNTER.fetch_add(1, Ordering::Relaxed));
        // No readers: each `:memory:` open is a separate database, so a second
        // connection would see an empty one.
        Self::finish(isle, driver, Vec::new(), Vec::new(), database, options).await
    }

    async fn finish(
        isle: AsyncIsle,
        driver: AsyncIsleDriver,
        readers: Vec<AsyncIsle>,
        reader_drivers: Vec<AsyncIsleDriver>,
        database: String,
        options: OpenOptions,
    ) -> Result<Self> {
        // The ladder runs here rather than in the isle's init closure: it
        // reports in this crate's error type, and the init closure can only
        // return rusqlite's. Nothing has a handle yet, so "before any append
        // is served" still holds.
        match isle
            .call(|conn: &mut Connection| Ok(schema::migrate(conn)))
            .await
        {
            Ok(inner) => inner?,
            Err(isle) => return Err(map_isle(isle)),
        }

        // Seed the watch with the head, so a subscriber that starts before the
        // first write of this process is not told the log is empty when it is
        // not.
        let head = match isle
            .call(|conn: &mut Connection| {
                Ok(conn
                    .query_row("SELECT COALESCE(MAX(position), 0) FROM events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(classify))
            })
            .await
        {
            Ok(inner) => inner?,
            Err(isle) => return Err(map_isle(isle)),
        };

        let (notify, _) = watch::channel(Position::new(head as u64));
        Ok(SqliteEventLog {
            shared: Arc::new(Shared {
                isle,
                readers,
                next_reader: std::sync::atomic::AtomicUsize::new(0),
                database,
                chain: options.upcasters,
                notify,
                poll_interval: options.poll_interval,
                live_runners: std::sync::Mutex::new(std::collections::HashSet::new()),
            }),
            driver: std::sync::Mutex::new(Some(driver)),
            reader_drivers: std::sync::Mutex::new(reader_drivers),
        })
    }

    /// Drain queued work and join the SQLite thread.
    ///
    /// Dropping the log does the same thing without waiting; this is for a
    /// caller that wants the join to have happened before it continues.
    pub async fn close(self) -> Result<()> {
        self.shutdown().await
    }

    /// Drain queued work and join the SQLite thread, without consuming the
    /// log.
    ///
    /// [`SqliteEventLog::close`] takes `self`, which a log behind an `Arc`
    /// with any live handle cannot satisfy — and joining is exactly what makes
    /// queued [`SqliteEventLog::detach_append`] work land before a host exits.
    ///
    /// Idempotent. Handles issued by this log stay valid as values and fail as
    /// storage errors once the thread is gone.
    pub async fn shutdown(&self) -> Result<()> {
        let readers = std::mem::take(
            &mut *self
                .reader_drivers
                .lock()
                .expect("the shutdown lock is never held across a panic"),
        );
        for reader in readers {
            reader.shutdown().await.map_err(map_isle)?;
        }

        let driver = self
            .driver
            .lock()
            .expect("the shutdown lock is never held across a panic")
            .take();
        match driver {
            Some(driver) => driver.shutdown().await.map_err(map_isle),
            None => Ok(()),
        }
    }

    /// Queue a stamped append and return without waiting for it.
    ///
    /// For a caller that must record a fact from somewhere it cannot await —
    /// a `Drop` implementation closing a session, most of all, where blocking
    /// is not allowed either.
    ///
    /// **Ordered.** The job goes onto the isle's own queue, so it lands before
    /// anything submitted after it. Spawning a task that later calls `append`
    /// would not: it would leave the queue and race everything on the log.
    ///
    /// The envelope is validated here, so a malformed event is refused
    /// synchronously. Everything after that — the write itself — is
    /// unreported: **a storage failure is dropped**, and subscribers are not
    /// woken, because there is nobody left to tell. Use it for the boundary
    /// record a normal path already wrote, not for a fact nothing else knows.
    ///
    /// Call [`SqliteEventLog::shutdown`] before exiting to be sure the queue
    /// drained.
    ///
    /// Naming the stream, for a caller that holds the log. One that holds a
    /// stream handle has the same verb without the parameter — it is
    /// [`EventStore::detach_append`], which this is.
    pub fn detach_append(&self, stream: &str, event: Map<String, Value>) -> Result<()> {
        self.stream_handle(stream).detach_append(event)
    }

    /// A typed handle rather than the boxed trait object, for callers that
    /// have no reason to erase it.
    pub fn stream_handle(&self, id: &str) -> SqliteEventStore {
        SqliteEventStore {
            shared: Arc::clone(&self.shared),
            stream: id.to_string(),
        }
    }
}

/// The cross-stream read, as a free function so a subscription can hold what
/// it needs without borrowing the log.
async fn read_all_shared(
    shared: &Arc<Shared>,
    from: Position,
    filter: &Filter,
    limit: usize,
) -> Result<Vec<Recorded>> {
    if filter.selects_nothing() || limit == 0 {
        return Ok(Vec::new());
    }

    let filter = filter.clone();
    let chain = shared.chain.clone();
    let from = from.get();

    let job = move |conn: &mut Connection| {
        Ok(select_recorded(
            conn,
            &chain,
            Position::new(from),
            &filter,
            limit,
        ))
    };

    // A reader: this is a standalone read, so it must not queue behind a
    // write. The projection runner does *not* come through here — its read is
    // inside the transaction it is about to write.
    match shared.reader().call(job).await {
        Ok(inner) => inner,
        Err(isle) => Err(map_isle(isle)),
    }
}

/// The cross-stream read against an open connection.
///
/// Takes `&Connection` rather than owning the call, so it serves both the
/// standalone read above and a projection runner that needs the read to be
/// inside the *same* transaction as the work it feeds.
pub(crate) fn select_recorded(
    conn: &Connection,
    chain: &UpcastChain,
    from: Position,
    filter: &Filter,
    limit: usize,
) -> Result<Vec<Recorded>> {
    let stored = select_stored(conn, from, filter, limit)?;

    // Upcast in one pass, then re-attach the coordinates the chain does not
    // see: the chain's business is the event, not where it sits.
    let coordinates: Vec<(u64, String)> = stored
        .iter()
        .map(|s| (s.position, s.stream.clone()))
        .collect();
    let events: Vec<Value> = stored.into_iter().map(|s| s.event).collect();
    let upcasted = apply_chain(chain, events);

    let mut out = Vec::with_capacity(upcasted.len());
    for ((position, stream), event) in coordinates.into_iter().zip(upcasted) {
        out.push(Recorded {
            position: Position::new(position),
            stream,
            event: Current::from_upcasted(event)?,
        });
    }
    Ok(out)
}

/// The same read, stopping before the upcaster chain.
///
/// What an export wants: the bytes as stored, so the receiving store holds
/// what this one held and runs its own chain over them.
pub(crate) fn select_stored(
    conn: &Connection,
    from: Position,
    filter: &Filter,
    limit: usize,
) -> Result<Vec<row::StoredRow>> {
    if filter.selects_nothing() || limit == 0 {
        return Ok(Vec::new());
    }
    filter.validate()?;

    let mut sql = format!("SELECT {} FROM events WHERE position > ?1", row::COLUMNS);
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(stored_position(from)?)];

    if let Some(streams) = &filter.streams {
        check_placeholders(streams.len(), "streams")?;
        let first = params.len() + 1;
        let holes: Vec<String> = (0..streams.len())
            .map(|i| format!("?{}", first + i))
            .collect();
        sql.push_str(&format!(" AND stream IN ({})", holes.join(", ")));
        for stream in streams {
            params.push(Box::new(stream.clone()));
        }
    }
    if let Some(prefix) = filter.stream_prefix.as_deref().filter(|p| !p.is_empty()) {
        params.push(Box::new(prefix.to_string()));
        sql.push_str(&format!(" AND stream >= ?{}", params.len()));
        if let Some(bound) = stream_prefix_bound(prefix) {
            params.push(Box::new(bound));
            sql.push_str(&format!(" AND stream < ?{}", params.len()));
        }
    }
    if let Some(kinds) = &filter.kinds {
        check_placeholders(kinds.len(), "kinds")?;
        let first = params.len() + 1;
        let holes: Vec<String> = (0..kinds.len())
            .map(|i| format!("?{}", first + i))
            .collect();
        sql.push_str(&format!(" AND kind IN ({})", holes.join(", ")));
        for kind in kinds {
            params.push(Box::new(kind.clone()));
        }
    }
    if let Some(pairs) = &filter.meta {
        check_placeholders(pairs.len(), "meta pairs")?;
        for (key, value) in pairs {
            params.push(meta_param(value));
            sql.push_str(&format!(" AND {} = ?{}", meta_expr(key)?, params.len()));
        }
    }
    params.push(Box::new(clamp_limit(limit)));
    sql.push_str(&format!(" ORDER BY position LIMIT ?{}", params.len()));

    let mut stmt = conn.prepare(&sql).map_err(classify)?;
    let rows = stmt
        .query_map(
            rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
            row::read,
        )
        .map_err(classify)?;

    let mut stored = Vec::new();
    for item in rows {
        stored.push(item.map_err(classify)?);
    }
    Ok(stored)
}

/// The one spelling of "this `meta` key", shared by the predicate and the
/// index so SQLite can see they are the same expression.
///
/// The key is a JSON path label in double quotes, which is what lets `.` and
/// `[` in a key name the key. A key that itself contains `"` has no spelling
/// in SQLite's path syntax and is refused; a `'` is doubled for the SQL
/// string literal around the path.
pub(crate) fn meta_expr(key: &str) -> Result<String> {
    if key.is_empty() {
        return Err(Error::validation(
            "a `meta` key to filter by must not be empty",
        ));
    }
    if key.contains('"') {
        return Err(Error::validation(format!(
            "a `meta` key cannot be filtered on when it contains `\"`: SQLite's JSON \
             path syntax has no way to write it (key `{key}`)"
        )));
    }
    Ok(format!(
        "json_extract(meta, '$.\"{}\"')",
        key.replace('\'', "''")
    ))
}

/// A filter value as the SQL value `json_extract` would produce for it —
/// integers where the number is one, `1`/`0` for a boolean. [`Filter::validate`]
/// has already refused anything that is not a scalar.
fn meta_param(value: &Value) -> Box<dyn rusqlite::ToSql> {
    match value {
        Value::String(s) => Box::new(s.clone()),
        Value::Bool(b) => Box::new(i64::from(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Box::new(i)
            } else {
                Box::new(n.as_f64().unwrap_or(f64::NAN))
            }
        }
        // Unreachable after `validate`; binding NULL keeps the statement well
        // formed and matches nothing, which is the honest fallback.
        _ => Box::new(rusqlite::types::Null),
    }
}

/// The index name for a `meta` key. A key made of identifier characters is
/// used as it is; any other key is spelled in hex, so two keys that differ
/// only in punctuation cannot collapse onto one name and have the second
/// `IF NOT EXISTS` silently skip.
pub(crate) fn meta_index_name(key: &str) -> String {
    let plain = key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if plain {
        format!("events_meta_{key}")
    } else {
        let hex: String = key.bytes().map(|b| format!("{b:02x}")).collect();
        format!("events_meta_x{hex}")
    }
}

/// The newest position in the log, against an open connection.
pub(crate) fn head_position_in(conn: &Connection) -> Result<Position> {
    conn.query_row("SELECT COALESCE(MAX(position), 0) FROM events", [], |row| {
        row.get::<_, i64>(0)
    })
    .map(|head| Position::new(head as u64))
    .map_err(classify)
}

/// The newest position, for a caller that holds only the shared handle.
async fn head_shared(shared: &Arc<Shared>) -> Result<Position> {
    match shared
        .reader()
        .call(|conn: &mut Connection| Ok(head_position_in(conn)))
        .await
    {
        Ok(inner) => inner,
        Err(isle) => Err(map_isle(isle)),
    }
}

/// Read a consumer's cursor against an open connection.
pub(crate) fn load_checkpoint(conn: &Connection, consumer: &str) -> Result<Position> {
    conn.query_row(
        "SELECT position FROM checkpoints WHERE consumer = ?1",
        [consumer],
        |row| row.get::<_, i64>(0),
    )
    .map(|position| Position::new(position as u64))
    .or_else(|error| match error {
        rusqlite::Error::QueryReturnedNoRows => Ok(Position::BEGINNING),
        other => Err(classify(other)),
    })
}

/// Write a consumer's cursor against an open connection.
///
/// When that connection is a transaction the projection is also writing
/// through, the cursor and the work it accounts for move together — which is
/// the whole of the exactly-once claim.
pub(crate) fn save_checkpoint(conn: &Connection, consumer: &str, at: Position) -> Result<()> {
    conn.execute(
        "INSERT INTO checkpoints (consumer, position, updated_ms) VALUES (?1, ?2, ?3) \
         ON CONFLICT(consumer) DO UPDATE SET \
           position = excluded.position, updated_ms = excluded.updated_ms",
        rusqlite::params![consumer, stored_position(at)?, now_ms() as i64],
    )
    .map(|_| ())
    .map_err(classify)
}

/// SQLite's ceiling on bound parameters in one statement.
///
/// `SQLITE_MAX_VARIABLE_NUMBER`, 32766 since 3.32. A list longer than this is
/// a caller mistake with a confusing symptom — the failure otherwise arrives
/// as a storage error about variable numbers, which reads like the database
/// broke rather than like the filter was too wide.
const MAX_PLACEHOLDERS: usize = 32_766;

pub(crate) fn check_placeholders(count: usize, what: &str) -> Result<()> {
    if count > MAX_PLACEHOLDERS {
        return Err(Error::Unsupported(format!(
            "{count} {what} in one statement, over SQLite's limit of {MAX_PLACEHOLDERS}; \
             narrow the filter, or run it in batches"
        )));
    }
    Ok(())
}

/// A position as SQLite stores it, refusing rather than wrapping.
///
/// Every position the store assigns is a rowid and fits. This exists because
/// [`Position::new`] is public: a value above `i64::MAX` would bind as a
/// negative number, turning "read after the end" into "read everything".
pub(crate) fn stored_position(position: Position) -> Result<i64> {
    position.as_stored().ok_or_else(|| {
        Error::validation(format!(
            "position {position} is beyond the range SQLite stores; \
             a position assigned by this store is always in range"
        ))
    })
}

/// What a paged read does when a batch comes back short.
#[derive(Clone, Copy)]
enum Exhausted {
    /// Wait for a commit or the poll interval, then read again: a
    /// subscription.
    Wait,
    /// Return: a replay.
    End,
}

/// The one loop behind [`SqliteEventLog::replay`] and
/// [`EventLog::subscribe`]: page `read_all` with the cursor fed back in,
/// borrowing a reader for each page and holding nothing between pages.
///
/// A batch that comes back full is followed immediately by another read, so
/// catching up does not crawl at the poll interval. A batch that comes back
/// short means the filtered range is exhausted as of that read, and
/// `on_exhausted` says whether that is the end or the moment to wait.
fn paged(
    shared: Arc<Shared>,
    from: Position,
    filter: Filter,
    on_exhausted: Exhausted,
) -> BoxStream<'static, Result<Recorded>> {
    let mut cursor = from;
    let mut woken = shared.notify.subscribe();
    let poll_interval = shared.poll_interval;

    let stream = async_stream::stream! {
        loop {
            // For the tail only: read the head before the batch, so advancing
            // to it below cannot skip an event that landed in between. A
            // replay never advances past what it read, so it never asks.
            let head = match on_exhausted {
                Exhausted::Wait => match head_shared(&shared).await {
                    Ok(head) => Some(head),
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                },
                Exhausted::End => None,
            };

            let batch = read_all_shared(&shared, cursor, &filter, BATCH).await;
            let batch = match batch {
                Ok(batch) => batch,
                Err(error) => {
                    yield Err(error);
                    return;
                }
            };

            let full = batch.len() == BATCH;
            for recorded in batch {
                cursor = recorded.position;
                yield Ok(recorded);
            }

            if full {
                // Still behind. Read again rather than wait.
                continue;
            }

            let Some(head) = head else {
                // A replay: the range is exhausted, and that is the end.
                return;
            };

            // The filtered range is exhausted, so everything up to `head`
            // has been looked at. Without this a narrow filter would leave
            // the cursor at the last match and re-scan the same prefix on
            // every poll, for ever.
            cursor = cursor.max(head);

            // Caught up. Wake on an in-process commit, or look again
            // after the interval — which is what covers a writer in
            // another process.
            let _ = tokio::time::timeout(poll_interval, woken.changed()).await;
        }
    };

    Box::pin(stream)
}

#[async_trait]
impl EventLog for SqliteEventLog {
    async fn stream(&self, id: &str) -> Result<Box<dyn EventStore>> {
        Ok(Box::new(self.stream_handle(id)))
    }

    async fn read_all(
        &self,
        from: Position,
        filter: &Filter,
        limit: usize,
    ) -> Result<Vec<Recorded>> {
        read_all_shared(&self.shared, from, filter, limit).await
    }

    async fn head_position(&self) -> Result<Position> {
        let shared = Arc::clone(&self.shared);
        match shared
            .isle
            .call(|conn: &mut Connection| {
                Ok(conn
                    .query_row("SELECT COALESCE(MAX(position), 0) FROM events", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(classify)
                    .map(|head| Position::new(head as u64)))
            })
            .await
        {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }

    /// Catch up, then tail.
    ///
    /// The catch-up half is [`SqliteEventLog::replay`]; the tail is what
    /// happens instead of returning when the range runs dry. See the module
    /// doc for the one loop both are.
    fn subscribe(
        &self,
        from: Position,
        filter: Filter,
    ) -> Result<BoxStream<'static, Result<Recorded>>> {
        Ok(paged(
            Arc::clone(&self.shared),
            from,
            filter,
            Exhausted::Wait,
        ))
    }

    async fn checkpoint_load(&self, consumer: &str) -> Result<Position> {
        let consumer = consumer.to_string();
        match self
            .shared
            .reader()
            .call(move |conn: &mut Connection| Ok(load_checkpoint(conn, &consumer)))
            .await
        {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }

    async fn checkpoint_save(&self, consumer: &str, at: Position) -> Result<()> {
        let consumer = consumer.to_string();
        match self
            .shared
            .isle
            .call(move |conn: &mut Connection| Ok(save_checkpoint(conn, &consumer, at)))
            .await
        {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }

    /// Off a reader connection, so an export does not queue behind the writer.
    async fn export(
        &self,
        from: Position,
        filter: &Filter,
        limit: usize,
    ) -> Result<Vec<ExportedEvent>> {
        crate::transfer::export(self, from, filter, limit).await
    }

    /// One transaction for the whole batch, with the stream counter read and
    /// written once per stream rather than once per event. A failed import
    /// leaves nothing behind.
    async fn import(&self, events: Vec<ExportedEvent>) -> Result<ImportReport> {
        crate::transfer::import(self, events).await
    }
}

impl SqliteEventLog {
    /// This database's identity, the same string every handle it issues
    /// reports from [`EventStore::database`].
    pub fn database(&self) -> &str {
        &self.shared.database
    }

    /// A runner for `projection`, reading this log and writing its read model
    /// through the same transactions.
    /// Fallible because a projection's name is the primary key of its
    /// checkpoint: two live runners answering the same name would share one
    /// cursor, each advancing it past events the other had not folded, and
    /// neither would be exactly-once any more. A second one is refused rather
    /// than allowed to do that quietly. Dropping a runner releases its name,
    /// so stopping and restarting a projection is ordinary.
    pub fn runner<P: Projection>(&self, projection: P) -> Result<ProjectionRunner<P>> {
        ProjectionRunner::new(Arc::clone(&self.shared), projection)
    }

    /// A runner whose name is known to be free, panicking if it is not.
    ///
    /// For a caller that owns the whole log and knows its own projections —
    /// a test, a single-purpose binary — where threading a `Result` through
    /// says nothing the caller did not already know.
    pub fn runner_now<P: Projection>(&self, projection: P) -> ProjectionRunner<P> {
        self.runner(projection)
            .expect("a runner for a name that is not already live")
    }

    /// Everything with `position > from` that `filter` selects, in position
    /// order, as a stream that ends when the range runs dry.
    ///
    /// This is [`EventLog::read_all`] with the cursor fed back in for you,
    /// and nothing more: each page borrows a reader for one query and holds
    /// nothing between pages, so a replay left half-consumed keeps no
    /// connection, no statement and no read transaction. Two of them side by
    /// side cost two queries at a time, not two readers for their lifetimes.
    ///
    /// The end is the first page that comes back short: the filtered range
    /// was exhausted *as of that read*. An event committed after that page
    /// was taken is not in the stream — a replay reports the log as it was,
    /// and a caller that wants to keep going from there has
    /// [`EventLog::subscribe`], which is this loop with waiting instead of
    /// returning.
    pub fn replay(&self, from: Position, filter: Filter) -> BoxStream<'static, Result<Recorded>> {
        paged(Arc::clone(&self.shared), from, filter, Exhausted::End)
    }

    /// Make reads that filter on `meta.<key>` cheap.
    ///
    /// Creates an index on exactly the expression the filter uses, so a
    /// `Filter::meta(key, _)` read becomes an index range rather than a scan
    /// of every row's `meta`. Idempotent: calling it again for a key that has
    /// one is a no-op. It is a schema change on the events table and goes
    /// through the writer, so it waits for any write in progress the way an
    /// append would.
    ///
    /// Which keys to index is the caller's call, the same way which keys
    /// exist is — see the module doc for why the store does not guess.
    pub async fn index_meta(&self, key: &str) -> Result<()> {
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {} ON events ({}, position)",
            meta_index_name(key),
            meta_expr(key)?
        );
        match self
            .shared
            .isle
            .call(move |conn: &mut Connection| Ok(conn.execute_batch(&sql).map_err(classify)))
            .await
        {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }

    /// The shared handle, for the sibling modules that need the isle.
    pub(crate) fn shared_handle(&self) -> Arc<Shared> {
        Arc::clone(&self.shared)
    }
}
