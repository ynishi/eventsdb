//! The database-level backend: reads across streams, the live tail, and
//! consumer checkpoints.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use eventsdb_core::error::{Error, Result};
use eventsdb_core::event::now_ms;
use eventsdb_core::log::{EventLog, Filter};
use eventsdb_core::position::{Position, Recorded};
use eventsdb_core::store::EventStore;
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
use crate::store::{self, clamp_limit, SqliteEventStore};

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

/// How many events a subscription reads per round.
const SUBSCRIBE_BATCH: usize = 256;

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
    pub fn detach_append(&self, stream: &str, event: Map<String, Value>) -> Result<()> {
        eventsdb_core::event::validate(&event)?;
        let stream = stream.to_string();
        self.shared
            .isle
            .spawn_call(move |conn: &mut Connection| {
                let _ = store::append_stamped_now(conn, &stream, event);
                Ok(())
            })
            .detach();
        Ok(())
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

    let mut sql = format!("SELECT {} FROM events WHERE position > ?1", row::COLUMNS);
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(stored_position(from)?)];

    if let Some(stream) = &filter.stream {
        params.push(Box::new(stream.clone()));
        sql.push_str(&format!(" AND stream = ?{}", params.len()));
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
    /// The two halves are the same range read; the only difference is that
    /// the second one waits first. A batch that comes back full is followed
    /// immediately by another read, so catching up does not crawl at the poll
    /// interval.
    fn subscribe(
        &self,
        from: Position,
        filter: Filter,
    ) -> Result<BoxStream<'static, Result<Recorded>>> {
        let shared = Arc::clone(&self.shared);
        let mut cursor = from;
        let mut woken = shared.notify.subscribe();
        let poll_interval = shared.poll_interval;

        let stream = async_stream::stream! {
            loop {
                // Before the batch, so advancing to it below cannot skip an
                // event that landed in between.
                let head = match head_shared(&shared).await {
                    Ok(head) => head,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };

                let batch = read_all_shared(&shared, cursor, &filter, SUBSCRIBE_BATCH).await;
                let batch = match batch {
                    Ok(batch) => batch,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };

                let full = batch.len() == SUBSCRIBE_BATCH;
                for recorded in batch {
                    cursor = recorded.position;
                    yield Ok(recorded);
                }

                if full {
                    // Still behind. Read again rather than wait.
                    continue;
                }

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

        Ok(Box::pin(stream))
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
}

impl SqliteEventLog {
    /// This database's identity, the same string every handle it issues
    /// reports from [`EventStore::database`].
    pub fn database(&self) -> &str {
        &self.shared.database
    }

    /// A runner for `projection`, reading this log and writing its read model
    /// through the same transactions.
    pub fn runner<P: Projection>(&self, projection: P) -> ProjectionRunner<P> {
        ProjectionRunner::new(Arc::clone(&self.shared), projection)
    }

    /// The shared handle, for the sibling modules that need the isle.
    pub(crate) fn shared_handle(&self) -> Arc<Shared> {
        Arc::clone(&self.shared)
    }
}
