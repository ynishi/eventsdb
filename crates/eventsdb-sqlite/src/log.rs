//! The database-level backend: reads across streams, the live tail, and
//! consumer checkpoints.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use eventsdb_core::error::Result;
use eventsdb_core::event::now_ms;
use eventsdb_core::log::{EventLog, Filter};
use eventsdb_core::position::{Position, Recorded};
use eventsdb_core::store::EventStore;
use eventsdb_core::upcast::{apply_chain, Current, UpcastChain};
use futures_core::stream::BoxStream;
use rusqlite::Connection;
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver};
use serde_json::Value;
use tokio::sync::watch;

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

/// How many events a subscription reads per round.
const SUBSCRIBE_BATCH: usize = 256;

/// Distinguishes in-memory databases from one another, so
/// [`EventStore::database`] stays an identity rather than a shared label.
static MEMORY_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct SqliteEventLog {
    shared: Arc<Shared>,
    /// Owns the isle's lifecycle. Held so the SQLite thread outlives every
    /// handle this log hands out.
    driver: Option<AsyncIsleDriver>,
}

/// Open options, so the two timings above can be set without a second
/// constructor for each combination.
pub struct OpenOptions {
    pub busy_timeout: Duration,
    pub poll_interval: Duration,
    pub upcasters: UpcastChain,
}

impl Default for OpenOptions {
    fn default() -> Self {
        OpenOptions {
            busy_timeout: DEFAULT_BUSY_TIMEOUT,
            poll_interval: DEFAULT_POLL_INTERVAL,
            upcasters: UpcastChain::new(),
        }
    }
}

impl SqliteEventLog {
    /// Open (or create) a log at `path`.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, OpenOptions::default()).await
    }

    pub async fn open_with(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let database = path.display().to_string();
        let busy_timeout = options.busy_timeout;

        let (isle, driver) =
            AsyncIsle::spawn(&path, move |conn| schema::apply_pragmas(conn, busy_timeout))
                .await
                .map_err(map_isle)?;

        Self::finish(isle, driver, database, options).await
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
        Self::finish(isle, driver, database, options).await
    }

    async fn finish(
        isle: AsyncIsle,
        driver: AsyncIsleDriver,
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
                database,
                chain: options.upcasters,
                notify,
                poll_interval: options.poll_interval,
            }),
            driver: Some(driver),
        })
    }

    /// Drain queued work and join the SQLite thread.
    ///
    /// Dropping the log does the same thing without waiting; this is for a
    /// caller that wants the join to have happened before it continues.
    pub async fn close(mut self) -> Result<()> {
        if let Some(driver) = self.driver.take() {
            driver.shutdown().await.map_err(map_isle)?;
        }
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
        Ok((|| {
            let mut sql = format!("SELECT {} FROM events WHERE position > ?1", row::COLUMNS);
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(from as i64)];

            if let Some(stream) = &filter.stream {
                params.push(Box::new(stream.clone()));
                sql.push_str(&format!(" AND stream = ?{}", params.len()));
            }
            if let Some(kinds) = &filter.kinds {
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

            // Upcast in one pass, then re-attach the coordinates the chain
            // does not see: the chain's business is the event, not where it
            // sits.
            let positions: Vec<(u64, String)> = stored
                .iter()
                .map(|s| (s.position, s.stream.clone()))
                .collect();
            let events: Vec<Value> = stored.into_iter().map(|s| s.event).collect();
            let upcasted = apply_chain(&chain, events);

            let mut out = Vec::with_capacity(upcasted.len());
            for ((position, stream), event) in positions.into_iter().zip(upcasted) {
                out.push(Recorded {
                    position: Position::new(position),
                    stream,
                    event: Current::from_upcasted(event)?,
                });
            }
            Ok(out)
        })())
    };

    match shared.isle.call(job).await {
        Ok(inner) => inner,
        Err(isle) => Err(map_isle(isle)),
    }
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
            .isle
            .call(move |conn: &mut Connection| {
                Ok(conn
                    .query_row(
                        "SELECT position FROM checkpoints WHERE consumer = ?1",
                        [consumer],
                        |row| row.get::<_, i64>(0),
                    )
                    .map(|position| Position::new(position as u64))
                    .or_else(|error| match error {
                        rusqlite::Error::QueryReturnedNoRows => Ok(Position::BEGINNING),
                        other => Err(classify(other)),
                    }))
            })
            .await
        {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }

    async fn checkpoint_save(&self, consumer: &str, at: Position) -> Result<()> {
        let consumer = consumer.to_string();
        let at = at.get() as i64;
        let updated = now_ms() as i64;
        match self
            .shared
            .isle
            .call(move |conn: &mut Connection| {
                Ok(conn
                    .execute(
                        "INSERT INTO checkpoints (consumer, position, updated_ms) \
                         VALUES (?1, ?2, ?3) \
                         ON CONFLICT(consumer) DO UPDATE SET \
                           position = excluded.position, updated_ms = excluded.updated_ms",
                        rusqlite::params![consumer, at, updated],
                    )
                    .map(|_| ())
                    .map_err(classify))
            })
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
}
