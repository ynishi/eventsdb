//! The durable per-stream backend.
//!
//! Every write takes an `IMMEDIATE` transaction, which is the point: a
//! `DEFERRED` transaction acquires the write lock on its first write, and an
//! upgrade at that moment fails immediately instead of waiting out the
//! `busy_timeout`. Taking the reserved lock at `BEGIN` is what makes the
//! timeout mean anything.
//!
//! Reads and writes share one connection on one thread, so a stream's writes
//! are serialized by construction rather than by a lock this code holds.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use eventsdb_core::error::{Error, Result};
use eventsdb_core::event::{now_ms, stamp, validate};
use eventsdb_core::position::{Committed, Position};
use eventsdb_core::store::{Decision, EventStore, Expected};
use eventsdb_core::upcast::{apply_chain, Current, UpcastChain};
use rusqlite::{Connection, Transaction, TransactionBehavior};
use serde_json::{Map, Value};

use crate::row;
use crate::shared::{backoff, classify, map_isle, Shared, MAX_BUSY_RETRIES};

pub struct SqliteEventStore {
    pub(crate) shared: Arc<Shared>,
    pub(crate) stream: String,
}

impl SqliteEventStore {
    /// Run `job` on the isle, re-submitting it while the lock is contended.
    ///
    /// `job` is built afresh for each attempt, because the closure the isle
    /// takes is `FnOnce` and a retry needs its own.
    async fn write_retrying<T, MakeJob, Job>(&self, mut make_job: MakeJob) -> Result<T>
    where
        T: Send + 'static,
        Job: FnOnce(&mut Connection) -> rusqlite::Result<Result<T>> + Send + 'static,
        MakeJob: FnMut() -> Job,
    {
        let mut attempt = 0;
        loop {
            let outcome = match self.shared.isle.call(make_job()).await {
                Ok(inner) => inner,
                Err(isle) => Err(map_isle(isle)),
            };
            match outcome {
                Err(error) if error.is_busy() && attempt < MAX_BUSY_RETRIES => {
                    attempt += 1;
                    tokio::time::sleep(backoff(attempt)).await;
                }
                other => return other,
            }
        }
    }

    /// Read on a **reader** connection, so it does not queue behind a write.
    ///
    /// Not retried: a contended read under WAL is already waited out by
    /// `busy_timeout`, and a failure past that is worth surfacing rather than
    /// papering over.
    async fn read_job<T, Job>(&self, job: Job) -> Result<T>
    where
        T: Send + 'static,
        Job: FnOnce(&mut Connection) -> rusqlite::Result<Result<T>> + Send + 'static,
    {
        match self.shared.reader().call(job).await {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }

    /// Read on the **writer**, for a call that must also write there.
    async fn writer_job<T, Job>(&self, job: Job) -> Result<T>
    where
        T: Send + 'static,
        Job: FnOnce(&mut Connection) -> rusqlite::Result<Result<T>> + Send + 'static,
    {
        match self.shared.isle.call(job).await {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }

    fn upcast_rows(chain: &UpcastChain, rows: Vec<Value>) -> Result<Vec<Current>> {
        apply_chain(chain, rows)
            .into_iter()
            .map(Current::from_upcasted)
            .collect()
    }
}

/// The next sequence number for `stream`, read inside the caller's transaction.
///
/// From a stored counter, not from `MAX(seq)` over the surviving rows. The
/// difference only shows up once something is removed: retention can empty a
/// stream entirely, and a derived maximum would then restart at 1 and hand
/// out a `seq` some earlier event already had. See `schema::STEP_3`.
pub(crate) fn next_seq(tx: &Transaction<'_>, stream: &str) -> Result<u64> {
    tx.query_row(
        "SELECT next_seq FROM stream_seq WHERE stream = ?1",
        [stream],
        |row| row.get::<_, i64>(0),
    )
    .map(|seq| seq as u64)
    .or_else(|error| match error {
        rusqlite::Error::QueryReturnedNoRows => Ok(1),
        other => Err(classify(other)),
    })
}

/// Record where `stream` will carry on from.
///
/// In the same transaction as the appends it accounts for, so a rolled-back
/// write does not consume a sequence number.
pub(crate) fn set_next_seq(tx: &Transaction<'_>, stream: &str, next: u64) -> Result<()> {
    tx.execute(
        "INSERT INTO stream_seq (stream, next_seq) VALUES (?1, ?2) \
         ON CONFLICT(stream) DO UPDATE SET next_seq = excluded.next_seq",
        rusqlite::params![stream, next as i64],
    )
    .map(|_| ())
    .map_err(classify)
}

/// Insert one already-stamped event, returning where it landed.
///
/// The position is the rowid the insert assigned, read back with
/// `last_insert_rowid` inside the same transaction — so it is allocated and
/// committed together, which is what keeps the global order gap-free as read.
pub(crate) fn insert_stamped(
    tx: &Transaction<'_>,
    stream: &str,
    event: &Map<String, Value>,
) -> Result<Committed> {
    let (kind, seq, epoch_ms, schema_version) = row::envelope_columns(event)?;
    let (meta, data) = row::json_columns(event)?;

    tx.execute(
        "INSERT INTO events (stream, seq, epoch_ms, kind, schema_version, meta, data) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            stream,
            seq as i64,
            epoch_ms as i64,
            kind,
            schema_version as i64,
            meta,
            data
        ],
    )
    .map_err(classify)?;

    Ok(Committed {
        seq,
        epoch_ms,
        position: Some(Position::new(tx.last_insert_rowid() as u64)),
    })
}

/// Read a stream's events inside the caller's transaction, still as stored.
pub(crate) fn select_stream(
    tx: &Transaction<'_>,
    stream: &str,
    kinds: Option<&[String]>,
    from_seq: u64,
    limit: usize,
) -> Result<Vec<Value>> {
    if kinds.is_some_and(|kinds| kinds.is_empty()) {
        return Ok(Vec::new());
    }

    let mut sql = format!(
        "SELECT {} FROM events WHERE stream = ?1 AND seq >= ?2",
        row::COLUMNS
    );
    let mut params: Vec<Box<dyn rusqlite::ToSql>> =
        vec![Box::new(stream.to_string()), Box::new(from_seq as i64)];

    if let Some(kinds) = kinds {
        let first = params.len() + 1;
        let holes: Vec<String> = (0..kinds.len())
            .map(|i| format!("?{}", first + i))
            .collect();
        sql.push_str(&format!(" AND kind IN ({})", holes.join(", ")));
        for kind in kinds {
            params.push(Box::new(kind.clone()));
        }
    }
    sql.push_str(&format!(" ORDER BY seq LIMIT ?{}", params.len() + 1));
    params.push(Box::new(clamp_limit(limit)));

    let mut stmt = tx.prepare(&sql).map_err(classify)?;
    let rows = stmt
        .query_map(
            rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
            row::read,
        )
        .map_err(classify)?;

    let mut out = Vec::new();
    for stored in rows {
        out.push(stored.map_err(classify)?.event);
    }
    Ok(out)
}

/// One stamped append on an open connection, taking its own transaction.
///
/// The shared body of the ordinary append and the detached one — the latter
/// has no caller to report to, so it needs the whole thing in one place rather
/// than a retry loop around it.
pub(crate) fn append_stamped_now(
    conn: &mut Connection,
    stream: &str,
    event: Map<String, Value>,
) -> Result<Committed> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(classify)?;
    let seq = next_seq(&tx, stream)?;
    let stamped = stamp(event, seq, now_ms())?;
    let committed = insert_stamped(&tx, stream, &stamped)?;
    set_next_seq(&tx, stream, seq + 1)?;
    tx.commit().map_err(classify)?;
    Ok(committed)
}

/// SQLite's `LIMIT` takes a signed 64-bit value; a negative one means "no
/// limit". `usize::MAX` is the caller's way of saying the same thing, so it
/// maps to -1 rather than overflowing into one.
pub(crate) fn clamp_limit(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(-1)
}

#[async_trait]
impl EventStore for SqliteEventStore {
    fn stream_id(&self) -> &str {
        &self.stream
    }

    fn database(&self) -> Option<&str> {
        Some(&self.shared.database)
    }

    async fn append(&mut self, event: Map<String, Value>) -> Result<Committed> {
        // Before the database, so a rejected event takes no lock and consumes
        // no sequence number.
        validate(&event)?;
        let stream = self.stream.clone();

        let committed = self
            .write_retrying(move || {
                let stream = stream.clone();
                let event = event.clone();
                move |conn: &mut Connection| {
                    Ok((|| {
                        let tx = conn
                            .transaction_with_behavior(TransactionBehavior::Immediate)
                            .map_err(classify)?;
                        let seq = next_seq(&tx, &stream)?;
                        let stamped = stamp(event, seq, now_ms())?;
                        let committed = insert_stamped(&tx, &stream, &stamped)?;
                        set_next_seq(&tx, &stream, seq + 1)?;
                        tx.commit().map_err(classify)?;
                        Ok(committed)
                    })())
                }
            })
            .await?;

        if let Some(position) = committed.position {
            self.shared.publish(position);
        }
        Ok(committed)
    }

    /// One `IMMEDIATE` transaction for the whole batch, so a batch that fails
    /// part-way leaves the stream exactly as it was.
    async fn append_many(&mut self, events: Vec<Map<String, Value>>) -> Result<Vec<Committed>> {
        for event in &events {
            validate(event)?;
        }
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let stream = self.stream.clone();

        let committed = self
            .write_retrying(move || {
                let stream = stream.clone();
                let events = events.clone();
                move |conn: &mut Connection| {
                    Ok((|| {
                        let tx = conn
                            .transaction_with_behavior(TransactionBehavior::Immediate)
                            .map_err(classify)?;
                        // One clock reading for the batch: these are records
                        // of one occurrence, and stamping them with times
                        // that differ by the cost of the loop would suggest
                        // an ordering that is not there.
                        let base_seq = next_seq(&tx, &stream)?;
                        let epoch_ms = now_ms();
                        let mut out = Vec::with_capacity(events.len());
                        for (offset, event) in events.into_iter().enumerate() {
                            let stamped = stamp(event, base_seq + offset as u64, epoch_ms)?;
                            out.push(insert_stamped(&tx, &stream, &stamped)?);
                        }
                        set_next_seq(&tx, &stream, base_seq + out.len() as u64)?;
                        tx.commit().map_err(classify)?;
                        Ok(out)
                    })())
                }
            })
            .await?;

        if let Some(last) = committed.last().and_then(|c| c.position) {
            self.shared.publish(last);
        }
        Ok(committed)
    }

    /// The read, the decision and the insert share one `IMMEDIATE`
    /// transaction, so the invariant is checked against the stream as it is at
    /// that instant.
    ///
    /// Not retried on contention: the decision is `FnOnce`. Contention
    /// surfaces as [`Error::Busy`], and the caller decides whether to build a
    /// fresh decision and call again.
    ///
    /// # This read stays on the writer, and holds the lock
    ///
    /// It cannot move to a reader connection: a reader sees only committed
    /// data, so the decision would be made against a stream another writer may
    /// already have moved past — which is the compare-and-swap this call exists
    /// to avoid.
    ///
    /// The price is that the write lock is held for the whole read, so **other
    /// writes wait for it**. Other *reads* do not: they are served elsewhere
    /// [measured: a decision over 20 000 events held the lock ~780 ms while a
    /// concurrent read took 536 µs, `tests/lock_hold.rs`, debug build].
    ///
    /// `kinds` is the control, and the difference is not marginal. Over 20 000
    /// events: **30.4 ms** reading every kind against **81 µs** naming the one
    /// it folded — 376× [benched: `decide` group, release]. The narrow form
    /// barely moves between 1 000 and 20 000 events (69 µs → 81 µs) because it
    /// reads its own kind off an index rather than the stream, while the wide
    /// one grows with the stream (3.9 ms → 30.4 ms). Name what the fold reads.
    async fn append_if(
        &mut self,
        kinds: Option<&[&str]>,
        decide: Decision,
    ) -> Result<Option<Committed>> {
        let stream = self.stream.clone();
        let kinds = kinds.map(|k| k.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let chain = self.shared.chain.clone();

        let job = move |conn: &mut Connection| {
            Ok((|| {
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(classify)?;
                let stored = select_stream(&tx, &stream, kinds.as_deref(), 0, usize::MAX)?;
                let seen = SqliteEventStore::upcast_rows(&chain, stored)?;

                let Some(event) = decide(&seen) else {
                    // Nothing to record. The transaction is dropped, which
                    // rolls back a read that changed nothing anyway.
                    return Ok(None);
                };
                validate(&event)?;
                let seq = next_seq(&tx, &stream)?;
                let stamped = stamp(event, seq, now_ms())?;
                let committed = insert_stamped(&tx, &stream, &stamped)?;
                set_next_seq(&tx, &stream, seq + 1)?;
                tx.commit().map_err(classify)?;
                Ok(Some(committed))
            })())
        };

        // On the writer, not a reader: this reads *and* appends in one
        // transaction, and a read-only connection could do neither the write
        // nor see it.
        let committed = self.writer_job(job).await?;
        if let Some(position) = committed.and_then(|c| c.position) {
            self.shared.publish(position);
        }
        Ok(committed)
    }

    async fn append_at(&mut self, epoch_ms: u64, event: Map<String, Value>) -> Result<Committed> {
        validate(&event)?;
        let stream = self.stream.clone();

        let committed = self
            .write_retrying(move || {
                let stream = stream.clone();
                let event = event.clone();
                move |conn: &mut Connection| {
                    Ok((|| {
                        let tx = conn
                            .transaction_with_behavior(TransactionBehavior::Immediate)
                            .map_err(classify)?;
                        let seq = next_seq(&tx, &stream)?;
                        let stamped = stamp(event, seq, epoch_ms)?;
                        let committed = insert_stamped(&tx, &stream, &stamped)?;
                        set_next_seq(&tx, &stream, seq + 1)?;
                        tx.commit().map_err(classify)?;
                        Ok(committed)
                    })())
                }
            })
            .await?;

        if let Some(position) = committed.position {
            self.shared.publish(position);
        }
        Ok(committed)
    }

    /// The head is read from the **stored counter**, not from `MAX(seq)`.
    ///
    /// That difference only shows once retention has run, and it is the whole
    /// safety of this call. After a whole stream is removed, `MAX(seq)` is
    /// `NULL` while the counter stands at, say, 51 — so a check against the
    /// maximum would let [`Expected::Unwritten`] pass on a stream that held 50
    /// events. A caller meaning "this is a new order" would be told yes about
    /// an order that was archived.
    ///
    /// So this deliberately disagrees with [`EventStore::head`] on a truncated
    /// stream, and the disagreement is correct: `head` answers "what is the
    /// newest event I can read", a question about the rows; this answers "what
    /// has happened here", a question about the stream.
    async fn append_expecting(
        &mut self,
        expected: Expected,
        event: Map<String, Value>,
    ) -> Result<Committed> {
        validate(&event)?;
        let stream = self.stream.clone();

        let committed = self
            .write_retrying(move || {
                let stream = stream.clone();
                let event = event.clone();
                move |conn: &mut Connection| {
                    Ok((|| {
                        let tx = conn
                            .transaction_with_behavior(TransactionBehavior::Immediate)
                            .map_err(classify)?;

                        // Inside the write, so a writer arriving between the
                        // check and the insert serializes behind the lock
                        // rather than slipping past it.
                        let next = next_seq(&tx, &stream)?;
                        let actual = match next {
                            1 => Expected::Unwritten,
                            next => Expected::Seq(next - 1),
                        };
                        if actual != expected {
                            return Err(Error::HeadMismatch { expected, actual });
                        }

                        let stamped = stamp(event, next, now_ms())?;
                        let committed = insert_stamped(&tx, &stream, &stamped)?;
                        set_next_seq(&tx, &stream, next + 1)?;
                        tx.commit().map_err(classify)?;
                        Ok(committed)
                    })())
                }
            })
            .await?;

        if let Some(position) = committed.position {
            self.shared.publish(position);
        }
        Ok(committed)
    }

    /// Onto the isle's own queue, which is what keeps it ordered against every
    /// other writer on this log. Spawning a task that later called `append`
    /// would leave the queue and race them.
    ///
    /// The write's result is discarded here — see the trait's doc for why
    /// there is nobody left to report it to. Subscribers are not woken for the
    /// same reason; a follower reading the tail finds the event on its next
    /// pass either way.
    fn detach_append(&self, event: Map<String, Value>) -> Result<()> {
        validate(&event)?;
        let stream = self.stream.clone();

        self.shared
            .isle
            .spawn_call(move |conn: &mut Connection| {
                let _ = append_stamped_now(conn, &stream, event);
                Ok(())
            })
            .detach();
        Ok(())
    }

    async fn read_kinds(
        &self,
        kinds: Option<&[&str]>,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<Current>> {
        let stream = self.stream.clone();
        let kinds = kinds.map(|k| k.iter().map(|s| s.to_string()).collect::<Vec<_>>());
        let chain = self.shared.chain.clone();

        self.read_job(move |conn: &mut Connection| {
            Ok((|| {
                let tx = conn.transaction().map_err(classify)?;
                let stored = select_stream(&tx, &stream, kinds.as_deref(), from_seq, limit)?;
                SqliteEventStore::upcast_rows(&chain, stored)
            })())
        })
        .await
    }

    /// `ORDER BY seq DESC LIMIT n`, reversed on the way back — so the tail of
    /// a long log costs `n` rows rather than the whole stream.
    async fn read_last(&self, n: usize) -> Result<Vec<Current>> {
        let stream = self.stream.clone();
        let chain = self.shared.chain.clone();

        self.read_job(move |conn: &mut Connection| {
            Ok((|| {
                let sql = format!(
                    "SELECT {} FROM events WHERE stream = ?1 ORDER BY seq DESC LIMIT ?2",
                    row::COLUMNS
                );
                let mut stmt = conn.prepare(&sql).map_err(classify)?;
                let rows = stmt
                    .query_map(rusqlite::params![stream, clamp_limit(n)], row::read)
                    .map_err(classify)?;
                let mut stored = Vec::new();
                for item in rows {
                    stored.push(item.map_err(classify)?.event);
                }
                stored.reverse();
                SqliteEventStore::upcast_rows(&chain, stored)
            })())
        })
        .await
    }

    async fn head(&self) -> Result<Option<u64>> {
        let stream = self.stream.clone();
        self.read_job(move |conn: &mut Connection| {
            Ok(conn
                .query_row(
                    "SELECT MAX(seq) FROM events WHERE stream = ?1",
                    [stream],
                    |row| row.get::<_, Option<i64>>(0),
                )
                .map_err(classify)
                .map(|max| max.map(|seq| seq as u64)))
        })
        .await
    }

    async fn len(&self) -> Result<usize> {
        let stream = self.stream.clone();
        self.read_job(move |conn: &mut Connection| {
            Ok(conn
                .query_row(
                    "SELECT COUNT(*) FROM events WHERE stream = ?1",
                    [stream],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(classify)
                .map(|count| count as usize))
        })
        .await
    }

    /// A caller's own read-only SQL.
    ///
    /// Identical to [`crate::SqliteEventLog::query`] — the statement is not
    /// scoped to this stream, because SQL over the log is a database-level
    /// question. Prefer the log's method; this one exists so the trait has an
    /// answer.
    /// Under the authorizer, exactly as [`crate::SqliteEventLog::query`] is.
    ///
    /// The readonly check alone is not the same gate: `sqlite3_stmt_readonly`
    /// answers *true* for `ATTACH`, which changes the connection's
    /// configuration rather than any file's contents. Reader connections are
    /// pooled and long-lived, so a database attached through one of them stays
    /// there for whoever draws that connection next. This path used to call
    /// `query_rows` directly and let exactly that through.
    async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Map<String, Value>>> {
        let sql = sql.to_string();
        self.read_job(move |conn: &mut Connection| {
            Ok(crate::hatch::guarded(
                conn,
                Arc::new(AtomicBool::new(false)),
                move |conn| crate::hatch::query_rows(conn, &sql, params, false),
            ))
        })
        .await
    }

    /// [`EventStore::query`] with a deadline, on this store's own reader.
    ///
    /// The same bound [`crate::SqliteEventLog::query_timeout`] gives, reached
    /// from a stream handle. `busy_timeout` covers waiting for a lock, which
    /// is a different thing from a statement that is simply expensive.
    async fn query_timeout(
        &self,
        sql: &str,
        params: Vec<Value>,
        timeout: Duration,
    ) -> Result<Vec<Map<String, Value>>> {
        let sql = sql.to_string();
        let job = move |conn: &mut Connection| {
            Ok(crate::hatch::guarded(
                conn,
                Arc::new(AtomicBool::new(false)),
                move |conn| crate::hatch::query_rows(conn, &sql, params, false),
            ))
        };

        match self.shared.reader().call_timeout(timeout, job).await {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }
}
