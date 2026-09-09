//! Reads on their own connections.
//!
//! Everything used to run on the one thread that serves writes, which threw
//! away the property WAL exists for. The first test here is the measurement
//! that showed it; the rest pin what the change must not have broken.

use std::sync::Arc;
use std::time::{Duration, Instant};

use eventsdb_core::error::Error;
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{OpenOptions, SqliteEventLog};
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

/// About 300ms of work, run while a write transaction is open.
const SLOW: &str = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c \
                    WHERE x < 12000000) SELECT count(*) FROM c";

/// The measurement this change came from.
///
/// Before readers existed: 73µs idle, **8.2 seconds** while one write
/// transaction was open — the read was not waiting for the database, it was
/// waiting for the queue. The assertion is deliberately loose (a CI machine
/// under load will not reproduce a ratio) but the failure it guards against is
/// three orders of magnitude wide.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_does_not_wait_for_a_write_to_finish() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let log = Arc::new(SqliteEventLog::open(&path).await.unwrap());
    log.stream_handle("s").append(event("a")).await.unwrap();

    let writer = Arc::clone(&log);
    let write = tokio::spawn(async move {
        writer
            .with_transaction(|tx| {
                tx.append("s", event("slow"))?;
                tx.execute_batch(SLOW)
                    .map_err(|e| Error::storage(e.to_string()))
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(40)).await;

    let started = Instant::now();
    let read = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    let latency = started.elapsed();

    write.await.unwrap().unwrap();

    assert_eq!(read.len(), 1, "the open transaction is not visible yet");
    assert!(
        latency < Duration::from_millis(100),
        "a read during a write took {latency:?}; it is queueing behind it"
    );
    log.shutdown().await.unwrap();
}

/// The risk the change introduces: an append commits on the writer, and the
/// very next read goes to a *different* connection.
#[tokio::test]
async fn a_read_sees_the_write_that_just_returned() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let log = SqliteEventLog::open(&path).await.unwrap();
    let mut s = log.stream_handle("s");

    for i in 1..=20 {
        let committed = s.append(event("a")).await.unwrap();
        assert_eq!(committed.seq, i);

        // Straight after the append returns, through a reader.
        assert_eq!(s.head().await.unwrap(), Some(i));
        assert_eq!(s.len().await.unwrap(), i as usize);
        assert_eq!(
            log.read_all(Position::BEGINNING, &Filter::all(), 100)
                .await
                .unwrap()
                .len(),
            i as usize
        );
        assert_eq!(log.head_position().await.unwrap(), Position::new(i));
    }
    log.shutdown().await.unwrap();
}

/// The same, for the two paths that write through the hatch rather than a
/// stream handle.
#[tokio::test]
async fn a_read_sees_a_hatch_write_and_a_retention_that_just_returned() {
    use eventsdb_sqlite::{Guard, Plan};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let log = SqliteEventLog::open(&path).await.unwrap();

    log.with_transaction(|tx| {
        tx.append("s", event("a"))?;
        tx.append("s", event("b"))?;
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        log.read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .len(),
        2
    );

    log.checkpoint_save("c", Position::new(1)).await.unwrap();
    assert_eq!(log.checkpoint_load("c").await.unwrap(), Position::new(1));

    log.retain(Plan::Before(Position::new(1)), Guard::Force)
        .await
        .unwrap();
    assert_eq!(log.removed_watermark().await.unwrap(), Position::new(1));
    assert_eq!(
        log.read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    log.shutdown().await.unwrap();
}

/// A reader connection is opened `READ_ONLY` and pinned with `query_only`, so
/// a write reaching one is refused by SQLite rather than by us noticing.
#[tokio::test]
async fn a_reader_cannot_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let log = SqliteEventLog::open(&path).await.unwrap();
    log.stream_handle("s").append(event("a")).await.unwrap();

    // `query` runs on a reader, and refuses a writing statement before it gets
    // there — but the connection would refuse it too.
    let error = log
        .query("DELETE FROM events", Vec::new())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(_)), "got {error}");

    assert_eq!(
        log.read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    log.shutdown().await.unwrap();
}

/// Turning readers off puts everything back on the writer, which is what an
/// in-memory log does anyway — each `:memory:` open is its own database, so a
/// second connection would see an empty one.
#[tokio::test]
async fn readers_can_be_turned_off_and_an_in_memory_log_never_has_them() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");

    let log = SqliteEventLog::open_with(
        &path,
        OpenOptions {
            readers: 0,
            ..OpenOptions::default()
        },
    )
    .await
    .unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("a")).await.unwrap();
    assert_eq!(s.head().await.unwrap(), Some(1));
    log.shutdown().await.unwrap();

    // In-memory ignores the setting and stays correct.
    let mem = SqliteEventLog::open_in_memory_with(OpenOptions {
        readers: 4,
        ..OpenOptions::default()
    })
    .await
    .unwrap();
    let mut s = mem.stream_handle("s");
    s.append(event("a")).await.unwrap();
    assert_eq!(s.head().await.unwrap(), Some(1));
    assert_eq!(
        mem.read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .len(),
        1,
        "an in-memory log reads its own writes because it has no second connection"
    );
}

/// Reads spread over the readers, so one slow read does not block the next.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_read_does_not_block_another_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let log = Arc::new(SqliteEventLog::open(&path).await.unwrap());
    log.stream_handle("s").append(event("a")).await.unwrap();

    let busy = Arc::clone(&log);
    let slow = tokio::spawn(async move { busy.query(SLOW, Vec::new()).await });

    tokio::time::sleep(Duration::from_millis(40)).await;

    let started = Instant::now();
    log.read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    let latency = started.elapsed();

    slow.await.unwrap().unwrap();
    assert!(
        latency < Duration::from_millis(100),
        "a read took {latency:?} while another read was running"
    );
    log.shutdown().await.unwrap();
}
