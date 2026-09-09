//! What it costs to keep some reads on the writer.
//!
//! Three read paths did not move to the reader connections: `append_if`'s
//! decision, a `TxnContext` read, and a projection's batch. Each has a reason,
//! and each has a price — the write lock is held while that read runs. These
//! measure the price rather than assume it.

use std::sync::Arc;
use std::time::{Duration, Instant};

use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::SqliteEventLog;
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind, "data": { "n": 1 } })
        .as_object()
        .unwrap()
        .clone()
}

async fn seeded(path: &std::path::Path, n: usize) -> Arc<SqliteEventLog> {
    let log = Arc::new(SqliteEventLog::open(path).await.unwrap());
    let mut s = log.stream_handle("ledger");
    let batch: Vec<Map<String, Value>> = (0..n).map(|_| event("granted")).collect();
    for chunk in batch.chunks(500) {
        s.append_many(chunk.to_vec()).await.unwrap();
    }
    log
}

/// The reassurance: a decision that reads a long stream holds the *write*
/// lock, and other **writers** wait for it — but a reader does not, because
/// readers are no longer on that queue.
///
/// This is the difference the reader connections make. Before them, this test
/// would have measured the whole `append_if`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_long_decision_does_not_delay_readers() {
    let dir = tempfile::tempdir().unwrap();
    let log = seeded(&dir.path().join("events.db"), 20_000).await;

    let deciding = Arc::clone(&log);
    let decision = tokio::spawn(async move {
        let mut ledger = deciding.stream_handle("ledger");
        // Reads the whole stream under the write lock, then appends.
        ledger
            .append_if(
                None,
                Box::new(|seen| {
                    assert_eq!(seen.len(), 20_000);
                    Some(event("decided"))
                }),
            )
            .await
    });

    tokio::time::sleep(Duration::from_millis(5)).await;

    let started = Instant::now();
    log.read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    let read_latency = started.elapsed();

    let started = Instant::now();
    decision.await.unwrap().unwrap();
    let decision_tail = started.elapsed();

    println!("read during a 20k-event decision: {read_latency:?}");
    println!("the decision still had {decision_tail:?} to run");
    assert!(
        read_latency < Duration::from_millis(100),
        "a read waited {read_latency:?} on a decision that holds the write lock"
    );
    log.shutdown().await.unwrap();
}

/// The price, stated: a decision over a long stream is slow, and every *other*
/// write waits for it. `kinds` is the control — naming what the fold reads is
/// what keeps the lock hold proportional to the fold rather than to the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn naming_the_kinds_a_decision_reads_bounds_how_long_it_holds_the_lock() {
    let dir = tempfile::tempdir().unwrap();
    let log = seeded(&dir.path().join("events.db"), 20_000).await;
    // One event of a different kind, which is all the narrow decision needs.
    log.stream_handle("ledger")
        .append(event("interesting"))
        .await
        .unwrap();

    let mut ledger = log.stream_handle("ledger");

    let started = Instant::now();
    ledger
        .append_if(None, Box::new(|_| Some(event("wide"))))
        .await
        .unwrap();
    let wide = started.elapsed();

    let started = Instant::now();
    ledger
        .append_if(
            Some(&["interesting"]),
            Box::new(|seen| {
                assert_eq!(seen.len(), 1);
                Some(event("narrow"))
            }),
        )
        .await
        .unwrap();
    let narrow = started.elapsed();

    println!("decision reading every kind: {wide:?}");
    println!("decision naming one kind:    {narrow:?}");
    assert!(
        narrow < wide,
        "naming the kinds should read less: wide {wide:?}, narrow {narrow:?}"
    );
    log.shutdown().await.unwrap();
}

/// A projection's batch is bounded, so its lock hold is bounded too — the
/// reason `with_batch` exists and the reason the default is not "everything".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_projection_holds_the_lock_for_a_batch_not_for_the_log() {
    use eventsdb_core::error::{Error, Result};
    use eventsdb_core::position::Recorded;
    use eventsdb_sqlite::{Projection, Transaction};

    struct Counter;
    impl Projection for Counter {
        fn name(&self) -> &str {
            "counter"
        }
        fn init(&mut self, tx: &Transaction<'_>) -> Result<()> {
            tx.execute_batch("CREATE TABLE IF NOT EXISTS seen (n INTEGER)")
                .map_err(|e| Error::storage(e.to_string()))
        }
        fn reset(&mut self, tx: &Transaction<'_>) -> Result<()> {
            tx.execute_batch("DELETE FROM seen")
                .map_err(|e| Error::storage(e.to_string()))
        }
        fn apply(&mut self, tx: &Transaction<'_>, _event: &Recorded) -> Result<()> {
            tx.execute("INSERT INTO seen (n) VALUES (1)", [])
                .map(|_| ())
                .map_err(|e| Error::storage(e.to_string()))
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let log = seeded(&dir.path().join("events.db"), 20_000).await;

    let mut runner = log.runner(Counter).with_batch(256);
    runner.init().await.unwrap();

    // One batch, not the whole log.
    let started = Instant::now();
    let applied = runner.run_once().await.unwrap();
    let one_batch = started.elapsed();
    assert_eq!(applied, 256);

    println!("one 256-event batch: {one_batch:?}");
    assert!(
        one_batch < Duration::from_millis(500),
        "a single batch took {one_batch:?}"
    );

    // And a read during the rest of the catch-up is still prompt.
    let catching = Arc::clone(&log);
    let work = tokio::spawn(async move {
        let mut runner = catching.runner(Counter).with_batch(256);
        runner.catch_up().await
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    let started = Instant::now();
    log.read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    let read_latency = started.elapsed();
    work.await.unwrap().unwrap();

    println!("read during a catch-up: {read_latency:?}");
    assert!(read_latency < Duration::from_millis(100));
    log.shutdown().await.unwrap();
}
