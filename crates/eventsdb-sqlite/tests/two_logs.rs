//! Two logs open on one file.
//!
//! The docs used to say the global order is gap-free "because there is one
//! writer", and that a second connection was the thing this store could not
//! survive. Nothing stops a caller opening the same file twice, so this asks
//! what actually happens when they do.
//!
//! The answer, measured: the order survives — the mechanism is
//! allocate-inside-the-committing-transaction under an `IMMEDIATE` lock, which
//! holds across connections — and so do projections and the retention guard,
//! which work through shared tables. What degrades is the wake-up, by about
//! three orders of magnitude. The docs now say that instead.

use std::sync::Arc;
use std::time::{Duration, Instant};

use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{OpenOptions, SqliteEventLog};
use futures_util::StreamExt;
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

/// The property the whole design rests on, put under two connections.
///
/// The stated reason is "a single writer", but the mechanism is narrower: the
/// position is allocated *inside* the transaction that commits it, and that
/// transaction holds SQLite's write lock from `BEGIN` because every write is
/// `IMMEDIATE`. Two connections cannot both hold it, so allocation order and
/// commit order still cannot diverge.
#[tokio::test]
async fn two_logs_on_one_file_still_produce_a_dense_ordered_global_position() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");

    let a = Arc::new(SqliteEventLog::open(&path).await.unwrap());
    let b = Arc::new(SqliteEventLog::open(&path).await.unwrap());

    const PER_LOG: usize = 60;
    let mut tasks = Vec::new();
    for (which, log) in [("a", Arc::clone(&a)), ("b", Arc::clone(&b))] {
        tasks.push(tokio::spawn(async move {
            let mut handle = log.stream_handle(which);
            for i in 0..PER_LOG {
                handle
                    .append(
                        json!({ "kind": "tick", "data": { "i": i } })
                            .as_object()
                            .unwrap()
                            .clone(),
                    )
                    .await
                    .expect("an append through either log");
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    let all = a
        .read_all(Position::BEGINNING, &Filter::all(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(all.len(), PER_LOG * 2);

    let positions: Vec<u64> = all.iter().map(|r| r.position.get()).collect();
    let expected: Vec<u64> = (1..=(PER_LOG * 2) as u64).collect();
    assert_eq!(
        positions, expected,
        "dense, ordered, no repeats — across two connections"
    );

    // And per stream, `seq` is still 1..=n through the shared counter.
    for which in ["a", "b"] {
        let one = a
            .read_all(
                Position::BEGINNING,
                &Filter::all().stream(which),
                usize::MAX,
            )
            .await
            .unwrap();
        let seqs: Vec<u64> = one.iter().map(|r| r.seq()).collect();
        assert_eq!(seqs, (1..=PER_LOG as u64).collect::<Vec<_>>());
    }

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// What does degrade: the wake-up.
///
/// `Shared::notify` is per-log, so a write through one log does not wake a
/// subscriber on the other. It is not lost — the subscriber's poll finds it —
/// but it arrives on the poll interval rather than immediately. This measures
/// the difference rather than asserting it from the design.
#[tokio::test]
async fn a_subscriber_on_one_log_learns_of_the_others_write_by_polling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");

    let slow_poll = Duration::from_millis(600);
    let options = || OpenOptions {
        poll_interval: slow_poll,
        ..OpenOptions::default()
    };

    let a = SqliteEventLog::open_with(&path, options()).await.unwrap();
    let b = SqliteEventLog::open_with(&path, options()).await.unwrap();

    let mut through_a = a.stream_handle("s");
    let mut through_b = b.stream_handle("s");

    // A fresh subscription reads immediately, so it has to be driven into the
    // parked state first — otherwise the measurement is of that first read and
    // says nothing about wake-ups.
    async fn park(
        sub: &mut (impl StreamExt<Item = eventsdb_core::Result<eventsdb_core::Recorded>> + Unpin),
    ) {
        assert!(
            tokio::time::timeout(Duration::from_millis(50), sub.next())
                .await
                .is_err(),
            "expected the subscription to be caught up and waiting"
        );
    }

    // Same log as the subscriber: woken directly.
    let mut own = a.subscribe(Position::BEGINNING, Filter::all()).unwrap();
    park(&mut own).await;
    let started = Instant::now();
    through_a.append(event("near")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), own.next())
        .await
        .expect("the local write arrives")
        .unwrap()
        .unwrap();
    let local_latency = started.elapsed();
    assert_eq!(got.kind(), "near");

    // The other log: no in-process wake-up reaches this subscriber.
    let mut far = a.subscribe(got.position, Filter::all()).unwrap();
    park(&mut far).await;
    let started = Instant::now();
    through_b.append(event("far")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), far.next())
        .await
        .expect("the other log's write still arrives")
        .unwrap()
        .unwrap();
    let cross_latency = started.elapsed();
    assert_eq!(got.kind(), "far", "nothing is lost — only delayed");

    println!("local {local_latency:?} / cross-log {cross_latency:?} / poll {slow_poll:?}");
    assert!(
        local_latency < slow_poll / 4,
        "a write on the subscriber's own log is woken directly, took {local_latency:?}"
    );
    assert!(
        cross_latency > local_latency * 10,
        "a write on the other log waits for the poll instead: \
         local {local_latency:?} vs cross-log {cross_latency:?}"
    );

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// A projection is unaffected: it reads and checkpoints inside one
/// transaction, and takes no notice of the watch at all.
#[tokio::test]
async fn a_projection_on_one_log_folds_events_written_through_the_other() {
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
    let path = dir.path().join("events.db");
    let a = SqliteEventLog::open(&path).await.unwrap();
    let b = SqliteEventLog::open(&path).await.unwrap();

    let mut through_b = b.stream_handle("s");
    for _ in 0..4 {
        through_b.append(event("a")).await.unwrap();
    }

    let mut runner = a.runner(Counter);
    runner.init().await.unwrap();
    assert_eq!(
        runner.catch_up().await.unwrap(),
        4,
        "the fold sees the other log's writes"
    );
    assert_eq!(runner.position().await.unwrap(), Position::new(4));

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// Retention's guard reads the checkpoints table, which is shared, so it sees
/// consumers registered through either log.
#[tokio::test]
async fn the_retention_guard_sees_a_consumer_registered_through_the_other_log() {
    use eventsdb_core::error::Error;
    use eventsdb_sqlite::{Guard, Plan};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let a = SqliteEventLog::open(&path).await.unwrap();
    let b = SqliteEventLog::open(&path).await.unwrap();

    let mut s = a.stream_handle("s");
    for _ in 0..5 {
        s.append(event("a")).await.unwrap();
    }
    b.checkpoint_save("slow", Position::new(2)).await.unwrap();

    let error = a
        .retain(Plan::Before(Position::new(4)), Guard::RegisteredConsumers)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::ConsumerBehind { .. }), "got {error}");

    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
