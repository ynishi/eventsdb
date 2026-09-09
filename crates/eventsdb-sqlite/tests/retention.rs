//! Retention tests.
//!
//! Two properties carry the design. First, a plan cannot quietly overrun a
//! consumer that has not caught up. Second, once anything has been removed, a
//! fold that would have needed it is refused rather than served a short
//! answer. The rest is behaviour around those.

use std::time::Duration;

use eventsdb_core::error::{Error, Result};
use eventsdb_core::event::now_ms;
use eventsdb_core::position::Recorded;
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{Completeness, Guard, Plan, Projection, SqliteEventLog, Transaction};
use futures_util::StreamExt;
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

/// Counts every event it is shown. A total over a partial log is wrong, so it
/// leaves `tolerates_truncation` at its default.
struct Counter {
    tolerant: bool,
}

impl Counter {
    fn strict() -> Self {
        Counter { tolerant: false }
    }
    fn tolerant() -> Self {
        Counter { tolerant: true }
    }
}

impl Projection for Counter {
    fn name(&self) -> &str {
        "counter"
    }

    fn tolerates_truncation(&self) -> bool {
        self.tolerant
    }

    fn init(&mut self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS counted (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)",
        )
        .map_err(|e| Error::storage(e.to_string()))
    }

    fn reset(&mut self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute_batch("DELETE FROM counted")
            .map_err(|e| Error::storage(e.to_string()))
    }

    fn apply(&mut self, tx: &Transaction<'_>, _event: &Recorded) -> Result<()> {
        tx.execute(
            "INSERT INTO counted (id, n) VALUES (1, 1) \
             ON CONFLICT(id) DO UPDATE SET n = n + 1",
            [],
        )
        .map(|_| ())
        .map_err(|e| Error::storage(e.to_string()))
    }
}

async fn counted(log: &SqliteEventLog) -> Option<i64> {
    let rows = log
        .query("SELECT n FROM counted WHERE id = 1", Vec::new())
        .await
        .unwrap();
    rows.first().map(|r| r["n"].as_i64().unwrap())
}

async fn positions(log: &SqliteEventLog) -> Vec<u64> {
    log.read_all(Position::BEGINNING, &Filter::all(), usize::MAX)
        .await
        .unwrap()
        .iter()
        .map(|r| r.position.get())
        .collect()
}

#[tokio::test]
async fn an_untouched_log_reports_a_complete_history() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("a")).await.unwrap();

    assert_eq!(log.removed_watermark().await.unwrap(), Position::BEGINNING);
    assert_eq!(
        log.completeness_from(Position::BEGINNING).await.unwrap(),
        Completeness::Complete
    );
}

#[tokio::test]
async fn before_removes_a_prefix_and_records_the_watermark() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for _ in 0..5 {
        s.append(event("a")).await.unwrap();
    }

    let report = log
        .retain(Plan::Before(Position::new(3)), Guard::default())
        .await
        .unwrap();
    assert_eq!(report.removed, 3);
    assert_eq!(report.highest_removed, Some(Position::new(3)));
    assert_eq!(report.streams_affected, 1);

    assert_eq!(positions(&log).await, vec![4, 5]);
    assert_eq!(log.removed_watermark().await.unwrap(), Position::new(3));
    assert_eq!(
        log.completeness_from(Position::BEGINNING).await.unwrap(),
        Completeness::Incomplete {
            removed_up_to: Position::new(3)
        }
    );
    // A consumer already past the removed range is unaffected.
    assert_eq!(
        log.completeness_from(Position::new(3)).await.unwrap(),
        Completeness::Complete
    );
}

#[tokio::test]
async fn older_than_removes_by_age() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("old")).await.unwrap();
    s.append(event("old")).await.unwrap();

    tokio::time::sleep(Duration::from_millis(30)).await;
    let cutoff = now_ms();
    tokio::time::sleep(Duration::from_millis(30)).await;

    s.append(event("new")).await.unwrap();

    let report = log
        .retain(Plan::OlderThan(cutoff), Guard::default())
        .await
        .unwrap();
    assert_eq!(report.removed, 2);

    let left = log
        .read_all(Position::BEGINNING, &Filter::all(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].kind(), "new");
}

#[tokio::test]
async fn dropping_a_stream_leaves_the_others_and_their_positions_alone() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut keep = log.stream_handle("keep");
    let mut drop = log.stream_handle("drop");

    keep.append(event("a")).await.unwrap(); // 1
    drop.append(event("a")).await.unwrap(); // 2
    keep.append(event("a")).await.unwrap(); // 3
    drop.append(event("a")).await.unwrap(); // 4
    keep.append(event("a")).await.unwrap(); // 5

    let report = log
        .retain(Plan::Streams(vec!["drop".to_string()]), Guard::default())
        .await
        .unwrap();
    assert_eq!(report.removed, 2);
    assert_eq!(report.streams_affected, 1);

    // The surviving positions are sparse, and keep their original numbers:
    // nothing is renumbered, so a cursor held elsewhere still means what it
    // meant.
    assert_eq!(positions(&log).await, vec![1, 3, 5]);
    assert_eq!(keep.len().await.unwrap(), 3);
    assert_eq!(keep.head().await.unwrap(), Some(3));
}

#[tokio::test]
async fn a_subscription_reads_across_a_hole_without_stalling() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut keep = log.stream_handle("keep");
    let mut gone = log.stream_handle("gone");
    keep.append(event("first")).await.unwrap(); // 1
    gone.append(event("x")).await.unwrap(); // 2
    keep.append(event("second")).await.unwrap(); // 3

    log.retain(Plan::Streams(vec!["gone".to_string()]), Guard::default())
        .await
        .unwrap();

    // Nothing waits for position 2. A range read simply does not find it.
    let mut sub = log.subscribe(Position::BEGINNING, Filter::all()).unwrap();
    let first = sub.next().await.unwrap().unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("the event after the hole arrives")
        .unwrap()
        .unwrap();

    assert_eq!(first.position, Position::new(1));
    assert_eq!(second.position, Position::new(3));
}

#[tokio::test]
async fn the_guard_refuses_to_overrun_a_registered_consumer() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for _ in 0..5 {
        s.append(event("a")).await.unwrap();
    }
    log.checkpoint_save("slow", Position::new(2)).await.unwrap();

    let error = log
        .retain(Plan::Before(Position::new(4)), Guard::RegisteredConsumers)
        .await
        .unwrap_err();

    match error {
        Error::ConsumerBehind {
            consumer,
            cursor,
            up_to,
        } => {
            assert_eq!(consumer, "slow");
            assert_eq!((cursor, up_to), (2, 4));
        }
        other => panic!("expected ConsumerBehind, got {other}"),
    }

    // Refused means nothing happened: not the delete, not the ledger entry.
    assert_eq!(positions(&log).await, vec![1, 2, 3, 4, 5]);
    assert_eq!(log.removed_watermark().await.unwrap(), Position::BEGINNING);
}

#[tokio::test]
async fn a_consumer_that_has_caught_up_does_not_block_retention() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for _ in 0..5 {
        s.append(event("a")).await.unwrap();
    }
    log.checkpoint_save("done", Position::new(4)).await.unwrap();

    let report = log
        .retain(Plan::Before(Position::new(4)), Guard::RegisteredConsumers)
        .await
        .unwrap();
    assert_eq!(report.removed, 4);
}

#[tokio::test]
async fn force_removes_past_a_consumer() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for _ in 0..5 {
        s.append(event("a")).await.unwrap();
    }
    log.checkpoint_save("slow", Position::new(2)).await.unwrap();

    let report = log
        .retain(Plan::Before(Position::new(4)), Guard::Force)
        .await
        .unwrap();
    assert_eq!(report.removed, 4);
    assert_eq!(positions(&log).await, vec![5]);
}

#[tokio::test]
async fn an_empty_stream_list_removes_nothing() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("a")).await.unwrap();

    let report = log
        .retain(Plan::Streams(Vec::new()), Guard::default())
        .await
        .unwrap();
    assert_eq!(report.removed, 0);
    assert_eq!(report.highest_removed, None);
    assert_eq!(positions(&log).await, vec![1]);
    assert_eq!(log.removed_watermark().await.unwrap(), Position::BEGINNING);
}

/// The second load-bearing property: a fold whose input is partly gone is
/// refused, not served short.
#[tokio::test]
async fn a_projection_behind_the_watermark_is_refused() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for _ in 0..5 {
        s.append(event("a")).await.unwrap();
    }

    // Fold the first two, then have retention take the first four.
    let mut runner = log.runner(Counter::strict()).with_batch(2);
    runner.init().await.unwrap();
    assert_eq!(runner.run_once().await.unwrap(), 2);
    log.retain(Plan::Before(Position::new(4)), Guard::Force)
        .await
        .unwrap();

    let error = runner.run_once().await.unwrap_err();
    match error {
        Error::Truncated {
            requested,
            removed_up_to,
        } => assert_eq!((requested, removed_up_to), (2, 4)),
        other => panic!("expected Truncated, got {other}"),
    }
    // The model is left exactly as the last good batch left it.
    assert_eq!(counted(&log).await, Some(2));
}

#[tokio::test]
async fn a_projection_past_the_watermark_keeps_going() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for _ in 0..5 {
        s.append(event("a")).await.unwrap();
    }

    let mut runner = log.runner(Counter::strict());
    runner.init().await.unwrap();
    assert_eq!(runner.catch_up().await.unwrap(), 5);

    log.retain(Plan::Before(Position::new(4)), Guard::default())
        .await
        .unwrap();

    // Its cursor is at 5, past everything removed, so nothing it needs is
    // gone.
    assert_eq!(runner.run_once().await.unwrap(), 0);
    s.append(event("a")).await.unwrap();
    assert_eq!(runner.run_once().await.unwrap(), 1);
    assert_eq!(counted(&log).await, Some(6));
}

#[tokio::test]
async fn a_tolerant_projection_proceeds_past_the_watermark() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for _ in 0..5 {
        s.append(event("a")).await.unwrap();
    }

    log.retain(Plan::Before(Position::new(3)), Guard::Force)
        .await
        .unwrap();

    let mut runner = log.runner(Counter::tolerant());
    runner.init().await.unwrap();
    assert_eq!(
        runner.catch_up().await.unwrap(),
        2,
        "it folds what survives, having declared that acceptable"
    );
}

#[tokio::test]
async fn a_rebuild_on_a_truncated_log_is_refused_before_anything_is_emptied() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for _ in 0..5 {
        s.append(event("a")).await.unwrap();
    }

    let mut runner = log.runner(Counter::strict());
    runner.init().await.unwrap();
    assert_eq!(runner.catch_up().await.unwrap(), 5);
    assert_eq!(counted(&log).await, Some(5));

    log.retain(Plan::Before(Position::new(2)), Guard::Force)
        .await
        .unwrap();

    let error = runner.rebuild().await.unwrap_err();
    assert!(matches!(error, Error::Truncated { .. }), "got {error}");

    // The refusal comes before `reset`, so the model that could not be
    // rebuilt is still the one that was there.
    assert_eq!(counted(&log).await, Some(5));
    assert_eq!(runner.position().await.unwrap(), Position::new(5));
}

#[tokio::test]
async fn the_ledger_outlives_the_events_it_describes() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for _ in 0..4 {
        s.append(event("a")).await.unwrap();
    }

    log.retain(Plan::Before(Position::new(2)), Guard::default())
        .await
        .unwrap();
    log.retain(Plan::Before(Position::new(3)), Guard::default())
        .await
        .unwrap();

    let rows = log
        .query(
            "SELECT plan, removed_count, highest_removed FROM retention ORDER BY id",
            Vec::new(),
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["plan"], json!("before:2"));
    assert_eq!(rows[0]["removed_count"], json!(2));
    assert_eq!(rows[1]["plan"], json!("before:3"));
    assert_eq!(rows[1]["removed_count"], json!(1));
    // The watermark is the highest ever reached, not the last one applied.
    assert_eq!(log.removed_watermark().await.unwrap(), Position::new(3));
}

#[tokio::test]
async fn reclaim_runs_on_a_freshly_created_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let log = SqliteEventLog::open(&path).await.unwrap();

    let mut s = log.stream_handle("s");
    for _ in 0..50 {
        s.append(event("a")).await.unwrap();
    }
    log.retain(Plan::Before(Position::new(40)), Guard::default())
        .await
        .unwrap();

    log.reclaim().await.unwrap();
    assert_eq!(positions(&log).await.len(), 10);
    log.close().await.unwrap();
}

/// The ladder's *upgrade* path, which a freshly created database never
/// exercises: it runs every step at once and so proves only that the steps
/// compose from nothing.
///
/// Rewinds a real database to `user_version = 1` — the shape the previous
/// release wrote — and reopens it, which is what an existing file will do
/// once this version ships.
#[tokio::test]
async fn an_existing_database_is_carried_up_the_ladder() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");

    {
        let log = SqliteEventLog::open(&path).await.unwrap();
        let mut s = log.stream_handle("s");
        s.append(event("kept")).await.unwrap();
        log.close().await.unwrap();
    }

    // Put the file back to what step 1 alone leaves behind.
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "DROP TABLE retention; \
             DROP INDEX events_epoch_ms; \
             PRAGMA user_version = 1;",
        )
        .unwrap();
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 1);
    }

    // Reopening runs step 2 and nothing else.
    let log = SqliteEventLog::open(&path).await.unwrap();
    assert_eq!(log.removed_watermark().await.unwrap(), Position::BEGINNING);

    let survived = log
        .read_all(Position::BEGINNING, &Filter::all(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(survived.len(), 1, "the events were not touched by the step");
    assert_eq!(survived[0].kind(), "kept");

    // And the facilities step 2 adds are there.
    log.retain(Plan::Before(Position::new(1)), Guard::default())
        .await
        .unwrap();
    assert_eq!(log.removed_watermark().await.unwrap(), Position::new(1));
    log.close().await.unwrap();

    let conn = rusqlite::Connection::open(&path).unwrap();
    let version: i64 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, eventsdb_sqlite::TARGET_USER_VERSION);
}

#[tokio::test]
async fn retention_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");

    {
        let log = SqliteEventLog::open(&path).await.unwrap();
        let mut s = log.stream_handle("s");
        for _ in 0..3 {
            s.append(event("a")).await.unwrap();
        }
        log.retain(Plan::Before(Position::new(2)), Guard::default())
            .await
            .unwrap();
        log.close().await.unwrap();
    }

    let log = SqliteEventLog::open(&path).await.unwrap();
    assert_eq!(log.removed_watermark().await.unwrap(), Position::new(2));
    assert_eq!(positions(&log).await, vec![3]);

    // And the next append continues from where the numbering was, not from
    // the highest surviving row.
    let mut s = log.stream_handle("s");
    let committed = s.append(event("a")).await.unwrap();
    assert_eq!(committed.position, Some(Position::new(4)));
    log.close().await.unwrap();
}
