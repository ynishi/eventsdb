//! Projection tests.
//!
//! The one that matters is `a_failing_apply_rolls_back_the_batch_and_the_cursor`.
//! Everything else here is behaviour; that one is the property the design is
//! built around.

use eventsdb_core::error::{Error, Result};
use eventsdb_core::position::Recorded;
use eventsdb_core::{EventStore, Position};
use eventsdb_sqlite::{Projection, SqliteEventLog, Transaction};
use serde_json::{json, Map, Value};

/// Sums `data.n` per stream, for events of kind `scored`.
struct Totals {
    /// When set, `apply` fails on the event at this position. Stands in for
    /// any mid-batch failure: a constraint violation, a panic in the fold, a
    /// process that dies.
    fail_at: Option<u64>,
}

impl Totals {
    fn new() -> Self {
        Totals { fail_at: None }
    }

    fn failing_at(position: u64) -> Self {
        Totals {
            fail_at: Some(position),
        }
    }
}

impl Projection for Totals {
    fn name(&self) -> &str {
        "totals"
    }

    fn kinds(&self) -> Option<Vec<String>> {
        Some(vec!["scored".to_string()])
    }

    fn init(&mut self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS totals (
                 stream TEXT PRIMARY KEY,
                 total  INTEGER NOT NULL
             )",
        )
        .map_err(|e| Error::storage(e.to_string()))
    }

    fn reset(&mut self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute_batch("DROP TABLE IF EXISTS totals")
            .map_err(|e| Error::storage(e.to_string()))
    }

    fn apply(&mut self, tx: &Transaction<'_>, event: &Recorded) -> Result<()> {
        if self.fail_at == Some(event.position.get()) {
            return Err(Error::storage("projection failed on purpose"));
        }
        let n = event.event["data"]["n"].as_i64().unwrap_or(0);
        tx.execute(
            "INSERT INTO totals (stream, total) VALUES (?1, ?2) \
             ON CONFLICT(stream) DO UPDATE SET total = total + excluded.total",
            rusqlite_params(&event.stream, n),
        )
        .map(|_| ())
        .map_err(|e| Error::storage(e.to_string()))
    }
}

fn rusqlite_params(stream: &str, n: i64) -> [Box<dyn rusqlite::ToSql>; 2] {
    [Box::new(stream.to_string()), Box::new(n)]
}

fn scored(n: i64) -> Map<String, Value> {
    json!({ "kind": "scored", "data": { "n": n } })
        .as_object()
        .unwrap()
        .clone()
}

fn noise() -> Map<String, Value> {
    json!({ "kind": "noise" }).as_object().unwrap().clone()
}

/// Read the model back, outside any runner.
async fn totals_of(log: &SqliteEventLog, stream: &str) -> Option<i64> {
    let handle = log.stream_handle("irrelevant");
    let rows = handle
        .query(
            "SELECT total FROM totals WHERE stream = ?1",
            vec![json!(stream)],
        )
        .await
        .unwrap();
    rows.first().map(|r| r["total"].as_i64().unwrap())
}

#[tokio::test]
async fn a_projection_folds_the_log_and_advances_its_cursor() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("player-1");
    for n in [1, 2, 3] {
        s.append(scored(n)).await.unwrap();
    }

    let mut runner = log.runner(Totals::new());
    runner.init().await.unwrap();
    assert_eq!(runner.catch_up().await.unwrap(), 3);

    assert_eq!(totals_of(&log, "player-1").await, Some(6));
    assert_eq!(runner.position().await.unwrap(), Position::new(3));
}

#[tokio::test]
async fn run_once_returns_zero_when_caught_up() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut runner = log.runner(Totals::new());
    runner.init().await.unwrap();
    assert_eq!(runner.run_once().await.unwrap(), 0);

    let mut s = log.stream_handle("player-1");
    s.append(scored(5)).await.unwrap();
    assert_eq!(runner.run_once().await.unwrap(), 1);
    assert_eq!(runner.run_once().await.unwrap(), 0);
}

/// The exactly-once property, stated as a test.
///
/// A failure part-way through a batch must leave *neither* the read model nor
/// the cursor moved — otherwise a restart would either double-count what was
/// applied or skip what was not. Both halves are checked.
#[tokio::test]
async fn a_failing_apply_rolls_back_the_batch_and_the_cursor() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("player-1");
    for n in [1, 2, 3, 4, 5] {
        s.append(scored(n)).await.unwrap();
    }

    // The table is created in its own committed transaction, so its existence
    // is not what the rollback below is being tested on.
    let mut runner = log.runner(Totals::failing_at(3));
    runner.init().await.unwrap();

    let error = runner.catch_up().await.unwrap_err();
    assert!(error.to_string().contains("on purpose"), "got {error}");

    // Nothing applied: not even the two events before the failing one.
    assert_eq!(
        totals_of(&log, "player-1").await,
        None,
        "a partial batch must not be visible"
    );
    // And the cursor did not move, so nothing is skipped on the retry.
    assert_eq!(runner.position().await.unwrap(), Position::BEGINNING);

    // Fix the projection and run again: every event is applied exactly once,
    // with no dedupe table anywhere.
    let mut projection = runner.into_inner().unwrap();
    projection.fail_at = None;
    let mut runner = log.runner(projection);
    assert_eq!(runner.catch_up().await.unwrap(), 5);
    assert_eq!(totals_of(&log, "player-1").await, Some(15));
    assert_eq!(runner.position().await.unwrap(), Position::new(5));
}

#[tokio::test]
async fn a_projection_only_sees_the_kinds_it_names() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("player-1");
    s.append(noise()).await.unwrap();
    s.append(scored(4)).await.unwrap();
    s.append(noise()).await.unwrap();

    let mut runner = log.runner(Totals::new());
    runner.init().await.unwrap();
    assert_eq!(runner.catch_up().await.unwrap(), 1, "only the scored event");
    assert_eq!(totals_of(&log, "player-1").await, Some(4));

    // The cursor sits on the event that was applied, not on the log's head:
    // the two `noise` events are simply never offered.
    assert_eq!(runner.position().await.unwrap(), Position::new(2));
}

#[tokio::test]
async fn a_batch_boundary_does_not_change_the_result() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("player-1");
    for _ in 0..10 {
        s.append(scored(1)).await.unwrap();
    }

    let mut runner = log.runner(Totals::new()).with_batch(3);
    runner.init().await.unwrap();
    assert_eq!(runner.catch_up().await.unwrap(), 10);
    assert_eq!(totals_of(&log, "player-1").await, Some(10));
}

#[tokio::test]
async fn rebuild_empties_the_model_and_replays_from_the_start() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("player-1");
    for n in [1, 2, 3] {
        s.append(scored(n)).await.unwrap();
    }

    let mut runner = log.runner(Totals::new());
    runner.init().await.unwrap();
    runner.catch_up().await.unwrap();
    assert_eq!(totals_of(&log, "player-1").await, Some(6));

    // A rebuild is not a second fold onto what is already there.
    assert_eq!(runner.rebuild().await.unwrap(), 3);
    assert_eq!(totals_of(&log, "player-1").await, Some(6));
    assert_eq!(runner.position().await.unwrap(), Position::new(3));
}

#[tokio::test]
async fn a_restart_resumes_from_the_stored_cursor_without_double_counting() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");

    {
        let log = SqliteEventLog::open(&path).await.unwrap();
        let mut s = log.stream_handle("player-1");
        s.append(scored(10)).await.unwrap();

        let mut runner = log.runner(Totals::new());
        runner.init().await.unwrap();
        assert_eq!(runner.catch_up().await.unwrap(), 1);
        log.close().await.unwrap();
    }

    let log = SqliteEventLog::open(&path).await.unwrap();
    let mut s = log.stream_handle("player-1");
    s.append(scored(5)).await.unwrap();

    let mut runner = log.runner(Totals::new());
    runner.init().await.unwrap();
    assert_eq!(
        runner.catch_up().await.unwrap(),
        1,
        "the first event was already folded and is not offered again"
    );
    assert_eq!(totals_of(&log, "player-1").await, Some(15));
    log.close().await.unwrap();
}

#[tokio::test]
async fn two_projections_keep_separate_cursors() {
    struct Counter {
        name: &'static str,
    }

    impl Projection for Counter {
        fn name(&self) -> &str {
            self.name
        }
        fn init(&mut self, tx: &Transaction<'_>) -> Result<()> {
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS counts (name TEXT PRIMARY KEY, n INTEGER NOT NULL)",
            )
            .map_err(|e| Error::storage(e.to_string()))
        }
        fn reset(&mut self, tx: &Transaction<'_>) -> Result<()> {
            tx.execute_batch("DELETE FROM counts")
                .map_err(|e| Error::storage(e.to_string()))
        }
        fn apply(&mut self, tx: &Transaction<'_>, _event: &Recorded) -> Result<()> {
            tx.execute(
                "INSERT INTO counts (name, n) VALUES (?1, 1) \
                 ON CONFLICT(name) DO UPDATE SET n = n + 1",
                [self.name],
            )
            .map(|_| ())
            .map_err(|e| Error::storage(e.to_string()))
        }
    }

    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(scored(1)).await.unwrap();
    s.append(scored(1)).await.unwrap();

    let mut fast = log.runner(Counter { name: "fast" });
    fast.init().await.unwrap();
    assert_eq!(fast.catch_up().await.unwrap(), 2);

    // A second consumer starts from the beginning: its cursor is its own.
    let mut slow = log.runner(Counter { name: "slow" });
    slow.init().await.unwrap();
    assert_eq!(slow.run_once().await.unwrap(), 2);

    assert_eq!(fast.position().await.unwrap(), Position::new(2));
    assert_eq!(slow.position().await.unwrap(), Position::new(2));
}
