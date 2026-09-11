//! Projection tests.
//!
//! The one that matters is `a_failing_apply_rolls_back_the_batch_and_the_cursor`.
//! Everything else here is behaviour; that one is the property the design is
//! built around.

use std::sync::Arc;
use std::time::{Duration, Instant};

use eventsdb_core::error::{Error, Result};
use eventsdb_core::position::Recorded;
use eventsdb_core::{EventLog, EventStore, Position};
use eventsdb_sqlite::{OpenOptions, Projection, SqliteEventLog, Transaction};
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
    let rows = log
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

    let mut runner = log.runner_now(Totals::new());
    runner.init().await.unwrap();
    assert_eq!(runner.catch_up().await.unwrap(), 3);

    assert_eq!(totals_of(&log, "player-1").await, Some(6));
    assert_eq!(runner.position().await.unwrap(), Position::new(3));
}

#[tokio::test]
async fn run_once_returns_zero_when_caught_up() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut runner = log.runner_now(Totals::new());
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
    let mut runner = log.runner_now(Totals::failing_at(3));
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
    let mut runner = log.runner_now(projection);
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

    let mut runner = log.runner_now(Totals::new());
    runner.init().await.unwrap();
    assert_eq!(runner.catch_up().await.unwrap(), 1, "only the scored event");
    assert_eq!(totals_of(&log, "player-1").await, Some(4));

    // The cursor tracks what the projection has *seen*, not what it applied.
    // The two `noise` events were offered and declined, so it is caught up at
    // the log's head rather than parked on the last match — otherwise it would
    // report itself behind for ever and block retention.
    assert_eq!(runner.position().await.unwrap(), Position::new(3));
}

/// A projection that has declined everything since its last match is caught
/// up, and must not hold retention hostage.
#[tokio::test]
async fn a_caught_up_filtered_projection_does_not_block_retention() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(scored(1)).await.unwrap();
    for _ in 0..20 {
        s.append(noise()).await.unwrap();
    }

    let mut runner = log.runner_now(Totals::new());
    runner.init().await.unwrap();
    assert_eq!(runner.catch_up().await.unwrap(), 1, "one scored event");
    assert_eq!(
        runner.position().await.unwrap(),
        log.head_position().await.unwrap(),
        "caught up means caught up with the log, not with its own matches"
    );

    log.retain(
        eventsdb_sqlite::Plan::Before(Position::new(10)),
        eventsdb_sqlite::Guard::RegisteredConsumers,
    )
    .await
    .expect("the default guard lets retention through");
}

/// The cursor only runs ahead when the batch is exhausted. A full batch says
/// nothing about what lies beyond it, so it must stop at the last event it
/// actually applied.
#[tokio::test]
async fn a_full_batch_leaves_the_cursor_on_the_last_applied_event() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for _ in 0..5 {
        s.append(scored(1)).await.unwrap();
    }

    let mut runner = log.runner_now(Totals::new()).with_batch(2);
    runner.init().await.unwrap();
    assert_eq!(runner.run_once().await.unwrap(), 2);
    assert_eq!(runner.position().await.unwrap(), Position::new(2));

    assert_eq!(runner.catch_up().await.unwrap(), 3);
    assert_eq!(runner.position().await.unwrap(), Position::new(5));
    assert_eq!(totals_of(&log, "s").await, Some(5));
}

#[tokio::test]
async fn a_batch_boundary_does_not_change_the_result() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("player-1");
    for _ in 0..10 {
        s.append(scored(1)).await.unwrap();
    }

    let mut runner = log.runner_now(Totals::new()).with_batch(3);
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

    let mut runner = log.runner_now(Totals::new());
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

        let mut runner = log.runner_now(Totals::new());
        runner.init().await.unwrap();
        assert_eq!(runner.catch_up().await.unwrap(), 1);
        log.close().await.unwrap();
    }

    let log = SqliteEventLog::open(&path).await.unwrap();
    let mut s = log.stream_handle("player-1");
    s.append(scored(5)).await.unwrap();

    let mut runner = log.runner_now(Totals::new());
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

    let mut fast = log.runner_now(Counter { name: "fast" });
    fast.init().await.unwrap();
    assert_eq!(fast.catch_up().await.unwrap(), 2);

    // A second consumer starts from the beginning: its cursor is its own.
    let mut slow = log.runner_now(Counter { name: "slow" });
    slow.init().await.unwrap();
    assert_eq!(slow.run_once().await.unwrap(), 2);

    assert_eq!(fast.position().await.unwrap(), Position::new(2));
    assert_eq!(slow.position().await.unwrap(), Position::new(2));
}

// ---------------------------------------------------------------------------
// `follow`: catch up, then wait on the log's wake-up channel.
//
// Every test here that claims the wake-up opens the log with a poll interval
// far longer than the test's own patience, so a pass cannot be the poll
// arriving early. The one test that is *about* the poll says so in its name.
// ---------------------------------------------------------------------------

/// Long enough that nothing below can pass by polling.
const SLOW_POLL: Duration = Duration::from_secs(30);

async fn slow_polling_memory_log() -> Arc<SqliteEventLog> {
    Arc::new(
        SqliteEventLog::open_in_memory_with(OpenOptions::default().poll_interval(SLOW_POLL))
            .await
            .unwrap(),
    )
}

/// Watch the read model until it says `want`, and report how long that took.
///
/// The read goes through the hatch on another handle to the same log, which is
/// how a read model is looked at anywhere: the runner is busy following.
async fn await_total(log: &SqliteEventLog, stream: &str, want: i64, within: Duration) -> Duration {
    let started = Instant::now();
    loop {
        if totals_of(log, stream).await == Some(want) {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < within,
            "totals for `{stream}` never reached {want}; waited {:?} and it was {:?}",
            started.elapsed(),
            totals_of(log, stream).await
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

/// The property the whole thing is for: a parked follow folds a commit on the
/// wake-up, not on the poll.
#[tokio::test]
async fn a_parked_follow_folds_a_commit_well_inside_the_poll_interval() {
    let log = slow_polling_memory_log().await;
    let mut s = log.stream_handle("player-1");

    let mut runner = log.runner_now(Totals::new());
    runner.init().await.unwrap();
    let follow = tokio::spawn(async move { runner.follow().await });

    // Drive the follow into the parked state first. Folding this one proves it
    // is running, and leaves it waiting on the channel — otherwise the
    // measurement below would be of its opening catch-up.
    s.append(scored(1)).await.unwrap();
    await_total(&log, "player-1", 1, Duration::from_secs(5)).await;

    s.append(scored(41)).await.unwrap();
    let latency = await_total(&log, "player-1", 42, Duration::from_secs(5)).await;

    println!("follow folded the commit in {latency:?} against a {SLOW_POLL:?} poll interval");
    assert!(
        latency < SLOW_POLL / 10,
        "a commit on the follow's own log is woken directly, took {latency:?}"
    );

    follow.abort();
    let _ = follow.await;
}

/// A commit landing between the last `run_once` and the wait must not sit
/// there until the poll.
///
/// The timing of that gap cannot be hit on purpose, so this appends a burst
/// across it instead: whichever of them falls in the gap, the read model has
/// to reflect all of them long before the poll interval could have rescued a
/// wake-up that was marked away.
#[tokio::test]
async fn a_burst_appended_across_the_gap_is_folded_without_the_poll() {
    const N: i64 = 200;

    let log = slow_polling_memory_log().await;
    let mut s = log.stream_handle("player-1");

    let mut runner = log.runner_now(Totals::new());
    runner.init().await.unwrap();
    let follow = tokio::spawn(async move { runner.follow().await });

    for _ in 0..N {
        s.append(scored(1)).await.unwrap();
    }

    let latency = await_total(&log, "player-1", N, Duration::from_secs(10)).await;
    println!("the tail of a {N}-event burst was folded {latency:?} after the last append");
    assert!(
        latency < SLOW_POLL / 10,
        "a wake-up was lost: the last events waited {latency:?}"
    );

    follow.abort();
    let _ = follow.await;
}

/// And the poll is still there for the write the channel cannot announce.
///
/// `Shared::notify` is per-log, so an append through a second log on the same
/// file reaches this follow only by looking — which is exactly what the poll
/// interval is for. What is asserted is that it arrives. **There is no lower
/// bound on how long it took**, and there was one once: it read
/// `cross > poll / 4` and failed on CI at 3.98 ms, because whether the round
/// that folded the write was the poll's or the one still finishing the local
/// write below is not observable from outside — `await_total` sees the local
/// fold the moment its transaction commits, while the follow is still in the
/// `run_once` that finds nothing more, and a write landing before that read
/// is folded by it. The ratio the poll costs is measured under `subscribe`
/// in `tests/two_logs.rs`, where the reader is the test itself and there is
/// no such gap.
#[tokio::test]
async fn a_follow_picks_up_another_logs_write_on_the_poll_interval() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");

    let poll = Duration::from_millis(400);
    let a = Arc::new(
        SqliteEventLog::open_with(&path, OpenOptions::default().poll_interval(poll))
            .await
            .unwrap(),
    );
    let b = SqliteEventLog::open(&path).await.unwrap();

    let mut runner = a.runner_now(Totals::new());
    runner.init().await.unwrap();
    let follow = tokio::spawn(async move { runner.follow().await });

    // A write through its own log first, which proves the follow is running.
    let mut through_a = a.stream_handle("player-1");
    through_a.append(scored(1)).await.unwrap();
    let local = await_total(&a, "player-1", 1, Duration::from_secs(5)).await;

    // Then one the channel cannot announce. The bound is generous: the wait
    // may have started at any moment, and a runner under load can miss a
    // poll or two. What would fail here is a follow that never looks.
    let mut through_b = b.stream_handle("player-1");
    through_b.append(scored(7)).await.unwrap();
    let cross = await_total(&a, "player-1", 8, poll * 10).await;

    println!("own log {local:?} / other log {cross:?} / poll {poll:?}");

    follow.abort();
    let _ = follow.await;
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

/// The error policy, stated as a test: the first one ends the follow, and the
/// cursor is where the failed batch left it — which is where it started.
#[tokio::test]
async fn a_failing_apply_ends_the_follow_and_leaves_the_cursor() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("player-1");
    for n in [1, 2, 3] {
        s.append(scored(n)).await.unwrap();
    }

    let mut runner = log.runner_now(Totals::failing_at(2));
    runner.init().await.unwrap();

    let error = runner.follow().await.unwrap_err();
    assert!(error.to_string().contains("on purpose"), "got {error}");

    assert_eq!(
        totals_of(&log, "player-1").await,
        None,
        "the failed batch is not visible"
    );
    assert_eq!(
        runner.position().await.unwrap(),
        Position::BEGINNING,
        "the cursor did not move past the batch that failed"
    );
}

/// Cancellation at the wait, which is where a follow spends its life.
#[tokio::test]
async fn dropping_a_parked_follow_leaves_the_cursor_for_the_next_catch_up() {
    let log = SqliteEventLog::open_in_memory_with(OpenOptions::default().poll_interval(SLOW_POLL))
        .await
        .unwrap();
    let mut s = log.stream_handle("player-1");
    for n in [1, 2, 3] {
        s.append(scored(n)).await.unwrap();
    }

    let mut runner = log.runner_now(Totals::new());
    runner.init().await.unwrap();

    // Nothing is left to fold after the opening catch-up, so the second the
    // timeout expires on is the wait — between batches, not inside one.
    let outcome = tokio::time::timeout(Duration::from_secs(1), runner.follow()).await;
    assert!(outcome.is_err(), "a follow returns only on error");

    assert_eq!(totals_of(&log, "player-1").await, Some(6));
    assert_eq!(runner.position().await.unwrap(), Position::new(3));

    // The runner survived the cancellation and resumes from that cursor.
    s.append(scored(4)).await.unwrap();
    assert_eq!(runner.catch_up().await.unwrap(), 1, "no event folded twice");
    assert_eq!(totals_of(&log, "player-1").await, Some(10));
    assert_eq!(runner.position().await.unwrap(), Position::new(4));
}

/// The wasted read: a projection that names its kinds is woken by a commit it
/// declines, and the round that follows is correct rather than merely harmless
/// — it moves the cursor to the head it has now seen through.
#[tokio::test]
async fn a_commit_of_a_declined_kind_wakes_the_follow_and_moves_the_cursor_to_the_head() {
    let log = SqliteEventLog::open_in_memory_with(OpenOptions::default().poll_interval(SLOW_POLL))
        .await
        .unwrap();
    let mut s = log.stream_handle("player-1");
    s.append(scored(1)).await.unwrap();

    let mut runner = log.runner_now(Totals::new());
    runner.init().await.unwrap();

    // Park it, caught up at the one event of its own kind.
    let outcome = tokio::time::timeout(Duration::from_millis(500), runner.follow()).await;
    assert!(outcome.is_err(), "a follow returns only on error");
    assert_eq!(runner.position().await.unwrap(), Position::new(1));

    // Now append a kind it declines, with the follow parked. The poll is 30
    // seconds away, so the round that moves the cursor below can only have
    // come from the wake-up.
    tokio::select! {
        result = runner.follow() => panic!("a follow returns only on error, got {result:?}"),
        _ = async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            s.append(noise()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(400)).await;
        } => {}
    }

    assert_eq!(
        totals_of(&log, "player-1").await,
        Some(1),
        "a declined kind folds nothing"
    );
    assert_eq!(
        runner.position().await.unwrap(),
        log.head_position().await.unwrap(),
        "the cursor tracks what was seen, so a decline still advances it"
    );
}
