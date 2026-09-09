//! What the two consumers of this crate turned out to need.
//!
//! Every test here exists because porting a real system hit the gap it
//! covers — a caller-supplied timestamp for an import, an append from a
//! `Drop`, a join that does not consume the log, and a bound on a statement
//! that is expensive rather than merely contended.

use std::sync::Arc;
use std::time::Duration;

use eventsdb_core::error::Error;
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{Guard, Plan, SqliteEventLog};
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

const AN_OLD_TIME: u64 = 1_600_000_000_000; // 2020-09
const A_NEWER_TIME: u64 = 1_700_000_000_000; // 2023-11

/// An import carries the time a fact happened. The store keeps assigning the
/// coordinates.
#[tokio::test]
async fn an_imported_event_keeps_its_own_timestamp() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    let committed = log
        .with_transaction(|tx| tx.append_at("chapter-1", AN_OLD_TIME, event("noted")))
        .await
        .unwrap();

    assert_eq!(committed.epoch_ms, AN_OLD_TIME);
    // Still the store's to give.
    assert_eq!(committed.seq, 1);
    assert_eq!(committed.position, Some(Position::new(1)));

    let read = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(read[0].event["epoch_ms"], json!(AN_OLD_TIME));
}

/// Order comes from `seq` and `position`, not from the clock — so an import
/// that arrives out of time order still reads back in the order it landed.
#[tokio::test]
async fn a_supplied_timestamp_does_not_reorder_anything() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    log.with_transaction(|tx| {
        tx.append_at("s", A_NEWER_TIME, event("first"))?;
        tx.append_at("s", AN_OLD_TIME, event("second"))?;
        Ok(())
    })
    .await
    .unwrap();

    let read = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    let kinds: Vec<&str> = read.iter().map(|r| r.kind()).collect();
    assert_eq!(kinds, vec!["first", "second"]);
    assert_eq!(read[0].seq(), 1);
    assert_eq!(read[1].seq(), 2);
}

/// The one thing a supplied timestamp does change: age-based retention sees
/// the supplied time, so imported history can be old and sit at a high
/// position. That is the shape `Plan::Streams` already produces.
#[tokio::test]
async fn age_based_retention_reads_the_supplied_time() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    log.with_transaction(|tx| {
        tx.append_at("s", AN_OLD_TIME, event("ancient"))?;
        tx.append("s", event("now"))?;
        Ok(())
    })
    .await
    .unwrap();

    let report = log
        .retain(Plan::OlderThan(A_NEWER_TIME), Guard::Force)
        .await
        .unwrap();
    assert_eq!(report.removed, 1);

    let left = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(left.len(), 1);
    assert_eq!(left[0].kind(), "now");
}

#[tokio::test]
async fn a_batch_may_carry_a_time_per_event() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    let committed = log
        .with_transaction(|tx| {
            tx.append_many_at(
                "s",
                vec![
                    (AN_OLD_TIME, event("a")),
                    (AN_OLD_TIME + 1, event("b")),
                    (A_NEWER_TIME, event("c")),
                ],
            )
        })
        .await
        .unwrap();

    assert_eq!(
        committed.iter().map(|c| c.epoch_ms).collect::<Vec<_>>(),
        vec![AN_OLD_TIME, AN_OLD_TIME + 1, A_NEWER_TIME]
    );
    assert_eq!(
        committed.iter().map(|c| c.seq).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}

/// A `Drop` cannot await and must not block, but a session boundary still has
/// to be recorded.
#[tokio::test]
async fn a_detached_append_lands_and_stays_ordered() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("opened")).await.unwrap();

    log.detach_append("s", event("closed")).unwrap();
    // Submitted after the detached one, on the same queue, so it lands after.
    let after = s.append(event("after")).await.unwrap();

    assert_eq!(after.seq, 3, "the detached append took seq 2");
    let read = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    let kinds: Vec<&str> = read.iter().map(|r| r.kind()).collect();
    assert_eq!(kinds, vec!["opened", "closed", "after"]);
}

/// The envelope is checked before queueing, so a malformed event is refused to
/// the caller's face rather than dropped into the void.
#[tokio::test]
async fn a_detached_append_refuses_a_malformed_event_synchronously() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let bad = json!({ "kind": "a", "surprise": 1 })
        .as_object()
        .unwrap()
        .clone();

    let error = log.detach_append("s", bad).unwrap_err();
    assert!(matches!(error, Error::Validation(_)), "got {error}");
}

/// Joining is what makes a queued detached append land before a host exits —
/// and `close(self)` cannot be called on a log behind an `Arc`.
#[tokio::test]
async fn shutdown_drains_the_queue_without_consuming_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");

    {
        let log = Arc::new(SqliteEventLog::open(&path).await.unwrap());
        let held = Arc::clone(&log);

        held.detach_append("s", event("closed")).unwrap();
        // A shared reference is all we have, and it is enough.
        held.shutdown().await.unwrap();
        // Idempotent.
        log.shutdown().await.unwrap();
    }

    let log = SqliteEventLog::open(&path).await.unwrap();
    let read = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(read.len(), 1, "the detached append drained before the join");
    assert_eq!(read[0].kind(), "closed");
    log.close().await.unwrap();
}

/// `busy_timeout` bounds waiting for a lock, not a statement that is simply
/// expensive. Without a deadline there is nothing to interrupt this, and
/// because SQL is served from the writer thread it would stall every append.
#[tokio::test]
async fn a_deadline_interrupts_an_expensive_statement() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    let runaway = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c \
                   WHERE x < 1000000000) SELECT count(*) FROM c";

    let error = log
        .query_timeout(runaway, Vec::new(), Duration::from_millis(150))
        .await
        .unwrap_err();
    assert!(
        error.is_timeout(),
        "a deadline is its own thing, not contention, got {error}"
    );
    assert!(
        !error.is_busy(),
        "and nothing should retry it automatically"
    );

    // And the log is still usable: the interrupt did not poison the thread.
    let mut s = log.stream_handle("s");
    assert_eq!(s.append(event("after")).await.unwrap().seq, 1);
}

#[tokio::test]
async fn a_deadline_that_is_not_reached_returns_normally() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("a")).await.unwrap();

    let rows: Vec<Map<String, Value>> = log
        .query_timeout(
            "SELECT count(*) AS n FROM events",
            Vec::new(),
            Duration::from_secs(5),
        )
        .await
        .unwrap();
    assert_eq!(rows[0]["n"], json!(1));
}

/// The import shape end to end: dedupe on a caller-built index over
/// `meta.event_id`, times preserved, the whole thing rolled back as one.
#[tokio::test]
async fn an_idempotent_import_is_expressible() {
    fn imported(id: &str, at: u64) -> (u64, Map<String, Value>) {
        (
            at,
            json!({ "kind": "noted", "meta": { "event_id": id }, "data": { "payload": "x" } })
                .as_object()
                .unwrap()
                .clone(),
        )
    }

    let log = SqliteEventLog::open_in_memory().await.unwrap();

    // The dedupe index: adding an index to `events` is deliberately allowed.
    log.with_transaction(|tx| {
        tx.execute_batch(
            "CREATE UNIQUE INDEX events_event_id \
             ON events (json_extract(meta, '$.event_id'))",
        )
        .map_err(|e| Error::storage(e.to_string()))
    })
    .await
    .unwrap();

    let batch = vec![
        imported("01H0", AN_OLD_TIME),
        imported("01H1", AN_OLD_TIME + 5),
    ];
    log.with_transaction(move |tx| tx.append_many_at("chapter-1", batch).map(|_| ()))
        .await
        .unwrap();

    // Re-importing the same ids conflicts, and the whole transaction rolls
    // back — so a partial re-import cannot happen.
    let again = vec![
        imported("01H1", AN_OLD_TIME + 5),
        imported("01H2", A_NEWER_TIME),
    ];
    let error = log
        .with_transaction(move |tx| tx.append_many_at("chapter-1", again).map(|_| ()))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Storage(_)), "got {error}");

    let read = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(read.len(), 2, "the failed re-import left nothing behind");
    assert_eq!(read[0].event["epoch_ms"], json!(AN_OLD_TIME));
}
