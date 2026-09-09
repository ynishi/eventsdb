//! End-to-end tests for the SQLite backend.
//!
//! The properties worth testing here are the ones the design rests on: that a
//! position is dense and ordered as read, that a decision is taken against the
//! stream under the lock, and that a schema survives being reopened.

use std::collections::BTreeSet;
use std::time::Duration;

use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::SqliteEventLog;
use futures_util::StreamExt;
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind })
        .as_object()
        .expect("literal is an object")
        .clone()
}

fn event_with(kind: &str, data: Value) -> Map<String, Value> {
    json!({ "kind": kind, "data": data })
        .as_object()
        .expect("literal is an object")
        .clone()
}

#[tokio::test]
async fn seq_is_per_stream_and_position_is_global() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut a = log.stream_handle("a");
    let mut b = log.stream_handle("b");

    let a1 = a.append(event("x")).await.unwrap();
    let b1 = b.append(event("x")).await.unwrap();
    let a2 = a.append(event("x")).await.unwrap();

    assert_eq!(
        (a1.seq, b1.seq, a2.seq),
        (1, 1, 2),
        "seq counts within a stream"
    );
    assert_eq!(a1.position, Some(Position::new(1)));
    assert_eq!(b1.position, Some(Position::new(2)));
    assert_eq!(a2.position, Some(Position::new(3)));
}

#[tokio::test]
async fn concurrent_writers_produce_positions_with_no_gap_and_no_repeat() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    const STREAMS: usize = 8;
    const PER_STREAM: usize = 25;

    let mut tasks = Vec::new();
    for s in 0..STREAMS {
        let mut handle = log.stream_handle(&format!("stream-{s}"));
        tasks.push(tokio::spawn(async move {
            for i in 0..PER_STREAM {
                handle
                    .append(event_with("tick", json!({ "i": i })))
                    .await
                    .unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }

    let all = log
        .read_all(Position::BEGINNING, &Filter::all(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(all.len(), STREAMS * PER_STREAM);

    // Dense: exactly 1..=n, each once. This is the property that lets a
    // subscription be a plain range read.
    let seen: BTreeSet<u64> = all.iter().map(|r| r.position.get()).collect();
    let expected: BTreeSet<u64> = (1..=(STREAMS * PER_STREAM) as u64).collect();
    assert_eq!(seen, expected, "positions are dense with no repeats");

    // And they come back in order.
    assert!(all.windows(2).all(|w| w[0].position < w[1].position));

    // Per stream, seq is still 1..=PER_STREAM in order.
    for s in 0..STREAMS {
        let one = log
            .read_all(
                Position::BEGINNING,
                &Filter::all().stream(format!("stream-{s}")),
                usize::MAX,
            )
            .await
            .unwrap();
        let seqs: Vec<u64> = one.iter().map(|r| r.seq()).collect();
        assert_eq!(seqs, (1..=PER_STREAM as u64).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn read_all_is_exclusive_on_from_so_a_cursor_feeds_back_in() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for _ in 0..5 {
        s.append(event("x")).await.unwrap();
    }

    let first = log
        .read_all(Position::BEGINNING, &Filter::all(), 2)
        .await
        .unwrap();
    assert_eq!(first.len(), 2);

    let cursor = first.last().unwrap().position;
    let next = log.read_all(cursor, &Filter::all(), 2).await.unwrap();
    assert_eq!(next.len(), 2);
    assert_eq!(
        next[0].position,
        Position::new(3),
        "no event is served twice"
    );
}

#[tokio::test]
async fn filters_select_by_kind_and_stream_and_an_empty_kind_list_selects_nothing() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut a = log.stream_handle("a");
    let mut b = log.stream_handle("b");
    a.append(event("keep")).await.unwrap();
    a.append(event("drop")).await.unwrap();
    b.append(event("keep")).await.unwrap();

    let by_kind = log
        .read_all(Position::BEGINNING, &Filter::kinds(["keep"]), usize::MAX)
        .await
        .unwrap();
    assert_eq!(by_kind.len(), 2);
    assert!(by_kind.iter().all(|r| r.kind() == "keep"));

    let by_both = log
        .read_all(
            Position::BEGINNING,
            &Filter::kinds(["keep"]).stream("a"),
            usize::MAX,
        )
        .await
        .unwrap();
    assert_eq!(by_both.len(), 1);
    assert_eq!(by_both[0].stream, "a");

    let none = log
        .read_all(
            Position::BEGINNING,
            &Filter::kinds(Vec::<String>::new()),
            usize::MAX,
        )
        .await
        .unwrap();
    assert!(none.is_empty());
}

#[tokio::test]
async fn a_subscription_catches_up_and_then_stays_live() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("before")).await.unwrap();

    let mut sub = log.subscribe(Position::BEGINNING, Filter::all()).unwrap();

    // The catch-up half.
    let first = sub.next().await.unwrap().unwrap();
    assert_eq!(first.kind(), "before");

    // The live half, with no seam the consumer has to handle.
    s.append(event("after")).await.unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("a live event arrives")
        .unwrap()
        .unwrap();
    assert_eq!(second.kind(), "after");
    assert_eq!(second.position, Position::new(2));
}

#[tokio::test]
async fn a_subscription_honours_its_filter() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");

    let mut sub = log
        .subscribe(Position::BEGINNING, Filter::kinds(["wanted"]))
        .unwrap();

    s.append(event("ignored")).await.unwrap();
    s.append(event("wanted")).await.unwrap();

    let got = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("the wanted event arrives")
        .unwrap()
        .unwrap();
    assert_eq!(got.kind(), "wanted");
    assert_eq!(got.position, Position::new(2));
}

#[tokio::test]
async fn a_decision_reads_the_stream_under_the_lock_and_may_refuse() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut ledger = log.stream_handle("ledger");

    ledger
        .append(event_with("granted", json!({ "amount": 10 })))
        .await
        .unwrap();

    // Spend 4 of 10: allowed.
    let spent = ledger
        .append_if(
            Some(&["granted", "spent"]),
            Box::new(|seen| {
                let balance: i64 = seen
                    .iter()
                    .map(|e| {
                        let amount = e["data"]["amount"].as_i64().unwrap_or(0);
                        if e.kind() == "granted" {
                            amount
                        } else {
                            -amount
                        }
                    })
                    .sum();
                (balance >= 4).then(|| event_with("spent", json!({ "amount": 4 })))
            }),
        )
        .await
        .unwrap();
    assert!(spent.is_some());

    // Spend 100 of the remaining 6: refused, and nothing is written.
    let refused = ledger
        .append_if(
            Some(&["granted", "spent"]),
            Box::new(|seen| {
                let balance: i64 = seen
                    .iter()
                    .map(|e| {
                        let amount = e["data"]["amount"].as_i64().unwrap_or(0);
                        if e.kind() == "granted" {
                            amount
                        } else {
                            -amount
                        }
                    })
                    .sum();
                (balance >= 100).then(|| event_with("spent", json!({ "amount": 100 })))
            }),
        )
        .await
        .unwrap();
    assert!(refused.is_none());
    assert_eq!(ledger.len().await.unwrap(), 2);
}

#[tokio::test]
async fn a_batch_lands_with_contiguous_sequence_numbers() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    let committed = s
        .append_many(vec![event("a"), event("b"), event("c")])
        .await
        .unwrap();
    assert_eq!(
        committed.iter().map(|c| c.seq).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(s.head().await.unwrap(), Some(3));
}

#[tokio::test]
async fn a_batch_that_fails_validation_writes_none_of_itself() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    let bad = json!({ "kind": "ok", "surprise": 1 })
        .as_object()
        .unwrap()
        .clone();
    assert!(s.append_many(vec![event("ok"), bad]).await.is_err());
    assert_eq!(s.len().await.unwrap(), 0);
}

#[tokio::test]
async fn checkpoints_default_to_the_beginning_and_round_trip() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    assert_eq!(
        log.checkpoint_load("projector").await.unwrap(),
        Position::BEGINNING
    );

    log.checkpoint_save("projector", Position::new(7))
        .await
        .unwrap();
    assert_eq!(
        log.checkpoint_load("projector").await.unwrap(),
        Position::new(7)
    );

    log.checkpoint_save("projector", Position::new(9))
        .await
        .unwrap();
    assert_eq!(
        log.checkpoint_load("projector").await.unwrap(),
        Position::new(9)
    );
}

#[tokio::test]
async fn a_write_through_sql_is_refused() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let s = log.stream_handle("s");
    let error = s.query("DELETE FROM events", Vec::new()).await.unwrap_err();
    assert!(
        matches!(error, eventsdb_core::Error::Unsupported(_)),
        "got {error}"
    );
}

#[tokio::test]
async fn sql_reads_the_stored_shape() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event_with("noted", json!({ "n": 3 })))
        .await
        .unwrap();

    let rows = s
        .query(
            "SELECT kind, json_extract(data, '$.n') AS n FROM events WHERE stream = ?1",
            vec![json!("s")],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["kind"], json!("noted"));
    assert_eq!(rows[0]["n"], json!(3));
}

#[tokio::test]
async fn reopening_a_file_keeps_the_log_and_re_runs_no_step() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");

    {
        let log = SqliteEventLog::open(&path).await.unwrap();
        let mut s = log.stream_handle("s");
        s.append(event("first")).await.unwrap();
        log.close().await.unwrap();
    }

    let log = SqliteEventLog::open(&path).await.unwrap();
    let all = log
        .read_all(Position::BEGINNING, &Filter::all(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].kind(), "first");

    // The next position continues rather than restarting.
    let mut s = log.stream_handle("s");
    let committed = s.append(event("second")).await.unwrap();
    assert_eq!(committed.position, Some(Position::new(2)));
    assert_eq!(committed.seq, 2);
    log.close().await.unwrap();
}

#[tokio::test]
async fn two_handles_on_one_database_report_the_same_identity() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let a = log.stream_handle("a");
    let b = log.stream_handle("b");
    assert_eq!(a.database(), b.database());
    assert_eq!(a.database(), Some(log.database()));

    let other = SqliteEventLog::open_in_memory().await.unwrap();
    assert_ne!(a.database(), other.stream_handle("a").database());
}
