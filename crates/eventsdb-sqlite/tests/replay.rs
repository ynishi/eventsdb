//! `replay`: the whole selected range as a stream that ends.
//!
//! The claims under test are the ones the shape was chosen for. A replay is
//! `read_all` in a loop, so it crosses page boundaries without duplicating or
//! skipping and ends at the first short page; and it holds nothing between
//! pages, so a replay left half-consumed does not keep a reader from anyone
//! else — which is checked with a log that has exactly one reader to keep.

use std::time::Duration;

use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{OpenOptions, SqliteEventLog};
use futures_util::StreamExt;
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind })
        .as_object()
        .expect("literal is an object")
        .clone()
}

/// More than two pages, so the loop has to turn the corner twice.
const MANY: usize = 600;

async fn seeded(n: usize) -> SqliteEventLog {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    let batch: Vec<_> = (0..n)
        .map(|i| event(if i % 3 == 0 { "wanted" } else { "other" }))
        .collect();
    s.append_many(batch).await.unwrap();
    log
}

/// The round trip this exists for: everything, once, in position order, and
/// then the stream ends rather than waiting.
#[tokio::test]
async fn a_replay_yields_every_event_in_position_order_and_ends() {
    let log = seeded(MANY).await;

    let got: Vec<u64> = log
        .replay(Position::BEGINNING, Filter::all())
        .map(|r| r.unwrap().position.get())
        .collect()
        .await;

    let expected: Vec<u64> = (1..=MANY as u64).collect();
    assert_eq!(got, expected, "dense, ordered, no page boundary visible");
}

/// The end is observable: `collect` above proved it terminates, this proves
/// it does so promptly rather than after a poll interval.
#[tokio::test]
async fn a_replay_ends_without_waiting_for_a_poll() {
    let log = seeded(3).await;
    let mut replay = log.replay(Position::BEGINNING, Filter::all());

    for _ in 0..3 {
        replay.next().await.unwrap().unwrap();
    }
    let end = tokio::time::timeout(Duration::from_millis(50), replay.next())
        .await
        .expect("the stream ends at once, not after the poll interval");
    assert!(end.is_none());
}

/// A filter that leaves the final page short, and one that selects nothing,
/// both end the same way.
#[tokio::test]
async fn a_replay_honours_the_filter_and_ends_on_an_empty_range() {
    let log = seeded(MANY).await;

    let wanted: Vec<u64> = log
        .replay(Position::BEGINNING, Filter::kinds(["wanted"]))
        .map(|r| r.unwrap().position.get())
        .collect()
        .await;
    let expected: Vec<u64> = (1..=MANY as u64).filter(|p| (p - 1) % 3 == 0).collect();
    assert_eq!(wanted, expected);

    let none: Vec<_> = log
        .replay(Position::BEGINNING, Filter::kinds(["absent"]))
        .collect()
        .await;
    assert!(none.is_empty());
}

/// Exclusive on `from`, the same as `read_all`: feeding back the last
/// position handled continues without repeating it.
#[tokio::test]
async fn a_replay_is_exclusive_on_from() {
    let log = seeded(10).await;

    let mut first = log.replay(Position::BEGINNING, Filter::all());
    let stopped_at = {
        first.next().await.unwrap().unwrap();
        first.next().await.unwrap().unwrap().position
    };
    drop(first);

    let rest: Vec<u64> = log
        .replay(stopped_at, Filter::all())
        .map(|r| r.unwrap().position.get())
        .collect()
        .await;
    assert_eq!(rest, (3..=10).collect::<Vec<u64>>());
}

/// The property the shape is chosen for. With exactly one reader, a replay
/// that has been started and left mid-way must not be holding it: another
/// read on the same log completes, and so does a write.
#[tokio::test]
async fn a_half_consumed_replay_holds_no_reader() {
    let log = SqliteEventLog::open_in_memory_with(OpenOptions {
        readers: 1,
        ..OpenOptions::default()
    })
    .await
    .unwrap();
    let mut s = log.stream_handle("s");
    s.append_many((0..MANY).map(|_| event("x")).collect())
        .await
        .unwrap();

    let mut paused = log.replay(Position::BEGINNING, Filter::all());
    paused.next().await.unwrap().unwrap();
    // `paused` is alive, one page in, and will not be polled again until
    // the end of this test.

    let other = tokio::time::timeout(
        Duration::from_secs(2),
        log.read_all(Position::BEGINNING, &Filter::all(), 5),
    )
    .await
    .expect("a read does not wait behind a paused replay")
    .unwrap();
    assert_eq!(other.len(), 5);

    let committed = tokio::time::timeout(Duration::from_secs(2), s.append(event("y")))
        .await
        .expect("a write does not wait behind a paused replay")
        .unwrap();
    assert_eq!(committed.position, Some(Position::new(MANY as u64 + 1)));

    drop(paused);
}

/// A replay reports the log as it was when its last page was taken. An
/// event committed after that page is not in it — that is `subscribe`'s
/// job, and the seam between the two is stated rather than blurred.
#[tokio::test]
async fn a_replay_does_not_include_what_lands_after_its_last_page() {
    let log = seeded(3).await;
    let mut s = log.stream_handle("s");

    let mut replay = log.replay(Position::BEGINNING, Filter::all());
    replay.next().await.unwrap().unwrap();
    // The first page (all three) has been read; this lands after it.
    s.append(event("late")).await.unwrap();

    let rest: Vec<u64> = replay.map(|r| r.unwrap().position.get()).collect().await;
    assert_eq!(rest, vec![2, 3], "the late event is not in this replay");

    let tail = log
        .read_all(Position::new(3), &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(tail.len(), 1, "and is where the next page would find it");
}
