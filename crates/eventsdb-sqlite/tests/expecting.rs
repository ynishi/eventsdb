//! Expected-version appends, and author-owned schema versions.
//!
//! Both came from prior art rather than from this design: nine of ten surveyed
//! event stores offer expected-version, and none of them lets the *store*
//! choose an event's schema version.

use eventsdb_core::error::Error;
use eventsdb_core::store::Expected;
use eventsdb_core::upcast::{UpcastChain, Upcaster};
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{Guard, OpenOptions, Plan, SqliteEventLog};
use serde_json::{json, Map, Value};
use std::sync::Arc;

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

fn versioned(kind: &str, version: u64) -> Map<String, Value> {
    json!({ "kind": kind, "_schema_version": version })
        .as_object()
        .unwrap()
        .clone()
}

/// The round trip this exists for: read at a head, leave, come back and apply
/// only if nothing moved.
#[tokio::test]
async fn an_append_lands_when_the_head_is_where_the_caller_left_it() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("order-1");

    // A new stream: the claim is "nothing has ever been appended here".
    let first = s
        .append_expecting(Expected::Unwritten, event("placed"))
        .await
        .unwrap();
    assert_eq!(first.seq, 1);

    // And then from the head the caller now knows.
    let second = s
        .append_expecting(Expected::Seq(1), event("paid"))
        .await
        .unwrap();
    assert_eq!(second.seq, 2);
}

#[tokio::test]
async fn an_append_is_refused_when_the_stream_moved_and_says_where_it_moved_to() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("order-1");
    s.append(event("placed")).await.unwrap();
    s.append(event("paid")).await.unwrap();

    // A caller that read at seq 1 and came back late.
    let error = s
        .append_expecting(Expected::Seq(1), event("cancelled"))
        .await
        .unwrap_err();

    match error {
        Error::HeadMismatch { expected, actual } => {
            assert_eq!(expected, Expected::Seq(1));
            assert_eq!(
                actual,
                Expected::Seq(2),
                "the caller needs where to read up to, not just that it lost"
            );
        }
        other => panic!("expected HeadMismatch, got {other}"),
    }

    // Refused means nothing happened, and no sequence number was consumed.
    assert_eq!(s.len().await.unwrap(), 2);
    assert_eq!(s.append(event("later")).await.unwrap().seq, 3);
}

/// A conflict is not contention: repeating it unchanged fails the same way, so
/// nothing may retry it automatically.
#[tokio::test]
async fn a_head_mismatch_is_not_busy() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("a")).await.unwrap();

    let error = s
        .append_expecting(Expected::Unwritten, event("b"))
        .await
        .unwrap_err();
    assert!(!error.is_busy(), "a stale head is not contention");
    assert!(!error.is_timeout());
}

/// The trap the implementation is built around. Retention can empty a stream
/// whose counter has moved on, and a check against `MAX(seq)` would then let
/// "nothing has ever been appended here" pass on a stream that held events.
#[tokio::test]
async fn unwritten_means_never_written_not_currently_empty() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("session-1");
    for _ in 0..3 {
        s.append(event("a")).await.unwrap();
    }

    log.retain(Plan::Streams(vec!["session-1".to_string()]), Guard::Force)
        .await
        .unwrap();

    // The stream reads empty...
    assert_eq!(s.len().await.unwrap(), 0);
    assert_eq!(s.head().await.unwrap(), None);

    // ...but it is not unwritten, and the check knows the difference.
    let error = s
        .append_expecting(Expected::Unwritten, event("forged"))
        .await
        .unwrap_err();
    match error {
        Error::HeadMismatch { actual, .. } => assert_eq!(actual, Expected::Seq(3)),
        other => panic!("expected HeadMismatch, got {other}"),
    }

    // The true head is what the counter says, so this one lands.
    assert_eq!(
        s.append_expecting(Expected::Seq(3), event("continued"))
            .await
            .unwrap()
            .seq,
        4
    );
}

/// The in-memory backend declines rather than imitating the check without a
/// transaction to make it one write.
#[tokio::test]
async fn a_backend_that_cannot_make_it_one_write_declines() {
    use eventsdb_core::MemEventStore;

    let mut store = MemEventStore::new("s");
    let error = store
        .append_expecting(Expected::Unwritten, event("a"))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(_)), "got {error}");
}

/// `append_at` is a first-class writer, not something reachable only by
/// entering the escape hatch — the README said it was one of three and it was
/// not.
#[tokio::test]
async fn a_backfill_does_not_require_entering_the_hatch() {
    const AN_OLD_TIME: u64 = 1_600_000_000_000;

    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");

    let committed = s.append_at(AN_OLD_TIME, event("noted")).await.unwrap();
    assert_eq!(committed.epoch_ms, AN_OLD_TIME);
    assert_eq!(committed.seq, 1);
    assert_eq!(committed.position, Some(Position::new(1)));

    // And it shares the counter with every other writer.
    assert_eq!(s.append(event("now")).await.unwrap().seq, 2);
}

#[tokio::test]
async fn a_backend_that_cannot_backfill_declines() {
    use eventsdb_core::MemEventStore;

    let mut store = MemEventStore::new("s");
    let error = store.append_at(1, event("a")).await.unwrap_err();
    assert!(matches!(error, Error::Unsupported(_)), "got {error}");
}

// --- author-owned schema version ---------------------------------------

/// The store carries the number; it does not choose it.
#[tokio::test]
async fn an_author_may_choose_the_schema_version() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");

    s.append(versioned("noted", 7)).await.unwrap();
    s.append(event("noted")).await.unwrap(); // said nothing

    let read = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(read[0].event["_schema_version"], json!(7));
    assert_eq!(
        read[1].event["_schema_version"],
        json!(eventsdb_core::event::DEFAULT_SCHEMA_VERSION),
        "an author who did not say gets the default, not a refusal"
    );
}

#[tokio::test]
async fn a_schema_version_that_is_not_a_number_is_refused() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");

    let bad = json!({ "kind": "noted", "_schema_version": "seven" })
        .as_object()
        .unwrap()
        .clone();
    let error = s.append(bad).await.unwrap_err();
    assert!(matches!(error, Error::Validation(_)), "got {error}");
}

/// An upcaster selects on `(kind, version)`, so one author's bump does not
/// move anybody else's events.
#[tokio::test]
async fn an_upcaster_selects_on_kind_and_version_together() {
    struct OrderV1ToV2;

    impl Upcaster for OrderV1ToV2 {
        fn upcast(&self, mut event: Value) -> Value {
            if event["kind"] == json!("order_placed") && event["_schema_version"] == json!(1) {
                event["data"]["total"] = json!(0);
                event["_schema_version"] = json!(2);
            }
            event
        }
    }

    let chain: UpcastChain = vec![Arc::new(OrderV1ToV2)];
    let log = SqliteEventLog::open_in_memory_with(OpenOptions {
        upcasters: chain,
        ..OpenOptions::default()
    })
    .await
    .unwrap();

    let mut s = log.stream_handle("s");
    s.append(versioned("order_placed", 1)).await.unwrap();
    // Another author's kind, at their own version 1. Untouched.
    s.append(versioned("note_written", 1)).await.unwrap();
    // And the same kind already at 2.
    s.append(versioned("order_placed", 2)).await.unwrap();

    let read = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();

    assert_eq!(read[0].event["_schema_version"], json!(2));
    assert_eq!(read[0].event["data"]["total"], json!(0), "moved forward");

    assert_eq!(
        read[1].event["_schema_version"],
        json!(1),
        "a different kind at version 1 is not this upcaster's business"
    );
    assert!(read[1].event["data"].get("total").is_none());

    assert_eq!(
        read[2].event["data"].get("total"),
        None,
        "already at 2, so the step does not run again"
    );
}

/// A chosen version survives a transfer, which is what makes the upcaster
/// still match on the far side.
#[tokio::test]
async fn a_chosen_version_survives_export_and_import() {
    let source = SqliteEventLog::open_in_memory().await.unwrap();
    source
        .stream_handle("s")
        .append(versioned("noted", 9))
        .await
        .unwrap();

    let exported = source
        .export(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(exported[0].event["_schema_version"], json!(9));

    let target = SqliteEventLog::open_in_memory().await.unwrap();
    target.import(exported).await.unwrap();

    let read = target
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(read[0].event["_schema_version"], json!(9));
}
