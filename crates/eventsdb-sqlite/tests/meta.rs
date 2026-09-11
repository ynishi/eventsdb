//! The `meta` read axis.
//!
//! `meta` has always had a job on the way in — scalars a reader filters by —
//! and until this file nothing on the way out honoured it. What is tested
//! here is that a `Filter` on a `meta` key selects on the stored value, that
//! absence and a different scalar type both fail to match, that the axis
//! combines with the other two by AND on every read path, and that the index
//! the store creates is spelled exactly as the predicate is — which is the
//! condition for SQLite to use it at all.

use eventsdb_core::error::Error;
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::SqliteEventLog;
use futures_util::StreamExt;
use serde_json::{json, Map, Value};

fn event(kind: &str, meta: Value) -> Map<String, Value> {
    json!({ "kind": kind, "meta": meta })
        .as_object()
        .expect("literal is an object")
        .clone()
}

/// Five events over three streams, so every axis has something to cut.
///
/// ```text
///   position  stream      kind    meta
///   1         orders/17   placed  tenant=a
///   2         orders/17   paid    tenant=a
///   3         orders/18   placed  tenant=b
///   4         orders/17   closed  tenant=a, closed=true
///   5         orders/19   placed  tenant=a, total=40
/// ```
async fn seeded() -> SqliteEventLog {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut o17 = log.stream_handle("orders/17");
    let mut o18 = log.stream_handle("orders/18");
    let mut o19 = log.stream_handle("orders/19");

    o17.append(event("placed", json!({ "tenant": "a" })))
        .await
        .unwrap();
    o17.append(event("paid", json!({ "tenant": "a" })))
        .await
        .unwrap();
    o18.append(event("placed", json!({ "tenant": "b" })))
        .await
        .unwrap();
    o17.append(event("closed", json!({ "tenant": "a", "closed": true })))
        .await
        .unwrap();
    o19.append(event("placed", json!({ "tenant": "a", "total": 40 })))
        .await
        .unwrap();
    log
}

async fn positions(log: &SqliteEventLog, filter: &Filter) -> Vec<u64> {
    log.read_all(Position::BEGINNING, filter, 100)
        .await
        .unwrap()
        .iter()
        .map(|r| r.position.get())
        .collect()
}

/// The round trip this exists for: a value written under `meta` comes back
/// out through a read that names it, across streams, in position order.
#[tokio::test]
async fn a_meta_key_selects_across_streams_in_position_order() {
    let log = seeded().await;
    assert_eq!(
        positions(&log, &Filter::all().meta("tenant", "a")).await,
        vec![1, 2, 4, 5]
    );
    assert_eq!(
        positions(&log, &Filter::all().meta("tenant", "b")).await,
        vec![3]
    );
}

/// Absence is not a value. An event without the key is not "the key is
/// null"; it is out of the answer, and nothing a caller can write selects it.
#[tokio::test]
async fn a_key_the_event_does_not_carry_matches_nothing() {
    let log = seeded().await;
    assert_eq!(
        positions(&log, &Filter::all().meta("closed", true)).await,
        vec![4]
    );
    assert_eq!(
        positions(&log, &Filter::all().meta("closed", false)).await,
        Vec::<u64>::new()
    );
}

/// The scalar types stay what they are. `json_extract` returns a stored
/// string as `TEXT` and a stored number as `INTEGER`, and an expression has
/// no column affinity to coerce one into the other, so `"40"` and `40` are
/// two different values — as they were on the way in.
#[tokio::test]
async fn a_number_and_the_same_digits_as_a_string_are_different_values() {
    let log = seeded().await;
    assert_eq!(
        positions(&log, &Filter::all().meta("total", 40)).await,
        vec![5]
    );
    assert_eq!(
        positions(&log, &Filter::all().meta("total", "40")).await,
        Vec::<u64>::new()
    );
}

/// Every pair must hold, and the axis ANDs with `kinds` and `streams`.
#[tokio::test]
async fn pairs_and_axes_combine_by_and() {
    let log = seeded().await;

    assert_eq!(
        positions(
            &log,
            &Filter::all().meta("tenant", "a").meta("closed", true)
        )
        .await,
        vec![4]
    );
    assert_eq!(
        positions(&log, &Filter::kinds(["placed"]).meta("tenant", "a")).await,
        vec![1, 5]
    );
    assert_eq!(
        positions(&log, &Filter::all().stream("orders/17").meta("tenant", "a")).await,
        vec![1, 2, 4]
    );
    // Two pairs on one key are "both", and no event holds both.
    assert_eq!(
        positions(&log, &Filter::all().meta("tenant", "a").meta("tenant", "b")).await,
        Vec::<u64>::new()
    );
}

/// A `null` could only ever select nothing, and a structured value has
/// nothing under `meta` to be compared with. Both are refused where the
/// filter meets the backend, with the reason in the message.
#[tokio::test]
async fn a_null_or_structured_value_is_refused_not_matched_against_nothing() {
    let log = seeded().await;

    let null = Filter::all().meta("tenant", Value::Null);
    let err = log
        .read_all(Position::BEGINNING, &null, 100)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Validation(_)), "{err}");
    assert!(err.to_string().contains("cannot match `null`"), "{err}");

    let object = Filter::all().meta("tenant", json!({ "id": "a" }));
    let err = log
        .read_all(Position::BEGINNING, &object, 100)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Validation(_)), "{err}");
    assert!(err.to_string().contains("found an object"), "{err}");
}

/// The live tail is the same range read resumed, so it narrows the same way.
#[tokio::test]
async fn a_subscription_honours_the_meta_axis_live() {
    let log = seeded().await;
    let mut sub = log
        .subscribe(Position::BEGINNING, Filter::all().meta("tenant", "b"))
        .unwrap();

    assert_eq!(sub.next().await.unwrap().unwrap().position.get(), 3);

    let mut o18 = log.stream_handle("orders/18");
    let mut o17 = log.stream_handle("orders/17");
    o17.append(event("noted", json!({ "tenant": "a" })))
        .await
        .unwrap();
    let wanted = o18
        .append(event("paid", json!({ "tenant": "b" })))
        .await
        .unwrap();

    let next = tokio::time::timeout(std::time::Duration::from_secs(2), sub.next())
        .await
        .expect("the tenant-b append wakes the subscriber")
        .unwrap()
        .unwrap();
    assert_eq!(next.position, wanted.position.unwrap());
    assert_eq!(next.event["kind"], json!("paid"));
}

/// Export is the read that stops before the upcaster, on the same SQL, so a
/// filtered export carries exactly the filtered rows.
#[tokio::test]
async fn export_carries_only_what_the_meta_axis_selects() {
    let log = seeded().await;
    let batch = log
        .export(Position::BEGINNING, &Filter::all().meta("tenant", "a"), 100)
        .await
        .unwrap();
    let positions: Vec<u64> = batch.iter().map(|e| e.position.get()).collect();
    assert_eq!(positions, vec![1, 2, 4, 5]);
}

/// The index is the predicate's expression, character for character. That
/// is not a style preference: SQLite matches a query to an expression index
/// by the text of the expression, so a predicate spelled any other way would
/// leave the index unused and the read a scan.
#[tokio::test]
async fn index_meta_creates_the_predicate_expression_and_is_idempotent() {
    let log = seeded().await;
    log.index_meta("tenant").await.unwrap();
    log.index_meta("tenant").await.unwrap();

    let rows = log
        .query(
            "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND name = ?1",
            vec![json!("events_meta_tenant")],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "one index, created once");
    let sql = rows[0]["sql"].as_str().unwrap();
    assert!(
        sql.contains(r#"json_extract(meta, '$."tenant"')"#),
        "the index is on the exact expression the filter uses: {sql}"
    );
    assert!(
        sql.contains(", position)"),
        "position follows, so ORDER BY position is served too: {sql}"
    );

    // And the read that would use it still answers the same.
    assert_eq!(
        positions(&log, &Filter::all().meta("tenant", "a")).await,
        vec![1, 2, 4, 5]
    );
}

/// A key with punctuation is a key, not a path: `$."order.id"` names one
/// key, and the index for it gets a name that cannot collide with a
/// sibling's.
#[tokio::test]
async fn a_key_with_punctuation_names_a_key_not_a_path() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event(
        "x",
        json!({ "order.id": "o-1", "order": "not this" }),
    ))
    .await
    .unwrap();
    s.append(event("x", json!({ "order": "o-1" })))
        .await
        .unwrap();

    assert_eq!(
        positions(&log, &Filter::all().meta("order.id", "o-1")).await,
        vec![1]
    );

    log.index_meta("order.id").await.unwrap();
    log.index_meta("order_id").await.unwrap();
    let rows = log
        .query(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'events_meta_%' \
             ORDER BY name",
            Vec::<Value>::new(),
        )
        .await
        .unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert_eq!(names.len(), 2, "two keys, two indices: {names:?}");
    assert!(names.contains(&"events_meta_order_id"));

    let err = log.index_meta(r#"say "hi""#).await.unwrap_err();
    assert!(matches!(err, Error::Validation(_)), "{err}");
}
