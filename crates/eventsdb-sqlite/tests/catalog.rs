//! The five listings: what is in the log, rather than what is at a name the
//! caller already holds.
//!
//! The property the issue names, and the one most of this file is about:
//! retention leaves the five tables in different states, and each listing
//! reports its own table rather than a smoothed-over average of them. A
//! stream the retention emptied still lists, because the counter is the truth
//! of its existence; a kind whose every event went does not, because kinds
//! have no counter. Everything else here is the paging shape, which is
//! `read_all`'s with a different key.

use std::collections::BTreeSet;

use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{
    ExportRecord, Guard, JsonLinesSink, Plan, RetentionEntry, SqliteEventLog, StreamInfo,
};
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind })
        .as_object()
        .expect("literal is an object")
        .clone()
}

/// Append `n` events of `kind` to `stream`.
async fn write(log: &SqliteEventLog, stream: &str, kind: &str, n: usize) {
    let mut handle = log.stream_handle(stream);
    handle
        .append_many((0..n).map(|_| event(kind)).collect())
        .await
        .unwrap();
}

/// Three streams, written out of alphabetical order so the ordering under
/// test is the listing's and not the order of the appends.
async fn seeded() -> SqliteEventLog {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    write(&log, "c", "tick", 2).await;
    write(&log, "a", "tick", 3).await;
    write(&log, "b", "tick", 1).await;
    log
}

fn names(streams: &[StreamInfo]) -> Vec<String> {
    streams.iter().map(|s| s.stream.clone()).collect()
}

/// The distinct stream names a read can see, which is the set `streams`
/// answers *minus* whatever retention emptied.
async fn readable_streams(log: &SqliteEventLog, filter: &Filter) -> BTreeSet<String> {
    log.read_all(Position::BEGINNING, filter, usize::MAX)
        .await
        .unwrap()
        .into_iter()
        .map(|recorded| recorded.stream)
        .collect()
}

// ---------------------------------------------------------------- streams

/// Every stream, its head `seq`, ordered by name — and the cursor is
/// exclusive, so the last name of a page is the next call's `after`.
#[tokio::test]
async fn streams_lists_every_stream_with_its_head_seq_and_pages_by_name() {
    let log = seeded().await;

    let all = log.streams(None, None, 100).await.unwrap();
    assert_eq!(
        all,
        vec![
            StreamInfo {
                stream: "a".to_string(),
                head_seq: 3
            },
            StreamInfo {
                stream: "b".to_string(),
                head_seq: 1
            },
            StreamInfo {
                stream: "c".to_string(),
                head_seq: 2
            },
        ],
        "ordered by name, with `next_seq - 1` as the head"
    );

    // Two at a time, feeding the last name back in.
    let first = log.streams(None, None, 2).await.unwrap();
    assert_eq!(names(&first), vec!["a", "b"]);
    let second = log.streams(Some(&first[1].stream), None, 2).await.unwrap();
    assert_eq!(names(&second), vec!["c"], "exclusive on `after`");

    // A page boundary landing exactly on the last row: the page comes back
    // full, and the next one is empty rather than repeating anything.
    let full = log.streams(None, None, 3).await.unwrap();
    assert_eq!(full.len(), 3);
    let past_the_end = log.streams(Some(&full[2].stream), None, 3).await.unwrap();
    assert!(past_the_end.is_empty(), "{past_the_end:?}");
}

/// The property this listing exists for. Retention removes events and never
/// touches `stream_seq`, so the emptied stream is still there with the head
/// it reached — which is what a caller about to reuse the name needs, and
/// what no read can tell it.
#[tokio::test]
async fn a_stream_retention_emptied_still_lists_with_its_counter_intact() {
    let log = seeded().await;

    let report = log
        .retain(Plan::Streams(vec!["a".to_string()]), Guard::Force)
        .await
        .unwrap();
    assert_eq!(report.removed, 3);

    let all = log.streams(None, None, 100).await.unwrap();
    assert_eq!(names(&all), vec!["a", "b", "c"], "`a` is still a stream");
    assert_eq!(
        all[0].head_seq, 3,
        "and its counter stands where the removed events left it"
    );

    // Nothing under it: the counter outlives the events, and says so.
    let under_a = log
        .read_all(Position::BEGINNING, &Filter::all().stream("a"), 100)
        .await
        .unwrap();
    assert!(under_a.is_empty(), "{under_a:?}");

    // And the next append carries on from the head rather than restarting.
    let mut a = log.stream_handle("a");
    assert_eq!(a.append(event("tick")).await.unwrap().seq, 4);
}

/// `head_seq` is `next_seq - 1` exactly, and the bottom of that range is
/// not reachable from here: the counter is written only after an append, so
/// the lowest stored `next_seq` is 2, and writing one by hand is refused —
/// inside the hatch's own transaction as everywhere else. So `head_seq` 0
/// means a `sqlite3` session on the file, and this asserts the refusal that
/// makes it mean that.
#[tokio::test]
async fn the_counter_cannot_be_written_by_hand_so_head_seq_never_falls() {
    let log = seeded().await;
    let error = log
        .with_transaction(|tx| {
            tx.execute("UPDATE stream_seq SET next_seq = 1 WHERE stream = 'b'", [])
                .map(|_| ())
                .map_err(|error| eventsdb_core::error::Error::storage(error.to_string()))
        })
        .await
        .unwrap_err();
    assert!(
        matches!(error, eventsdb_core::error::Error::Unsupported(_)),
        "got {error}"
    );

    let all = log.streams(None, None, 100).await.unwrap();
    assert_eq!(all[1].stream, "b");
    assert_eq!(all[1].head_seq, 1, "the counter the appends left");
}

/// The prefix answers the same set a read under
/// `Filter::all().stream_prefix(..)` sees — until retention empties one of
/// them, which the read cannot report and the listing can. That difference
/// is the point, so it is asserted rather than papered over.
#[tokio::test]
async fn a_prefix_answers_the_reads_set_plus_the_streams_it_emptied() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    write(&log, "session-1", "tick", 2).await;
    write(&log, "session-2", "tick", 1).await;
    // Just outside the range: `sessions` sorts above the bound `session.`.
    write(&log, "sessions", "tick", 1).await;
    write(&log, "other", "tick", 1).await;

    let filter = Filter::all().stream_prefix("session-");
    let listed: BTreeSet<String> = log
        .streams(None, Some("session-"), 100)
        .await
        .unwrap()
        .into_iter()
        .map(|info| info.stream)
        .collect();
    assert_eq!(
        listed,
        readable_streams(&log, &filter).await,
        "the same range the read walks"
    );
    assert_eq!(
        listed,
        BTreeSet::from(["session-1".to_string(), "session-2".to_string()])
    );

    // An empty prefix is every stream, as the filter axis documents.
    assert_eq!(
        log.streams(None, Some(""), 100).await.unwrap(),
        log.streams(None, None, 100).await.unwrap()
    );

    log.retain(Plan::Streams(vec!["session-2".to_string()]), Guard::Force)
        .await
        .unwrap();

    let listed: Vec<String> = names(&log.streams(None, Some("session-"), 100).await.unwrap());
    assert_eq!(
        listed,
        vec!["session-1", "session-2"],
        "the counter is still there"
    );
    assert_eq!(
        readable_streams(&log, &filter).await,
        BTreeSet::from(["session-1".to_string()]),
        "and the read cannot see the emptied one at all"
    );

    // The cursor pages within the prefix, not across the whole table.
    let page = log.streams(None, Some("session-"), 1).await.unwrap();
    assert_eq!(names(&page), vec!["session-1"]);
    let page = log
        .streams(Some("session-1"), Some("session-"), 10)
        .await
        .unwrap();
    assert_eq!(names(&page), vec!["session-2"]);
}

// ------------------------------------------------------------------ kinds

/// Distinct kinds in order, paged — and then the asymmetry with `streams`:
/// a kind whose every event has been removed does not list, because nothing
/// counts kinds, while one with a single surviving event lists like any
/// other.
#[tokio::test]
async fn kinds_lists_what_is_there_and_a_fully_removed_kind_is_not() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    write(&log, "x", "zeta", 1).await;
    write(&log, "x", "alpha", 2).await;
    write(&log, "y", "zeta", 1).await;

    assert_eq!(log.kinds(None, 100).await.unwrap(), vec!["alpha", "zeta"]);

    let page = log.kinds(None, 1).await.unwrap();
    assert_eq!(page, vec!["alpha"]);
    assert_eq!(log.kinds(Some("alpha"), 10).await.unwrap(), vec!["zeta"]);
    assert!(log.kinds(Some("zeta"), 10).await.unwrap().is_empty());

    // `x` held every `alpha` and one of the two `zeta`s.
    log.retain(Plan::Streams(vec!["x".to_string()]), Guard::Force)
        .await
        .unwrap();

    assert_eq!(
        log.kinds(None, 100).await.unwrap(),
        vec!["zeta"],
        "`alpha` has no events left and no counter to remember it by"
    );
    assert_eq!(
        log.streams(None, None, 100).await.unwrap().len(),
        2,
        "while both streams are still listed"
    );
}

// ------------------------------------------------------------ checkpoints

/// What `checkpoint_save` wrote, ordered by consumer and paged — and the
/// blind spot `Guard::RegisteredConsumers` documents: a consumer that has
/// never reported is not here, because nothing knows it exists.
#[tokio::test]
async fn checkpoints_lists_consumers_that_checked_in_and_only_those() {
    let log = seeded().await;
    log.checkpoint_save("beta", Position::new(5)).await.unwrap();
    log.checkpoint_save("alpha", Position::new(2))
        .await
        .unwrap();

    let all = log.checkpoints(None, 100).await.unwrap();
    assert_eq!(
        all.iter().map(|c| c.consumer.as_str()).collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );
    assert_eq!(all[0].position, Position::new(2));
    assert_eq!(all[1].position, Position::new(5));
    assert!(all.iter().all(|c| c.updated_ms > 0), "{all:?}");

    // A later save moves the row rather than adding one.
    log.checkpoint_save("alpha", Position::new(4))
        .await
        .unwrap();
    let all = log.checkpoints(None, 100).await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].position, Position::new(4));

    // Paged, exclusive on the name.
    let first = log.checkpoints(None, 1).await.unwrap();
    assert_eq!(first[0].consumer, "alpha");
    let second = log.checkpoints(Some("alpha"), 100).await.unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].consumer, "beta");

    // Never checked in: readable as `BEGINNING`, invisible to the listing.
    assert_eq!(
        log.checkpoint_load("gamma").await.unwrap(),
        Position::BEGINNING
    );
    assert!(!all.iter().any(|c| c.consumer == "gamma"), "{all:?}");
}

// -------------------------------------------------------- retention ledger

/// One row per application, in the order they happened, carrying what the
/// `Report` said at the time — and an archive's row reads
/// `archive-then-remove: <plan>` verbatim, since the store does not
/// interpret the description it stored.
#[tokio::test]
async fn the_ledger_round_trips_what_retain_returned() {
    let log = seeded().await;

    let first = log
        .retain(Plan::Before(Position::new(2)), Guard::Force)
        .await
        .unwrap();
    let second = log
        .retain(Plan::Streams(vec!["b".to_string()]), Guard::Force)
        .await
        .unwrap();

    let rows = log.retention_ledger(None, 100).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].id < rows[1].id, "ordered by id, oldest first");
    assert!(rows.iter().all(|row| row.applied_ms > 0), "{rows:?}");

    let matches = |row: &RetentionEntry, report: &eventsdb_sqlite::Report, plan: &str| {
        row.removed_count == report.removed
            && Some(row.highest_removed) == report.highest_removed
            && row.plan == plan
    };
    assert!(matches(&rows[0], &first, "before:2"), "{rows:?}");
    assert!(matches(&rows[1], &second, "streams:1"), "{rows:?}");

    // A plan that matches nothing writes no row at all.
    let nothing = log
        .retain(Plan::Streams(vec!["b".to_string()]), Guard::Force)
        .await
        .unwrap();
    assert_eq!(nothing.removed, 0);
    assert_eq!(log.retention_ledger(None, 100).await.unwrap().len(), 2);

    // The archive names itself, and the listing hands the string back as it
    // is stored.
    let mut sink = JsonLinesSink::new(Vec::new());
    let archived = log
        .archive_then_retain(Plan::Before(Position::new(5)), &mut sink, 2)
        .await
        .unwrap();
    assert!(archived.removed > 0);

    let rows = log.retention_ledger(None, 100).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].plan, "archive-then-remove: before:5");
    assert!(matches(
        &rows[2],
        &archived,
        "archive-then-remove: before:5"
    ));

    // Paged, exclusive on the id.
    let rest = log.retention_ledger(Some(rows[0].id), 100).await.unwrap();
    assert_eq!(
        rest.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![rows[1].id, rows[2].id]
    );
    let one = log.retention_ledger(Some(rows[0].id), 1).await.unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].id, rows[1].id);
}

// -------------------------------------------------------- export receipts

/// Taken and landed are two states of one row, and `confirm_export` moves a
/// receipt from the first to the second. A filtered page is visible as
/// filtered, which is what stops it extending the chain the guard walks.
#[tokio::test]
async fn export_receipts_show_taken_landed_and_filtered() {
    let log = seeded().await;

    let (_, whole) = log
        .export_recorded(Position::BEGINNING, &Filter::all(), 2)
        .await
        .unwrap();

    let rows = log.export_receipts(None, 100).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].receipt, whole,
        "the wrapper carries the receipt `export_recorded` returned"
    );
    assert!(rows[0].taken_ms > 0);
    assert_eq!(rows[0].landed_ms, None, "taken, not landed");
    assert!(!rows[0].landed());

    log.confirm_export(whole.id).await.unwrap();
    let rows = log.export_receipts(None, 100).await.unwrap();
    assert!(rows[0].landed(), "and now it has landed");
    assert!(rows[0].landed_ms.unwrap() >= rows[0].taken_ms);
    assert_eq!(
        rows[0].receipt, whole,
        "confirming changes when, not what was taken"
    );

    // A filtered page: still a receipt, and still not whole.
    let (_, filtered) = log
        .export_recorded(Position::BEGINNING, &Filter::all().stream_prefix("a"), 10)
        .await
        .unwrap();
    assert!(!filtered.whole);

    let rows = log.export_receipts(None, 100).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].receipt.whole);
    assert!(!rows[1].receipt.whole, "visible as filtered: {rows:?}");
    assert_eq!(rows[1].receipt, filtered);

    // Only the whole, landed one counts towards the chain — the listing is
    // the evidence behind that number.
    assert_eq!(log.exported_through().await.unwrap(), whole.through);

    // Paged, exclusive on the id.
    let rest = log.export_receipts(Some(whole.id), 100).await.unwrap();
    assert_eq!(
        rest.iter().map(|row| row.receipt.id).collect::<Vec<_>>(),
        vec![filtered.id]
    );
    assert!(log
        .export_receipts(Some(filtered.id), 100)
        .await
        .unwrap()
        .is_empty());
}

// ------------------------------------------------------------ empty, zero

/// An empty database answers every listing with an empty page, not an error.
/// There is nothing exceptional about a log nothing has been written to.
#[tokio::test]
async fn every_listing_of_an_empty_database_is_an_empty_page() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    assert!(log.streams(None, None, 100).await.unwrap().is_empty());
    assert!(log.streams(None, Some("x"), 100).await.unwrap().is_empty());
    assert!(log.kinds(None, 100).await.unwrap().is_empty());
    assert!(log.checkpoints(None, 100).await.unwrap().is_empty());
    assert!(log.retention_ledger(None, 100).await.unwrap().is_empty());
    assert!(log.export_receipts(None, 100).await.unwrap().is_empty());

    // And a cursor past the end of an empty table is the same answer.
    assert!(log
        .streams(Some("zzz"), None, 100)
        .await
        .unwrap()
        .is_empty());
    assert!(log.retention_ledger(Some(9), 100).await.unwrap().is_empty());
}

/// `limit` of 0 is an empty page, which is what `read_all` does with one.
#[tokio::test]
async fn a_limit_of_zero_is_an_empty_page_as_it_is_for_a_read() {
    let log = seeded().await;
    log.checkpoint_save("c", Position::new(1)).await.unwrap();
    log.retain(Plan::Before(Position::new(1)), Guard::Force)
        .await
        .unwrap();
    log.export_recorded(Position::BEGINNING, &Filter::all(), 2)
        .await
        .unwrap();

    // Each of these has rows to return, and returns none.
    assert!(log.streams(None, None, 0).await.unwrap().is_empty());
    assert!(log.kinds(None, 0).await.unwrap().is_empty());
    assert!(log.checkpoints(None, 0).await.unwrap().is_empty());
    assert!(log.retention_ledger(None, 0).await.unwrap().is_empty());
    assert!(log.export_receipts(None, 0).await.unwrap().is_empty());

    assert!(log
        .read_all(Position::BEGINNING, &Filter::all(), 0)
        .await
        .unwrap()
        .is_empty());
    assert!(
        !log.streams(None, None, 1).await.unwrap().is_empty(),
        "the rows are there; 0 is the bound, not the state"
    );
}

// ------------------------------------------------------------------ plans

/// `EXPLAIN QUERY PLAN` through the hatch, as `tests/stream_prefix.rs` does:
/// a plan is a fact about a statement, so the statement has to be the one
/// that runs.
async fn plan(log: &SqliteEventLog, sql: &str, params: Vec<Value>) -> String {
    log.query(sql, params)
        .await
        .unwrap()
        .iter()
        .map(|row| row["detail"].as_str().unwrap().to_string())
        .collect::<Vec<_>>()
        .join(" | ")
}

/// No listing needs an index the ladder does not already ship, which is why
/// none was added. Three of the five page by a primary key; `streams` and
/// `checkpoints` page by the automatic index their `TEXT PRIMARY KEY`
/// creates, and `kinds` walks `events_kind_position`.
///
/// The one with a cost worth naming is `kinds`: `DISTINCT` over an index has
/// no seek-to-the-next-distinct-value plan in SQLite, so it walks the index
/// entries — one per event — of the kinds in the page. `LIMIT` is what
/// bounds that walk, which is why this listing takes one.
#[tokio::test]
async fn the_listings_seek_on_indices_the_ladder_already_ships() {
    let log = seeded().await;

    let detail = plan(
        &log,
        "EXPLAIN QUERY PLAN SELECT stream, next_seq FROM stream_seq \
         WHERE stream > ?1 AND stream < ?2 AND stream >= ?3 ORDER BY stream LIMIT ?4",
        vec![json!("a"), json!("t"), json!("s"), json!(10)],
    )
    .await;
    assert!(
        detail.contains("USING INDEX") || detail.contains("USING PRIMARY KEY"),
        "a seek on the counter's key, not a scan: {detail}"
    );
    assert!(!detail.contains("TEMP B-TREE"), "and no sort: {detail}");

    let detail = plan(
        &log,
        "EXPLAIN QUERY PLAN SELECT DISTINCT kind FROM events WHERE kind > ?1 \
         ORDER BY kind LIMIT ?2",
        vec![json!("a"), json!(10)],
    )
    .await;
    assert!(
        detail.contains("events_kind_position"),
        "the shipped index on (kind, position): {detail}"
    );
    assert!(!detail.contains("TEMP B-TREE"), "and no sort: {detail}");

    for (table, column) in [
        ("checkpoints", "consumer"),
        ("retention", "id"),
        ("exports", "id"),
    ] {
        let detail = plan(
            &log,
            &format!(
                "EXPLAIN QUERY PLAN SELECT {column} FROM {table} WHERE {column} > ?1 \
                 ORDER BY {column} LIMIT ?2"
            ),
            vec![json!(1), json!(10)],
        )
        .await;
        assert!(!detail.contains("TEMP B-TREE"), "{table}: {detail}");
    }
}

/// The wrapper is a plain record: what `export_recorded` handed back, plus
/// the two moments the row keeps. A caller holding one gets the other by
/// naming the field.
#[tokio::test]
async fn an_export_record_is_the_receipt_plus_its_two_moments() {
    let log = seeded().await;
    let (_, receipt) = log
        .export_recorded(Position::BEGINNING, &Filter::all(), 100)
        .await
        .unwrap();
    log.confirm_export(receipt.id).await.unwrap();

    let record: ExportRecord = log
        .export_receipts(None, 1)
        .await
        .unwrap()
        .pop()
        .expect("one receipt");
    assert_eq!(record.receipt.id, receipt.id);
    assert_eq!(record.receipt.from, Position::BEGINNING);
    assert_eq!(record.receipt.through, receipt.through);
    assert_eq!(record.receipt.count, 6);
    assert!(record.receipt.whole);
    assert!(record.landed());
}
