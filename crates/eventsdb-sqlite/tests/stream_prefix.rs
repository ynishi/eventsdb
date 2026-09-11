//! The stream-prefix read axis.
//!
//! A stream here is a period — `session-2026-09-01`, `session-2026-09-02` —
//! so the streams that belong together are not a set anybody holds, and
//! before this axis a reader wanting "every session" had to enumerate them
//! first. What is tested here is that the prefix answers the same question
//! the enumerated set does, that it ANDs with the other three axes rather
//! than replacing any of them, that the range is exact where UTF-8 makes it
//! interesting — a multi-byte character, and a last character with no
//! successor — that every read path honours it, and that an export taken
//! under one is a filtered export.

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

/// Eight events over six streams: three periods of one session, and three
/// names chosen to sit next to `session-` without starting with it.
///
/// ```text
///   position  stream               kind    meta
///   1         session-2026-09-01   opened  tenant=a
///   2         order-1              placed  tenant=b     before the range
///   3         session-2026-09-02   opened  tenant=a
///   4         sessions-x           opened  tenant=a     past the bound
///   5         session-2026-09-01   closed  tenant=a
///   6         sessio               opened  tenant=a     short of the prefix
///   7         session-2026-09-03   opened  tenant=b
///   8         session-2026-09-02   closed  tenant=a
/// ```
///
/// `order-1` sorts below `session-`, `sessions-x` above its bound and
/// `sessio` below the prefix itself, so a range that is off by one name
/// fails rather than passes by accident.
async fn seeded() -> SqliteEventLog {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let writes: [(&str, &str, Value); 8] = [
        ("session-2026-09-01", "opened", json!({ "tenant": "a" })),
        ("order-1", "placed", json!({ "tenant": "b" })),
        ("session-2026-09-02", "opened", json!({ "tenant": "a" })),
        ("sessions-x", "opened", json!({ "tenant": "a" })),
        ("session-2026-09-01", "closed", json!({ "tenant": "a" })),
        ("sessio", "opened", json!({ "tenant": "a" })),
        ("session-2026-09-03", "opened", json!({ "tenant": "b" })),
        ("session-2026-09-02", "closed", json!({ "tenant": "a" })),
    ];
    for (stream, kind, meta) in writes {
        log.stream_handle(stream)
            .append(event(kind, meta))
            .await
            .unwrap();
    }
    log
}

/// The three session periods, named in full, which is what a reader had to
/// assemble before the prefix existed.
const ENUMERATED: [&str; 3] = [
    "session-2026-09-01",
    "session-2026-09-02",
    "session-2026-09-03",
];

async fn positions(log: &SqliteEventLog, filter: &Filter) -> Vec<u64> {
    log.read_all(Position::BEGINNING, filter, 100)
        .await
        .unwrap()
        .iter()
        .map(|r| r.position.get())
        .collect()
}

/// The question the axis exists for: the prefix reads what the enumerated
/// set reads, in the same order, without the reader having to know the
/// names.
#[tokio::test]
async fn a_prefix_reads_what_the_enumerated_set_reads() {
    let log = seeded().await;
    let by_prefix = positions(&log, &Filter::all().stream_prefix("session-")).await;
    let by_set = positions(&log, &Filter::all().streams(ENUMERATED)).await;

    assert_eq!(by_prefix, vec![1, 3, 5, 7, 8]);
    assert_eq!(by_prefix, by_set, "same events, same position order");
}

/// A name that sorts next to the range but is not in it. `sessio` is short
/// of the prefix, `sessions-x` is past its bound, and `order-1` is below it
/// — none of the three is reachable by any prefix that reaches the sessions.
#[tokio::test]
async fn a_neighbouring_name_is_not_in_the_range() {
    let log = seeded().await;
    assert_eq!(
        positions(&log, &Filter::all().stream_prefix("sessions")).await,
        vec![4]
    );
    assert_eq!(
        positions(&log, &Filter::all().stream_prefix("sessio")).await,
        vec![1, 3, 4, 5, 6, 7, 8],
        "a shorter prefix is a wider range, and the store parses no separator"
    );
    assert_eq!(
        positions(&log, &Filter::all().stream_prefix("nothing-")).await,
        Vec::<u64>::new()
    );
}

/// Both are predicates on the stream name, so a filter carrying both reads
/// the members of the set that start with the prefix — and the member that
/// does not is simply not in the answer.
#[tokio::test]
async fn a_set_and_a_prefix_narrow_each_other() {
    let log = seeded().await;
    let filter = Filter::all()
        .streams(["session-2026-09-01", "order-1"])
        .stream_prefix("session-");
    assert_eq!(positions(&log, &filter).await, vec![1, 5]);

    // A set with no member in the range answers with nothing, rather than
    // one predicate winning over the other.
    let filter = Filter::all().streams(["order-1"]).stream_prefix("session-");
    assert_eq!(positions(&log, &filter).await, Vec::<u64>::new());
}

/// Four axes, one AND.
#[tokio::test]
async fn the_prefix_composes_with_kinds_and_meta() {
    let log = seeded().await;

    assert_eq!(
        positions(&log, &Filter::kinds(["opened"]).stream_prefix("session-")).await,
        vec![1, 3, 7]
    );
    assert_eq!(
        positions(
            &log,
            &Filter::kinds(["opened"])
                .stream_prefix("session-")
                .meta("tenant", "a")
        )
        .await,
        vec![1, 3]
    );
    assert_eq!(
        positions(
            &log,
            &Filter::kinds(["opened"])
                .streams(ENUMERATED)
                .stream_prefix("session-")
                .meta("tenant", "b")
        )
        .await,
        vec![7]
    );
}

/// Every name starts with nothing, so an empty prefix is every stream. It is
/// not "select nothing" — that is what an empty `streams` set says, and the
/// two readings are kept apart deliberately.
#[tokio::test]
async fn an_empty_prefix_is_every_stream() {
    let log = seeded().await;
    let all = positions(&log, &Filter::all()).await;
    assert_eq!(all, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(positions(&log, &Filter::all().stream_prefix("")).await, all);
}

/// A prefix ending in a multi-byte character. The bound moves that character
/// to the next scalar value, and `会該` is exactly that next value — so a
/// stream named with it is the one name the range must not reach.
#[tokio::test]
async fn a_multi_byte_prefix_stops_at_the_next_scalar_value() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    for stream in ["会話-a", "会話-b", "会", "会該-1", "会話"] {
        log.stream_handle(stream)
            .append(event("noted", json!({})))
            .await
            .unwrap();
    }

    // 1 会話-a, 2 会話-b, 3 会 (short), 4 会該-1 (the bound itself), 5 会話
    assert_eq!(
        positions(&log, &Filter::all().stream_prefix("会話")).await,
        vec![1, 2, 5]
    );
    assert_eq!(
        positions(&log, &Filter::all().stream_prefix("会")).await,
        vec![1, 2, 3, 4, 5]
    );
}

/// A prefix whose last character is the maximum scalar value has no
/// successor, so the bound carries to the character before it — and a prefix
/// that is nothing but the maximum has no upper bound at all and reads to the
/// end of the name order. Both are ranges, and neither panics.
#[tokio::test]
async fn a_prefix_ending_at_the_maximum_scalar_value_still_bounds() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    for stream in ["a\u{10FFFF}", "a\u{10FFFF}z", "ab", "b", "\u{10FFFF}tail"] {
        log.stream_handle(stream)
            .append(event("noted", json!({})))
            .await
            .unwrap();
    }

    // 1 a<MAX>, 2 a<MAX>z, 3 ab, 4 b, 5 <MAX>tail
    assert_eq!(
        positions(&log, &Filter::all().stream_prefix("a\u{10FFFF}")).await,
        vec![1, 2],
        "the carry excludes `ab`, which sorts below the dropped character"
    );
    assert_eq!(
        positions(&log, &Filter::all().stream_prefix("\u{10FFFF}")).await,
        vec![5],
        "no upper bound, and nothing else sorts that high"
    );
    assert_eq!(
        positions(&log, &Filter::all().stream_prefix("a")).await,
        vec![1, 2, 3]
    );
}

/// The live tail is the same range read, resumed, so it narrows the same
/// way: a write to a stream outside the prefix does not surface, and the
/// next one inside it does.
#[tokio::test]
async fn a_subscription_honours_the_prefix_live() {
    let log = seeded().await;
    let mut sub = log
        .subscribe(Position::new(8), Filter::all().stream_prefix("session-"))
        .unwrap();

    log.stream_handle("sessions-x")
        .append(event("noted", json!({})))
        .await
        .unwrap();
    let wanted = log
        .stream_handle("session-2026-09-04")
        .append(event("opened", json!({})))
        .await
        .unwrap();

    let next = tokio::time::timeout(std::time::Duration::from_secs(2), sub.next())
        .await
        .expect("the in-range append wakes the subscriber")
        .unwrap()
        .unwrap();
    assert_eq!(
        next.position,
        wanted.position.unwrap(),
        "the `sessions-x` write sat between the two and was not yielded"
    );
    assert_eq!(next.stream, "session-2026-09-04");
}

/// `replay` is the same loop that ends where `subscribe` waits, on the same
/// query, so it narrows identically.
#[tokio::test]
async fn a_replay_honours_the_prefix() {
    let log = seeded().await;
    let seen: Vec<u64> = log
        .replay(Position::BEGINNING, Filter::all().stream_prefix("session-"))
        .map(|r| r.unwrap().position.get())
        .collect()
        .await;
    assert_eq!(seen, vec![1, 3, 5, 7, 8]);
}

/// Export is the same read stopped before the upcaster, and a receipt for it
/// says whether the filter selected everything in the range. A prefix is a
/// filter, so it does not — and the chain [`SqliteEventLog::exported_through`]
/// walks does not advance however faithfully the page is confirmed.
#[tokio::test]
async fn an_export_under_a_prefix_is_a_filtered_export() {
    let log = seeded().await;
    let filter = Filter::all().stream_prefix("session-");

    let batch = log.export(Position::BEGINNING, &filter, 100).await.unwrap();
    let exported: Vec<u64> = batch
        .iter()
        .map(|e| {
            e.position
                .expect("an export off this log witnesses itself")
                .get()
        })
        .collect();
    assert_eq!(exported, vec![1, 3, 5, 7, 8]);

    let (page, receipt) = log
        .export_recorded(Position::BEGINNING, &filter, 100)
        .await
        .unwrap();
    assert_eq!(page.len(), 5);
    assert!(!receipt.whole, "a prefix preserved part of its range");
    assert_eq!(receipt.through, Position::new(8));

    log.confirm_export(receipt.id).await.unwrap();
    assert_eq!(log.exported_through().await.unwrap(), Position::BEGINNING);
}

/// An empty prefix places no condition, so an export under one is whole —
/// the same reading an empty `meta` pair list gets on the same line.
#[tokio::test]
async fn an_empty_prefix_leaves_an_export_whole() {
    let log = seeded().await;
    let (_, receipt) = log
        .export_recorded(Position::BEGINNING, &Filter::all().stream_prefix(""), 100)
        .await
        .unwrap();
    assert!(receipt.whole);
    log.confirm_export(receipt.id).await.unwrap();
    assert_eq!(log.exported_through().await.unwrap(), Position::new(8));
}

/// The `SELECT` list the `WHERE` builder reads, so the plan is costed
/// against the columns a real read asks for rather than a narrower set that
/// an index might cover.
const SELECTED: &str = "position, stream, seq, epoch_ms, kind, schema_version, meta, data";

/// `EXPLAIN QUERY PLAN` through the hatch, which allows it: it is a read,
/// and `query` asks SQLite's own `sqlite3_stmt_readonly` rather than
/// inspecting the text.
async fn plan(log: &SqliteEventLog, sql: &str, params: Vec<Value>) -> String {
    log.query(sql, params)
        .await
        .unwrap()
        .iter()
        .map(|row| row["detail"].as_str().unwrap().to_string())
        .collect::<Vec<_>>()
        .join(" | ")
}

/// What the planner does with the range, asked rather than assumed. The
/// statements are the shapes the `WHERE` builder produces, which is why they
/// are written out here: a plan is a fact about a statement, so the statement
/// has to be the one that runs.
///
/// A prefix alone is a seek on a range of `stream`. The index it seeks is
/// `sqlite_autoindex_events_1` — the one `UNIQUE (stream, seq)` creates —
/// rather than `events_stream_kind_seq`; both lead on `stream` and either
/// serves the range, and SQLite costs the narrower one lower. Neither orders
/// by `position`, so the sort is a temp B-tree, which is the price of the
/// seek.
#[tokio::test]
async fn a_prefix_alone_is_a_seek_on_a_stream_leading_index() {
    let log = seeded().await;
    let detail = plan(
        &log,
        &format!(
            "EXPLAIN QUERY PLAN SELECT {SELECTED} FROM events \
             WHERE position > ?1 AND stream >= ?2 AND stream < ?3 \
             ORDER BY position LIMIT ?4"
        ),
        vec![json!(0), json!("session-"), json!("session."), json!(100)],
    )
    .await;

    assert!(
        detail.contains("SEARCH events USING INDEX"),
        "a seek, not a scan: {detail}"
    );
    assert!(
        detail.contains("(stream>? AND stream<?)"),
        "and the range is what it seeks on: {detail}"
    );
    assert!(
        detail.contains("sqlite_autoindex_events_1") || detail.contains("events_stream_kind_seq"),
        "on an index leading with `stream`: {detail}"
    );
}

/// The other half of the same question, which the issue left open: with
/// `kinds` present as well, SQLite does **not** use the stream range. It
/// takes `events_kind_position` and seeks `kind=? AND position>?` — that
/// index is on `(kind, position)`, so it answers the `kind` term and the
/// `ORDER BY position` together, and the stream range is applied as a filter
/// on the rows it returns. There is no temp B-tree in this plan, which is
/// what it is buying.
#[tokio::test]
async fn with_kinds_the_planner_takes_the_kind_index_and_orders_by_it() {
    let log = seeded().await;
    let detail = plan(
        &log,
        &format!(
            "EXPLAIN QUERY PLAN SELECT {SELECTED} FROM events \
             WHERE position > ?1 AND stream >= ?2 AND stream < ?3 AND kind IN (?4) \
             ORDER BY position LIMIT ?5"
        ),
        vec![
            json!(0),
            json!("session-"),
            json!("session."),
            json!("opened"),
            json!(100),
        ],
    )
    .await;

    assert!(
        detail.contains("SEARCH events USING INDEX events_kind_position"),
        "the kind index, not the stream range: {detail}"
    );
    assert!(
        !detail.contains("TEMP B-TREE"),
        "which is what it buys — position order comes off the index: {detail}"
    );
}
