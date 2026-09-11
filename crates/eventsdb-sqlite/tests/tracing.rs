#![cfg(feature = "tracing")]
//! What the instrumentation emits, and what it must never emit.
//!
//! The subscriber here is a `Layer` of this file's own rather than a `fmt`
//! one reading back text: a span's fields are the thing under test, and a
//! layer sees them as fields — name, value, level — instead of as a line
//! somebody's formatter chose. It also keeps the dev-dependency to
//! `tracing-subscriber` with `registry` alone, which is what keeps `log` out
//! of the graph.
//!
//! Every test installs it with `set_default`, which is **per thread**, so
//! every test here runs on `#[tokio::test]`'s current-thread runtime. Nothing
//! this crate emits comes from the SQLite thread — the counts and coordinates
//! travel back out of the isle's job and are recorded on the async side,
//! exactly so that they are recorded where the span is current.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use eventsdb_core::error::Error;
use eventsdb_core::{EventLog, EventStore, Filter, Position, Result};
use eventsdb_sqlite::{
    Guard, JsonLinesSink, OpenOptions, Plan, Projection, ProjectionRunner, SqliteEventLog,
    Transaction,
};
use futures_util::StreamExt;
use serde_json::{json, Map, Value};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record as SpanRecord};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// The distinctive string the payload test hunts for. It goes into `data` and
/// into `meta` — the two fields that are the caller's — and into nothing else,
/// so finding it anywhere in the captured output means something recorded a
/// payload.
const NEEDLE: &str = "tenant-a1b2c3-do-not-log-me";

// ---------------------------------------------------------------- the layer

#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    Span,
    Event,
}

#[derive(Debug, Clone)]
struct Captured {
    kind: Kind,
    level: Level,
    /// The span's name, or the event's target.
    name: String,
    /// The name of the span this one was opened under, when there was one.
    /// Always `None` for an event; only the nesting of spans is under test.
    parent: Option<String>,
    fields: Vec<(String, String)>,
}

impl Captured {
    fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// Everything this record would ever show a reader, as one string. What
    /// the payload test searches.
    fn rendered(&self) -> String {
        let mut out = format!("{:?} {} {}", self.kind, self.level, self.name);
        for (key, value) in &self.fields {
            out.push_str(&format!(" {key}={value}"));
        }
        out
    }
}

#[derive(Default)]
struct Fields(Vec<(String, String)>);

impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_string(), value.to_string()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .push((field.name().to_string(), format!("{value:?}")));
    }
}

/// Collects finished spans and every event, in order.
#[derive(Clone, Default)]
struct Collector(Arc<Mutex<Vec<Captured>>>);

impl Collector {
    fn taken(&self) -> Vec<Captured> {
        self.0.lock().expect("the capture lock").clone()
    }

    fn spans(&self, name: &str) -> Vec<Captured> {
        self.taken()
            .into_iter()
            .filter(|record| record.kind == Kind::Span && record.name == name)
            .collect()
    }

    fn events(&self) -> Vec<Captured> {
        self.taken()
            .into_iter()
            .filter(|record| record.kind == Kind::Event)
            .collect()
    }
}

impl<S> Layer<S> for Collector
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        let span = ctx.span(id).expect("a span that was just created");
        let parent = span.parent().map(|parent| parent.name().to_string());
        span.extensions_mut().insert(Captured {
            kind: Kind::Span,
            level: *attrs.metadata().level(),
            name: attrs.metadata().name().to_string(),
            parent,
            fields: fields.0,
        });
    }

    /// The fields recorded on the way out — `folded`, `rows`, `position` —
    /// arrive here rather than at creation.
    fn on_record(&self, id: &Id, values: &SpanRecord<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("an open span");
        let mut fields = Fields::default();
        values.record(&mut fields);
        let mut extensions = span.extensions_mut();
        if let Some(captured) = extensions.get_mut::<Captured>() {
            captured.fields.extend(fields.0);
        }
    }

    /// On close, so a span is captured with everything it ever carried.
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("a closing span");
        let captured = span.extensions().get::<Captured>().cloned();
        if let Some(captured) = captured {
            self.0.lock().expect("the capture lock").push(captured);
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0.lock().expect("the capture lock").push(Captured {
            kind: Kind::Event,
            level: *event.metadata().level(),
            name: event.metadata().target().to_string(),
            parent: None,
            fields: fields.0,
        });
    }
}

/// Install the collector for this thread, at every level including `trace`.
fn capturing() -> (Collector, tracing::subscriber::DefaultGuard) {
    let collector = Collector::default();
    let guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(collector.clone()));
    (collector, guard)
}

// ------------------------------------------------------------------ fixtures

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

/// An event whose `data` and `meta` both carry [`NEEDLE`].
fn loaded(kind: &str) -> Map<String, Value> {
    json!({
        "kind": kind,
        "data": { "customer": NEEDLE, "total": 40 },
        "meta": { "tenant": NEEDLE },
    })
    .as_object()
    .unwrap()
    .clone()
}

/// Counts what it is shown, into a table of its own.
struct Counter;

impl Projection for Counter {
    fn name(&self) -> &str {
        "counter"
    }

    fn init(&mut self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute_batch("CREATE TABLE IF NOT EXISTS counted (n INTEGER)")
            .map_err(|e| Error::storage(e.to_string()))
    }

    fn reset(&mut self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute_batch("DELETE FROM counted")
            .map_err(|e| Error::storage(e.to_string()))
    }

    fn apply(&mut self, tx: &Transaction<'_>, _event: &eventsdb_core::Recorded) -> Result<()> {
        tx.execute("INSERT INTO counted (n) VALUES (1)", [])
            .map(|_| ())
            .map_err(|e| Error::storage(e.to_string()))
    }

    fn tolerates_truncation(&self) -> bool {
        true
    }
}

// --------------------------------------------------------------------- tests

/// The field the whole `decide` span exists for: how many events the decision
/// was shown, which is the number the README's 30.4 ms against 81 µs is about.
#[tokio::test]
async fn the_decide_span_carries_how_many_events_the_decision_was_shown() {
    let (captured, _guard) = capturing();
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut orders = log.stream_handle("order-1");

    orders.append(event("placed")).await.unwrap();
    orders.append(event("paid")).await.unwrap();

    orders
        .append_if(None, Box::new(|_seen| Some(event("shipped"))))
        .await
        .unwrap()
        .expect("the decision appended");

    let decide = captured.spans("eventsdb.decide");
    assert_eq!(decide.len(), 1, "one decision, one span");
    assert_eq!(decide[0].level, Level::DEBUG);
    assert_eq!(decide[0].field("folded"), Some("2"));
    assert_eq!(decide[0].field("appended"), Some("true"));
    assert_eq!(decide[0].field("stream"), Some("order-1"));

    // The append the decision made is not a second `eventsdb.append` span:
    // the insert happens inside the decision's own transaction.
    assert_eq!(captured.spans("eventsdb.append").len(), 2);
}

/// The rule the whole feature is subject to. Every instrumented path runs
/// over an event whose `data` and `meta` carry [`NEEDLE`], with the
/// subscriber at `trace`, and the string appears nowhere.
#[tokio::test]
async fn the_payload_reaches_nothing_that_is_captured() {
    let (captured, _guard) = capturing();
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut orders = log.stream_handle("order-1");

    // append, append_many, append_at, append_expecting
    orders.append(loaded("placed")).await.unwrap();
    orders
        .append_many(vec![loaded("paid"), loaded("packed")])
        .await
        .unwrap();
    orders
        .append_at(1_700_000_000_000, loaded("held"))
        .await
        .unwrap();
    orders
        .append_expecting(eventsdb_core::store::Expected::Seq(4), loaded("released"))
        .await
        .unwrap();

    // decide: the decision is shown every event, and writes another one.
    orders
        .append_if(None, Box::new(|_seen| Some(loaded("shipped"))))
        .await
        .unwrap()
        .expect("the decision appended");

    // project
    let mut runner = log.runner_now(Counter);
    runner.init().await.unwrap();
    assert!(runner.catch_up().await.unwrap() > 0);

    // the hatch, both doors
    log.query("SELECT stream, position FROM events", vec![])
        .await
        .unwrap();
    log.with_transaction(|tx| {
        tx.append("order-2", loaded("noted"))?;
        Ok(())
    })
    .await
    .unwrap();

    // replay, which is the paging loop the subscription shares
    let replayed: Vec<_> = log
        .replay(Position::BEGINNING, Filter::all())
        .collect()
        .await;
    assert_eq!(replayed.len(), 7);

    // export by both doors: the page is the payload itself, and the span must
    // carry nothing of it
    let page = log
        .export(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(page.len(), 7);
    let (page, receipt) = log
        .export_recorded(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(page.len(), 7);
    log.confirm_export(receipt.id).await.unwrap();

    // index: the key is a name under `meta`, never the value under it
    log.index_meta("tenant").await.unwrap();

    // backup: the whole file, payload included, goes past the span
    let dir = tempfile::tempdir().unwrap();
    log.backup_to(dir.path().join("copy.db")).await.unwrap();

    // retain, late so that nothing above is refused for a missing front,
    // and reclaim after it, which is the order they run in
    log.retain(Plan::Before(Position::new(2)), Guard::Force)
        .await
        .unwrap();
    log.reclaim().await.unwrap();

    let records = captured.taken();
    assert!(
        records.len() > 10,
        "the paths ran instrumented: {} records",
        records.len()
    );
    // The search has teeth: a field's *value* is part of what `rendered`
    // returns, so a payload recorded as one would be found the way the stream
    // id is.
    assert!(
        records
            .iter()
            .any(|record| record.rendered().contains("order-1")),
        "field values are rendered"
    );
    for record in &records {
        let rendered = record.rendered();
        assert!(
            !rendered.contains(NEEDLE),
            "a payload reached the instrumentation: {rendered}"
        );
    }
}

/// A refusal is a `warn` and carries the error's own text — the whole text,
/// so whoever reads the log reads what the caller was told.
#[tokio::test]
async fn a_refusal_is_a_warning_carrying_the_errors_own_text() {
    let (captured, _guard) = capturing();
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut orders = log.stream_handle("order-1");
    for kind in ["placed", "paid", "shipped"] {
        orders.append(event(kind)).await.unwrap();
    }

    // A consumer that has seen the first event and no more.
    log.checkpoint_save("reader", Position::new(1))
        .await
        .unwrap();

    let refused = log
        .retain(Plan::Before(Position::new(3)), Guard::RegisteredConsumers)
        .await
        .expect_err("a consumer is behind the plan");
    assert!(matches!(refused, Error::ConsumerBehind { .. }));

    let warnings: Vec<_> = captured
        .events()
        .into_iter()
        .filter(|record| record.level == Level::WARN)
        .collect();
    assert_eq!(warnings.len(), 1, "one refusal, one warning");
    assert_eq!(warnings[0].field("refusal"), Some("consumer_behind"));
    assert_eq!(
        warnings[0].field("error"),
        Some(refused.to_string().as_str())
    );
    // The consumer's name is a field of the error and is allowed; the payload
    // is not, and there is none in it.
    assert!(warnings[0].field("error").unwrap().contains("`reader`"));
}

/// The distinction the README quotes in microseconds against milliseconds:
/// the subscription was woken by a commit, not by the poll interval.
#[tokio::test]
async fn a_wake_says_it_was_a_commit() {
    let (captured, _guard) = capturing();
    // A poll interval far longer than this test, so an interval wake cannot
    // happen at all and the assertion is not a race.
    let log = SqliteEventLog::open_in_memory_with(
        OpenOptions::default().poll_interval(Duration::from_secs(300)),
    )
    .await
    .unwrap();
    let mut orders = log.stream_handle("order-1");
    let mut tail = log.subscribe(Position::BEGINNING, Filter::all()).unwrap();

    // Park the subscription on the wait: the log is empty, so the first read
    // comes back short and there is nothing to yield.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), tail.next())
            .await
            .is_err(),
        "nothing to yield yet"
    );

    orders.append(event("placed")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), tail.next())
        .await
        .expect("the commit woke it")
        .expect("a batch")
        .unwrap();

    let wakes: Vec<_> = captured
        .events()
        .into_iter()
        .filter(|record| record.field("woken_by").is_some())
        .collect();
    assert!(!wakes.is_empty(), "the wait recorded what woke it");
    assert!(wakes.iter().all(|record| record.level == Level::TRACE));
    assert!(wakes
        .iter()
        .any(|record| record.field("woken_by") == Some("commit")));
    assert!(
        wakes
            .iter()
            .all(|record| record.field("woken_by") != Some("interval")),
        "nothing could have woken on a 300-second interval"
    );
}

/// The other half: nobody writes, so every wake is the interval.
#[tokio::test]
async fn a_wake_says_it_was_the_interval() {
    let (captured, _guard) = capturing();
    let log = SqliteEventLog::open_in_memory_with(
        OpenOptions::default().poll_interval(Duration::from_millis(20)),
    )
    .await
    .unwrap();
    let mut tail = log.subscribe(Position::BEGINNING, Filter::all()).unwrap();

    // No writer at all, so nothing can publish and every round out of the
    // wait is the interval expiring.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), tail.next())
            .await
            .is_err(),
        "an empty log yields nothing"
    );

    let wakes: Vec<_> = captured
        .events()
        .into_iter()
        .filter(|record| record.field("woken_by").is_some())
        .collect();
    assert!(!wakes.is_empty(), "the interval expired at least once");
    assert!(wakes
        .iter()
        .all(|record| record.field("woken_by") == Some("interval")));
    assert!(wakes.iter().all(|record| record.field("cursor").is_some()));
}

/// The hatch's `sql` is the caller's text, so it is at `trace` and bounded,
/// and the span a `debug` subscriber sees carries the row count instead.
#[tokio::test]
async fn the_hatch_keeps_the_statement_at_trace_and_clips_it() {
    let (captured, _guard) = capturing();
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut orders = log.stream_handle("order-1");
    orders.append(event("placed")).await.unwrap();

    // Long enough to be clipped: the comment is padded past the bound.
    let padding = "x".repeat(400);
    let sql = format!("SELECT position FROM events /* {padding} */");
    let rows = log.query(&sql, vec![]).await.unwrap();
    assert_eq!(rows.len(), 1);

    let hatch = captured.spans("eventsdb.hatch");
    assert_eq!(hatch.len(), 1);
    assert_eq!(hatch[0].level, Level::DEBUG);
    assert_eq!(hatch[0].field("op"), Some("query"));
    assert_eq!(hatch[0].field("rows"), Some("1"));
    assert!(
        hatch[0].field("sql").is_none(),
        "the span a `debug` subscriber reads never carries the statement"
    );

    let statements: Vec<_> = captured
        .events()
        .into_iter()
        .filter(|record| record.field("sql").is_some())
        .collect();
    assert_eq!(statements.len(), 1);
    assert_eq!(statements[0].level, Level::TRACE);
    let emitted = statements[0].field("sql").unwrap();
    assert!(emitted.starts_with("SELECT position FROM events"));
    assert_eq!(emitted.chars().count(), 257, "256 characters and the mark");
    assert!(emitted.ends_with('…'));
}

/// The runner's span reports the batch it was given and where the cursor
/// ended up, which is what tells a reader whether a fold is keeping up.
#[tokio::test]
async fn the_project_span_reports_the_batch_and_the_cursor() {
    let (captured, _guard) = capturing();
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut orders = log.stream_handle("order-1");
    for kind in ["placed", "paid", "shipped"] {
        orders.append(event(kind)).await.unwrap();
    }

    let mut runner: ProjectionRunner<Counter> = log.runner_now(Counter).with_batch(2);
    runner.init().await.unwrap();
    assert_eq!(runner.run_once().await.unwrap(), 2);

    let project = captured.spans("eventsdb.project");
    assert_eq!(project.len(), 1, "`init` is not a batch and has no span");
    assert_eq!(project[0].field("name"), Some("counter"));
    assert_eq!(project[0].field("batch"), Some("2"));
    assert_eq!(project[0].field("applied"), Some("2"));
    assert_eq!(project[0].field("cursor"), Some("2"));
}

// ------------------------------------------------- the second round (#54)

/// The page count is the whole file — which is the number that says what
/// the reader's transaction was pinned across.
#[tokio::test]
async fn the_backup_span_carries_the_page_count_of_the_file() {
    let (captured, _guard) = capturing();
    let dir = tempfile::tempdir().unwrap();
    let log = SqliteEventLog::open(dir.path().join("events.db"))
        .await
        .unwrap();
    let mut orders = log.stream_handle("order-1");
    for kind in ["placed", "paid", "shipped"] {
        orders.append(event(kind)).await.unwrap();
    }

    let copy = dir.path().join("copy.db");
    log.backup_to(&copy).await.unwrap();

    let backup = captured.spans("eventsdb.backup");
    assert_eq!(backup.len(), 1, "one copy, one span");
    assert_eq!(backup[0].level, Level::DEBUG);
    assert_eq!(
        backup[0].field("path"),
        Some(copy.display().to_string().as_str()),
        "the path as the caller gave it"
    );
    let pages: i64 = backup[0].field("pages").unwrap().parse().unwrap();
    assert!(pages > 0, "a file with a schema in it has pages: {pages}");

    // The copy is that many pages, which is the same statement made twice.
    let restored = SqliteEventLog::open(&copy).await.unwrap();
    let rows = restored.query("PRAGMA page_count", vec![]).await.unwrap();
    let copied = rows[0].values().next().and_then(Value::as_i64).unwrap();
    assert_eq!(copied, pages);
}

/// A refused destination is a bad argument, not a declined request: it is
/// refused before the copy begins, so before the span opens, and no `warn`
/// is raised for it.
#[tokio::test]
async fn a_refused_destination_is_not_a_refusal_event() {
    let (captured, _guard) = capturing();
    let dir = tempfile::tempdir().unwrap();
    let log = SqliteEventLog::open(dir.path().join("events.db"))
        .await
        .unwrap();
    let occupied = dir.path().join("taken.db");
    std::fs::write(&occupied, b"not a backup").unwrap();

    let error = log.backup_to(&occupied).await.unwrap_err();
    assert!(matches!(error, Error::Validation(_)));

    assert!(
        captured.spans("eventsdb.backup").is_empty(),
        "refused before the copy began, so before the span opened"
    );
    assert!(
        captured
            .events()
            .iter()
            .all(|record| record.level != Level::WARN),
        "no refusal event for a bad argument"
    );
}

/// `reclaim` on the writer, and how many pages it handed back — the number
/// that separates a vacuum that did something from the no-op on an older
/// file.
#[tokio::test]
async fn the_reclaim_span_counts_the_pages_it_freed() {
    let (captured, _guard) = capturing();
    let dir = tempfile::tempdir().unwrap();
    let log = SqliteEventLog::open(dir.path().join("events.db"))
        .await
        .unwrap();
    let mut s = log.stream_handle("s");
    let filler = "x".repeat(400);
    for n in 0..600 {
        s.append(
            json!({ "kind": "k", "data": { "n": n, "filler": filler } })
                .as_object()
                .unwrap()
                .clone(),
        )
        .await
        .unwrap();
    }
    log.retain(Plan::Before(Position::new(500)), Guard::Force)
        .await
        .unwrap();

    let before = freelist(&log).await;
    assert!(before > 0, "the removal left pages on the free list");
    log.reclaim().await.unwrap();
    let after = freelist(&log).await;

    let reclaim = captured.spans("eventsdb.reclaim");
    assert_eq!(reclaim.len(), 1);
    assert_eq!(reclaim[0].level, Level::DEBUG);
    let freed: i64 = reclaim[0].field("freed").unwrap().parse().unwrap();
    assert!(freed > 0, "the vacuum gave pages back: {freed}");
    assert_eq!(
        freed,
        before - after,
        "`freed` is the free list's own arithmetic: {before} -> {after}"
    );

    // A second pass, and the field is the same arithmetic again, whatever
    // the first pass left.
    let before = freelist(&log).await;
    log.reclaim().await.unwrap();
    let after = freelist(&log).await;
    let reclaim = captured.spans("eventsdb.reclaim");
    assert_eq!(reclaim.len(), 2);
    let second: i64 = reclaim[1].field("freed").unwrap().parse().unwrap();
    assert_eq!(second, before - after);
}

/// `PRAGMA freelist_count`, through the hatch.
async fn freelist(log: &SqliteEventLog) -> i64 {
    let rows = log.query("PRAGMA freelist_count", vec![]).await.unwrap();
    rows[0].values().next().and_then(Value::as_i64).unwrap()
}

/// `index_meta` twice: a scan the first time, the promised no-op the second,
/// and the span is what tells them apart.
#[tokio::test]
async fn the_index_span_says_whether_it_created_the_index() {
    let (captured, _guard) = capturing();
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut orders = log.stream_handle("order-1");
    orders.append(event("placed")).await.unwrap();

    log.index_meta("tenant").await.unwrap();
    log.index_meta("tenant").await.unwrap();

    let index = captured.spans("eventsdb.index");
    assert_eq!(index.len(), 2);
    assert_eq!(index[0].level, Level::DEBUG);
    assert_eq!(index[0].field("key"), Some("tenant"));
    assert_eq!(index[0].field("created"), Some("true"));
    assert_eq!(index[1].field("key"), Some("tenant"));
    assert_eq!(index[1].field("created"), Some("false"));
}

/// Both doors emit the same span. The receipted one carries the receipt's
/// own fields; the plain one carries what a page without a receipt has.
#[tokio::test]
async fn the_export_span_carries_the_receipt_by_one_door_and_the_count_by_both() {
    let (captured, _guard) = capturing();
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut orders = log.stream_handle("order-1");
    for kind in ["placed", "paid", "shipped"] {
        orders.append(event(kind)).await.unwrap();
    }

    let page = log
        .export(Position::BEGINNING, &Filter::all(), 2)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    let (page, receipt) = log
        .export_recorded(Position::new(2), &Filter::kinds(["shipped"]), 10)
        .await
        .unwrap();
    assert_eq!(page.len(), 1);

    let export = captured.spans("eventsdb.export");
    assert_eq!(export.len(), 2, "one span per door");

    let plain = &export[0];
    assert_eq!(plain.level, Level::DEBUG);
    assert_eq!(plain.field("from"), Some("0"));
    assert_eq!(plain.field("limit"), Some("2"));
    assert_eq!(plain.field("exported"), Some("2"));
    assert_eq!(plain.field("receipt"), None, "no receipt by this door");

    let recorded = &export[1];
    assert_eq!(recorded.field("from"), Some("2"));
    assert_eq!(recorded.field("limit"), Some("10"));
    assert_eq!(recorded.field("exported"), Some("1"));
    assert_eq!(recorded.field("through"), Some("3"));
    assert_eq!(recorded.field("whole"), Some("false"), "a filter was on");
    assert_eq!(
        recorded.field("receipt"),
        Some(receipt.id.to_string().as_str())
    );
}

/// Under the archive loop the page span nests inside `eventsdb.archive`,
/// which keeps its totals: the parent says how far the loop got, the child
/// says what one page was.
#[tokio::test]
async fn the_export_span_nests_under_the_archive_span() {
    let (captured, _guard) = capturing();
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut orders = log.stream_handle("order-1");
    for kind in ["placed", "paid", "shipped", "returned", "refunded"] {
        orders.append(event(kind)).await.unwrap();
    }

    let mut sink = JsonLinesSink::new(Vec::new());
    log.archive_then_retain(Plan::Before(Position::new(4)), &mut sink, 2)
        .await
        .unwrap();

    let archive = captured.spans("eventsdb.archive");
    assert_eq!(archive.len(), 1);
    assert_eq!(archive[0].field("pages"), Some("2"));
    assert_eq!(archive[0].field("exported"), Some("4"));

    let export = captured.spans("eventsdb.export");
    assert_eq!(export.len(), 2, "one page span per page the loop took");
    for page in &export {
        assert_eq!(page.parent.as_deref(), Some("eventsdb.archive"));
        assert_eq!(page.field("exported"), Some("2"));
        assert_eq!(page.field("whole"), Some("true"));
    }
}
