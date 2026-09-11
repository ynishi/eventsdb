//! `archive_then_retain`: the export-confirm-remove loop, run by the store.
//!
//! The property under test is the order and what a failure leaves behind.
//! Export the page, hand it to the sink, confirm only if the sink returned,
//! remove only what confirmed pages cover — so a sink that fails removes
//! nothing and a second call resumes from the chain rather than from the
//! beginning. The rest is the shapes around that: the two sinks, the guard
//! that still refuses to overrun a consumer, and the over-export a scattered
//! plan needs to keep the chain a prefix.

use async_trait::async_trait;
use eventsdb_core::error::{Error, Result};
use eventsdb_core::transfer::ExportedEvent;
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{Guard, JsonLinesSink, LogSink, Plan, Sink, SqliteEventLog};
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

async fn seeded(n: usize) -> SqliteEventLog {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append_many((0..n).map(|_| event("e")).collect())
        .await
        .unwrap();
    log
}

async fn positions(log: &SqliteEventLog) -> Vec<u64> {
    log.read_all(Position::BEGINNING, &Filter::all(), usize::MAX)
        .await
        .unwrap()
        .iter()
        .map(|recorded| recorded.position.get())
        .collect()
}

async fn count(log: &SqliteEventLog) -> usize {
    positions(log).await.len()
}

/// The `plan` column of the retention ledger, oldest first.
async fn ledger(log: &SqliteEventLog) -> Vec<String> {
    log.query("SELECT plan FROM retention ORDER BY id", Vec::new())
        .await
        .unwrap()
        .iter()
        .map(|row| row["plan"].as_str().unwrap().to_string())
        .collect()
}

/// The receipts, oldest first, as `(from, through, landed)`.
async fn receipts(log: &SqliteEventLog) -> Vec<(u64, u64, bool)> {
    log.query(
        "SELECT from_position, through, landed_ms FROM exports ORDER BY id",
        Vec::new(),
    )
    .await
    .unwrap()
    .iter()
    .map(|row| {
        (
            row["from_position"].as_u64().unwrap(),
            row["through"].as_u64().unwrap(),
            !row["landed_ms"].is_null(),
        )
    })
    .collect()
}

/// JSON Lines back into records, the way a reader of the archive would.
fn read_back(bytes: &[u8]) -> Vec<ExportedEvent> {
    std::str::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|line| ExportedEvent::from_json(serde_json::from_str(line).unwrap()).unwrap())
        .collect()
}

fn witnesses(records: &[ExportedEvent]) -> Vec<u64> {
    records
        .iter()
        .map(|record| record.position.unwrap().get())
        .collect()
}

/// A sink that keeps what it was shown, and can be told to fail on the *n*th
/// page rather than take it.
#[derive(Debug, Default)]
struct Recorder {
    pages: usize,
    fail_on_page: Option<usize>,
    received: Vec<ExportedEvent>,
}

impl Recorder {
    fn failing_on_page(page: usize) -> Self {
        Recorder {
            fail_on_page: Some(page),
            ..Recorder::default()
        }
    }
}

#[async_trait]
impl Sink for Recorder {
    async fn write(&mut self, page: &[ExportedEvent]) -> Result<()> {
        self.pages += 1;
        if self.fail_on_page == Some(self.pages) {
            return Err(Error::storage("the archive refused the page"));
        }
        self.received.extend_from_slice(page);
        Ok(())
    }
}

/// The whole of the README's seven-line loop in one call: every event the
/// plan reaches is in the archive as JSON Lines, the plan was applied, and
/// the ledger says an export came first.
#[tokio::test]
async fn one_call_writes_the_pages_out_and_then_removes_them() {
    let log = seeded(7).await;
    let mut sink = JsonLinesSink::new(Vec::new());

    let report = log
        .archive_then_retain(Plan::Before(Position::new(6)), &mut sink, 3)
        .await
        .unwrap();

    let archived = read_back(&sink.into_inner());
    assert_eq!(
        witnesses(&archived),
        vec![1, 2, 3, 4, 5, 6],
        "exactly the events the plan reaches, in order, one per line"
    );

    assert_eq!(report.removed, 6);
    assert_eq!(report.highest_removed, Some(Position::new(6)));
    assert_eq!(positions(&log).await, vec![7]);
    assert_eq!(log.exported_through().await.unwrap(), Position::new(6));

    let ledger = ledger(&log).await;
    assert_eq!(ledger, vec!["archive-then-remove: before:6".to_string()]);
    assert!(ledger[0].starts_with("archive-then-remove:"));
}

/// A plain `retain` still writes what it always wrote. The archive's marker
/// is a second rendering of the same plan, not a change to the first.
#[tokio::test]
async fn a_plain_retain_is_still_described_without_the_marker() {
    let log = seeded(3).await;
    log.retain(Plan::Before(Position::new(2)), Guard::Force)
        .await
        .unwrap();
    assert_eq!(ledger(&log).await, vec!["before:2".to_string()]);
}

/// The failure the order exists for. The sink refuses the second page, and
/// the call returns that error with nothing removed: the first page is
/// confirmed and the second is a receipt that was taken and never landed.
#[tokio::test]
async fn a_sink_that_fails_mid_loop_removes_nothing_and_leaves_its_receipt_open() {
    let log = seeded(7).await;
    let mut sink = Recorder::failing_on_page(2);

    let error = log
        .archive_then_retain(Plan::Before(Position::new(6)), &mut sink, 3)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Storage(_)), "{error}");
    assert!(error.to_string().contains("refused the page"), "{error}");

    assert_eq!(count(&log).await, 7, "nothing was removed");
    assert!(ledger(&log).await.is_empty(), "the plan was never applied");
    assert_eq!(witnesses(&sink.received), vec![1, 2, 3]);
    assert_eq!(
        log.exported_through().await.unwrap(),
        Position::new(3),
        "the chain is where the confirmed page left it"
    );
    assert_eq!(
        receipts(&log).await,
        vec![(0, 3, true), (3, 6, false)],
        "the second page was taken and never landed"
    );
}

/// The resume. A second call starts from `exported_through`, so the page the
/// first call confirmed is not written to the second sink.
#[tokio::test]
async fn a_second_call_resumes_from_the_chain_rather_than_the_beginning() {
    let log = seeded(7).await;
    let mut broken = Recorder::failing_on_page(2);
    log.archive_then_retain(Plan::Before(Position::new(6)), &mut broken, 3)
        .await
        .unwrap_err();

    let mut working = Recorder::default();
    let report = log
        .archive_then_retain(Plan::Before(Position::new(6)), &mut working, 3)
        .await
        .unwrap();

    assert_eq!(
        witnesses(&working.received),
        vec![4, 5, 6],
        "the confirmed page is not exported a second time"
    );
    assert_eq!(report.removed, 6);
    assert_eq!(positions(&log).await, vec![7]);
    assert_eq!(log.exported_through().await.unwrap(), Position::new(6));
}

/// "Archive into another eventsdb file" as one line. The receiving log's own
/// report is kept rather than dropped, so the copy can be checked to be the
/// same log rather than merely the same events.
#[tokio::test]
async fn the_log_sink_moves_the_prefix_into_another_log() {
    let source = seeded(6).await;
    let target = SqliteEventLog::open_in_memory().await.unwrap();
    let mut sink = LogSink::new(&target);

    let report = source
        .archive_then_retain(Plan::Before(Position::new(6)), &mut sink, 2)
        .await
        .unwrap();

    assert_eq!(sink.reports().len(), 3, "one report per page");
    assert_eq!(sink.imported(), 6);
    assert!(
        sink.reproduced_coordinates(),
        "an in-order import into an empty target is the same log"
    );

    assert_eq!(report.removed, 6);
    assert!(positions(&source).await.is_empty());
    assert_eq!(positions(&target).await, vec![1, 2, 3, 4, 5, 6]);
}

/// The guard is the strict one and stays it: a consumer that has not caught
/// up fails the call. The export still happened and is still confirmed —
/// the bytes are not thrown away because the removal was refused.
#[tokio::test]
async fn a_consumer_behind_the_plan_refuses_the_removal_after_the_export_landed() {
    let log = seeded(6).await;
    log.checkpoint_save("slow", Position::new(2)).await.unwrap();
    let mut sink = JsonLinesSink::new(Vec::new());

    let error = log
        .archive_then_retain(Plan::Before(Position::new(6)), &mut sink, 3)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::ConsumerBehind { .. }), "{error}");

    assert_eq!(count(&log).await, 6, "nothing was removed");
    assert_eq!(
        log.exported_through().await.unwrap(),
        Position::new(6),
        "the export is not wasted: the chain advanced"
    );
    assert_eq!(witnesses(&read_back(&sink.into_inner())).len(), 6);
}

/// A stream plan removes a scattered set while the chain is a prefix, so the
/// export runs through the highest position the plan touches and hands the
/// sink the events that stay as well. Over-exporting is the price of the
/// chain having no holes; the removal is still exactly the plan's set.
#[tokio::test]
async fn a_stream_plan_exports_through_its_highest_position_and_removes_only_its_own() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut keep = log.stream_handle("keep");
    let mut drop = log.stream_handle("drop");
    keep.append(event("a")).await.unwrap(); // 1
    drop.append(event("a")).await.unwrap(); // 2
    keep.append(event("a")).await.unwrap(); // 3
    drop.append(event("a")).await.unwrap(); // 4
    keep.append(event("a")).await.unwrap(); // 5

    let mut sink = Recorder::default();
    let report = log
        .archive_then_retain(Plan::Streams(vec!["drop".to_string()]), &mut sink, 2)
        .await
        .unwrap();

    assert_eq!(
        witnesses(&sink.received),
        vec![1, 2, 3, 4],
        "a prefix through the highest position the plan touches, gaps included"
    );
    assert_eq!(log.exported_through().await.unwrap(), Position::new(4));
    assert_eq!(report.removed, 2);
    assert_eq!(report.highest_removed, Some(Position::new(4)));
    assert_eq!(positions(&log).await, vec![1, 3, 5]);
    assert_eq!(
        ledger(&log).await,
        vec!["archive-then-remove: streams:1".to_string()]
    );
}

/// The same for an age plan on a log that has backfilled history: the old
/// event sits at a high position, so the plan is not a prefix and the export
/// runs past events it will not remove.
#[tokio::test]
async fn an_age_plan_on_backfilled_history_over_exports_the_same_way() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("now")).await.unwrap(); // 1
    s.append(event("now")).await.unwrap(); // 2

    // A backfill: the same stored shape, carrying a time coordinate from
    // long before the events it lands after.
    let mut old = log
        .export(Position::BEGINNING, &Filter::all(), 1)
        .await
        .unwrap()
        .remove(0);
    old.stream = "backfill".to_string();
    old.position = None;
    old.event.insert("epoch_ms".to_string(), json!(1_000u64));
    log.import(vec![old]).await.unwrap(); // 3

    s.append(event("now")).await.unwrap(); // 4

    let mut sink = Recorder::default();
    let report = log
        .archive_then_retain(Plan::OlderThan(2_000), &mut sink, 2)
        .await
        .unwrap();

    assert_eq!(witnesses(&sink.received), vec![1, 2, 3, 4]);
    assert_eq!(log.exported_through().await.unwrap(), Position::new(4));
    assert_eq!(report.removed, 1);
    assert_eq!(report.highest_removed, Some(Position::new(3)));
    assert_eq!(positions(&log).await, vec![1, 2, 4]);
    assert_eq!(
        ledger(&log).await,
        vec!["archive-then-remove: older_than:2000".to_string()]
    );
}

/// A plan that matches nothing is nothing to preserve and nothing to remove.
/// The sink is not called at all — an empty page is a write the caller would
/// have to interpret.
#[tokio::test]
async fn a_plan_that_removes_nothing_writes_nothing() {
    let log = seeded(4).await;
    let mut sink = Recorder::default();

    let report = log
        .archive_then_retain(
            Plan::Streams(vec!["never-existed".to_string()]),
            &mut sink,
            2,
        )
        .await
        .unwrap();

    assert_eq!(report, eventsdb_sqlite::Report::nothing());
    assert_eq!(sink.pages, 0);
    assert_eq!(count(&log).await, 4);
    assert!(receipts(&log).await.is_empty(), "no page was ever taken");
    assert!(ledger(&log).await.is_empty());
}

/// A chain an earlier loop already built is a chain: the pages under it are
/// not exported again, and the archive picks up where they stopped.
#[tokio::test]
async fn pages_already_confirmed_are_not_exported_again() {
    let log = seeded(6).await;
    let (_, receipt) = log
        .export_recorded(Position::BEGINNING, &Filter::all(), 3)
        .await
        .unwrap();
    log.confirm_export(receipt.id).await.unwrap();
    assert_eq!(log.exported_through().await.unwrap(), Position::new(3));

    let mut sink = Recorder::default();
    let report = log
        .archive_then_retain(Plan::Before(Position::new(6)), &mut sink, 3)
        .await
        .unwrap();

    assert_eq!(witnesses(&sink.received), vec![4, 5, 6]);
    assert_eq!(sink.pages, 1);
    assert_eq!(report.removed, 6);
    assert!(positions(&log).await.is_empty());
}

/// A page size of nothing is refused rather than quietly exporting nothing
/// and failing the guard a step later.
#[tokio::test]
async fn a_page_size_of_zero_is_a_validation_error() {
    let log = seeded(2).await;
    let mut sink = Recorder::default();

    let error = log
        .archive_then_retain(Plan::Before(Position::new(2)), &mut sink, 0)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Validation(_)), "{error}");
    assert_eq!(count(&log).await, 2);
}

/// The sink is taken as `&mut (impl Sink + ?Sized)`, so a caller that chose
/// its destination at run time passes `&mut dyn Sink` and the call is the
/// same one.
#[tokio::test]
async fn the_sink_may_be_a_trait_object() {
    let log = seeded(3).await;
    let mut sink: Box<dyn Sink> = Box::new(JsonLinesSink::new(Vec::new()));

    let report = log
        .archive_then_retain(Plan::Before(Position::new(3)), sink.as_mut(), 2)
        .await
        .unwrap();

    assert_eq!(report.removed, 3);
    assert!(positions(&log).await.is_empty());
}
