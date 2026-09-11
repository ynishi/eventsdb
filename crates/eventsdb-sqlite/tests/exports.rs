//! Export receipts and `Guard::Exported`.
//!
//! The property under test: retention under `Guard::Exported` removes only
//! what a confirmed, whole export chain covers from the beginning of the
//! log. Everything else here is the shape of that chain — a page taken but
//! not confirmed, a filtered page, a gap — and that the receipts table is as
//! reserved as the retention ledger.

use eventsdb_core::error::Error;
use eventsdb_core::transfer::ExportedEvent;
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{ExportReceipt, Guard, Plan, SqliteEventLog};
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

async fn seeded(n: usize) -> SqliteEventLog {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append_many(
        (0..n)
            .map(|i| event(if i % 2 == 0 { "even" } else { "odd" }))
            .collect(),
    )
    .await
    .unwrap();
    log
}

/// Page the whole log out with receipts, the way a caller would, returning
/// the pages and their receipts in order. Confirms nothing.
async fn export_paged(
    log: &SqliteEventLog,
    page: usize,
) -> Vec<(Vec<ExportedEvent>, ExportReceipt)> {
    let mut out = Vec::new();
    let mut cursor = Position::BEGINNING;
    loop {
        let (events, receipt) = log
            .export_recorded(cursor, &Filter::all(), page)
            .await
            .unwrap();
        let short = events.len() < page;
        cursor = receipt.through;
        out.push((events, receipt));
        if short {
            return out;
        }
    }
}

async fn count(log: &SqliteEventLog) -> usize {
    log.read_all(Position::BEGINNING, &Filter::all(), usize::MAX)
        .await
        .unwrap()
        .len()
}

/// The round trip this exists for: page out with receipts, confirm each
/// page, and the reach follows the confirmations — then retention under the
/// guard removes exactly what is covered.
#[tokio::test]
async fn confirmed_pages_chain_into_a_reach_and_retention_may_remove_up_to_it() {
    let log = seeded(7).await;
    let pages = export_paged(&log, 3).await;

    // 3 + 3 + 1: the receipts are exclusive on `from` and chain by `through`.
    let ranges: Vec<(u64, u64, usize)> = pages
        .iter()
        .map(|(_, r)| (r.from.get(), r.through.get(), r.count))
        .collect();
    assert_eq!(ranges, vec![(0, 3, 3), (3, 6, 3), (6, 7, 1)]);
    assert!(pages.iter().all(|(_, r)| r.whole));

    // Taken is not landed: nothing reaches yet.
    assert_eq!(log.exported_through().await.unwrap(), Position::BEGINNING);

    log.confirm_export(pages[0].1.id).await.unwrap();
    assert_eq!(log.exported_through().await.unwrap(), Position::new(3));
    log.confirm_export(pages[1].1.id).await.unwrap();
    log.confirm_export(pages[2].1.id).await.unwrap();
    assert_eq!(log.exported_through().await.unwrap(), Position::new(7));

    let report = log
        .retain(Plan::Before(Position::new(6)), Guard::Exported)
        .await
        .unwrap();
    assert_eq!(report.removed, 6);
    assert_eq!(count(&log).await, 1);
}

/// A page that was handed out and never confirmed may or may not exist
/// anywhere. The guard refuses, and refusing means nothing happened: no
/// rows gone, no ledger entry.
#[tokio::test]
async fn an_unconfirmed_export_does_not_let_retention_through() {
    let log = seeded(5).await;
    let pages = export_paged(&log, 10).await;
    assert_eq!(pages.len(), 1);

    let error = log
        .retain(Plan::Before(Position::new(5)), Guard::Exported)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::NotExported {
                up_to: 5,
                exported_through: 0
            }
        ),
        "{error}"
    );
    assert_eq!(count(&log).await, 5, "nothing was removed");
    assert_eq!(log.removed_watermark().await.unwrap(), Position::BEGINNING);
    let ledger = log
        .query("SELECT COUNT(*) AS n FROM retention", Vec::<Value>::new())
        .await
        .unwrap();
    assert_eq!(ledger[0]["n"], json!(0), "a refusal writes no ledger row");
}

/// The guard is about the plan's reach, not its shape: a plan that stays
/// within the chain goes through, one that would step past it does not,
/// and the error says where the chain ends.
#[tokio::test]
async fn the_plan_may_reach_the_chain_but_not_past_it() {
    let log = seeded(7).await;
    let pages = export_paged(&log, 3).await;
    log.confirm_export(pages[0].1.id).await.unwrap();

    let error = log
        .retain(Plan::Before(Position::new(4)), Guard::Exported)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::NotExported {
                up_to: 4,
                exported_through: 3
            }
        ),
        "{error}"
    );

    let report = log
        .retain(Plan::Before(Position::new(3)), Guard::Exported)
        .await
        .unwrap();
    assert_eq!(report.removed, 3);
    assert_eq!(count(&log).await, 4);
}

/// A filtered export preserved part of its range and cannot vouch for the
/// rest, so it does not extend the chain however faithfully it is confirmed.
#[tokio::test]
async fn a_filtered_export_does_not_count() {
    let log = seeded(6).await;
    let (evens, receipt) = log
        .export_recorded(Position::BEGINNING, &Filter::kinds(["even"]), 100)
        .await
        .unwrap();
    assert_eq!(evens.len(), 3);
    assert!(!receipt.whole);
    assert_eq!(receipt.through, Position::new(5), "the last even event");

    log.confirm_export(receipt.id).await.unwrap();
    assert_eq!(log.exported_through().await.unwrap(), Position::BEGINNING);

    let error = log
        .retain(Plan::Before(Position::new(2)), Guard::Exported)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::NotExported { .. }), "{error}");
}

/// The chain stops at the first page that did not land. What lies beyond may
/// well be preserved, but not contiguously with what came before, and a
/// history with a hole in it is what this exists to refuse.
#[tokio::test]
async fn a_gap_in_the_chain_stops_the_reach_at_the_gap() {
    // 3 + 3 + 2: the last page is short, so the loop takes exactly three.
    let log = seeded(8).await;
    let pages = export_paged(&log, 3).await;
    assert_eq!(pages.len(), 3);

    log.confirm_export(pages[0].1.id).await.unwrap();
    // pages[1] is never confirmed.
    log.confirm_export(pages[2].1.id).await.unwrap();

    assert_eq!(log.exported_through().await.unwrap(), Position::new(3));

    let error = log
        .retain(Plan::Before(Position::new(8)), Guard::Exported)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            Error::NotExported {
                up_to: 8,
                exported_through: 3
            }
        ),
        "{error}"
    );
}

/// `Exported` is the strict guard: it also refuses to overrun a consumer,
/// the way the default does.
#[tokio::test]
async fn exported_also_refuses_to_overrun_a_registered_consumer() {
    let log = seeded(4).await;
    let pages = export_paged(&log, 10).await;
    log.confirm_export(pages[0].1.id).await.unwrap();
    log.checkpoint_save("slow", Position::new(1)).await.unwrap();

    let error = log
        .retain(Plan::Before(Position::new(4)), Guard::Exported)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::ConsumerBehind { .. }), "{error}");
    assert_eq!(count(&log).await, 4);
}

/// Confirming is idempotent, and confirming a receipt nobody was given is a
/// mistake rather than a no-op.
#[tokio::test]
async fn confirming_twice_is_once_and_an_unknown_receipt_is_refused() {
    let log = seeded(2).await;
    let (_, receipt) = log
        .export_recorded(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();

    log.confirm_export(receipt.id).await.unwrap();
    log.confirm_export(receipt.id).await.unwrap();
    assert_eq!(log.exported_through().await.unwrap(), Position::new(2));

    let error = log.confirm_export(receipt.id + 100).await.unwrap_err();
    assert!(matches!(error, Error::Validation(_)), "{error}");
    assert!(error.to_string().contains("no export receipt"), "{error}");
}

/// An empty page is a receipt that reaches nowhere. Taking one is not an
/// error — it is how a paging loop learns it is done — and confirming it
/// changes nothing.
#[tokio::test]
async fn an_empty_page_reaches_nowhere() {
    let log = seeded(0).await;
    let (events, receipt) = log
        .export_recorded(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert!(events.is_empty());
    assert_eq!(
        (receipt.from, receipt.through, receipt.count),
        (Position::BEGINNING, Position::BEGINNING, 0)
    );
    log.confirm_export(receipt.id).await.unwrap();
    assert_eq!(log.exported_through().await.unwrap(), Position::BEGINNING);
}

/// The receipts are bookkeeping the store vouches for, so the hatch refuses
/// to write them the way it refuses the ledger. Reading them is fine.
#[tokio::test]
async fn the_hatch_can_read_receipts_but_not_write_them() {
    let log = seeded(2).await;
    let (_, receipt) = log
        .export_recorded(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();

    let rows = log
        .query(
            "SELECT id, from_position, through, count, whole, landed_ms FROM exports",
            Vec::<Value>::new(),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], json!(receipt.id));
    assert_eq!(rows[0]["landed_ms"], Value::Null);

    let error = log
        .with_transaction(|tx| {
            tx.execute_batch("UPDATE exports SET landed_ms = 1")
                .map_err(|e| Error::storage(e.to_string()))
        })
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(_)), "{error}");
    assert_eq!(
        log.exported_through().await.unwrap(),
        Position::BEGINNING,
        "the forged confirmation did not land"
    );
}
