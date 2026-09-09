//! Moving a log, and getting the same log back.
//!
//! The point of these is the word *same*. It is easy to move events and end up
//! with a log that holds the same facts under different coordinates, or one
//! whose old events have quietly been relabelled as current — both of which
//! look like success. Each test here pins one part of "identical".

use std::sync::Arc;

use eventsdb_core::upcast::{UpcastChain, Upcaster};
use eventsdb_core::{EventLog, EventStore, ExportedEvent, Filter, Position};
use eventsdb_sqlite::{Guard, OpenOptions, Plan, SqliteEventLog};
use serde_json::{json, Map, Value};

fn event(kind: &str, n: i64) -> Map<String, Value> {
    json!({ "kind": kind, "meta": { "n": n }, "data": { "payload": "x" } })
        .as_object()
        .unwrap()
        .clone()
}

/// Export the whole log by paging, the way a caller would.
async fn export_all(log: &SqliteEventLog) -> Vec<ExportedEvent> {
    let mut out = Vec::new();
    let mut cursor = Position::BEGINNING;
    loop {
        let batch = log.export(cursor, &Filter::all(), 3).await.unwrap();
        let full = batch.len() == 3;
        if let Some(last) = batch.last() {
            cursor = last.position;
        }
        out.extend(batch);
        if !full {
            return out;
        }
    }
}

async fn seeded(streams: &[(&str, usize)]) -> SqliteEventLog {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    for (stream, count) in streams {
        let mut handle = log.stream_handle(stream);
        for i in 0..*count {
            handle.append(event("noted", i as i64)).await.unwrap();
        }
    }
    log
}

/// The whole claim, in one test: export a log, import it into an empty one,
/// and the two are the same log — same events, same streams, same sequence
/// numbers, same positions, same times.
#[tokio::test]
async fn a_log_exported_and_imported_is_the_same_log() {
    let source = seeded(&[("a", 4), ("b", 3), ("c", 1)]).await;
    let before = export_all(&source).await;
    assert_eq!(before.len(), 8);

    let target = SqliteEventLog::open_in_memory().await.unwrap();
    let report = target.import(before.clone()).await.unwrap();

    assert_eq!(report.imported, 8);
    assert!(
        report.reproduced_coordinates,
        "an in-order import into an empty store lands on the same positions"
    );
    assert_eq!(report.first, Some(Position::new(1)));
    assert_eq!(report.last, Some(Position::new(8)));

    let after = export_all(&target).await;
    assert_eq!(after, before, "byte for byte, including every stored field");
}

/// The one that would be easiest to get wrong and hardest to notice: an old
/// event re-stamped as current falls out of reach of the upcaster written for
/// it, and is then read as a shape it never had.
#[tokio::test]
async fn an_imported_event_keeps_the_schema_version_it_was_written_under() {
    struct RenameV1 {
        seen: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Upcaster for RenameV1 {
        fn upcast(&self, mut event: Value) -> Value {
            if event["_schema_version"] == json!(1) && event["kind"] == json!("old_name") {
                self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                event["kind"] = json!("new_name");
            }
            event
        }
    }

    // A log holding an event written under version 1.
    let source = SqliteEventLog::open_in_memory().await.unwrap();
    source
        .stream_handle("s")
        .append(event("old_name", 1))
        .await
        .unwrap();
    let exported = export_all(&source).await;
    assert_eq!(exported[0].event["_schema_version"], json!(1));

    let target = SqliteEventLog::open_in_memory().await.unwrap();
    target.import(exported).await.unwrap();

    // The version travelled, so the chain still recognises it.
    let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let reader = SqliteEventLog::open_in_memory_with(OpenOptions {
        upcasters: {
            let chain: UpcastChain = vec![Arc::new(RenameV1 {
                seen: Arc::clone(&seen),
            })];
            chain
        },
        ..OpenOptions::default()
    })
    .await
    .unwrap();
    reader.import(export_all(&target).await).await.unwrap();

    let read = reader
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(read[0].kind(), "new_name");
    assert_eq!(
        seen.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the upcaster matched on the version the event kept"
    );
}

/// An export gives the stored bytes, not this build's reading of them —
/// otherwise a copy bakes in the current chain and the original is gone.
#[tokio::test]
async fn an_export_is_not_upcasted() {
    struct Rename;
    impl Upcaster for Rename {
        fn upcast(&self, mut event: Value) -> Value {
            event["kind"] = json!("rewritten");
            event
        }
    }

    let chain: UpcastChain = vec![Arc::new(Rename)];
    let log = SqliteEventLog::open_in_memory_with(OpenOptions {
        upcasters: chain,
        ..OpenOptions::default()
    })
    .await
    .unwrap();
    log.stream_handle("s")
        .append(event("as_written", 1))
        .await
        .unwrap();

    // The ordinary read shows the chain's answer.
    let read = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(read[0].kind(), "rewritten");

    // The export shows what is actually on disk.
    let exported = export_all(&log).await;
    assert_eq!(exported[0].event["kind"], json!("as_written"));
}

/// A record is one JSON object, so a file of them is JSON Lines.
#[tokio::test]
async fn a_record_round_trips_through_json() {
    let log = seeded(&[("s", 2)]).await;
    let exported = export_all(&log).await;

    let lines: Vec<String> = exported.iter().map(|e| e.to_json().to_string()).collect();
    let parsed: Vec<ExportedEvent> = lines
        .iter()
        .map(|line| ExportedEvent::from_json(serde_json::from_str(line).unwrap()).unwrap())
        .collect();

    assert_eq!(parsed, exported);

    let target = SqliteEventLog::open_in_memory().await.unwrap();
    let report = target.import(parsed).await.unwrap();
    assert!(report.reproduced_coordinates);
    assert_eq!(export_all(&target).await, exported);
}

/// Merging into a store that already has history renumbers, and the report
/// says so rather than letting a caller assume a faithful copy.
#[tokio::test]
async fn a_merge_renumbers_and_reports_that_it_did() {
    let source = seeded(&[("a", 2)]).await;
    let exported = export_all(&source).await;

    let target = seeded(&[("existing", 1)]).await;
    let report = target.import(exported).await.unwrap();

    assert_eq!(report.imported, 2);
    assert!(
        !report.reproduced_coordinates,
        "the copy holds the same events at different coordinates, and says so"
    );
    assert_eq!(report.first, Some(Position::new(2)));

    // The times still travelled, even though the positions did not.
    let all = target
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(all.len(), 3);
}

/// A failed import leaves nothing behind, so a retry starts from a known
/// place rather than from wherever it stopped.
#[tokio::test]
async fn a_failed_import_is_all_or_nothing() {
    let source = seeded(&[("a", 3)]).await;
    let mut exported = export_all(&source).await;

    // Corrupt the last record: a stored event must carry its schema version.
    exported[2].event.remove("_schema_version");

    let target = SqliteEventLog::open_in_memory().await.unwrap();
    let error = target.import(exported).await.unwrap_err();
    assert!(error.to_string().contains("_schema_version"), "got {error}");

    assert!(
        target
            .read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .is_empty(),
        "the two good records rolled back with the bad one"
    );
}

#[tokio::test]
async fn an_export_can_be_filtered_and_still_imports() {
    let source = SqliteEventLog::open_in_memory().await.unwrap();
    let mut a = source.stream_handle("keep");
    let mut b = source.stream_handle("drop");
    a.append(event("noted", 1)).await.unwrap();
    b.append(event("noted", 2)).await.unwrap();
    a.append(event("noted", 3)).await.unwrap();

    let exported = source
        .export(Position::BEGINNING, &Filter::all().stream("keep"), 100)
        .await
        .unwrap();
    assert_eq!(exported.len(), 2);

    let target = SqliteEventLog::open_in_memory().await.unwrap();
    let report = target.import(exported).await.unwrap();
    assert_eq!(report.imported, 2);
    assert!(
        !report.reproduced_coordinates,
        "a partial export cannot land on its old positions, and says so"
    );

    // Sequence numbers within the stream are contiguous in the copy.
    let read = target
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(read.iter().map(|r| r.seq()).collect::<Vec<_>>(), vec![1, 2]);
}

/// The migration shape end to end: retention removes old history from the
/// live log, and an export taken first is what preserves it.
#[tokio::test]
async fn an_export_taken_before_retention_outlives_it() {
    let log = seeded(&[("s", 5)]).await;
    let archive = export_all(&log).await;

    log.retain(Plan::Before(Position::new(3)), Guard::Force)
        .await
        .unwrap();
    assert_eq!(
        log.read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .len(),
        2
    );

    // The archive still holds all five, and restores as the same log.
    let restored = SqliteEventLog::open_in_memory().await.unwrap();
    let report = restored.import(archive.clone()).await.unwrap();
    assert_eq!(report.imported, 5);
    assert!(report.reproduced_coordinates);
    assert_eq!(export_all(&restored).await, archive);
}
