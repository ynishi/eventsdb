//! The physical copy: `backup_to`.
//!
//! `export` is the logical copy and carries the events. This is the file, and
//! what these tests are for is the difference — the read models, the reserved
//! tables and the pragmas that `export` leaves behind, and the fact that a
//! writer holding the lock neither blocks the copy nor gets into it.

use std::sync::Arc;
use std::time::Duration;

use eventsdb_core::error::{Error, Result};
use eventsdb_core::position::Recorded;
use eventsdb_core::store::Expected;
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{Guard, Plan, Projection, SqliteEventLog, Transaction, TARGET_USER_VERSION};
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

fn scored(n: i64) -> Map<String, Value> {
    json!({ "kind": "scored", "data": { "n": n } })
        .as_object()
        .unwrap()
        .clone()
}

/// About 300ms of work, run while a write transaction is open. The same
/// statement `tests/readers.rs` holds a write open with.
const SLOW: &str = "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c \
                    WHERE x < 12000000) SELECT count(*) FROM c";

/// Sums `data.n` per stream, so there is a read-model table to travel.
struct Totals;

impl Projection for Totals {
    fn name(&self) -> &str {
        "totals"
    }

    fn kinds(&self) -> Option<Vec<String>> {
        Some(vec!["scored".to_string()])
    }

    fn init(&mut self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute_batch(
            "CREATE TABLE IF NOT EXISTS totals (
                 stream TEXT PRIMARY KEY,
                 total  INTEGER NOT NULL
             )",
        )
        .map_err(|e| Error::storage(e.to_string()))
    }

    fn reset(&mut self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute_batch("DROP TABLE IF EXISTS totals")
            .map_err(|e| Error::storage(e.to_string()))
    }

    fn apply(&mut self, tx: &Transaction<'_>, event: &Recorded) -> Result<()> {
        let n = event.event["data"]["n"].as_i64().unwrap_or(0);
        tx.execute(
            "INSERT INTO totals (stream, total) VALUES (?1, ?2) \
             ON CONFLICT(stream) DO UPDATE SET total = total + excluded.total",
            [
                Box::new(event.stream.clone()) as Box<dyn rusqlite::ToSql>,
                Box::new(n),
            ],
        )
        .map(|_| ())
        .map_err(|e| Error::storage(e.to_string()))
    }
}

/// Read one integer pragma off a log, through the hatch.
async fn pragma(log: &SqliteEventLog, name: &str) -> i64 {
    let rows = log
        .query(&format!("PRAGMA {name}"), Vec::new())
        .await
        .unwrap_or_else(|error| panic!("reading `PRAGMA {name}`: {error}"));
    rows.first()
        .and_then(|row| row.values().next())
        .and_then(Value::as_i64)
        .unwrap_or_else(|| panic!("`PRAGMA {name}` answered {rows:?}"))
}

/// The question the issue is about: a copy taken while somebody is writing.
///
/// The write is held open the way `tests/readers.rs` holds one — a slow
/// statement inside `with_transaction`, which keeps SQLite's write lock for
/// its duration. The backup runs on a reader, so it neither waits for that
/// transaction nor refuses because of it: it holds what had committed when it
/// started, and the event being written lands in the source afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_taken_while_a_write_is_in_flight_holds_what_had_committed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let copy = dir.path().join("copy.db");

    let log = Arc::new(SqliteEventLog::open(&path).await.unwrap());
    let mut s = log.stream_handle("s");
    for kind in ["a", "b", "c"] {
        s.append(event(kind)).await.unwrap();
    }

    let writer = Arc::clone(&log);
    let write = tokio::spawn(async move {
        writer
            .with_transaction(|tx| {
                tx.append("s", event("during"))?;
                tx.execute_batch(SLOW)
                    .map_err(|e| Error::storage(e.to_string()))
            })
            .await
    });

    // Long enough for the transaction to be open and holding the lock.
    tokio::time::sleep(Duration::from_millis(40)).await;
    log.backup_to(&copy).await.expect("a backup during a write");

    write.await.unwrap().unwrap();
    assert_eq!(
        log.read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .len(),
        4,
        "the fourth event committed to the source after the copy was taken"
    );
    log.shutdown().await.unwrap();

    let restored = SqliteEventLog::open(&copy).await.unwrap();
    assert_eq!(
        pragma(&restored, "user_version").await,
        TARGET_USER_VERSION,
        "the copy opened without the ladder running a step"
    );

    let events = restored
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    let seen: Vec<(u64, &str)> = events
        .iter()
        .map(|r| (r.position.get(), r.event["kind"].as_str().unwrap()))
        .collect();
    assert_eq!(
        seen,
        vec![(1, "a"), (2, "b"), (3, "c")],
        "exactly what had committed, at the positions it had"
    );
    restored.shutdown().await.unwrap();
}

/// What makes this a backup rather than an export: everything `export` leaves
/// behind comes too.
#[tokio::test]
async fn the_read_models_and_the_reserved_tables_travel() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let copy = dir.path().join("copy.db");

    let log = SqliteEventLog::open(&path).await.unwrap();
    let mut s = log.stream_handle("player-1");
    for n in 1..=6 {
        s.append(scored(n)).await.unwrap();
    }

    // A read model.
    let mut runner = log.runner_now(Totals);
    runner.init().await.unwrap();
    assert_eq!(runner.catch_up().await.unwrap(), 6);

    // A consumer checkpoint.
    log.checkpoint_save("reporting", Position::new(4))
        .await
        .unwrap();

    // A retention ledger row.
    log.retain(Plan::Before(Position::new(2)), Guard::Force)
        .await
        .unwrap();

    // An export receipt.
    let (_, receipt) = log
        .export_recorded(Position::BEGINNING, &Filter::all(), 100)
        .await
        .unwrap();

    let ledger = log.retention_ledger(None, 10).await.unwrap();
    let receipts = log.export_receipts(None, 10).await.unwrap();
    assert_eq!(ledger.len(), 1);
    assert_eq!(receipts.len(), 1);

    log.backup_to(&copy).await.unwrap();
    log.shutdown().await.unwrap();

    let restored = SqliteEventLog::open(&copy).await.unwrap();

    let totals = restored
        .query(
            "SELECT stream, total FROM totals ORDER BY stream",
            Vec::new(),
        )
        .await
        .expect("the read model's table is in the copy");
    assert_eq!(totals.len(), 1);
    assert_eq!(totals[0]["stream"], json!("player-1"));
    assert_eq!(totals[0]["total"], json!(21));

    assert_eq!(
        restored.checkpoint_load("reporting").await.unwrap(),
        Position::new(4)
    );
    assert_eq!(
        restored.checkpoint_load("totals").await.unwrap(),
        Position::new(6),
        "the projection's own cursor, so it resumes rather than rebuilds"
    );
    assert_eq!(restored.retention_ledger(None, 10).await.unwrap(), ledger);
    assert_eq!(restored.export_receipts(None, 10).await.unwrap(), receipts);
    assert_eq!(
        restored.exported_through().await.unwrap(),
        Position::BEGINNING,
        "and the receipt is still unconfirmed on the copy, as it was here"
    );
    assert_eq!(receipt.through, Position::new(6));

    // The watermark travels with the ledger, so a read from before it is
    // still refused on the copy.
    assert_eq!(
        restored.removed_watermark().await.unwrap(),
        Position::new(2)
    );
    restored.shutdown().await.unwrap();
}

/// The copy is a log, not an archive of one: it is appended to, and it carries
/// the counters that say where it was.
#[tokio::test]
async fn the_copy_is_a_working_log() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let copy = dir.path().join("copy.db");

    let log = SqliteEventLog::open(&path).await.unwrap();
    let mut s = log.stream_handle("s");
    for kind in ["a", "b", "c"] {
        s.append(event(kind)).await.unwrap();
    }
    log.backup_to(&copy).await.unwrap();
    log.shutdown().await.unwrap();

    let restored = SqliteEventLog::open(&copy).await.unwrap();
    let mut s = restored.stream_handle("s");

    let committed = s.append(event("d")).await.unwrap();
    assert_eq!(
        committed.position,
        Some(Position::new(4)),
        "the global position continues from where the source was"
    );
    assert_eq!(committed.seq, 4, "and so does the stream's own counter");

    // `stream_seq` travelled, so the stream is not a new one over here.
    let error = s
        .append_expecting(Expected::Unwritten, event("forged"))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::HeadMismatch { .. }),
        "an existing stream is not unwritten on the copy either; got {error}"
    );

    let streams = restored.streams(None, None, 10).await.unwrap();
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].head_seq, 4);
    restored.shutdown().await.unwrap();
}

/// The pragmas the file was created with survive the copy, and the one that
/// matters is `auto_vacuum`.
///
/// SQLite accepts a change to `auto_vacuum` only before the first table
/// exists, so a copy that lost it would be a database where
/// [`SqliteEventLog::reclaim`] is a permanent silent no-op and nothing says
/// so. The second half is what is actually measured: after a removal on the
/// copy, `PRAGMA freelist_count` is above zero and `reclaim()` brings it back
/// down — pages returned to the database rather than held for reuse.
///
/// Not the file size. Under WAL the shrink lands in the `-wal` file until a
/// checkpoint moves it, so a size assertion here would be measuring when
/// SQLite happened to checkpoint. The free-list is the thing reclaim acts on.
#[tokio::test]
async fn the_pragmas_survive_and_reclaim_still_works_on_the_copy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let copy = dir.path().join("copy.db");

    let log = SqliteEventLog::open(&path).await.unwrap();
    let mut s = log.stream_handle("s");
    let filler = "x".repeat(400);
    for n in 0..2000 {
        s.append(
            json!({ "kind": "k", "data": { "n": n, "filler": filler } })
                .as_object()
                .unwrap()
                .clone(),
        )
        .await
        .unwrap();
    }

    let source_auto_vacuum = pragma(&log, "auto_vacuum").await;
    assert_eq!(
        source_auto_vacuum, 2,
        "the source was created with auto_vacuum = INCREMENTAL"
    );

    log.backup_to(&copy).await.unwrap();
    log.shutdown().await.unwrap();

    let restored = SqliteEventLog::open(&copy).await.unwrap();
    assert_eq!(pragma(&restored, "user_version").await, TARGET_USER_VERSION);
    assert_eq!(
        pragma(&restored, "auto_vacuum").await,
        source_auto_vacuum,
        "auto_vacuum came with the pages; it cannot be set after the fact"
    );

    let report = restored
        .retain(Plan::Before(Position::new(1500)), Guard::Force)
        .await
        .unwrap();
    assert_eq!(report.removed, 1500);

    let free_before = pragma(&restored, "freelist_count").await;
    assert!(
        free_before > 0,
        "the removal freed pages and SQLite is holding them"
    );

    restored.reclaim().await.unwrap();

    let free_after = pragma(&restored, "freelist_count").await;
    assert!(
        free_after < free_before,
        "reclaim returned pages on the copy: free list went {free_before} -> {free_after}"
    );
    restored.shutdown().await.unwrap();
}

/// A destination that already holds something is refused, and the thing it
/// holds is still there afterwards.
#[tokio::test]
async fn an_existing_destination_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let copy = dir.path().join("copy.db");

    let log = SqliteEventLog::open(&path).await.unwrap();
    log.stream_handle("s").append(event("a")).await.unwrap();
    log.backup_to(&copy).await.unwrap();

    // A second event, so a copy taken now would differ from the one on disk.
    log.stream_handle("s").append(event("b")).await.unwrap();

    let error = log.backup_to(&copy).await.unwrap_err();
    assert!(
        matches!(error, Error::Validation(_)),
        "an occupied destination is the caller's path, not a storage failure; got {error}"
    );
    assert!(
        error.to_string().contains("already exists"),
        "the message says what is wrong: {error}"
    );

    // Any file at all, not only a database.
    let occupied = dir.path().join("notes.txt");
    std::fs::write(&occupied, b"not a database").unwrap();
    assert!(matches!(
        log.backup_to(&occupied).await.unwrap_err(),
        Error::Validation(_)
    ));
    assert_eq!(std::fs::read(&occupied).unwrap(), b"not a database");

    // A path whose directory is not there is a storage failure, and leaves
    // nothing behind either.
    let nowhere = dir.path().join("no-such-dir").join("copy.db");
    assert!(matches!(
        log.backup_to(&nowhere).await.unwrap_err(),
        Error::Storage(_)
    ));
    assert!(!nowhere.exists());

    log.shutdown().await.unwrap();

    // The refusal left the first backup as it was: one event, not two.
    let restored = SqliteEventLog::open(&copy).await.unwrap();
    assert_eq!(
        restored
            .read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    restored.shutdown().await.unwrap();
}

/// `:memory:` is a database like any other, so it backs up to a file — which
/// is also the only way to make an in-memory log outlive its process.
///
/// It has no reader connections (each `:memory:` open is a separate database,
/// so a second connection would see an empty one), and `Shared::reader` falls
/// back to the writer there. The copy is taken on that connection.
#[tokio::test]
async fn an_in_memory_log_backs_up_to_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let copy = dir.path().join("copy.db");

    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for kind in ["a", "b"] {
        s.append(event(kind)).await.unwrap();
    }
    log.checkpoint_save("reporting", Position::new(1))
        .await
        .unwrap();
    log.backup_to(&copy).await.unwrap();
    log.shutdown().await.unwrap();

    let restored = SqliteEventLog::open(&copy).await.unwrap();
    assert_eq!(pragma(&restored, "user_version").await, TARGET_USER_VERSION);
    assert_eq!(
        restored
            .read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        restored.checkpoint_load("reporting").await.unwrap(),
        Position::new(1)
    );
    restored.shutdown().await.unwrap();
}
