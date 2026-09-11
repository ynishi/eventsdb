//! Opening a file that already holds a table under one of this crate's names.
//!
//! Two halves. The ladder refuses such a file rather than running `CREATE
//! TABLE` at it and reporting SQLite's "table events already exists", which
//! says corruption and means nothing of the kind. And the way in is a rename
//! plus an `import`, which is what the README's "A table this crate did not
//! create" describes and what the last test here walks end to end.

use eventsdb_core::error::Error;
use eventsdb_core::{EventLog, ExportedEvent, Filter, Position};
use eventsdb_sqlite::SqliteEventLog;
use rusqlite::Connection;
use serde_json::{json, Map, Value};

/// A hand-rolled event table from before this crate was adopted: its own
/// columns, and — the part that bites on the way out — an index whose name
/// the ladder also uses.
const FOREIGN_LOG: &str = "
    CREATE TABLE events (
        id     INTEGER PRIMARY KEY,
        stream TEXT    NOT NULL,
        kind   TEXT    NOT NULL,
        body   TEXT    NOT NULL,
        at     INTEGER NOT NULL
    );
    CREATE INDEX events_stream_kind_seq ON events (stream, kind, id);
";

fn raw(path: &std::path::Path) -> Connection {
    Connection::open(path).unwrap()
}

#[tokio::test]
async fn a_foreign_events_table_is_refused_rather_than_migrated_over() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    raw(&path).execute_batch(FOREIGN_LOG).unwrap();

    let Err(refused) = SqliteEventLog::open(&path).await else {
        panic!("the ladder ran over a table it did not create")
    };

    let message = refused.to_string();
    assert!(
        matches!(refused, Error::Unsupported(_)),
        "this crate refusing to do something, not the disk failing: {message}"
    );
    assert!(
        message.contains("`events`"),
        "the message names the table in the way: {message}"
    );
    assert!(
        message.contains("import"),
        "and says how the rows get in: {message}"
    );
}

/// Every name in `RESERVED_TABLES` is checked, not only `events`. The ladder
/// would have failed on this one at the same step, one `CREATE TABLE` later.
#[tokio::test]
async fn a_foreign_table_under_another_reserved_name_is_refused_too() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    raw(&path)
        .execute_batch("CREATE TABLE checkpoints (who TEXT PRIMARY KEY, seen INTEGER)")
        .unwrap();

    let Err(refused) = SqliteEventLog::open(&path).await else {
        panic!("the ladder ran over a table it did not create")
    };
    let message = refused.to_string();
    assert!(matches!(refused, Error::Unsupported(_)), "{message}");
    assert!(message.contains("`checkpoints`"), "{message}");
}

/// `sqlite_sequence` is in `RESERVED_TABLES` and is deliberately not part of
/// the check. SQLite creates it for any `AUTOINCREMENT` table and leaves it
/// behind when that table is dropped, so a file can carry it at
/// `user_version` 0 with no foreign log anywhere in it — and refusing that
/// file would be refusing a fact about SQLite's bookkeeping.
#[tokio::test]
async fn a_leftover_sqlite_sequence_is_not_a_foreign_log() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("had-autoincrement.db");
    {
        let conn = raw(&path);
        conn.execute_batch(
            "CREATE TABLE tickets (id INTEGER PRIMARY KEY AUTOINCREMENT, who TEXT);
             INSERT INTO tickets (who) VALUES ('a'), ('b');
             DROP TABLE tickets;
             CREATE TABLE notes (k TEXT PRIMARY KEY, v TEXT);",
        )
        .unwrap();

        // The premise of the test, asserted rather than assumed.
        let left: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_sequence'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(left, 1, "SQLite kept sqlite_sequence after the drop");
        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 0, "and none of that moved user_version");
    }

    let log = SqliteEventLog::open(&path).await.unwrap();
    let mut stream = log.stream("s").await.unwrap();
    stream
        .append(json!({ "kind": "noted" }).as_object().unwrap().clone())
        .await
        .unwrap();
    assert_eq!(
        log.read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    log.close().await.unwrap();
}

/// The documented path, end to end: rename, open, read the old rows through
/// the hatch, build records, import, and drop what is left over.
#[tokio::test]
async fn the_documented_rename_and_import_brings_a_foreign_log_in() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    {
        let conn = raw(&path);
        conn.execute_batch(FOREIGN_LOG).unwrap();
        for (id, stream, kind, body, at) in [
            (1, "cart-1", "added", r#"{"sku":"a"}"#, 1_700_000_000_000i64),
            (2, "cart-1", "added", r#"{"sku":"b"}"#, 1_700_000_001_000),
            (3, "cart-2", "paid", r#"{"total":9}"#, 1_700_000_002_000),
        ] {
            conn.execute(
                "INSERT INTO events (id, stream, kind, body, at) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![id, stream, kind, body, at],
            )
            .unwrap();
        }
    }

    // Renaming the table is not enough: SQLite carries an index's name across
    // `ALTER TABLE ... RENAME`, so `events_stream_kind_seq` would collide with
    // the one `STEP_1` creates.
    {
        let conn = raw(&path);
        conn.execute_batch(
            "ALTER TABLE events RENAME TO legacy_events;
             DROP INDEX events_stream_kind_seq;",
        )
        .unwrap();
    }

    let log = SqliteEventLog::open(&path).await.unwrap();

    // The old rows come out through the hatch, which reads anything.
    let rows = log
        .query(
            "SELECT id, stream, kind, body, at FROM legacy_events ORDER BY id",
            Vec::<Value>::new(),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 3);

    let records: Vec<ExportedEvent> = rows
        .iter()
        .map(|row| {
            let data: Value = serde_json::from_str(row["body"].as_str().unwrap()).unwrap();
            let mut event = Map::new();
            event.insert("kind".to_string(), row["kind"].clone());
            event.insert("data".to_string(), data);
            event.insert("meta".to_string(), json!({}));
            event.insert("epoch_ms".to_string(), row["at"].clone());
            event.insert("_schema_version".to_string(), json!(1));
            ExportedEvent {
                stream: row["stream"].as_str().unwrap().to_string(),
                // The position it had in the old table. A witness, not an
                // instruction — see `ImportReport::reproduced_coordinates`.
                // `Some`, because this table did have keys: a foreign record
                // that carries no coordinate at all writes `None`.
                position: Some(Position::new(row["id"].as_u64().unwrap())),
                event,
            }
        })
        .collect();

    let report = log.import(records).await.unwrap();
    assert_eq!(report.imported, 3);
    // True here, and it is a claim about this file rather than about adoption
    // in general: the old table numbered its rows 1, 2, 3 with no gaps, the
    // log was empty, and the records went in in that order — so every event
    // landed on the position it carried. A foreign table whose keys start
    // anywhere else, or skip, would land the same events at other positions
    // and report `false` without anything being wrong.
    assert!(report.reproduced_coordinates, "{report:?}");
    assert_eq!(report.first, Some(Position::new(1)));
    assert_eq!(report.last, Some(Position::new(3)));

    // The old table goes once the report is right.
    log.with_transaction(|tx| {
        tx.execute_batch("DROP TABLE legacy_events")
            .map_err(|e| Error::storage(e.to_string()))
    })
    .await
    .unwrap();

    let read_back = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    let kinds: Vec<&str> = read_back
        .iter()
        .map(|e| e.event["kind"].as_str().unwrap())
        .collect();
    assert_eq!(kinds, ["added", "added", "paid"]);
    assert_eq!(read_back[2].event["data"], json!({ "total": 9 }));
    assert_eq!(read_back[0].stream, "cart-1");
    assert_eq!(read_back[2].stream, "cart-2");

    // And through the other door, with the stored fields intact.
    let exported = log
        .export(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    assert_eq!(exported.len(), 3);
    assert_eq!(exported[0].event["epoch_ms"], json!(1_700_000_000_000i64));
    assert_eq!(exported[0].event["_schema_version"], json!(1));
    assert_eq!(exported[1].event["data"], json!({ "sku": "b" }));

    log.close().await.unwrap();
}
