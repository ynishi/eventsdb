//! Escape-hatch tests.
//!
//! The hatch exists so nobody has to open the database file themselves. That
//! only holds if it is genuinely useful (your tables, your SQL, joined
//! against the log) *and* genuinely safe (it cannot write the log behind the
//! store's back). Both halves are tested here, and so is the thing that would
//! quietly break everything else: the authorizer must not outlive the call.

use eventsdb_core::error::{Error, Result};
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{Guard, Plan, SqliteEventLog};
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

fn sql_error(e: rusqlite::Error) -> Error {
    Error::storage(e.to_string())
}

async fn seeded() -> SqliteEventLog {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("a")).await.unwrap();
    s.append(event("b")).await.unwrap();
    log
}

#[tokio::test]
async fn the_hatch_creates_and_writes_your_own_tables() {
    let log = seeded().await;

    log.with_transaction(|tx| {
        tx.execute_batch(
            "CREATE TABLE notes (k TEXT PRIMARY KEY, v TEXT NOT NULL);
             INSERT INTO notes (k, v) VALUES ('hello', 'world');",
        )
        .map_err(sql_error)
    })
    .await
    .unwrap();

    let rows = log
        .query("SELECT v FROM notes WHERE k = ?1", vec![json!("hello")])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["v"], json!("world"));
}

#[tokio::test]
async fn the_hatch_can_read_the_log_and_join_against_it() {
    let log = seeded().await;

    let kinds: Vec<String> = log
        .with_transaction(|tx| {
            tx.execute_batch("CREATE TABLE wanted (kind TEXT PRIMARY KEY)")
                .map_err(sql_error)?;
            tx.execute("INSERT INTO wanted (kind) VALUES ('b')", [])
                .map_err(sql_error)?;

            let mut stmt = tx
                .prepare(
                    "SELECT e.kind FROM events e \
                     JOIN wanted w ON w.kind = e.kind ORDER BY e.position",
                )
                .map_err(sql_error)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(sql_error)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(sql_error)?);
            }
            Ok(out)
        })
        .await
        .unwrap();

    assert_eq!(kinds, vec!["b".to_string()]);
}

#[tokio::test]
async fn an_error_rolls_the_whole_transaction_back() {
    let log = seeded().await;

    let error = log
        .with_transaction(|tx| -> Result<()> {
            tx.execute_batch("CREATE TABLE half (k TEXT)")
                .map_err(sql_error)?;
            tx.execute("INSERT INTO half (k) VALUES ('x')", [])
                .map_err(sql_error)?;
            Err(Error::storage("changed my mind"))
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("changed my mind"), "got {error}");

    // The table was created *and* rolled back, so it is not there at all.
    let missing = log.query("SELECT k FROM half", Vec::new()).await;
    assert!(missing.is_err(), "the table should not exist");
}

#[tokio::test]
async fn the_hatch_refuses_to_write_the_log() {
    let log = seeded().await;

    for statement in [
        "INSERT INTO events (stream, seq, epoch_ms, kind, schema_version, meta, data) \
         VALUES ('x', 1, 0, 'forged', 1, '{}', '{}')",
        "DELETE FROM events",
        "UPDATE events SET kind = 'rewritten'",
        "DROP TABLE events",
    ] {
        let owned = statement.to_string();
        let error = log
            .with_transaction(move |tx| tx.execute_batch(&owned).map_err(sql_error))
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Unsupported(_)),
            "`{statement}` should be refused, got {error}"
        );
    }

    // Nothing got through.
    let all = log
        .read_all(Position::BEGINNING, &Filter::all(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].kind(), "a");
    assert_eq!(all[1].kind(), "b");
}

#[tokio::test]
async fn the_hatch_refuses_to_write_the_bookkeeping_tables() {
    let log = seeded().await;

    for statement in [
        "INSERT INTO checkpoints (consumer, position, updated_ms) VALUES ('c', 99, 0)",
        "DELETE FROM retention",
        "UPDATE checkpoints SET position = 0",
    ] {
        let owned = statement.to_string();
        let error = log
            .with_transaction(move |tx| tx.execute_batch(&owned).map_err(sql_error))
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Unsupported(_)),
            "`{statement}` should be refused, got {error}"
        );
    }

    // A cursor cannot be forged, so retention's guard still means something.
    log.checkpoint_save("real", Position::new(1)).await.unwrap();
    let error = log
        .retain(Plan::Before(Position::new(2)), Guard::RegisteredConsumers)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::ConsumerBehind { .. }), "got {error}");
}

#[tokio::test]
async fn the_hatch_refuses_to_attach_a_database_or_set_a_pragma() {
    let log = seeded().await;

    let attach = log
        .with_transaction(|tx| {
            tx.execute_batch("ATTACH DATABASE ':memory:' AS other")
                .map_err(sql_error)
        })
        .await
        .unwrap_err();
    assert!(matches!(attach, Error::Unsupported(_)), "got {attach}");

    let pragma = log
        .with_transaction(|tx| {
            tx.execute_batch("PRAGMA user_version = 99")
                .map_err(sql_error)
        })
        .await
        .unwrap_err();
    assert!(matches!(pragma, Error::Unsupported(_)), "got {pragma}");
}

/// The failure that would be worst and quietest: an authorizer left installed
/// would silently refuse the store's own writes from then on.
#[tokio::test]
async fn the_authorizer_does_not_outlive_the_call() {
    let log = seeded().await;

    let refused = log
        .with_transaction(|tx| tx.execute_batch("DELETE FROM events").map_err(sql_error))
        .await;
    assert!(refused.is_err());

    // Everything the store does itself still works afterwards.
    let mut s = log.stream_handle("s");
    let committed = s.append(event("after")).await.unwrap();
    assert_eq!(committed.seq, 3);

    log.checkpoint_save("c", Position::new(1)).await.unwrap();
    assert_eq!(log.checkpoint_load("c").await.unwrap(), Position::new(1));

    log.retain(Plan::Before(Position::new(1)), Guard::Force)
        .await
        .unwrap();
    assert_eq!(log.removed_watermark().await.unwrap(), Position::new(1));
}

#[tokio::test]
async fn query_refuses_a_write() {
    let log = seeded().await;
    let error = log
        .query("DELETE FROM events", Vec::new())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(_)), "got {error}");
}

#[tokio::test]
async fn query_reads_the_stored_shape_across_the_whole_database() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut a = log.stream_handle("a");
    let mut b = log.stream_handle("b");
    a.append(
        json!({ "kind": "scored", "data": { "n": 2 } })
            .as_object()
            .unwrap()
            .clone(),
    )
    .await
    .unwrap();
    b.append(
        json!({ "kind": "scored", "data": { "n": 5 } })
            .as_object()
            .unwrap()
            .clone(),
    )
    .await
    .unwrap();

    let rows = log
        .query(
            "SELECT stream, json_extract(data, '$.n') AS n FROM events \
             WHERE kind = ?1 ORDER BY position",
            vec![json!("scored")],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["stream"], json!("a"));
    assert_eq!(rows[0]["n"], json!(2));
    assert_eq!(rows[1]["n"], json!(5));
}
