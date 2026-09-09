//! Regressions for defects found in the pre-publication review.
//!
//! Each test here asserts the *correct* behaviour of something that was
//! genuinely broken, so a name that stops making sense is a signal the fix
//! was undone.

use eventsdb_core::error::{Error, Result};
use eventsdb_core::position::Recorded;
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{Guard, Plan, Projection, SqliteEventLog, Transaction};
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

fn sql_error(e: rusqlite::Error) -> Error {
    Error::storage(e.to_string())
}

/// A `TEMP TABLE events` shadows the log for every unqualified statement on
/// the connection, so appends would land in it, report a reused position, and
/// leave the durable log silently not growing.
#[tokio::test]
async fn the_hatch_cannot_shadow_the_log_with_a_temp_table() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("first")).await.unwrap();

    let refused = log
        .with_transaction(|tx| {
            tx.execute_batch(
                "CREATE TEMP TABLE events (
                     position INTEGER PRIMARY KEY AUTOINCREMENT,
                     stream TEXT NOT NULL, seq INTEGER NOT NULL,
                     epoch_ms INTEGER NOT NULL, kind TEXT NOT NULL,
                     schema_version INTEGER NOT NULL,
                     meta TEXT NOT NULL, data TEXT NOT NULL,
                     UNIQUE (stream, seq))",
            )
            .map_err(sql_error)
        })
        .await
        .unwrap_err();
    assert!(matches!(refused, Error::Unsupported(_)), "got {refused}");

    // A temp *view* is as good a shadow, and is refused too.
    let view = log
        .with_transaction(|tx| {
            tx.execute_batch("CREATE TEMP VIEW events AS SELECT 1")
                .map_err(sql_error)
        })
        .await
        .unwrap_err();
    assert!(matches!(view, Error::Unsupported(_)), "got {view}");

    // The log kept growing, with no reused position.
    let second = s.append(event("second")).await.unwrap();
    assert_eq!(second.position, Some(Position::new(2)));
    assert_eq!(
        log.read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .len(),
        2
    );
}

/// A panic used to unwind past the authorizer's uninstall. The isle keeps the
/// connection alive after a caught panic, so the guard stayed installed and
/// every later write — including the store's own — was refused as
/// `not authorized`, while reads carried on working.
#[tokio::test]
async fn a_panic_in_the_hatch_does_not_leave_the_authorizer_installed() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("before")).await.unwrap();

    let error = log
        .with_transaction(|_tx| -> Result<()> { panic!("projection author had a bad day") })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("panicked"), "got {error}");

    // Everything the store does itself still works.
    assert_eq!(s.append(event("after")).await.unwrap().seq, 2);
    log.checkpoint_save("c", Position::new(1)).await.unwrap();
    log.retain(Plan::Before(Position::new(1)), Guard::Force)
        .await
        .unwrap();
    log.with_transaction(|tx| {
        tx.execute_batch("CREATE TABLE mine (k TEXT)")
            .map_err(sql_error)
    })
    .await
    .unwrap();
}

/// `seq` was derived from `MAX(seq)` over surviving rows, so emptying a
/// stream restarted it at 1 and handed out a coordinate an earlier event
/// already had — the reuse `position` is careful to avoid, applied to the
/// wrong column.
#[tokio::test]
async fn seq_does_not_rewind_after_retention_empties_a_stream() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("session-1");
    for _ in 0..3 {
        s.append(event("a")).await.unwrap();
    }

    log.retain(
        Plan::Streams(vec!["session-1".to_string()]),
        Guard::default(),
    )
    .await
    .unwrap();
    assert_eq!(s.len().await.unwrap(), 0, "the stream really is empty");

    let next = s.append(event("a")).await.unwrap();
    assert_eq!(next.seq, 4, "seq carries on rather than restarting");
}

#[tokio::test]
async fn seq_does_not_rewind_when_age_removes_a_whole_stream() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("old")).await.unwrap();
    s.append(event("old")).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let cutoff = eventsdb_core::event::now_ms();

    log.retain(Plan::OlderThan(cutoff), Guard::default())
        .await
        .unwrap();
    assert_eq!(s.len().await.unwrap(), 0);

    assert_eq!(s.append(event("new")).await.unwrap().seq, 3);
}

/// The counter is bumped inside the append's own transaction, so a rejected
/// or rolled-back write must not consume a sequence number.
#[tokio::test]
async fn a_refused_write_consumes_no_sequence_number() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("a")).await.unwrap();

    // Refused by validation, before the database.
    let bad = json!({ "kind": "b", "surprise": 1 })
        .as_object()
        .unwrap()
        .clone();
    assert!(s.append(bad).await.is_err());

    // Refused by the decision, inside the transaction.
    let none = s.append_if(None, Box::new(|_| None)).await.unwrap();
    assert!(none.is_none());

    assert_eq!(s.append(event("b")).await.unwrap().seq, 2);
}

#[tokio::test]
async fn the_stream_counter_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");

    {
        let log = SqliteEventLog::open(&path).await.unwrap();
        let mut s = log.stream_handle("s");
        s.append(event("a")).await.unwrap();
        s.append(event("a")).await.unwrap();
        log.retain(Plan::Streams(vec!["s".to_string()]), Guard::Force)
            .await
            .unwrap();
        log.close().await.unwrap();
    }

    let log = SqliteEventLog::open(&path).await.unwrap();
    let mut s = log.stream_handle("s");
    assert_eq!(s.append(event("a")).await.unwrap().seq, 3);
    log.close().await.unwrap();
}

/// `sqlite3_stmt_readonly` reports ATTACH as read-only — it changes the
/// connection's configuration, not a file's contents — so the readonly gate
/// alone let a caller attach a database to the long-lived connection.
#[tokio::test]
async fn query_cannot_attach_a_database() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    let error = log
        .query("ATTACH DATABASE ':memory:' AS side", Vec::new())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(_)), "got {error}");

    // Nothing was attached, so a name qualified with it does not resolve.
    assert!(log
        .query("SELECT count(*) FROM side.sqlite_master", Vec::new())
        .await
        .is_err());
}

/// Adding an index to `events` is deliberately allowed: it changes no data,
/// and it is the only way to make a read cheap when the shipped indices do
/// not cover what a caller filters on.
#[tokio::test]
async fn the_hatch_may_index_the_log_but_not_drop_the_shipped_indices() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    log.with_transaction(|tx| {
        tx.execute_batch(
            "CREATE INDEX events_meta_trace \
             ON events (json_extract(meta, '$.trace'))",
        )
        .map_err(sql_error)
    })
    .await
    .unwrap();

    let dropped = log
        .with_transaction(|tx| {
            tx.execute_batch("DROP INDEX events_kind_position")
                .map_err(sql_error)
        })
        .await
        .unwrap_err();
    assert!(matches!(dropped, Error::Unsupported(_)), "got {dropped}");
}

/// A `Projection` is a public trait, so `apply` is caller code — and it used
/// to be handed a transaction with no authorizer on it at all. That was a
/// second door onto the log's tables sitting beside the one the hatch closes:
/// a projection could insert an event that never went through validation or
/// stamping, and it would read back as legitimate.
#[tokio::test]
async fn a_projection_cannot_write_the_logs_own_tables() {
    struct Saboteur {
        statement: &'static str,
    }

    impl Projection for Saboteur {
        fn name(&self) -> &str {
            "saboteur"
        }
        fn reset(&mut self, _tx: &Transaction<'_>) -> Result<()> {
            Ok(())
        }
        fn apply(&mut self, tx: &Transaction<'_>, _event: &Recorded) -> Result<()> {
            tx.execute_batch(self.statement).map_err(sql_error)
        }
    }

    for statement in [
        "INSERT INTO events (stream, seq, epoch_ms, kind, schema_version, meta, data) \
         VALUES ('x', 1, 0, 'forged', 1, '{}', '{}')",
        // Erasing these would remove retention's only input for deciding what
        // is safe to delete.
        "DELETE FROM checkpoints",
        "DELETE FROM retention",
        "UPDATE stream_seq SET next_seq = 1",
    ] {
        let log = SqliteEventLog::open_in_memory().await.unwrap();
        let mut s = log.stream_handle("s");
        s.append(event("real")).await.unwrap();

        let mut runner = log.runner(Saboteur { statement });
        runner.init().await.unwrap();
        let error = runner.catch_up().await.unwrap_err();
        assert!(
            matches!(error, Error::Unsupported(_)),
            "`{statement}` should be refused, got {error}"
        );

        // Refused *and* rolled back: one event, the real one.
        let all = log
            .read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].kind(), "real");
    }
}

/// The other half: the runner's own bookkeeping writes `checkpoints`, which is
/// reserved, so it runs trusted — and the projection's own tables stay
/// writable. Trust is dropped only for the duration of the callback.
#[tokio::test]
async fn a_projection_still_writes_its_own_tables_and_the_cursor_still_moves() {
    struct Counter;

    impl Projection for Counter {
        fn name(&self) -> &str {
            "counter"
        }
        fn init(&mut self, tx: &Transaction<'_>) -> Result<()> {
            tx.execute_batch("CREATE TABLE IF NOT EXISTS seen (n INTEGER)")
                .map_err(sql_error)
        }
        fn reset(&mut self, tx: &Transaction<'_>) -> Result<()> {
            tx.execute_batch("DELETE FROM seen").map_err(sql_error)
        }
        fn apply(&mut self, tx: &Transaction<'_>, _event: &Recorded) -> Result<()> {
            tx.execute("INSERT INTO seen (n) VALUES (1)", [])
                .map(|_| ())
                .map_err(sql_error)
        }
    }

    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    for _ in 0..3 {
        s.append(event("a")).await.unwrap();
    }

    let mut runner = log.runner(Counter);
    runner.init().await.unwrap();
    assert_eq!(runner.catch_up().await.unwrap(), 3);
    assert_eq!(runner.position().await.unwrap(), Position::new(3));

    let rows = log
        .query("SELECT count(*) AS n FROM seen", Vec::new())
        .await
        .unwrap();
    assert_eq!(rows[0]["n"], serde_json::json!(3));

    // And a rebuild, which calls reset + init inside the same guard.
    assert_eq!(runner.rebuild().await.unwrap(), 3);
    let rows = log
        .query("SELECT count(*) AS n FROM seen", Vec::new())
        .await
        .unwrap();
    assert_eq!(rows[0]["n"], serde_json::json!(3));
}

#[tokio::test]
async fn the_stream_counter_table_is_reserved_from_the_hatch() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(event("a")).await.unwrap();

    let error = log
        .with_transaction(|tx| {
            tx.execute_batch("UPDATE stream_seq SET next_seq = 1")
                .map_err(sql_error)
        })
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(_)), "got {error}");

    assert_eq!(s.append(event("b")).await.unwrap().seq, 2);
}
