//! Appending inside the caller's transaction.
//!
//! The three needs this exists for: two streams written atomically, an append
//! beside a row in the caller's own table, and a derived row keyed on the
//! append's real position. Plus the two properties that make it safe — the
//! trust flag never outlives a stamped statement, and subscribers are not
//! woken for a transaction that rolls back.

use std::time::Duration;

use eventsdb_core::error::{Error, Result};
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::SqliteEventLog;
use futures_util::StreamExt;
use serde_json::{json, Map, Value};

fn event(kind: &str) -> Map<String, Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

fn sql_error(e: rusqlite::Error) -> Error {
    Error::storage(e.to_string())
}

/// N2: an event and a row in the caller's own table, together or not at all.
#[tokio::test]
async fn an_append_and_a_caller_row_land_together() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    log.with_transaction(|tx| {
        tx.execute_batch("CREATE TABLE order_index (position INTEGER PRIMARY KEY, stream TEXT)")
            .map_err(sql_error)?;
        let committed = tx.append("order-1", event("placed"))?;
        tx.execute(
            "INSERT INTO order_index (position, stream) VALUES (?1, ?2)",
            rusqlite::params![committed.position.unwrap().get() as i64, "order-1"],
        )
        .map_err(sql_error)?;
        Ok(())
    })
    .await
    .unwrap();

    let rows = log
        .query(
            "SELECT position, stream FROM order_index",
            Vec::<Value>::new(),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["position"], json!(1));
    assert_eq!(
        log.read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

/// The atomicity claim, from the other side.
#[tokio::test]
async fn an_error_rolls_back_the_append_as_well_as_the_row() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    log.with_transaction(|tx| {
        tx.execute_batch("CREATE TABLE side (k TEXT)")
            .map_err(sql_error)
    })
    .await
    .unwrap();

    let error = log
        .with_transaction(|tx| -> Result<()> {
            tx.append("order-1", event("placed"))?;
            tx.execute("INSERT INTO side (k) VALUES ('x')", [])
                .map_err(sql_error)?;
            Err(Error::storage("no"))
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no"), "got {error}");

    assert!(log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap()
        .is_empty());
    assert!(log
        .query("SELECT k FROM side", Vec::<Value>::new())
        .await
        .unwrap()
        .is_empty());
}

/// N1: two streams, one commit. No grouping rule and no ordering restriction —
/// interleaving the two streams is fine, because each append takes its own
/// `seq` from the stored counter and its own position from the rowid.
#[tokio::test]
async fn two_streams_append_atomically_and_may_interleave() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    log.with_transaction(|tx| {
        tx.append("parent", event("reserved"))?;
        tx.append("child", event("opened"))?;
        tx.append("parent", event("noted"))?; // back to the first stream
        Ok(())
    })
    .await
    .unwrap();

    let all = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap();
    let seen: Vec<(&str, u64)> = all.iter().map(|r| (r.stream.as_str(), r.seq())).collect();
    assert_eq!(seen, vec![("parent", 1), ("child", 1), ("parent", 2)]);
}

/// T4: the closest precedent (Marten) defers the write, so an inline
/// projection there reads the sequence as zero. Here the transaction is
/// already open, so the coordinates are real and usable straight away.
#[tokio::test]
async fn a_derived_row_can_be_keyed_on_the_real_position() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    let committed = log
        .with_transaction(|tx| {
            tx.execute_batch("CREATE TABLE derived (position INTEGER PRIMARY KEY, seq INTEGER)")
                .map_err(sql_error)?;
            let c = tx.append("s", event("a"))?;
            assert_ne!(c.position, Some(Position::BEGINNING));
            assert_eq!(c.seq, 1);
            tx.execute(
                "INSERT INTO derived (position, seq) VALUES (?1, ?2)",
                rusqlite::params![c.position.unwrap().get() as i64, c.seq as i64],
            )
            .map_err(sql_error)?;
            Ok(c)
        })
        .await
        .unwrap();

    let rows = log
        .query("SELECT position, seq FROM derived", Vec::<Value>::new())
        .await
        .unwrap();
    assert_eq!(
        rows[0]["position"],
        json!(committed.position.unwrap().get())
    );
    assert_eq!(rows[0]["seq"], json!(1));
}

/// Read, decide, append — all before anyone else can write.
#[tokio::test]
async fn the_context_reads_what_it_has_already_appended() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    log.with_transaction(|tx| {
        assert_eq!(tx.head("ledger")?, None);
        tx.append("ledger", event("granted"))?;

        let seen = tx.read("ledger", None, 0, 100)?;
        assert_eq!(seen.len(), 1, "the append is visible to this transaction");
        assert_eq!(seen[0].kind(), "granted");
        assert_eq!(tx.head("ledger")?, Some(1));

        // A decision made against what was just read.
        if seen.len() == 1 {
            tx.append("ledger", event("spent"))?;
        }
        Ok(())
    })
    .await
    .unwrap();

    assert_eq!(log.stream_handle("ledger").len().await.unwrap(), 2);
}

/// T2. Postgres-based stores need tombstone rows because a failed transaction
/// burns sequence numbers there. SQLite rolls back the rowid, so a rolled-back
/// append leaves no hole — and this pins that, because the immunity holds only
/// while the appends and the commit share a transaction.
#[tokio::test]
async fn a_rolled_back_append_burns_no_position() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    let _ = log
        .with_transaction(|tx| -> Result<()> {
            tx.append("s", event("discarded"))?;
            tx.append("s", event("also discarded"))?;
            Err(Error::storage("rolled back"))
        })
        .await
        .unwrap_err();

    let mut s = log.stream_handle("s");
    let committed = s.append(event("kept")).await.unwrap();
    assert_eq!(committed.position, Some(Position::new(1)));
    assert_eq!(committed.seq, 1);
}

/// The trust flag lifts only around the crate's own stamped statements.
#[tokio::test]
async fn raw_writes_to_the_log_are_still_refused_inside_the_context() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    for statement in [
        "INSERT INTO events (stream, seq, epoch_ms, kind, schema_version, meta, data) \
         VALUES ('x', 1, 0, 'forged', 1, '{}', '{}')",
        "DELETE FROM events",
        "UPDATE stream_seq SET next_seq = 1",
    ] {
        let owned = statement.to_string();
        let error = log
            .with_transaction(move |tx| {
                // A legitimate append first, so the flag has been raised and
                // lowered before the forgery is attempted.
                tx.append("s", event("real"))?;
                tx.execute_batch(&owned).map_err(sql_error)
            })
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::Unsupported(_)),
            "`{statement}` should be refused, got {error}"
        );
    }

    // All of those rolled back, so the log is still empty.
    assert!(log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap()
        .is_empty());
}

/// The flag is raised and lowered by a `Drop` guard. A panic in the middle of
/// the closure must not leave it raised for the next call.
#[tokio::test]
async fn a_panic_does_not_leave_the_trust_flag_raised() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    let error = log
        .with_transaction(|tx| -> Result<()> {
            tx.append("s", event("a"))?;
            panic!("mid-closure");
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("panicked"), "got {error}");

    let forged = log
        .with_transaction(|tx| {
            tx.execute_batch(
                "INSERT INTO events (stream, seq, epoch_ms, kind, schema_version, meta, data) \
                 VALUES ('x', 1, 0, 'forged', 1, '{}', '{}')",
            )
            .map_err(sql_error)
        })
        .await
        .unwrap_err();
    assert!(matches!(forged, Error::Unsupported(_)), "got {forged}");

    // And the store itself still works.
    let mut s = log.stream_handle("s");
    assert_eq!(s.append(event("after")).await.unwrap().seq, 1);
}

/// T1: waking a subscriber at a position a rollback then erases would walk it
/// past a hole it can never fill, and the watch only moves forward.
#[tokio::test]
async fn subscribers_are_not_woken_by_a_transaction_that_rolls_back() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut sub = log.subscribe(Position::BEGINNING, Filter::all()).unwrap();

    let _ = log
        .with_transaction(|tx| -> Result<()> {
            tx.append("s", event("discarded"))?;
            Err(Error::storage("rolled back"))
        })
        .await
        .unwrap_err();

    // Nothing to deliver.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), sub.next())
            .await
            .is_err(),
        "a rolled-back append must not reach a subscriber"
    );

    // A committed one does.
    log.with_transaction(|tx| tx.append("s", event("kept")).map(|_| ()))
        .await
        .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("the committed append arrives")
        .unwrap()
        .unwrap();
    assert_eq!(got.kind(), "kept");
    assert_eq!(got.position, Position::new(1));
}

/// The context and the stream handle share one counter, so sequence numbers
/// continue across the two ways of appending.
#[tokio::test]
async fn the_context_and_the_stream_handle_share_the_sequence() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");

    s.append(event("a")).await.unwrap();
    log.with_transaction(|tx| tx.append("s", event("b")).map(|_| ()))
        .await
        .unwrap();
    assert_eq!(s.append(event("c")).await.unwrap().seq, 3);

    let seqs: Vec<u64> = log
        .read_all(Position::BEGINNING, &Filter::all(), 10)
        .await
        .unwrap()
        .iter()
        .map(|r| r.seq())
        .collect();
    assert_eq!(seqs, vec![1, 2, 3]);
}

#[tokio::test]
async fn a_rejected_event_inside_the_context_consumes_nothing() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    log.with_transaction(|tx| {
        let bad = json!({ "kind": "a", "surprise": 1 })
            .as_object()
            .unwrap()
            .clone();
        assert!(tx.append("s", bad).is_err());
        let good = tx.append("s", event("a"))?;
        assert_eq!(good.seq, 1);
        assert_eq!(good.position, Some(Position::new(1)));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn append_many_inside_the_context_keeps_sequences_contiguous() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();

    let committed = log
        .with_transaction(|tx| tx.append_many("s", vec![event("a"), event("b"), event("c")]))
        .await
        .unwrap();
    assert_eq!(
        committed.iter().map(|c| c.seq).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(log.stream_handle("s").head().await.unwrap(), Some(3));
}
