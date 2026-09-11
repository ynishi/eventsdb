//! Escape-hatch tests.
//!
//! The hatch exists so nobody has to open the database file themselves. That
//! only holds if it is genuinely useful (your tables, your SQL, joined
//! against the log) *and* genuinely safe (it cannot write the log behind the
//! store's back). Both halves are tested here, and so is the thing that would
//! quietly break everything else: the authorizer must not outlive the call.

use eventsdb_core::error::{Error, Result};
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{Guard, Plan, QueryOptions, SqliteEventLog};
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
    let missing = log.query("SELECT k FROM half", Vec::<Value>::new()).await;
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

/// Where the authorizer stops, and what carries on past it.
///
/// The authorizer is installed on this crate's own connection, for the length
/// of a hatch call. Somebody who opens the file themselves never meets it — a
/// `sqlite3` shell, another program, a later build of this one. So the refusal
/// above is a property of *this API*, and on its own it is not the same claim
/// as "a stored event cannot be rewritten".
///
/// The trigger is in the schema, which is a property of the *file*. That is
/// the one this checks: a connection with no authorizer, full write access,
/// and nothing of this crate between it and the table.
#[tokio::test]
async fn a_connection_outside_this_crate_still_cannot_rewrite_an_event() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");

    let log = SqliteEventLog::open(&path).await.unwrap();
    log.stream_handle("s")
        .append(event("placed"))
        .await
        .unwrap();
    log.close().await.unwrap();

    let conn = rusqlite::Connection::open(&path).unwrap();
    let error = conn
        .execute("UPDATE events SET kind = 'rewritten'", [])
        .unwrap_err();
    assert!(
        error.to_string().contains("append-only"),
        "the trigger should have refused it, got {error}"
    );

    // Refused, not partially applied.
    let kind: String = conn
        .query_row("SELECT kind FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(kind, "placed");
}

/// The trigger guards the events, and nothing else — the counter is a table
/// that has to be updated, and retention is still allowed to remove.
#[tokio::test]
async fn the_trigger_leaves_the_paths_that_must_still_work_alone() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");

    // The stream counter is upserted on every append after the first.
    s.append(event("a")).await.unwrap();
    assert_eq!(s.append(event("b")).await.unwrap().seq, 2);

    // And removal is what retention is for. `Before` is inclusive, so this
    // takes the first event and leaves the second.
    let report = log
        .retain(Plan::Before(Position::new(1)), Guard::Force)
        .await
        .unwrap();
    assert_eq!(report.removed, 1);
    assert_eq!(s.len().await.unwrap(), 1);
}

/// The same refusals, whichever handle the SQL came in through.
///
/// `ATTACH` is the one that separates the two gates. `sqlite3_stmt_readonly`
/// answers *true* for it — it changes the connection's configuration rather
/// than any file's contents — so a path that relies on the readonly check
/// alone lets a caller attach a database to a long-lived pooled connection and
/// leave it there for whoever gets that connection next. Only the authorizer
/// refuses it, and the authorizer has to be installed on both paths.
#[tokio::test]
async fn a_stream_handles_sql_is_gated_exactly_as_the_logs_is() {
    let log = seeded().await;
    let s = log.stream_handle("s");

    for statement in [
        "ATTACH DATABASE ':memory:' AS smuggled",
        "DELETE FROM events",
        "PRAGMA journal_mode = DELETE",
    ] {
        let by_log = log.query(statement, Vec::<Value>::new()).await;
        let by_handle = s.query(statement, Vec::new()).await;

        assert!(by_log.is_err(), "the log should refuse `{statement}`");
        assert!(
            by_handle.is_err(),
            "the handle let `{statement}` through, and the log did not"
        );
    }
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

/// The other half of that promise: reading a pragma is allowed, and the
/// introspection ones take the name of what to describe.
///
/// SQLite puts that name in the same slot as an assignment's value, so a rule
/// reading "has a value" refuses `table_info` along with `user_version = 99`.
/// The two are told apart by name, and this is the case the name has to let
/// through.
#[tokio::test]
async fn the_hatch_reads_a_pragma_that_names_a_table() {
    let log = seeded().await;

    let columns: Vec<String> = log
        .with_transaction(|tx| {
            let mut stmt = tx.prepare("PRAGMA table_info(events)").map_err(sql_error)?;
            let rows = stmt
                .query_map([], |row| row.get::<_, String>("name"))
                .map_err(sql_error)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row.map_err(sql_error)?);
            }
            Ok(out)
        })
        .await
        .unwrap();

    for expected in ["position", "stream", "seq", "kind", "data"] {
        assert!(
            columns.contains(&expected.to_string()),
            "`{expected}` missing from {columns:?}"
        );
    }
}

/// And through `query`, which has a second gate in front of the authorizer.
///
/// `sqlite3_stmt_readonly` answers *true* for `PRAGMA table_info`, so the
/// statement reaches the authorizer and the allowlist is what decides.
#[tokio::test]
async fn query_reads_a_pragma_that_names_a_table() {
    let log = seeded().await;

    let rows = log
        .query("PRAGMA table_info(events)", Vec::<Value>::new())
        .await
        .unwrap();

    let columns: Vec<&Value> = rows.iter().filter_map(|row| row.get("name")).collect();
    for expected in ["position", "stream", "seq", "kind", "data"] {
        assert!(
            columns.contains(&&json!(expected)),
            "`{expected}` missing from {columns:?}"
        );
    }
}

/// A pragma nobody here has considered is refused, argument or not.
///
/// `integrity_check` reads rather than writes, and it still does not get in:
/// the rule is an allowlist, so the default for an unlisted name is refusal.
/// A denylist of the state-bearing pragmas would have inverted that and
/// admitted whatever SQLite adds next.
#[tokio::test]
async fn the_hatch_refuses_a_pragma_that_is_not_on_the_allowlist() {
    let log = seeded().await;

    let error = log
        .with_transaction(|tx| {
            tx.execute_batch("PRAGMA integrity_check(1)")
                .map_err(sql_error)
        })
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(_)), "got {error}");
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
        .query("DELETE FROM events", Vec::<Value>::new())
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

/// A caller table holding one `TEXT` cell that is not valid UTF-8, and the
/// proof SQLite really did store it as `TEXT` rather than as a blob.
async fn text_that_is_not_utf8() -> SqliteEventLog {
    let log = seeded().await;
    log.with_transaction(|tx| {
        tx.execute_batch(
            "CREATE TABLE raw (t TEXT);
             INSERT INTO raw (t) VALUES (CAST(x'ff' AS TEXT));",
        )
        .map_err(sql_error)
    })
    .await
    .unwrap();

    let kind = log
        .query("SELECT typeof(t) AS k FROM raw", Vec::<Value>::new())
        .await
        .unwrap();
    assert_eq!(kind[0]["k"], json!("text"), "the cell has to be TEXT");
    log
}

#[tokio::test]
async fn query_refuses_a_blob_and_names_the_sql_that_gets_it() {
    let log = seeded().await;
    let error = log
        .query("SELECT x'00ff' AS b", Vec::<Value>::new())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(_)), "got {error}");
    let message = error.to_string();
    assert!(message.contains('b'), "got {message}");
    assert!(message.contains("hex("), "got {message}");
}

#[tokio::test]
async fn query_refuses_a_non_finite_real_and_names_the_sql_that_gets_it() {
    let log = seeded().await;
    let error = log
        .query("SELECT 1e999 AS r", Vec::<Value>::new())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(_)), "got {error}");
    let message = error.to_string();
    assert!(message.contains('r'), "got {message}");
    assert!(message.contains("CAST("), "got {message}");
}

#[tokio::test]
async fn query_refuses_text_that_is_not_utf8() {
    let log = text_that_is_not_utf8().await;
    let error = log
        .query("SELECT t FROM raw", Vec::<Value>::new())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Unsupported(_)), "got {error}");
    assert!(error.to_string().contains("UTF-8"), "got {error}");
}

#[tokio::test]
async fn query_refuses_to_bind_an_integer_above_i64_max() {
    let log = seeded().await;
    let error = log
        .query("SELECT ?1 AS n", vec![json!(u64::MAX)])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Validation(_)), "got {error}");

    // A bind is not a read, so `lossy` does not reach it.
    let still = log
        .query_with(
            "SELECT ?1 AS n",
            vec![json!(u64::MAX)],
            QueryOptions::default().lossy(true),
        )
        .await
        .unwrap_err();
    assert!(matches!(still, Error::Validation(_)), "got {still}");
}

#[tokio::test]
async fn lossy_answers_with_what_sql_itself_renders() {
    let log = seeded().await;
    let lossy = QueryOptions::default().lossy(true);

    let blob = log
        .query_with("SELECT x'00ff' AS c", Vec::<Value>::new(), lossy.clone())
        .await
        .unwrap();
    let hex = log
        .query("SELECT hex(x'00ff') AS c", Vec::<Value>::new())
        .await
        .unwrap();
    assert_eq!(blob[0]["c"], hex[0]["c"]);
    assert_eq!(blob[0]["c"], json!("00FF"));

    let plus = log
        .query_with("SELECT 1e999 AS c", Vec::<Value>::new(), lossy.clone())
        .await
        .unwrap();
    let cast_plus = log
        .query("SELECT CAST(1e999 AS TEXT) AS c", Vec::<Value>::new())
        .await
        .unwrap();
    assert_eq!(plus[0]["c"], cast_plus[0]["c"]);
    assert_eq!(plus[0]["c"], json!("Inf"));

    let minus = log
        .query_with("SELECT -1e999 AS c", Vec::<Value>::new(), lossy)
        .await
        .unwrap();
    let cast_minus = log
        .query("SELECT CAST(-1e999 AS TEXT) AS c", Vec::<Value>::new())
        .await
        .unwrap();
    assert_eq!(minus[0]["c"], cast_minus[0]["c"]);
    assert_eq!(minus[0]["c"], json!("-Inf"));
}

#[tokio::test]
async fn lossy_replaces_the_invalid_sequences_in_text() {
    let log = text_that_is_not_utf8().await;
    let rows = log
        .query_with(
            "SELECT t FROM raw",
            Vec::<Value>::new(),
            QueryOptions::default().lossy(true),
        )
        .await
        .unwrap();
    assert_eq!(
        rows[0]["t"],
        json!(String::from_utf8_lossy(&[0xff]).into_owned())
    );
}

#[tokio::test]
async fn the_cells_that_have_a_json_value_are_untouched_either_way() {
    let log = seeded().await;
    let sql = "SELECT NULL AS a, 7 AS b, 1.5 AS c, 'text' AS d";

    let strict = log.query(sql, Vec::<Value>::new()).await.unwrap();
    let lossy = log
        .query_with(
            sql,
            Vec::<Value>::new(),
            QueryOptions::default().lossy(true),
        )
        .await
        .unwrap();

    assert_eq!(strict, lossy);
    assert_eq!(strict[0]["a"], json!(null));
    assert_eq!(strict[0]["b"], json!(7));
    assert_eq!(strict[0]["c"], json!(1.5));
    assert_eq!(strict[0]["d"], json!("text"));
}

#[tokio::test]
async fn a_named_query_answers_what_its_positional_twin_does() {
    let log = seeded().await;

    let by_position = log
        .query(
            "SELECT stream, kind FROM events WHERE kind = ?1",
            vec![json!("b")],
        )
        .await
        .unwrap();
    let by_name = log
        .query(
            "SELECT stream, kind FROM events WHERE kind = :kind",
            vec![(":kind".to_string(), json!("b"))],
        )
        .await
        .unwrap();

    assert_eq!(by_position, by_name);
    assert_eq!(by_name.len(), 1);
    assert_eq!(by_name[0]["kind"], json!("b"));

    // And through `query_with`, which is where the options live.
    let with_options = log
        .query_with(
            "SELECT stream, kind FROM events WHERE kind = :kind",
            vec![(":kind".to_string(), json!("b"))],
            QueryOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(with_options, by_name);
}

/// `$` and `@` are the other two spellings of the same thing, and the `$`
/// one shares its character with `json_extract`'s path syntax — which is why
/// rewriting named placeholders to numbered ones outside the crate is a
/// reading of the SQL rather than a substitution.
#[tokio::test]
async fn every_sigil_binds_and_a_dollar_in_a_literal_is_left_alone() {
    let log = SqliteEventLog::open_in_memory().await.unwrap();
    let mut s = log.stream_handle("s");
    s.append(
        json!({ "kind": "scored", "data": { "n": 7 } })
            .as_object()
            .unwrap()
            .clone(),
    )
    .await
    .unwrap();

    let rows = log
        .query(
            "SELECT json_extract(data, '$.n') AS n FROM events \
             WHERE kind = $kind AND stream = @stream",
            vec![
                ("$kind".to_string(), json!("scored")),
                ("@stream".to_string(), json!("s")),
            ],
        )
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["n"], json!(7));
}

#[tokio::test]
async fn a_name_the_statement_declares_and_the_call_omits_is_refused() {
    let log = seeded().await;
    let error = log
        .query(
            "SELECT * FROM events WHERE kind = :kind AND stream = :stream",
            vec![(":kind".to_string(), json!("a"))],
        )
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Validation(_)), "got {error}");
    assert!(
        error.to_string().contains(":stream"),
        "the refusal names the parameter: {error}"
    );
}

/// The reason the check above exists: rusqlite leaves an unbound named
/// parameter at `NULL`, so without it this statement would have answered.
#[tokio::test]
async fn an_omitted_name_is_refused_rather_than_read_as_null() {
    let log = seeded().await;
    let answered = log
        .query(
            "SELECT count(*) AS n FROM events WHERE kind IS :kind",
            vec![(":kind".to_string(), json!("a"))],
        )
        .await
        .unwrap();
    assert_eq!(answered[0]["n"], json!(1));

    // Same statement, nothing bound. `kind IS NULL` would have answered 0.
    let error = log
        .query(
            "SELECT count(*) AS n FROM events WHERE kind IS :kind",
            Vec::<(String, Value)>::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Validation(_)), "got {error}");
}

#[tokio::test]
async fn a_name_the_call_supplies_and_the_statement_lacks_is_refused() {
    let log = seeded().await;
    let error = log
        .query(
            "SELECT * FROM events WHERE kind = :kind",
            vec![
                (":kind".to_string(), json!("a")),
                (":stream".to_string(), json!("s")),
            ],
        )
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Validation(_)), "got {error}");
    assert!(
        error.to_string().contains(":stream"),
        "the refusal names the parameter: {error}"
    );
}

/// A name without its sigil matches no placeholder, and the refusal says so
/// rather than binding it to nothing.
#[tokio::test]
async fn a_name_without_its_sigil_is_refused() {
    let log = seeded().await;
    let error = log
        .query(
            "SELECT * FROM events WHERE kind = :kind",
            vec![("kind".to_string(), json!("a"))],
        )
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Validation(_)), "got {error}");
    assert!(error.to_string().contains(":kind"), "got {error}");
}

/// SQLite counts named and numbered placeholders in one space, so a statement
/// bound by name with a bare `?` in it has a slot nothing supplies.
#[tokio::test]
async fn a_positional_placeholder_in_a_statement_bound_by_name_is_refused() {
    let log = seeded().await;
    let error = log
        .query(
            "SELECT * FROM events WHERE kind = :kind AND stream = ?",
            vec![(":kind".to_string(), json!("a"))],
        )
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Validation(_)), "got {error}");
    assert!(error.to_string().contains("positional"), "got {error}");
}

#[tokio::test]
async fn a_json_object_binds_by_name() {
    let log = seeded().await;
    let mut params = Map::new();
    params.insert(":kind".to_string(), json!("a"));

    let rows = log
        .query("SELECT kind FROM events WHERE kind = :kind", params)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["kind"], json!("a"));
}

/// A positional set whose count does not match the statement's used to be
/// `Error::Storage` — corruption's class for the caller's own typo.
#[tokio::test]
async fn a_positional_count_that_does_not_match_is_refused_as_validation() {
    let log = seeded().await;
    let error = log
        .query("SELECT ?1 AS a, ?2 AS b", vec![json!(1)])
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Validation(_)), "got {error}");
    assert!(error.to_string().contains('2'), "got {error}");
    assert!(error.to_string().contains('1'), "got {error}");
}

/// The trait's `query` is positional and stays that way: a
/// `Box<dyn EventStore>` cannot dispatch a generic argument.
#[tokio::test]
async fn the_traits_query_still_takes_a_vec() {
    let log = seeded().await;
    let store: Box<dyn EventStore> = Box::new(log.stream_handle("s"));
    let rows = store
        .query("SELECT kind FROM events WHERE kind = ?1", vec![json!("a")])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}
