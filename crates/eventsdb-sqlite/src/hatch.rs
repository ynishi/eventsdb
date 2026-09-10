//! The escape hatch: your own SQL, on the log's connection.
//!
//! # Why this has to exist
//!
//! Without it, anyone who needs a query the API does not have opens the
//! database file themselves — and what they then do to it is unbounded.
//!
//! **The danger is not what an earlier draft of this comment claimed.** It
//! said a second writing connection would break the global order. It does
//! not: the position is allocated inside the transaction that commits it, and
//! `IMMEDIATE` means that transaction holds the write lock from `BEGIN`, so
//! allocation and commit order cannot diverge across connections either. That
//! was measured, not reasoned — see `tests/two_logs.rs`.
//!
//! What a raw connection actually costs is everything the store does *around*
//! the write. An `INSERT` straight into `events` skips validation, `seq`
//! allocation and the schema stamp, so it reads back afterwards as a
//! legitimate event that no rule ever governed. A `DELETE` removes history
//! with no retention ledger entry, so nothing downstream is told that a fold
//! is now missing its input — which is the one failure retention was built to
//! make impossible.
//!
//! **How far the guard actually reaches.** It is installed for the duration of
//! a call, on the connection this store owns, at the three sites that hand a
//! connection to caller code. So it covers everything that comes *through*
//! this crate — the hatch, `query`, a projection's `apply` — and it covers
//! nothing else. A `sqlite3` session on the same file, or any other program
//! that opens it, is not refused anything: SQLite has no way to make an
//! authorizer a property of the file.
//!
//! What *is* a property of the file is a trigger, which is how `ai-store`
//! defends the same invariant. This crate now carries one for half of it:
//! [`crate::schema`] step 4 installs `trg_events_no_update`, so **no
//! connection anywhere can rewrite a stored event**, `sqlite3` included. That
//! half was free, because nothing here updates an `events` row on any path.
//!
//! The other half is not covered, and saying so is the point of this
//! paragraph: a `DELETE` from outside this crate still succeeds, leaving no
//! ledger entry. A `no_delete` trigger would have to be dropped and recreated
//! inside retention's own transaction — the guard would be switched off in
//! exactly the code most able to get removal wrong — so the gap is recorded
//! rather than closed badly. A reader who took this section as covering
//! removal by the CLI would be trusting something that is not there.
//!
//! So the hatch is not a convenience. It is what makes "go through the store"
//! a reasonable thing to ask of code that has a choice, because there is
//! somewhere to go.
//!
//! # What it gives and what it refuses
//!
//! [`SqliteEventLog::with_transaction`] hands over a [`TxnContext`] on the
//! log's own connection, inside the isle, under one `IMMEDIATE` transaction.
//! It derefs to a real [`rusqlite::Transaction`] — your tables, your SQL, your
//! schema — and carries the log's own stamped `append` as the only route to
//! `events`. Everything in the closure commits or rolls back together.
//!
//! What it refuses, through SQLite's own authorizer rather than by inspecting
//! the text: writing any table in [`RESERVED_TABLES`], **creating anything
//! that shares one of those names in any schema** (a `TEMP TABLE events`
//! shadows the real one for every unqualified statement on the connection),
//! attaching another database, and setting pragmas. Reading those tables is
//! allowed and often the point, and so is adding your own index to `events`.
//!
//! The refusals are not paternalism about your data. Each one is an invariant
//! something else in this crate already promised: appends go through
//! [`crate::SqliteEventStore`] so they are stamped and ordered; removals go
//! through [`crate::retention`] so they leave a ledger; `stream_seq` is what
//! keeps `seq` from rewinding after a removal; `user_version` is the migration
//! ladder's, and `journal_mode` is what the concurrency story rests on.
//!
//! The guard is installed for the duration of one call and taken off again
//! whatever the body does — a panic included, which is why the body runs under
//! `catch_unwind`. It sits on a connection that outlives the call, and the
//! isle keeps that connection alive after a caught panic, so an unwind past
//! the uninstall would leave every later write refused as `not authorized`
//! while reads carried on working.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use eventsdb_core::error::{Error, Result};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, TransactionBehavior};
use serde_json::{Map, Value};

use crate::log::SqliteEventLog;
use crate::shared::{classify, map_isle};
use crate::txn::TxnContext;

/// Tables this crate owns. Reading them is fine; writing them is not.
pub const RESERVED_TABLES: [&str; 6] = [
    "events",
    "stream_seq",
    "checkpoints",
    "retention",
    "exports",
    "sqlite_sequence",
];

fn is_reserved(table: &str) -> bool {
    RESERVED_TABLES
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(table))
}

/// Whether an attempted action is allowed.
///
/// Reads fall through to `Allow`: the log is meant to be queryable, and
/// joining a read model against `events` is a good use of this.
///
/// # A reserved name is refused in *any* schema, not just `main`
///
/// SQLite resolves an unqualified table name in `temp` before `main`, so a
/// `TEMP TABLE events` shadows the log for every statement on the
/// connection — including the store's own. Guarding only `main` therefore
/// left the log capturable through the one API documented as unable to touch
/// it: appends would land in the temp table, report a reused position, and
/// the durable log would silently stop growing. The name is what has to be
/// reserved, wherever it is being created.
fn authorize(context: &AuthContext<'_>, trusted: bool) -> Authorization {
    // The crate's own stamped statements run with the flag raised, for exactly
    // as long as they take. It is set and cleared by a `Drop` guard in
    // `TxnContext`, so an unwind between an append's two statements cannot
    // leave the door open.
    if trusted {
        return Authorization::Allow;
    }

    match context.action {
        // Writing, reshaping or dropping a reserved table.
        AuthAction::Insert { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::Update { table_name, .. }
        | AuthAction::DropTable { table_name }
        | AuthAction::DropTempTable { table_name }
        | AuthAction::AlterTable { table_name, .. }
        | AuthAction::CreateTrigger { table_name, .. }
        | AuthAction::CreateTempTrigger { table_name, .. }
        | AuthAction::DropTrigger { table_name, .. }
        | AuthAction::DropTempTrigger { table_name, .. }
        | AuthAction::DropIndex { table_name, .. }
        | AuthAction::DropTempIndex { table_name, .. }
            if is_reserved(table_name) =>
        {
            Authorization::Deny
        }

        // Creating something *named* like a reserved table. In `main` these
        // would fail on the existing object anyway; in `temp` they are the
        // shadowing attack, and a view is as good a shadow as a table.
        AuthAction::CreateTable { table_name } | AuthAction::CreateTempTable { table_name }
            if is_reserved(table_name) =>
        {
            Authorization::Deny
        }
        AuthAction::CreateView { view_name } | AuthAction::CreateTempView { view_name }
            if is_reserved(view_name) =>
        {
            Authorization::Deny
        }

        // Attaching would put tables outside this guard's reach and outside
        // the isle's single-writer discipline.
        AuthAction::Attach { .. } | AuthAction::Detach { .. } => Authorization::Deny,

        // Reading a pragma is fine; setting one is not. `user_version` belongs
        // to the migration ladder, `journal_mode` and `auto_vacuum` to the
        // concurrency and reclaim stories.
        AuthAction::Pragma { pragma_value, .. } if pragma_value.is_some() => Authorization::Deny,

        // Adding an index to `events` is deliberately *allowed*: it changes no
        // data, and a caller that filters on something the shipped indices do
        // not cover — a correlation value under `meta` — has no other way to
        // make that read cheap. Dropping the shipped ones is refused above.
        _ => Authorization::Allow,
    }
}

/// Turn SQLite's bare "not authorized" into a message that says what was
/// refused and where to go instead.
///
/// Applied wherever a guarded body's error surfaces — which is not only
/// [`guarded`] itself: the projection runner carries its outcome out inside a
/// tuple, so the mapping has to be reachable from there too.
pub(crate) fn map_denial(error: Error) -> Error {
    match &error {
        Error::Storage(message) if message.contains("not authorized") => denied(),
        _ => error,
    }
}

/// The message a denial produces, which SQLite reports only as
/// "not authorized".
fn denied() -> Error {
    Error::Unsupported(format!(
        "not authorized inside the hatch: writing {} , attaching a database, \
         or setting a pragma. Append through a stream handle, remove through \
         retention, and keep your own tables under your own names",
        RESERVED_TABLES.join(" / ")
    ))
}

/// Run a read-only statement and return its rows as JSON objects.
///
/// The check is SQLite's own `sqlite3_stmt_readonly`, not a scan of the text:
/// a denylist of keywords is a guess about a parser, and this is the parser's
/// answer.
pub(crate) fn query_rows(
    conn: &Connection,
    sql: &str,
    params: Vec<Value>,
) -> Result<Vec<Map<String, Value>>> {
    let stmt = conn.prepare(sql).map_err(classify)?;
    if !stmt.readonly() {
        return Err(Error::Unsupported(
            "this statement writes; `query` only reads. Use `with_transaction` \
             for your own tables, a stream handle to append, or retention to remove"
                .to_string(),
        ));
    }
    drop(stmt);

    let mut stmt = conn.prepare(sql).map_err(classify)?;
    let names: Vec<String> = stmt
        .column_names()
        .into_iter()
        .map(str::to_string)
        .collect();
    let bound: Vec<Box<dyn rusqlite::ToSql>> = params.into_iter().map(bind_value).collect();

    let rows = stmt
        .query_map(
            rusqlite::params_from_iter(bound.iter().map(|p| p.as_ref())),
            |row| {
                let mut out = Map::new();
                for (index, name) in names.iter().enumerate() {
                    out.insert(name.clone(), sql_to_json(row, index)?);
                }
                Ok(out)
            },
        )
        .map_err(classify)?;

    let mut out = Vec::new();
    for item in rows {
        out.push(item.map_err(classify)?);
    }
    Ok(out)
}

/// Bind a JSON parameter. Arrays and objects go as their text, which is what
/// `json_extract` and friends expect anyway.
///
/// The one place this loses information is stated with the conversions in the
/// other direction, on [`SqliteEventLog::query`].
pub(crate) fn bind_value(value: Value) -> Box<dyn rusqlite::ToSql> {
    match value {
        Value::Null => Box::new(Option::<String>::None),
        Value::Bool(b) => Box::new(b),
        Value::Number(n) => match n.as_i64() {
            Some(i) => Box::new(i),
            None => Box::new(n.as_f64().unwrap_or(0.0)),
        },
        Value::String(s) => Box::new(s),
        other => Box::new(other.to_string()),
    }
}

/// One cell of a result row as JSON.
///
/// [`SqliteEventLog::query`] states what each SQLite type becomes and which of
/// those conversions lose something; this is the code that performs them. It
/// is the only route from a column to the JSON a caller reads, so a conversion
/// stated there and not done here is a defect in one of the two.
pub(crate) fn sql_to_json(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Value> {
    use rusqlite::types::ValueRef;
    Ok(match row.get_ref(index)? {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::from(i),
        ValueRef::Real(f) => Value::from(f),
        ValueRef::Text(t) => Value::String(String::from_utf8_lossy(t).into_owned()),
        ValueRef::Blob(_) => Value::String("<blob>".to_string()),
    })
}

impl SqliteEventLog {
    /// Run a read-only statement against the whole database.
    ///
    /// The database-level read: the log, a projection's read model, your own
    /// tables, and any join across them.
    ///
    /// Reads the **stored** shape. Every other read goes through the upcaster
    /// chain; SQL runs against the bytes as they were written, because the
    /// chain is Rust and the query is SQLite's. A statement reading across a
    /// schema change reads the versions it finds.
    ///
    /// **What a cell becomes, and where that loses something.** `NULL` becomes
    /// `null`, an `INTEGER` a JSON integer, a finite `REAL` a JSON number, and
    /// `TEXT` a JSON string. The rest do not arrive intact, and nothing in the
    /// value that does arrive says so:
    ///
    /// - a `BLOB` becomes the string `"<blob>"`. The bytes are gone, and the
    ///   result cannot be told apart from a `TEXT` cell holding those seven
    ///   characters.
    /// - a non-finite `REAL` — `±Inf` — becomes `null`, because
    ///   `serde_json::Value::from(f64)` maps a non-finite number to `Null`, so
    ///   it cannot be told apart from a real `NULL`. Stored data reaches this
    ///   and not only an expression: `9e999` is a literal SQLite keeps and
    ///   reads back as `real`. NaN does not reach it, because SQLite stores
    ///   NaN as `NULL`.
    /// - `TEXT` that is not valid UTF-8 goes through `String::from_utf8_lossy`,
    ///   which substitutes rather than refuses.
    /// - on the way in, a JSON integer too large for `i64` is bound as an
    ///   `f64`: SQLite's `INTEGER` is an `i64`, and there is nothing wider to
    ///   bind it to.
    ///
    /// The authorizer runs here too, and not only as belt-and-braces:
    /// `sqlite3_stmt_readonly` reports `ATTACH` and `DETACH` as read-only —
    /// they change the connection's configuration rather than any file's
    /// contents — so the readonly gate alone would let a caller attach a
    /// database to the long-lived connection and keep it there.
    pub async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Map<String, Value>>> {
        self.query_within(sql, params, None).await
    }

    /// [`SqliteEventLog::query`] with a deadline.
    ///
    /// `busy_timeout` bounds *waiting for a lock*, which is a different thing
    /// from a statement that is simply expensive — a recursive CTE with a
    /// runaway bound takes the lock immediately and then runs. Without this
    /// there is nothing to interrupt it, and because SQL is served from the
    /// single writer thread, one such statement stalls every append on the log
    /// indefinitely.
    ///
    /// The deadline interrupts the statement through SQLite's own interrupt
    /// handle and reports [`Error::Timeout`], so a caller can decide whether a
    /// narrower query is worth another go. Why that is deliberately not
    /// [`Error::Busy`] is on the variant.
    pub async fn query_timeout(
        &self,
        sql: &str,
        params: Vec<Value>,
        timeout: Duration,
    ) -> Result<Vec<Map<String, Value>>> {
        self.query_within(sql, params, Some(timeout)).await
    }

    async fn query_within(
        &self,
        sql: &str,
        params: Vec<Value>,
        timeout: Option<Duration>,
    ) -> Result<Vec<Map<String, Value>>> {
        let shared = self.shared_handle();
        let sql = sql.to_string();
        let job = move |conn: &mut Connection| {
            Ok(guarded(
                conn,
                Arc::new(AtomicBool::new(false)),
                move |conn| query_rows(conn, &sql, params),
            ))
        };

        // A reader: `query` only reads, and running it on the writer is what
        // let one expensive statement stall every append.
        let isle = shared.reader();
        let outcome = match timeout {
            Some(timeout) => isle.call_timeout(timeout, job).await,
            None => isle.call(job).await,
        };

        match outcome {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }

    /// Your writes and the log's own, in one `IMMEDIATE` transaction.
    ///
    /// The closure is handed a [`TxnContext`]: it derefs to the raw
    /// [`rusqlite::Transaction`] for your tables, and carries `append` / `append_many` /
    /// `read` / `head` as the only route to the log's. Returning `Err` rolls
    /// back everything, appended events included.
    ///
    /// This is how "record this fact **and** update that row, together or not
    /// at all" is expressed — and how two streams are written atomically:
    /// two `append` calls, no grouping rule, no ordering restriction.
    ///
    /// Raw writes to the log's tables are still refused by SQLite's
    /// authorizer, so an append cannot skip validation, sequencing or
    /// ordering. A denial surfaces as [`Error::Unsupported`].
    ///
    /// Subscribers are woken **after** the commit, never during: waking one at
    /// a position a rollback then erases would walk it past a hole it can
    /// never fill.
    ///
    /// Not retried on contention. The closure is `FnOnce` and is not pure —
    /// re-running it would repeat whatever else it did. [`Error::Busy`]
    /// surfaces and the caller decides.
    ///
    /// ```no_run
    /// # use eventsdb_core::Result;
    /// # use eventsdb_sqlite::SqliteEventLog;
    /// # use serde_json::json;
    /// # async fn example(log: &SqliteEventLog) -> Result<()> {
    /// log.with_transaction(|tx| {
    ///     let committed = tx.append(
    ///         "order-1",
    ///         json!({ "kind": "placed" }).as_object().unwrap().clone(),
    ///     )?;
    ///     tx.execute(
    ///         "INSERT INTO my_index (position, stream) VALUES (?1, ?2)",
    ///         rusqlite::params![committed.position.unwrap().get() as i64, "order-1"],
    ///     )
    ///     .map_err(|e| eventsdb_core::Error::storage(e.to_string()))?;
    ///     Ok(())
    /// })
    /// .await
    /// # }
    /// ```
    pub async fn with_transaction<T, F>(&self, body: F) -> Result<T>
    where
        T: Send + 'static,
        F: for<'t> FnOnce(&TxnContext<'t>) -> Result<T> + Send + 'static,
    {
        let shared = self.shared_handle();
        let chain = shared.chain.clone();
        let trusted = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&trusted);

        let job = move |conn: &mut Connection| {
            Ok(guarded(conn, trusted, move |conn| {
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(classify)?;
                let context = TxnContext::new(&tx, chain, flag);
                let value = body(&context)?;
                // Collected before the commit, published after it.
                let positions = context.into_published();
                tx.commit().map_err(classify)?;
                Ok((value, positions))
            }))
        };

        let (value, positions) = match shared.isle.call(job).await {
            Ok(inner) => inner?,
            Err(isle) => return Err(map_isle(isle)),
        };

        if let Some(highest) = positions.into_iter().max() {
            shared.publish(highest);
        }
        Ok(value)
    }
}

/// Run `body` with the authorizer installed, and take it off again — whatever
/// `body` does.
///
/// **Including panicking.** The authorizer sits on a connection that outlives
/// this call, and the isle catches a panicking job and keeps serving on the
/// same connection, so an unwind past the uninstall does not crash anything:
/// it leaves the guard permanently installed, and every subsequent write the
/// store itself makes is refused as `not authorized` while reads keep working.
/// A log that looks alive and silently cannot be written to is the worst
/// failure available here, so the unwind is caught and reported rather than
/// allowed past this frame.
pub(crate) fn guarded<T, F>(conn: &mut Connection, trusted: Arc<AtomicBool>, body: F) -> Result<T>
where
    F: FnOnce(&mut Connection) -> Result<T>,
{
    let flag = Arc::clone(&trusted);
    // Refused rather than reported, because a failure here is the guard not
    // being installed, and `body` is the untrusted statement it exists to
    // stand in front of. Running it anyway would turn an enforced boundary
    // into a documented one at the moment it matters.
    conn.authorizer(Some(move |context: AuthContext<'_>| {
        authorize(&context, flag.load(Ordering::SeqCst))
    }))
    .map_err(|e| Error::storage(format!("could not install the authorizer: {e}")))?;

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(conn)));

    // The uninstall's own failure is not allowed to replace `outcome`. A
    // guard left installed refuses every later write on this connection,
    // including the store's own, so it is reported — but after the call this
    // frame was asked to make has been accounted for, not instead of it.
    let uninstalled = conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
    trusted.store(false, Ordering::SeqCst);
    if let Err(e) = uninstalled {
        return Err(Error::storage(format!(
            "the authorizer could not be removed, so this connection now \
             refuses every write: {e}"
        )));
    }

    match outcome {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(map_denial(error)),
        Err(panic) => Err(Error::storage(format!(
            "the statement panicked: {}",
            panic_message(&panic)
        ))),
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown".to_string()
    }
}
