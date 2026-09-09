//! The escape hatch: your own SQL, on the log's connection.
//!
//! # Why this has to exist
//!
//! Without it, anyone who needs a query the API does not have opens the
//! database file themselves — and a second *writing* connection is exactly
//! what this store cannot survive. The order positions become visible in is
//! guaranteed by there being one writer allocating inside the committing
//! transaction ([`eventsdb_core::Position`]); a second writer commits on its
//! own schedule, and a subscriber can then pass a position that is still
//! uncommitted and never come back for it. Deleting through a second
//! connection is worse still: it removes events without a retention ledger
//! entry, so nothing downstream is told that a fold is now missing its input.
//!
//! So the hatch is not a convenience. It is the thing that makes "do not open
//! the file yourself" a reasonable rule to state, because there is somewhere
//! else to go.
//!
//! # What it gives and what it refuses
//!
//! [`SqliteEventLog::with_transaction`] hands over a real
//! [`rusqlite::Transaction`] on the log's own connection, inside the isle,
//! under one `IMMEDIATE` transaction. Your tables, your SQL, your schema —
//! and it commits or rolls back with everything else in that transaction.
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

use eventsdb_core::error::{Error, Result};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, Transaction, TransactionBehavior};
use serde_json::{Map, Value};

use crate::log::SqliteEventLog;
use crate::shared::{classify, map_isle};

/// Tables this crate owns. Reading them is fine; writing them is not.
pub const RESERVED_TABLES: [&str; 5] = [
    "events",
    "stream_seq",
    "checkpoints",
    "retention",
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
fn authorize(context: &AuthContext<'_>) -> Authorization {
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
    /// The authorizer runs here too, and not only as belt-and-braces:
    /// `sqlite3_stmt_readonly` reports `ATTACH` and `DETACH` as read-only —
    /// they change the connection's configuration rather than any file's
    /// contents — so the readonly gate alone would let a caller attach a
    /// database to the long-lived connection and keep it there.
    pub async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Map<String, Value>>> {
        let shared = self.shared_handle();
        let sql = sql.to_string();
        match shared
            .isle
            .call(move |conn: &mut Connection| {
                Ok(guarded(conn, move |conn| query_rows(conn, &sql, params)))
            })
            .await
        {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }

    /// Run your own SQL — reads and writes — in one `IMMEDIATE` transaction
    /// on the log's connection.
    ///
    /// This is how a caller keeps its own tables in the same database without
    /// opening a second connection to the file. Returning `Err` rolls the
    /// whole transaction back.
    ///
    /// Writes to the log's own tables are refused by SQLite's authorizer, not
    /// by this crate reading your SQL — see the module docs for which, and
    /// why each one. A denial surfaces as [`Error::Unsupported`].
    ///
    /// ```no_run
    /// # use eventsdb_core::Result;
    /// # use eventsdb_sqlite::SqliteEventLog;
    /// # async fn example(log: &SqliteEventLog) -> Result<()> {
    /// log.with_transaction(|tx| {
    ///     tx.execute_batch("CREATE TABLE IF NOT EXISTS my_view (k TEXT PRIMARY KEY)")
    ///         .map_err(|e| eventsdb_core::Error::storage(e.to_string()))
    /// })
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_transaction<T, F>(&self, body: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Transaction<'_>) -> Result<T> + Send + 'static,
    {
        let shared = self.shared_handle();

        let job = move |conn: &mut Connection| {
            Ok(guarded(conn, move |conn| {
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(classify)?;
                let value = body(&tx)?;
                tx.commit().map_err(classify)?;
                Ok(value)
            }))
        };

        match shared.isle.call(job).await {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
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
fn guarded<T, F>(conn: &mut Connection, body: F) -> Result<T>
where
    F: FnOnce(&mut Connection) -> Result<T>,
{
    conn.authorizer(Some(|context: AuthContext<'_>| authorize(&context)));

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(conn)));

    conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);

    match outcome {
        Ok(Ok(value)) => Ok(value),
        // SQLite reports every denial the same way, so the useful message has
        // to be built here.
        Ok(Err(Error::Storage(message))) if message.contains("not authorized") => Err(denied()),
        Ok(Err(error)) => Err(error),
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
