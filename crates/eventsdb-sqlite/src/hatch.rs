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
//! the text: writing to the log's tables (`events`, `checkpoints`,
//! `retention`, `sqlite_sequence`), attaching another database, and setting
//! pragmas. Reading those tables is allowed and often the point.
//!
//! The refusals are not paternalism about your data. Each one is an invariant
//! something else in this crate already promised: appends go through
//! [`crate::SqliteEventStore`] so they are stamped and ordered; removals go
//! through [`crate::retention`] so they leave a ledger; `user_version` is the
//! migration ladder's, and `journal_mode` is what the concurrency story rests
//! on.

use eventsdb_core::error::{Error, Result};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, Transaction, TransactionBehavior};
use serde_json::{Map, Value};

use crate::log::SqliteEventLog;
use crate::shared::{classify, map_isle};

/// Tables this crate owns. Reading them is fine; writing them is not.
pub const RESERVED_TABLES: [&str; 4] = ["events", "checkpoints", "retention", "sqlite_sequence"];

fn is_reserved(table: &str) -> bool {
    RESERVED_TABLES
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(table))
}

/// Whether an attempted action is allowed inside the hatch.
///
/// Reads fall through to `Allow`: the log is meant to be queryable, and a
/// projection that joins its read model against `events` is a good use of
/// this.
fn authorize(context: &AuthContext<'_>) -> Authorization {
    // Only guard the main database. A write to an attached file is not ours
    // to police — and attaching is refused below anyway.
    let main = matches!(context.database_name, None | Some("main") | Some(""));

    match context.action {
        AuthAction::Insert { table_name }
        | AuthAction::Delete { table_name }
        | AuthAction::Update { table_name, .. }
        | AuthAction::DropTable { table_name }
        | AuthAction::AlterTable { table_name, .. }
        | AuthAction::CreateTrigger { table_name, .. }
        | AuthAction::DropTrigger { table_name, .. }
        | AuthAction::DropIndex { table_name, .. }
            if main && is_reserved(table_name) =>
        {
            Authorization::Deny
        }

        // Attaching would put tables outside the guard's reach and outside
        // the isle's single-writer discipline.
        AuthAction::Attach { .. } | AuthAction::Detach { .. } => Authorization::Deny,

        // Reading a pragma is fine; setting one is not. `user_version` belongs
        // to the migration ladder, `journal_mode` and `auto_vacuum` to the
        // concurrency and reclaim stories.
        AuthAction::Pragma { pragma_value, .. } if pragma_value.is_some() => Authorization::Deny,

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
    pub async fn query(&self, sql: &str, params: Vec<Value>) -> Result<Vec<Map<String, Value>>> {
        let shared = self.shared_handle();
        let sql = sql.to_string();
        match shared
            .isle
            .call(move |conn: &mut Connection| Ok(query_rows(conn, &sql, params)))
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
            conn.authorizer(Some(|context: AuthContext<'_>| authorize(&context)));

            let outcome = (|| {
                let tx = conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(classify)?;
                let value = body(&tx)?;
                tx.commit().map_err(classify)?;
                Ok(value)
            })();

            // Off again whatever happened: the authorizer is installed on the
            // connection, and the connection outlives this call.
            conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);

            Ok(outcome.map_err(|error| match &error {
                // SQLite reports every denial the same way, so the useful
                // message has to be built here.
                Error::Storage(message) if message.contains("not authorized") => denied(),
                _ => error,
            }))
        };

        match shared.isle.call(job).await {
            Ok(inner) => inner,
            Err(isle) => Err(map_isle(isle)),
        }
    }
}
