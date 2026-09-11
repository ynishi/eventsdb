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
//! attaching another database, and setting a pragma. Reading those tables is
//! allowed and often the point; so is reading a pragma, including the
//! introspection ones that take a table or index name, which
//! [`READ_ONLY_PRAGMAS`] lists; and so is adding your own index to `events`.
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
use eventsdb_core::params::Params;
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

/// Pragmas a caller may read *with an argument*, because the argument names
/// something to describe rather than state to set.
///
/// Setting a pragma is refused and reading one is allowed, but SQLite's
/// authorizer does not draw that line: a pragma's argument arrives in the same
/// slot whether it is an assignment (`PRAGMA user_version = 99`) or a name to
/// describe (`PRAGMA table_info(events)`), and `PRAGMA table_info = events` is
/// a valid read written in assignment form. So the two are told apart by name,
/// and this is the list: a pragma carrying an argument is refused unless it is
/// one of these, and a pragma carrying none is allowed as before.
///
/// An allowlist rather than a denylist of the state-bearing pragmas, so a
/// pragma this crate has not considered is refused rather than admitted.
/// Names are matched case-insensitively, as SQLite matches them.
pub const READ_ONLY_PRAGMAS: [&str; 6] = [
    "table_info",
    "table_xinfo",
    "index_list",
    "index_info",
    "index_xinfo",
    "foreign_key_list",
];

fn is_read_only_pragma(pragma: &str) -> bool {
    READ_ONLY_PRAGMAS
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(pragma))
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
        // concurrency and reclaim stories. Which of the two this is cannot be
        // read off the argument — see [`READ_ONLY_PRAGMAS`] — so the name
        // decides.
        AuthAction::Pragma {
            pragma_name,
            pragma_value: Some(_),
        } if !is_read_only_pragma(pragma_name) => Authorization::Deny,

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
         setting a pragma, or reading one with an argument outside {}. Append \
         through a stream handle, remove through retention, and keep your own \
         tables under your own names",
        RESERVED_TABLES.join(" / "),
        READ_ONLY_PRAGMAS.join(" / ")
    ))
}

/// What one statement through the hatch is allowed to do.
///
/// `#[non_exhaustive]`, so an option can be added without breaking a caller.
/// Start from [`QueryOptions::default`] and set what differs through the
/// methods — `QueryOptions::default().lossy(true)` — or assign the public
/// fields; only the struct literal is reserved.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// A deadline for the statement, as [`SqliteEventLog::query_timeout`]
    /// gives it. `None` is no deadline.
    pub timeout: Option<Duration>,
    /// Accept a substitute for a cell that has no JSON value, instead of
    /// refusing it.
    ///
    /// Strict is the default because a hatch answering with a stand-in is a
    /// hatch whose answers cannot be trusted while debugging, which is what it
    /// is for. What each cell does under either setting:
    ///
    /// ```text
    /// cell             strict (default)                    lossy
    /// ---------------  ----------------------------------  ---------------------------------
    /// BLOB             Unsupported: <col>, use hex(<col>)  String, the bytes as hex(<col>)
    /// REAL +-Inf       Unsupported: <col>, use CAST        String "Inf" / "-Inf", as CAST(<col> AS TEXT)
    /// TEXT not UTF-8   Unsupported: <col>                  String, from_utf8_lossy
    /// bind > i64::MAX  Validation                          Validation (a bind is not a read)
    /// ```
    ///
    /// A substitute is the same bytes SQL itself would have rendered, so the
    /// strict error's advice and this path agree: `hex()` in uppercase, `Inf`
    /// and `-Inf` as `CAST(x AS TEXT)` writes them.
    ///
    /// **Nothing marks a substituted cell.** It arrives as a
    /// [`Value::String`], indistinguishable from a `TEXT` cell holding the
    /// same characters. `serde_json::Value` is not this crate's type, so there
    /// is nowhere to put a tag that would not be a shape this conversion never
    /// otherwise produces — and the caller who set `lossy` is the one who
    /// knows which columns they meant.
    pub lossy: bool,
}

impl QueryOptions {
    /// A deadline for the statement; see the field.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Substitute rather than refuse a cell that has no JSON value; see the
    /// field for what each one becomes.
    pub fn lossy(mut self, lossy: bool) -> Self {
        self.lossy = lossy;
        self
    }
}

/// Run a read-only statement and return its rows as JSON objects.
///
/// The check is SQLite's own `sqlite3_stmt_readonly`, not a scan of the text:
/// a denylist of keywords is a guess about a parser, and this is the parser's
/// answer.
///
/// The rows are walked rather than run through `query_map`, because a refusal
/// from [`sql_to_json`] is this crate's [`Error`] and the closure `query_map`
/// takes can only carry a [`rusqlite::Error`] out. Wrapping one in the other
/// would reach [`classify`] and come back as [`Error::Storage`] — the word for
/// the database failing, which a cell that has no JSON value is not.
pub(crate) fn query_rows(
    conn: &Connection,
    sql: &str,
    params: Params,
    lossy: bool,
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

    let mut rows = match params {
        Params::Positional(values) => {
            let bound = values
                .into_iter()
                .map(bind_value)
                .collect::<Result<Vec<Box<dyn rusqlite::ToSql>>>>()?;
            stmt.query(rusqlite::params_from_iter(bound.iter().map(|p| p.as_ref())))
                .map_err(classify)?
        }
        Params::Named(pairs) => {
            let bound = pairs
                .into_iter()
                .map(|(name, value)| Ok((name, bind_value(value)?)))
                .collect::<Result<Vec<(String, Box<dyn rusqlite::ToSql>)>>>()?;
            // Before the statement runs, not after: an answer computed with a
            // placeholder left at `NULL` is the failure this is here to stop,
            // and it is one that would otherwise look like a result.
            every_declared_name_was_supplied(&stmt, &bound)?;
            let by_name: Vec<(&str, &dyn rusqlite::ToSql)> = bound
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_ref()))
                .collect();
            stmt.query(by_name.as_slice()).map_err(classify)?
        }
    };

    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(classify)? {
        let mut object = Map::new();
        for (index, name) in names.iter().enumerate() {
            object.insert(name.clone(), sql_to_json(row, index, name, lossy)?);
        }
        out.push(object);
    }
    Ok(out)
}

/// Refuse a named set that leaves one of the statement's placeholders unbound.
///
/// rusqlite does not: an unbound named parameter keeps whatever it was last
/// bound with and falls back to `NULL`, so a forgotten `:kind` turns
/// `WHERE kind = :kind` into `WHERE kind = NULL` — a statement that runs,
/// answers nothing, and reports no fault. [`Params`] carries the rule; this is
/// where it is enforced.
///
/// A parameter with no name is a bare `?` or `?N` in a statement being bound by
/// name. Nothing in this call supplies it, so it is the same refusal.
fn every_declared_name_was_supplied(
    stmt: &rusqlite::Statement<'_>,
    bound: &[(String, Box<dyn rusqlite::ToSql>)],
) -> Result<()> {
    for index in 1..=stmt.parameter_count() {
        match stmt.parameter_name(index) {
            Some(name) if bound.iter().any(|(supplied, _)| supplied == name) => {}
            Some(name) => {
                return Err(Error::Validation(format!(
                    "the statement declares the parameter `{name}` and nothing was \
                     bound to it. A name is the placeholder as written in the SQL, \
                     sigil included — `{name}`, not `{}`. An unbound name would be \
                     read as NULL",
                    name.trim_start_matches([':', '@', '$', '?'])
                )))
            }
            None => {
                return Err(Error::Validation(format!(
                    "parameter {index} of this statement is positional and this call \
                     binds by name, so nothing supplies it. Number every placeholder \
                     or name every placeholder — SQLite counts both in one space, and \
                     mixing them is how a parameter ends up bound to the wrong value"
                )))
            }
        }
    }
    Ok(())
}

/// Bind a JSON parameter. Arrays and objects go as their text, which is what
/// `json_extract` and friends expect anyway.
///
/// A JSON integer above `i64::MAX` is refused rather than rounded: SQLite's
/// `INTEGER` is an `i64` and there is nothing wider to bind it to, so the only
/// alternative is an `f64` that compares equal to neighbours it is not. A bind
/// is not a read, so [`QueryOptions::lossy`] does not reach this — the table
/// there says so.
pub(crate) fn bind_value(value: Value) -> Result<Box<dyn rusqlite::ToSql>> {
    Ok(match value {
        Value::Null => Box::new(Option::<String>::None),
        Value::Bool(b) => Box::new(b),
        Value::Number(n) => match n.as_i64() {
            Some(i) => Box::new(i),
            // `is_u64` is what separates "too large for `i64`" from a JSON
            // float, which still binds as an `f64`.
            None if n.is_u64() => {
                return Err(Error::Validation(format!(
                    "{n} is above i64::MAX and SQLite's INTEGER is an i64, so it \
                     cannot be bound as one. Bind it as a string and compare \
                     against text, or narrow the value before binding"
                )))
            }
            None => Box::new(n.as_f64().unwrap_or(0.0)),
        },
        Value::String(s) => Box::new(s),
        other => Box::new(other.to_string()),
    })
}

/// One cell of a result row as JSON.
///
/// Three cells have no JSON value, and under `lossy == false` each is refused
/// by name with the SQL that gets it through.
/// [`QueryOptions::lossy`] carries the table of what either setting does; this
/// is the code that performs it, and the two have to agree.
pub(crate) fn sql_to_json(
    row: &rusqlite::Row<'_>,
    index: usize,
    column: &str,
    lossy: bool,
) -> Result<Value> {
    use rusqlite::types::ValueRef;
    Ok(match row.get_ref(index).map_err(classify)? {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(i) => Value::from(i),
        ValueRef::Real(f) if f.is_finite() => Value::from(f),
        // `Value::from(f64)` answers `Null` for a non-finite number, which
        // cannot be told apart from a real `NULL`. Stored data reaches here
        // and not only an expression: `9e999` is a literal SQLite keeps and
        // reads back as `real`. NaN does not, because SQLite stores it as
        // `NULL`.
        ValueRef::Real(f) => {
            if !lossy {
                return Err(Error::Unsupported(format!(
                    "column `{column}` holds a non-finite REAL, which JSON has no \
                     number for. Select `CAST({column} AS TEXT)` for what SQL \
                     renders, or set `QueryOptions::lossy`"
                )));
            }
            Value::String(infinity_as_text(f))
        }
        ValueRef::Text(t) => match std::str::from_utf8(t) {
            Ok(text) => Value::String(text.to_string()),
            Err(_) if lossy => Value::String(String::from_utf8_lossy(t).into_owned()),
            Err(_) => {
                return Err(Error::Unsupported(format!(
                    "column `{column}` holds TEXT that is not valid UTF-8, and a \
                     JSON string is UTF-8. Set `QueryOptions::lossy` to take it \
                     with the invalid sequences replaced"
                )))
            }
        },
        ValueRef::Blob(bytes) => {
            if !lossy {
                return Err(Error::Unsupported(format!(
                    "column `{column}` holds a BLOB, which JSON has no value for. \
                     Select `hex({column})` for the bytes, or set \
                     `QueryOptions::lossy`"
                )));
            }
            Value::String(hex(bytes))
        }
    })
}

/// The bytes as SQLite's `hex()` writes them: two uppercase digits each, in
/// order, nothing between.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing to a `String` cannot fail.
        let _ = write!(out, "{byte:02X}");
    }
    out
}

/// A non-finite `REAL` as `CAST(x AS TEXT)` renders it: `Inf` or `-Inf`.
fn infinity_as_text(f: f64) -> String {
    if f.is_sign_negative() { "-Inf" } else { "Inf" }.to_string()
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
    /// **What a cell becomes, and what is refused.** `NULL` becomes `null`, an
    /// `INTEGER` a JSON integer, a finite `REAL` a JSON number, and `TEXT` a
    /// JSON string. A cell with no JSON value — a `BLOB`, a non-finite `REAL`,
    /// `TEXT` that is not UTF-8 — is **refused**, as [`Error::Unsupported`]
    /// naming the column and the SQL that gets the value through, rather than
    /// answered with a stand-in nothing in the result says is one. On the way
    /// in, a JSON integer above `i64::MAX` is [`Error::Validation`] for the
    /// same reason.
    ///
    /// [`SqliteEventLog::query_with`] with [`QueryOptions::lossy`] takes the
    /// substitute instead, per statement; the table of what each cell becomes
    /// under either setting is on that field.
    ///
    /// The authorizer runs here too, and not only as belt-and-braces:
    /// `sqlite3_stmt_readonly` reports `ATTACH` and `DETACH` as read-only —
    /// they change the connection's configuration rather than any file's
    /// contents — so the readonly gate alone would let a caller attach a
    /// database to the long-lived connection and keep it there.
    ///
    /// **Parameters bind by position or by name.** `vec![json!("placed")]`
    /// fills `?1`; `vec![(":kind".to_string(), json!("placed"))]` fills
    /// `:kind`, sigil and all. [`Params`] states the rule for a name and what
    /// happens to a name the statement declares and the call leaves out. The
    /// [`eventsdb_core::EventStore`] trait's `query` stays positional — a
    /// `Box<dyn EventStore>` cannot dispatch a generic argument.
    pub async fn query(
        &self,
        sql: &str,
        params: impl Into<Params>,
    ) -> Result<Vec<Map<String, Value>>> {
        self.query_within(sql, params.into(), QueryOptions::default())
            .await
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
        params: impl Into<Params>,
        timeout: Duration,
    ) -> Result<Vec<Map<String, Value>>> {
        self.query_within(sql, params.into(), QueryOptions::default().timeout(timeout))
            .await
    }

    /// [`SqliteEventLog::query`] with the options set per statement.
    ///
    /// [`SqliteEventLog::query`] is this with [`QueryOptions::default`] and
    /// [`SqliteEventLog::query_timeout`] is it with the deadline set; nothing
    /// else about the call differs. The only option that changes an answer
    /// rather than a bound is [`QueryOptions::lossy`], and the table on that
    /// field is what it changes.
    ///
    /// Per statement rather than per log: whoever reaches for this knows that
    /// *this* column is binary, which is not something the log can be told
    /// once. It is on the log and not on [`crate::SqliteEventStore`] — the
    /// handle's `query` is [`eventsdb_core::EventStore`]'s, and the trait
    /// stays strict.
    pub async fn query_with(
        &self,
        sql: &str,
        params: impl Into<Params>,
        options: QueryOptions,
    ) -> Result<Vec<Map<String, Value>>> {
        self.query_within(sql, params.into(), options).await
    }

    async fn query_within(
        &self,
        sql: &str,
        params: Params,
        options: QueryOptions,
    ) -> Result<Vec<Map<String, Value>>> {
        let shared = self.shared_handle();
        let sql = sql.to_string();
        let lossy = options.lossy;
        let job = move |conn: &mut Connection| {
            Ok(guarded(
                conn,
                Arc::new(AtomicBool::new(false)),
                move |conn| query_rows(conn, &sql, params, lossy),
            ))
        };

        // A reader: `query` only reads, and running it on the writer is what
        // let one expensive statement stall every append.
        let isle = shared.reader();
        let outcome = match options.timeout {
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
