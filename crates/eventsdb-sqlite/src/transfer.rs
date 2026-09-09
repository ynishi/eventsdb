//! The log-level export and import.
//!
//! See [`eventsdb_core::transfer`] for what travels and why. This is what
//! moves it, on this backend.
//!
//! Both are reached through [`eventsdb_core::EventLog`] rather than as
//! inherent methods: being movable is a claim the crate makes, not something
//! this backend happens to offer. The bodies live here as free functions so
//! that the trait impl — which has to sit in one block, in `log.rs` — is two
//! lines and this module stays the place the transfer code is read.

use eventsdb_core::error::Result;
use eventsdb_core::log::Filter;
use eventsdb_core::position::Position;
use eventsdb_core::transfer::{ExportedEvent, ImportReport};
use rusqlite::Connection;
use serde_json::Value;

use crate::log::{select_stored, SqliteEventLog};
use crate::shared::map_isle;

/// Read events out in position order, as they are stored.
///
/// The rows come off a reader connection, so an export does not queue behind
/// whatever the writer is doing. It is the stored shape: no upcaster runs, for
/// the reason [`eventsdb_core::EventLog::export`] gives.
pub(crate) async fn export(
    log: &SqliteEventLog,
    from: Position,
    filter: &Filter,
    limit: usize,
) -> Result<Vec<ExportedEvent>> {
    let shared = log.shared_handle();
    let filter = filter.clone();

    match shared
        .reader()
        .call(move |conn: &mut Connection| {
            Ok(select_stored(conn, from, &filter, limit).map(|rows| {
                rows.into_iter()
                    .map(|row| ExportedEvent {
                        stream: row.stream,
                        position: Position::new(row.position),
                        event: match row.event {
                            Value::Object(object) => object,
                            _ => unreachable!("a stored row is always an object"),
                        },
                    })
                    .collect()
            }))
        })
        .await
    {
        Ok(inner) => inner,
        Err(isle) => Err(map_isle(isle)),
    }
}

/// Write exported events into this log, in the order given.
///
/// One transaction: a failed import leaves nothing behind, so a retry starts
/// from a known place rather than from wherever it stopped.
///
/// The whole batch is one call into [`crate::TxnContext::import_many`], so the
/// stream counter is read and written once per stream rather than once per
/// event. Page a large log through this rather than calling it per event: one
/// transaction of *n* costs far less than *n* transactions, and the counter
/// work does not grow with the run.
pub(crate) async fn import(
    log: &SqliteEventLog,
    events: Vec<ExportedEvent>,
) -> Result<ImportReport> {
    if events.is_empty() {
        return Ok(ImportReport::nothing());
    }

    log.with_transaction(move |tx| {
        let committed = tx.import_many(&events)?;

        let reproduced = committed
            .iter()
            .zip(&events)
            .all(|(landed, exported)| landed.position == Some(exported.position));

        Ok(ImportReport {
            imported: committed.len(),
            first: committed.first().and_then(|c| c.position),
            last: committed.last().and_then(|c| c.position),
            reproduced_coordinates: reproduced,
        })
    })
    .await
}
