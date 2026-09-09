//! The log-level export and import.
//!
//! See [`eventsdb_core::transfer`] for what travels and why. This is the pair
//! of calls that move it.

use eventsdb_core::error::Result;
use eventsdb_core::log::Filter;
use eventsdb_core::position::Position;
use eventsdb_core::transfer::{ExportedEvent, ImportReport};
use rusqlite::Connection;
use serde_json::Value;

use crate::log::{select_stored, SqliteEventLog};
use crate::shared::map_isle;

impl SqliteEventLog {
    /// Read events out in position order, as they are stored.
    ///
    /// **Not upcasted.** Every other read runs the chain, because a reader
    /// wants the current shape; an export wants the bytes, so the receiving
    /// store can hold exactly what this one held and run its own chain on
    /// them. Upcasting on the way out would bake this build's reading of an
    /// old event into the copy and lose the original.
    ///
    /// Page with `from` and `limit`: feed the last returned position back in.
    /// A whole log is `export(Position::BEGINNING, &Filter::all(), n)` until a
    /// short batch comes back.
    pub async fn export(
        &self,
        from: Position,
        filter: &Filter,
        limit: usize,
    ) -> Result<Vec<ExportedEvent>> {
        let shared = self.shared_handle();
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
    /// One transaction: a failed import leaves nothing behind, so a retry
    /// starts from a known place rather than from wherever it stopped.
    ///
    /// Each event keeps its `epoch_ms` and its `_schema_version`; `seq` and
    /// `position` are this store's to assign. Imported in order into an empty
    /// store, they come out identical — and
    /// [`ImportReport::reproduced_coordinates`] says whether they did, rather
    /// than leaving a migration to assume it.
    ///
    /// The whole batch is one call into [`crate::TxnContext::import_many`], so
    /// the stream counter is read and written once per stream rather than once
    /// per event. Page a large log through this rather than calling it per
    /// event: one transaction of *n* costs far less than *n* transactions, and
    /// the counter work does not grow with the run.
    pub async fn import(&self, events: Vec<ExportedEvent>) -> Result<ImportReport> {
        if events.is_empty() {
            return Ok(ImportReport::nothing());
        }

        self.with_transaction(move |tx| {
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
}
