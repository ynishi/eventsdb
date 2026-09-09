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
    /// Import is the slower half by some way: 5 000 events export in **5.5 ms**
    /// and import in **45 ms** [benched: `transfer` group, release]. Each event
    /// reads and writes the stream counter individually, which a batch could
    /// do once per run — worth doing before anyone moves a large log, and not
    /// worth doing speculatively before then.
    pub async fn import(&self, events: Vec<ExportedEvent>) -> Result<ImportReport> {
        if events.is_empty() {
            return Ok(ImportReport::nothing());
        }

        self.with_transaction(move |tx| {
            let mut first = None;
            let mut last = None;
            let mut reproduced = true;

            for exported in &events {
                let committed = tx.import(exported)?;
                let landed = committed.position;
                if first.is_none() {
                    first = landed;
                }
                last = landed;
                if landed != Some(exported.position) {
                    reproduced = false;
                }
            }

            Ok(ImportReport {
                imported: events.len(),
                first,
                last,
                reproduced_coordinates: reproduced,
            })
        })
        .await
    }
}
