//! Moving a log somewhere else, and getting the same log back.
//!
//! # Why this is part of the store and not an afterthought
//!
//! A log that cannot be moved is a log its owner cannot leave, and every
//! system that already has one has to be able to bring it. Retention removes
//! history; this is the other half — history that outlives the file it was
//! written in.
//!
//! # What travels, and what is reassigned
//!
//! An [`ExportedEvent`] carries the whole stored object: `kind`, `meta`,
//! `data`, `epoch_ms` and `_schema_version`. Import keeps all of it and
//! reassigns only `seq` and `position`, because those are allocations of the
//! receiving store — the same split [`crate::event::restamp`] draws.
//!
//! Keeping `_schema_version` is the one that matters. An old event re-stamped
//! as current falls out of reach of the upcaster written for it, and is then
//! read as though it had a shape it never had. A migration that quietly did
//! that would produce a log that looks right and is not.
//!
//! # The witnesses
//!
//! Each record also carries the `stream` and the `position` it had where it
//! came from. `position` is not restored — it is kept so an import into an
//! empty store can *check* that it reproduced the same log rather than merely
//! claim to. See [`ImportReport::reproduced_coordinates`].
//!
//! # Where a page goes
//!
//! [`Sink`] is the other end of a transfer: one call per page, taking the
//! records a backend just read. It exists so that "export, hand it over,
//! record that it landed" can be one loop inside a backend instead of a
//! sequence each caller reassembles; `SqliteEventLog::archive_then_retain` is
//! that loop. Two sinks ship: [`JsonLinesSink`] over any [`std::io::Write`],
//! and [`LogSink`] over another [`EventLog`].
//!
//! A record from somewhere that is not an eventsdb log has no such witness,
//! and says so: `position` is an `Option` and `None` is the honest answer for
//! a row built out of a foreign table. It used to be required, so the caller
//! bringing rows in from elsewhere wrote `Position::BEGINNING` — which is
//! `Position(0)`, while a stored position is a rowid and starts at 1, so `0`
//! was already serving as "none" in band, by a convention nobody had written
//! down.

use std::io::Write;

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::log::EventLog;
use crate::position::Position;

/// One event as it travels: the stored object, plus where it lived.
///
/// A struct with public fields and no `#[non_exhaustive]`, because it is a
/// record a caller both receives and builds — which is the shape the API
/// guidelines name public fields for, and adding a field to it is a version
/// bump rather than something a marker should hide.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportedEvent {
    pub stream: String,
    /// The position it had in the source log. A witness, not an instruction.
    ///
    /// `None` when there was none: a record built from a table this crate did
    /// not write has no position of ours to carry, and saying so is not the
    /// same as claiming position 0. Import never reads this to write — it
    /// reassigns `seq` and `position` either way — so the only thing it
    /// changes is what [`ImportReport::reproduced_coordinates`] can claim
    /// afterwards, and a record with no witness cannot be said to have landed
    /// back on it.
    ///
    /// **In JSON it is a key that is simply not there.**
    /// [`ExportedEvent::to_json`] omits `position` when it is `None`, and
    /// [`ExportedEvent::from_json`] reads a missing key and an explicit `null`
    /// both as `None`. Anything else that is not a non-negative integer is
    /// still refused. A reader from before this field became optional does not
    /// accept a line without it.
    pub position: Option<Position>,
    /// The stored object, `seq` / `epoch_ms` / `_schema_version` included.
    pub event: Map<String, Value>,
}

impl ExportedEvent {
    /// The record as one JSON object, so a file of these is JSON Lines and
    /// needs nothing but a `serde_json` writer.
    pub fn to_json(&self) -> Value {
        let mut out = Map::new();
        out.insert("stream".to_string(), Value::String(self.stream.clone()));
        // Absence is the key not being there, rather than a `null` a reader
        // has to know to treat as absent — which is how every format that
        // encodes an optional field at all encodes it.
        if let Some(position) = self.position {
            out.insert("position".to_string(), Value::from(position.get()));
        }
        out.insert("event".to_string(), Value::Object(self.event.clone()));
        Value::Object(out)
    }

    pub fn from_json(value: Value) -> Result<Self> {
        let Value::Object(mut object) = value else {
            return Err(Error::validation("an exported record must be an object"));
        };
        let stream = match object.remove("stream") {
            Some(Value::String(stream)) if !stream.is_empty() => stream,
            _ => {
                return Err(Error::validation(
                    "`stream` is required and must be a string",
                ))
            }
        };
        // A missing key and an explicit `null` are the same answer — there was
        // no witness — because a writer that spells absence either way is
        // saying the same thing, and a reader that took one and refused the
        // other would be refusing the line rather than reading it. Anything
        // else is still a fault: a position that is present and unreadable is
        // not an absent one.
        let position = match object.remove("position") {
            None | Some(Value::Null) => None,
            Some(value) => match value.as_u64() {
                Some(position) => Some(Position::new(position)),
                None => {
                    return Err(Error::validation(
                        "`position` must be a non-negative integer, `null`, or absent",
                    ))
                }
            },
        };
        let event = match object.remove("event") {
            Some(Value::Object(event)) => event,
            _ => {
                return Err(Error::validation(
                    "`event` is required and must be an object",
                ))
            }
        };
        crate::event::validate_stored(&event)?;
        Ok(ExportedEvent {
            stream,
            position,
            event,
        })
    }
}

/// What an import did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportReport {
    pub imported: usize,
    /// Where the imported run landed, or `None` if nothing was imported.
    pub first: Option<Position>,
    pub last: Option<Position>,
    /// Whether every event landed at the position it carried.
    ///
    /// True when a log is imported in order into an empty store, which is what
    /// a migration is — and it is the check that the copy really is the same
    /// log rather than merely the same events. False is not an error: merging
    /// into a store that already has history renumbers by design.
    pub reproduced_coordinates: bool,
}

impl ImportReport {
    pub fn nothing() -> Self {
        ImportReport {
            imported: 0,
            first: None,
            last: None,
            reproduced_coordinates: true,
        }
    }
}

/// Where an archived page goes.
///
/// One call per page, between two store calls: the page has been read and a
/// receipt written before `write` is called, and the receipt is confirmed
/// only once it has returned `Ok`. So the only claim a store makes about a
/// sink is that `write` returned — it never reads the sink back and never
/// judges whether the bytes are still there. Where they go, in what format,
/// and with what retry is the sink's, which is to say the caller's.
///
/// An error stops the run and is returned unchanged. The page's receipt then
/// stays unconfirmed, which is what keeps it out of the chain a guard walks.
///
/// **Async because the useful sink is.** Importing into another log is
/// [`EventLog::import`], which is async; a sink over a blocking writer is an
/// `async fn` that never awaits, which costs a `Box` per page and nothing
/// else. The alternative — a sync trait plus a separate method for the log
/// case — would make the two destinations two APIs.
#[async_trait]
pub trait Sink: Send {
    /// Take one page, in position order.
    async fn write(&mut self, page: &[ExportedEvent]) -> Result<()>;
}

/// A [`Sink`] that writes one JSON object per line.
///
/// [`ExportedEvent::to_json`] renders each record, so a file of them is JSON
/// Lines and reads back with nothing but a line splitter and
/// [`ExportedEvent::from_json`].
///
/// **Flushed at the end of every page, not at the end of the run.** The
/// page's receipt is confirmed as soon as `write` returns, so anything still
/// sitting in the writer at that moment would be vouched for and not written.
/// What the operating system then does with the bytes is outside this type: a
/// sink that must survive the machine losing power wraps a writer that syncs.
#[derive(Debug)]
pub struct JsonLinesSink<W> {
    writer: W,
}

impl<W: Write> JsonLinesSink<W> {
    pub fn new(writer: W) -> Self {
        JsonLinesSink { writer }
    }

    /// The writer back, for the caller that needs what it collected — a
    /// `Vec<u8>` to inspect, a file to close in its own order.
    pub fn into_inner(self) -> W {
        self.writer
    }

    pub fn get_ref(&self) -> &W {
        &self.writer
    }
}

#[async_trait]
impl<W: Write + Send> Sink for JsonLinesSink<W> {
    async fn write(&mut self, page: &[ExportedEvent]) -> Result<()> {
        for record in page {
            let line = serde_json::to_vec(&record.to_json())
                .map_err(|error| Error::storage(format!("rendering an export: {error}")))?;
            self.writer
                .write_all(&line)
                .and_then(|()| self.writer.write_all(b"\n"))
                .map_err(|error| Error::storage(format!("writing an export: {error}")))?;
        }
        self.writer
            .flush()
            .map_err(|error| Error::storage(format!("flushing an export: {error}")))
    }
}

/// A [`Sink`] that imports each page into another log.
///
/// "Archive into another eventsdb file" is this sink over the receiving log.
/// Each page is one [`EventLog::import`] — one transaction, so a page lands
/// whole or not at all — and a refusal arrives at the caller of the loop as
/// the error it was, leaving that page's receipt unconfirmed.
///
/// The receiving log's answer is kept rather than dropped: [`LogSink::reports`]
/// is one [`ImportReport`] per page in the order the pages went, and
/// [`LogSink::reproduced_coordinates`] is their conjunction — true when every
/// event landed on the position it carried, which is what archiving a prefix
/// in order into an empty target does.
pub struct LogSink<'a, L: EventLog + ?Sized> {
    target: &'a L,
    reports: Vec<ImportReport>,
}

impl<'a, L: EventLog + ?Sized> LogSink<'a, L> {
    pub fn new(target: &'a L) -> Self {
        LogSink {
            target,
            reports: Vec::new(),
        }
    }

    /// One report per page written, in order. Empty before the first page.
    pub fn reports(&self) -> &[ImportReport] {
        &self.reports
    }

    /// How many events the receiving log took across every page.
    pub fn imported(&self) -> usize {
        self.reports.iter().map(|report| report.imported).sum()
    }

    /// Whether every page landed on the coordinates it carried.
    ///
    /// The conjunction of [`ImportReport::reproduced_coordinates`] over the
    /// pages, and `true` before the first one — the same answer
    /// [`ImportReport::nothing`] gives, for the same reason: nothing has
    /// landed anywhere it should not have.
    pub fn reproduced_coordinates(&self) -> bool {
        self.reports
            .iter()
            .all(|report| report.reproduced_coordinates)
    }
}

impl<L: EventLog + ?Sized> std::fmt::Debug for LogSink<'_, L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogSink")
            .field("reports", &self.reports)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<L: EventLog + ?Sized> Sink for LogSink<'_, L> {
    async fn write(&mut self, page: &[ExportedEvent]) -> Result<()> {
        // An empty page is not a write. `import` already answers one with
        // `ImportReport::nothing`, and a report for a page nobody sent would
        // make `reports()` disagree with how many pages there were.
        if page.is_empty() {
            return Ok(());
        }
        let report = self.target.import(page.to_vec()).await?;
        self.reports.push(report);
        Ok(())
    }
}
