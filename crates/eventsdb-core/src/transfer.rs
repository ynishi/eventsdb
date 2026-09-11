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
//! A record from somewhere that is not an eventsdb log has no such witness,
//! and says so: `position` is an `Option` and `None` is the honest answer for
//! a row built out of a foreign table. It used to be required, so the caller
//! bringing rows in from elsewhere wrote `Position::BEGINNING` — which is
//! `Position(0)`, while a stored position is a rowid and starts at 1, so `0`
//! was already serving as "none" in band, by a convention nobody had written
//! down.

use serde_json::{Map, Value};

use crate::error::{Error, Result};
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
