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

use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::position::Position;

/// One event as it travels: the stored object, plus where it lived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportedEvent {
    pub stream: String,
    /// The position it had in the source log. A witness, not an instruction.
    pub position: Position,
    /// The stored object, `seq` / `epoch_ms` / `_schema_version` included.
    pub event: Map<String, Value>,
}

impl ExportedEvent {
    /// The record as one JSON object, so a file of these is JSON Lines and
    /// needs nothing but a `serde_json` writer.
    pub fn to_json(&self) -> Value {
        let mut out = Map::new();
        out.insert("stream".to_string(), Value::String(self.stream.clone()));
        out.insert("position".to_string(), Value::from(self.position.get()));
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
        let position = match object.remove("position") {
            Some(value) => match value.as_u64() {
                Some(position) => Position::new(position),
                None => {
                    return Err(Error::validation(
                        "`position` must be a non-negative integer",
                    ))
                }
            },
            None => return Err(Error::validation("`position` is required")),
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
