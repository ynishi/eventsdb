//! Turning rows back into events, and events into rows.
//!
//! The envelope is columns and the two JSON levels are `TEXT`. That split is
//! the reason a SQL view over the log is stable: a view that reads `stream`,
//! `kind` or `epoch_ms` is reading columns, and no change to what a kind
//! records can move them.

use eventsdb_core::error::{Error, Result};
use eventsdb_core::event::{
    FIELD_DATA, FIELD_EPOCH_MS, FIELD_KIND, FIELD_META, FIELD_SCHEMA_VERSION, FIELD_SEQ,
};
use rusqlite::Row;
use serde_json::{Map, Value};

/// The column list every read shares, so the row indices below are the same
/// in each of them.
pub const COLUMNS: &str = "position, stream, seq, epoch_ms, kind, schema_version, meta, data";

/// One row of [`COLUMNS`], still as stored: no upcaster has run.
pub struct StoredRow {
    pub position: u64,
    pub stream: String,
    pub event: Value,
}

/// Read a row of [`COLUMNS`] into the event object it was written from.
pub fn read(row: &Row<'_>) -> rusqlite::Result<StoredRow> {
    let position: i64 = row.get(0)?;
    let stream: String = row.get(1)?;
    let seq: i64 = row.get(2)?;
    let epoch_ms: i64 = row.get(3)?;
    let kind: String = row.get(4)?;
    let schema_version: i64 = row.get(5)?;
    let meta: String = row.get(6)?;
    let data: String = row.get(7)?;

    let mut event = Map::new();
    event.insert(FIELD_KIND.to_string(), Value::String(kind));
    event.insert(FIELD_META.to_string(), parse(&meta, FIELD_META, position)?);
    event.insert(FIELD_DATA.to_string(), parse(&data, FIELD_DATA, position)?);
    event.insert(FIELD_SEQ.to_string(), Value::from(seq as u64));
    event.insert(FIELD_EPOCH_MS.to_string(), Value::from(epoch_ms as u64));
    event.insert(
        FIELD_SCHEMA_VERSION.to_string(),
        Value::from(schema_version as u64),
    );

    Ok(StoredRow {
        position: position as u64,
        stream,
        event: Value::Object(event),
    })
}

/// A stored JSON column that will not parse is a storage failure, never an
/// empty object: a fold over a silently emptied event produces a wrong state
/// rather than an obvious break.
fn parse(text: &str, field: &str, position: i64) -> rusqlite::Result<Value> {
    serde_json::from_str(text).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("event at position {position} has unparsable `{field}`: {error}"),
            )),
        )
    })
}

/// The two JSON columns of a stamped event, as the text they are stored as.
pub fn json_columns(event: &Map<String, Value>) -> Result<(String, String)> {
    let meta = event.get(FIELD_META).unwrap_or(&Value::Null);
    let data = event.get(FIELD_DATA).unwrap_or(&Value::Null);
    let meta = serde_json::to_string(meta)
        .map_err(|error| Error::storage(format!("cannot serialize `{FIELD_META}`: {error}")))?;
    let data = serde_json::to_string(data)
        .map_err(|error| Error::storage(format!("cannot serialize `{FIELD_DATA}`: {error}")))?;
    Ok((meta, data))
}

/// The envelope fields that become columns.
pub fn envelope_columns(event: &Map<String, Value>) -> Result<(String, u64, u64, u64)> {
    let kind = event
        .get(FIELD_KIND)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::storage("stamped event has no `kind`"))?
        .to_string();
    let seq = required_u64(event, FIELD_SEQ)?;
    let epoch_ms = required_u64(event, FIELD_EPOCH_MS)?;
    let schema_version = required_u64(event, FIELD_SCHEMA_VERSION)?;
    Ok((kind, seq, epoch_ms, schema_version))
}

fn required_u64(event: &Map<String, Value>, field: &str) -> Result<u64> {
    event
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::storage(format!("stamped event has no `{field}`")))
}
