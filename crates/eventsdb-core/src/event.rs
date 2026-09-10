//! The event envelope: what a caller may write, and what the store stamps.
//!
//! # The stored shape is envelope + meta + data
//!
//! An event is a JSON object with three caller-facing keys and three the
//! store stamps, **and no others**:
//!
//! | key                  | written by | what it is                                          |
//! |----------------------|------------|-----------------------------------------------------|
//! | [`FIELD_KIND`]       | the caller | required, a string: what happened                    |
//! | [`FIELD_META`]       | the caller | optional, a **shallow** object: scalars only         |
//! | [`FIELD_DATA`]       | the caller | optional (default `{}`), an object of any depth      |
//! | [`FIELD_SEQ`]        | the store  | `u64`, starts at 1, strictly increasing per stream   |
//! | [`FIELD_EPOCH_MS`]   | the store  | `u64`, wall clock at append time                     |
//! | [`FIELD_SCHEMA_VERSION`] | the store | the shape the event was written under             |
//!
//! A top-level key that is none of those is refused. That refusal is the
//! point of the split. When an event is one flat object, an envelope key and
//! a kind's own field sit at the same level, and a reader — a SQL view most
//! of all — cannot tell which of them it is reading. A change to what one
//! kind records then breaks a `json_extract` path silently, because nothing
//! said where the kind's shape ended and the log's began.
//!
//! So the three levels are separated by rule:
//!
//! - the **envelope** is the stable contract. Its keys are never renamed;
//!   they are columns of the log's table, and a view built on them is not
//!   affected by any kind changing shape.
//! - **`meta`** is shallow *by rule* — its values are scalars — so it can be
//!   read without knowing the kind. It is the place for a correlation value,
//!   a label, a flag: anything a reader groups or filters by. A nested value
//!   is refused, and the refusal says where it goes instead.
//! - **`data`** is the one place structured JSON lives, and its shape belongs
//!   to whoever writes the kind. A view that reads a `data` path is updated
//!   in the same round as the kind whose shape it reads — a rule a reader can
//!   follow, because the paths that need watching are all under one key.
//!
//! # `epoch_ms` is a time coordinate, not an ordering key
//!
//! Every event carries a time coordinate ([`FIELD_EPOCH_MS`]) and log
//! positions (`seq`, and the backend's global position). **The positions order
//! the log; the coordinate never does.** Every read is `ORDER BY seq` or
//! `ORDER BY position`, and nothing anywhere sorts by time.
//!
//! Which moment the coordinate names depends on which call wrote the event,
//! and there are exactly three:
//!
//! | written by | `epoch_ms` is |
//! |------------|---------------|
//! | an ordinary append | the wall clock of that write |
//! | a backfill (`append_at`) | the moment the change happened where it came from |
//! | a transfer (`import`) | whatever the source log recorded, unchanged |
//!
//! There is no fourth field saying which — the call you made says it. A second
//! field would have to be trusted to be accurate, and a verb cannot be wrong
//! about itself.
//!
//! `ai-store` draws the same line, and the pairing is worth stating exactly
//! because it is easy to get off by one: its `import_event` is **this crate's
//! `append_at`** — new content, a caller-supplied time, no source log. Its
//! `append` is this crate's `append`. Its counterpart to this crate's
//! `import` is *nothing*: moving whole stored records out of one log and into
//! another, schema version and all, is a verb it does not have.
//!
//! The consequence to know is that a log with backfilled or imported history
//! has a coordinate that is not non-decreasing in position order, so an
//! age-based question ([`crate::log::Filter`]'s consumers, and retention's
//! `OlderThan`) answers differently than it would on an append-only log.
//! Backfilling into an empty stream in chronological order keeps the
//! non-decreasing property by construction, which is the shape to prefer.
//!
//! # The store does not interpret `kind`
//!
//! `kind` is an opaque string here. Which kinds exist, which of them a
//! particular caller may write, and what their `data` must contain are
//! questions for the layer above; this crate checks the envelope and stores
//! the `data` verbatim. A store that knew a vocabulary would be a store that
//! had to be changed to record something new.
//!
//! # `meta` is the caller's property bag, lifecycle included
//!
//! The three levels above say where a value goes. This section says what
//! `meta` is *for*, because the answer decides three things a store could
//! otherwise be asked to own.
//!
//! ```text
//!   what a caller knows about an event      where it goes    who reads it
//!   ─────────────────────────────────────    ─────────────    ───────────
//!   what happened                            kind             everyone
//!   the kind's own content                   data             the kind's author
//!   a property a reader selects by:
//!     an id, a tenant, a correlation value   meta             any reader, no
//!     closed_at, archived, superseded_by     meta             kind knowledge
//!     valid_from / valid_to                  meta             needed
//! ```
//!
//! The last three rows are the point. A stream's lifecycle — that it was
//! closed, that a later stream supersedes it, the period a fact was true for
//! — is knowledge the caller has and the store does not. It is written the
//! way any fact is written: as a key on the event that carries it, on an
//! ordinary append. The store keeps **no stream state past the counter**
//! ([`crate::store::Expected::Unwritten`] is the one question the counter
//! answers), reserves no key, and never reads a `meta` value to decide
//! anything. What `archived: true` means is a projection's to say.
//!
//! This is where the line sits between this store and the ones that grew a
//! first-class surface for the same state. KurrentDB's stream metadata is a
//! reserved `$`-namespace the server interprets, stored as events in a
//! `$$stream`; Marten's `is_archived` is a column so a partition can be
//! pruned. Both are the same knowledge moved into the store, and each pulled
//! a second feature after it — a `StreamExists` expectation to refuse the
//! soft-deleted, default exclusion rules for the archived. Keeping the
//! knowledge in `meta` keeps the store out of the domain and keeps the
//! feature count where it is.
//!
//! What the store owes in return is a **read axis**. A property a reader
//! selects by is only that if a reader can select by it, so
//! [`crate::log::Filter`] matches on `meta` keys, and a backend indexes them
//! on request. Which keys exist is the caller's; that a key can be read
//! cheaply is the store's.
//!
//! Two things `meta` is not:
//!
//! - **Not an identity the store enforces.** An id under `meta` is carried
//!   and indexed, never compared. Two events with the same id are two events.
//!   The deduplication a networked store performs on a caller-supplied id is
//!   a retry protocol for a client that lost a response; in one process the
//!   await returns or the process is gone, and there is nothing to retry.
//! - **Not mutable.** Stored bytes are never rewritten, so a property that
//!   changes is a new event carrying the new value. That is not a limitation
//!   to work around; it is the fact that changed.

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};

use crate::error::{Error, Result};

/// Caller-written: what happened. Required, and a string.
pub const FIELD_KIND: &str = "kind";
/// Caller-written: a shallow object of scalars, for what readers filter by.
pub const FIELD_META: &str = "meta";
/// Caller-written: the kind's own content, of any depth.
pub const FIELD_DATA: &str = "data";
/// Store-written: per-stream sequence, from 1, strictly increasing.
pub const FIELD_SEQ: &str = "seq";
/// Store-written: wall clock at append time, milliseconds since the epoch.
pub const FIELD_EPOCH_MS: &str = "epoch_ms";
/// Store-written: the schema version the event was written under.
pub const FIELD_SCHEMA_VERSION: &str = "_schema_version";

/// The keys a caller may set. Anything else at the top level is refused.
const CALLER_FIELDS: [&str; 4] = [FIELD_KIND, FIELD_META, FIELD_DATA, FIELD_SCHEMA_VERSION];

/// What an event is stamped with when its author did not say.
///
/// # The version belongs to whoever declares the shape
///
/// The store carries this number in a column and hands it to the reader; it
/// does not choose it. Whoever owns a `kind` owns what that kind's `data`
/// looks like, so they own the number that says which shape it is in — the
/// store cannot know, and a number defined by *this crate's* release history
/// would be meaningless to a consumer and to anyone reading an export.
///
/// This is Axon's arrangement: the revision is declared by the event's author,
/// persisted in a column beside the type, and absent is a legal value that the
/// first upcaster selects on. No surveyed event store has one where the store
/// picks the number.
///
/// # Select on `(kind, version)`, never on the version alone
///
/// One number shared by every kind would mean one consumer's bump silently
/// bumped everyone else's. An upcaster should ask "is this *my* kind, at the
/// version I know how to move" — see [`crate::upcast`].
///
/// # The envelope has a version too, and it is not this one
///
/// The `kind`/`meta`/`data` split, and the columns it becomes, can change as
/// well — but an upcaster transforms JSON and cannot add a column, so an
/// envelope change is a storage migration. That number is the backend's
/// migration ladder (`PRAGMA user_version` in the SQLite backend), which runs
/// once at open, before any handle is issued. A database is therefore
/// homogeneous in envelope shape by the time anything reads it, and a
/// per-event envelope version would have nothing to disambiguate.
pub const DEFAULT_SCHEMA_VERSION: u64 = 1;

/// Kept as the old name for the default. Prefer [`DEFAULT_SCHEMA_VERSION`],
/// which says what it now is: a fallback, not the store's opinion.
pub const CURRENT_SCHEMA_VERSION: u64 = DEFAULT_SCHEMA_VERSION;

/// Check that `event` satisfies the envelope contract.
///
/// Called before anything is written, so a rejected event leaves no trace and
/// consumes no sequence number.
pub fn validate(event: &Map<String, Value>) -> Result<()> {
    match event.get(FIELD_KIND) {
        None => return Err(Error::validation(format!("`{FIELD_KIND}` is required"))),
        Some(Value::String(kind)) if kind.is_empty() => {
            return Err(Error::validation(format!(
                "`{FIELD_KIND}` must not be empty"
            )))
        }
        Some(Value::String(_)) => {}
        Some(other) => {
            return Err(Error::validation(format!(
                "`{FIELD_KIND}` must be a string, found {}",
                type_name(other)
            )))
        }
    }

    for (key, value) in event {
        if !CALLER_FIELDS.contains(&key.as_str()) {
            return Err(Error::validation(format!(
                "unknown top-level key `{key}`; a kind's own fields go under `{FIELD_DATA}`, \
                 and a value readers filter by goes under `{FIELD_META}`"
            )));
        }
        match key.as_str() {
            FIELD_META => validate_meta(value)?,
            FIELD_DATA if !value.is_object() => {
                return Err(Error::validation(format!(
                    "`{FIELD_DATA}` must be an object, found {}",
                    type_name(value)
                )))
            }
            FIELD_SCHEMA_VERSION if !value.is_u64() => {
                return Err(Error::validation(format!(
                    "`{FIELD_SCHEMA_VERSION}` must be a non-negative integer, found {}",
                    type_name(value)
                )))
            }
            _ => {}
        }
    }

    Ok(())
}

/// `meta` is one level deep and holds scalars only.
///
/// The rule exists so that a reader can use `meta` without knowing the kind.
/// A nested value there would be a second `data` that no schema governs.
fn validate_meta(meta: &Value) -> Result<()> {
    let Some(object) = meta.as_object() else {
        return Err(Error::validation(format!(
            "`{FIELD_META}` must be an object, found {}",
            type_name(meta)
        )));
    };
    for (key, value) in object {
        match value {
            Value::String(_) | Value::Number(_) | Value::Bool(_) => {}
            other => {
                return Err(Error::validation(format!(
                    "`{FIELD_META}.{key}` must be a string, number or boolean, found {}; \
                     a structured value goes under `{FIELD_DATA}`",
                    type_name(other)
                )))
            }
        }
    }
    Ok(())
}

/// Validate `event`, then fill in the store-written fields.
///
/// `data` is defaulted to `{}` and `meta` to `{}` on the way in, so a reader
/// never has to tell an empty object from a missing one.
pub fn stamp(mut event: Map<String, Value>, seq: u64, epoch_ms: u64) -> Result<Map<String, Value>> {
    validate(&event)?;
    event
        .entry(FIELD_META.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    event
        .entry(FIELD_DATA.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    event.insert(FIELD_SEQ.to_string(), Value::from(seq));
    event.insert(FIELD_EPOCH_MS.to_string(), Value::from(epoch_ms));
    // `or_insert`, not `insert`: the version is the author's to choose, and
    // this is only the value for an author who did not.
    event
        .entry(FIELD_SCHEMA_VERSION.to_string())
        .or_insert_with(|| Value::from(DEFAULT_SCHEMA_VERSION));
    Ok(event)
}

/// Check an event that already carries the store-written fields.
///
/// [`validate`] refuses them, because a caller writing a new event must not
/// choose its own coordinates. An event coming back from another store is the
/// other case: it *has* them, and the point of moving it is that what it
/// carries survives.
pub fn validate_stored(event: &Map<String, Value>) -> Result<()> {
    for (key, value) in event {
        let known = CALLER_FIELDS.contains(&key.as_str())
            || matches!(
                key.as_str(),
                FIELD_SEQ | FIELD_EPOCH_MS | FIELD_SCHEMA_VERSION
            );
        if !known {
            return Err(Error::validation(format!(
                "unknown top-level key `{key}` in a stored event"
            )));
        }
        match key.as_str() {
            FIELD_META => validate_meta(value)?,
            FIELD_DATA if !value.is_object() => {
                return Err(Error::validation(format!(
                    "`{FIELD_DATA}` must be an object, found {}",
                    type_name(value)
                )))
            }
            FIELD_EPOCH_MS | FIELD_SCHEMA_VERSION | FIELD_SEQ if !value.is_u64() => {
                return Err(Error::validation(format!(
                    "`{key}` must be a non-negative integer, found {}",
                    type_name(value)
                )))
            }
            _ => {}
        }
    }

    match event.get(FIELD_KIND) {
        Some(Value::String(kind)) if !kind.is_empty() => {}
        _ => return Err(Error::validation(format!("`{FIELD_KIND}` is required"))),
    }
    for required in [FIELD_EPOCH_MS, FIELD_SCHEMA_VERSION] {
        if !event.get(required).map(Value::is_u64).unwrap_or(false) {
            return Err(Error::validation(format!(
                "a stored event must carry `{required}`"
            )));
        }
    }
    Ok(())
}

/// Take an event that already has its own time and schema version, and give it
/// a sequence number in its new home.
///
/// The counterpart of [`stamp`] for a transfer. `seq` is reassigned because it
/// is an allocation the receiving store owns; `epoch_ms` and
/// `_schema_version` are kept exactly, because they are what the event *is*.
///
/// Keeping `_schema_version` is not a nicety. Re-stamping an old event as
/// current would take it out of reach of the upcaster written for it, and it
/// would then be read as though it had a shape it never had — the one failure
/// an append-only store exists to prevent, arriving through the door marked
/// "migration".
pub fn restamp(mut stored: Map<String, Value>, seq: u64) -> Result<Map<String, Value>> {
    validate_stored(&stored)?;
    stored
        .entry(FIELD_META.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    stored
        .entry(FIELD_DATA.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    stored.insert(FIELD_SEQ.to_string(), Value::from(seq));
    Ok(stored)
}

/// Wall clock in milliseconds. Saturates rather than panicking on a clock set
/// before the epoch: a wrong timestamp is recoverable, a panic inside a write
/// is not.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The JSON type name, for a refusal that says what was actually found.
pub(crate) fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn object(value: Value) -> Map<String, Value> {
        value
            .as_object()
            .expect("test literal is an object")
            .clone()
    }

    #[test]
    fn a_kind_alone_is_a_valid_event() {
        assert!(validate(&object(json!({ "kind": "noted" }))).is_ok());
    }

    #[test]
    fn a_missing_kind_is_refused() {
        let error = validate(&object(json!({ "data": {} }))).unwrap_err();
        assert!(matches!(error, Error::Validation(_)));
    }

    #[test]
    fn an_unknown_top_level_key_is_refused_and_the_message_says_where_it_goes() {
        let error = validate(&object(json!({ "kind": "noted", "amount": 3 }))).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("amount"), "{message}");
        assert!(message.contains("data"), "{message}");
    }

    #[test]
    fn a_nested_meta_value_is_refused() {
        let event = json!({ "kind": "noted", "meta": { "nested": { "x": 1 } } });
        let error = validate(&object(event)).unwrap_err();
        assert!(error.to_string().contains("meta.nested"), "{error}");
    }

    #[test]
    fn scalar_meta_values_pass() {
        let event = json!({ "kind": "noted", "meta": { "s": "x", "n": 1, "b": true } });
        assert!(validate(&object(event)).is_ok());
    }

    #[test]
    fn stamping_fills_defaults_and_store_written_fields() {
        let stamped = stamp(object(json!({ "kind": "noted" })), 7, 1_700_000_000_000).unwrap();
        assert_eq!(stamped[FIELD_SEQ], json!(7));
        assert_eq!(stamped[FIELD_EPOCH_MS], json!(1_700_000_000_000u64));
        assert_eq!(stamped[FIELD_SCHEMA_VERSION], json!(CURRENT_SCHEMA_VERSION));
        assert_eq!(stamped[FIELD_META], json!({}));
        assert_eq!(stamped[FIELD_DATA], json!({}));
    }

    #[test]
    fn a_rejected_event_is_not_stamped() {
        assert!(stamp(object(json!({ "data": {} })), 1, 0).is_err());
    }
}
