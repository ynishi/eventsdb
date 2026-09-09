//! Schema evolution: stored bytes are never rewritten, readers are moved
//! forward instead.
//!
//! Every change to the shape of a stored event ships in the same round as
//! (a) a bump of [`crate::event::CURRENT_SCHEMA_VERSION`], which every new
//! event is stamped with, and (b) an [`Upcaster`] for the `n -> n+1` step,
//! registered in the chain the store is opened with and applied at read time.
//!
//! A round that renames a kind or a field without an upcaster is incomplete:
//! an old log would be silently misread, which is the one failure an
//! append-only store exists to prevent.
//!
//! # This is not the table's migration
//!
//! An upcaster transforms an event's JSON. It cannot add a column, an index
//! or a constraint — those are the *table's* shape, tracked separately by the
//! backend's migration ladder (`PRAGMA user_version` in the SQLite backend).
//! Conflating the two leaves a schema change with no defined place to run.

use std::sync::Arc;

use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::event::{FIELD_KIND, FIELD_SEQ};

/// One `n -> n+1` step.
///
/// Applied to every event read, in registration order, whatever version the
/// event carries: a step is responsible for recognising the shapes it applies
/// to and leaving everything else alone.
pub trait Upcaster: Send + Sync {
    fn upcast(&self, event: Value) -> Value;
}

/// A registered chain, cheap to clone into each store handle.
pub type UpcastChain = Vec<Arc<dyn Upcaster>>;

/// Run `events` through `chain`, in registration order, one event at a time.
pub fn apply_chain(chain: &[Arc<dyn Upcaster>], events: Vec<Value>) -> Vec<Value> {
    if chain.is_empty() {
        return events;
    }
    events
        .into_iter()
        .map(|event| chain.iter().fold(event, |event, step| step.upcast(event)))
        .collect()
}

/// An event as it reads *now*: after the chain, with the envelope fields a
/// reader depends on known to be present.
///
/// Constructing one is the check. A reader that holds a `Current` does not
/// re-test for `kind` or `seq`, which is what keeps those tests from being
/// scattered across every fold in the system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Current(Map<String, Value>);

impl Current {
    /// Take an upcasted event, checking what a reader relies on.
    ///
    /// Fails rather than dropping: a row that cannot be understood must
    /// surface, because a fold over a silently truncated log produces a wrong
    /// state instead of an obvious failure.
    pub fn from_upcasted(event: Value) -> Result<Self> {
        let Value::Object(object) = event else {
            return Err(Error::storage("stored event is not a JSON object"));
        };
        if !object
            .get(FIELD_KIND)
            .map(Value::is_string)
            .unwrap_or(false)
        {
            return Err(Error::storage(format!(
                "stored event has no string `{FIELD_KIND}` after upcasting"
            )));
        }
        if !object.get(FIELD_SEQ).map(Value::is_u64).unwrap_or(false) {
            return Err(Error::storage(format!(
                "stored event has no `{FIELD_SEQ}` after upcasting"
            )));
        }
        Ok(Current(object))
    }

    /// For a value this process just stamped, which is current by construction.
    pub fn assume_current(event: Map<String, Value>) -> Self {
        Current(event)
    }

    pub fn kind(&self) -> &str {
        self.0
            .get(FIELD_KIND)
            .and_then(Value::as_str)
            .expect("checked on construction")
    }

    pub fn seq(&self) -> u64 {
        self.0
            .get(FIELD_SEQ)
            .and_then(Value::as_u64)
            .expect("checked on construction")
    }

    pub fn into_inner(self) -> Map<String, Value> {
        self.0
    }
}

impl std::ops::Deref for Current {
    type Target = Map<String, Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct RenameKind {
        from: &'static str,
        to: &'static str,
    }

    impl Upcaster for RenameKind {
        fn upcast(&self, mut event: Value) -> Value {
            if event[FIELD_KIND] == json!(self.from) {
                event[FIELD_KIND] = json!(self.to);
            }
            event
        }
    }

    #[test]
    fn an_empty_chain_is_the_identity() {
        let events = vec![json!({ "kind": "noted", "seq": 1 })];
        assert_eq!(apply_chain(&[], events.clone()), events);
    }

    #[test]
    fn steps_compose_per_event_in_registration_order() {
        let chain: UpcastChain = vec![
            Arc::new(RenameKind { from: "a", to: "b" }),
            Arc::new(RenameKind { from: "b", to: "c" }),
        ];
        let out = apply_chain(&chain, vec![json!({ "kind": "a", "seq": 1 })]);
        assert_eq!(out[0][FIELD_KIND], json!("c"));
    }

    #[test]
    fn a_step_leaves_an_unrecognised_event_alone() {
        let chain: UpcastChain = vec![Arc::new(RenameKind { from: "a", to: "b" })];
        let out = apply_chain(&chain, vec![json!({ "kind": "z", "seq": 1 })]);
        assert_eq!(out[0][FIELD_KIND], json!("z"));
    }

    #[test]
    fn an_event_without_a_kind_after_upcasting_is_a_storage_failure() {
        let error = Current::from_upcasted(json!({ "seq": 1 })).unwrap_err();
        assert!(matches!(error, Error::Storage(_)));
    }

    #[test]
    fn an_event_without_a_seq_after_upcasting_is_a_storage_failure() {
        let error = Current::from_upcasted(json!({ "kind": "noted" })).unwrap_err();
        assert!(matches!(error, Error::Storage(_)));
    }
}
