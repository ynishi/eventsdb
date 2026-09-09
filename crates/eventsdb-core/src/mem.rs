//! The in-memory backend.
//!
//! One stream, owned by one handle in one process, which is what makes its
//! writes serialized without a lock. It is the store a test uses and the
//! store an ephemeral session uses; it has no database, so it answers no SQL
//! and assigns no [`crate::position::Position`].

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::error::Result;
use crate::event::{now_ms, stamp, validate};
use crate::position::Committed;
use crate::store::{Decision, EventStore};
use crate::upcast::{apply_chain, Current, UpcastChain};

#[derive(Default)]
pub struct MemEventStore {
    stream_id: String,
    events: Vec<Map<String, Value>>,
    chain: UpcastChain,
}

impl MemEventStore {
    pub fn new(stream_id: impl Into<String>) -> Self {
        MemEventStore {
            stream_id: stream_id.into(),
            events: Vec::new(),
            chain: UpcastChain::new(),
        }
    }

    /// Register the upcaster chain applied to every read.
    pub fn with_upcasters(mut self, chain: UpcastChain) -> Self {
        self.chain = chain;
        self
    }

    /// Read back through the chain, in `seq` order, filtered as `read_kinds`
    /// filters.
    fn project(&self, kinds: Option<&[&str]>, from_seq: u64, limit: usize) -> Result<Vec<Current>> {
        let selected: Vec<Value> = self
            .events
            .iter()
            .filter(|event| stored_seq(event) >= from_seq)
            .filter(|event| match kinds {
                None => true,
                Some(kinds) => stored_kind(event).is_some_and(|k| kinds.contains(&k)),
            })
            .take(limit)
            .map(|event| Value::Object(event.clone()))
            .collect();
        apply_chain(&self.chain, selected)
            .into_iter()
            .map(Current::from_upcasted)
            .collect()
    }

    fn next_seq(&self) -> u64 {
        self.events.len() as u64 + 1
    }
}

#[async_trait]
impl EventStore for MemEventStore {
    fn stream_id(&self) -> &str {
        &self.stream_id
    }

    async fn append(&mut self, event: Map<String, Value>) -> Result<Committed> {
        let seq = self.next_seq();
        let epoch_ms = now_ms();
        let stamped = stamp(event, seq, epoch_ms)?;
        self.events.push(stamped);
        Ok(Committed {
            seq,
            epoch_ms,
            position: None,
        })
    }

    /// Validates the whole batch before writing any of it, which is the only
    /// way this backend's writes fail — so all-or-nothing holds here too.
    async fn append_many(&mut self, events: Vec<Map<String, Value>>) -> Result<Vec<Committed>> {
        for event in &events {
            validate(event)?;
        }
        let mut committed = Vec::with_capacity(events.len());
        for event in events {
            committed.push(self.append(event).await?);
        }
        Ok(committed)
    }

    async fn append_if(
        &mut self,
        kinds: Option<&[&str]>,
        decide: Decision,
    ) -> Result<Option<Committed>> {
        let seen = self.project(kinds, 0, usize::MAX)?;
        match decide(&seen) {
            None => Ok(None),
            Some(event) => self.append(event).await.map(Some),
        }
    }

    async fn read_kinds(
        &self,
        kinds: Option<&[&str]>,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<Current>> {
        self.project(kinds, from_seq, limit)
    }

    async fn read_last(&self, n: usize) -> Result<Vec<Current>> {
        let from = self.events.len().saturating_sub(n);
        let selected: Vec<Value> = self.events[from..]
            .iter()
            .map(|event| Value::Object(event.clone()))
            .collect();
        apply_chain(&self.chain, selected)
            .into_iter()
            .map(Current::from_upcasted)
            .collect()
    }

    async fn head(&self) -> Result<Option<u64>> {
        Ok(self.events.last().map(stored_seq))
    }

    async fn len(&self) -> Result<usize> {
        Ok(self.events.len())
    }
}

fn stored_seq(event: &Map<String, Value>) -> u64 {
    event
        .get(crate::event::FIELD_SEQ)
        .and_then(Value::as_u64)
        .expect("every stored event was stamped")
}

fn stored_kind(event: &Map<String, Value>) -> Option<&str> {
    event.get(crate::event::FIELD_KIND).and_then(Value::as_str)
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

    fn event(kind: &str) -> Map<String, Value> {
        object(json!({ "kind": kind }))
    }

    #[tokio::test]
    async fn seq_starts_at_one_and_increases() {
        let mut store = MemEventStore::new("s");
        assert_eq!(store.append(event("a")).await.unwrap().seq, 1);
        assert_eq!(store.append(event("b")).await.unwrap().seq, 2);
        assert_eq!(store.head().await.unwrap(), Some(2));
        assert_eq!(store.len().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn a_rejected_event_consumes_no_sequence_number() {
        let mut store = MemEventStore::new("s");
        store.append(event("a")).await.unwrap();
        assert!(store.append(object(json!({ "nope": 1 }))).await.is_err());
        assert_eq!(store.append(event("b")).await.unwrap().seq, 2);
    }

    #[tokio::test]
    async fn a_batch_that_fails_validation_writes_none_of_itself() {
        let mut store = MemEventStore::new("s");
        let batch = vec![event("a"), object(json!({ "nope": 1 }))];
        assert!(store.append_many(batch).await.is_err());
        assert_eq!(store.len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn reads_filter_by_kind_and_limit_counts_what_came_back() {
        let mut store = MemEventStore::new("s");
        for kind in ["a", "b", "a", "b", "a"] {
            store.append(event(kind)).await.unwrap();
        }
        let read = store.read_kinds(Some(&["a"]), 0, 2).await.unwrap();
        assert_eq!(read.len(), 2);
        assert!(read.iter().all(|e| e.kind() == "a"));
        assert_eq!(read[0].seq(), 1);
        assert_eq!(read[1].seq(), 3);
    }

    #[tokio::test]
    async fn an_empty_kind_slice_selects_nothing() {
        let mut store = MemEventStore::new("s");
        store.append(event("a")).await.unwrap();
        assert!(store.read_kinds(Some(&[]), 0, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_decision_sees_the_filtered_stream_and_may_write_nothing() {
        let mut store = MemEventStore::new("s");
        store.append(event("a")).await.unwrap();
        store.append(event("b")).await.unwrap();

        let refused = store
            .append_if(
                Some(&["a"]),
                Box::new(|seen| {
                    assert_eq!(seen.len(), 1);
                    assert_eq!(seen[0].kind(), "a");
                    None
                }),
            )
            .await
            .unwrap();
        assert!(refused.is_none());
        assert_eq!(store.len().await.unwrap(), 2);

        let written = store
            .append_if(
                None,
                Box::new(|seen| {
                    Some(object(
                        json!({ "kind": "c", "data": { "saw": seen.len() } }),
                    ))
                }),
            )
            .await
            .unwrap();
        assert_eq!(written.unwrap().seq, 3);
    }

    #[tokio::test]
    async fn read_last_returns_the_end_in_seq_order() {
        let mut store = MemEventStore::new("s");
        for kind in ["a", "b", "c"] {
            store.append(event(kind)).await.unwrap();
        }
        let last = store.read_last(2).await.unwrap();
        assert_eq!(last.len(), 2);
        assert_eq!(last[0].kind(), "b");
        assert_eq!(last[1].kind(), "c");
    }

    #[tokio::test]
    async fn this_backend_assigns_no_global_position() {
        let mut store = MemEventStore::new("s");
        assert!(store.append(event("a")).await.unwrap().position.is_none());
    }
}
