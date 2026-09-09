# eventsdb

An append-only event store that lives in your process, on SQLite.

It keeps what an event store is for — immutable facts, per-stream ordering,
decisions taken inside the write, schema evolution without rewriting stored
bytes — and drops what needs a cluster.

## Status

Early. The write path, the global order, cross-stream reads, subscriptions,
consumer checkpoints and exactly-once projections are implemented and tested.
Retention is not.

## Crates

| crate | what it is |
|-------|-----------|
| `eventsdb-core` | the envelope contract, schema versioning, the two traits, and an in-memory backend |
| `eventsdb-sqlite` | the durable backend: one file, one writer thread, WAL — plus projections, which need the same connection to be exactly-once |

## The shape of an event

Three keys are yours and three belong to the store:

    {
      "kind": "order_placed",          // required, yours
      "meta": { "customer": "c-12" },  // optional, yours — shallow, scalars only
      "data": { "total": 40 },         // optional, yours — any depth

      "seq": 4,                        // the store's: per-stream, from 1
      "epoch_ms": 1757000000000,       // the store's
      "_schema_version": 1             // the store's
    }

Any other top-level key is refused. That refusal is the point: the envelope is
a stable contract whose keys are columns, `meta` is readable without knowing
the kind, and `data` is the one place a kind's own shape lives. A SQL view
built on the envelope is not broken by a kind changing shape.

`kind` is an opaque string here. Which kinds exist and what their `data` must
contain belong to the layer above.

## Two things a networked event store cannot give you

**Positions have no holes.** The backend allocates a global position inside
the transaction that commits it, under a single writer, so a reader never sees
`n + 1` while `n` is still uncommitted. Following the log is a range read —
no gap detection, no grace window.

**A projection can be exactly-once.** When the read model lives in the same
SQLite file as the log, applying an event and advancing the consumer's
checkpoint are one transaction. No dedupe table, and no idempotence
requirement on the projection author.

Both come from the same place: one writer, one transaction. Distributing the
store forfeits both, which is why this one does not distribute.

## Usage

    use eventsdb_core::{EventLog, EventStore, Filter, Position};
    use eventsdb_sqlite::SqliteEventLog;
    use serde_json::json;

    let log = SqliteEventLog::open("events.db").await?;

    let mut orders = log.stream_handle("order-1");
    orders.append(json!({ "kind": "placed", "data": { "total": 40 } })
        .as_object().unwrap().clone()).await?;

    // Read across every stream, in commit order.
    let batch = log.read_all(Position::BEGINNING, &Filter::all(), 100).await?;

    // Or follow it: catch up, then stay live.
    let mut events = log.subscribe(Position::BEGINNING, Filter::all())?;

### A command with an invariant

`append_if` reads the stream, calls your decision and appends its answer
inside one transaction, so the check runs against the stream as it is at that
instant rather than a head you cached:

    ledger.append_if(Some(&["granted", "spent"]), Box::new(|seen| {
        let balance = fold_balance(seen);
        (balance >= 4).then(|| spend(4))
    })).await?;

### A projection

A projection's `apply` is handed the transaction the log is being read on, so
the fold and the cursor move together:

    impl Projection for Totals {
        fn name(&self) -> &str { "totals" }
        fn kinds(&self) -> Option<Vec<String>> { Some(vec!["scored".into()]) }

        fn init(&mut self, tx: &Transaction<'_>) -> Result<()> { /* CREATE TABLE */ }
        fn reset(&mut self, tx: &Transaction<'_>) -> Result<()> { /* DROP TABLE */ }

        fn apply(&mut self, tx: &Transaction<'_>, event: &Recorded) -> Result<()> {
            // write the read model through `tx`
        }
    }

    let mut runner = log.runner(Totals::new());
    runner.init().await?;
    runner.catch_up().await?;   // or run_once(), or rebuild()

If `apply` fails part-way through a batch, neither the read model nor the
cursor moves — so the retry neither double-counts nor skips. Writing the read
model anywhere other than that transaction gives the guarantee up.

## Schema evolution

Two axes, and they are not the same one:

| axis | subject | mechanism | marker |
|------|---------|-----------|--------|
| event shape | `data`, `meta`, the meaning of a kind | upcaster chain, applied on read | `_schema_version` |
| table shape | columns, indices, constraints | migration ladder, applied at open | `PRAGMA user_version` |

Stored bytes are never rewritten. An upcaster moves the reader forward
instead.

## Limitations

- **Cross-process subscriptions poll.** SQLite has no `LISTEN`/`NOTIFY`, so a
  write from another process is invisible until someone looks. In-process
  subscribers are woken directly.
- **No clustering, replication or network protocol**, by design — see above.
- **Retention is unspecified.** Positions are allocated with `AUTOINCREMENT`
  so that deleting rows will be safe, but what may be deleted, and how a
  consumer whose cursor points into a removed range is told, is not settled.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this crate by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
