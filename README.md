# eventsdb

An append-only event store that lives in your process, on SQLite.

It keeps what an event store is for — immutable facts, per-stream ordering,
decisions taken inside the write, schema evolution without rewriting stored
bytes — and drops what needs a cluster.

## Status

Early. The write path, the global order, cross-stream reads, subscriptions,
consumer checkpoints, exactly-once projections and retention are implemented
and tested.

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

## Reads do not queue behind writes

One writer thread owns the connection that appends. Reads are served from
their own read-only connections (two by default, `OpenOptions::readers`), so a
statement does not wait for a write transaction to finish — which is the whole
point of WAL, and was being given away when everything ran on one thread: a
read measured at 73µs idle took **8.2 seconds** with one write open. It is now
367µs [measured, `tests/readers.rs`].

A reader sees committed data, which is the right answer for a standalone read
and the wrong one inside a write transaction — so the reads a projection or a
decide-then-append makes stay on the writer, where they can see the
transaction's own work. An in-memory log has no readers, because each
`:memory:` open is a separate database.

## Two things a networked event store cannot give you

**Positions have no holes.** The backend allocates a global position inside
the transaction that commits it, and `IMMEDIATE` means that transaction holds
the write lock from `BEGIN` — so allocation order and commit order cannot
diverge, and a reader never sees `n + 1` while `n` is still uncommitted.
Following the log is a range read: no gap detection, no grace window. This
holds across connections too, which is measured rather than assumed
(`tests/two_logs.rs`).

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

## Moving a log

A log that cannot be moved is a log its owner cannot leave, and any system
that already has one has to be able to bring it. Export and import are part of
the store, not an afterthought:

    // Page it out. Feed the last position back in for the next batch.
    let batch = log.export(Position::BEGINNING, &Filter::all(), 1000).await?;

    // And back in, in one transaction.
    let report = target.import(batch).await?;
    assert!(report.reproduced_coordinates);

Each record is one JSON object (`ExportedEvent::to_json` / `from_json`), so a
file of them is JSON Lines and needs no format of its own.

**What travels**: `kind`, `meta`, `data`, `epoch_ms` and `_schema_version` —
the whole stored object. **What is reassigned**: `seq` and `position`, because
those are allocations of the receiving store.

Keeping `_schema_version` is the half that matters. An old event re-stamped as
current falls out of reach of the upcaster written for it, and is then read as
a shape it never had — a migration that looks like it worked and did not. The
export is also **not upcasted**, for the same reason: it gives you the bytes,
so the receiving store holds what this one held and runs its own chain.

`reproduced_coordinates` is the check that the copy is the *same log* rather
than merely the same events. It is true for an in-order import into an empty
store, which is what a migration is, and false when merging into a store that
already has history — stated rather than left to assume.

## The escape hatch

Without one, anyone needing a query the API does not have opens the database
file themselves — and a second *writing* connection is what this store cannot
survive. Position order is guaranteed by there being one writer; a second one
commits on its own schedule, and a subscriber can pass a position that is
still uncommitted. Deleting through a second connection is worse: it removes
events with no retention ledger entry, so nothing downstream learns that a
fold is now missing its input.

So the hatch is not a convenience. It is what makes "do not open the file
yourself" a reasonable thing to ask.

    // Read anything, across the log and your own tables.
    let rows = log.query(
        "SELECT stream, json_extract(data, '$.n') AS n FROM events WHERE kind = ?1",
        vec![json!("scored")],
    ).await?;

    // Or take a real transaction on the log's own connection.
    log.with_transaction(|tx| {
        tx.execute_batch("CREATE TABLE IF NOT EXISTS my_view (k TEXT PRIMARY KEY)")?;
        tx.execute("INSERT INTO my_view (k) VALUES ('x')", [])?;
        Ok(())
    }).await?;

Your tables, your SQL, your schema, committed or rolled back with everything
else in that transaction. Inside a projection you already have this — `apply`
is handed the same kind of transaction.

What the hatch refuses, through SQLite's authorizer rather than by reading
your SQL: writing `events`, `stream_seq`, `checkpoints`, `retention` or
`sqlite_sequence`; creating anything that *shares* one of those names in any
schema, `TEMP` included, since a temp table shadows the real one for every
unqualified statement on the connection; attaching another database; setting a
pragma. Reading any of them is allowed and often the point, and so is adding
your own index to `events` — that changes no data, and it is the only way to
make a read cheap when the shipped indices do not cover what you filter on.

Each refusal is an invariant something else already promised — appends get
stamped and ordered by the store, removals leave a ledger, `stream_seq` keeps
`seq` from rewinding after a removal, `user_version` belongs to the migration
ladder, `journal_mode` to the concurrency story.

## Schema evolution

Two axes, and they are not the same one:

| axis | subject | mechanism | marker |
|------|---------|-----------|--------|
| event shape | `data`, `meta`, the meaning of a kind | upcaster chain, applied on read | `_schema_version` |
| table shape | columns, indices, constraints | migration ladder, applied at open | `PRAGMA user_version` |

Stored bytes are never rewritten. An upcaster moves the reader forward
instead.

## Retention

Removal is the one operation that can make a correct-looking read wrong: a
fold that starts before a deleted range comes back short, and nothing in the
shape of the result says so. So it is not just a `DELETE`.

    // Three shapes. Prefix, age, or whole streams.
    log.retain(Plan::Before(Position::new(1000)), Guard::default()).await?;
    log.retain(Plan::OlderThan(cutoff_ms), Guard::default()).await?;
    log.retain(Plan::Streams(vec!["session-7".into()]), Guard::default()).await?;

    log.reclaim().await?;   // give the freed pages back to the filesystem

Two things keep it honest:

- **The default guard refuses to overrun a consumer.** `Guard::RegisteredConsumers`
  fails with `ConsumerBehind` if any stored checkpoint sits below what the plan
  would remove. It can only see consumers that have saved a checkpoint, so have
  yours check in before its first batch. `Guard::Force` removes anyway.
- **What was removed outlives it.** Every application writes a row to a
  retention ledger in the same transaction as the delete, and the highest
  position removed is a watermark. A projection whose cursor sits below the
  watermark is refused with `Truncated` rather than served a short answer, and
  a rebuild on a truncated log is refused *before* the old model is emptied.
  A projection that genuinely does not care — a "last 30 days" view — says so
  with `tolerates_truncation`.

Dropping whole streams leaves holes in the global order. That is safe:
positions are never reused (`AUTOINCREMENT`), and nothing waits for a specific
one — a subscription reads `position > cursor` and does not see what is gone.

## Limitations

- **Subscriptions outside the writing log poll.** SQLite has no
  `LISTEN`/`NOTIFY`, so a write anywhere but the subscriber's own log is
  invisible until someone looks — including a second log opened on the same
  file in the same process, since the wake-up channel belongs to the log.
  Nothing is lost, only delayed, by roughly three orders of magnitude
  [measured: 552µs against 552ms on a 600ms poll].
- **Open the file once per process.** It is safe not to — the order holds —
  but two logs mean the polling latency above, and two logs opened with
  different upcaster chains will read the same bytes differently with nothing
  to detect it.
- **No clustering, replication or network protocol**, by design — see above.
- **Retention deletes; it does not archive.** Taking an `export` before a
  `retain` is what preserves the history — the pieces are here, the policy
  that decides when to do it is not.
- **`reclaim` needs a database created by this version.** It relies on
  `auto_vacuum = INCREMENTAL`, which SQLite only accepts before the first
  table exists. On an older file it does nothing.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this crate by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
