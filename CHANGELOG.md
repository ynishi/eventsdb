# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Filter::meta(key, value)`: a third read axis. A cross-stream read, a
  subscription and an export narrow on a `meta` key holding a scalar, ANDed
  with `kinds` and `streams`. An event without the key is out of the answer;
  a `null` or structured value is refused as validation rather than matched
  against nothing. `Filter` gains the public field `meta` (existing struct
  literals need `..Filter::default()`).
- `SqliteEventLog::index_meta(key)`: an expression index on exactly the
  `json_extract` the predicate uses, so SQLite can use it. Idempotent.

### Changed

- The rustdoc now says what `meta` is for — the caller's own properties,
  lifecycle included — and why the store keeps no stream state past the
  sequence counter (`eventsdb_core::event`). `Expected` documents why two
  variants are the whole surface and `StreamExists` is not one of them.
  `Limitations` states that a long stream is designed away, not compacted.

## [0.2.0] - 2026-09-10

One crate joins the workspace and every version moves together, so the three are
readable as one family on crates.io. Nothing in `eventsdb-core` or
`eventsdb-sqlite` changed: their 0.2.0 is their 0.1.1 with a new number.

### Added

- **`eventsdb`**, a facade over the two published crates. It re-exports
  `eventsdb-core` and, behind a `sqlite` feature, `eventsdb-sqlite`. It adds no
  API of its own: depending on the two directly is the same code. The reason it
  exists is the name — two crates were published under a stem that was not, and
  anything published there by anyone else would read as their parent, with no
  way to correct it afterwards.

## [0.1.1] - 2026-09-10

Metadata only. The published sources are identical to 0.1.0.

### Added

- `rust-version = "1.82"`, measured rather than guessed. `libsqlite3-sys`
  declares no MSRV of its own and uses `unsafe extern "C"`, so 1.78 cannot
  build it; 1.82 is the earliest release checked and passing.
- An explicit `include` on both crates, so what ships is stated in the manifest
  rather than left to whatever happens not to be ignored. The file set is
  unchanged — `eventsdb-sqlite` keeps its tests and benches because the README
  cites them by name as the measurements behind its claims.
- This changelog.

## [0.1.0] - 2026-09-10

First release. The write path, the global order, cross-stream reads,
subscriptions, consumer checkpoints, exactly-once projections and retention are
implemented and tested.

### Added

- **An envelope with a fixed shape.** Three keys belong to the caller (`kind`,
  `meta`, `data`) and three to the store (`seq`, `epoch_ms`, `_schema_version`).
  Any other top-level key is refused, so the envelope stays a contract a SQL
  view can be built on.
- **Two traits.** `EventStore` is one stream; `EventLog` is the database —
  reads across streams, subscriptions, checkpoints. `eventsdb-core` carries both
  plus an in-memory backend; `eventsdb-sqlite` is the durable one.
- **A global order with no holes.** The position is allocated inside the
  transaction that commits it, under an `IMMEDIATE` write lock, so allocation
  order and commit order cannot diverge. Following the log is a range read: no
  gap detection, no grace window.
- **Reads that do not queue behind writes.** Read-only connections of their own
  (two by default) rather than the writer's. Three paths stay on the writer
  because a committed-data view would break them: `append_if`'s decision,
  `TxnContext::read`, and a projection's batch.
- **Decisions taken inside the write.** `append_if` folds the stream under the
  lock and appends what it decides. `append_expecting` takes the version it
  believes it read.
- **Appends inside the caller's transaction.** `TxnContext` lets a caller write
  its own rows and the events in one transaction, and read back what it has
  already appended.
- **Exactly-once projections.** When the read model lives in the same file,
  applying an event and advancing the checkpoint are one transaction. No dedupe
  table, no idempotence requirement on the projection author.
- **Schema evolution on two axes.** The event's shape moves with an upcaster
  chain applied on read, versioned by whoever owns the `kind`. The table's shape
  moves with a migration ladder applied at open. Stored bytes are never
  rewritten.
- **Retention that cannot silently make a fold wrong.** Three plans (prefix,
  age, whole streams), a guard that refuses to overrun a registered consumer,
  and a ledger whose watermark turns a short answer into a `Truncated` error.
- **Transfer.** `export` and `import` move a log to another file with its
  positions and its recorded time intact.
- **An escape hatch.** `with_transaction` hands out a real connection for the
  queries this crate does not model, with an authorizer that still refuses
  writes to the log's own tables — an enforced boundary rather than a documented
  one.
- **Append-only enforced in the file.** A trigger in the schema makes a stored
  event impossible to rewrite, whoever opened it. Removal stays uncovered
  because retention deletes as its whole purpose.
- **A Criterion suite** over `append`, `read`, `decide`, `project`, `transfer`
  and `retention`, against a file-backed log. Contention questions live in
  `tests/` instead, where a regression is a failure rather than a slower bar.

[0.2.0]: https://github.com/ynishi/eventsdb/releases/tag/v0.2.0
[0.1.1]: https://github.com/ynishi/eventsdb/releases/tag/v0.1.1
[0.1.0]: https://github.com/ynishi/eventsdb/releases/tag/v0.1.0
