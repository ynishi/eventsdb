# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-09-11

A minor rather than a patch, for three independent reasons. `Guard` gains a
variant, `Exported`, so an exhaustive `match` on it stops compiling. `Filter`,
`OpenOptions`, `Guard` and `Completeness` become `#[non_exhaustive]`, so a
struct literal of the first two outside the crate stops compiling as well —
the Changed entry says what to write instead. And the schema moves to
`user_version` 5: a file this release has opened is refused by 0.3.0, which
does not write to a schema it does not know, so a downgrade needs the file
from before the upgrade.

### Added

- **Export receipts and `Guard::Exported`.** `SqliteEventLog::export_recorded`
  is `export` plus a row in a new `exports` table saying the page was handed
  out; `confirm_export(id)` says it landed. `Guard::Exported` chains the
  confirmed, unfiltered receipts from the beginning of the log and refuses,
  with the new `Error::NotExported { up_to, exported_through }`, any plan
  that would remove past the chain's end — and refuses to overrun a consumer,
  as the default guard does. `exported_through()` reports the chain's end.
  Where the export goes stays the caller's; what the store no longer allows is
  for "I exported it first" to be something the caller merely remembers.
- Schema step 5: the `exports` table. `TARGET_USER_VERSION` is 5, and
  `exports` joins `RESERVED_TABLES`, so the hatch can read receipts but not
  write them.
- `OpenOptions::busy_timeout`, `poll_interval`, `upcasters` and `readers`,
  consuming setters in the shape `Filter` already has, so an option can be
  set from a default without a struct literal.

### Changed

- **`Filter`, `OpenOptions`, `Guard` and `Completeness` are
  `#[non_exhaustive]`.** A field or a variant can now be added to any of
  them without breaking a caller — which is what 0.3.0's `Filter` change and
  this release's `Guard::Exported` each did. The cost lands once, here: a
  struct literal of `Filter` or `OpenOptions` outside the crate stops
  compiling, `..Default::default()` included — build from `Filter::all()` /
  `OpenOptions::default()` and the methods, or assign the public fields — and
  an exhaustive `match` on `Guard` or `Completeness` needs a wildcard arm.

## [0.3.0] - 2026-09-10

A minor rather than a patch, for two independent reasons. `Filter` gains a
public field, so a struct literal that named every field stops compiling —
add `..Filter::default()`. And the MSRV moves to 1.85.

The Security entry is the one to act on: a database opened by 0.2.0 was
opened by a SQLite that can corrupt it, and only taking this release changes
that.

### Added

- `SqliteEventLog::replay(from, filter)`: every selected event from `from`
  as a stream that ends when the range runs dry. It is `read_all` paged with
  the cursor fed back in — one borrowed reader per page, nothing held between
  pages — and the same loop as `subscribe`, which waits where `replay`
  returns.
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
- `rusqlite` 0.37 to 0.40 and `rusqlite-isle` 0.5 to 0.6, which move
  together: `libsqlite3-sys` declares `links = "sqlite3"`, so a build graph
  holds exactly one major of it.
- **The MSRV is 1.85**, up from 1.82. `rusqlite` 0.40 pulls `hashlink` 0.12
  and `hashbrown` 0.17, both of which declare 1.85.
- The escape hatch now refuses to run a statement when its authorizer cannot
  be installed. `Connection::authorizer` returns a `Result` in `rusqlite`
  0.40; ignoring it would have let an untrusted statement run with no guard
  in front of it. A failure to *remove* the authorizer is reported too, since
  it leaves the connection refusing every later write.

### Fixed

- **`open` no longer returns `Busy` when two connections create one file at
  the same time.** `apply_pragmas` set `busy_timeout` after the pragma batch
  rather than before it, and `journal_mode = WAL` takes an exclusive lock to
  rewrite the header — so the second of two concurrent opens met that lock
  with the timeout still at its default of zero and failed instead of
  waiting. The reader connections never had this: they set the timeout on
  the builder, before the connection is opened.

### Security

- **The bundled SQLite is no longer one affected by the WAL-reset corruption
  bug.** Two connections on one WAL-mode database, in separate threads or
  processes, writing or checkpointing at the same instant can corrupt the
  file. `SqliteEventLog` opens a writer and two readers on one file by
  default, so that is this crate's standard arrangement rather than an edge
  case. The bug is in every SQLite from 3.7.0 (2010) through 3.51.2 and fixed
  in 3.51.3; `rusqlite` 0.40 brings `libsqlite3-sys` 0.38, which bundles
  3.53.2. The previous pin bundled 3.50.2.

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

[0.4.0]: https://github.com/ynishi/eventsdb/releases/tag/v0.4.0
[0.3.0]: https://github.com/ynishi/eventsdb/releases/tag/v0.3.0
[0.2.0]: https://github.com/ynishi/eventsdb/releases/tag/v0.2.0
[0.1.1]: https://github.com/ynishi/eventsdb/releases/tag/v0.1.1
[0.1.0]: https://github.com/ynishi/eventsdb/releases/tag/v0.1.0
