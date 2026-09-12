# Changelog

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- The Instrumentation section's dependency line asks for `version = "0.6"`,
  which is the first release that has the `tracing` feature; it said `"0.5"`,
  the number the workspace stood at when the line was written, and a reader
  who pasted it got Cargo refusing a feature 0.5.x does not have. Prose only.

## [0.6.0] - 2026-09-12

A minor rather than a patch, and this time nothing in it breaks a caller. Every
entry below is an addition — six, each closing an issue opened against 0.5.0 —
and the one type that grew a field, `Filter`, is `#[non_exhaustive]`, so its
fourth axis lands without touching a struct literal outside the crate. The
number still moves by a minor because the only patch this project has cut,
0.1.1, shipped sources identical to the release before it, and a patch that
carried five new methods, a fourth read axis, a new way to run a projection, a
physical copy, and a feature flag would read as a fix to 0.5.0 when 0.5.0
needed none. Two additions are manifest-level and stated on their own:
`rusqlite`'s `backup` feature is now on, which pulls no native code the
amalgamation was not already building, and `tracing` 0.1 is an optional
dependency behind a feature that is off by default and out of the graph when
it is. Neither moves the MSRV from 1.85.

### Added

- **The store can be watched.** A `tracing` feature on `eventsdb-sqlite`, off
  by default and forwarded by the `eventsdb` facade, puts a span on the paths
  this project describes in numbers and an event on the two moments that have
  no duration. Nothing in the crate emitted anything before it, so the only
  way to get any of those numbers out of a running process was to wrap each
  call from outside — which cannot see how long the write lock was held, which
  batch size the runner chose, or whether a subscription woke on the commit
  channel or on the poll interval, because all three are inside the call.

  Spans at `debug`: `eventsdb.append` (`stream`, `count`, `position`),
  `eventsdb.decide` (`stream`, `folded`, `appended`), `eventsdb.project`
  (`name`, `batch`, `applied`, `cursor`), `eventsdb.retain` (`plan`, `guard`,
  `removed`, `highest_removed`), `eventsdb.archive` (`plan`, `page`, `pages`,
  `exported`), `eventsdb.open` (`path`, `user_version` either side of the
  ladder) and `eventsdb.hatch` (`op`, `rows` or `appended`). Events: a wake
  per subscription round at `trace`, carrying `woken_by` as `commit` or
  `interval` — the difference the README quotes in microseconds against
  milliseconds, and the one thing about a subscription that cannot be seen
  from outside its loop — and a refusal at `warn` carrying the error's own
  text, for the four cases where the store declines rather than fails:
  `Truncated`, `NotExported`, `ConsumerBehind`, and a statement the hatch's
  authorizer turned down.

  **No event's `data` or `meta` is a field, at any level.** Both are the
  caller's and are where a tenant or a customer id lives, so what is emitted
  is coordinates, counts, durations, a consumer or projection name, and — at
  `debug`, never above — a stream id and a kind. The one caller-authored text
  that is emitted at all is the hatch's `sql`, which a literal may carry data
  through: it is a `trace` event rather than a span field, clipped to 256
  characters, so a subscriber at `debug` sees the statement's row count and
  never its text. `tests/tracing.rs` writes an event whose `data` and `meta`
  hold a distinctive string, runs every instrumented path with a collecting
  subscriber at `trace`, and asserts the string appears nowhere.

  The feature is `dep:tracing` and nothing else: with it off `tracing` is not
  in the dependency graph, the call sites compile to the code that was there
  before, and `PRAGMA user_version` — the one field that costs a query — is
  not read. No `log` dependency; a caller already on `log` reaches this
  through `tracing`'s own compatibility layer. `tracing` 0.1 declares
  `rust-version = "1.65"`, so the MSRV stays at 1.85.

- **An open log can be copied.** `SqliteEventLog::backup_to(path)` takes a
  consistent physical copy of the file while the log is open and being written
  to. `export` is the logical copy and is deliberately only the events, so
  restoring from one means rebuilding every projection and doing without the
  retention ledger — the only remaining evidence of what a removal took. `cp`
  is the physical copy and on a WAL database with a writer open it is not a
  consistent one: the `-wal` file holds committed pages the main file does not
  yet, and a copy taken between two writes can carry half a transaction. This
  is the whole file — events, `stream_seq`, `checkpoints`, the ledger, the
  export receipts, the indices, the append-only trigger and the read-model
  tables projections built — so the copy opens with `open`, finds itself
  already at `TARGET_USER_VERSION`, and resumes its projections from their
  cursors rather than rebuilding them.

  It runs on a **reader** connection, in one step under one read transaction.
  A `with_transaction` or a projection batch holding the write lock while the
  copy runs is neither waited for nor refused: the copy holds what had
  committed when it started, and the transaction in flight lands in the source
  afterwards. The one step matters — the backup API restarts a copy that
  another connection writes under, and this crate has a writer on another
  thread by design, so a stepped copy that yielded between steps is the shape
  that can restart for ever on a busy log.

  **`VACUUM INTO` was measured against SQLite's online backup API and lost on
  the reader, not on the pragmas.** Both were taken from a log this crate
  created, standing at `user_version` 5 with `auto_vacuum` 2 (`INCREMENTAL`),
  and both copies came back at `user_version` 5 and `auto_vacuum` 2 — so
  either would have carried the pragma `reclaim` depends on and that SQLite
  accepts only before the first table exists. What separated them is that a
  reader connection here is pinned with `PRAGMA query_only = 1`, and
  `VACUUM INTO` on one fails with `SQLITE_READONLY`, "attempt to write a
  readonly database". It is the pragma and not the `SQLITE_OPEN_READ_ONLY`
  flag that refuses it, measured both ways round, and that the destination is
  a different file does not enter into it — so `VACUUM INTO` could only be had
  by moving the copy to the writer, where it would hold every append behind
  it, or by switching the reader's guard off around it. The backup API asks
  the source connection for nothing but reads. `rusqlite`'s `backup` feature
  is therefore enabled; it pulls no extra native code, the C functions being
  in the amalgamation `bundled` already builds. The copy is taken with
  `Backup::step(-1)` rather than `run_to_completion(-1, ..)`, which asserts its
  page count is positive and panics on `-1`.

  **The destination must not exist.** It is taken with an exclusive create, so
  a path that already holds anything — a previous backup most of all — is
  refused as `Error::Validation` rather than overwritten; the backup API
  itself would overwrite it silently, which is a shape that loses data on a
  typo. A failed copy removes the file the call created, so a retry to the
  same path is not refused by the leftover of the attempt before it. Nothing
  is written into the live file and nothing records the copy: this is not a
  restore — restoring is opening the copy — and the `exports` table is for
  pages of events and does not fit one.

- **The log can be listed.** Five methods on `SqliteEventLog` —
  `streams(after, prefix, limit)`, `kinds(after, limit)`,
  `checkpoints(after, limit)`, `retention_ledger(after, limit)` and
  `export_receipts(after, limit)` — answer "which ones are there" for the five
  reserved tables. Every other read takes a name the caller already holds, so
  the only way to ask which streams, kinds, consumers, removals or receipts
  existed was `log.query("SELECT stream, next_seq - 1 FROM stream_seq", …)`
  through the escape hatch: the contract for "list my streams" was the column
  layout of a table the migration ladder owns.

  Each pages, and the shape is `read_all`'s — an exclusive cursor and a limit,
  where the cursor is the ordering key of the last row of the previous page,
  `None` starts at the beginning and a `limit` of 0 is an empty page. One
  method per table rather than a `catalog()` returning all five, because a
  catalogue cannot page and these have to: a log of many short streams has
  many streams. All five read off a reader connection, so a listing does not
  queue behind a write, and none of them is on the `EventLog` trait — that
  stays as it is until a second backend wants them, and `MemEventStore` is one
  stream with nothing to satisfy. No schema change and no new index: three of
  the five page by a primary key and the other two by an index the ladder
  already ships.

  **Retention leaves the five tables in different states, and the listings
  report their own table rather than an average of them.** A removal never
  touches `stream_seq`, so a stream whose every event has been removed still
  lists, with its head `seq` intact and no events under it — the counter is
  the truth of a stream's existence, which is what `Expected::Unwritten`
  already says, and a caller about to reuse the name needs to know it stands
  at 50. No read can report that stream, so the listing is the only thing that
  can. A kind has no counter, so one whose every event has been removed does
  not list: "was this ever written here" stops being answerable, and `kinds`
  says so rather than guessing. The ledger and the receipts are append-only
  and outlive what they describe.

  `StreamInfo { stream, head_seq }` carries `next_seq - 1`, the same
  arithmetic `append_if` does to answer with `Expected::Seq`;
  `ConsumerCheckpoint { consumer, position, updated_ms }` is the read
  `Guard::RegisteredConsumers` makes on the caller's behalf, blind spot
  included — a consumer that never checked in is not there either;
  `RetentionEntry { id, applied_ms, plan, removed_count, highest_removed }`
  hands back the stored description as the string it is, `archive-then-remove:
  <plan>` included, because the store does not interpret it. All four have
  public fields and no `#[non_exhaustive]`, which is what `Report` and
  `ExportReceipt` already are: a row read out of a reserved table is a record
  a caller only ever receives, so the marker would cost the destructuring
  these are read with and buy freedom only a ladder step could use, and a
  ladder step is a version bump on its own.

  `export_receipts` returns `ExportRecord { receipt, taken_ms, landed_ms }`
  rather than growing `ExportReceipt` by two fields. Not only because that
  struct has public fields and no marker, so adding to it is a breaking
  change: the two timestamps are not facts the returning path has.
  `export_recorded` hands back a receipt at the moment it writes the row,
  where `taken_ms` is "now" and `landed_ms` is structurally always `None`.
  They become facts later, which is when this listing reads them — so the
  receipt stays the range that was handed out, and the record is that plus
  what has happened to it since. `record.receipt` is the value
  `export_recorded` returned, the same `id` `confirm_export` takes.

  The prefix on `streams` is a parameter rather than a `streams_with_prefix`
  twin or a `&Filter`. It is `Filter::stream_prefix`'s axis through the same
  `stream_prefix_bound`, so the range walked is the range a read walks — but a
  `Filter` carries four axes and three of them are about *events*, and a
  stream retention has emptied has no event to carry a kind or a `meta` key. A
  listing taking a `Filter` and honouring only the stream half would be a
  filter whose other half silently did nothing, on precisely the streams this
  exists to show.

- **The store runs the archive loop.**
  `SqliteEventLog::archive_then_retain(plan, sink, page)` exports everything
  `plan` would remove, hands each page to a `Sink`, confirms that page's
  receipt only once the sink's write has returned, and then applies the plan
  under `Guard::Exported`. What the caller supplies is the sink. The paging,
  the confirming, the cursor and the stopping — four of the seven lines the
  README used to show — are the store's, and so is the order between them,
  which was the half a caller could get wrong while holding a guard that
  could not stop it.

  A sink error returns unchanged with that page's receipt unconfirmed:
  nothing is removed, `exported_through()` is where the last confirmed page
  left it, and the plan was never applied at all. That is what makes the next
  call a resume rather than a restart — it starts at `exported_through()`, so
  a page already in the chain is not written to a sink twice — and a process
  that dies between a confirmation and the removal loses nothing either,
  because the next call exports nothing and applies the plan. The guard is
  fixed at `Guard::Exported` rather than taken as an argument: one the caller
  could lower is the misuse this exists to remove. It still refuses to
  overrun a registered consumer, and that refusal arriving after the export
  landed wastes nothing, since the chain stays where the confirmations put
  it.

  `Sink` is an async trait — `async fn write(&mut self, page: &[ExportedEvent])`
  — because the sink worth having imports into another log and
  `EventLog::import` is async; a sink over a blocking writer is an `async fn`
  that never awaits. Two ship, in `eventsdb-core` beside `ExportedEvent`
  rather than in the backend, since neither needs SQLite for anything:
  `JsonLinesSink` over any `std::io::Write`, flushed at the end of every page
  because the receipt is confirmed the moment the write returns, and
  `LogSink` over any `&EventLog`, which keeps the receiving log's
  `ImportReport` per page so that `reproduced_coordinates()` can say whether
  the archive is the same log rather than merely the same events. Both are
  re-exported from `eventsdb-sqlite`.

  What is exported is everything from `exported_through()` up to **the
  highest position the plan would remove**, under `Filter::all()` — whole
  pages, since a filtered receipt does not extend the chain `Guard::Exported`
  walks. For `Plan::Before` that is the prefix the plan already is.
  `Plan::OlderThan` and `Plan::Streams` remove a scattered set while the
  chain is a prefix, so the export runs past events they will not remove;
  over-exporting is safe and a chain with holes is the thing retention
  refuses. That reach is read once off a reader and deliberately not in one
  transaction with the export: a `Before` cannot grow, because positions are
  allocated in increasing order and never reused, and where the other two can
  — a backfilled timestamp, an append to a named stream — the plan
  re-evaluated under the write lock reaches past the chain and the guard
  refuses with `NotExported`. A refusal, never a removal of something no sink
  was shown.

  The ledger row an archive writes reads `archive-then-remove: <plan>`, so a
  reader of the ledger can tell a removal that preserved first from one that
  did not; a plain `retain` writes what it always wrote. `Plan`'s doc named
  an archive-then-remove *variant* as the obvious next one, and that variant
  does not fit: a `Plan` is `Clone + Debug` and is stored in the ledger as a
  string, and a sink is none of those things. The doc names the method.

- **A projection can follow the log instead of being polled.**
  `ProjectionRunner::follow` catches up, waits for the next commit, and catches
  up again, returning only on error. The wait is the one `subscribe` already
  does — the log's in-process wake-up channel, with `poll_interval` as the
  ceiling that covers a writer in another process — rather than a sleep, and
  that is the whole point: a sleep loop has to be either slower than a
  subscriber of the same log or short enough to spend a transaction per tick on
  an idle one, and the wake-up that avoids both was already there with nothing
  above it able to reach it. Measured: a commit landed in the read model
  **373µs** after the append against a 30-second poll interval, so a pass of
  that test cannot be the poll arriving early.

  The receiver is subscribed before the first catch-up and every round marks
  the channel seen before its reads, never after them — so a commit landing in
  the gap between the last `run_once` and the wait leaves the receiver changed
  and is folded at once rather than sitting for an interval. What that order
  costs is a spurious round now and then, which is also what a projection that
  names its `kinds` pays for being woken by a commit it declines: a wasted
  read, not a wrong one, and the same cost `subscribe` pays.

  The first error ends the follow and is returned; there is no retry inside,
  because a projection whose `apply` failed has not advanced and retrying it
  without the caller knowing is what the caller's own loop is for. Nothing is
  spawned and nothing is held across the wait: the runner is still one value
  holding its one name in the registry, each batch is still one `IMMEDIATE`
  transaction, and dropping the future while it is parked stops the follow with
  the read model and the cursor agreeing.

- **A read narrows by stream-name prefix.** `Filter::stream_prefix(..)` and the
  public field behind it are a fourth read axis, honoured by everything that
  takes a `Filter`: `read_all`, `subscribe`, `replay`, `export` and
  `export_recorded`. `Filter` is `#[non_exhaustive]`, so a field and a method
  are additive. The axis exists because this store's answer to a stream that
  grows without bound is that a stream is a period — the sessions of one month
  are `session-2026-09-01`, `session-2026-09-02`, … and the streams that belong
  together are then not a set anybody holds, so `streams`, which names every
  stream in full, could not ask for them without the caller enumerating them
  first. No separator is assumed and no category is parsed: a prefix is a range
  on a name the caller chose, and `stream_prefix("sess")` means what it says.

  It compiles to `stream >= ?a AND stream < ?b`, a range rather than
  `LIKE 'p%'`, because two indices lead on `stream` and a range on a leading
  column is a seek, where `LIKE` reaches that plan only under
  `case_sensitive_like` — a connection-wide pragma this crate does not set and
  the hatch refuses to set, so a `LIKE` here would be a predicate whose plan
  depended on state outside the query. The upper bound is the prefix with its
  **last character** moved to the next Unicode scalar value, not its last byte
  incremented. SQLite compares `TEXT` under `BINARY` collation — `memcmp` over
  the stored UTF-8 — so a byte range is the right shape, but an incremented
  last byte need not be valid UTF-8 and there would be nothing to bind: as
  `TEXT` it is not a `String`, and as a `BLOB` it would be worse than wrong,
  since SQLite orders by storage class before value and every `BLOB` sorts
  above every `TEXT`, which would leave `stream < ?b` excluding nothing. UTF-8
  preserves code-point order under bytewise comparison, so the character
  successor falls exactly where the byte increment would. A last character at
  `char::MAX` has no successor, so it is dropped and the character before it
  carries — the carry a trailing `0xFF` takes, exact for the same reason — and
  a prefix that is nothing but `char::MAX`, like the empty prefix, leaves the
  range open above.

  A prefix and `streams` **AND**: a filter carrying both reads the members of
  the set that start with the prefix. Refusing the pair as contradictory would
  have made `Filter` the one place in this API that judges a caller's predicate
  for usefulness. An empty prefix is every stream rather than none of them,
  which is the reading `Some(vec![])` deliberately does not get for `streams`.
  And a prefix makes an export a filtered export: `export_recorded` reports
  `whole = false`, so such a receipt does not extend the chain
  `Guard::Exported` walks, however faithfully it is confirmed.

## [0.5.0] - 2026-09-11

A minor rather than a patch, for two independent reasons. `ExportedEvent`'s
public `position` field becomes an `Option<Position>`, so every construction
and every read of it outside this crate has to say `Some` or handle `None`.
And `SqliteEventLog::query`, `query_timeout` and `query_with` take
`impl Into<Params>` where they took `Vec<Value>` — argument-position
`impl Trait` is a generic parameter, so the number of generic arguments those
methods have changes, which the Rust Reference calls a breaking change for a
caller who turbofishes them. Nobody here does, and every ordinary call site
is untouched: `vec![]`, `Vec::new()` and `vec![json!(..)]` all still compile
as the positional set they always were.

### Added

- **The hatch binds by name, not only by position.** `eventsdb_core::Params`
  (re-exported by `eventsdb-sqlite`) is `Positional(Vec<Value>)` or
  `Named(Vec<(String, Value)>)`, with `From` for `Vec<Value>` — the
  positional set — and for `Map<String, Value>`, the named one;
  `SqliteEventLog::query`, `query_timeout` and `query_with` take anything that
  converts, and `Params::Named(vec![..])` names the variant for pairs already
  in a `Vec`. There is deliberately no conversion from `Vec<(String, Value)>`:
  a second `From` over a `Vec` makes `query(sql, vec![])` ambiguous at every
  existing call site, which is a cost paid by callers who never asked for
  named parameters. SQLite has one
  parameter space and four spellings for a slot in it, and refusing the named
  half guarded no invariant — it pushed the rewriting of `:name` to `?N` onto
  the caller, which means reading the SQL past its string literals, and this
  crate's own example has a `$` inside one (`json_extract(data, '$.n')`). A
  name is the placeholder as written, sigil included: `(":kind", json!("x"))`.
  Every name a statement declares must be supplied — one that is not is
  `Error::Validation` naming it, because rusqlite leaves an unbound named
  parameter at `NULL` and the statement would otherwise answer as though the
  caller had meant that. The `EventStore` trait's `query` and `query_timeout`
  keep `Vec<Value>` and stay positional: the trait is used as
  `Box<dyn EventStore>`, and a dispatchable method may not have type
  parameters.

- **A table this crate did not create is refused at open.** At `user_version`
  0, `migrate` looks in `sqlite_master` before the first ladder step and
  returns `Error::Unsupported` if the file already holds a table under one of
  the names in `RESERVED_TABLES`. It used to run `CREATE TABLE` at such a file
  and pass SQLite's "table events already exists" through as
  `Error::Storage` — corruption's word for something that is not corrupt, on
  a path where retrying is not the question. The check is inside the same
  `IMMEDIATE` transaction that reads the version, so the concurrent-open race
  stays closed. `sqlite_sequence` is excluded: SQLite creates it for any
  `AUTOINCREMENT` table and leaves it behind when that table is dropped, so
  its presence says nothing about a foreign log. The README's "Moving a log"
  now carries the way in — rename the table and any index whose name the
  ladder uses, open, read the old rows through the hatch, `import`, drop what
  is left.

### Changed

- **`ExportedEvent.position` is an `Option<Position>`.** The field is a
  witness rather than an instruction — import reassigns `seq` and `position`
  and never reads it to write — and a record from anywhere but an eventsdb log
  has no witness to carry. The type used to require one, so a caller adopting
  rows from a foreign table wrote `Position::BEGINNING`: that is `Position(0)`,
  a stored position is a rowid and starts at 1, and `0` was therefore already
  serving as "none" in band by a convention nobody had written down.
  `reproduced_coordinates` is now
  `exported.position.is_some_and(|p| landed.position == Some(p))`, so an
  import of witness-less records reports `false` because the records say there
  was nothing to reproduce, rather than by arithmetic accident. In JSON,
  `to_json` omits the key when it is `None` and `from_json` reads a missing
  key and an explicit `null` alike as `None`, still refusing anything else
  that is not a non-negative integer. Struct literals outside the crate need
  `Some(..)`, and a reader of the field needs to handle `None`; the struct
  itself keeps public fields and stays not `#[non_exhaustive]`, because it is
  a record a caller both receives and builds. A 0.4.0 reader cannot parse a
  line that omits `position`; that direction was never promised.

- A parameter set that does not match its statement is `Error::Validation`
  where it was `Error::Storage`. rusqlite raises `InvalidParameterName` and
  `InvalidParameterCount` before SQLite runs anything, so nothing about the
  database has failed — a positional count mismatch used to arrive as
  `Storage("Wrong number of parameters passed to query. Got 1, needed 2")`,
  which is the class that tells a caller to consider the database, for the
  caller's own typo. The message carries the name, or both counts.

- The escape hatch's prose says what its code does. `query_timeout` documented
  `Error::Busy` and returns `Error::Timeout`; `SqliteEventLog::query` now
  states what each SQLite type becomes as JSON, including the four conversions
  that lose information — a `BLOB`, a non-finite `REAL`, `TEXT` that is not
  UTF-8, and an inbound integer too large for `i64`; and the README's escape
  hatch section no longer calls a second writing connection unsurvivable, which
  `tests/two_logs.rs` measured and disproved. Nothing in the code changed.

- **The hatch refuses a cell it cannot hand over intact, where it used to
  substitute.** This is a behaviour change, and it is the four conversions the
  entry above documented: `SqliteEventLog::query` and `EventStore::query`
  return `Error::Unsupported` for a `BLOB`, a non-finite `REAL`, and `TEXT`
  that is not valid UTF-8, naming the column and the SQL that gets the value
  through — `hex(<col>)` and `CAST(<col> AS TEXT)`; and `Error::Validation`
  for an inbound JSON integer above `i64::MAX` instead of rounding it through
  an `f64`. A caller who relied on the string `"<blob>"`, or on `null` for an
  infinite real, now gets an error rather than a value they could not tell
  from a genuine one. To keep the old shape of answer, call the new
  `SqliteEventLog::query_with` with `QueryOptions::default().lossy(true)`: it
  substitutes per statement, and what it substitutes is what SQL itself
  renders — `00FF` for `x'00ff'`, `Inf` and `-Inf` for `±` infinity,
  `from_utf8_lossy` for the text — so it is a string rather than the
  `"<blob>"` marker or the `null`. `QueryOptions` is `#[non_exhaustive]` with
  `Default`, public fields and the setters `timeout` and `lossy`, as
  `OpenOptions` has. `query_with` is on the log only; the `EventStore` trait's
  `query` and `query_timeout` stay strict.

### Fixed

- **Opening a file another connection is holding waits `busy_timeout` instead
  of failing at once.** Two opens of the same fresh file could hand one of
  them `Error::Busy` in well under a millisecond, whatever timeout the caller
  had set — which is the opposite of what setting one says. The statement that
  lost is `PRAGMA journal_mode = WAL`: it reads the header's version bytes
  under a shared lock and then writes them, and a read promoted to a write
  inside one statement is the case `sqlite3_busy_handler` documents as exempt,
  because invoking the handler there could leave two connections each waiting
  for the other. SQLite returns `SQLITE_BUSY` immediately and expects the
  loser to let go and try again, which is now what `apply_pragmas` does:
  the batch is retried, backing off from 1 ms to 50 ms, until it succeeds or
  `busy_timeout` has passed from the first attempt, and then the last failure
  is returned. The timeout stays the bound — nothing waits longer than the
  caller asked — and a file already in WAL finds the version bytes set and
  never reaches the write, so nothing about it changes.

- **The hatch no longer refuses `PRAGMA table_info(events)`.** The authorizer
  denied any pragma SQLite handed it a value for, while the documentation
  promised that only *setting* one was refused. SQLite passes a pragma's
  argument in that slot whether it is an assignment or a name to describe, so
  every introspection pragma taking a table or index name was refused — and
  refused with a message saying the caller had tried to set one. The rule is
  now by name, against the allowlist in `READ_ONLY_PRAGMAS`: `table_info`,
  `table_xinfo`, `index_list`, `index_info`, `index_xinfo` and
  `foreign_key_list` may carry an argument, anything else carrying one is
  still refused, and a pragma carrying none is allowed as before. An
  allowlist, so a pragma this crate has not considered defaults to refused.

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

[Unreleased]: https://github.com/ynishi/eventsdb/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/ynishi/eventsdb/releases/tag/v0.6.0
[0.5.0]: https://github.com/ynishi/eventsdb/releases/tag/v0.5.0
[0.4.0]: https://github.com/ynishi/eventsdb/releases/tag/v0.4.0
[0.3.0]: https://github.com/ynishi/eventsdb/releases/tag/v0.3.0
[0.2.0]: https://github.com/ynishi/eventsdb/releases/tag/v0.2.0
[0.1.1]: https://github.com/ynishi/eventsdb/releases/tag/v0.1.1
[0.1.0]: https://github.com/ynishi/eventsdb/releases/tag/v0.1.0
