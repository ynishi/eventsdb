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
| `eventsdb` | one name for both: re-exports `eventsdb-core`, and `eventsdb-sqlite` behind the `sqlite` feature |
| `eventsdb-core` | the envelope contract, schema versioning, the two traits, and an in-memory backend |
| `eventsdb-sqlite` | the durable backend: one file, one writer thread, WAL — plus projections, which need the same connection to be exactly-once |

Taking the two directly is the same code; the facade adds nothing but the
name. Everything below names the crates the items come from.

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

`meta` is where a property a reader selects by goes — an id, a tenant, a
correlation value, and the stream's lifecycle too: `closed_at`, `archived`,
`superseded_by`, `valid_from`. The store carries these as it carries any
fact, keeps no stream state of its own past the sequence counter, and never
reads one to decide anything; what `archived: true` means is a projection's
to say. What the store owes in return is a read axis, below. The rustdoc of
`eventsdb_core::event` has the reasoning.

## Time is a coordinate, not an order

Every event carries a time coordinate (`epoch_ms`) and log positions (`seq`,
`position`). **The positions order the log; the coordinate never does.** Every
read is `ORDER BY seq` or `ORDER BY position` — nothing sorts by time.

Which moment the coordinate names is decided by the call that wrote it, and
there are three:

| written by | `epoch_ms` is |
|---|---|
| `append` | the wall clock of that write |
| `append_at` — backfill | the moment the change happened where it came from |
| `import` — transfer | whatever the source log recorded, unchanged |

There is no fourth field recording which: the verb says it. A field would have
to be trusted to be accurate; a verb cannot be wrong about itself.

The consequence worth knowing: a log holding backfilled or imported history has
a coordinate that is not non-decreasing in position order, so `Plan::OlderThan`
removes a scattered set rather than a prefix. That is safe — the watermark
already handles scattered removals — but it is a different answer than the same
call gives on an append-only log. Backfilling into an empty stream in
chronological order keeps the non-decreasing property by construction, and is
the shape to prefer.

## Reads do not queue behind writes

One writer thread owns the connection that appends. Reads are served from
their own read-only connections (two by default, `OpenOptions::readers`), so a
statement does not wait for a write transaction to finish — which is the whole
point of WAL, and was being given away when everything ran on one thread: a
read measured at 73µs idle took **8.2 seconds** with one write open. It is now
367µs [measured, `tests/readers.rs`].

A reader sees committed data, which is the right answer for a standalone read
and the wrong one inside a write transaction — so three read paths stay on the
writer, each because a reader would break it:

| path | why it cannot move |
|------|--------------------|
| `append_if`'s decision | must see the stream under the lock, or it is the compare-and-swap it exists to avoid |
| `TxnContext::read` | must see what the same closure already appended, which is uncommitted |
| a projection's batch | must share a transaction with the completeness check, or retention lands between them |

They hold the write lock while they read, so **other writes wait for them** —
other reads do not. A decision over 20 000 events held the lock ~780ms while a
concurrent read took 536µs. `kinds` is the control and the difference is not
marginal: the same decision was **706ms** reading every kind and **1.05ms**
naming the one it folded. For projections the knob is `with_batch`.

An in-memory log has no readers, because each `:memory:` open is a separate
database.

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

    // Or read all of it and stop: the same loop, ending where the log ends.
    let mut history = log.replay(Position::BEGINNING, Filter::all());

`read_all` is a page: `limit` bounds it and it is exclusive on `from`, so the
last position handled is the next call's `from`. `replay` and `subscribe` are
that call in a loop — each page borrows a reader for one query and nothing is
held between pages, so a stream left half-consumed keeps no connection open
and no read transaction pinning the WAL. `replay` ends at the first short
page; `subscribe` waits there instead.

A read narrows along four axes — `streams`, `stream_prefix`, `kinds`, and
`meta` keys — and combines them by AND. `meta` is equality on a scalar the
caller wrote; an event without the key is out of the answer. The SQLite log
indexes a key on request, on exactly the expression the filter uses:

    log.index_meta("tenant").await?;
    let theirs = Filter::kinds(["placed"]).meta("tenant", "a");
    let batch = log.read_all(Position::BEGINNING, &theirs, 100).await?;

`stream_prefix` is a range on the stream name, which is how a group of
streams is read when a stream is a period — every session of one month,
without naming the days:

    let month = Filter::all().stream_prefix("session-2026-09-");

### A command with an invariant

Two shapes, and which one you want depends on **where the decision was made**,
not on whether the check is atomic — both check inside the write.

**The decision runs at the write** — `append_if` reads the stream, calls your
decision and appends its answer inside one transaction, so the check runs
against the stream as it is at that instant rather than a head you cached:

    ledger.append_if(Some(&["granted", "spent"]), Box::new(|seen| {
        let balance = fold_balance(seen);
        (balance >= 4).then(|| spend(4))
    })).await?;

Prefer this wherever it fits. It folds the stream rather than comparing one
number, so it produces no false conflicts; a decision that finds nothing to do
returns `None` and is idempotent for free; and there is no retry loop, because
there is nothing to retry.

**The decision was made before the call, somewhere this process cannot reach** —
an HTTP client holding an `ETag`, a form somebody filled in, a message that sat
in a queue. Then it is expected-version:

    orders.append_expecting(Expected::Seq(4), cancelled()).await?;
    // Err(HeadMismatch { expected: Seq(4), actual: Seq(6) })  → HTTP 412

The error carries both coordinates, so the caller folds only what it missed
rather than the stream from the start. It is **not** `Busy`: nothing is
contended, and repeating it unchanged fails the same way.

`Expected::Unwritten` means *nothing has ever been appended here* — which is
not the same as "the stream reads empty". Retention can empty a stream whose
counter stands at 50, and the check is against the counter, so a caller meaning
"this is a new order" is not told yes about an order that was archived.

Those two are the whole of `Expected`. There is no `StreamExists`: the state
it was invented to refuse — written, then soft-deleted — does not exist in a
store with no delete, and past the existence check it is `Any`. "Did the
command that creates this stream run?" is a `meta` key on the creating event,
read through `Filter`.

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
    runner.follow().await?;     // and to stay current: returns only on error

`follow` is `catch_up` with the wait `subscribe` uses put between the rounds,
so a read model updates on the commit rather than on a poll of the caller's
choosing. The caller owns the future — `tokio::select!`, or a task of its own —
and dropping it between batches stops the follow with the model and the cursor
agreeing.

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

A record's `position` is an `Option`, and `None` means there was no witness:
the record came from somewhere that is not an eventsdb log, so it carries no
coordinate of ours to check against. An import of such records is
`reproduced_coordinates == false`, because a record that never had a position
here cannot have landed back on it. In JSON the key is simply absent; a
`null` reads the same way, and anything else that is not a non-negative
integer is still refused.

Both calls are on the `EventLog` trait, so a migration is written once against
the trait rather than once per backend. `ExportedEvent` is backend-neutral, and
a log that cannot do this declines rather than offering a partial import: a
transfer that stopped half way is worse than one that refused, because from the
outside there is no way to tell how far it got.

### The logical copy and the physical one

Export is the **logical** copy: the events, as records that carry no SQLite in
them. It is backend-neutral and it survives a change of format — a log exported
today reads into a store whose tables look nothing like these. What it
deliberately leaves behind is everything that is not an event: the read models
projections built, the `checkpoints` they advanced, the retention ledger and
the export receipts. Restoring from an export means rebuilding every projection
and doing without the ledger, which is the only remaining evidence of what
retention removed.

`backup_to` is the **physical** copy: every page of the file, read models and
reserved tables included, in the SQLite file format the file is already in.

    log.backup_to("events-backup.db").await?;

    // Restoring is opening the copy. Nothing writes into the live file.
    let restored = SqliteEventLog::open("events-backup.db").await?;

It runs on a reader connection through SQLite's online backup API, in one step
under one read transaction, so the copy is consistent while the log is open and
a writer holding the lock is neither waited for nor refused: the copy holds
what had committed when it started. That is the part `cp` cannot do — on a WAL
database the `-wal` file holds committed pages the main file does not yet, and
a copy taken between two writes can carry half a transaction.

The destination is a path the caller names, and it must not exist: a backup
that silently overwrote the backup before it is a shape that loses data on a
typo, so an occupied path is refused and the copy is not taken.

Which one to reach for follows from what each keeps. The physical copy is the
same log — positions, per-stream counters, projections at their cursors — and
it is a SQLite file, so it is as portable as SQLite is. The logical copy is the
events, and it is as portable as JSON is.

### A table this crate did not create

The file can already hold an `events` table — a hand-rolled log from before
this crate was adopted, in the file this crate is now asked to open. Opening
it is refused rather than migrated over, because adopting a table of unknown
shape under this crate's `user_version` would be a claim about columns nobody
checked. The refusal names the table and says `user_version` is 0, so a reader
who has hit it is reading the right section.

The way in is a rename and an `import`. Through a raw connection, before the
log is opened:

    ALTER TABLE events RENAME TO legacy_events;
    DROP INDEX events_stream_kind_seq;

The second line is the one that surprises. SQLite carries an index's name
across `ALTER TABLE ... RENAME`, so an index on the renamed table still holds
the name the ladder is about to create, and the open fails a step later than
before. `schema.rs` is where the ladder's index names are; rename or drop
whichever of them the old table carries.

Then the ladder runs clean and the rows come in through the front door:

    let log = SqliteEventLog::open(&path).await?;

    // The hatch reads anything, the old table included.
    let rows = log.query("SELECT * FROM legacy_events ORDER BY id", vec![]).await?;

    // One record per row, carrying the position it had over there.
    let report = log.import(rows.iter().map(to_exported_event).collect()).await?;

    // The old table goes once the report is right.
    log.with_transaction(|tx| {
        tx.execute_batch("DROP TABLE legacy_events")?;
        Ok(())
    }).await?;

Each record's `position` is the coordinate the row had in the old table, and
it is a witness rather than an instruction, as in any other import. So
`reproduced_coordinates` is true only when the import happened to land every
event back on it — which it does when the old keys ran from 1 with no gaps and
the log was empty, and does not when they did not. A table with no key worth
carrying writes `None` and gets `false`, which is the same answer without the
pretence of a coordinate.

`tests/adopt.rs` is this sequence, run.

## What is in the log

Every other read takes a name the caller already holds: a stream id for
`stream_handle`, a consumer for `checkpoint_load`, a receipt id for
`confirm_export`. These five answer "which ones are there", off a reader
connection, each a page with an exclusive cursor and a limit:

    log.streams(None, None, 100).await?;              // name and head seq
    log.streams(None, Some("session-"), 100).await?;  // the same, one prefix
    log.kinds(None, 100).await?;                      // the kinds written
    log.checkpoints(None, 100).await?;                // consumers, and where each sits
    log.retention_ledger(None, 100).await?;           // what went, when, by which plan
    log.export_receipts(None, 100).await?;            // taken, and whether it landed

The first argument is the cursor, and it is exclusive: the key of the last row
of a page — a name, or an id — handed straight back in reads the next one, and
`None` starts at the beginning. That is `read_all`'s contract with a different
key, for the reason `read_all` has it: a log of many short streams has many
streams, and a long-running one has many ledger rows. The prefix on `streams`
is `Filter::stream_prefix`'s axis, over the same range a read walks.

What retention leaves behind differs by table, and each listing reports its
own table rather than a smoothed-over average of them. A removal does not
touch `stream_seq`, so **a stream whose every event has been removed still
lists**, with its head `seq` intact and no events under it — which is what a
caller about to reuse the name needs, since the counter stands at 50 and the
next append is 51. No read can report that stream; there is nothing left of it
to read. A kind has no counter, so a kind whose every event has been removed
does not list at all: "was this ever written here" is not a question the store
can answer once the events are gone, and the listing says so rather than
guessing. The ledger and the receipts are append-only and outlive the events
they describe.

## The escape hatch

Without one, anyone needing a query the API does not have opens the database
file themselves — and what they then do to that file is unbounded. The global
order is not what gives way: two logs on one file were measured, and the order
holds, because a position is allocated inside the transaction that commits it
and every write is `IMMEDIATE`, so two connections cannot both be allocating.
Projections and the retention guard hold too, working as they do through
shared tables. What degrades is the wake-up — a subscription on the other log
sees the write on its poll interval rather than at once, at the cost
`Limitations` measures. Deleting through a second connection is the one
nothing recovers from: it removes events with no retention ledger entry, so
nothing downstream learns that a fold is now missing its input.

So the hatch is not a convenience. It is what makes "do not open the file
yourself" a reasonable thing to ask.

Listing what the log holds is not one of the queries it is for. *What is in
the log* answers those, and the difference is which contract the caller ends
up holding: a listing is a method, while `SELECT stream, next_seq - 1 FROM
stream_seq` is the column layout of a reserved table, which belongs to the
migration ladder and moves when it does.

    // Read anything, across the log and your own tables.
    let rows = log.query(
        "SELECT stream, json_extract(data, '$.n') AS n FROM events WHERE kind = ?1",
        vec![json!("scored")],
    ).await?;

    // The same query with named placeholders: a JSON object, one entry per
    // placeholder. The name is the placeholder as written, sigil included —
    // `:kind`, not `kind` — and `$` / `@` bind too, so the `'$.n'` in the SQL
    // is a literal and stays one.
    let rows = log.query(
        "SELECT stream, json_extract(data, '$.n') AS n FROM events WHERE kind = :kind",
        json!({ ":kind": "scored" }).as_object().cloned().unwrap(),
    ).await?;

    // Or take a real transaction on the log's own connection.
    log.with_transaction(|tx| {
        tx.execute_batch("CREATE TABLE IF NOT EXISTS my_view (k TEXT PRIMARY KEY)")?;
        tx.execute("INSERT INTO my_view (k) VALUES ('x')", [])?;
        Ok(())
    }).await?;

Every placeholder a named statement declares has to be supplied: one that is
not is refused, because rusqlite would otherwise leave it at `NULL` and the
statement would answer as though the caller had meant that. A `Vec` is the
positional set and an object is the named one, so `vec![]` still means what
it always did; a named set held in a `Vec` of pairs is written
`Params::Named(vec![..])`. `Params` in the rustdoc is the whole rule.

`query` answers in JSON, and a few SQLite cells have no JSON value at all. It
refuses those rather than handing over a stand-in: the error names the column
and the SQL that gets the value through — `hex(col)` for a blob, `CAST(col AS
TEXT)` for an infinite real. `query_with` can be told to take the substitute
instead, per statement, and the rustdoc on `QueryOptions::lossy` is the table
of what each cell becomes.

Your tables, your SQL, your schema, committed or rolled back with everything
else in that transaction. Inside a projection you already have this — `apply`
is handed the same kind of transaction.

What the hatch refuses, through SQLite's authorizer rather than by reading
your SQL: writing `events`, `stream_seq`, `checkpoints`, `retention`,
`exports` or `sqlite_sequence`; creating anything that *shares* one of those
names in any
schema, `TEMP` included, since a temp table shadows the real one for every
unqualified statement on the connection; attaching another database; setting a
pragma. Reading any of them is allowed and often the point — a pragma
included, and `READ_ONLY_PRAGMAS` lists the introspection pragmas that may
carry a table or index name, since SQLite's authorizer cannot tell that
argument from an assignment. So is adding your own index to `events` — that
changes no data, and it is the only way to make a read cheap when the shipped
indices do not cover what you filter on.

Each refusal is an invariant something else already promised — appends get
stamped and ordered by the store, removals leave a ledger, `stream_seq` keeps
`seq` from rewinding after a removal, `user_version` belongs to the migration
ladder, `journal_mode` to the concurrency story.

**How far that reaches.** The authorizer is a property of this API: it is
installed on the connection this store owns, for the length of a call, so it
covers everything coming through the crate and nothing else. A `sqlite3`
session on the same file never meets it.

One half of append-only is stronger than that. A trigger in the schema —
which every connection that opens the file gets — makes a stored event
impossible to *rewrite*, whoever opened it. Removal is deliberately not
covered: retention deletes as its whole purpose, so the same trigger would
have to be switched off inside the one transaction allowed to delete, which
is the code least worth leaving unguarded.

## Benchmarks

    cargo bench -p eventsdb-sqlite                        # everything
    cargo bench -p eventsdb-sqlite -- decide              # one group
    cargo bench -p eventsdb-sqlite -- --save-baseline before
    cargo bench -p eventsdb-sqlite -- --baseline before   # compare

Groups: `append`, `read`, `decide`, `project`, `transfer`, `retention`. All of
them run against a file-backed log in a temporary directory, because that is
what production uses — an in-memory log has no reader connections and no WAL on
disk, so its numbers would flatter every path that matters.

**What is not benchmarked here, deliberately.** Criterion measures steady-state
cost: how long an operation takes when nothing is fighting it. "Does a read
wait behind a write" is a question about contention — it has one answer rather
than a distribution, and a regression there is a defect, not a slower number.
Those live as assertions instead:

| question | where |
|---|---|
| how long does this operation take | `benches/eventsdb.rs` |
| does a read wait behind a write | `tests/readers.rs` |
| does a read see the write that just returned | `tests/readers.rs` |
| what does holding the write lock during a read cost | `tests/lock_hold.rs` |
| does a second log on one file still order correctly | `tests/two_logs.rs` |

Numbers that should get better go in benches; properties that must not regress
stay in tests, where a failure is a failure rather than a slower bar on a
chart.

## Schema evolution

Two axes, and they are not the same one:

| axis | subject | who owns the number | mechanism | marker |
|------|---------|---------------------|-----------|--------|
| event shape | `data`, `meta`, the meaning of a kind | **the author of the kind** | upcaster chain, applied on read | `_schema_version` |
| table shape | columns, indices, constraints | this crate | migration ladder, applied at open | `PRAGMA user_version` |

Stored bytes are never rewritten. An upcaster moves the reader forward
instead.

**The store carries `_schema_version`; it does not choose it.** Whoever owns a
`kind` owns what its `data` looks like, so they own the number that says which
shape it is in — a number defined by *this crate's* release history would mean
nothing to a consumer or to anyone reading an export. Set it on the event, or
leave it out and get `DEFAULT_SCHEMA_VERSION`. This is Axon's arrangement: the
revision is declared by the author, persisted in a column beside the type, and
absent is a legal value the first upcaster selects on.

**Select on `(kind, version)`, never on the version alone** — one shared number
would mean one author's bump silently bumped everyone else's.

The envelope has a shape too, and that one *is* the crate's: an upcaster
transforms JSON and cannot add a column, so an envelope change is a ladder
step. The ladder runs at open before any handle is issued, so a database is
homogeneous in envelope shape by the time anything reads it — which is why
there is no second per-event number.

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

### Removing only what is preserved elsewhere

Retention deletes; where an export goes is yours. So you hand the store a
sink and it runs the rest — the order Kafka's tiered storage and KurrentDB's
archiving keep, with the upload left to you:

    let mut archive = JsonLinesSink::new(File::create("2026-08.jsonl")?);
    log.archive_then_retain(Plan::Before(cutoff), &mut archive, 1000).await?;

    // Into another eventsdb file, which is the same call with another sink.
    let mut archive = LogSink::new(&cold);
    log.archive_then_retain(Plan::Before(cutoff), &mut archive, 1000).await?;
    assert!(archive.reproduced_coordinates());

What it does, spelled out:

    // `target` is the highest position the plan would remove.
    let mut cursor = log.exported_through().await?;   // resume, not restart
    while cursor < target {
        let (page, receipt) = log.export_recorded(cursor, &Filter::all(), 1000).await?;
        archive.write(&page).await?;              // yours: a file, another log
        log.confirm_export(receipt.id).await?;    // only if the write returned
        cursor = receipt.through;
    }
    log.retain(plan, Guard::Exported).await?;

The order is the point. `export_recorded` is `export` plus a receipt row —
taken, not yet landed — and `confirm_export` is the second half; a sink that
fails returns its error with that receipt unconfirmed and nothing removed, so
the next call resumes from `exported_through()` instead of exporting the
prefix twice. `Guard::Exported` chains the confirmed, unfiltered receipts from
the beginning of the log and refuses with `NotExported` any plan that would
remove past the chain's end; a page taken and never confirmed, a filtered one,
or a gap all leave the end where it was. It also refuses to overrun a
consumer, as the default does — and refusing after the export has landed
wastes nothing, because the chain stays where the confirmations put it.

`Plan::OlderThan` and `Plan::Streams` remove a scattered set while the chain
is a prefix, so the export runs through the **highest position the plan
touches** and the sink sees events that will stay. Over-exporting is safe; a
chain with holes is what this refuses. The rustdoc of
`eventsdb_sqlite::retention` has the two moments as a diagram.

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
- **Retention deletes; where the bytes go is yours.** The store runs the loop
  — `archive_then_retain` exports, confirms, then applies the plan under
  `Guard::Exported` — and writes nothing itself: each page goes to a `Sink`
  you supply, and the destination, the format, the durability and the retry
  are all on the far side of that one method. Two sinks ship, JSON Lines over
  any `Write` and another log; anything else is a trait with one method.
- **A long stream is designed away, not compacted.** `append_if` folds the
  stream under the write lock, and nothing in the store bounds how long a
  stream gets. The answer is a stream per period — a shift, a session, a
  month — closed by an event that carries its summary, so the next period
  starts from that rather than from the beginning. There is no snapshot in
  the log and no archive flag; a closed period's bytes are retention's to
  remove.
- **`reclaim` needs a database created by this version.** It relies on
  `auto_vacuum = INCREMENTAL`, which SQLite only accepts before the first
  table exists. On an older file it does nothing.

## Contributing

Three files in the repository, absolute-linked because this README ships inside
the crates and they do not:
[CONTRIBUTING.md](https://github.com/ynishi/eventsdb/blob/main/CONTRIBUTING.md)
has the issue, branch, verification and commit conventions,
[PUBLIC_DEVELOPMENT.md](https://github.com/ynishi/eventsdb/blob/main/PUBLIC_DEVELOPMENT.md)
the disclosure policy that outranks it, and
[AGENTS.md](https://github.com/ynishi/eventsdb/blob/main/AGENTS.md) the same
pointers arranged for a coding agent.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in this crate by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
