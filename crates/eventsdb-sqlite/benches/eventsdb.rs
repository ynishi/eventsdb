//! What each operation costs, on a real file.
//!
//! # What belongs here, and what does not
//!
//! Criterion measures **steady-state cost**: how long one operation takes when
//! nothing is fighting it. That is the right tool for "is a decision that names
//! its kinds cheaper than one that does not", and the wrong tool for "does a
//! read wait behind a write" — the second is a question about contention, it
//! has one answer rather than a distribution, and it lives in
//! `tests/readers.rs` and `tests/lock_hold.rs` as assertions instead.
//!
//! So: numbers that should get better go here. Properties that must not
//! regress stay as tests, where a failure is a failure rather than a slower
//! bar on a chart.
//!
//! # Running
//!
//! ```text
//! cargo bench -p eventsdb-sqlite                 # everything
//! cargo bench -p eventsdb-sqlite -- decide       # one group
//! cargo bench -p eventsdb-sqlite -- --save-baseline before
//! cargo bench -p eventsdb-sqlite -- --baseline before
//! ```
//!
//! Every benchmark runs against a **file-backed** log in a temporary
//! directory, because that is what production uses: an in-memory log has no
//! reader connections and no WAL on disk, so its numbers would flatter every
//! path that matters.
//!
//! # Why `iter_custom` and not `iter_batched`
//!
//! Several of these need a fresh log per iteration — a catch-up applies
//! nothing the second time, an import into a populated store measures
//! something else. Criterion's `iter_batched` runs its setup closure on the
//! runtime's own thread, so an async setup there has to `block_on` inside a
//! runtime, which panics. `iter_custom` lets the setup be part of the same
//! async block and the clock cover only the call under test.

use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use eventsdb_core::error::{Error, Result};
use eventsdb_core::position::Recorded;
use eventsdb_core::{EventLog, EventStore, ExportedEvent, Filter, Position};
use eventsdb_sqlite::{Guard, Plan, Projection, SqliteEventLog, Transaction};
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("a runtime")
}

fn event(kind: &str, n: i64) -> Map<String, Value> {
    json!({ "kind": kind, "meta": { "n": n }, "data": { "payload": "some content" } })
        .as_object()
        .expect("literal is an object")
        .clone()
}

/// A fresh log on disk, plus the directory keeping it alive.
async fn fresh() -> (tempfile::TempDir, SqliteEventLog) {
    let dir = tempfile::tempdir().expect("a temp dir");
    let log = SqliteEventLog::open(dir.path().join("bench.db"))
        .await
        .expect("an open log");
    (dir, log)
}

async fn seeded(events: usize, kind: &str) -> (tempfile::TempDir, SqliteEventLog) {
    let (dir, log) = fresh().await;
    let mut stream = log.stream_handle("bench");
    let batch: Vec<Map<String, Value>> = (0..events).map(|i| event(kind, i as i64)).collect();
    for chunk in batch.chunks(500) {
        stream.append_many(chunk.to_vec()).await.expect("a seed");
    }
    (dir, log)
}

fn append(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("append");

    // One log, appended to repeatedly — which is the steady state, and what
    // `next_seq` reading a counter rather than scanning is meant to keep flat.
    let held = rt.block_on(fresh());
    group.bench_function("one", |b| {
        b.to_async(&rt).iter(|| async {
            held.1
                .stream_handle("bench")
                .append(event("noted", 1))
                .await
                .expect("an append")
        });
    });

    for size in [10usize, 100] {
        let held = rt.block_on(fresh());
        group.throughput(Throughput::Elements(size as u64));
        group.bench_function(format!("many_{size}"), |b| {
            b.to_async(&rt).iter(|| async {
                let batch: Vec<Map<String, Value>> =
                    (0..size).map(|i| event("noted", i as i64)).collect();
                held.1
                    .stream_handle("bench")
                    .append_many(batch)
                    .await
                    .expect("a batch")
            });
        });
    }

    // One event and one row of the caller's own, in one transaction — the
    // reason `TxnContext` exists, and worth watching against a bare append.
    let held = rt.block_on(async {
        let (dir, log) = fresh().await;
        log.with_transaction(|tx| {
            tx.execute_batch("CREATE TABLE side (position INTEGER PRIMARY KEY)")
                .map_err(|e| Error::storage(e.to_string()))
        })
        .await
        .expect("the caller's table");
        (dir, log)
    });
    group.throughput(Throughput::Elements(1));
    group.bench_function("with_a_caller_row", |b| {
        b.to_async(&rt).iter(|| async {
            held.1
                .with_transaction(|tx| {
                    let committed = tx.append("bench", event("noted", 1))?;
                    tx.execute(
                        "INSERT INTO side (position) VALUES (?1)",
                        [committed.position.expect("a position").get() as i64],
                    )
                    .map(|_| ())
                    .map_err(|e| Error::storage(e.to_string()))
                })
                .await
                .expect("the pair")
        });
    });

    group.finish();
}

fn read(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("read");

    for size in [100usize, 10_000] {
        let held = rt.block_on(seeded(size, "noted"));

        group.throughput(Throughput::Elements(size as u64));
        group.bench_function(format!("read_all_{size}"), |b| {
            b.to_async(&rt).iter(|| async {
                held.1
                    .read_all(Position::BEGINNING, &Filter::all(), usize::MAX)
                    .await
                    .expect("a read")
            });
        });

        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("head_of_{size}"), |b| {
            b.to_async(&rt)
                .iter(|| async { held.1.head_position().await.expect("a head") });
        });

        // A read from the end, which is the one the range reads cannot express
        // and the backend answers with `ORDER BY seq DESC LIMIT n`.
        group.bench_function(format!("tail_10_of_{size}"), |b| {
            b.to_async(&rt).iter(|| async {
                held.1
                    .stream_handle("bench")
                    .read_last(10)
                    .await
                    .expect("a tail")
            });
        });
    }

    // SQL over the log, which is the escape hatch's read half.
    let held = rt.block_on(seeded(10_000, "noted"));
    group.throughput(Throughput::Elements(1));
    group.bench_function("query_count_of_10000", |b| {
        b.to_async(&rt).iter(|| async {
            held.1
                .query("SELECT count(*) AS n FROM events", Vec::new())
                .await
                .expect("a query")
        });
    });

    group.finish();
}

/// Naming the kinds a decision folds is the control on how long it holds the
/// write lock. This is the size of that control.
fn decide(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("decide");
    group.sample_size(20);

    for size in [1_000usize, 20_000] {
        // A long stream of one kind, plus a single event of the kind a narrow
        // decision actually folds.
        let held = rt.block_on(async {
            let (dir, log) = seeded(size, "noise").await;
            log.stream_handle("bench")
                .append(event("interesting", 0))
                .await
                .expect("the one that matters");
            (dir, log)
        });

        group.bench_function(format!("every_kind_{size}"), |b| {
            b.to_async(&rt).iter(|| async {
                held.1
                    .stream_handle("bench")
                    .append_if(
                        None,
                        Box::new(|seen| Some(event("decided", seen.len() as i64))),
                    )
                    .await
                    .expect("a decision")
            });
        });

        group.bench_function(format!("named_kind_{size}"), |b| {
            b.to_async(&rt).iter(|| async {
                held.1
                    .stream_handle("bench")
                    .append_if(
                        Some(&["interesting"]),
                        Box::new(|seen| Some(event("decided", seen.len() as i64))),
                    )
                    .await
                    .expect("a decision")
            });
        });
    }

    group.finish();
}

struct Counter;

impl Projection for Counter {
    fn name(&self) -> &str {
        "bench"
    }
    fn init(&mut self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute_batch("CREATE TABLE IF NOT EXISTS seen (n INTEGER)")
            .map_err(|e| Error::storage(e.to_string()))
    }
    fn reset(&mut self, tx: &Transaction<'_>) -> Result<()> {
        tx.execute_batch("DELETE FROM seen")
            .map_err(|e| Error::storage(e.to_string()))
    }
    fn apply(&mut self, tx: &Transaction<'_>, _event: &Recorded) -> Result<()> {
        tx.execute("INSERT INTO seen (n) VALUES (1)", [])
            .map(|_| ())
            .map_err(|e| Error::storage(e.to_string()))
    }
}

/// `with_batch` bounds how long a fold holds the write lock. This is what it
/// costs at either end of that trade.
fn project(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("project");
    group.sample_size(10);

    const EVENTS: usize = 5_000;
    group.throughput(Throughput::Elements(EVENTS as u64));

    for batch in [64usize, 1_024] {
        group.bench_function(format!("catch_up_batch_{batch}"), |b| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    // A catch-up applies nothing the second time, so the log
                    // has to be new — and the seeding must not be timed.
                    let (dir, log) = seeded(EVENTS, "noted").await;
                    let mut runner = log.runner(Counter).with_batch(batch);
                    runner.init().await.expect("init");

                    let started = Instant::now();
                    let applied = runner.catch_up().await.expect("a catch-up");
                    total += started.elapsed();

                    assert_eq!(applied, EVENTS);
                    log.shutdown().await.expect("a clean shutdown");
                    drop(dir);
                }
                total
            });
        });
    }

    group.finish();
}

fn transfer(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("transfer");
    group.sample_size(10);

    const EVENTS: usize = 5_000;
    group.throughput(Throughput::Elements(EVENTS as u64));

    let source = rt.block_on(seeded(EVENTS, "noted"));

    group.bench_function("export", |b| {
        b.to_async(&rt).iter(|| async {
            source
                .1
                .export(Position::BEGINNING, &Filter::all(), usize::MAX)
                .await
                .expect("an export")
        });
    });

    let exported: Vec<ExportedEvent> = rt.block_on(async {
        source
            .1
            .export(Position::BEGINNING, &Filter::all(), usize::MAX)
            .await
            .expect("an export")
    });

    group.bench_function("import", |b| {
        let exported = exported.clone();
        b.to_async(&rt).iter_custom(move |iters| {
            let exported = exported.clone();
            async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    // Into an empty store every time, or it is measuring a
                    // merge rather than a restore.
                    let (dir, log) = fresh().await;
                    let events = exported.clone();

                    let started = Instant::now();
                    let report = log.import(events).await.expect("an import");
                    total += started.elapsed();

                    assert!(report.reproduced_coordinates);
                    log.shutdown().await.expect("a clean shutdown");
                    drop(dir);
                }
                total
            }
        });
    });

    group.finish();
}

fn retention(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("retention");
    group.sample_size(10);

    const EVENTS: usize = 5_000;
    group.throughput(Throughput::Elements(EVENTS as u64 / 2));

    group.bench_function("before_half_of_5000", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let (dir, log) = seeded(EVENTS, "noted").await;

                let started = Instant::now();
                let report = log
                    .retain(Plan::Before(Position::new(EVENTS as u64 / 2)), Guard::Force)
                    .await
                    .expect("a retain");
                total += started.elapsed();

                assert_eq!(report.removed, EVENTS / 2);
                log.shutdown().await.expect("a clean shutdown");
                drop(dir);
            }
            total
        });
    });

    group.finish();
}

criterion_group! {
    name = benches;
    // Opening a file-backed log costs a few milliseconds, so the default
    // three-second warm-up would spend most of itself on setup rather than on
    // the thing being measured.
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(5));
    targets = append, read, decide, project, transfer, retention
}
criterion_main!(benches);
