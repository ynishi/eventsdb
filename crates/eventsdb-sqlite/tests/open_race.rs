//! Opening a file another connection is holding.
//!
//! `concurrent_first_opens_of_one_file_do_not_race_the_ladder` in
//! `regressions.rs` is the probabilistic net for this: it opens one fresh file
//! twice and hopes the scheduler lands the two pragma batches on top of each
//! other, which a CI runner manages about once in thirteen runs and a laptop
//! does not manage at all. These two hold the lock from a raw connection
//! instead, for a length of time the test chooses, so the wait is measured
//! rather than raced for.

use std::path::Path;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use eventsdb_core::error::Error;
use eventsdb_core::{EventLog, EventStore, Filter, Position};
use eventsdb_sqlite::{OpenOptions, SqliteEventLog};
use serde_json::json;

/// A raw connection holding the reserved lock on `path`, in rollback-journal
/// mode, with the database header already written.
///
/// Both halves matter. The header has to exist, because the statement that
/// loses is `PRAGMA journal_mode = WAL` reading the header's version bytes
/// and then writing them; with no page 1 to read there is no read to promote,
/// and the opening connection waits on the busy handler like anything else.
/// `application_id` is a header field the ladder does not look at, so writing
/// it leaves a file that is fresh in every sense this crate cares about —
/// `user_version` still 0, no tables.
struct Holder {
    release: Option<mpsc::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Holder {
    /// Returns once the lock is held, so a caller can start an open knowing
    /// it will lose.
    fn take(path: &Path) -> Holder {
        let path = path.to_path_buf();
        let (took, taken) = mpsc::channel();
        let (release, released) = mpsc::channel::<()>();

        let thread = thread::spawn(move || {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch("PRAGMA application_id = 1; BEGIN IMMEDIATE;")
                .unwrap();
            took.send(()).unwrap();
            // `Err` once the sender is dropped, which is the release.
            let _ = released.recv();
            conn.execute_batch("COMMIT;").unwrap();
        });
        taken.recv().unwrap();

        Holder {
            release: Some(release),
            thread: Some(thread),
        }
    }

    /// Commit and let go, and do not return until the lock is gone.
    fn release(&mut self) {
        drop(self.release.take());
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

impl Drop for Holder {
    fn drop(&mut self) {
        self.release();
    }
}

fn event(kind: &str) -> serde_json::Map<String, serde_json::Value> {
    json!({ "kind": kind }).as_object().unwrap().clone()
}

/// The open waits for the lock rather than failing on it.
///
/// Without the retry in `apply_pragmas` this fails after a few hundred
/// microseconds with `Error::Busy`, whatever `busy_timeout` says: SQLite hands
/// the header write `SQLITE_BUSY` without consulting the busy handler at all.
/// The elapsed time is the other half of the assertion — an open that returned
/// before the holder let go was never contended, and would prove nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_against_a_held_file_waits_for_the_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("held.db");

    let mut holder = Holder::take(&path);
    let start = Instant::now();
    let releasing = tokio::task::spawn_blocking(move || {
        thread::sleep(Duration::from_millis(200));
        holder.release();
    });

    let opened = SqliteEventLog::open_with(
        &path,
        OpenOptions::default().busy_timeout(Duration::from_secs(2)),
    )
    .await;
    let elapsed = start.elapsed();
    releasing.await.unwrap();

    let log = opened.unwrap_or_else(|e| panic!("open failed after {elapsed:?}: {e}"));
    assert!(
        elapsed >= Duration::from_millis(200),
        "the open returned after {elapsed:?}, before the holder let go"
    );

    // And the log it waited for is a working one: the ladder ran on the file
    // the holder had already written a header into.
    let mut s = log.stream_handle("s");
    s.append(event("a")).await.unwrap();
    assert_eq!(
        log.read_all(Position::BEGINNING, &Filter::all(), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    log.close().await.unwrap();
}

/// `busy_timeout` stays the bound. A holder that never lets go gets the caller
/// the `Busy` it asked to wait that long for, and not before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_against_a_file_held_for_good_fails_busy_at_the_timeout() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("held-for-good.db");

    let holder = Holder::take(&path);
    let start = Instant::now();
    let opened = SqliteEventLog::open_with(
        &path,
        OpenOptions::default().busy_timeout(Duration::from_millis(300)),
    )
    .await;
    let elapsed = start.elapsed();

    let error = match opened {
        Ok(_) => panic!("the open succeeded against a lock nobody released"),
        Err(error) => error,
    };
    assert!(matches!(error, Error::Busy(_)), "got {error}");
    assert!(
        elapsed >= Duration::from_millis(300),
        "gave up after {elapsed:?}, short of the timeout the caller set"
    );

    // Before the tempdir goes, so the holder is not still on the file.
    drop(holder);
}
