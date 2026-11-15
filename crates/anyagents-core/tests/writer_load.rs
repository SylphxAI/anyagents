//! ADR 0006 invariant I2: no lock error surfaces under load, and a process
//! keeps exactly one read-write connection per database.
//!
//! One hundred actors, each with its own `Store::open` handle, run mailbox,
//! delivery, turn and usage writes while a second OS process writes to the same
//! file. Every operation must succeed, and this process must hold exactly one
//! descriptor opened for writing on the database file throughout.

use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyagents_core::{BillingMode, Incoming, NewAgent, NewUsage, Store};

const ACTORS: usize = 100;
/// Operations per actor. ADR 0006 states 1,000; every operation is a durable
/// commit, so the default keeps the suite fast and
/// `ANYAGENTS_WRITER_LOAD_OPS=1000` runs the full figure.
const DEFAULT_OPS: usize = 20;
const CHILD_DB: &str = "ANYAGENTS_WRITER_LOAD_CHILD_DB";
const CHILD_STOP: &str = "ANYAGENTS_WRITER_LOAD_CHILD_STOP";

fn ops_per_actor() -> usize {
    std::env::var("ANYAGENTS_WRITER_LOAD_OPS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(DEFAULT_OPS)
}

fn usage(agent_id: &anyagents_core::AgentId) -> NewUsage {
    NewUsage {
        agent_id: agent_id.clone(),
        harness: "codex".into(),
        model: Some("m".into()),
        input_tokens: 1,
        output_tokens: 1,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
        reported_cost_micros: None,
        estimated_cost_micros: None,
        billing_mode: BillingMode::Subscription,
        quota_state: None,
        day: "2026-11-13".into(),
    }
}

/// Descriptors this process holds on `file` opened for writing. SQLite opens
/// a read-only connection's file `O_RDONLY`, so this counts read-write
/// connections. `None` where the platform has no `/proc/self/fd`.
fn read_write_descriptors(file: &Path) -> Option<usize> {
    // Compare inodes, not link text: /proc may render a live file's link
    // differently, and a replaced file must not count.
    let file = std::fs::metadata(file).unwrap();
    let fds = std::fs::read_dir("/proc/self/fd").ok()?;
    let count = fds
        .filter_map(Result::ok)
        .filter(|entry| {
            std::fs::metadata(entry.path())
                .is_ok_and(|open| open.dev() == file.dev() && open.ino() == file.ino())
        })
        .filter(|entry| {
            let info =
                std::fs::read_to_string(Path::new("/proc/self/fdinfo").join(entry.file_name()))
                    .unwrap_or_default();
            info.lines()
                .filter_map(|line| line.strip_prefix("flags:"))
                .filter_map(|flags| i64::from_str_radix(flags.trim(), 8).ok())
                .any(|flags| flags & i64::from(libc::O_ACCMODE) == i64::from(libc::O_RDWR))
        })
        .count();
    Some(count)
}

/// The external writer. Run as a child of the test below; a no-op otherwise.
#[test]
fn external_writer_child() {
    let (Some(db), Some(stop)) = (std::env::var_os(CHILD_DB), std::env::var_os(CHILD_STOP)) else {
        return;
    };
    let store = Store::open(PathBuf::from(db)).unwrap();
    let agent = store
        .create_agent(NewAgent::new("external-writer", "codex"))
        .unwrap();
    let stop = PathBuf::from(stop);
    let mut writes = 0usize;
    // At least a few writes even if the parent finishes first.
    while writes < 20 || !stop.exists() {
        store
            .receive(&agent.id, Incoming::new(format!("external {writes}")))
            .unwrap();
        store.record_usage(usage(&agent.id)).unwrap();
        writes += 1;
    }
    println!("external writes: {writes}");
}

#[test]
fn hundred_actors_and_an_external_writer_never_see_a_lock_error() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.db");
    let stop = dir.path().join("stop");
    let ops = ops_per_actor();
    let setup = Store::open(&db).unwrap();
    let agents: Vec<_> = (0..ACTORS)
        .map(|n| {
            setup
                .create_agent(NewAgent::new(format!("actor-{n}"), "codex"))
                .unwrap()
                .id
        })
        .collect();

    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "external_writer_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD_DB, &db)
        .env(CHILD_STOP, &stop)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Start the load only once the other process is writing.
    let deadline = Instant::now() + Duration::from_secs(60);
    while setup.agent_by_name("external-writer").is_err() {
        assert!(
            Instant::now() < deadline,
            "the external writer never started"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let failures = AtomicUsize::new(0);
    let done = AtomicBool::new(false);
    let start = Barrier::new(ACTORS + 1);
    let most_writers = std::thread::scope(|scope| {
        let actors: Vec<_> = agents
            .iter()
            .map(|id| {
                let (db, start, failures) = (&db, &start, &failures);
                scope.spawn(move || {
                    let store = Store::open(db).unwrap();
                    start.wait();
                    for n in 1..=ops as i64 {
                        let result = (|| -> Result<(), anyagents_core::Error> {
                            store.receive(id, Incoming::new(format!("line {n}")))?;
                            let next = store.next_undelivered(id)?.expect("the line just received");
                            assert_eq!(next.seq, n);
                            store.note_delivered(id, n)?;
                            store.note_turn_completed(id)?;
                            store.record_usage(usage(id))?;
                            Ok(())
                        })();
                        if let Err(error) = result {
                            eprintln!("actor operation failed: {error}");
                            failures.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        start.wait();
        // Sample this process's read-write descriptors while the actors run.
        let sampler = scope.spawn(|| {
            let mut most = None;
            while !done.load(Ordering::Relaxed) {
                most = most.max(read_write_descriptors(&db));
                std::thread::sleep(Duration::from_millis(5));
            }
            most
        });
        for actor in actors {
            actor.join().unwrap();
        }
        done.store(true, Ordering::Relaxed);
        sampler.join().unwrap()
    });

    std::fs::write(&stop, b"").unwrap();
    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "external writer failed: {stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("external writes:"), "{stdout}");

    assert_eq!(failures.load(Ordering::Relaxed), 0);
    // Descriptor counts need /proc; elsewhere the lock-error checks stand alone.
    if let Some(most) = most_writers {
        assert_eq!(most, 1, "one read-write connection in this process");
    }
    if let Some(count) = read_write_descriptors(&db) {
        assert_eq!(count, 1);
    }
    for id in &agents {
        assert_eq!(setup.delivered_cursor(id).unwrap(), ops as i64);
        assert_eq!(setup.list_usage(Some(id)).unwrap().len(), ops);
    }
}
