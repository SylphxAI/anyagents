//! The keeper with real processes: a kernel that crashes, detaches or is
//! replaced while its harness runs; a keeper or a harness that dies; a kernel
//! and a keeper that speak different protocol versions; a request cut off by
//! a dying kernel.
#![cfg(unix)]

use std::io::Write as _;
use std::os::fd::{AsFd as _, AsRawFd as _};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyagents_harness::{DeliverOutcome, Harness, OutboundMessage};
use anyagents_keeper::{Hosted as _, Program, RemoteHarness, Spec, attach, testing};
use serde_json::json;

/// The harness: logs every id it reads, waits while the hold file exists,
/// then says it took the id. When its input closes it goes on running, as a
/// vendor CLI finishing a long tool call does, so only a signal ends it.
const HARNESS: &str = r#"log="$1"; hold="$2"
while IFS= read -r id; do
  printf '%s\n' "$id" >> "$log"
  while [ -e "$hold" ]; do sleep 0.02; done
  printf 'took %s\n' "$id"
done
exec sleep 600"#;

struct Fixture {
    _tmp: tempfile::TempDir,
    root: PathBuf,
    log: PathBuf,
    hold: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        // A short base, so the socket path stays within the platform limit.
        let tmp = tempfile::Builder::new()
            .prefix("kp")
            .tempdir_in(short_base())
            .unwrap();
        let root = tmp.path().join("keepers");
        let log = tmp.path().join("read.log");
        let hold = tmp.path().join("hold");
        Self {
            _tmp: tmp,
            root,
            log,
            hold,
        }
    }

    fn spec(&self) -> Spec {
        Spec {
            agent: "agt_test".into(),
            harness: "line".into(),
            model: Some("m-1".into()),
            seat: None,
            labels: [("pointer".to_string(), "p".to_string())].into(),
            launch: json!({
                "script": HARNESS,
                "args": [self.log.display().to_string(), self.hold.display().to_string()],
            }),
        }
    }

    fn spawn(&self) -> RemoteHarness {
        anyagents_keeper::spawn(&program(&[]), &self.root, &self.spec(), None).unwrap()
    }

    fn hold(&self) {
        std::fs::write(&self.hold, b"").unwrap();
    }

    fn release(&self) {
        std::fs::remove_file(&self.hold).unwrap();
    }

    /// Every id the harness process read, in order.
    fn read(&self) -> Vec<String> {
        std::fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }
}

fn short_base() -> PathBuf {
    let tmp = std::env::temp_dir();
    if tmp.as_os_str().len() <= 40 {
        tmp
    } else {
        PathBuf::from("/tmp")
    }
}

fn program(extra: &[&str]) -> Program {
    Program {
        path: PathBuf::from(env!("CARGO_BIN_EXE_kept_line")),
        args: extra.iter().map(|arg| arg.to_string()).collect(),
    }
}

fn message(id: &str) -> OutboundMessage {
    OutboundMessage {
        id: id.into(),
        seq: 1,
        text: format!("body of {id}"),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Default::default(),
    }
}

#[track_caller]
fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !done() {
        assert!(Instant::now() < deadline, "never happened: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn alive(pid: u32) -> bool {
    // SAFETY: signal 0 only probes for the process.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

fn group_alive(pgid: u32) -> bool {
    // SAFETY: signal 0 to a group only probes for its members.
    let rc = unsafe { libc::kill(-(pgid as i32), 0) };
    rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn kill9(pid: u32) {
    // SAFETY: SIGKILL to a process this test started.
    unsafe { libc::kill(pid as i32, libc::SIGKILL) };
}

#[test]
fn a_kernel_crash_mid_turn_keeps_the_harness_and_its_pending_steers() {
    let fx = Fixture::new();
    fx.hold();
    let mut kernel = fx.spawn();
    let pid = kernel.process_id().unwrap();
    for id in ["m1", "m2", "m3"] {
        assert_eq!(
            kernel.deliver(&message(id)).unwrap(),
            DeliverOutcome::InFlight
        );
    }
    eventually("the harness reads the first steer", || fx.read() == ["m1"]);
    // The kernel dies with three lines in flight.
    kernel.crash();
    // The harness takes them while no kernel is connected.
    fx.release();
    eventually("the harness reads every steer", || {
        fx.read() == ["m1", "m2", "m3"]
    });
    let mut next = attach(&fx.root, "agt_test")
        .unwrap()
        .expect("a live keeper");
    assert_eq!(next.process_id(), Some(pid), "the harness was restarted");
    assert_eq!(next.session_id().as_deref(), Some("line-session"));
    for id in ["m1", "m2", "m3"] {
        eventually("a consumed steer is answered from the keeper", || {
            next.deliver(&message(id)).unwrap()
                == DeliverOutcome::Accepted {
                    turn_finished: true,
                }
        });
    }
    assert_eq!(
        next.deliver(&message("m4")).unwrap(),
        DeliverOutcome::InFlight
    );
    eventually("the next line is taken", || {
        next.deliver(&message("m4")).unwrap()
            == DeliverOutcome::Accepted {
                turn_finished: true,
            }
    });
    assert_eq!(
        fx.read(),
        ["m1", "m2", "m3", "m4"],
        "a line was written twice"
    );
    drop(next);
    assert!(!group_alive(pid), "stop left the harness running");
    assert!(attach(&fx.root, "agt_test").unwrap().is_none());
}

#[test]
fn a_detached_keeper_keeps_the_note_and_the_process_lock_for_the_next_kernel() {
    let fx = Fixture::new();
    let lock_path = fx.root.with_file_name("agent.lock");
    let lock = std::fs::File::create(&lock_path).unwrap();
    // SAFETY: flock on a file this test owns.
    assert_eq!(
        unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let kernel =
        anyagents_keeper::spawn(&program(&[]), &fx.root, &fx.spec(), Some(lock.as_fd())).unwrap();
    kernel.set_note("served=3").unwrap();
    let pid = kernel.process_id().unwrap();
    kernel.detach();
    drop(lock);
    let probe = || {
        let file = std::fs::File::open(&lock_path).unwrap();
        // SAFETY: flock on a file this test opened.
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) == 0 }
    };
    assert!(!probe(), "the keeper does not hold the agent's lock");
    let mut next = attach(&fx.root, "agt_test")
        .unwrap()
        .expect("a live keeper");
    assert_eq!(next.hello().note.as_deref(), Some("served=3"));
    assert_eq!(next.hello().labels["pointer"], "p");
    assert_eq!(next.hello().harness, "line");
    assert_eq!(next.hello().model.as_deref(), Some("m-1"));
    assert_eq!(next.process_id(), Some(pid));
    let adopted = next.take_lock().expect("the keeper hands its lock over");
    next.stop();
    assert!(!probe(), "the adopted copy no longer holds the lock");
    drop(adopted);
    eventually("the lock is free once every copy closed", probe);
}

#[test]
fn a_keeper_that_dies_takes_its_harness_with_it() {
    let fx = Fixture::new();
    let kernel = fx.spawn();
    let pid = kernel.process_id().unwrap();
    let dir = kernel.dir().to_path_buf();
    kill9(kernel.keeper_pid());
    eventually("the kernel sees the harness dead", || {
        !kernel.health().process_alive
    });
    assert!(!group_alive(pid), "the dead keeper's harness still runs");
    assert!(!dir.exists(), "the dead keeper's directory remains");
    drop(kernel);
    assert!(attach(&fx.root, "agt_test").unwrap().is_none());
}

#[test]
fn a_keeper_found_dead_by_the_next_kernel_has_its_harness_ended() {
    let fx = Fixture::new();
    let kernel = fx.spawn();
    let pid = kernel.process_id().unwrap();
    let keeper = kernel.keeper_pid();
    let mut process = kernel.crash().expect("this kernel started the keeper");
    kill9(keeper);
    process.wait().unwrap();
    assert!(group_alive(pid), "the orphaned harness should still run");
    assert!(attach(&fx.root, "agt_test").unwrap().is_none());
    eventually("the orphaned harness is ended", || !group_alive(pid));
}

#[test]
fn a_harness_that_dies_is_reported_and_its_keeper_stops_on_drop() {
    let fx = Fixture::new();
    let mut kernel = fx.spawn();
    let pid = kernel.process_id().unwrap();
    let keeper = kernel.keeper_pid();
    let dir = kernel.dir().to_path_buf();
    kill9(pid);
    eventually("the harness exit is reported", || {
        !kernel.health().process_alive
    });
    assert!(alive(keeper), "the keeper outlives its harness");
    assert_eq!(
        kernel.deliver(&message("late")),
        Err(anyagents_harness::HarnessError::Dead)
    );
    drop(kernel);
    assert!(!alive(keeper), "the keeper did not stop");
    assert!(!dir.exists());
}

#[test]
fn a_keeper_speaking_no_shared_protocol_is_refused_at_start_and_stopped_on_attach() {
    let fx = Fixture::new();
    let refused =
        anyagents_keeper::spawn(&program(&["--protocol", "2-3"]), &fx.root, &fx.spec(), None);
    let error = refused.err().expect("an unspoken protocol was accepted");
    assert!(error.contains("protocol 2..=3"), "{error}");
    assert!(anyagents_keeper::instances(&fx.root).is_empty());

    // One a previous kernel started, from a binary this kernel cannot speak to.
    let (dir, mut keeper) =
        testing::start_unattached(&program(&["--protocol", "2-3"]), &fx.root, &fx.spec());
    assert!(dir.exists());
    assert!(attach(&fx.root, "agt_test").unwrap().is_none());
    keeper.wait().unwrap();
    assert!(!dir.exists());

    // An overlap is spoken at the highest shared version.
    let kernel =
        anyagents_keeper::spawn(&program(&["--protocol", "1-9"]), &fx.root, &fx.spec(), None)
            .unwrap();
    assert_eq!(kernel.hello().protocol, anyagents_keeper::PROTOCOL);
    // An op the keeper does not know is answered as unknown, never as a fault:
    // the kernel falls back to the contract's default.
    assert_eq!(
        kernel.raw_call("an_op_from_the_future").unwrap_err(),
        "unknown_op"
    );
}

#[test]
fn a_request_cut_off_by_a_dying_kernel_is_never_acted_on() {
    let fx = Fixture::new();
    let kernel = fx.spawn();
    let dir = kernel.dir().to_path_buf();
    kernel.crash();
    let raw = testing::raw_owner(&dir);
    (&raw)
        .write_all(br#"{"id":7,"op":"deliver","args":{"message":{"id":"half","seq":1,"#)
        .unwrap();
    drop(raw);
    let mut next = attach(&fx.root, "agt_test").unwrap().unwrap();
    assert_eq!(
        next.deliver(&message("m1")).unwrap(),
        DeliverOutcome::InFlight
    );
    eventually("the line is taken", || {
        next.deliver(&message("m1")).unwrap()
            == DeliverOutcome::Accepted {
                turn_finished: true,
            }
    });
    assert_eq!(fx.read(), ["m1"]);
}

#[test]
fn a_request_whose_answer_was_lost_is_answered_from_the_keeper_memory() {
    let fx = Fixture::new();
    fx.hold();
    let kernel = fx.spawn();
    let dir = kernel.dir().to_path_buf();
    kernel.crash();
    // A kernel writes a whole steer and dies before reading the answer.
    let raw = testing::raw_owner(&dir);
    let request = json!({"id": 9, "op": "deliver", "args": {"message": message("m1")}});
    (&raw).write_all(format!("{request}\n").as_bytes()).unwrap();
    drop(raw);
    eventually("the keeper carried the request out", || fx.read() == ["m1"]);
    let mut next = attach(&fx.root, "agt_test").unwrap().unwrap();
    assert_eq!(
        next.deliver(&message("m1")).unwrap(),
        DeliverOutcome::InFlight
    );
    fx.release();
    eventually("the line is taken", || {
        next.deliver(&message("m1")).unwrap()
            == DeliverOutcome::Accepted {
                turn_finished: true,
            }
    });
    assert_eq!(fx.read(), ["m1"], "the repeat was written again");
}

#[test]
fn a_second_kernel_supersedes_the_first_without_ending_the_harness() {
    let fx = Fixture::new();
    let first = fx.spawn();
    let pid = first.process_id().unwrap();
    let mut second = attach(&fx.root, "agt_test").unwrap().unwrap();
    eventually("the first kernel is told", || first.keeper_gone());
    assert!(group_alive(pid), "a superseded kernel ended the harness");
    assert_eq!(second.process_id(), Some(pid));
    assert!(second.deliver(&message("m1")).is_ok());
    // The superseded handle lets go without stopping anything.
    drop(first);
    assert!(group_alive(pid));
    second.stop();
    assert!(!group_alive(pid));
}
