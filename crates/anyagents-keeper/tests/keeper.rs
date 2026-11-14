//! The keeper with real processes: a kernel that crashes, detaches or is
//! replaced while its harness runs; a keeper or a harness that dies; a kernel
//! and a keeper that speak different protocol versions; a request cut off by
//! a dying kernel.
#![cfg(unix)]

use std::io::Write as _;
use std::os::fd::{AsFd as _, AsRawFd as _};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessHealth, OutboundMessage, Proof,
    TurnState,
};
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
            lock_path: None,
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
    Program::new(
        PathBuf::from(env!("CARGO_BIN_EXE_kept_line")),
        extra.iter().map(|arg| arg.to_string()).collect(),
    )
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
    next.end().unwrap();
    assert!(!probe(), "the adopted copy no longer holds the lock");
    drop(adopted);
    eventually("the lock is free once every copy closed", probe);
}

// Ending a harness group needs a provable identity: Linux only.
#[cfg(target_os = "linux")]
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

// Ending a harness group needs a provable identity: Linux only.
#[cfg(target_os = "linux")]
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
        anyagents_keeper::spawn(&program(&["--protocol", "3-4"]), &fx.root, &fx.spec(), None);
    let error = refused.err().expect("an unspoken protocol was accepted");
    assert!(error.contains("protocol 3..=4"), "{error}");
    assert!(anyagents_keeper::instances(&fx.root).is_empty());

    // One a previous kernel started, from a binary this kernel cannot speak to.
    let (dir, mut keeper) =
        testing::start_unattached(&program(&["--protocol", "1-1"]), &fx.root, &fx.spec());
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
    second.end().unwrap();
    assert!(!group_alive(pid));
}

/// A kernel that stops its own wedged keeper takes it over on a fresh
/// connection; the keeper tells the old link it was superseded. That is the
/// kernel's own doing, not another server's: it must not stand down.
#[test]
fn a_kernel_that_stops_its_own_keeper_afresh_is_not_superseded() {
    let fx = Fixture::new();
    let mut kernel = fx.spawn();
    kernel.set_call_deadline(Duration::from_millis(300));
    assert!(kernel.deliver(&message("wedge")).is_err());
    let dir = kernel.dir().to_path_buf();
    Harness::stop(&mut kernel);
    assert!(!dir.exists(), "the wedged keeper was not stopped");
    assert!(
        !kernel.superseded(),
        "the kernel took its own fresh connection for another server"
    );
}

/// A keeper taken over and then gone still reports the usage it had: a
/// kernel that read nothing before it died would see none, as if its turns
/// had changed.
#[test]
fn a_keeper_taken_over_keeps_its_usage_after_it_dies() {
    let fx = Fixture::new();
    let mut kernel = fx.spawn();
    kernel.deliver(&message("m1")).unwrap();
    eventually("m1 is taken", || kernel.turns_served() == 1);
    let mut keeper = kernel.crash().expect("this kernel started the keeper");
    let next = attach(&fx.root, "agt_test").unwrap().unwrap();
    keeper.kill().unwrap();
    keeper.wait().unwrap();
    eventually("the keeper is gone", || next.keeper_gone());
    assert!(!next.health().process_alive);
    assert_eq!(next.turns_served(), 1, "the usage was lost with the keeper");
}

/// Why attach refused to take over (or clear) the keeper there.
#[cfg(target_os = "linux")]
fn refused_attach(root: &std::path::Path) -> String {
    match attach(root, "agt_test") {
        Err(error) => error,
        Ok(_) => panic!("attach went ahead where it had no proof"),
    }
}

/// A process outside any keeper, leading its own group, that a stale record
/// could name.
#[cfg(target_os = "linux")]
fn bystander() -> std::process::Child {
    use std::os::unix::process::CommandExt as _;
    std::process::Command::new("sleep")
        .arg("600")
        .process_group(0)
        .spawn()
        .unwrap()
}

/// A keeper directory a dead kernel's keeper left, with `record` as its
/// record and no socket.
#[cfg(target_os = "linux")]
fn left_behind(root: &std::path::Path, record: serde_json::Value) -> PathBuf {
    use std::os::unix::fs::DirBuilderExt as _;
    std::fs::DirBuilder::new().mode(0o700).create(root).ok();
    let dir = root.join("000000000001");
    std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
    std::fs::write(dir.join("agent"), "agt_test").unwrap();
    std::fs::write(dir.join("keeper.json"), record.to_string()).unwrap();
    dir
}

#[cfg(target_os = "linux")]
fn identity(pid: u32) -> serde_json::Value {
    serde_json::to_value(anyagents_keeper::ProcessIdentity::of(pid).unwrap()).unwrap()
}

/// The identity of a process that has ended and been reaped.
#[cfg(target_os = "linux")]
fn ended_identity() -> serde_json::Value {
    let mut gone = std::process::Command::new("true").spawn().unwrap();
    let recorded = identity(gone.id());
    gone.wait().unwrap();
    recorded
}

/// After a pod restart, a reboot or a restore, a recorded pid names another
/// process. A record whose harness identity does not match the process now
/// at that pid (another start time, another boot) signals nothing; the
/// directory is cleared.
#[cfg(target_os = "linux")]
#[test]
fn a_stale_record_never_signals_the_process_that_now_has_its_pid() {
    let fx = Fixture::new();
    let mut other = bystander();
    let mut harness = identity(other.id());
    harness["start_ticks"] = json!(harness["start_ticks"].as_u64().unwrap() + 1);
    let mut rebooted = identity(other.id());
    rebooted["boot_id"] = json!("a-boot-before-the-restore");
    for recorded in [harness, rebooted] {
        let dir = left_behind(
            &fx.root,
            json!({
                "protocol": [1, 1], "binary": "old", "agent": "agt_test", "harness": "line",
                "model": null, "seat": null, "started_at_ms": 1,
                "keeper": ended_identity(), "harness_process": recorded,
            }),
        );
        assert!(attach(&fx.root, "agt_test").unwrap().is_none());
        assert!(!dir.exists(), "the stale directory stays");
        assert!(
            alive(other.id()),
            "a stale record signalled another process"
        );
    }
    other.kill().unwrap();
    other.wait().unwrap();
}

/// A keeper is never taken for dead on anything short of proof. One whose
/// process still runs, one that holds the agent's lock, and a socket that
/// closes without answering all leave the harness and the directory alone,
/// and attach refuses rather than starting another.
#[cfg(target_os = "linux")]
#[test]
fn a_keeper_that_does_not_answer_is_never_treated_as_dead() {
    let fx = Fixture::new();
    let mut other = bystander();
    let harness = identity(other.id());

    // Its socket is gone, but its process runs.
    let keeper = fx.spawn();
    let pid = keeper.process_id().unwrap();
    let dir = keeper.dir().to_path_buf();
    let process = keeper.crash();
    std::fs::remove_file(dir.join("s")).unwrap();
    let refused = refused_attach(&fx.root);
    assert!(refused.contains("still runs"), "{refused}");
    assert!(group_alive(pid) && dir.exists());
    let mut process = process.unwrap();
    process.kill().unwrap();
    process.wait().unwrap();
    // SAFETY: ending the harness group this test's keeper started.
    unsafe { libc::kill(-(pid as i32), libc::SIGKILL) };
    // Nothing records which process its keeper was, and its lock is held: it
    // may be alive. (A keeper proven ended is gone whoever holds the lock.)
    std::fs::remove_dir_all(&dir).unwrap();
    let lock_file = fx.root.with_file_name("agent.lock");
    let held = std::fs::File::create(&lock_file).unwrap();
    // SAFETY: flock on a file this test owns.
    assert_eq!(
        unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    let dir = left_behind(
        &fx.root,
        json!({
            "protocol": [1, 1], "binary": "old", "agent": "agt_test", "harness": "line",
            "model": null, "seat": null, "started_at_ms": 1,
            "keeper": null, "harness_process": harness,
        }),
    );
    std::fs::write(dir.join("lock"), lock_file.display().to_string()).unwrap();
    let refused = refused_attach(&fx.root);
    assert!(refused.contains("still held"), "{refused}");
    assert!(alive(other.id()) && dir.exists());
    // A socket that closes before answering hello.
    drop(held);
    let listener = std::os::unix::net::UnixListener::bind(dir.join("s")).unwrap();
    let closer = std::thread::spawn(move || {
        use std::io::BufRead as _;
        let (stream, _) = listener.accept().unwrap();
        let mut hello = String::new();
        std::io::BufReader::new(&stream)
            .read_line(&mut hello)
            .unwrap();
        drop(stream);
    });
    let refused = refused_attach(&fx.root);
    assert!(refused.contains("without answering"), "{refused}");
    closer.join().unwrap();
    assert!(alive(other.id()) && dir.exists());
    other.kill().unwrap();
    other.wait().unwrap();
}

/// A call that never returns (a write into a stuck vendor) does not hang the
/// kernel: it fails at its deadline, the harness reads as dead, `hello`
/// still answers from the cached health, and stop ends the harness without
/// the adapter.
#[test]
fn a_wedged_adapter_is_bounded_and_still_stopped() {
    let fx = Fixture::new();
    let mut kernel = fx.spawn();
    let pid = kernel.process_id().unwrap();
    let dir = kernel.dir().to_path_buf();
    kernel.set_call_deadline(Duration::from_millis(500));
    let started = Instant::now();
    let refused = kernel.deliver(&message("wedge")).unwrap_err();
    assert!(
        matches!(&refused, anyagents_harness::HarnessError::Failed(why) if why.contains("did not answer")),
        "{refused:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(
        !kernel.health().process_alive,
        "a wedged keeper reads as alive"
    );
    let observed = anyagents_keeper::inspect(&dir).unwrap();
    assert_eq!(observed.harness_pid, Some(pid));
    drop(kernel);
    assert!(!dir.exists(), "the wedged keeper did not stop");
    // Ended from its recorded identity, which only Linux proves.
    #[cfg(target_os = "linux")]
    eventually("the wedged keeper's harness ends", || !group_alive(pid));
    #[cfg(not(target_os = "linux"))]
    let _ = pid;
}

/// A keeper whose adapter never finishes starting is ended with everything
/// it started, and its directory cleared.
#[test]
fn a_keeper_that_never_becomes_ready_is_ended() {
    let fx = Fixture::new();
    let mut spec = fx.spec();
    spec.launch["hang_start"] = json!(true);
    let mut slow = program(&[]);
    slow.ready = Duration::from_secs(2);
    let refused = anyagents_keeper::spawn(&slow, &fx.root, &spec, None)
        .err()
        .expect("a keeper that never got ready was accepted");
    assert!(refused.contains("not ready"), "{refused}");
    assert!(anyagents_keeper::instances(&fx.root).is_empty());
}

/// Only the keeper's current owner may act on it: an observer, or any other
/// connection, is refused, and says so.
#[test]
fn only_the_owner_may_act_on_a_keeper() {
    let fx = Fixture::new();
    let mut kernel = fx.spawn();
    let observer = testing::raw_connection(kernel.dir(), false);
    let request = json!({"id": 5, "op": "deliver", "args": {"message": message("sneaked")}});
    (&observer)
        .write_all(format!("{request}\n").as_bytes())
        .unwrap();
    let mut answer = String::new();
    use std::io::BufRead as _;
    std::io::BufReader::new(&observer)
        .read_line(&mut answer)
        .unwrap();
    assert!(answer.contains("forbidden"), "{answer}");
    assert_eq!(
        kernel.deliver(&message("m1")).unwrap(),
        DeliverOutcome::InFlight
    );
    eventually("the owner's line is read", || fx.read() == ["m1"]);
}

/// Nothing the kernel had open reaches the keeper (or its harness).
#[cfg(target_os = "linux")]
#[test]
fn a_keeper_closes_what_it_inherits() {
    let fx = Fixture::new();
    let leaked_path = fx.root.with_file_name("leaked");
    let leaked = std::fs::File::create(&leaked_path).unwrap();
    // SAFETY: clearing close-on-exec on a descriptor this test owns.
    unsafe {
        let flags = libc::fcntl(leaked.as_raw_fd(), libc::F_GETFD);
        libc::fcntl(leaked.as_raw_fd(), libc::F_SETFD, flags & !libc::FD_CLOEXEC);
    }
    let kernel = fx.spawn();
    let open: Vec<PathBuf> = std::fs::read_dir(format!("/proc/{}/fd", kernel.keeper_pid()))
        .unwrap()
        .flatten()
        .filter_map(|entry| std::fs::read_link(entry.path()).ok())
        .collect();
    assert!(
        !open.contains(&leaked_path),
        "the keeper holds a descriptor it inherited: {open:?}"
    );
    drop(leaked);
}

/// Keepers live only under this user's private directory.
#[test]
fn keepers_refuse_a_directory_that_is_not_private() {
    use std::os::unix::fs::PermissionsExt as _;
    let fx = Fixture::new();
    let real = fx.root.with_file_name("elsewhere");
    std::fs::create_dir(&real).unwrap();
    std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::os::unix::fs::symlink(&real, &fx.root).unwrap();
    let refused = anyagents_keeper::spawn(&program(&[]), &fx.root, &fx.spec(), None)
        .err()
        .expect("a symlinked keeper directory was used");
    assert!(refused.contains("not a directory"), "{refused}");
    std::fs::remove_file(&fx.root).unwrap();
    std::fs::create_dir(&fx.root).unwrap();
    std::fs::set_permissions(&fx.root, std::fs::Permissions::from_mode(0o755)).unwrap();
    let refused = anyagents_keeper::spawn(&program(&[]), &fx.root, &fx.spec(), None)
        .err()
        .expect("a keeper directory open to others was used");
    assert!(refused.contains("open to other users"), "{refused}");
}

/// A kernel that crashes while it replaces a harness can leave the new
/// keeper's directory behind with no socket and no record of its own, while
/// the old keeper still holds the agent lock. The identity the kernel wrote
/// when it started that keeper proves it gone: the directory is cleared, and
/// the process group the dead keeper recorded starting is ended, instead of
/// the agent waiting for a lock that will never be free.
#[cfg(target_os = "linux")]
#[test]
fn a_keeper_proven_ended_is_cleared_while_another_holds_the_lock() {
    use std::os::unix::fs::DirBuilderExt as _;
    use std::os::unix::process::CommandExt as _;
    let fx = Fixture::new();
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&fx.root)
        .unwrap();
    // The old keeper's lock, still held.
    let lock_path = fx.root.parent().unwrap().join("agent.lock");
    let held = std::fs::File::create(&lock_path).unwrap();
    // SAFETY: flock on a descriptor this test owns.
    assert_eq!(
        unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0
    );
    // A keeper that led its own session and died; the harness it started,
    // in a process group of its own, runs on.
    let mut keeper = std::process::Command::new("/bin/sh");
    keeper
        .args(["-c", "read never"])
        .stdin(std::process::Stdio::piped());
    // SAFETY: setsid between fork and exec, as a keeper starts.
    unsafe {
        keeper.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut keeper = keeper.spawn().unwrap();
    let mut harness = std::process::Command::new("sleep")
        .arg("600")
        .process_group(0)
        .spawn()
        .unwrap();
    let spawned = identity(keeper.id());
    kill9(keeper.id());
    keeper.wait().unwrap();

    let dir = fx.root.join("000000000002");
    std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
    std::fs::write(dir.join("agent"), "agt_test").unwrap();
    std::fs::write(dir.join("lock"), lock_path.as_os_str().as_encoded_bytes()).unwrap();
    std::fs::write(
        dir.join("spawned"),
        json!({"keeper": spawned, "seat": null}).to_string(),
    )
    .unwrap();
    std::fs::write(
        dir.join("groups"),
        json!([identity(harness.id())]).to_string(),
    )
    .unwrap();
    assert!(attach(&fx.root, "agt_test").unwrap().is_none());
    assert!(!dir.exists(), "the stale directory was left");
    eventually("the dead keeper's harness is ended", || {
        harness.try_wait().unwrap().is_some()
    });
    drop(held);
}

/// A keeper whose socket is gone while its process runs cannot be stopped,
/// nor proven gone; `keepers stop --force` ends it by signal once its
/// recorded identity proves the process is that keeper, with its harness.
#[cfg(target_os = "linux")]
#[test]
fn a_keeper_without_its_socket_is_ended_only_by_force() {
    let fx = Fixture::new();
    let kernel = fx.spawn();
    let dir = kernel.dir().to_path_buf();
    let harness = kernel.process_id().unwrap();
    let keeper_pid = kernel.keeper_pid();
    let mut keeper = kernel.crash().expect("this kernel started the keeper");
    std::fs::remove_file(dir.join("s")).unwrap();
    let refused = anyagents_keeper::stop_agent(&fx.root, "agt_test").unwrap_err();
    assert!(refused.contains("still runs"), "{refused}");
    assert!(alive(keeper_pid) && group_alive(harness));
    anyagents_keeper::stop_agent_with(&fx.root, "agt_test", true).unwrap();
    keeper.wait().unwrap();
    eventually("the harness is ended", || !group_alive(harness));
    assert!(!dir.exists());
}

/// A dead keeper's session is not swept: only the process groups it
/// recorded starting are ended. A process under its session id that it did
/// not record (whatever reused that number) is never signalled.
#[cfg(target_os = "linux")]
#[test]
fn a_process_the_keeper_did_not_record_is_never_signalled() {
    use std::io::BufRead as _;
    use std::os::unix::fs::DirBuilderExt as _;
    use std::os::unix::process::CommandExt as _;
    let fx = Fixture::new();
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&fx.root)
        .unwrap();
    let mut leader = std::process::Command::new("/bin/sh");
    leader
        .args(["-c", "sleep 600 & echo $!; read never"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped());
    // SAFETY: setsid between fork and exec.
    unsafe {
        leader.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut leader = leader.spawn().unwrap();
    let mut line = String::new();
    std::io::BufReader::new(leader.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let member: u32 = line.trim().parse().unwrap();
    let recorded = identity(leader.id());
    kill9(leader.id());
    leader.wait().unwrap();
    let dir = fx.root.join("000000000003");
    std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
    std::fs::write(dir.join("agent"), "agt_test").unwrap();
    std::fs::write(
        dir.join("spawned"),
        json!({"keeper": recorded, "seat": null}).to_string(),
    )
    .unwrap();
    assert!(attach(&fx.root, "agt_test").unwrap().is_none());
    assert!(!dir.exists());
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        alive(member),
        "a process the keeper never recorded was signalled"
    );
    kill9(member);
}

/// A keeper records each process group it starts, by its leader's identity,
/// before that process runs anything.
#[cfg(target_os = "linux")]
#[test]
fn a_keeper_records_the_process_groups_it_starts() {
    let fx = Fixture::new();
    let kernel = fx.spawn();
    let harness = kernel.process_id().unwrap();
    let dir = kernel.dir().to_path_buf();
    eventually("the harness group is recorded", || {
        std::fs::read(dir.join("groups"))
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Vec<serde_json::Value>>(&bytes).ok())
            .is_some_and(|groups| groups.iter().any(|group| group == &identity(harness)))
    });
    kernel.end().unwrap();
}

/// A v2 hello from a fake keeper whose record says it has `keeper` for its
/// process, with `health` as its health.
#[cfg(target_os = "linux")]
fn fake_hello(keeper: &serde_json::Value, health: serde_json::Value) -> serde_json::Value {
    json!({"id": 1, "ok": {
        "protocol": anyagents_keeper::PROTOCOL, "binary": "fake", "keeper_pid": 1,
        "keeper": keeper, "harness_pid": null, "agent": "agt_test", "harness": "line",
        "model": null, "seat": null, "started_at_ms": 1, "labels": {}, "note": null,
        "lock": false, "health": health,
    }})
}

#[cfg(target_os = "linux")]
fn idle_health() -> serde_json::Value {
    serde_json::to_value(HarnessHealth {
        process_alive: true,
        turn_state: TurnState::Idle,
        delivery: Delivery {
            level: DeliveryLevel::NextStep,
            idempotent: true,
            proof: "took <id>".into(),
            peer: None,
        },
        quota_reset_at_ms: None,
        sign_in_fault: None,
    })
    .unwrap()
}

/// Serve one owner connection of a fake keeper: answer its hello with
/// `hello`, then carry out its `stop` as a keeper does (the directory goes,
/// then the answer, then the connection). Every other request is ignored.
#[cfg(target_os = "linux")]
fn serve_stop(
    stream: std::os::unix::net::UnixStream,
    hello: &serde_json::Value,
    dir: &std::path::Path,
) {
    use std::io::BufRead as _;
    let mut lines = std::io::BufReader::new(stream.try_clone().unwrap()).lines();
    let _hello = lines.next();
    (&stream)
        .write_all(format!("{hello}\n").as_bytes())
        .unwrap();
    for line in lines {
        let request: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        if request["op"] == "stop" {
            std::fs::remove_dir_all(dir).unwrap();
            let answer = json!({"id": request["id"], "ok": {"stopped": true}});
            (&stream)
                .write_all(format!("{answer}\n").as_bytes())
                .unwrap();
            return;
        }
    }
}

/// An answer the kernel cannot read is an error to its caller, never a wait
/// without end. The keeper is driven no further: its harness reads as dead,
/// so the supervisor replaces it, and `stop` reaches it over a fresh
/// connection.
#[cfg(target_os = "linux")]
#[test]
fn an_unreadable_answer_fails_the_call_that_waits_for_it() {
    use std::io::BufRead as _;
    let fx = Fixture::new();
    let dir = left_behind(&fx.root, json!({}));
    let listener = std::os::unix::net::UnixListener::bind(dir.join("s")).unwrap();
    let hello = fake_hello(&ended_identity(), idle_health());
    let served = dir.clone();
    let fake = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut lines = std::io::BufReader::new(stream.try_clone().unwrap()).lines();
        let _hello = lines.next();
        (&stream)
            .write_all(format!("{hello}\n").as_bytes())
            .unwrap();
        // The usage a taken-over keeper is read for at once; then `deliver`,
        // answered in a shape the kernel cannot read.
        for line in lines.by_ref() {
            let request: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
            let ok = match request["op"].as_str() {
                Some("turns_served") => json!(0),
                Some("served_usage" | "last_usage") => {
                    serde_json::to_value(anyagents_harness::TurnUsage::default()).unwrap()
                }
                _ => {
                    let id = request["id"].as_u64().unwrap();
                    (&stream)
                        .write_all(format!("{{\"id\":{id},\"ok\":{{\"accepted\n").as_bytes())
                        .unwrap();
                    break;
                }
            };
            let answer = json!({"id": request["id"], "ok": ok});
            (&stream)
                .write_all(format!("{answer}\n").as_bytes())
                .unwrap();
        }
        let (fresh, _) = listener.accept().unwrap();
        serve_stop(fresh, &hello, &served);
        drop(lines);
    });
    let mut kernel = attach(&fx.root, "agt_test").unwrap().unwrap();
    let started = Instant::now();
    let refused = kernel.deliver(&message("m1")).unwrap_err();
    assert!(format!("{refused:?}").contains("unreadable"), "{refused:?}");
    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(!kernel.health().process_alive);
    drop(kernel);
    fake.join().unwrap();
    assert!(!dir.exists(), "the keeper was not stopped");
}

/// A keeper whose hello this kernel cannot read (a newer health shape under
/// a version both claim) is stopped through the hello that took it over and
/// replaced: never an error retried while it holds the agent lock.
#[cfg(target_os = "linux")]
#[test]
fn a_keeper_whose_hello_cannot_be_read_is_stopped_not_retried() {
    let fx = Fixture::new();
    let dir = left_behind(&fx.root, json!({}));
    let listener = std::os::unix::net::UnixListener::bind(dir.join("s")).unwrap();
    let mut newer = idle_health();
    newer["delivery"] = json!("a shape from a later release");
    let hello = fake_hello(&ended_identity(), newer);
    let served = dir.clone();
    let fake = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_stop(stream, &hello, &served);
    });
    let started = Instant::now();
    assert!(attach(&fx.root, "agt_test").unwrap().is_none());
    assert!(started.elapsed() < Duration::from_secs(10));
    fake.join().unwrap();
    assert!(!dir.exists(), "the keeper was not stopped");
}

/// What the conformance contract adds crosses the keeper: a rejection for
/// good, proofs, the delivery a harness reports, and `stop` of the vendor
/// process with its proofs still answered after. `proven` only reads: a
/// kernel that dies before reading its answer loses nothing, and the next
/// kernel is answered the same.
#[test]
fn a_kept_adapter_forwards_rejections_proofs_and_stop() {
    let fx = Fixture::new();
    let mut kernel = fx.spawn();
    let health = kernel.health();
    assert_eq!(health.delivery.level, DeliveryLevel::NextStep);
    assert_eq!(health.delivery.proof, "took <id>");
    assert_eq!(
        kernel.deliver(&message("refuse")).unwrap(),
        DeliverOutcome::rejected("refused for good", true)
    );
    assert_eq!(
        kernel.deliver(&message("m1")).unwrap(),
        DeliverOutcome::InFlight
    );
    eventually("m1 is proven", || {
        kernel.proven("m1")
            == Some(Proof::Taken {
                turn_finished: true,
            })
    });
    assert_eq!(kernel.proven("m2"), None);

    let dir = kernel.dir().to_path_buf();
    kernel.crash();
    let raw = testing::raw_owner(&dir);
    let request = json!({"id": 9, "op": "proven", "args": {"id": "m1"}});
    (&raw).write_all(format!("{request}\n").as_bytes()).unwrap();
    drop(raw);
    let mut next = attach(&fx.root, "agt_test").unwrap().unwrap();
    assert_eq!(
        next.proven("m1"),
        Some(Proof::Taken {
            turn_finished: true,
        })
    );

    let pid = next.process_id().unwrap();
    Harness::stop(&mut next);
    assert!(!group_alive(pid), "stop left the vendor process");
    eventually("the harness reads as ended", || {
        !next.health().process_alive
    });
    assert!(dir.exists(), "stop ended the keeper, not only its harness");
    assert_eq!(
        next.proven("m1"),
        Some(Proof::Taken {
            turn_finished: true,
        }),
        "a proof was lost with the process that gave it"
    );
    next.end().unwrap();
    assert!(!dir.exists());
}
