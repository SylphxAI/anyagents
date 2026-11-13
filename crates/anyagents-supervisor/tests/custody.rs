//! Harness processes that outlive the kernel: a running agent's worker takes
//! over the process its last kernel left, without a restart or a recovery
//! note, and resumes delivery from the mailbox cursor; a stopping kernel can
//! leave every process to the next one; a process left for an agent that no
//! longer runs is ended.

use std::collections::HashMap;
use std::os::fd::AsFd as _;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyagents_core::{AgentId, AgentLock, Incoming, NewAgent, Store};
use anyagents_harness::{
    DeliverOutcome, DeliveryMode, FakeHarness, Harness, HarnessError, HarnessHealth,
    OutboundMessage,
};
use anyagents_supervisor::{Attached, Clock, Custody, LaunchRequest, ManualClock, Supervisor};

/// A fake vendor process the test can still look at after handing it over.
#[derive(Clone)]
struct Kept {
    fake: Arc<Mutex<FakeHarness>>,
    ended: Arc<AtomicBool>,
}

impl Kept {
    fn new() -> Self {
        Self {
            fake: Arc::new(Mutex::new(FakeHarness::new(DeliveryMode::Steer))),
            ended: Arc::new(AtomicBool::new(false)),
        }
    }

    fn written(&self) -> Vec<String> {
        self.fake.lock().unwrap().written_ids().to_vec()
    }

    fn given(&self) -> Vec<String> {
        self.fake.lock().unwrap().given_ids().to_vec()
    }
}

/// The handle a kernel holds, with the copy of the agent's lock the
/// process's keeper holds. Dropping it ends the process and closes that copy,
/// as a keeper's stop does; the custody's `retain` keeps both instead.
struct Handle(Option<Kept>, Option<AgentLock>);

impl Drop for Handle {
    fn drop(&mut self) {
        if let Some(kept) = self.0.take() {
            kept.ended.store(true, Ordering::SeqCst);
        }
    }
}

impl Harness for Handle {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        self.kept().fake.lock().unwrap().deliver(message)
    }

    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn resume(&mut self, _session: &str) -> Result<(), HarnessError> {
        Ok(())
    }

    fn cancel(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }

    fn health(&self) -> HarnessHealth {
        self.kept().fake.lock().unwrap().health()
    }

    fn process_id(&self) -> Option<u32> {
        Some(4242)
    }
}

impl Handle {
    fn kept(&self) -> &Kept {
        self.0.as_ref().expect("live handle")
    }
}

/// One process a previous kernel left: the process, the copy of its agent's
/// lock its host holds, the harness it was launched for and the kept note.
type LeftProcess = (Kept, AgentLock, String, Option<String>);

/// Processes a previous kernel left.
#[derive(Default)]
struct Left {
    processes: Mutex<HashMap<AgentId, LeftProcess>>,
    released: Mutex<Vec<AgentId>>,
    retained: Mutex<Vec<(Kept, Option<AgentLock>)>>,
    notes: Mutex<Vec<String>>,
}

struct FakeCustody(Arc<Left>);

impl Custody<Handle> for FakeCustody {
    fn outlives_kernel(&self) -> bool {
        true
    }

    fn retained(&self) -> Vec<AgentId> {
        self.0.processes.lock().unwrap().keys().cloned().collect()
    }

    fn attach(&self, agent: &AgentId) -> Result<Option<Attached<Handle>>, String> {
        let Some((kept, host_lock, harness, note)) = self.0.processes.lock().unwrap().remove(agent)
        else {
            return Ok(None);
        };
        let lock = AgentLock::adopt(host_lock.as_fd().try_clone_to_owned().unwrap());
        Ok(Some(Attached {
            harness: Handle(Some(kept), Some(host_lock)),
            lock,
            harness_name: harness,
            model: None,
            started_at_ms: Some(7),
            note,
        }))
    }

    fn release(&self, agent: &AgentId) {
        if let Some((kept, _, _, _)) = self.0.processes.lock().unwrap().remove(agent) {
            kept.ended.store(true, Ordering::SeqCst);
        }
        self.0.released.lock().unwrap().push(agent.clone());
    }

    fn retain(&self, mut harness: Handle) {
        if let Some(kept) = harness.0.take() {
            self.0
                .retained
                .lock()
                .unwrap()
                .push((kept, harness.1.take()));
        }
    }

    fn note(&self, _harness: &Handle, note: &str) {
        self.0.notes.lock().unwrap().push(note.to_string());
    }
}

fn wait_until(mut ready: impl FnMut() -> bool) -> bool {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(30) {
        if ready() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    ready()
}

struct Fixture {
    _dir: tempfile::TempDir,
    store: Arc<Store>,
    left: Arc<Left>,
    launches: Arc<AtomicUsize>,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
        Self {
            _dir: dir,
            store,
            left: Arc::new(Left::default()),
            launches: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn running(&self, name: &str, harness: &str) -> AgentId {
        let agent = self
            .store
            .create_agent(NewAgent::new(name, harness))
            .unwrap();
        let work = self.store.data_dir().join(format!("work-{name}"));
        std::fs::create_dir_all(&work).unwrap();
        self.store
            .set_agent_running(&agent.id, &work.display().to_string())
            .unwrap();
        agent.id
    }

    /// What a previous kernel leaves: a live process holding the agent's
    /// lock, and the kernel's own copy released without unlocking.
    fn leave(&self, agent: &AgentId, kept: &Kept, harness: &str, note: Option<&str>) {
        let lock = self.store.try_lock_agent(agent).unwrap();
        let host = lock.share().unwrap();
        lock.retain();
        drop(lock);
        self.left.processes.lock().unwrap().insert(
            agent.clone(),
            (
                kept.clone(),
                host,
                harness.to_string(),
                note.map(str::to_string),
            ),
        );
    }

    fn start(&self) -> Supervisor<Handle> {
        let launches = Arc::clone(&self.launches);
        Supervisor::start_with_custody(
            Arc::clone(&self.store),
            Arc::new(ManualClock::new(1_699_000_000_000)) as Arc<dyn Clock>,
            Arc::new(move |request: LaunchRequest| {
                let lock = request
                    .lock
                    .as_ref()
                    .expect("a keeper launch carries the lock");
                launches.fetch_add(1, Ordering::SeqCst);
                Ok(Handle(Some(Kept::new()), Some(lock.share().unwrap())))
            }),
            Vec::new(),
            Arc::new(FakeCustody(Arc::clone(&self.left))),
        )
        .unwrap()
    }
}

#[test]
fn a_running_agent_takes_over_the_process_its_last_kernel_left() {
    let fx = Fixture::new();
    let agent = fx.running("alpha", "claude-code");
    fx.store.receive(&agent, Incoming::new("before")).unwrap();
    let first = fx.store.next_undelivered(&agent).unwrap().unwrap();
    // The last kernel wrote the line, then died before moving the cursor.
    let kept = Kept::new();
    kept.fake
        .lock()
        .unwrap()
        .deliver(&OutboundMessage {
            id: first.id.as_str().to_string(),
            seq: first.seq,
            text: first.text.clone(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: Default::default(),
        })
        .unwrap();
    fx.leave(&agent, &kept, "claude-code", None);

    let supervisor = fx.start();
    assert!(
        wait_until(|| fx.store.delivered_cursor(&agent).unwrap() == first.seq),
        "the cursor did not move over the line the process already holds"
    );
    assert_eq!(
        kept.written(),
        [first.id.as_str()],
        "a line was written twice"
    );
    assert_eq!(
        fx.launches.load(Ordering::SeqCst),
        0,
        "a process was started"
    );
    assert!(
        supervisor
            .live(&agent)
            .is_some_and(|live| live.process_alive && live.pid == Some(4242))
    );

    fx.store.receive(&agent, Incoming::new("after")).unwrap();
    assert!(wait_until(|| kept.written().len() == 2));
    assert!(
        !fx.left.notes.lock().unwrap().is_empty(),
        "no note was kept"
    );

    // A kernel that stops for an upgrade leaves the process and its lock.
    supervisor.retain_processes();
    assert!(!kept.ended.load(Ordering::SeqCst), "the process was ended");
    assert_eq!(fx.left.retained.lock().unwrap().len(), 1);
    assert!(matches!(
        fx.store.try_lock_agent(&agent),
        Err(anyagents_core::Error::AgentBusy)
    ));
}

#[test]
fn a_turn_in_a_process_taken_over_gets_no_recovery_note() {
    let fx = Fixture::new();
    let agent = fx.running("alpha", "claude-code");
    fx.store.receive(&agent, Incoming::new("working")).unwrap();
    let first = fx.store.next_undelivered(&agent).unwrap().unwrap();
    let kept = Kept::new();
    kept.fake
        .lock()
        .unwrap()
        .deliver(&OutboundMessage {
            id: first.id.as_str().to_string(),
            seq: first.seq,
            text: first.text.clone(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: Default::default(),
        })
        .unwrap();
    // The last kernel moved the cursor; the turn is still open when it stops.
    fx.store.note_delivered(&agent, first.seq).unwrap();
    assert!(fx.store.has_open_turn(&agent).unwrap());
    fx.leave(&agent, &kept, "claude-code", None);
    let supervisor = fx.start();
    assert!(wait_until(|| {
        supervisor
            .live(&agent)
            .is_some_and(|live| live.process_alive)
    }));
    // A restarted process would be offered a `[Recovery]` note for that turn.
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        kept.given(),
        [first.id.as_str()],
        "a recovery note was offered"
    );
    assert_eq!(fx.launches.load(Ordering::SeqCst), 0);
    // The turn ends in the process that ran it.
    kept.fake.lock().unwrap().complete_turn();
    drop(supervisor);
}

#[test]
fn a_stopping_kernel_without_retention_ends_the_process_and_frees_the_lock() {
    let fx = Fixture::new();
    let agent = fx.running("alpha", "claude-code");
    let kept = Kept::new();
    fx.leave(&agent, &kept, "claude-code", None);
    let supervisor = fx.start();
    assert!(wait_until(|| {
        supervisor
            .live(&agent)
            .is_some_and(|live| live.process_alive)
    }));
    drop(supervisor);
    assert!(kept.ended.load(Ordering::SeqCst));
    assert!(fx.left.retained.lock().unwrap().is_empty());
    fx.store.try_lock_agent(&agent).unwrap();
}

#[test]
fn a_process_left_for_an_agent_that_no_longer_runs_is_ended() {
    let fx = Fixture::new();
    let agent = fx.running("alpha", "claude-code");
    let kept = Kept::new();
    fx.leave(&agent, &kept, "claude-code", None);
    fx.store.set_agent_state(&agent, "paused").unwrap();
    let supervisor = fx.start();
    assert!(wait_until(|| fx
        .left
        .released
        .lock()
        .unwrap()
        .contains(&agent)));
    assert!(kept.ended.load(Ordering::SeqCst));
    assert_eq!(fx.launches.load(Ordering::SeqCst), 0);
    drop(supervisor);
}

#[test]
fn a_process_launched_for_another_harness_is_switched_at_its_boundary() {
    let fx = Fixture::new();
    let agent = fx.running("alpha", "claude-code");
    let kept = Kept::new();
    // The agent moved to Claude Code while no kernel ran; its process is Codex.
    fx.leave(&agent, &kept, "codex", None);
    let supervisor = fx.start();
    assert!(
        wait_until(|| fx.launches.load(Ordering::SeqCst) == 1),
        "the idle process was not replaced"
    );
    assert!(wait_until(|| kept.ended.load(Ordering::SeqCst)));
    drop(supervisor);
}

#[test]
fn the_note_a_kernel_kept_restores_its_usage_accounting() {
    let fx = Fixture::new();
    let agent = fx.running("alpha", "claude-code");
    let kept = Kept::new();
    let note = r#"{"served":{"input_tokens":5,"output_tokens":3,"cache_read_tokens":0,"cache_write_tokens":0,"reported_cost_micros":null},"turns_recorded":2,"written":0,"profile_rotated_ms":0,"completed_turn":true}"#;
    fx.leave(&agent, &kept, "claude-code", Some(note));
    let supervisor = fx.start();
    assert!(wait_until(|| !fx.left.notes.lock().unwrap().is_empty()));
    let saved = fx.left.notes.lock().unwrap()[0].clone();
    let saved: serde_json::Value = serde_json::from_str(&saved).unwrap();
    assert_eq!(saved["turns_recorded"], 2);
    assert_eq!(saved["served"]["input_tokens"], 5);
    assert_eq!(saved["completed_turn"], true);
    drop(supervisor);
}
