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
    OutboundMessage, Proof, TurnUsage,
};
use anyagents_supervisor::{Attached, Clock, Custody, LaunchRequest, ManualClock, Supervisor};

/// A fake vendor process the test can still look at after handing it over.
#[derive(Clone)]
struct Kept {
    fake: Arc<Mutex<FakeHarness>>,
    ended: Arc<AtomicBool>,
    /// What the vendor reports it served, and in how many turns.
    usage: Arc<Mutex<(TurnUsage, u32)>>,
    /// Another kernel took the process over.
    superseded: Arc<AtomicBool>,
}

impl Kept {
    fn new() -> Self {
        Self {
            fake: Arc::new(Mutex::new(FakeHarness::new(DeliveryMode::Steer))),
            ended: Arc::new(AtomicBool::new(false)),
            usage: Arc::new(Mutex::new((TurnUsage::default(), 0))),
            superseded: Arc::new(AtomicBool::new(false)),
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

    fn proven(&self, message_id: &str) -> Option<Proof> {
        self.kept().fake.lock().unwrap().proven(message_id)
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

    fn served_usage(&self) -> TurnUsage {
        self.kept().usage.lock().unwrap().0.clone()
    }

    fn turns_served(&self) -> u32 {
        self.kept().usage.lock().unwrap().1
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
    /// Each note kept, with how many lines the process had been given then.
    given_at_note: Mutex<Vec<(serde_json::Value, usize)>>,
    /// Older processes an interrupted replacement left beside an agent's.
    older: Mutex<HashMap<AgentId, Vec<Kept>>>,
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
        let older = self
            .0
            .older
            .lock()
            .unwrap()
            .remove(agent)
            .into_iter()
            .flatten()
            .map(|kept| Handle(Some(kept), None))
            .collect();
        let lock = AgentLock::adopt(host_lock.as_fd().try_clone_to_owned().unwrap());
        Ok(Some(Attached {
            harness: Handle(Some(kept), Some(host_lock)),
            lock,
            harness_name: harness,
            model: None,
            started_at_ms: Some(7),
            note,
            older,
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

    fn note(&self, harness: &Handle, note: &str) {
        self.0.notes.lock().unwrap().push(note.to_string());
        let given = harness.kept().given().len();
        if let Ok(value) = serde_json::from_str(note) {
            self.0.given_at_note.lock().unwrap().push((value, given));
        }
    }

    fn superseded(&self, harness: &Handle) -> bool {
        harness.kept().superseded.load(Ordering::SeqCst)
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
    clock: Arc<ManualClock>,
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
            clock: Arc::new(ManualClock::new(1_699_000_000_000)),
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
            Arc::clone(&self.clock) as Arc<dyn Clock>,
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

/// A usage row the last kernel recorded for a process is not recorded again
/// by the kernel that takes the process over: the note it kept carries the
/// accounting.
#[test]
fn usage_a_previous_kernel_recorded_is_not_recorded_again() {
    let fx = Fixture::new();
    let agent = fx.running("alpha", "claude-code");
    let kept = Kept::new();
    *kept.usage.lock().unwrap() = (
        TurnUsage {
            input_tokens: 5,
            output_tokens: 3,
            ..TurnUsage::default()
        },
        1,
    );
    fx.leave(&agent, &kept, "claude-code", None);
    let first = fx.start();
    assert!(wait_until(|| fx.store.list_usage(None).unwrap().len() == 1));
    first.retain_processes();
    let note = fx.left.notes.lock().unwrap().last().cloned();
    let (kept_again, keeper_lock) = fx.left.retained.lock().unwrap().pop().unwrap();
    // The next kernel finds the same process, with the note the last one kept.
    fx.left.processes.lock().unwrap().insert(
        agent.clone(),
        (kept_again, keeper_lock.unwrap(), "claude-code".into(), note),
    );
    let second = fx.start();
    assert!(wait_until(|| fx.left.processes.lock().unwrap().is_empty()));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        fx.store.list_usage(None).unwrap().len(),
        1,
        "the usage was recorded twice"
    );
    drop(second);
}

/// A recovery note the last kernel still owed (it died before sending it) is
/// sent by the kernel that takes the process over; so is one for a turn the
/// store holds open while the process sits idle.
#[test]
fn an_owed_recovery_survives_the_kernel_that_owed_it() {
    for owed_in_note in [true, false] {
        let fx = Fixture::new();
        let agent = fx.running("alpha", "claude-code");
        fx.store.receive(&agent, Incoming::new("working")).unwrap();
        let first = fx.store.next_undelivered(&agent).unwrap().unwrap();
        fx.store.note_delivered(&agent, first.seq).unwrap();
        assert!(fx.store.has_open_turn(&agent).unwrap());
        // The process holds no open turn: it was lost while no kernel ran.
        let kept = Kept::new();
        let note = owed_in_note.then(|| {
            r#"{"served":null,"turns_recorded":0,"written":1,"profile_rotated_ms":0,"completed_turn":false,"pending_recovery":true}"#.to_string()
        });
        fx.leave(&agent, &kept, "claude-code", note.as_deref());
        let supervisor = fx.start();
        assert!(
            wait_until(|| kept.given().iter().any(|id| id.starts_with("recovery_"))),
            "the owed recovery note was not sent (owed in the note: {owed_in_note})"
        );
        assert_eq!(fx.launches.load(Ordering::SeqCst), 0);
        drop(supervisor);
    }
}

/// A turn the process finished while no kernel ran is recorded as completed
/// by the kernel that takes the process over, with its usage once: a routine
/// restart sends no `[Recovery]` note.
#[test]
fn a_turn_that_ended_while_no_kernel_ran_is_completed_not_recovered() {
    let fx = Fixture::new();
    let agent = fx.running("alpha", "claude-code");
    fx.store.receive(&agent, Incoming::new("working")).unwrap();
    let first = fx.store.next_undelivered(&agent).unwrap().unwrap();
    fx.store.note_delivered(&agent, first.seq).unwrap();
    assert!(fx.store.has_open_turn(&agent).unwrap());
    // The last kernel wrote the line; the process finished the turn after.
    let kept = Kept::new();
    *kept.usage.lock().unwrap() = (
        TurnUsage {
            input_tokens: 5,
            output_tokens: 3,
            ..TurnUsage::default()
        },
        1,
    );
    let note = r#"{"served":null,"turns_recorded":0,"written":1,"profile_rotated_ms":0,"completed_turn":false,"pending_recovery":false}"#;
    fx.leave(&agent, &kept, "claude-code", Some(note));
    let supervisor = fx.start();
    assert!(wait_until(|| !fx.store.has_open_turn(&agent).unwrap()));
    assert!(wait_until(|| fx.store.list_usage(None).unwrap().len() == 1));
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        !kept.given().iter().any(|id| id.starts_with("recovery_")),
        "a turn that ended was recovered: {:?}",
        kept.given()
    );
    assert_eq!(fx.store.list_usage(None).unwrap().len(), 1);
    assert_eq!(fx.launches.load(Ordering::SeqCst), 0);
    drop(supervisor);
}

/// The note kept with a process says an owed recovery is no longer owed as
/// soon as the harness takes the recovery note, before any further mail: a
/// kernel that dies right after leaves no second note to send.
#[test]
fn an_accepted_recovery_note_is_kept_before_more_mail() {
    let fx = Fixture::new();
    let agent = fx.running("alpha", "claude-code");
    fx.store.receive(&agent, Incoming::new("working")).unwrap();
    let first = fx.store.next_undelivered(&agent).unwrap().unwrap();
    fx.store.note_delivered(&agent, first.seq).unwrap();
    fx.store.receive(&agent, Incoming::new("next")).unwrap();
    let kept = Kept::new();
    let note = r#"{"served":null,"turns_recorded":0,"written":1,"profile_rotated_ms":0,"completed_turn":false,"pending_recovery":true}"#;
    fx.leave(&agent, &kept, "claude-code", Some(note));
    let supervisor = fx.start();
    assert!(wait_until(|| kept.given().len() >= 2));
    assert!(
        kept.given()[0].starts_with("recovery_"),
        "{:?}",
        kept.given()
    );
    let kept_notes = fx.left.given_at_note.lock().unwrap().clone();
    assert!(
        kept_notes
            .iter()
            .any(|(note, given)| note["pending_recovery"] == false && *given == 1),
        "the accepted recovery was not kept before more mail: {kept_notes:?}"
    );
    drop(supervisor);
}

/// A process an interrupted replacement left beside the newer one is
/// retired as a replacement retires one: stopped, what it proved committed,
/// then ended. What it took is never handed to the newer process.
#[test]
fn an_older_process_left_beside_is_committed_then_ended() {
    let fx = Fixture::new();
    let agent = fx.running("alpha", "claude-code");
    fx.store
        .receive(&agent, Incoming::new("taken by the old one"))
        .unwrap();
    let first = fx.store.next_undelivered(&agent).unwrap().unwrap();
    let older = Kept::new();
    older
        .fake
        .lock()
        .unwrap()
        .deliver(&anyagents_harness::OutboundMessage {
            id: first.id.as_str().to_string(),
            seq: first.seq,
            text: first.text.clone(),
            sender: None,
            reply_to: None,
            source_time_ms: None,
            context: Default::default(),
        })
        .unwrap();
    let newer = Kept::new();
    fx.leave(&agent, &newer, "claude-code", None);
    fx.left
        .older
        .lock()
        .unwrap()
        .insert(agent.clone(), vec![older.clone()]);
    let supervisor = fx.start();
    assert!(wait_until(|| older.ended.load(Ordering::SeqCst)));
    assert!(wait_until(|| fx
        .store
        .next_undelivered(&agent)
        .unwrap()
        .is_none()));
    // The old process's turn was interrupted: the newer process gets that
    // mail in a recovery note, never as a line of its own.
    assert!(
        wait_until(|| newer.given().iter().any(|id| id.starts_with("recovery_"))),
        "the interrupted turn of the old process was never recovered: {:?}",
        newer.given()
    );
    std::thread::sleep(Duration::from_millis(300));
    assert!(
        !newer.given().contains(&first.id.as_str().to_string()),
        "what the old process took was handed to the newer one"
    );
    assert!(!newer.ended.load(Ordering::SeqCst));
    assert_eq!(fx.launches.load(Ordering::SeqCst), 0);
    drop(supervisor);
}

/// A process found dead when it is taken over reads as idle with no usage
/// (its keeper is gone); that is no proof its turn finished. The turn stays
/// open and the replacement gets its recovery note.
#[test]
fn a_process_found_dead_is_recovered_not_completed() {
    let fx = Fixture::new();
    let agent = fx.running("alpha", "claude-code");
    fx.store.receive(&agent, Incoming::new("working")).unwrap();
    let first = fx.store.next_undelivered(&agent).unwrap().unwrap();
    fx.store.note_delivered(&agent, first.seq).unwrap();
    let kept = Kept::new();
    kept.fake.lock().unwrap().kill();
    let note = r#"{"served":{"input_tokens":5,"output_tokens":3,"cache_read_tokens":0,"cache_write_tokens":0,"reported_cost_micros":null},"turns_recorded":1,"written":1,"profile_rotated_ms":0,"completed_turn":true,"pending_recovery":false}"#;
    fx.leave(&agent, &kept, "claude-code", Some(note));
    let supervisor = fx.start();
    // The dead process is replaced after its backoff.
    let mut now = 1_699_000_000_000;
    assert!(
        wait_until(|| {
            now += 60_000;
            fx.clock.set_ms(now);
            fx.store
                .open_turn(&agent)
                .unwrap()
                .is_some_and(|open| open.recovery_count >= 1)
        }),
        "the interrupted turn was not recovered"
    );
    assert!(fx.store.has_open_turn(&agent).unwrap());
    assert!(fx.launches.load(Ordering::SeqCst) >= 1);
    drop(supervisor);
}

/// A kernel whose process another kernel took over stands down: it neither
/// ends the process nor starts a second one, and the lock stays held.
#[test]
fn a_superseded_kernel_stands_down() {
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
    kept.superseded.store(true, Ordering::SeqCst);
    // Mail wakes the worker, which finds itself superseded.
    fx.store
        .receive(&agent, Incoming::new("for the other kernel"))
        .unwrap();
    assert!(wait_until(|| fx.left.retained.lock().unwrap().len() == 1));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(
        fx.launches.load(Ordering::SeqCst),
        0,
        "a second process started"
    );
    assert!(
        !kept.ended.load(Ordering::SeqCst),
        "the other kernel's process was ended"
    );
    assert!(
        kept.written().is_empty(),
        "mail went to a process it no longer owns"
    );
    assert!(matches!(
        fx.store.try_lock_agent(&agent),
        Err(anyagents_core::Error::AgentBusy)
    ));
    drop(supervisor);
}
