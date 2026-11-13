//! Supervisor-level delivery scenarios: what the supervisor does with the
//! proofs, refusals and interrupted turns an adapter reports, across process
//! replacements. A scripted vendor keeps one record per process it started.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyagents_core::{AgentId, Incoming, Message, NewAgent, ReceiveOutcome, Store, Wake};
use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    Proof, TurnState,
};
use anyagents_supervisor::{Clock, LaunchRequest, ManualClock, Supervisor};

const CEILING: Duration = Duration::from_secs(30);
const START_MS: i64 = 1_700_000_000_000;

/// One vendor process, as the test scripts it.
#[derive(Default)]
struct Process {
    alive: bool,
    in_turn: bool,
    quota_reset_at_ms: Option<i64>,
    /// Every line written to this process, by id, in order, with its text.
    written: Vec<(String, String)>,
    /// Lines this process proved it took.
    proven: HashSet<String>,
    /// Proofs still in the process's output pipe: read once it is stopped
    /// and drained to the end.
    proven_at_stop: HashSet<String>,
    /// Prove every line as it is written.
    prove_on_write: bool,
    /// Ids the vendor refuses for good.
    refused: HashMap<String, String>,
    stopped: bool,
}

#[derive(Clone, Default)]
struct Vendor {
    processes: Arc<Mutex<Vec<Arc<Mutex<Process>>>>>,
    /// How each new process starts.
    prove_on_write: Arc<Mutex<bool>>,
}

impl Vendor {
    fn process(&self, generation: usize) -> Arc<Mutex<Process>> {
        Arc::clone(&self.processes.lock().unwrap()[generation])
    }

    fn launches(&self) -> usize {
        self.processes.lock().unwrap().len()
    }

    /// Every line written to every process, as (generation, id, text).
    fn writes(&self) -> Vec<(usize, String, String)> {
        let processes = self.processes.lock().unwrap();
        processes
            .iter()
            .enumerate()
            .flat_map(|(generation, process)| {
                process
                    .lock()
                    .unwrap()
                    .written
                    .iter()
                    .map(|(id, text)| (generation, id.clone(), text.clone()))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn launch(&self) -> Scripted {
        let process = Arc::new(Mutex::new(Process {
            alive: true,
            prove_on_write: *self.prove_on_write.lock().unwrap(),
            ..Process::default()
        }));
        self.processes.lock().unwrap().push(Arc::clone(&process));
        Scripted { process }
    }
}

struct Scripted {
    process: Arc<Mutex<Process>>,
}

impl Harness for Scripted {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        if let Some(Proof::Taken { turn_finished }) = self.proven(&message.id) {
            return Ok(DeliverOutcome::Accepted { turn_finished });
        }
        let mut process = self.process.lock().unwrap();
        if !process.alive {
            return Err(HarnessError::Dead);
        }
        if let Some(reason) = process.refused.get(&message.id) {
            return Ok(DeliverOutcome::Rejected {
                reason: reason.clone(),
            });
        }
        if let Some(reset_at_ms) = process.quota_reset_at_ms {
            return Ok(DeliverOutcome::Quota { reset_at_ms });
        }
        if process.written.iter().any(|(id, _)| id == &message.id) {
            return Ok(DeliverOutcome::InFlight);
        }
        process
            .written
            .push((message.id.clone(), message.text.clone()));
        process.in_turn = true;
        if process.prove_on_write {
            process.proven.insert(message.id.clone());
            return Ok(DeliverOutcome::Accepted {
                turn_finished: false,
            });
        }
        Ok(DeliverOutcome::InFlight)
    }

    fn proven(&self, message_id: &str) -> Option<Proof> {
        self.process
            .lock()
            .unwrap()
            .proven
            .contains(message_id)
            .then_some(Proof::Taken {
                turn_finished: false,
            })
    }

    fn stop(&mut self) {
        let mut process = self.process.lock().unwrap();
        let late: Vec<String> = process.proven_at_stop.drain().collect();
        process.proven.extend(late);
        process.alive = false;
        process.stopped = true;
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
        let process = self.process.lock().unwrap();
        HarnessHealth {
            process_alive: process.alive,
            turn_state: if process.in_turn {
                TurnState::InTurn
            } else {
                TurnState::Idle
            },
            delivery: Delivery {
                level: DeliveryLevel::NextStep,
                idempotent: false,
                proof: std::borrow::Cow::Borrowed("scripted proof"),
                peer: None,
            },
            quota_reset_at_ms: process.quota_reset_at_ms,
            sign_in_fault: None,
        }
    }
}

struct Scene {
    _dir: tempfile::TempDir,
    data: std::path::PathBuf,
    store: Arc<Store>,
    clock: ManualClock,
    vendor: Vendor,
    agent: AgentId,
    supervisor: Supervisor<Scripted>,
}

impl Scene {
    fn new(prove_on_write: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().to_path_buf();
        let store = Arc::new(Store::open(data.join("state.db")).unwrap());
        let agent = store
            .create_agent(NewAgent::new("worker", "codex"))
            .unwrap();
        store
            .set_agent_running(&agent.id, &data.display().to_string())
            .unwrap();
        let clock = ManualClock::new(START_MS);
        let vendor = Vendor::default();
        *vendor.prove_on_write.lock().unwrap() = prove_on_write;
        let supervisor = Supervisor::start(
            Arc::clone(&store),
            Arc::new(clock.clone()) as Arc<dyn Clock>,
            {
                let vendor = vendor.clone();
                Arc::new(move |_: LaunchRequest| Ok(vendor.launch()))
            },
            Vec::new(),
        )
        .unwrap();
        let scene = Self {
            _dir: dir,
            data,
            store,
            clock,
            vendor,
            agent: agent.id,
            supervisor,
        };
        scene.until("the first process starts", |scene| {
            scene.vendor.launches() == 1
        });
        scene
    }

    fn receive(&self, text: &str) -> Message {
        self.receive_from(text, None)
    }

    fn receive_from(&self, text: &str, sender: Option<&str>) -> Message {
        let mut incoming = Incoming::new(text);
        incoming.sender = sender.map(str::to_string);
        match self.store.receive(&self.agent, incoming).unwrap() {
            ReceiveOutcome::New(message) => message,
            ReceiveOutcome::Duplicate(_) => panic!("duplicate"),
        }
    }

    fn cursor(&self) -> i64 {
        self.store.delivered_cursor(&self.agent).unwrap()
    }

    fn poke(&self) {
        self.store.poke(Wake::Agent {
            agent_id: self.agent.clone(),
        });
    }

    /// Wait for `done` while the clock stands still.
    fn until(&self, what: &str, mut done: impl FnMut(&Self) -> bool) {
        let started = Instant::now();
        while !done(self) {
            assert!(started.elapsed() < CEILING, "never: {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Wait for `done` while time passes, a second per tick, so backoffs and
    /// review timers run.
    fn until_ticking(&self, what: &str, mut done: impl FnMut(&Self) -> bool) {
        let started = Instant::now();
        let mut ticks = 0;
        while !done(self) {
            assert!(started.elapsed() < CEILING, "never: {what}");
            ticks += 1;
            self.clock.set_ms(START_MS + ticks * 1_000);
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn written_to(&self, generation: usize) -> Vec<String> {
        self.vendor
            .process(generation)
            .lock()
            .unwrap()
            .written
            .iter()
            .map(|(id, _)| id.clone())
            .collect()
    }

    fn times_written(&self, id: &str) -> usize {
        self.vendor
            .writes()
            .iter()
            .filter(|(_, written, _)| written == id)
            .count()
    }
}

#[test]
fn a_harness_switch_commits_the_proofs_the_old_process_gave_before_it_stopped() {
    let scene = Scene::new(false);
    let first = scene.receive("first task");
    scene.until("the first line is written", |s| {
        s.written_to(0) == [first.id.to_string()]
    });
    // The vendor took it; its proof is still in the pipe when the switch comes.
    {
        let process = scene.vendor.process(0);
        let mut process = process.lock().unwrap();
        process.proven_at_stop.insert(first.id.to_string());
        process.in_turn = false;
    }
    scene
        .store
        .set_agent_model(&scene.agent, "next-model")
        .unwrap();
    scene.poke();
    scene.until_ticking("the switch starts a new process", |s| {
        s.vendor.launches() == 2
    });
    scene.until("the old process's proof is committed", |s| {
        s.cursor() == first.seq
    });
    assert!(scene.vendor.process(0).lock().unwrap().stopped);
    assert_eq!(scene.times_written(first.id.as_str()), 1);
    assert!(!scene.written_to(1).contains(&first.id.to_string()));
}

#[test]
fn a_profile_rotation_during_a_quota_wait_commits_what_the_old_process_proved() {
    let scene = Scene::new(false);
    let first = scene.receive("first task");
    scene.until("the first line is written", |s| s.written_to(0).len() == 1);
    {
        let process = scene.vendor.process(0);
        let mut process = process.lock().unwrap();
        process.quota_reset_at_ms = Some(START_MS + 3_600_000);
        process.proven_at_stop.insert(first.id.to_string());
        process.in_turn = false;
    }
    let config = anyagents_core::agent_config_dir(&scene.data, &scene.agent);
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join(".anyagents-profile-rotated-ms"),
        format!("{}\n", START_MS + 1),
    )
    .unwrap();
    scene.poke();
    scene.until_ticking("the rotation starts a new process", |s| {
        s.vendor.launches() == 2
    });
    scene.until("the proof is committed", |s| s.cursor() == first.seq);
    assert_eq!(scene.times_written(first.id.as_str()), 1);
}

#[test]
fn a_crash_puts_the_recovery_note_ahead_of_mail_that_came_after_it() {
    let scene = Scene::new(true);
    let first = scene.receive("the interrupted task");
    scene.until("the first line is committed", |s| s.cursor() == first.seq);
    // The process dies mid-turn; new mail arrives before its successor runs.
    scene.vendor.process(0).lock().unwrap().alive = false;
    let later = scene.receive("mail after the crash");
    scene.poke();
    scene.until_ticking("a new process starts and takes the later mail", |s| {
        s.vendor.launches() >= 2 && s.cursor() == later.seq
    });
    let written = scene.written_to(1);
    assert_eq!(written.len(), 2, "{written:?}");
    assert!(written[0].starts_with("recovery_"), "{written:?}");
    assert_eq!(written[1], later.id.to_string());
    let note = scene.vendor.process(1).lock().unwrap().written[0].1.clone();
    assert!(note.contains(first.id.as_str()), "{note}");
}

#[test]
fn an_interrupted_message_reaches_the_recovery_note_whole() {
    let scene = Scene::new(true);
    let tail = "and this is where it ends";
    let long = format!("{}{tail}", "y".repeat(5_000));
    let first = scene.receive(&long);
    scene.until("the long line is committed", |s| s.cursor() == first.seq);
    scene.vendor.process(0).lock().unwrap().alive = false;
    scene.poke();
    scene.until_ticking("the successor takes the recovery note", |s| {
        s.vendor.launches() >= 2 && !s.written_to(1).is_empty()
    });
    let note = scene.vendor.process(1).lock().unwrap().written[0].1.clone();
    assert!(note.contains(&long), "the note clipped the message");
}

#[test]
fn a_crash_between_consumption_and_proof_hands_the_line_to_the_next_process() {
    let scene = Scene::new(false);
    let first = scene.receive("taken, never proved");
    scene.until("the line is written", |s| s.written_to(0).len() == 1);
    scene.vendor.process(0).lock().unwrap().alive = false;
    scene.poke();
    scene.until_ticking("the successor is handed the unproved line", |s| {
        s.vendor.launches() >= 2 && s.written_to(1).contains(&first.id.to_string())
    });
    assert_eq!(scene.cursor(), 0, "nothing is committed without a proof");
    scene
        .vendor
        .process(1)
        .lock()
        .unwrap()
        .proven
        .insert(first.id.to_string());
    scene.poke();
    scene.until_ticking("the successor's proof is committed", |s| {
        s.cursor() == first.seq
    });
}

#[test]
fn a_poison_message_is_parked_its_sender_told_and_the_mail_behind_it_flows() {
    let scene = Scene::new(true);
    let sender = scene
        .store
        .create_agent(NewAgent::new("alice", "codex"))
        .unwrap();
    let poison = scene.receive_from("a message the vendor can never take", Some("agent:alice"));
    scene
        .vendor
        .process(0)
        .lock()
        .unwrap()
        .refused
        .insert(poison.id.to_string(), "malformed request (-32602)".into());
    let after = scene.receive("the next message");
    scene.poke();
    scene.until_ticking("the mail behind the poison is committed", |s| {
        s.cursor() == after.seq
    });
    assert_eq!(scene.vendor.launches(), 1, "a refusal restarts nothing");
    assert_eq!(scene.times_written(poison.id.as_str()), 0);
    let rejected = scene.store.rejected_messages(&scene.agent).unwrap();
    assert_eq!(rejected.len(), 1);
    assert_eq!(rejected[0].message_id, poison.id.to_string());
    assert!(rejected[0].reason.contains("-32602"));
    let note = scene
        .store
        .next_undelivered(&sender.id)
        .unwrap()
        .expect("the sender is told");
    assert!(note.text.starts_with("[Undeliverable]"), "{}", note.text);
    assert!(note.text.contains(poison.id.as_str()));
    let live = scene.supervisor.live(&scene.agent).unwrap();
    assert!(
        live.last_error
            .as_deref()
            .is_some_and(|error| error.contains(poison.id.as_str())),
        "{live:?}"
    );
}
