//! A turn the harness ends with an error is closed with that outcome, never
//! left open to hold the mailbox: the work it held comes back as a
//! `[Recovery]` note, and at `MAX_ERRORED_TURNS` in a row the agent is parked
//! and its parent told.

mod support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyagents_core::{
    Incoming, MAX_ERRORED_TURNS, MAX_TRANSIENT_ERRORED_TURNS, NewAgent, SYSTEM_SENDER, Store,
};
use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    TurnState,
};
use anyagents_supervisor::{Clock, LaunchRequest, ManualClock, Supervisor, SystemClock};

const OVERFLOW: &str = "Sylphx AI failure Some(\"context_length_exceeded\"): The request exceeds the model's context window.";

/// What a busy provider answers: the same request may succeed later.
const UNAVAILABLE: &str = "Sylphx AI failure Some(\"service_unavailable\"): The model response was interrupted. Retry the request.";

/// A vendor whose turns end at once: the next `fail` of them with `error`
/// (as Spiron reports a turn the model refused), the rest completed.
#[derive(Default)]
struct Vendor {
    fail: usize,
    error: Option<&'static str>,
    served: u32,
    fault: Option<String>,
    given: Vec<(String, String)>,
}

struct ErroringHarness {
    vendor: Arc<Mutex<Vendor>>,
}

impl Harness for ErroringHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let mut vendor = self.vendor.lock().unwrap();
        if vendor.given.iter().any(|(id, _)| id == &message.id) {
            return Ok(DeliverOutcome::Accepted {
                turn_finished: true,
            });
        }
        vendor
            .given
            .push((message.id.clone(), message.text.clone()));
        // Each input is one turn, and its result comes back at once.
        vendor.served += 1;
        vendor.fault = if vendor.fail > 0 {
            vendor.fail -= 1;
            Some(vendor.error.unwrap_or(OVERFLOW).to_string())
        } else {
            None
        };
        Ok(DeliverOutcome::Accepted {
            turn_finished: true,
        })
    }
    fn background(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }
    fn resume(&mut self, _: &str) -> Result<(), HarnessError> {
        Ok(())
    }
    fn cancel(&mut self) -> Result<(), HarnessError> {
        Ok(())
    }
    fn health(&self) -> HarnessHealth {
        HarnessHealth {
            process_alive: true,
            turn_state: TurnState::Idle,
            delivery: Delivery {
                level: DeliveryLevel::MidTurn,
                idempotent: false,
                proof: std::borrow::Cow::Borrowed("scripted double"),
                peer: None,
            },
            quota_reset_at_ms: None,
            sign_in_fault: self.vendor.lock().unwrap().fault.clone(),
        }
    }
    fn turns_served(&self) -> u32 {
        self.vendor.lock().unwrap().served
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
    vendor: Arc<Mutex<Vendor>>,
    lead: anyagents_core::Agent,
    worker: anyagents_core::Agent,
    supervisor: Supervisor<ErroringHarness>,
}

fn start(fail: usize) -> Fixture {
    start_with(fail, None, Arc::new(SystemClock::new()))
}

fn start_with(fail: usize, error: Option<&'static str>, clock: Arc<dyn Clock>) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let lead = store.create_agent(NewAgent::new("lead", "spiron")).unwrap();
    let worker = store
        .create_agent(NewAgent {
            parent_id: Some(lead.id.clone()),
            ..NewAgent::new("worker", "spiron")
        })
        .unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    store
        .set_agent_running(&worker.id, &work.display().to_string())
        .unwrap();
    let vendor = Arc::new(Mutex::new(Vendor {
        fail,
        error,
        ..Vendor::default()
    }));
    let supervisor = Supervisor::start(
        Arc::clone(&store),
        clock,
        {
            let vendor = Arc::clone(&vendor);
            Arc::new(move |request: LaunchRequest| {
                // Only the worker runs here: a notice that wakes its parent
                // starts nothing.
                if request.agent.name != "worker" {
                    return Err("not under test".to_string());
                }
                Ok(ErroringHarness {
                    vendor: Arc::clone(&vendor),
                })
            })
        },
        Vec::new(),
    )
    .unwrap();
    Fixture {
        _dir: dir,
        store,
        vendor,
        lead,
        worker,
        supervisor,
    }
}

fn notices(store: &Store, agent: &anyagents_core::Agent) -> Vec<String> {
    store
        .inbox(&agent.id, None)
        .unwrap()
        .into_iter()
        .filter(|m| m.sender.as_deref() == Some(SYSTEM_SENDER))
        .map(|m| m.text)
        .collect()
}

#[test]
fn an_errored_turn_is_closed_and_its_work_comes_back_with_a_recovery_note() {
    let fx = start(1);
    let task = match fx
        .store
        .receive(&fx.worker.id, Incoming::new("summarise the incident"))
        .unwrap()
    {
        anyagents_core::ReceiveOutcome::New(message) => message,
        anyagents_core::ReceiveOutcome::Duplicate(_) => unreachable!(),
    };
    // The task errors, the note that follows it completes.
    assert!(
        wait_until(|| fx.vendor.lock().unwrap().given.len() == 2),
        "{:?}",
        fx.vendor.lock().unwrap().given
    );
    let (_, note) = fx.vendor.lock().unwrap().given[1].clone();
    assert!(
        note.contains("[Recovery] Your previous turn ended with an error"),
        "{note}"
    );
    assert!(note.contains("context_length_exceeded"), "{note}");
    assert!(note.contains(task.id.as_str()), "{note}");
    assert!(note.contains("summarise the incident"), "{note}");
    assert!(note.contains("errored turn 1 of"), "{note}");
    assert!(wait_until(|| {
        fx.store
            .turn_errors(&fx.worker.id)
            .unwrap()
            .is_some_and(|errors| errors.streak == 0)
    }));
    assert!(!fx.store.has_open_turn(&fx.worker.id).unwrap());
    assert_ne!(fx.store.agent(&fx.worker.id).unwrap().state, "paused");
    assert!(notices(&fx.store, &fx.lead).is_empty());

    // Mail that arrives later is delivered as usual.
    fx.store
        .receive(&fx.worker.id, Incoming::new("next task"))
        .unwrap();
    assert!(wait_until(|| fx.vendor.lock().unwrap().given.len() == 3));
    support::stop(fx.supervisor);
}

#[test]
fn turns_that_keep_erroring_park_the_agent_and_tell_its_parent() {
    let fx = start(usize::MAX);
    fx.store
        .receive(&fx.worker.id, Incoming::new("summarise the incident"))
        .unwrap();
    assert!(wait_until(|| {
        fx.store
            .agent(&fx.worker.id)
            .is_ok_and(|agent| agent.state == "paused")
    }));
    let errors = fx.store.turn_errors(&fx.worker.id).unwrap().unwrap();
    assert_eq!(errors.streak, MAX_ERRORED_TURNS);
    assert!(errors.faulted_at_ms.is_some());
    assert!(
        errors
            .last_error
            .unwrap()
            .contains("context_length_exceeded")
    );
    // The task, then one note per errored turn below the limit: bounded.
    let given = fx.vendor.lock().unwrap().given.len();
    assert_eq!(given, MAX_ERRORED_TURNS as usize);
    assert!(
        fx.store.has_open_turn(&fx.worker.id).unwrap(),
        "the last turn waits for a resume to recover it"
    );

    // The parking commits first; its notice follows.
    assert!(wait_until(|| !notices(&fx.store, &fx.lead).is_empty()));
    let told = notices(&fx.store, &fx.lead);
    assert_eq!(told.len(), 1, "{told:?}");
    assert!(
        told[0].starts_with("[Sub-Agent Faulted] Your sub-agent `worker`"),
        "{}",
        told[0]
    );
    assert!(told[0].contains("context_length_exceeded"), "{}", told[0]);

    // Mail while parked waits, and its sender is told once.
    fx.store
        .receive(&fx.worker.id, {
            let mut incoming = Incoming::new("are you there?");
            incoming.sender = Some(format!("agent:{}", fx.lead.id));
            incoming
        })
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(fx.vendor.lock().unwrap().given.len(), given);
    let told = notices(&fx.store, &fx.lead);
    assert_eq!(told.len(), 2, "{told:?}");
    assert!(told[1].starts_with("[Queued] `worker`"), "{}", told[1]);
    assert!(told[1].contains("kept ending in an error"), "{}", told[1]);

    // A resume clears the parking and the count; the waiting mail goes out.
    fx.vendor.lock().unwrap().fail = 0;
    fx.store.set_agent_state(&fx.worker.id, "running").unwrap();
    let errors = fx.store.turn_errors(&fx.worker.id).unwrap().unwrap();
    assert_eq!(errors.streak, 0);
    assert!(errors.faulted_at_ms.is_none());
    assert!(wait_until(|| {
        fx.vendor
            .lock()
            .unwrap()
            .given
            .iter()
            .any(|(_, text)| text.contains("are you there?"))
    }));
    // The parked turn is recovered first, then closed by its completion.
    let given = fx.vendor.lock().unwrap().given.clone();
    assert!(
        given
            .iter()
            .any(|(id, text)| id.starts_with("recovery_") && text.contains("[Recovery]")),
        "{given:?}"
    );
    assert!(wait_until(|| !fx
        .store
        .has_open_turn(&fx.worker.id)
        .unwrap()));
    support::stop(fx.supervisor);
}

/// A provider that is unavailable is not a refusal: each retry waits out a
/// growing backoff, the turn stays open (a restart would recover it) while
/// it waits, and only a long run parks the agent.
#[test]
fn transient_errors_back_off_and_park_only_after_a_long_run() {
    let clock = ManualClock::new(1_700_000_000_000);
    let fx = start_with(
        usize::MAX,
        Some(UNAVAILABLE),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
    );
    let given = || fx.vendor.lock().unwrap().given.len();
    let advance = |ms: i64| clock.set_ms(clock.now_ms() + ms);
    fx.store
        .receive(&fx.worker.id, Incoming::new("summarise the incident"))
        .unwrap();
    assert!(wait_until(|| given() == 1));
    // The next pass finds the errored turn and starts its backoff.
    advance(1_000);
    assert!(wait_until(|| {
        fx.store
            .turn_errors(&fx.worker.id)
            .unwrap()
            .is_some_and(|errors| errors.streak == 1)
    }));
    advance(10_000);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(given(), 1, "nothing is retried inside the backoff");
    assert!(
        fx.store.has_open_turn(&fx.worker.id).unwrap(),
        "the turn stays open while it waits"
    );
    // Past it, the note goes out and the turn closes.
    advance(5_000);
    assert!(wait_until(|| given() == 2));
    let (_, note) = fx.vendor.lock().unwrap().given[1].clone();
    assert!(note.contains("errored turn 1 of 8"), "{note}");

    // Long after the refusal limit, a transient run still retries.
    let mut steps = 0;
    while given() < MAX_ERRORED_TURNS as usize + 1 && steps < 200 {
        advance(700_000);
        std::thread::sleep(Duration::from_millis(30));
        steps += 1;
    }
    assert!(given() > MAX_ERRORED_TURNS as usize);
    assert_ne!(fx.store.agent(&fx.worker.id).unwrap().state, "paused");
    // Until its own limit.
    let mut steps = 0;
    while fx.store.agent(&fx.worker.id).unwrap().state != "paused" && steps < 400 {
        advance(700_000);
        std::thread::sleep(Duration::from_millis(30));
        steps += 1;
    }
    assert_eq!(fx.store.agent(&fx.worker.id).unwrap().state, "paused");
    assert_eq!(
        fx.store.turn_errors(&fx.worker.id).unwrap().unwrap().streak,
        MAX_TRANSIENT_ERRORED_TURNS
    );
    assert_eq!(given(), MAX_TRANSIENT_ERRORED_TURNS as usize);
    assert!(wait_until(|| notices(&fx.store, &fx.lead).len() == 1));
    support::stop(fx.supervisor);
}
