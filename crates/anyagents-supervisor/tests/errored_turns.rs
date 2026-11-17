//! A turn the harness ends with an error is closed with that outcome, never
//! left open to hold the mailbox: the work it held comes back as a
//! `[Recovery]` note, and at `MAX_ERRORED_TURNS` in a row the agent is parked
//! and its parent told.

mod support;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyagents_core::{Incoming, MAX_ERRORED_TURNS, NewAgent, SYSTEM_SENDER, Store};
use anyagents_harness::{
    DeliverOutcome, Delivery, DeliveryLevel, Harness, HarnessError, HarnessHealth, OutboundMessage,
    Proof, TurnState,
};
use anyagents_supervisor::{Clock, LaunchRequest, ManualClock, Supervisor, SystemClock};
use support::RecordingClock;

const OVERFLOW: &str = "Sylphx AI failure Some(\"context_length_exceeded\"): The request exceeds the model's context window.";

/// What a busy provider answers: the same request may succeed later.
const UNAVAILABLE: &str = "Sylphx AI failure Some(\"service_unavailable\"): The model response was interrupted. Retry the request.";

/// A vendor whose turns end at once: the next `fail` of them with `error`
/// (as Spiron reports a turn the model refused), the rest completed. With
/// `deferred` each input is written, taken and finished before `deliver`
/// returns, yet answered `Deferred`: the vendor's acceptance and result reach
/// the adapter after the write, so the line is proven rather than reported.
#[derive(Default, Clone)]
struct Vendor {
    fail: usize,
    error: Option<&'static str>,
    deferred: bool,
    served: u32,
    fault: Option<String>,
    given: Vec<(String, String)>,
    attempts: Vec<String>,
    launches: usize,
}

struct ErroringHarness {
    vendor: Arc<Mutex<Vendor>>,
}

impl Harness for ErroringHarness {
    fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
        let mut vendor = self.vendor.lock().unwrap();
        vendor.attempts.push(message.id.clone());
        if vendor.given.iter().any(|(id, _)| id == &message.id) {
            return Ok(DeliverOutcome::Duplicate);
        }
        vendor
            .given
            .push((message.id.clone(), message.text.clone()));
        // Each new input is consumed and finished before returning. No input
        // remains outstanding; duplicates never advance the terminal counter.
        vendor.served += 1;
        vendor.fault = if vendor.fail > 0 {
            vendor.fail -= 1;
            Some(vendor.error.unwrap_or(OVERFLOW).to_string())
        } else {
            None
        };
        Ok(if vendor.deferred {
            DeliverOutcome::Deferred
        } else {
            DeliverOutcome::Accepted {
                turn_finished: true,
            }
        })
    }
    fn proven(&self, message_id: &str) -> Option<Proof> {
        let vendor = self.vendor.lock().unwrap();
        (vendor.deferred && vendor.given.iter().any(|(id, _)| id == message_id)).then_some(
            Proof::Taken {
                turn_finished: true,
            },
        )
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
    start_vendor(
        Vendor {
            fail,
            error,
            ..Vendor::default()
        },
        clock,
    )
}

fn start_vendor(vendor: Vendor, clock: Arc<dyn Clock>) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let lead = store.create_agent(NewAgent::new("lead", "spiron")).unwrap();
    // Idle, as an agent a notice can reach and wake; it never launches here.
    let lead = store.set_agent_state(&lead.id, "idle").unwrap();
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
    let vendor = Arc::new(Mutex::new(vendor));
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
                vendor.lock().unwrap().launches += 1;
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
    assert!(
        note.contains("error 1 of 3 in a row about this session's own content"),
        "{note}"
    );
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

// This fixture proves terminal consumption using a new, nonduplicate served
// counter and Idle with no outstanding inputs. It does not infer a terminal
// result from an arbitrary provider error or authorise replay of partial work.
fn terminal_systemic_fault_keeps_backoff_and_queued_mail(error: &'static str) {
    const START: i64 = 1_700_000_000_000;
    let clock = ManualClock::new(START);
    let recorded = RecordingClock::new(clock.clone());
    let fx = start_with(1, Some(error), recorded.clone());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        fx.store
            .receive(&fx.worker.id, Incoming::new("original task"))
            .unwrap();
        assert!(wait_until(|| fx.vendor.lock().unwrap().given.len() == 1));
        // The pass that records the error runs on the short timer the worker
        // installs after the delivery: fire it once it is installed, never
        // earlier, and never past the second the backoff window allows for.
        let errored = || {
            fx.store
                .turn_errors(&fx.worker.id)
                .unwrap()
                .is_some_and(|errors| errors.last_error.as_deref() == Some(error))
        };
        assert!(wait_until(|| errored() || {
            recorded.fire_next_timer(START + 1_000);
            false
        }));
        assert!(wait_until(|| {
            recorded.waits().iter().any(|(_, deadline)| {
                deadline.is_some_and(|at| (START + 15_000..=START + 16_000).contains(&at))
            }) || fx.vendor.lock().unwrap().attempts.len() > 1
        }));
        let (original_id, attempts) = {
            let vendor = fx.vendor.lock().unwrap();
            (vendor.given[0].0.clone(), vendor.attempts.clone())
        };
        assert_eq!(attempts.as_slice(), std::slice::from_ref(&original_id));
        let deadline = recorded
            .waits()
            .iter()
            .find_map(|(_, deadline)| {
                deadline.filter(|at| (START + 15_000..=START + 16_000).contains(at))
            })
            .unwrap();
        // A mailbox wake must neither erase the terminal follow-up nor bypass its
        // deadline. Process replacement must not turn it into an open-turn replay.
        // Time stands just before the deadline when the mail arrives, so the
        // wake the mail causes re-installs the backoff at that time.
        clock.set_ms(deadline - 1);
        let observed = recorded.waits().len();
        fx.store
            .receive(&fx.worker.id, Incoming::new("mail during backoff"))
            .unwrap();
        assert!(wait_until(
            || recorded.waits()[observed..].contains(&(deadline - 1, Some(deadline)))
        ));
        {
            let vendor = fx.vendor.lock().unwrap().clone();
            assert_eq!(
                vendor.attempts.as_slice(),
                std::slice::from_ref(&original_id)
            );
            assert_eq!(vendor.launches, 1);
        }
        assert!(fx.store.has_open_turn(&fx.worker.id).unwrap());
        assert_eq!(fx.store.agent(&fx.worker.id).unwrap().state, "running");
        clock.set_ms(deadline);
        assert!(wait_until(|| fx.vendor.lock().unwrap().given.len() == 3));
        assert!(wait_until(|| !fx
            .store
            .has_open_turn(&fx.worker.id)
            .unwrap()));
        {
            let vendor = fx.vendor.lock().unwrap().clone();
            assert_eq!(
                vendor.launches, 1,
                "a proven terminal fault needs no replacement"
            );
            assert_eq!(
                vendor.attempts.len(),
                3,
                "count duplicate attempts, not just effects"
            );
            assert_eq!(
                vendor
                    .attempts
                    .iter()
                    .filter(|id| *id == &original_id)
                    .count(),
                1
            );
            assert!(
                vendor
                    .given
                    .iter()
                    .any(|(_, text)| text == "mail during backoff")
            );
            assert_eq!(
                vendor
                    .given
                    .iter()
                    .filter(|(id, text)| id != &original_id && text.contains("[Recovery]"))
                    .count(),
                1
            );
        }
        assert!(notices(&fx.store, &fx.lead).is_empty());
    }));
    support::stop(fx.supervisor);
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[test]
fn exact_unavailable_receipt_preserves_terminal_backoff_and_mail() {
    terminal_systemic_fault_keeps_backoff_and_queued_mail(UNAVAILABLE);
}

#[test]
fn upstream_stream_codes_preserve_terminal_backoff_and_mail() {
    for error in [
        "Sylphx AI failure Some(\"service_unavailable\"): The model response was interrupted. Retry the request.",
        "Sylphx AI failure Some(\"sylphx_ai_stream_no_output\"): The model response was interrupted. Retry the request.",
    ] {
        terminal_systemic_fault_keeps_backoff_and_queued_mail(error);
    }
}

#[test]
fn transient_text_cannot_discard_proven_terminal_backoff_and_mail() {
    terminal_systemic_fault_keeps_backoff_and_queued_mail("HTTP 503: service unavailable");
}

fn received(store: &Store, agent: &anyagents_core::Agent, text: &str) -> anyagents_core::Message {
    match store.receive(&agent.id, Incoming::new(text)).unwrap() {
        anyagents_core::ReceiveOutcome::New(message) => message,
        anyagents_core::ReceiveOutcome::Duplicate(_) => unreachable!(),
    }
}

/// A vendor that has taken and finished a turn before `deliver` returns
/// answers `Deferred`: the line is proven, not reported. The pass that finds
/// the turn ended commits that proof first, so the turn is held open for its
/// follow-up (an error) or closed (a completion), and the line is never
/// handed over again.
#[test]
fn an_errored_turn_proven_while_deferred_stays_open_for_its_follow_up() {
    const START: i64 = 1_700_000_000_000;
    let clock = ManualClock::new(START);
    let fx = start_vendor(
        Vendor {
            fail: 1,
            error: Some(UNAVAILABLE),
            deferred: true,
            ..Vendor::default()
        },
        Arc::new(clock.clone()) as Arc<dyn Clock>,
    );
    let task = received(&fx.store, &fx.worker, "summarise the incident");
    assert!(wait_until(|| {
        fx.store
            .turn_errors(&fx.worker.id)
            .unwrap()
            .is_some_and(|errors| errors.last_error.as_deref() == Some(UNAVAILABLE))
    }));
    assert!(
        fx.store.has_open_turn(&fx.worker.id).unwrap(),
        "the errored turn is held open for its follow-up"
    );
    assert_eq!(
        fx.store.delivered_cursor(&fx.worker.id).unwrap(),
        task.seq,
        "the proof is committed, not handed over again"
    );
    // The systemic backoff ends: the note goes out and closes the turn.
    clock.set_ms(START + 15_000);
    assert!(wait_until(|| fx.vendor.lock().unwrap().given.len() == 2));
    assert!(wait_until(|| !fx
        .store
        .has_open_turn(&fx.worker.id)
        .unwrap()));
    let vendor = fx.vendor.lock().unwrap().clone();
    assert!(
        vendor.given[1].1.contains("[Recovery]") && vendor.given[1].1.contains(task.id.as_str()),
        "{:?}",
        vendor.given
    );
    assert_eq!(
        vendor
            .attempts
            .iter()
            .filter(|id| **id == vendor.given[0].0)
            .count(),
        1,
        "the proven task is never offered again: {:?}",
        vendor.attempts
    );
    support::stop(fx.supervisor);
}

#[test]
fn a_completed_turn_proven_while_deferred_is_closed() {
    let fx = start_vendor(
        Vendor {
            deferred: true,
            ..Vendor::default()
        },
        Arc::new(SystemClock::new()),
    );
    let task = received(&fx.store, &fx.worker, "summarise the incident");
    assert!(wait_until(|| fx
        .store
        .delivered_cursor(&fx.worker.id)
        .unwrap()
        >= task.seq));
    assert!(
        wait_until(|| !fx.store.has_open_turn(&fx.worker.id).unwrap()),
        "a turn the vendor finished is not left open"
    );
    assert_eq!(fx.vendor.lock().unwrap().attempts.len(), 1);
    support::stop(fx.supervisor);
}

/// A provider that is unavailable is the fleet's trouble, not the agent's:
/// each retry waits out a growing backoff, the turn stays open (a restart
/// would recover it) while it waits, and no run of them ever parks it.
#[test]
fn systemic_errors_back_off_and_never_park() {
    const START: i64 = 1_700_000_000_000;
    let clock = ManualClock::new(START);
    let recorded = RecordingClock::new(clock.clone());
    let fx = start_with(usize::MAX, Some(UNAVAILABLE), recorded.clone());
    let given = || fx.vendor.lock().unwrap().given.len();
    let advance = |ms: i64| clock.set_ms(clock.now_ms() + ms);
    fx.store
        .receive(&fx.worker.id, Incoming::new("summarise the incident"))
        .unwrap();
    assert!(wait_until(|| given() == 1));
    // The next pass finds the errored turn and starts its backoff. It runs on
    // the short timer the worker installs after the delivery: fire it once it
    // is installed, as time moved earlier is read by the worker as its own.
    let errored = || {
        fx.store
            .turn_errors(&fx.worker.id)
            .unwrap()
            .is_some_and(|errors| errors.last_error.as_deref() == Some(UNAVAILABLE))
    };
    assert!(wait_until(|| errored() || {
        recorded.fire_next_timer(START + 1_000);
        false
    }));
    advance(10_000);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(given(), 1, "nothing is retried inside the backoff");
    assert!(
        fx.store.has_open_turn(&fx.worker.id).unwrap(),
        "the turn stays open while it waits"
    );
    // Past it, the note goes out and the turn closes with it.
    advance(5_000);
    assert!(wait_until(|| given() == 2));
    let (_, note) = fx.vendor.lock().unwrap().given[1].clone();
    assert!(note.contains("never pauses the agent"), "{note}");

    // Far past the strike limit, it still only retries.
    let mut steps = 0;
    while given() < 3 * MAX_ERRORED_TURNS as usize && steps < 300 {
        advance(700_000);
        std::thread::sleep(Duration::from_millis(30));
        steps += 1;
    }
    assert!(given() >= 3 * MAX_ERRORED_TURNS as usize, "{}", given());
    assert_ne!(fx.store.agent(&fx.worker.id).unwrap().state, "paused");
    let errors = fx.store.turn_errors(&fx.worker.id).unwrap().unwrap();
    assert_eq!(errors.streak, 0, "a systemic error is no strike");
    assert!(errors.faulted_at_ms.is_none());
    assert!(notices(&fx.store, &fx.lead).is_empty());

    // The next success clears it.
    fx.vendor.lock().unwrap().fail = 0;
    let before = given();
    let mut steps = 0;
    while given() == before && steps < 100 {
        advance(700_000);
        std::thread::sleep(Duration::from_millis(30));
        steps += 1;
    }
    assert!(wait_until(|| !fx
        .store
        .has_open_turn(&fx.worker.id)
        .unwrap()));
    support::stop(fx.supervisor);
}

#[test]
fn stale_custody_error_callback_suppresses_strikes_and_followup_side_effects() {
    #[derive(Default)]
    struct StaleState {
        admitted: bool,
        fire_error: bool,
        given: Vec<String>,
    }

    struct StaleHarness(Arc<Mutex<StaleState>>);

    impl Harness for StaleHarness {
        fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
            let mut state = self.0.lock().unwrap();
            if !state.admitted {
                state.admitted = true;
                state.given.push(message.id.clone());
                return Ok(DeliverOutcome::Accepted {
                    turn_finished: false,
                });
            }
            Ok(DeliverOutcome::Deferred)
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
            let state = self.0.lock().unwrap();
            HarnessHealth {
                process_alive: true,
                turn_state: if state.admitted && !state.fire_error {
                    TurnState::InTurn
                } else {
                    TurnState::Idle
                },
                delivery: Delivery {
                    level: DeliveryLevel::MidTurn,
                    idempotent: false,
                    proof: std::borrow::Cow::Borrowed("stale-double"),
                    peer: None,
                },
                quota_reset_at_ms: None,
                sign_in_fault: state.fire_error.then(|| OVERFLOW.to_string()),
            }
        }
        fn turns_served(&self) -> u32 {
            u32::from(self.0.lock().unwrap().fire_error)
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let worker = store
        .create_agent(NewAgent::new("worker", "spiron"))
        .unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    store
        .set_agent_running(&worker.id, &work.display().to_string())
        .unwrap();
    let shared = Arc::new(Mutex::new(StaleState::default()));
    let supervisor = Supervisor::start(
        Arc::clone(&store),
        Arc::new(SystemClock::new()),
        {
            let shared = Arc::clone(&shared);
            Arc::new(move |_: LaunchRequest| Ok(StaleHarness(Arc::clone(&shared))))
        },
        Vec::new(),
    )
    .unwrap();

    let first = received(&store, &worker, "first turn");
    assert!(wait_until(
        || store.delivered_cursor(&worker.id).unwrap() == first.seq
    ));

    // Advance durable custody to a newer message before the old turn's error arrives.
    let second = received(&store, &worker, "newer turn");
    store.note_delivered(&worker.id, second.seq).unwrap();
    shared.lock().unwrap().fire_error = true;
    store.poke(anyagents_core::Wake::Agent {
        agent_id: worker.id.clone(),
    });
    std::thread::sleep(Duration::from_millis(400));

    assert!(
        store.turn_errors(&worker.id).unwrap().is_none(),
        "stale error callback must not record strikes"
    );
    assert_eq!(store.agent_turn_snapshot(&worker.id).unwrap().turn_seq, 0);
    assert_eq!(
        store.open_turn(&worker.id).unwrap().map(|o| o.opened_seq),
        Some(second.seq)
    );
    assert_eq!(store.inbox(&worker.id, None).unwrap().len(), 2);
    support::stop(supervisor);
}
