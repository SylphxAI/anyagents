use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyagents_agent::DeliveryLoop;
use anyagents_core::{Incoming, NewAgent, Store, Wake, utc_day};
use anyagents_harness::{DeliveryMode, FakeHarness};
use anyagents_harness_claude::ClaudeHarness;
use anyagents_supervisor::{Clock, LaunchRequest, ManualClock, Supervisor};

fn short_dir() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

fn scripted(mode: &'static str, log: PathBuf) -> anyagents_supervisor::Launch<ClaudeHarness> {
    let program = env!("CARGO_BIN_EXE_scripted_claude").to_string();
    let log = log.to_string_lossy().into_owned();
    Arc::new(move |request: LaunchRequest| {
        let env = [
            ("ANYAGENTS_FAKE_MODE", mode),
            ("ANYAGENTS_FAKE_LOG", log.as_str()),
        ];
        ClaudeHarness::start_in(&program, None, &env, Some(&request.work_dir))
            .map_err(|err| err.to_string())
    })
}

/// Polls until `ready` holds. The bound is a ceiling, not a delay: a passing
/// check returns at once (about 0.07 s alone), while a loaded parallel test run
/// can take several seconds to spawn the scripted vendor processes.
const WAIT_CEILING: Duration = Duration::from_secs(30);

fn wait_until(mut ready: impl FnMut() -> bool) -> bool {
    let started = Instant::now();
    while started.elapsed() < WAIT_CEILING {
        if ready() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    ready()
}

fn argv_count(path: &Path) -> usize {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|line| line.starts_with("argv:"))
        .count()
}

fn log_contains(path: &Path, needle: &str) -> bool {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .contains(needle)
}

#[test]
fn a_message_in_another_process_wakes_the_supervisor() {
    let dir = short_dir();
    let wake = dir.path().join("wake.sock");
    assert!(
        wake.as_os_str().len() <= 107,
        "wake path is too long: {}",
        wake.display()
    );
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let clock = ManualClock::new(0);
    let supervisor = Supervisor::<ClaudeHarness>::start(
        Arc::clone(&store),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
        Arc::new(|_request: LaunchRequest| Err("not launched".into())),
        Vec::new(),
    )
    .unwrap();
    let events = store.subscribe();
    let other = Store::open(dir.path().join("state.db")).unwrap();
    let agent = other
        .create_agent(NewAgent::new("alpha", "claude-code"))
        .unwrap();
    other
        .receive(&agent.id, Incoming::new("from-the-other-process"))
        .unwrap();
    let wake = events.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(matches!(wake, Wake::Mail { .. }));
    drop(supervisor);
}

#[test]
fn one_process_restarts_after_a_kill_and_refuses_a_second() {
    let dir = short_dir();
    let log = dir.path().join("vendor.log");
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let agent = store
        .create_agent(NewAgent::new("alpha", "claude-code"))
        .unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    store
        .set_agent_running(&agent.id, &work.display().to_string())
        .unwrap();
    let clock = ManualClock::new(1_699_000_000_000);
    let supervisor = Supervisor::start(
        Arc::clone(&store),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
        scripted("hold", log.clone()),
        Vec::new(),
    )
    .unwrap();
    assert!(wait_until(|| {
        supervisor
            .live(&agent.id)
            .is_some_and(|live| live.pid.is_some() && live.process_alive && argv_count(&log) == 1)
    }));
    let pid = supervisor.live(&agent.id).unwrap().pid.unwrap();

    let locked = Store::open(dir.path().join("state.db")).unwrap();
    assert!(matches!(
        locked.try_lock_agent(&agent.id),
        Err(anyagents_core::Error::AgentBusy)
    ));
    let second = Store::open(dir.path().join("state.db")).unwrap();
    match DeliveryLoop::start(
        second,
        agent.id.clone(),
        FakeHarness::new(DeliveryMode::Followup),
    ) {
        Err(err) => assert!(err.agent_busy()),
        Ok(_) => panic!("a second loop was accepted"),
    }

    let killed = std::process::Command::new("kill")
        .args(["-KILL", &pid.to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    assert!(wait_until(|| {
        supervisor
            .live(&agent.id)
            .is_some_and(|live| !live.process_alive && live.restart_at_ms.is_some())
    }));
    let restart_at = supervisor.live(&agent.id).unwrap().restart_at_ms.unwrap();
    assert!(restart_at - clock.now_ms() >= 200);
    assert_eq!(argv_count(&log), 1);

    clock.set_ms(restart_at);
    assert!(wait_until(|| {
        supervisor.live(&agent.id).is_some_and(|live| {
            live.process_alive && live.pid.is_some_and(|next| next != pid) && argv_count(&log) >= 2
        })
    }));
    drop(supervisor);
}

#[test]
fn a_running_agent_receives_mail_without_a_manual_deliver() {
    let dir = short_dir();
    let log = dir.path().join("vendor.log");
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let agent = store
        .create_agent(NewAgent::new("alpha", "claude-code"))
        .unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    store
        .set_agent_running(&agent.id, &work.display().to_string())
        .unwrap();
    let clock = ManualClock::new(1_699_000_000_000);
    let supervisor = Supervisor::start(
        Arc::clone(&store),
        Arc::new(clock) as Arc<dyn Clock>,
        scripted("echo", log.clone()),
        Vec::new(),
    )
    .unwrap();
    assert!(wait_until(|| {
        supervisor
            .live(&agent.id)
            .is_some_and(|live| live.pid.is_some() && live.process_alive)
    }));
    let other = Store::open(dir.path().join("state.db")).unwrap();
    other
        .receive(&agent.id, Incoming::new("ping-body"))
        .unwrap();
    assert!(
        wait_until(|| log_contains(&log, "ping-body")),
        "{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    drop(supervisor);
}

#[test]
fn quota_pauses_until_the_reset_time() {
    let dir = short_dir();
    let log = dir.path().join("vendor.log");
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let agent = store
        .create_agent(NewAgent::new("alpha", "claude-code"))
        .unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    store
        .set_agent_running(&agent.id, &work.display().to_string())
        .unwrap();
    let clock = ManualClock::new(1_699_000_000_000);
    let supervisor = Supervisor::start(
        Arc::clone(&store),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
        scripted("quota", log.clone()),
        Vec::new(),
    )
    .unwrap();
    assert!(wait_until(|| {
        supervisor
            .live(&agent.id)
            .is_some_and(|live| live.quota_reset_at_ms == Some(1_700_000_000_000))
    }));
    let other = Store::open(dir.path().join("state.db")).unwrap();
    other
        .receive(&agent.id, Incoming::new("quota-body"))
        .unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert!(
        !log_contains(&log, "quota-body"),
        "{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    clock.set_ms(1_700_000_000_000);
    assert!(
        wait_until(|| log_contains(&log, "quota-body")),
        "{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    drop(supervisor);
}

#[test]
fn finished_turns_sum_by_agent_and_day() {
    let dir = short_dir();
    let log = dir.path().join("vendor.log");
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let mut alpha_new = NewAgent::new("alpha", "claude-code");
    alpha_new.model = Some("sonnet".to_string());
    let alpha = store.create_agent(alpha_new).unwrap();
    let mut beta_new = NewAgent::new("beta", "claude-code");
    beta_new.model = Some("sonnet".to_string());
    let beta = store.create_agent(beta_new).unwrap();
    for agent in [&alpha, &beta] {
        let work = dir.path().join(&agent.name);
        std::fs::create_dir_all(&work).unwrap();
        store
            .set_agent_running(&agent.id, &work.display().to_string())
            .unwrap();
    }
    let started = 1_699_000_000_000;
    let later = 1_700_000_000_000 + 5 * 86_400_000;
    let clock = ManualClock::new(started);
    let supervisor = Supervisor::start(
        Arc::clone(&store),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
        scripted("finish", log.clone()),
        Vec::new(),
    )
    .unwrap();
    assert!(wait_until(|| {
        supervisor
            .live(&alpha.id)
            .is_some_and(|live| live.process_alive)
            && supervisor
                .live(&beta.id)
                .is_some_and(|live| live.process_alive)
    }));
    let other = Store::open(dir.path().join("state.db")).unwrap();
    other.receive(&alpha.id, Incoming::new("one")).unwrap();
    other.receive(&beta.id, Incoming::new("two")).unwrap();
    // The mock clock only moves when this test moves it. The supervisor skips a
    // quota wait when the clock has reached the reset time and reads the
    // harness's usage when the mailbox has drained, so each step below advances
    // it past that reset and then past the idle review.
    let due = 1_700_000_000_000;
    let mut stepped = 0;
    while store.list_usage(None).unwrap().len() < 2 {
        assert!(stepped < 40, "{:?}", store.list_usage(None).unwrap());
        stepped += 1;
        clock.set_ms(due + i64::from(stepped) * 1_000);
        std::thread::sleep(Duration::from_millis(50));
    }
    // The first turns ran after the quota reset the scripted child announced,
    // on the day the clock reached it.
    let first_day = utc_day(due + i64::from(stepped) * 1_000);
    for record in store.list_usage(None).unwrap() {
        assert_eq!(record.input_tokens, 11);
        assert_eq!(record.output_tokens, 7);
        assert_eq!(record.cache_read_tokens, 3);
        assert_eq!(record.cache_write_tokens, 2);
        assert_eq!(record.reported_cost_micros, Some(500_000));
        assert_eq!(
            record.billing_mode,
            anyagents_core::BillingMode::Subscription
        );
        assert_eq!(record.model.as_deref(), Some("sonnet"));
        assert_eq!(record.day, first_day);
    }
    clock.set_ms(later);
    other.receive(&alpha.id, Incoming::new("three")).unwrap();
    assert!(wait_until(
        || other.delivered_cursor(&alpha.id).unwrap() >= 2
    ));
    let mut stepped = 0;
    while store.list_usage(None).unwrap().len() < 3 {
        assert!(stepped < 40, "{:?}", store.list_usage(None).unwrap());
        stepped += 1;
        clock.set_ms(later + i64::from(stepped) * 1_000);
        std::thread::sleep(Duration::from_millis(50));
    }
    let rows = store.list_usage(None).unwrap();
    let alpha_input: i64 = rows
        .iter()
        .filter(|row| row.agent_id == alpha.id)
        .map(|row| row.input_tokens)
        .sum();
    let beta_input: i64 = rows
        .iter()
        .filter(|row| row.agent_id == beta.id)
        .map(|row| row.input_tokens)
        .sum();
    // The first two steps are charged once each. The third is read by a
    // restarted process, which can read it before the child has printed its
    // result: that run is then not charged, and a later step never is.
    assert!((11..=22).contains(&alpha_input), "{alpha_input}");
    assert_eq!(beta_input, 11);
    assert!(rows.iter().any(|row| row.day == utc_day(later)));
    assert_ne!(utc_day(later), first_day);
    drop(supervisor);
}

/// Three lines arrive while the child holds its turn open. Tool-boundary hook
/// responses carry each line once, and the cursor ends on the last line.
#[test]
fn three_lines_sent_while_a_turn_is_open_are_all_taken_and_none_twice() {
    let dir = short_dir();
    let log = dir.path().join("vendor.log");
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let agent = store
        .create_agent(NewAgent::new("alpha", "claude-code"))
        .unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    store
        .set_agent_running(&agent.id, &work.display().to_string())
        .unwrap();
    let clock = ManualClock::new(1_699_000_000_000);
    let supervisor = Supervisor::start(
        Arc::clone(&store),
        Arc::new(clock) as Arc<dyn Clock>,
        scripted("steer", log.clone()),
        Vec::new(),
    )
    .unwrap();
    assert!(wait_until(|| {
        supervisor
            .live(&agent.id)
            .is_some_and(|live| live.pid.is_some() && live.process_alive)
    }));
    let other = Store::open(dir.path().join("state.db")).unwrap();
    for text in ["one", "two", "three"] {
        other.receive(&agent.id, Incoming::new(text)).unwrap();
    }
    assert!(
        wait_until(|| other.delivered_cursor(&agent.id).unwrap() >= 3),
        "{:?}",
        other.inbox(&agent.id, None)
    );
    let text = std::fs::read_to_string(&log).unwrap_or_default();
    for seq in 1..=3 {
        let id = other
            .inbox(&agent.id, None)
            .unwrap()
            .into_iter()
            .find(|message| message.seq == seq)
            .unwrap()
            .id;
        assert_eq!(
            text.matches(&format!("anyagents-message:{id}")).count(),
            1,
            "{id} was written once: {text}"
        );
    }
    assert_eq!(other.delivered_cursor(&agent.id).unwrap(), 3);
    assert!(
        other
            .inbox(&agent.id, None)
            .unwrap()
            .iter()
            .all(|message| message.ack == anyagents_core::Ack::Delivered)
    );
    drop(supervisor);
}

/// A line already taken is not written again after the server restarts, and a
/// line taken just before the restart is written exactly once more.
#[test]
fn a_restart_mid_stream_loses_nothing_and_writes_nothing_twice() {
    let dir = short_dir();
    let log = dir.path().join("vendor.log");
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let agent = store
        .create_agent(NewAgent::new("alpha", "claude-code"))
        .unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    store
        .set_agent_running(&agent.id, &work.display().to_string())
        .unwrap();
    let clock = ManualClock::new(1_699_000_000_000);
    let supervisor = Supervisor::start(
        Arc::clone(&store),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
        scripted("steer", log.clone()),
        Vec::new(),
    )
    .unwrap();
    assert!(wait_until(|| {
        supervisor
            .live(&agent.id)
            .is_some_and(|live| live.pid.is_some() && live.process_alive)
    }));
    let other = Store::open(dir.path().join("state.db")).unwrap();
    other.receive(&agent.id, Incoming::new("one")).unwrap();
    assert!(wait_until(
        || other.delivered_cursor(&agent.id).unwrap() >= 1
    ));
    // The process dies with the turn still open. The supervisor starts a new
    // one after its backoff, and the next two lines go to it in order.
    let pid = supervisor.live(&agent.id).unwrap().pid.unwrap();
    let killed = std::process::Command::new("kill")
        .args(["-KILL", &pid.to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    assert!(wait_until(|| {
        supervisor
            .live(&agent.id)
            .is_some_and(|live| !live.process_alive && live.restart_at_ms.is_some())
    }));
    other.receive(&agent.id, Incoming::new("two")).unwrap();
    other.receive(&agent.id, Incoming::new("three")).unwrap();
    clock.set_ms(supervisor.live(&agent.id).unwrap().restart_at_ms.unwrap());
    assert!(
        wait_until(|| other.delivered_cursor(&agent.id).unwrap() >= 3),
        "{:?}",
        other.inbox(&agent.id, None)
    );
    let inbox = other.inbox(&agent.id, None).unwrap();
    let text = std::fs::read_to_string(&log).unwrap_or_default();
    for message in &inbox {
        assert_eq!(
            text.matches(&format!("anyagents-message:{}", message.id))
                .count(),
            1,
            "{} was written once across the restart: {text}",
            message.id
        );
        assert_eq!(message.ack, anyagents_core::Ack::Delivered);
    }
    drop(supervisor);
}

#[test]
fn backpressure_parks_until_the_input_activity_notification() {
    use anyagents_harness::{
        DeliverOutcome, Harness, HarnessError, HarnessHealth, OutboundMessage,
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    };

    type Notify = Arc<dyn Fn() + Send + Sync>;
    struct Input {
        ready: Arc<AtomicBool>,
        calls: Arc<AtomicUsize>,
        notify: Arc<Mutex<Option<Notify>>>,
        inner: FakeHarness,
    }
    impl Harness for Input {
        fn deliver(&mut self, message: &OutboundMessage) -> Result<DeliverOutcome, HarnessError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.input_ready() {
                return Ok(DeliverOutcome::Deferred);
            }
            self.inner.deliver(message)
        }
        fn input_ready(&self) -> bool {
            self.ready.load(Ordering::SeqCst)
        }
        fn watch_activity(&self, notify: Notify) {
            *self.notify.lock().unwrap() = Some(notify);
        }
        fn background(&mut self) -> Result<(), HarnessError> {
            self.inner.background()
        }
        fn resume(&mut self, session: &str) -> Result<(), HarnessError> {
            self.inner.resume(session)
        }
        fn cancel(&mut self) -> Result<(), HarnessError> {
            self.inner.cancel()
        }
        fn health(&self) -> HarnessHealth {
            self.inner.health()
        }
    }
    struct ParkClock {
        inner: ManualClock,
        calls: Arc<AtomicUsize>,
        parked: mpsc::Sender<usize>,
    }
    impl Clock for ParkClock {
        fn now_ms(&self) -> i64 {
            self.inner.now_ms()
        }
        fn wait(&self, deadline: Option<i64>, woken: &AtomicBool) {
            let _ = self.parked.send(self.calls.load(Ordering::SeqCst));
            self.inner.wait(deadline, woken);
        }
        fn poke(&self) {
            self.inner.poke();
        }
    }
    let dir = short_dir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let agent = store.create_agent(NewAgent::new("blocked", "agy")).unwrap();
    store
        .set_agent_running(&agent.id, &dir.path().display().to_string())
        .unwrap();
    store.receive(&agent.id, Incoming::new("pending")).unwrap();
    let ready = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let notify: Arc<Mutex<Option<Notify>>> = Arc::new(Mutex::new(None));
    let (tx, rx) = mpsc::channel();
    let clock = Arc::new(ParkClock {
        inner: ManualClock::new(0),
        calls: Arc::clone(&calls),
        parked: tx,
    });
    let launch = {
        let ready = Arc::clone(&ready);
        let calls = Arc::clone(&calls);
        let notify = Arc::clone(&notify);
        Arc::new(move |_: LaunchRequest| {
            Ok(Input {
                ready: Arc::clone(&ready),
                calls: Arc::clone(&calls),
                notify: Arc::clone(&notify),
                inner: FakeHarness::new(DeliveryMode::Followup),
            })
        })
    };
    let supervisor = Supervisor::start(Arc::clone(&store), clock, launch, vec![]).unwrap();
    loop {
        if rx
            .recv_timeout(Duration::from_secs(3))
            .expect("backpressure must reach clock wait")
            > 0
        {
            break;
        }
    }
    assert_eq!(store.delivered_cursor(&agent.id).unwrap(), 0);
    ready.store(true, Ordering::SeqCst);
    notify.lock().unwrap().as_ref().unwrap()();
    assert!(wait_until(
        || store.delivered_cursor(&agent.id).unwrap() == 1
    ));
    drop(supervisor);
}

#[test]
fn an_interrupted_turn_with_no_new_messages_receives_a_recovery_note_on_restart() {
    let dir = short_dir();
    let log = dir.path().join("vendor.log");
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let agent = store
        .create_agent(NewAgent::new("alpha", "claude-code"))
        .unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    store
        .set_agent_running(&agent.id, &work.display().to_string())
        .unwrap();
    let clock = ManualClock::new(1_699_000_000_000);
    let supervisor = Supervisor::start(
        Arc::clone(&store),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
        scripted("steer", log.clone()),
        Vec::new(),
    )
    .unwrap();
    assert!(wait_until(|| {
        supervisor
            .live(&agent.id)
            .is_some_and(|live| live.pid.is_some() && live.process_alive)
    }));
    let other = Store::open(dir.path().join("state.db")).unwrap();
    other
        .receive(&agent.id, Incoming::new("deploy-spiron-task"))
        .unwrap();
    assert!(wait_until(
        || other.delivered_cursor(&agent.id).unwrap() >= 1
    ));
    // Simulate a partial outbound reply sent before the process crashed mid-turn.
    other
        .record_outbound(
            &agent.id,
            "telegram:chat:-5460290765",
            None,
            "part 1 report already delivered",
        )
        .unwrap();
    assert!(other.open_turn(&agent.id).unwrap().is_some());

    let pid = supervisor.live(&agent.id).unwrap().pid.unwrap();
    let killed = std::process::Command::new("kill")
        .args(["-KILL", &pid.to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    assert!(wait_until(|| {
        supervisor
            .live(&agent.id)
            .is_some_and(|live| !live.process_alive && live.restart_at_ms.is_some())
    }));
    // Advance clock to trigger process replacement with NO new messages in the mailbox.
    clock.set_ms(supervisor.live(&agent.id).unwrap().restart_at_ms.unwrap());
    assert!(
        wait_until(|| {
            log_contains(
                &log,
                "[Recovery] Your previous turn was interrupted: the process running it stopped",
            )
        }),
        "expected recovery note in vendor log:\n{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    let text = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(text.contains("deploy-spiron-task"), "{text}");
    assert!(
        text.contains("completed (do not resend): part 1 report already delivered"),
        "{text}"
    );
    drop(supervisor);
}

#[test]
fn a_finished_turn_does_not_receive_a_recovery_note_on_restart() {
    let dir = short_dir();
    let log = dir.path().join("vendor.log");
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let mut alpha_new = NewAgent::new("alpha", "claude-code");
    alpha_new.model = Some("sonnet".to_string());
    let agent = store.create_agent(alpha_new).unwrap();
    let work = dir.path().join("work");
    std::fs::create_dir_all(&work).unwrap();
    store
        .set_agent_running(&agent.id, &work.display().to_string())
        .unwrap();
    let clock = ManualClock::new(1_699_000_000_000);
    let supervisor = Supervisor::start(
        Arc::clone(&store),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
        scripted("finish", log.clone()),
        Vec::new(),
    )
    .unwrap();
    assert!(wait_until(|| {
        supervisor
            .live(&agent.id)
            .is_some_and(|live| live.pid.is_some() && live.process_alive)
    }));
    let other = Store::open(dir.path().join("state.db")).unwrap();
    other
        .receive(&agent.id, Incoming::new("complete-me"))
        .unwrap();
    let due = 1_700_000_000_000;
    let mut stepped = 0;
    while store.list_usage(None).unwrap().is_empty() {
        assert!(stepped < 40);
        stepped += 1;
        clock.set_ms(due + i64::from(stepped) * 1_000);
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(
        other.open_turn(&agent.id).unwrap().is_none(),
        "completed turn must clear open_turn"
    );
    let pid = supervisor.live(&agent.id).unwrap().pid.unwrap();
    let killed = std::process::Command::new("kill")
        .args(["-KILL", &pid.to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    assert!(wait_until(|| {
        supervisor
            .live(&agent.id)
            .is_some_and(|live| !live.process_alive && live.restart_at_ms.is_some())
    }));
    let restart_at = supervisor.live(&agent.id).unwrap().restart_at_ms.unwrap();
    clock.set_ms(restart_at);
    assert!(wait_until(|| {
        supervisor
            .live(&agent.id)
            .is_some_and(|live| live.process_alive && live.pid != Some(pid))
    }));
    std::thread::sleep(Duration::from_millis(150));
    assert!(
        !log_contains(&log, "[Recovery]"),
        "finished turn must not receive recovery note:\n{}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    drop(supervisor);
}

#[test]
fn a_finished_subagent_automatically_scales_to_zero_and_wakes_on_follow_up_mail() {
    let dir = short_dir();
    let log = dir.path().join("vendor.log");
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let root = store
        .create_agent(NewAgent::new("alpha", "claude-code"))
        .unwrap();
    let mut sub_new = NewAgent::new("alpha-worker", "claude-code");
    sub_new.model = Some("sonnet".to_string());
    sub_new.parent_id = Some(root.id.clone());
    let sub = store.create_agent(sub_new).unwrap();
    let work = dir.path().join("work-sub");
    std::fs::create_dir_all(&work).unwrap();
    store
        .set_agent_running(&sub.id, &work.display().to_string())
        .unwrap();
    let clock = ManualClock::new(1_699_000_000_000);
    let supervisor = Supervisor::start(
        Arc::clone(&store),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
        scripted("finish", log.clone()),
        Vec::new(),
    )
    .unwrap();
    assert!(wait_until(|| {
        supervisor
            .live(&sub.id)
            .is_some_and(|live| live.pid.is_some() && live.process_alive)
    }));

    let other = Store::open(dir.path().join("state.db")).unwrap();
    other
        .receive(&sub.id, Incoming::new("subagent-task-1"))
        .unwrap();
    let due = 1_700_000_000_000;
    let mut stepped = 0;
    while !log_contains(&log, "subagent-task-1") {
        assert!(stepped < 40, "sub-agent did not process task 1");
        stepped += 1;
        clock.set_ms(due + i64::from(stepped) * 1_000);
        std::thread::sleep(Duration::from_millis(50));
    }
    // Completing the turn automatically scales the sub-agent to `idle` and releases its process.
    assert!(wait_until(|| {
        other.agent(&sub.id).unwrap().state == "idle"
            && supervisor
                .live(&sub.id)
                .is_some_and(|live| !live.process_alive && live.pid.is_none())
    }));

    // Sending a follow-up message wakes the `idle` sub-agent back up automatically.
    other
        .receive(&sub.id, Incoming::new("subagent-task-2"))
        .unwrap();
    let mut stepped2 = 0;
    while !log_contains(&log, "subagent-task-2") {
        assert!(stepped2 < 40, "sub-agent did not wake and process task 2");
        stepped2 += 1;
        clock.set_ms(due + 100_000 + i64::from(stepped2) * 1_000);
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(wait_until(|| {
        other.agent(&sub.id).unwrap().state == "idle"
    }));
    drop(supervisor);
}

#[test]
fn a_model_or_harness_change_automatically_switches_running_process_at_next_idle_point() {
    let dir = short_dir();
    let log = dir.path().join("vendor.log");
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let mut new_agent = NewAgent::new("alpha", "claude-code");
    new_agent.model = Some("sonnet".to_string());
    let agent = store.create_agent(new_agent).unwrap();
    let work = dir.path().join("work-switch");
    std::fs::create_dir_all(&work).unwrap();
    store
        .set_agent_running(&agent.id, &work.display().to_string())
        .unwrap();
    let clock = ManualClock::new(1_699_000_000_000);
    let supervisor = Supervisor::start(
        Arc::clone(&store),
        Arc::new(clock) as Arc<dyn Clock>,
        scripted("idle", log),
        Vec::new(),
    )
    .unwrap();
    assert!(wait_until(|| {
        supervisor.live(&agent.id).is_some_and(|live| {
            live.pid.is_some()
                && live.process_alive
                && live.live_harness.as_deref() == Some("claude-code")
                && live.live_model.as_deref() == Some("sonnet")
        })
    }));
    let first_pid = supervisor.live(&agent.id).unwrap().pid.unwrap();

    // Update the agent's model in the store while it is idle: the supervisor
    // automatically replaces the running process and records the new live model.
    store.set_agent_model(&agent.id, "opus").unwrap();
    assert!(wait_until(|| {
        supervisor.live(&agent.id).is_some_and(|live| {
            live.process_alive
                && live.pid.is_some()
                && live.pid != Some(first_pid)
                && live.live_model.as_deref() == Some("opus")
        })
    }));
    drop(supervisor);
}
