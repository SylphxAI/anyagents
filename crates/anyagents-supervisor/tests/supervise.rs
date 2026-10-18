use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyagents_agent::DeliveryLoop;
use anyagents_core::{Incoming, NewAgent, Store, Wake, utc_day};
use anyagents_harness::{DeliveryMode, FakeHarness};
use anyagents_harness_claude::ClaudeHarness;
use anyagents_supervisor::{Clock, LaunchRequest, ManualClock, Supervisor};

fn short_dir() -> tempfile::TempDir {
    tempfile::TempDir::new_in("/tmp").unwrap()
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
    let clock = ManualClock::new(1_600_000_000_000);
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
    let clock = ManualClock::new(1_600_000_000_000);
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
        scripted("finish", log),
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
    assert!(wait_until(|| store.list_usage(None).unwrap().len() == 2));
    let first_day = utc_day(started);
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
    assert!(wait_until(|| store.list_usage(None).unwrap().len() == 3));
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
    assert_eq!(alpha_input, 22);
    assert_eq!(beta_input, 11);
    assert!(rows.iter().any(|row| row.day == utc_day(later)));
    assert_ne!(utc_day(later), first_day);
    drop(supervisor);
}
