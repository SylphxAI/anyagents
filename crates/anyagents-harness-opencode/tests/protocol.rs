//! The OpenCode adapter against a scripted `opencode`.
//!
//! The scripted program speaks the CLI's real surface: `serve` holds a service,
//! and `api <METHOD> <path>` prints one JSON answer. Nothing here talks to the
//! machine's own OpenCode.

use std::path::Path;
use std::time::Duration;

use anyagents_harness::{
    Context, DeliverOutcome, DeliveryLevel, Harness, HarnessError, OutboundMessage, Proof,
    TurnState,
};
use anyagents_harness_opencode::OpenCodeHarness;
use tempfile::TempDir;

fn msg(id: &str) -> OutboundMessage {
    OutboundMessage {
        id: id.to_string(),
        seq: 1,
        text: "hello".to_string(),
        sender: Some("agent:owner".to_string()),
        reply_to: None,
        source_time_ms: None,
        context: Context::default(),
    }
}

/// A scripted `opencode` built with these tests.
fn scripted_opencode() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_fake_opencode"))
}

fn start(dir: &TempDir, mode: &str) -> OpenCodeHarness {
    let log = dir.path().join("log");
    OpenCodeHarness::start(
        scripted_opencode(),
        Some("ses_fake"),
        &[
            ("ANYAGENTS_FAKE_MODE", mode),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("ANYAGENTS_FAKE_STATE", dir.path().to_str().unwrap()),
        ],
    )
    .unwrap_or_else(|error| panic!("{error}: {}", log_of(dir)))
}

/// The same scripted CLI, with the agent's config directory for its id.
fn start_with_config(dir: &TempDir, mode: &str, config: &Path) -> OpenCodeHarness {
    let log = dir.path().join("log");
    OpenCodeHarness::start_in_config(
        scripted_opencode(),
        Some("ses_fake"),
        &[
            ("ANYAGENTS_FAKE_MODE", mode),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("ANYAGENTS_FAKE_STATE", dir.path().to_str().unwrap()),
        ],
        None,
        config,
    )
    .unwrap_or_else(|error| panic!("{error}: {}", log_of(dir)))
}

/// Offer `message` until the service has answered its prompt call: the call
/// runs on a thread of its own, and the line is in flight until it answers.
fn settle(
    harness: &mut OpenCodeHarness,
    message: &OutboundMessage,
) -> Result<DeliverOutcome, HarnessError> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        match harness.deliver(message) {
            Ok(DeliverOutcome::InFlight) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the prompt call never answered"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            outcome => return outcome,
        }
    }
}

/// Wait until `done` holds for the harness.
fn eventually(harness: &OpenCodeHarness, what: &str, done: impl Fn(&OpenCodeHarness) -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !done(harness) {
        assert!(std::time::Instant::now() < deadline, "never: {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// What a line the prompt API took is reported as.
fn accepted() -> DeliverOutcome {
    DeliverOutcome::Accepted {
        turn_finished: false,
    }
}

fn log_of(dir: &TempDir) -> String {
    std::fs::read_to_string(dir.path().join("log")).unwrap_or_default()
}

#[test]
fn a_delivered_line_is_a_prompt_carrying_its_mailbox_id() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish");
    assert_eq!(harness.session_id().as_deref(), Some("ses_fake"));
    settle(&mut harness, &msg("m1")).unwrap();
    let text = log_of(&dir);
    // The prompt is the real endpoint, and the line carries its id first.
    assert!(
        text.contains("api:POST /api/session/ses_fake/prompt"),
        "{text}"
    );
    assert!(text.contains("anyagents-message:m1"), "{text}");
    // Every line is a steer: the session's inbox decides whether it joins the
    // running turn or starts the next one.
    assert!(text.contains("\"delivery\":\"steer\""), "{text}");
    assert!(!text.contains("\"delivery\":\"queue\""), "{text}");
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
}

#[test]
fn a_finished_turn_reports_its_answer_from_the_session() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish");
    settle(&mut harness, &msg("m1")).unwrap();
    assert!(harness.wait_answered("m1", Duration::from_secs(10)));
    assert_eq!(harness.answer("m1").as_deref(), Some("pong from opencode"));
}

#[test]
fn an_open_turn_never_blocks_the_next_line() {
    // The service holds its turn open forever. The second line is written
    // into that turn at once, not deferred behind it.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold");
    assert_eq!(settle(&mut harness, &msg("m1")).unwrap(), accepted());
    assert_eq!(settle(&mut harness, &msg("m2")).unwrap(), accepted());
    assert_eq!(harness.health().turn_state, TurnState::InTurn);
    assert_eq!(harness.health().delivery.level, DeliveryLevel::MidTurn);
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string()]
    );
    let text = log_of(&dir);
    assert_eq!(
        text.matches("api:POST /api/session/ses_fake/prompt")
            .count(),
        2,
        "{text}"
    );
}

#[test]
fn three_lines_sent_while_a_turn_is_open_are_all_taken_at_once_and_never_twice() {
    // The session's turn is open for the whole test. Each mailbox line must go
    // to the session as a prompt with `delivery: steer`, be accepted when the
    // prompt API answers, and be written exactly once even when handed over
    // again.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "inbox");
    for id in ["m1", "m2", "m3"] {
        assert_eq!(settle(&mut harness, &msg(id)).unwrap(), accepted(), "{id}");
    }
    assert!(harness.wait_seen("m3", Duration::from_secs(5)));
    // A replay of every line writes nothing more.
    for id in ["m1", "m2", "m3"] {
        assert_eq!(settle(&mut harness, &msg(id)).unwrap(), accepted(), "{id}");
    }
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string(), "m3".to_string()]
    );
    let text = log_of(&dir);
    for id in ["m1", "m2", "m3"] {
        let written = text
            .matches("api:POST /api/session/ses_fake/prompt")
            .count();
        assert_eq!(written, 3, "one prompt per line, none twice: {text}");
        assert_eq!(
            text.matches(&format!("accepted:anyagents-message:{id}"))
                .count(),
            1,
            "{id} reached the session exactly once: {text}"
        );
    }
    let prompts = std::fs::read_to_string(dir.path().join("prompts")).unwrap();
    assert_eq!(prompts.lines().count(), 3, "{prompts}");
    assert!(
        text.lines()
            .filter(|line| line.starts_with("api:POST") && line.contains("/prompt"))
            .all(|line| line.contains("\"delivery\":\"steer\"")),
        "every prompt is a steer: {text}"
    );
}

#[test]
fn a_delivered_line_is_taken_even_when_the_session_reports_a_failed_turn() {
    // Delivery is the prompt API taking the line, and nothing after it: a turn
    // the session later reports failed does not hold the next line back.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "failed");
    assert_eq!(settle(&mut harness, &msg("m1")).unwrap(), accepted());
    assert_eq!(settle(&mut harness, &msg("m2")).unwrap(), accepted());
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string()]
    );
}

#[test]
fn a_missing_session_is_created_again() {
    let dir = TempDir::new().unwrap();
    let harness = start(&dir, "finish");
    assert!(harness.session_id().is_some());
    let text = log_of(&dir);
    assert!(text.contains("api:GET /api/session/ses_fake"), "{text}");
}

#[test]
fn the_duplicate_delivery_is_not_written_twice() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish");
    settle(&mut harness, &msg("m1")).unwrap();
    assert!(harness.wait_answered("m1", Duration::from_secs(10)));
    let before = log_of(&dir).matches("anyagents-message:m1").count();
    settle(&mut harness, &msg("m1")).unwrap();
    let after = log_of(&dir).matches("anyagents-message:m1").count();
    assert_eq!(before, after);
    let _ = Path::new("");
}

#[test]
fn an_earlier_turns_answer_does_not_close_this_turn() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "history");
    settle(&mut harness, &msg("m1")).unwrap();
    // The session holds an old completed turn; this one has not finished.
    assert!(!harness.wait_answered("m1", Duration::from_secs(2)));
    assert_eq!(
        harness.answer("m1"),
        None,
        "an old answer is not this turn's"
    );
}

#[test]
fn only_the_newest_answer_is_the_turns_answer() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "two-turns");
    settle(&mut harness, &msg("m1")).unwrap();
    assert!(harness.wait_answered("m1", Duration::from_secs(5)));
    assert_eq!(harness.answer("m1").as_deref(), Some("second answer"));
}

#[test]
fn an_answer_from_before_the_turn_is_never_returned() {
    // The session holds one old answer and nothing new: the turn has produced
    // nothing yet, so there is no answer to record. This is the case that made
    // every reply repeat the first line's answer.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "history");
    settle(&mut harness, &msg("m1")).unwrap();
    assert!(
        harness.answer("m1").is_none(),
        "an old answer is not this turn's"
    );
}

#[test]
fn the_stored_session_id_is_kept_for_the_next_start() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    std::fs::create_dir_all(&config).unwrap();
    let harness = start_with_config(&dir, "finish", &config);
    assert_eq!(harness.session_id().as_deref(), Some("ses_fake"));
    let stored = std::fs::read_to_string(config.join("opencode-session-id")).unwrap();
    assert_eq!(stored.trim(), "ses_fake", "the id is kept for a restart");
}

#[test]
fn a_stored_session_that_is_gone_starts_a_new_one() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    std::fs::create_dir_all(&config).unwrap();
    // The store no longer holds the session this agent last ran.
    let harness = start_with_config(&dir, "gone", &config);
    let id = harness.session_id().expect("a session");
    assert_ne!(id, "ses_fake", "a new session was created");
    let stored = std::fs::read_to_string(config.join("opencode-session-id")).unwrap();
    assert_eq!(stored.trim(), id);
}

#[test]
fn the_models_thinking_is_never_the_answer() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "reasoning");
    settle(&mut harness, &msg("m1")).unwrap();
    assert!(harness.wait_answered("m1", Duration::from_secs(5)));
    assert_eq!(harness.answer("m1").as_deref(), Some("the real answer"));
}

#[test]
fn each_message_is_answered_by_the_message_that_follows_it() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish");
    settle(&mut harness, &msg("m1")).unwrap();
    assert!(harness.wait_answered("m1", Duration::from_secs(5)));
    assert_eq!(harness.answer("m1").as_deref(), Some("pong from opencode"));
    // The session now holds m1's prompt and answer. A second message is
    // answered by its own turn, not by m1's.
    settle(&mut harness, &msg("m2")).unwrap();
    assert!(harness.wait_answered("m2", Duration::from_secs(5)));
    assert_eq!(harness.answer("m2").as_deref(), Some("pong from opencode"));
}

#[test]
fn a_retried_line_does_not_hold_the_turn_open() {
    // `finish` closes the turn. Handing the same line over again must not
    // reopen it, or the next line would defer forever.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish");
    settle(&mut harness, &msg("m1")).unwrap();
    assert!(harness.wait_answered("m1", Duration::from_secs(5)));
    settle(&mut harness, &msg("m1")).unwrap();
    // A new line is delivered, not deferred behind a re-opened turn.
    assert_eq!(
        settle(&mut harness, &msg("m2")).unwrap(),
        DeliverOutcome::Accepted {
            turn_finished: false
        }
    );
}

fn port_of(config: &Path) -> u16 {
    std::fs::read_to_string(config.join("opencode-service-port"))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

#[test]
fn concurrent_agents_and_restart_bind_distinct_private_ports() {
    let alpha = TempDir::new().unwrap();
    let beta = TempDir::new().unwrap();
    let a_config = alpha.path().join("config");
    let b_config = beta.path().join("config");
    let (a, b) = std::thread::scope(|scope| {
        let a = scope.spawn(|| start_with_config(&alpha, "finish", &a_config));
        let b = scope.spawn(|| start_with_config(&beta, "finish", &b_config));
        (a.join().unwrap(), b.join().unwrap())
    });
    let a_port = port_of(&a_config);
    let b_port = port_of(&b_config);
    assert_ne!(a_port, b_port);
    assert!((20_000..30_000).contains(&a_port));
    for port in [a_port, b_port] {
        assert!(std::net::TcpListener::bind(("127.0.0.1", port)).is_err());
    }
    drop(a);
    let restarted = start_with_config(&alpha, "finish", &a_config);
    let restarted_port = port_of(&a_config);
    assert_ne!(restarted_port, b_port);
    assert!(std::net::TcpListener::bind(("127.0.0.1", restarted_port)).is_err());
    assert!(log_of(&alpha).contains(&format!("server:http://127.0.0.1:{restarted_port}")));
    drop(restarted);
    drop(b);
}

#[test]
fn an_occupied_stored_port_is_replaced_and_persisted() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    // Claim a valid candidate before storing it, and keep ownership throughout.
    // A service shutdown followed by bind(previous) has an unowned socket gap.
    let occupied = (20_000..30_000)
        .find_map(|port| std::net::TcpListener::bind(("127.0.0.1", port)).ok())
        .expect("an available private fixture port");
    let previous = occupied.local_addr().unwrap().port();
    std::fs::create_dir(&config).unwrap();
    std::fs::write(config.join("opencode-service-port"), previous.to_string()).unwrap();
    let harness = start_with_config(&dir, "finish", &config);
    let replacement = port_of(&config);
    assert_ne!(replacement, previous);
    assert!(log_of(&dir).contains(&format!("server:http://127.0.0.1:{replacement}")));
    // The replacement is held by this live service and recorded in config.
    // Once released, another process may claim it; deterministic free-port
    // reuse and occupied-next-port cases live at the allocator boundary.
    assert!(std::net::TcpListener::bind(("127.0.0.1", replacement)).is_err());
    drop(harness);
    drop(occupied);
}

#[test]
fn startup_without_an_endpoint_never_calls_api_or_discovers_a_managed_service() {
    let dir = TempDir::new().unwrap();
    let log = dir.path().join("log");
    let result = OpenCodeHarness::start_in_config(
        scripted_opencode(),
        None,
        &[
            ("ANYAGENTS_FAKE_MODE", "no-announcement"),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("OPENCODE_PASSWORD", "inherited-test-value"),
            ("OPENCODE_SERVER_PASSWORD", "inherited-test-value"),
        ],
        None,
        &dir.path().join("config"),
    );
    let error = result.err().expect("startup must fail").to_string();
    assert!(
        error.contains("did not announce its private endpoint"),
        "{error}"
    );
    assert!(!log_of(&dir).contains("api:"));
}

#[test]
fn a_collision_during_socket_handoff_retries_and_persists_the_new_port() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    std::thread::scope(|scope| {
        let launch = scope.spawn(|| start_with_config(&dir, "bind-race", &config));
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let first = dir.path().join("first-serve-port");
        let port: u16 = loop {
            if let Ok(text) = std::fs::read_to_string(&first)
                && let Ok(port) = text.parse()
            {
                break port;
            }
            assert!(std::time::Instant::now() < deadline, "first launch barrier");
            std::thread::sleep(Duration::from_millis(10));
        };
        // The scripted child already owns the collision socket. Rebinding it
        // here would race unrelated holders and reject a legitimate collision.
        std::fs::write(dir.path().join("collision-bound"), "bound").unwrap();
        let harness = launch.join().unwrap();
        assert_ne!(port_of(&config), port);
        assert!(log_of(&dir).contains(&format!("bind failed:{port}:")));
        assert!(log_of(&dir).matches("serve:").count() >= 2);
        drop(harness);
    });
}

#[test]
fn inherited_host_roots_and_credentials_do_not_select_the_service() {
    let dir = TempDir::new().unwrap();
    let host = dir.path().join("host-config");
    let config = dir.path().join("agent-config");
    std::fs::create_dir(&host).unwrap();
    std::fs::write(host.join("sentinel"), "unchanged").unwrap();
    let log = dir.path().join("log");
    let mut harness = OpenCodeHarness::start_in_config(
        scripted_opencode(),
        Some("ses_fake"),
        &[
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("OPENCODE_CONFIG_DIR", host.to_str().unwrap()),
            ("XDG_CONFIG_HOME", host.to_str().unwrap()),
            ("OPENCODE_PASSWORD", "inherited-test-value"),
            ("OPENCODE_SERVER_PASSWORD", "inherited-test-value"),
        ],
        None,
        &config,
    )
    .unwrap_or_else(|error| panic!("{error}: {}", log_of(&dir)));
    settle(&mut harness, &msg("isolated")).unwrap();
    assert!(port_of(&config) > 0);
    assert!(log_of(&dir).contains(&format!(
        "config-root:{}",
        config.join("opencode").display()
    )));
    assert_eq!(std::fs::read_dir(&host).unwrap().count(), 1);
    assert_eq!(
        std::fs::read_to_string(host.join("sentinel")).unwrap(),
        "unchanged"
    );
}

/// Optional vendor acceptance: starts only disposable private services, without
/// a provider call. Set ANYAGENTS_REAL_OPENCODE to the installed executable.
#[test]
#[ignore = "requires an installed OpenCode v2 CLI"]
fn real_opencode_services_use_private_ports_across_restart() {
    let program = std::env::var_os("ANYAGENTS_REAL_OPENCODE")
        .expect("set ANYAGENTS_REAL_OPENCODE to the installed executable");
    let alpha = TempDir::new().unwrap();
    let beta = TempDir::new().unwrap();
    let launch = |dir: &TempDir| {
        OpenCodeHarness::start_in_config(
            std::path::PathBuf::from(&program),
            None,
            &[
                ("OPENCODE_DISABLE_MODELS_FETCH", "1"),
                ("OPENCODE_CONFIG_PROJECT_DISABLE", "1"),
            ],
            Some(dir.path()),
            &dir.path().join("config"),
        )
        .unwrap()
    };
    let (a, b) = std::thread::scope(|scope| {
        let a = scope.spawn(|| launch(&alpha));
        let b = scope.spawn(|| launch(&beta));
        (a.join().unwrap(), b.join().unwrap())
    });
    let a_port = port_of(&alpha.path().join("config"));
    let b_port = port_of(&beta.path().join("config"));
    assert_ne!(a_port, b_port);
    assert!(a.session_id().is_some());
    assert!(b.session_id().is_some());
    drop(a);
    let restarted = launch(&alpha);
    let restarted_port = port_of(&alpha.path().join("config"));
    assert_ne!(restarted_port, b_port);
    assert!(std::net::TcpListener::bind(("127.0.0.1", restarted_port)).is_err());
    drop(restarted);
    drop(b);
}

#[test]
fn a_transient_bind_collision_is_retried_even_after_the_holder_releases_it() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    std::thread::scope(|scope| {
        let launch = scope.spawn(|| start_with_config(&dir, "transient-bind-race", &config));
        let wait = |path: &Path| {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while !path.exists() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "barrier {}",
                    path.display()
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        };
        let first = dir.path().join("first-serve-port");
        wait(&first);
        let port: u16 = std::fs::read_to_string(&first).unwrap().parse().unwrap();
        std::fs::write(dir.path().join("collision-bound"), "bound").unwrap();
        wait(&dir.path().join("collision-reported"));
        // The child releases its holder before reporting this barrier and
        // cannot exit until acknowledged. A later bind probe alone therefore
        // cannot diagnose the collision that actually occurred.
        std::fs::write(dir.path().join("collision-released"), "released").unwrap();
        let harness = launch.join().unwrap();
        assert_ne!(port_of(&config), port);
        assert!(log_of(&dir).contains(&format!("bind failed:{port}:")));
        assert!(log_of(&dir).matches("serve:").count() >= 2);
        drop(harness);
    });
}

#[test]
fn startup_reports_session_identity_persistence_failure() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    std::fs::create_dir_all(config.join("opencode-session-id")).unwrap();
    let log = dir.path().join("log");
    let result = OpenCodeHarness::start_in_config(
        scripted_opencode(),
        Some("ses_fake"),
        &[
            ("ANYAGENTS_FAKE_MODE", "hold"),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("ANYAGENTS_FAKE_STATE", dir.path().to_str().unwrap()),
        ],
        None,
        &config,
    );
    let error = result
        .err()
        .expect("an unwritable session path must fail startup")
        .to_string();
    assert!(error.contains("persist OpenCode session id"), "{error}");
    assert!(!log_of(&dir).contains("api:POST /api/session "));
}

#[test]
fn resume_persists_identity_and_rolls_back_when_the_write_fails() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    std::fs::create_dir_all(&config).unwrap();
    let mut harness = start_with_config(&dir, "hold", &config);
    harness.resume("ses_resumed").unwrap();
    assert_eq!(
        std::fs::read_to_string(config.join("opencode-session-id")).unwrap(),
        "ses_resumed\n"
    );
    std::fs::remove_file(config.join("opencode-session-id")).unwrap();
    std::fs::create_dir(config.join("opencode-session-id")).unwrap();
    let error = harness.resume("ses_other").unwrap_err().to_string();
    assert!(error.contains("persist OpenCode session id"), "{error}");
    assert_eq!(harness.session_id().as_deref(), Some("ses_resumed"));
}

#[test]
fn failed_interrupt_keeps_the_turn_open_while_accepted_interrupt_closes_it() {
    for (mode, succeeds) in [("hold", true), ("interrupt-failed", false)] {
        let dir = TempDir::new().unwrap();
        let mut harness = start(&dir, mode);
        settle(&mut harness, &msg("m1")).unwrap();
        assert_eq!(harness.health().turn_state, TurnState::InTurn);
        let result = harness.cancel();
        if succeeds {
            result.unwrap();
            assert_eq!(harness.health().turn_state, TurnState::Idle);
        } else {
            let err = result.unwrap_err().to_string();
            assert!(err.contains("interruption refused (HTTP 503)"), "{err}");
            assert_eq!(harness.health().turn_state, TurnState::InTurn);
        }
        assert_eq!(
            log_of(&dir)
                .matches("api:POST /api/session/ses_fake/interrupt")
                .count(),
            1
        );
    }
}

#[test]
fn unavailable_session_is_not_replaced_by_a_new_conversation() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(config.join("opencode-session-id"), "ses_fake\n").unwrap();
    let log = dir.path().join("log");
    let result = OpenCodeHarness::start_in_config(
        scripted_opencode(),
        Some("ses_fake"),
        &[
            ("ANYAGENTS_FAKE_MODE", "session-unavailable"),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("ANYAGENTS_FAKE_STATE", dir.path().to_str().unwrap()),
        ],
        None,
        &config,
    );
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("availability is unknown")
    );
    assert!(!log_of(&dir).contains("api:POST /api/session "));
    assert_eq!(
        std::fs::read_to_string(config.join("opencode-session-id")).unwrap(),
        "ses_fake\n"
    );
}

#[test]
fn prompt_404_creates_new_session_restores_context_retries_once_and_persists_session_id() {
    for mode in ["prompt-404", "prompt-404-stderr"] {
        let dir = TempDir::new().unwrap();
        let config = dir.path().join("config");
        std::fs::create_dir_all(config.join("opencode")).unwrap();
        std::fs::write(config.join("opencode-session-id"), "ses_fake\n").unwrap();
        std::fs::write(
            config.join("SYSTEM_PROMPT.md"),
            "# Agent Identity\n- **Agent Name**: `alpha`\n",
        )
        .unwrap();
        std::fs::write(
            config.join("opencode/AGENTS.md"),
            "# Agent Identity\n- **Agent Name**: `alpha`\n",
        )
        .unwrap();
        std::fs::write(
            config.join(".anyagents-session-recovery.md"),
            "## 4. Cross-Harness Session Continuity (`state.db` -> `opencode`)\n\n### Recent Completed Two-Way Turns (`state.db`)\n\n- `[OUT (already sent/completed)]` `msg_prev`: Prior turn completed.\n",
        )
        .unwrap();

        let mut harness = start_with_config(&dir, mode, &config);
        assert_eq!(harness.session_id().as_deref(), Some("ses_fake"));

        let mut first = msg("m1");
        first.text = "resume work after restart".to_string();
        assert_eq!(settle(&mut harness, &first).unwrap(), accepted());
        assert_eq!(harness.session_id().as_deref(), Some("ses_fresh"));
        assert_eq!(
            std::fs::read_to_string(config.join("opencode-session-id")).unwrap(),
            "ses_fresh\n"
        );
        assert!(
            std::fs::read_to_string(config.join("opencode/AGENTS.md"))
                .unwrap()
                .contains("Cross-Harness Session Continuity")
        );
        let recovered_prompt = std::fs::read_to_string(dir.path().join("last-prompt")).unwrap();
        assert!(
            recovered_prompt.starts_with("anyagents-message:m1\n"),
            "{recovered_prompt}"
        );
        assert!(
            recovered_prompt.contains(
                "<session_recovery stale_session=\"ses_fake\" new_session=\"ses_fresh\">"
            ),
            "{recovered_prompt}"
        );
        assert!(
            recovered_prompt.contains("Prior turn completed."),
            "{recovered_prompt}"
        );
        assert!(
            recovered_prompt.contains("resume work after restart"),
            "{recovered_prompt}"
        );
        assert!(harness.wait_answered("m1", Duration::from_secs(5)));
        assert_eq!(harness.answer("m1").as_deref(), Some("pong from opencode"));

        // Subsequent wake/turn uses `ses_fresh` directly without hitting `ses_fake`.
        let mut second = msg("m2");
        second.text = "follow-up wake".to_string();
        assert_eq!(settle(&mut harness, &second).unwrap(), accepted());
        let second_prompt = std::fs::read_to_string(dir.path().join("last-prompt")).unwrap();
        assert!(
            !second_prompt.contains("<session_recovery"),
            "recovery block is only seeded once on the new session: {second_prompt}"
        );
        let log = log_of(&dir);
        assert_eq!(
            log.matches("api:POST /api/session/ses_fake/prompt").count(),
            1,
            "stale session must not be reused after 404 recovery: {log}"
        );
        assert_eq!(
            log.matches("api:POST /api/session ").count(),
            1,
            "exactly one replacement session is created: {log}"
        );
        assert_eq!(
            log.matches("api:POST /api/session/ses_fresh/prompt")
                .count(),
            2,
            "retried prompt and subsequent wake both target the new session: {log}"
        );
    }
}

#[test]
fn prompt_404_after_prior_turn_restores_in_memory_history_and_topology_on_new_session() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    std::fs::create_dir_all(&config).unwrap();
    let mut harness = start_with_config(&dir, "prompt-404-after-first", &config);

    let mut first = msg("m1");
    first.text = "initial assignment context".to_string();
    first.context.topology =
        Some("<topology event=\"initial\" self=\"alpha\" scope=\"private\" />".to_string());
    assert_eq!(settle(&mut harness, &first).unwrap(), accepted());
    assert_eq!(harness.session_id().as_deref(), Some("ses_fake"));

    let mut second = msg("m2");
    second.text = "second wake after service restart".to_string();
    assert_eq!(settle(&mut harness, &second).unwrap(), accepted());
    assert_eq!(harness.session_id().as_deref(), Some("ses_fresh"));
    assert_eq!(
        std::fs::read_to_string(config.join("opencode-session-id")).unwrap(),
        "ses_fresh\n"
    );
    let recovered_prompt = std::fs::read_to_string(dir.path().join("last-prompt")).unwrap();
    assert!(
        recovered_prompt
            .contains("<session_recovery stale_session=\"ses_fake\" new_session=\"ses_fresh\">"),
        "{recovered_prompt}"
    );
    assert!(
        recovered_prompt.contains("initial assignment context"),
        "{recovered_prompt}"
    );
    assert!(
        recovered_prompt
            .contains("<topology event=\"initial\" self=\"alpha\" scope=\"private\" />"),
        "{recovered_prompt}"
    );
    assert!(
        recovered_prompt.contains("second wake after service restart"),
        "{recovered_prompt}"
    );
}

#[test]
fn non_404_prompt_errors_and_repeated_404_fail_closed_without_minting_duplicate_sessions() {
    for (mode, refused) in [
        ("prompt-500", false),
        ("prompt-400", true),
        ("prompt-409", true),
    ] {
        let dir = TempDir::new().unwrap();
        let config = dir.path().join("config");
        std::fs::create_dir_all(&config).unwrap();
        let log = dir.path().join("log");
        // Session ID intentionally contains "404" to verify status detection
        // inspects the error reason rather than matching digits in the URL path.
        let mut harness = OpenCodeHarness::start_in_config(
            scripted_opencode(),
            Some("ses_404_keep"),
            &[
                ("ANYAGENTS_FAKE_MODE", mode),
                ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
                ("ANYAGENTS_FAKE_STATE", dir.path().to_str().unwrap()),
            ],
            None,
            &config,
        )
        .unwrap();

        let outcome = settle(&mut harness, &msg("m1"));
        if refused {
            // Nothing was admitted, and nothing ever will be: the line is
            // rejected, and the process is not at fault.
            assert!(
                matches!(&outcome, Ok(DeliverOutcome::Rejected { reason, .. }) if reason.contains("HTTP 4")),
                "{mode}: {outcome:?}"
            );
            assert!(harness.health().process_alive, "{mode}");
        } else {
            // A server error may or may not have admitted the line: the same
            // id is written once more, and a second one is a fault.
            let err = outcome.unwrap_err().to_string();
            assert!(err.contains("500"), "{err}");
            let log = log_of(&dir);
            assert_eq!(
                log.matches("api:POST /api/session/ses_404_keep/prompt")
                    .count(),
                2,
                "{log}"
            );
            assert_eq!(
                log.matches("\"id\":\"msg_m1\"").count(),
                2,
                "both carry the line's own id: {log}"
            );
        }
        assert_eq!(harness.session_id().as_deref(), Some("ses_404_keep"));
        assert_eq!(
            std::fs::read_to_string(config.join("opencode-session-id")).unwrap(),
            "ses_404_keep\n"
        );
        assert!(harness.written_ids().is_empty());
        let text = log_of(&dir);
        assert_eq!(
            text.matches("api:POST /api/session ").count(),
            0,
            "non-404 error must never mint a new session: {text}"
        );
    }

    // If the replacement session also returns 404, retry happens at most once.
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    std::fs::create_dir_all(&config).unwrap();
    let mut harness = start_with_config(&dir, "prompt-404-always", &config);
    let err = settle(&mut harness, &msg("m1")).unwrap_err().to_string();
    assert!(err.contains("404"), "{err}");
    let text = log_of(&dir);
    assert_eq!(
        text.matches("api:POST /api/session ").count(),
        1,
        "404 recovery must retry only once and not loop: {text}"
    );
}

#[test]
fn begin_reaps_prior_service_pid_and_reclaims_stable_port() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    std::fs::create_dir_all(&config).unwrap();
    let first = start_with_config(&dir, "hold", &config);
    let first_port = port_of(&config);
    let first_pid: u32 = std::fs::read_to_string(config.join("opencode-service-pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(first_pid > 1);
    // Simulate a daemon crash where Drop did not run and the previous service
    // kept holding the port and opencode-service-pid.
    std::mem::forget(first);
    assert!(std::net::TcpListener::bind(("127.0.0.1", first_port)).is_err());

    let second = start_with_config(&dir, "hold", &config);
    let second_port = port_of(&config);
    let second_pid: u32 = std::fs::read_to_string(config.join("opencode-service-pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert_eq!(second_port, first_port);
    assert_ne!(second_pid, first_pid);
    drop(second);
    assert!(!config.join("opencode-service-pid").exists());
}

#[test]
fn every_prompt_carries_the_lines_own_message_id() {
    // OpenCode takes a prompt id that starts with `msg_` and admits it once.
    // A mailbox id already is one; any other id gets the prefix.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish");
    settle(&mut harness, &msg("msg_01mailbox")).unwrap();
    settle(&mut harness, &msg("recovery_msg_01mailbox_1")).unwrap();
    let log = log_of(&dir);
    assert!(log.contains("\"id\":\"msg_01mailbox\""), "{log}");
    assert!(
        log.contains("\"id\":\"msg_recovery_msg_01mailbox_1\""),
        "{log}"
    );
}

#[test]
fn the_service_names_itself_and_delivery_is_mid_turn_and_idempotent() {
    let dir = TempDir::new().unwrap();
    let harness = start(&dir, "finish");
    let delivery = harness.health().delivery;
    assert_eq!(delivery.level, DeliveryLevel::MidTurn);
    assert!(delivery.idempotent);
    assert_eq!(delivery.peer.as_deref(), Some("opencode 2.1.4"));
}

#[test]
fn a_turn_ends_when_the_service_is_idle_and_says_so_to_the_host() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish");
    let woken = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = std::sync::Arc::clone(&woken);
    harness.watch_activity(std::sync::Arc::new(move || {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }));
    assert_eq!(settle(&mut harness, &msg("m1")).unwrap(), accepted());
    eventually(&harness, "the turn ends", |harness| {
        harness.health().turn_state == TurnState::Idle && harness.turns_served() == 1
    });
    assert!(woken.load(std::sync::atomic::Ordering::SeqCst) >= 2);
    let log = log_of(&dir);
    assert!(
        log.contains("api:POST /api/experimental/session/ses_fake/wait"),
        "{log}"
    );

    // A turn the service never ends stays open.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold");
    settle(&mut harness, &msg("m1")).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(harness.health().turn_state, TurnState::InTurn);
    assert_eq!(harness.turns_served(), 0);
}

#[test]
fn a_line_an_earlier_process_applied_is_a_duplicate_and_opens_no_turn() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "applied");
    assert_eq!(
        settle(&mut harness, &msg("m1")).unwrap(),
        DeliverOutcome::Duplicate
    );
    assert_eq!(harness.health().turn_state, TurnState::Idle);
    assert!(harness.written_ids().is_empty());
    assert!(log_of(&dir).contains("api:GET /api/session/ses_fake/message/msg_m1"));

    // One an earlier process wrote and the session has not consumed yet is in
    // its inbox now: this process's prompt woke the session for it.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "parked");
    assert_eq!(settle(&mut harness, &msg("m1")).unwrap(), accepted());
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
}

#[test]
fn an_unknown_outcome_is_settled_by_writing_the_same_id_once_more() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "prompt-502-once");
    assert_eq!(settle(&mut harness, &msg("m1")).unwrap(), accepted());
    let log = log_of(&dir);
    assert_eq!(log.matches("\"id\":\"msg_m1\"").count(), 2, "{log}");
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
}

#[test]
fn a_line_the_readiness_gate_turned_away_waits_its_retry_after_and_the_host_is_woken() {
    // OpenCode's readiness gate answers 503 `service_starting` before any
    // route: nothing is admitted, and it asks to be called again after one
    // second. The line goes back to the mailbox once that second is over, and
    // the host is told so, which brings the next offer.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "prompt-503-once");
    let woken = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = std::sync::Arc::clone(&woken);
    harness.watch_activity(std::sync::Arc::new(move || {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }));
    let before = woken.load(std::sync::atomic::Ordering::SeqCst);
    let sent = std::time::Instant::now();
    assert_eq!(
        settle(&mut harness, &msg("m1")).unwrap(),
        DeliverOutcome::Deferred
    );
    assert!(
        sent.elapsed() >= Duration::from_millis(900),
        "no retry-after"
    );
    eventually(&harness, "the host is woken", |_| {
        woken.load(std::sync::atomic::Ordering::SeqCst) > before
    });
    assert!(harness.written_ids().is_empty());
    assert!(harness.health().process_alive);
    assert_eq!(settle(&mut harness, &msg("m1")).unwrap(), accepted());
    assert_eq!(harness.written_ids(), vec!["m1".to_string()]);
    let log = log_of(&dir);
    assert_eq!(log.matches("\"id\":\"msg_m1\"").count(), 2, "{log}");
}

#[test]
fn a_poison_line_is_rejected_once_and_never_written_again() {
    // A 409 (`ConflictError`: the id belongs to another durable record) or a
    // 400 admits nothing and never will. The line is rejected; every later
    // offer gets the same answer without another call, even once the
    // process is gone, and the next line is not held behind it.
    for (mode, status) in [("prompt-409", "HTTP 409"), ("prompt-400", "HTTP 400")] {
        let dir = TempDir::new().unwrap();
        let mut harness = start(&dir, mode);
        let rejected = match settle(&mut harness, &msg("m1")) {
            Ok(DeliverOutcome::Rejected { reason, .. }) => reason,
            other => panic!("{mode}: {other:?}"),
        };
        assert!(rejected.contains(status), "{mode}: {rejected}");
        for _ in 0..5 {
            assert_eq!(
                harness.deliver(&msg("m1")),
                Ok(DeliverOutcome::Rejected {
                    reason: rejected.clone(),
                    message_specific: rejected.contains("(HTTP 409)"),
                }),
                "{mode}"
            );
        }
        assert_eq!(
            harness.proven("m1"),
            None,
            "{mode}: a rejection proves nothing"
        );
        assert!(harness.health().process_alive, "{mode}: no restart");
        assert!(harness.written_ids().is_empty(), "{mode}");
        // The send slot is free: the next line is written at once.
        assert_eq!(
            harness.deliver(&msg("m2")),
            Ok(DeliverOutcome::InFlight),
            "{mode}"
        );
        assert!(matches!(
            settle(&mut harness, &msg("m2")),
            Ok(DeliverOutcome::Rejected { .. })
        ));
        harness.stop();
        assert_eq!(
            harness.deliver(&msg("m1")),
            Ok(DeliverOutcome::Rejected {
                message_specific: rejected.contains("(HTTP 409)"),
                reason: rejected,
            }),
            "{mode}: a rejection stands after the process"
        );
        let log = log_of(&dir);
        assert_eq!(
            log.matches("\"id\":\"msg_m1\"").count(),
            1,
            "{mode}: written once: {log}"
        );
    }
}

#[test]
fn a_failed_service_behind_the_readiness_gate_is_a_fault() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "prompt-503-failed");
    let err = settle(&mut harness, &msg("m1")).unwrap_err();
    assert!(
        matches!(&err, HarnessError::Failed(reason) if reason.contains("HTTP 503")),
        "{err:?}"
    );
}

#[test]
fn an_opencode_1x_service_fails_the_start_and_no_session_is_touched() {
    for mode in ["v1", "info-404", "info-1x"] {
        let dir = TempDir::new().unwrap();
        let log = dir.path().join("log");
        let started = OpenCodeHarness::start(
            scripted_opencode(),
            Some("ses_fake"),
            &[
                ("ANYAGENTS_FAKE_MODE", mode),
                ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
                ("ANYAGENTS_FAKE_STATE", dir.path().to_str().unwrap()),
            ],
        );
        let Err(HarnessError::Failed(reason)) = started else {
            panic!("{mode}: a 1.x service started: {}", log_of(&dir));
        };
        assert!(
            reason.contains("1.x") && reason.contains("2.x"),
            "{mode}: {reason}"
        );
        let text = log_of(&dir);
        assert!(
            !text.contains("api:GET /api/session/ses_fake")
                && !text.contains("api:POST /api/session"),
            "{mode}: a session was touched: {text}"
        );
        if mode == "v1" {
            // Known from its announcement: no `opencode api` call at all.
            assert!(!text.contains("api:"), "{mode}: {text}");
        } else {
            assert!(text.contains("api:GET /api/info"), "{mode}: {text}");
        }
    }
}

#[test]
fn a_proof_given_just_before_stop_is_still_proven_after_it() {
    // The prompt call is still out when the host stops the process: `stop`
    // waits for its answer, so the admission is known before a new process
    // starts, and it stays readable after the service is gone.
    let dir = TempDir::new().unwrap();
    let log = dir.path().join("log");
    let mut harness = OpenCodeHarness::start(
        scripted_opencode(),
        Some("ses_fake"),
        &[
            ("ANYAGENTS_FAKE_MODE", "hold"),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("ANYAGENTS_FAKE_DELAY_MS", "300"),
        ],
    )
    .unwrap();
    assert_eq!(harness.deliver(&msg("m1")), Ok(DeliverOutcome::InFlight));
    assert_eq!(harness.proven("m1"), None);
    harness.stop();
    assert_eq!(
        harness.proven("m1"),
        Some(Proof::Taken {
            turn_finished: false
        })
    );
    assert!(!harness.health().process_alive);
    assert_eq!(harness.deliver(&msg("m1")), Ok(accepted()));
    assert_eq!(harness.deliver(&msg("m2")), Err(HarnessError::Dead));

    // A line an earlier process applied is proven a duplicate the same way.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "applied");
    assert_eq!(
        settle(&mut harness, &msg("m1")).unwrap(),
        DeliverOutcome::Duplicate
    );
    harness.stop();
    assert_eq!(harness.proven("m1"), Some(Proof::Duplicate));
    assert_eq!(harness.deliver(&msg("m1")), Ok(DeliverOutcome::Duplicate));
}

#[test]
fn a_refused_credential_is_a_fault_and_never_waits_as_a_refusal() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "prompt-401");
    let err = settle(&mut harness, &msg("m1")).unwrap_err();
    assert!(
        matches!(&err, HarnessError::Failed(reason) if reason.contains("HTTP 401")),
        "{err:?}"
    );
    assert!(harness.written_ids().is_empty());
}

#[test]
fn lines_are_written_one_at_a_time_in_mailbox_order() {
    let dir = TempDir::new().unwrap();
    let log = dir.path().join("log");
    let mut harness = OpenCodeHarness::start(
        scripted_opencode(),
        Some("ses_fake"),
        &[
            ("ANYAGENTS_FAKE_MODE", "hold"),
            ("ANYAGENTS_FAKE_LOG", log.to_str().unwrap()),
            ("ANYAGENTS_FAKE_DELAY_MS", "300"),
        ],
    )
    .unwrap();
    // The first call is out: the second line waits behind it, unwritten.
    assert_eq!(
        harness.deliver(&msg("m1")).unwrap(),
        DeliverOutcome::InFlight
    );
    assert_eq!(
        harness.deliver(&msg("m2")).unwrap(),
        DeliverOutcome::Deferred
    );
    assert_eq!(
        harness.deliver(&msg("m1")).unwrap(),
        DeliverOutcome::InFlight
    );
    assert_eq!(settle(&mut harness, &msg("m1")).unwrap(), accepted());
    assert_eq!(settle(&mut harness, &msg("m2")).unwrap(), accepted());
    let text = log_of(&dir);
    let first = text.find("\"id\":\"msg_m1\"").expect("m1 written");
    let second = text.find("\"id\":\"msg_m2\"").expect("m2 written");
    assert!(first < second, "{text}");
    assert_eq!(text.matches("/prompt").count(), 2, "{text}");
}

#[test]
fn what_the_service_admitted_stands_after_it_died() {
    let dir = TempDir::new().unwrap();
    let config = dir.path().join("config");
    std::fs::create_dir_all(&config).unwrap();
    let mut harness = start_with_config(&dir, "hold", &config);
    assert_eq!(settle(&mut harness, &msg("m1")).unwrap(), accepted());
    let pid = std::fs::read_to_string(config.join("opencode-service-pid")).unwrap();
    let killed = std::process::Command::new("kill")
        .args(["-KILL", pid.trim()])
        .status()
        .unwrap();
    assert!(killed.success());
    eventually(&harness, "the service is gone", |harness| {
        !harness.health().process_alive
    });
    assert_eq!(harness.deliver(&msg("m1")).unwrap(), accepted());
    assert_eq!(harness.deliver(&msg("m2")), Err(HarnessError::Dead));
}
