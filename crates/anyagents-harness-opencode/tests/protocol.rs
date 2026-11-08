//! The OpenCode adapter against a scripted `opencode`.
//!
//! The scripted program speaks the CLI's real surface: `serve` holds a service,
//! and `api <METHOD> <path>` prints one JSON answer. Nothing here talks to the
//! machine's own OpenCode.

use std::path::Path;
use std::time::Duration;

use anyagents_harness::{
    Context, DeliverOutcome, DeliveryMode, Harness, OutboundMessage, SteerSupport, TurnState,
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

/// A scripted `opencode` beside this test binary. The server crate ships the
/// same program as `scripted_opencode`; this copy is built with the tests.
fn scripted_opencode() -> std::path::PathBuf {
    let exe = std::env::current_exe().expect("the test binary path");
    // The test binary lives in `deps/`; a bin of the crate is one level up.
    let deps = exe.parent().expect("a directory");
    deps.parent()
        .map(|dir| dir.join("fake_opencode"))
        .filter(|path| path.is_file())
        .or_else(|| Some(deps.join("fake_opencode")).filter(|path| path.is_file()))
        .expect("the scripted opencode is built with these tests")
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
    harness.deliver(&msg("m1")).unwrap();
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
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_answered("m1", Duration::from_secs(10)));
    assert_eq!(harness.answer("m1").as_deref(), Some("pong from opencode"));
}

#[test]
fn an_open_turn_never_blocks_the_next_line() {
    // The service holds its turn open forever. The adapter reports steer, not
    // followup: the second line is written at once, not deferred behind a turn
    // the adapter cannot see the end of.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "hold");
    assert_eq!(harness.deliver(&msg("m1")).unwrap(), accepted());
    assert_eq!(harness.deliver(&msg("m2")).unwrap(), accepted());
    assert_eq!(harness.health().turn_state, TurnState::InTurn);
    assert_eq!(harness.health().delivery_mode, DeliveryMode::Steer);
    assert_eq!(harness.health().steer_support, SteerSupport::Yes);
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
        assert_eq!(harness.deliver(&msg(id)).unwrap(), accepted(), "{id}");
    }
    assert!(harness.wait_seen("m3", Duration::from_secs(5)));
    // A replay of every line writes nothing more.
    for id in ["m1", "m2", "m3"] {
        assert_eq!(harness.deliver(&msg(id)).unwrap(), accepted(), "{id}");
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
            .all(|line| !line.starts_with("api:POST") || line.contains("\"delivery\":\"steer\"")),
        "every prompt is a steer: {text}"
    );
}

#[test]
fn a_delivered_line_is_taken_even_when_the_session_reports_a_failed_turn() {
    // Delivery is the prompt API taking the line, and nothing after it: a turn
    // the session later reports failed does not hold the next line back.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "failed");
    assert_eq!(harness.deliver(&msg("m1")).unwrap(), accepted());
    assert_eq!(harness.deliver(&msg("m2")).unwrap(), accepted());
    assert_eq!(
        harness.written_ids(),
        vec!["m1".to_string(), "m2".to_string()]
    );
}

#[test]
fn a_pushed_line_joins_the_open_turn() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("steer.jsonl");
    std::fs::write(&path, "{\"id\":\"m7\",\"seq\":7,\"message\":\"later\"}\n").unwrap();
    let mut harness = start(&dir, "steer").with_steer(anyagents_harness::SteerQueue::new(&path, 4));
    harness.deliver(&msg("m1")).unwrap();
    let item = harness.poll_steer().unwrap().expect("taken");
    assert_eq!(item.message.id, "m7");
    assert!(harness.inject(&item.message).unwrap());
    // A mailbox line the prompt API already took is taken, not written again.
    assert!(harness.inject(&msg("m1")).unwrap());
    let text = log_of(&dir);
    assert!(text.contains("steered:anyagents-message:m7"), "{text}");
    assert!(text.contains("\"delivery\":\"steer\""), "{text}");
    // One turn, not two: the steer joined the open one.
    assert_eq!(
        text.matches("api:POST /api/session/ses_fake/prompt")
            .count(),
        2
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
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_answered("m1", Duration::from_secs(10)));
    let before = log_of(&dir).matches("anyagents-message:m1").count();
    harness.deliver(&msg("m1")).unwrap();
    let after = log_of(&dir).matches("anyagents-message:m1").count();
    assert_eq!(before, after);
    let _ = Path::new("");
}

#[test]
fn an_earlier_turns_answer_does_not_close_this_turn() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "history");
    harness.deliver(&msg("m1")).unwrap();
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
    harness.deliver(&msg("m1")).unwrap();
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
    harness.deliver(&msg("m1")).unwrap();
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
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_answered("m1", Duration::from_secs(5)));
    assert_eq!(harness.answer("m1").as_deref(), Some("the real answer"));
}

#[test]
fn each_message_is_answered_by_the_message_that_follows_it() {
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish");
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_answered("m1", Duration::from_secs(5)));
    assert_eq!(harness.answer("m1").as_deref(), Some("pong from opencode"));
    // The session now holds m1's prompt and answer. A second message is
    // answered by its own turn, not by m1's.
    harness.deliver(&msg("m2")).unwrap();
    assert!(harness.wait_answered("m2", Duration::from_secs(5)));
    assert_eq!(harness.answer("m2").as_deref(), Some("pong from opencode"));
}

#[test]
fn a_retried_line_does_not_hold_the_turn_open() {
    // `finish` closes the turn. Handing the same line over again must not
    // reopen it, or the next line would defer forever.
    let dir = TempDir::new().unwrap();
    let mut harness = start(&dir, "finish");
    harness.deliver(&msg("m1")).unwrap();
    assert!(harness.wait_answered("m1", Duration::from_secs(5)));
    harness.deliver(&msg("m1")).unwrap();
    // A new line is delivered, not deferred behind a re-opened turn.
    assert_eq!(
        harness.deliver(&msg("m2")).unwrap(),
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
    harness.deliver(&msg("isolated")).unwrap();
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
        harness.deliver(&msg("m1")).unwrap();
        assert_eq!(harness.health().turn_state, TurnState::InTurn);
        let result = harness.cancel();
        if succeeds {
            result.unwrap();
            assert_eq!(harness.health().turn_state, TurnState::Idle);
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("interruption refused (503)")
            );
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
