use super::super::agents::{get_agent, list_agents};
use super::super::agents_roster::{
    archived_agent_view_fast, compute_caller_relationship, list_agents_sync_for_principal,
};
use super::super::channels_groups::channels_route;
use super::super::messaging::{can_actor_message_agent, can_actor_read_agent_private};
use super::super::types::is_self_or_ancestor;
use super::super::*;
use super::helpers::{archetype_validation_state, env_lock};
use crate::api::AgentView;
use anyagents_core::{Agent, now_ms};
use axum::Json;
use axum::http::StatusCode;

#[test]
fn an_agent_view_keeps_the_fields_a_client_from_before_delivery_requires() {
    // A CLI or MCP client from the release before `delivery` requires both.
    #[derive(serde::Deserialize)]
    struct OlderClientView {
        delivery_mode: String,
        steer_support: String,
    }
    let temp = crate::test_tempdir();
    let store = Store::open(temp.path().join("state.db")).unwrap();
    for (harness, mode, steer) in [
        ("spiron", "steer", "yes"),
        ("opencode", "steer", "yes"),
        ("claude-code", "steer", "yes"),
        ("codex", "steer", "yes"),
        ("spiron-cloud", "followup", "no"),
    ] {
        let agent = store
            .create_agent(anyagents_core::NewAgent::new(
                format!("view-{harness}"),
                harness,
            ))
            .unwrap();
        let json = serde_json::to_string(&archived_agent_view_fast(&agent)).unwrap();
        let older: OlderClientView = serde_json::from_str(&json).unwrap();
        assert_eq!(
            (older.delivery_mode.as_str(), older.steer_support.as_str()),
            (mode, steer),
            "{harness}"
        );
    }
    let mid_turn = crate::api::DeliveryView::from(&anyagents_harness_stream::LINE_INTO_TURN);
    assert_eq!(
        (
            mid_turn.legacy_mode().as_str(),
            mid_turn.legacy_steer().as_str()
        ),
        ("steer", "unproven")
    );
}
#[tokio::test]
async fn single_agent_endpoint_resolves_identity_alias_and_enforces_scope() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let root = state
        .store
        .create_agent(anyagents_core::NewAgent::new("root", "opencode"))
        .unwrap();
    let renamed = state.store.rename_agent(&root.id, "renamed").unwrap();
    for name in ["root", "renamed", renamed.id.as_str()] {
        let Json(view) = get_agent(
            Principal::Operator,
            State(state.clone()),
            AxumPath(name.to_string()),
        )
        .await
        .unwrap();
        assert_eq!(view.id, root.id.as_str());
        assert!(view.aliases.contains(&"root".into()));
    }
    let mut child = anyagents_core::NewAgent::new("isolated-child", "opencode");
    child.parent_id = Some(root.id.clone());
    let child = state.store.create_agent(child).unwrap();
    state.store.set_agent_scope(&child.id, "isolated").unwrap();
    let principal = Principal::Agent {
        agent_id: child.id.clone(),
        name: child.name.clone(),
    };
    let Json(own) = get_agent(
        principal.clone(),
        State(state.clone()),
        AxumPath("self".into()),
    )
    .await
    .unwrap();
    assert_eq!(own.id, child.id.as_str());
    let peer = state
        .store
        .create_agent(anyagents_core::NewAgent::new("peer", "opencode"))
        .unwrap();
    assert_eq!(
        get_agent(principal.clone(), State(state.clone()), AxumPath(peer.name))
            .await
            .unwrap_err()
            .status,
        StatusCode::FORBIDDEN
    );
    state.store.delete_agent(&child.id).unwrap();
    assert_eq!(
        get_agent(principal, State(state), AxumPath("self".into()))
            .await
            .unwrap_err()
            .status,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn roster_omits_archive_except_ancestors_and_reports_count() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let root = state
        .store
        .create_agent(anyagents_core::NewAgent::new("archived-parent", "opencode"))
        .unwrap();
    let mut child = anyagents_core::NewAgent::new("live-child", "opencode");
    child.parent_id = Some(root.id.clone());
    state.store.create_agent(child).unwrap();
    state.store.set_agent_state(&root.id, "archived").unwrap();
    for i in 0..40 {
        let a = state
            .store
            .create_agent(anyagents_core::NewAgent::new(
                format!("old-{i}"),
                "opencode",
            ))
            .unwrap();
        state.store.set_agent_state(&a.id, "archived").unwrap();
    }
    // A poisoned diagnostic path would fail if archived views read it.
    let cfg = anyagents_core::agent_config_dir(dir.path(), &root.id);
    std::fs::create_dir_all(cfg.join(".anyagents-profile-rotated-ms")).unwrap();
    let Json(default) = list_agents(
        Principal::Operator,
        State(state.clone()),
        Query(ListAgentsQuery::default()),
    )
    .await
    .unwrap();
    assert_eq!(default.archived_count, 41);
    assert_eq!(default.agents.len(), 2);
    let archived = default
        .agents
        .iter()
        .find(|a| a.id == root.id.as_str())
        .unwrap();
    assert_eq!(archived.install, "archived");
    assert!(archived.schedules.is_empty() && archived.oldest_undelivered_age_ms.is_none());
    for (filter, expected) in [("all", 42), ("archived", 41)] {
        let Json(list) = list_agents(
            Principal::Operator,
            State(state.clone()),
            Query(ListAgentsQuery {
                state: Some(filter.into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(list.agents.len(), expected);
        assert_eq!(list.archived_count, 41);
    }
    let Json(list) = list_agents(
        Principal::Operator,
        State(state.clone()),
        Query(ListAgentsQuery {
            parent: Some("archived-parent".into()),
            name: Some("live-child".into()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert!(list.agents.iter().any(|a| a.name == "live-child"));
    assert_eq!(list.agents.len(), 2);
}

#[test]
#[ignore = "manual benchmark against an isolated production snapshot"]
fn production_roster_query_benchmark() {
    let path = std::env::var("ANYAGENTS_BENCH_DB").expect("isolated snapshot path");
    let start = std::time::Instant::now();
    let store = Store::open(path).unwrap();
    eprintln!(
        "store_open_ms={:.3}",
        start.elapsed().as_secs_f64() * 1000.0
    );
    let health = HealthBoard::default();
    let mut times = Vec::new();
    for _ in 0..20 {
        let start = std::time::Instant::now();
        let (agents, archived) = store.roster_agents(None).unwrap();
        let views = roster_views(
            &store,
            &health,
            &agents.iter().collect::<Vec<_>>(),
            archived,
            &ListAgentsQuery::default(),
        )
        .unwrap();
        let bytes = serde_json::to_vec(&views).unwrap();
        times.push(start.elapsed().as_secs_f64() * 1000.0);
        assert!(bytes.len() > 10);
    }
    times.sort_by(f64::total_cmp);
    eprintln!(
        "roster_ms min={:.3} median={:.3} max={:.3}",
        times[0], times[10], times[19]
    );
    let target = store.agent_by_name("vulcan").unwrap();
    let start = std::time::Instant::now();
    for _ in 0..100 {
        store
            .inbox_query(&target.id, None, Some(20), false)
            .unwrap();
    }
    eprintln!(
        "inbox20_mean_ms={:.3}",
        start.elapsed().as_secs_f64() * 10.0
    );
    let start = std::time::Instant::now();
    for _ in 0..100 {
        agent_view(&store, &health, &store.agent_by_name("vulcan").unwrap()).unwrap();
    }
    eprintln!(
        "single_agent_mean_ms={:.3}",
        start.elapsed().as_secs_f64() * 10.0
    );
}

#[test]
fn roster_authorization_matches_the_single_agent_checks() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let store = &state.store;
    let make = |name: &str, parent: Option<&Agent>, scope: Option<&str>| {
        let mut new = anyagents_core::NewAgent::new(name, "codex");
        new.parent_id = parent.map(|p| p.id.clone());
        new.instructions = Some(format!("{name} private instructions"));
        let agent = store.create_agent(new).unwrap();
        match scope {
            Some(scope) => store.set_agent_scope(&agent.id, scope).unwrap(),
            None => agent,
        }
    };
    let root = make("root", None, None);
    let admin = make("admin", None, None);
    let peer = make("peer", None, None);
    let lead = make("lead", Some(&root), Some("siblings"));
    let sib = make("sib", Some(&root), Some("siblings"));
    let worker = make("worker", Some(&lead), None);
    let iso = make("iso", Some(&root), Some("isolated"));
    let iso_child = make("iso-child", Some(&iso), None);
    let public = make("public", Some(&peer), Some("public"));
    let private = make("private", Some(&peer), None);
    let gone = make("gone", Some(&lead), None);
    store.set_agent_state(&gone.id, "archived").unwrap();
    store.join_group("team", &iso.id).unwrap();
    store.join_group("team", &private.id).unwrap();
    store
        .record_outbound(&private.id, "agent:worker", None, "hello")
        .unwrap();
    let callers = [
        &root, &admin, &peer, &lead, &sib, &worker, &iso, &iso_child, &public, &private,
    ];
    for caller in callers {
        let caller = store.agent(&caller.id).unwrap();
        let principal = Principal::Agent {
            agent_id: caller.id.clone(),
            name: caller.name.clone(),
        };
        for filter in [None, Some("all"), Some("archived")] {
            let query = ListAgentsQuery {
                state: filter.map(str::to_string),
                ..Default::default()
            };
            let Json(listed) = list_agents_sync_for_principal(&state, &principal, &query).unwrap();
            // The isolated-scope rule, checked row by row against the store.
            let (roster, archived) = store.roster_agents(filter).unwrap();
            let visible: Vec<&Agent> = roster
                .iter()
                .filter(|agent| {
                    !(caller.is_isolated()
                        && !is_self_or_ancestor(store, &caller.id, agent)
                        && caller.parent_id.as_ref() != Some(&agent.id)
                        && !store.share_any_group_or_chat(&caller.id, &agent.id))
                })
                .collect();
            let expected = roster_views(store, &state.health, &visible, archived, &query).unwrap();
            let ids = |views: &[AgentView]| -> Vec<String> {
                views.iter().map(|view| view.id.clone()).collect()
            };
            assert_eq!(
                ids(&listed.agents),
                ids(&expected.agents),
                "{}",
                caller.name
            );
            for view in &listed.agents {
                let target = store.agent_by_name(&view.id).unwrap();
                let context = format!("{} -> {}", caller.name, target.name);
                assert_eq!(
                    view.relationship,
                    Some(compute_caller_relationship(store, &caller, &target)),
                    "{context}"
                );
                assert_eq!(
                    view.can_message,
                    Some(
                        can_actor_message_agent(store, &Actor::Agent(caller.name.clone()), &target)
                            .is_ok()
                    ),
                    "{context}"
                );
                let expect_instructions = target.state != "archived"
                    && can_actor_read_agent_private(store, &principal, &target);
                assert_eq!(
                    view.instructions.is_some(),
                    expect_instructions,
                    "{context}"
                );
            }
        }
    }
}

/// Vendor history each harness leaves behind that the view once scanned:
/// a Claude Code session ending in a rate-limit rejection, an AGY CLI log
/// ending in an error, and Codex rollouts ending in a 401 turn fault.
fn write_vendor_history(cfg: &Path) {
    let reset_s = now_ms() / 1000 + 3600;
    let projects = cfg.join("projects/-work");
    std::fs::create_dir_all(&projects).unwrap();
    std::fs::write(
        projects.join("session.jsonl"),
        format!(
            "{}\n",
            serde_json::json!({
                "type": "assistant",
                "apiError": "usage_limit_reached",
                "quotaLimits": {"status": "rejected", "resetsAt": reset_s},
                "error": "rate_limit",
                "apiErrorStatus": 429
            })
        ),
    )
    .unwrap();
    let logs = cfg.join(".gemini/antigravity-cli/log");
    std::fs::create_dir_all(&logs).unwrap();
    std::fs::write(
        logs.join("cli-previous-session.log"),
        "run ended with error and no response: previous agy session failed\n",
    )
    .unwrap();
    let sessions = cfg.join("sessions/2026/11/13");
    std::fs::create_dir_all(&sessions).unwrap();
    let fault = serde_json::json!({
        "type": "event_msg",
        "payload": {"type": "task_complete", "error": {"message": "unauthorized (401)"}}
    });
    std::fs::write(
        sessions.join("rollout-2026-11-13T00-00-00-x.jsonl"),
        format!("{fault}\n{fault}\n{fault}\n"),
    )
    .unwrap();
}

#[test]
fn agent_views_never_read_vendor_history() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let health = HealthBoard::default();
    for harness in [
        "agy",
        "claude-code",
        "codex",
        "opencode",
        "muse",
        "omp",
        "spiron",
    ] {
        let agent = store
            .create_agent(anyagents_core::NewAgent::new(harness, harness))
            .unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &agent.id);
        write_vendor_history(&cfg);
        for view in [
            agent_view(&store, &health, &agent).unwrap(),
            crate::agent::view_named(dir.path(), &agent.name).unwrap(),
        ] {
            assert_eq!(view.last_error, None, "{harness}");
            assert_eq!(view.quota_reset_at_ms, None, "{harness}");
            assert_eq!(view.block_reason, None, "{harness}");
        }
        let (agents, archived) = store.roster_agents(None).unwrap();
        let roster = roster_views(
            &store,
            &health,
            &agents.iter().collect::<Vec<_>>(),
            archived,
            &ListAgentsQuery::default(),
        )
        .unwrap();
        assert!(
            roster
                .agents
                .iter()
                .all(|view| view.last_error.is_none() && view.quota_reset_at_ms.is_none()),
            "{harness}"
        );

        // What the adapter persisted is what the view reports.
        let reset_ms = now_ms() + 3_600_000;
        std::fs::write(
            cfg.join(".anyagents-last-error.json"),
            serde_json::json!({
                "timestamp_ms": now_ms(),
                "error": "current harness failed",
                "quota_reset_at_ms": reset_ms,
            })
            .to_string(),
        )
        .unwrap();
        let expected_quota = (harness != "claude-code").then_some(reset_ms);
        for view in [
            agent_view(&store, &health, &agent).unwrap(),
            crate::agent::view_named(dir.path(), &agent.name).unwrap(),
        ] {
            assert_eq!(view.last_error.as_deref(), Some("current harness failed"));
            assert_eq!(view.quota_reset_at_ms, expected_quota, "{harness}");
        }
        std::fs::remove_file(cfg.join(".anyagents-last-error.json")).unwrap();
    }
}

/// A quota refusal persisted with only its text still names its reset:
/// the wait it states counts from when it was recorded, however many days
/// or hours it spans, and an elapsed one releases the agent.
#[test]
fn agent_view_reads_a_multi_day_reset_from_a_persisted_quota_text() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let health = HealthBoard::default();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("quota-agy", "agy"))
        .unwrap();
    store.set_agent_state(&agent.id, "running").unwrap();
    let agent = store.agent(&agent.id).unwrap();
    let cfg = anyagents_core::agent_config_dir(dir.path(), &agent.id);
    std::fs::create_dir_all(&cfg).unwrap();
    let text = "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 86h43m28s.";
    let recorded_at = now_ms();
    std::fs::write(
        cfg.join(".anyagents-last-error.json"),
        serde_json::json!({"error": text, "quota_reset_at_ms": null, "timestamp_ms": recorded_at})
            .to_string(),
    )
    .unwrap();
    let reset_ms = recorded_at + ((86 * 60 + 43) * 60 + 28) * 1_000;
    for view in [
        agent_view(&store, &health, &agent).unwrap(),
        crate::agent::view_named(dir.path(), &agent.name).unwrap(),
    ] {
        assert_eq!(view.quota_reset_at_ms, Some(reset_ms));
        assert_eq!(view.block_reason.as_deref(), Some("quota_exhausted"));
        assert_ne!(view.turn_state, "errored");
    }

    // The stated wait has passed: the record no longer holds the agent.
    std::fs::write(
        cfg.join(".anyagents-last-error.json"),
        serde_json::json!({
            "error": "Individual quota reached. Resets in 3d14h.",
            "quota_reset_at_ms": null,
            "timestamp_ms": now_ms() - (3 * 24 + 15) * 3_600_000,
        })
        .to_string(),
    )
    .unwrap();
    let view = agent_view(&store, &health, &agent).unwrap();
    assert_eq!(view.quota_reset_at_ms, None);
    assert_eq!(view.last_error, None);
    assert_eq!(view.block_reason, None);
}

#[test]
fn agent_view_reports_the_live_board_over_persisted_faults() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let health = HealthBoard::default();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("claude", "claude-code"))
        .unwrap();
    store.set_agent_state(&agent.id, "running").unwrap();
    let agent = store.agent(&agent.id).unwrap();
    let cfg = anyagents_core::agent_config_dir(dir.path(), &agent.id);
    write_vendor_history(&cfg);
    let reset_ms = now_ms() + 3_600_000;
    let live = |quota: Option<i64>, error: Option<&str>| anyagents_supervisor::LiveState {
        process_alive: true,
        turn_state: anyagents_harness::TurnState::Idle,
        delivery: Some(crate::vendor::declared_delivery("claude-code")),
        quota_reset_at_ms: quota,
        restart_at_ms: None,
        pid: Some(42),
        live_harness: Some("claude-code".into()),
        live_model: None,
        process_started_at_ms: Some(1),
        last_error: error.map(str::to_string),
        retry_at_ms: None,
    };
    health.publish(&agent.id, live(Some(reset_ms), None));
    let view = agent_view(&store, &health, &agent).unwrap();
    assert_eq!(view.process, "alive");
    assert_eq!(view.quota_reset_at_ms, Some(reset_ms));
    assert_eq!(view.block_reason.as_deref(), Some("quota_exhausted"));
    assert_eq!(view.turn_state, "idle");

    health.publish(&agent.id, live(None, None));
    let view = agent_view(&store, &health, &agent).unwrap();
    assert_eq!(view.quota_reset_at_ms, None);
    assert_eq!(view.block_reason, None);
    assert_eq!(view.turn_state, "idle");

    health.publish(&agent.id, live(None, Some("not logged in")));
    let view = agent_view(&store, &health, &agent).unwrap();
    assert_eq!(view.turn_state, "errored");
    assert_eq!(view.block_reason.as_deref(), Some("sign_in_required"));
}

/// A capacity fault the supervisor retries is work in progress: the view
/// shows the agent working with its retry time, never `errored`, even
/// though the adapter persisted the fault, and its parent is told nothing.
/// Without the retry the same fault is an error, and the parent is told.
#[test]
fn agent_view_shows_a_capacity_retry_as_work_in_progress_without_a_parent_notice() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let health = HealthBoard::default();
    let lead = store
        .create_agent(anyagents_core::NewAgent::new("lead", "spiron"))
        .unwrap();
    let worker = store
        .create_agent(anyagents_core::NewAgent {
            parent_id: Some(lead.id.clone()),
            ..anyagents_core::NewAgent::new("worker", "spiron")
        })
        .unwrap();
    store.set_agent_state(&worker.id, "running").unwrap();
    let worker = store.agent(&worker.id).unwrap();
    let cfg = anyagents_core::agent_config_dir(dir.path(), &worker.id);
    std::fs::create_dir_all(&cfg).unwrap();
    let fault = r#"Sylphx AI failure Some("included_capacity_unavailable"): Included capacity is not available right now. Retry later. (request req_7)"#;
    // What the stream adapter persists when the turn fails.
    std::fs::write(
        cfg.join(".anyagents-last-error.json"),
        serde_json::json!({"error": fault, "quota_reset_at_ms": null, "timestamp_ms": now_ms()})
            .to_string(),
    )
    .unwrap();
    let retry_at = now_ms() + 15_000;
    let live = |retry_at_ms: Option<i64>| anyagents_supervisor::LiveState {
        process_alive: true,
        turn_state: if retry_at_ms.is_some() {
            anyagents_harness::TurnState::InTurn
        } else {
            anyagents_harness::TurnState::Idle
        },
        delivery: Some(crate::vendor::declared_delivery("spiron")),
        quota_reset_at_ms: None,
        restart_at_ms: None,
        pid: Some(42),
        live_harness: Some("spiron".into()),
        live_model: None,
        process_started_at_ms: Some(1),
        last_error: retry_at_ms.is_none().then(|| fault.to_string()),
        retry_at_ms,
    };
    let parent_notices = || {
        store
            .inbox(&lead.id, None)
            .unwrap()
            .into_iter()
            .filter(|m| m.text.starts_with("[Sub-Agent Error]"))
            .count()
    };

    health.publish(&worker.id, live(Some(retry_at)));
    let view = agent_view(&store, &health, &worker).unwrap();
    assert_eq!(view.turn_state, "working");
    assert_eq!(view.retry_at_ms, Some(retry_at));
    assert_eq!(view.last_error, None);
    assert_eq!(view.block_reason, None);
    assert_eq!(parent_notices(), 0);

    health.publish(&worker.id, live(None));
    let view = agent_view(&store, &health, &worker).unwrap();
    assert_eq!(view.turn_state, "errored");
    assert_eq!(view.retry_at_ms, None);
    assert_eq!(view.last_error.as_deref(), Some(fault));
    assert_eq!(parent_notices(), 1);
}

#[test]
fn channel_roster_refuses_corrupt_persisted_state_without_mutation() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let channel = state
        .store
        .create_channel(anyagents_core::NewChannel {
            kind: "telegram".into(),
            identity: Some("example_bot".into()),
            config: "{}".into(),
            credential_ref: None,
        })
        .unwrap();
    let db = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let Json(valid) = channels_route(Principal::Operator, State(state.clone()))
            .await
            .unwrap_or_else(|error| panic!("{}", error.message));
        assert_eq!(valid["channels"].as_array().unwrap().len(), 1);
        for invalid in ["{", "[]", "null"] {
            db.execute(
                "UPDATE channels SET state = ?1 WHERE id = ?2",
                rusqlite::params![invalid, channel.id.as_str()],
            )
            .unwrap();
            let error = channels_route(Principal::Operator, State(state.clone()))
                .await
                .expect_err("damaged channel state cannot become an empty view");
            assert!(error.message.contains("state"), "{}", error.message);
            let retained: String = db
                .query_row(
                    "SELECT state FROM channels WHERE id = ?1",
                    [channel.id.as_str()],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(retained, invalid);
        }
    });
}

/// A parked agent lists as `faulted`, with its errored turns and why it
/// is blocked; a resume lists it as it was.
#[test]
fn a_parked_agent_is_shown_faulted_until_it_is_resumed() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("worker", "spiron"))
        .unwrap();
    store.set_agent_state(&agent.id, "running").unwrap();
    for _ in 0..anyagents_core::MAX_ERRORED_TURNS {
        store
            .note_turn_errored(&agent.id, "context_length_exceeded", true)
            .unwrap();
    }
    assert!(store.park_faulted(&agent.id).unwrap());
    let health = HealthBoard::default();
    let agent = store.agent(&agent.id).unwrap();
    let view = agent_view(&store, &health, &agent).unwrap();
    assert_eq!(view.state, "paused");
    assert_eq!(view.display_state(), "faulted");
    assert_eq!(view.turn_state, "faulted");
    assert_eq!(view.block_reason.as_deref(), Some("faulted"));
    assert_eq!(view.last_error.as_deref(), Some("context_length_exceeded"));
    let errors = view.turn_errors.expect("errored turns are shown");
    assert_eq!(errors.streak, anyagents_core::MAX_ERRORED_TURNS);
    assert_eq!(errors.limit, anyagents_core::MAX_ERRORED_TURNS);
    assert!(errors.faulted_at_ms.is_some());
    let (roster, _) = store.roster_agents(None).unwrap();
    let listed = roster_views(
        &store,
        &health,
        &roster.iter().collect::<Vec<_>>(),
        0,
        &ListAgentsQuery::default(),
    )
    .unwrap_or_else(|error| panic!("{}", error.message));
    assert_eq!(listed.agents[0].display_state(), "faulted");

    store.set_agent_state(&agent.id, "running").unwrap();
    let agent = store.agent(&agent.id).unwrap();
    let view = agent_view(&store, &health, &agent).unwrap();
    assert_eq!(view.display_state(), "running");
    assert!(view.turn_errors.is_none());
}

#[tokio::test]
async fn list_agents_all_paginates_live_first_and_omits_archived_instructions_while_single_get_preserves_them()
 {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let store = &state.store;

    let mut root_new = anyagents_core::NewAgent::new("root-live", "codex");
    root_new.instructions = Some("ROOT_PRIVATE".into());
    let root = store.create_agent(root_new).unwrap();
    let mut child_new = anyagents_core::NewAgent::new("child-live", "codex");
    child_new.parent_id = Some(root.id.clone());
    let child = store.create_agent(child_new).unwrap();
    let mut gc_new = anyagents_core::NewAgent::new("grandchild-arch", "codex");
    gc_new.parent_id = Some(child.id.clone());
    gc_new.instructions = Some("GRANDCHILD_PRIVATE".into());
    let gc = store.create_agent(gc_new).unwrap();
    store.set_agent_state(&gc.id, "archived").unwrap();

    for idx in 0..209 {
        let mut n = anyagents_core::NewAgent::new(format!("arch-{idx:03}"), "codex");
        n.instructions = Some(format!("HUGE_INSTRUCTIONS_{idx}"));
        let a = store.create_agent(n).unwrap();
        store.set_agent_state(&a.id, "archived").unwrap();
    }

    // Default `state=all` caps at 200 rows, sorts live agents first, omits archived instructions,
    // and still reports the exact total `archived_count` (210).
    let Json(page1) = list_agents(
        Principal::Operator,
        State(state.clone()),
        Query(ListAgentsQuery {
            state: Some("all".into()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(page1.archived_count, 210);
    assert_eq!(page1.agents.len(), 200);
    assert!(page1.agents[0].state != "archived");
    assert!(page1.agents[1].state != "archived");
    assert!(
        page1
            .agents
            .iter()
            .filter(|a| a.state == "archived")
            .all(|a| a.instructions.is_none()),
        "archived entries in list view must omit instructions"
    );

    // Explicit offset/limit reaches the remaining 12 archived rows.
    let Json(page2) = list_agents(
        Principal::Operator,
        State(state.clone()),
        Query(ListAgentsQuery {
            state: Some("all".into()),
            limit: Some(20),
            offset: Some(200),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(page2.archived_count, 210);
    assert_eq!(page2.agents.len(), 12);

    // `include_descendants=true` on `state=all` returns both child and archived grandchild.
    let Json(subtree) = list_agents(
        Principal::Operator,
        State(state.clone()),
        Query(ListAgentsQuery {
            state: Some("all".into()),
            parent: Some("root-live".into()),
            include_descendants: Some(true),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(subtree.agents.len(), 2);
    assert!(subtree.agents.iter().any(|a| a.name == "child-live"));
    assert!(subtree.agents.iter().any(|a| a.name == "grandchild-arch"));

    // Single-agent `GET /v1/agents/{name}` STILL returns full instructions for archived agents.
    let Json(single) = get_agent(
        Principal::Operator,
        State(state.clone()),
        AxumPath("grandchild-arch".to_string()),
    )
    .await
    .unwrap();
    assert_eq!(single.state, "archived");
    assert!(
        single
            .instructions
            .as_deref()
            .is_some_and(|s| s.contains("GRANDCHILD_PRIVATE"))
    );
}
