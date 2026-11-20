//! Unit and in-process integration tests for the HTTP/JSON mailbox server.

use std::path::PathBuf;

use anyagents_core::{Agent, Error, Incoming, now_ms};
use anyagents_installer::{CommandRunner, InstallError, LineSink};
use axum::Json;
use axum::http::StatusCode;

use crate::api::{AgentView, SendRequest, SendResponse};
use crate::create;
use crate::vendor::HarnessHost;

use super::agents::{
    DescriptionRequest, HarnessRequest, SpawnFinish, create_agent_route, finish_interrupted_spawns,
    get_agent, get_description_route, harness_route, list_agents, record_spawn_finish,
    reset_session_route, set_description_route, spawn_finish_marker,
};
use super::agents_lifecycle::{
    archive_agent_route, delete_agent_route, mark_archive_generation_locked, pause_agent_route,
    reap_archived_agents_at, reap_subagent_ephemeral_dirs_in, remove_agent_identity_files,
    wake_agent_route, workdirs_overlap,
};
use super::agents_roster::{
    archived_agent_view_fast, compute_caller_relationship, list_agents_sync_for_principal,
};
use super::bootstrap::{
    active_seat_ids, binds_loopback, consolidate_idle_agent_memory, local_listen_addr,
    local_listen_addr_with_port, run_with_identity, stopped,
};
use super::channels_groups::channels_route;
use super::messaging::{
    SendPlan, can_actor_message_agent, can_actor_read_agent_private, can_sender_message_agent,
    check_targets, media_upload, plan_send, send, send_key, send_sync,
};
use super::messaging_conversation::{
    ConversationQuery, Since, inbox, operator_inbox, operator_reports,
};
use super::ops::{
    RevokeRemoteSessionsRequest, UsageQuery, revoke_remote_sessions_route, usage_report,
};
use super::schedules::{
    ScheduleRequest, authorize_schedule_target, handle_schedule_action, list_fleet_schedules,
    list_schedules_route,
};
use super::types::is_self_or_ancestor;
use super::webhooks_mcp::{
    MCP_PAYLOAD_ENCODING_HEADER, decode_mcp_request_payload, mcp_get_route, mcp_post_route,
};
use super::*;

include!("../a2a_media_tests.rs");

#[test]
fn memory_backlog_follows_wakes_and_idle_consolidation_visits_nothing() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("sleeper", "codex"))
        .unwrap();
    let root = dir.path().join("agents").join(agent.id.as_str());
    let sleep_state = root.join("memory/.sleep-state.json");
    let write_topic = |topic: &str| {
        anyagents_core::learned_memory::upsert_memory_topic(
            &root,
            "sleeper",
            None,
            topic,
            Some("fact"),
            "learned",
            anyagents_core::now_ms(),
        )
        .unwrap();
    };
    write_topic("first");
    let backlog = memory_backlog::start(&store);
    assert_eq!(backlog.pending(), std::slice::from_ref(&agent.id));
    consolidate_idle_agent_memory(&store, dir.path(), &HealthBoard::default(), &backlog);
    assert!(sleep_state.exists());
    assert!(backlog.pending().is_empty());

    // Nothing woke the agent, so a pass does not look at it.
    write_topic("second");
    std::fs::remove_file(&sleep_state).unwrap();
    consolidate_idle_agent_memory(&store, dir.path(), &HealthBoard::default(), &backlog);
    assert!(!sleep_state.exists(), "idle agent was visited");

    store
        .receive(&agent.id, anyagents_core::Incoming::new("new work"))
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while backlog.pending().is_empty() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(backlog.pending(), std::slice::from_ref(&agent.id));
    consolidate_idle_agent_memory(&store, dir.path(), &HealthBoard::default(), &backlog);
    assert!(sleep_state.exists(), "woken agent was not consolidated");
    assert!(backlog.pending().is_empty());
}

#[test]
fn idle_memory_consolidation_preserves_selected_and_disabled_session_pins() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    for (name, source) in [
        ("default", Some(std::path::Path::new("memory/MEMORY.md"))),
        ("selected", Some(std::path::Path::new("selected.md"))),
        ("disabled", None),
    ] {
        let agent = store
            .create_agent(anyagents_core::NewAgent::new(name, "codex"))
            .unwrap();
        let root = dir.path().join("agents").join(agent.id.as_str());
        std::fs::create_dir_all(root.join("memory")).unwrap();
        std::fs::write(root.join("memory/MEMORY.md"), "original default").unwrap();
        std::fs::write(root.join("selected.md"), "original selected").unwrap();
        let pinned =
            anyagents_core::host_session::new_session_with_source(&root, "codex", source).unwrap();
        anyagents_core::learned_memory::upsert_memory_topic(
            &root,
            name,
            None,
            "new-topic",
            Some("new fact"),
            "changed after pin",
            anyagents_core::now_ms(),
        )
        .unwrap();
        std::fs::write(root.join("selected.md"), "changed selected").unwrap();
        let backlog = memory_backlog::MemoryBacklog::default();
        backlog.mark(&agent.id);
        consolidate_idle_agent_memory(&store, dir.path(), &HealthBoard::default(), &backlog);
        assert!(backlog.pending().is_empty(), "caught-up agent stays queued");
        assert_eq!(
            anyagents_core::host_session::active(&root).unwrap(),
            Some(pinned.clone())
        );
        let next =
            anyagents_core::host_session::new_session_with_source(&root, "codex", source).unwrap();
        assert_ne!(next.generation, pinned.generation);
        if source.is_some() {
            assert_ne!(next.memory.sha256, pinned.memory.sha256);
        } else {
            assert_eq!(next.memory.sha256, pinned.memory.sha256);
        }
    }
}

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
use std::io::{Read, Write};
use std::sync::MutexGuard;

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
    store.set_agent_admin(&admin.id, true).unwrap();
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

/// Who may message whom: an ancestor reaches every non-isolated
/// descendant, and every refusal names the route that works.
#[test]
fn ancestors_reach_descendants_and_refusals_name_the_route() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let store = &state.store;
    let make = |name: &str, parent: Option<&Agent>, scope: Option<&str>| {
        let mut new = anyagents_core::NewAgent::new(name, "codex");
        new.parent_id = parent.map(|p| p.id.clone());
        let agent = store.create_agent(new).unwrap();
        match scope {
            Some(scope) => store.set_agent_scope(&agent.id, scope).unwrap(),
            None => agent,
        }
    };
    let sage = make("sage", None, None);
    let vulcan = make("vulcan", None, None);
    let rollout = make("rollout", Some(&sage), None);
    let bump = make("bump", Some(&rollout), None);
    let deeper = make("deeper", Some(&bump), None);
    let sealed = make("sealed", Some(&rollout), Some("isolated"));
    let fix = make("fix", Some(&rollout), None);
    let cousin = make("cousin", Some(&vulcan), None);

    // A grandparent and a great-grandparent reach their descendants.
    assert!(can_sender_message_agent(store, &sage, &bump).is_ok());
    assert!(can_sender_message_agent(store, &sage, &deeper).is_ok());
    assert!(can_sender_message_agent(store, &rollout, &deeper).is_ok());
    // An isolated descendant stays 1-to-1 with its direct parent.
    assert!(can_sender_message_agent(store, &sage, &sealed).is_err());
    assert!(can_sender_message_agent(store, &rollout, &sealed).is_ok());
    // Upward stays through the parent.
    assert!(can_sender_message_agent(store, &bump, &sage).is_err());

    let text = |result: Result<(), ApiError>| result.unwrap_err().message;
    let sibling = text(can_sender_message_agent(store, &fix, &bump));
    assert!(sibling.contains("it is your sibling"), "{sibling}");
    assert!(sibling.contains("`to: [\"parent\"]`"), "{sibling}");
    assert!(sibling.contains("agent:rollout"), "{sibling}");
    let outsider = text(can_sender_message_agent(store, &vulcan, &bump));
    assert!(
        outsider.contains("Message `agent:rollout` instead"),
        "{outsider}"
    );
    assert!(outsider.contains("scope: \"public\""), "{outsider}");
    let cross = text(can_sender_message_agent(store, &cousin, &fix));
    assert!(cross.contains("Message `agent:rollout` instead"), "{cross}");
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

#[test]
fn deletion_retains_identity_and_resources_until_process_stop() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("busy", "opencode"))
        .unwrap();
    let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(config.join("preserve"), "retained state").unwrap();
    let mut live = anyagents_supervisor::LiveState {
        process_alive: true,
        turn_state: anyagents_harness::TurnState::Idle,
        delivery: Some(crate::vendor::declared_delivery(&agent.harness)),
        quota_reset_at_ms: None,
        restart_at_ms: None,
        pid: None,
        live_harness: Some(agent.harness.clone()),
        live_model: None,
        process_started_at_ms: None,
        last_error: None,
        retry_at_ms: None,
    };
    state.health.publish(&agent.id, live.clone());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let refused = delete_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(agent.name.clone()),
        )
        .await
        .expect_err("live process must retain identity");
        assert_eq!(refused.status, StatusCode::CONFLICT);
        assert_eq!(state.store.agent(&agent.id).unwrap().state, "archived");
        assert_eq!(
            std::fs::read_to_string(config.join("preserve")).unwrap(),
            "retained state"
        );
        let process = state.store.try_lock_agent(&agent.id).unwrap();
        live.process_alive = false;
        state.health.publish(&agent.id, live);
        let refused = delete_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(agent.name.clone()),
        )
        .await
        .expect_err("optimistic health must not revoke process custody");
        assert_eq!(refused.status, StatusCode::CONFLICT);
        assert!(state.store.agent(&agent.id).is_ok());
        assert_eq!(
            std::fs::read_to_string(config.join("preserve")).unwrap(),
            "retained state"
        );
        drop(process);
        let _ = delete_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(agent.name.clone()),
        )
        .await
        .unwrap_or_else(|error| panic!("{}", error.message));
        assert!(state.store.agent(&agent.id).is_err());
        assert!(!config.exists());
    });
}

#[test]
fn deletion_waits_for_process_custody_after_optimistic_stopped_health() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("stopping", "opencode"))
        .unwrap();
    state.store.set_agent_running(&agent.id, "/work").unwrap();
    let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(config.join("preserve"), "retained state").unwrap();
    let process = state.store.try_lock_agent(&agent.id).unwrap();
    // An absent health row is the same optimistic stopped projection used
    // during supervisor shutdown; actual process custody remains occupied.
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let deleting = tokio::spawn(delete_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(agent.name.clone()),
        ));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while state.store.agent(&agent.id).unwrap().state != "archived" {
            assert!(std::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert!(
            !deleting.is_finished(),
            "shutdown must wait for actual custody"
        );
        assert_eq!(
            std::fs::read_to_string(config.join("preserve")).unwrap(),
            "retained state"
        );
        drop(process);
        let _ = deleting
            .await
            .unwrap()
            .unwrap_or_else(|error| panic!("{}", error.message));
        assert!(state.store.agent(&agent.id).is_err());
        assert!(!config.exists());
    });
}

#[test]
fn deleted_authenticated_agent_cannot_read_roster_or_send() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let root = state
        .store
        .create_agent(anyagents_core::NewAgent::new("private-root", "opencode"))
        .unwrap();
    state
        .store
        .set_agent_instructions(&root.id, Some("private instruction sentinel"))
        .unwrap();
    let mut new_child = anyagents_core::NewAgent::new("deleted-caller", "opencode");
    new_child.parent_id = Some(root.id.clone());
    let child = state.store.create_agent(new_child).unwrap();
    let principal = Principal::Agent {
        agent_id: child.id.clone(),
        name: child.name.clone(),
    };
    let actor = Actor::Agent(child.name.clone());
    let body = SendRequest {
        to: vec![root.name.clone()],
        text: "must not arrive".into(),
        ..Default::default()
    };
    let plan = plan_send(&state.store, &principal, &actor, &body)
        .unwrap_or_else(|e| panic!("{}", e.message));
    // Positive controls use the same authenticated handler and plan path.
    let visible = list_agents_sync_for_principal(&state, &principal, &ListAgentsQuery::default())
        .unwrap_or_else(|e| panic!("{}", e.message));
    assert!(
        visible
            .0
            .agents
            .iter()
            .all(|a| a.name != root.name || a.instructions.is_none())
    );
    assert!(can_actor_message_agent(&state.store, &actor, &root).is_ok());
    state.store.delete_agent(&child.id).unwrap();
    let denied = list_agents_sync_for_principal(&state, &principal, &ListAgentsQuery::default())
        .expect_err("deleted caller must not receive any roster");
    assert_eq!(denied.status, StatusCode::FORBIDDEN);
    let denied = send_sync(&state.store, &actor, body.clone(), plan, None)
        .expect_err("deleted principal must not deliver");
    assert_eq!(denied.status, StatusCode::FORBIDDEN);
    assert!(state.store.inbox(&root.id, None).unwrap().is_empty());
    assert!(plan_send(&state.store, &principal, &actor, &body).is_err());
    // Reusing the mutable display name does not restore the old principal.
    state
        .store
        .create_agent(anyagents_core::NewAgent::new(&child.name, "opencode"))
        .unwrap();
    let denied = plan_send(&state.store, &principal, &actor, &body)
        .expect_err("name reuse must not authorize old credentials");
    assert_eq!(denied.status, StatusCode::FORBIDDEN);
    let operator =
        list_agents_sync_for_principal(&state, &Principal::Operator, &ListAgentsQuery::default())
            .unwrap_or_else(|e| panic!("{}", e.message));
    let root_view = operator
        .0
        .agents
        .iter()
        .find(|a| a.name == root.name)
        .unwrap();
    let expected = format!(
        "{}\n\nprivate instruction sentinel",
        create::BUILTIN_GENERAL_ROLE_BODY
    );
    assert_eq!(root_view.instructions.as_deref(), Some(expected.as_str()));
}

#[test]
fn renamed_authenticated_sender_does_not_acquire_reused_name_identity() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let original = store
        .create_agent(anyagents_core::NewAgent::new("original", "opencode"))
        .unwrap();
    let principal = Principal::Agent {
        agent_id: original.id.clone(),
        name: original.name.clone(),
    };
    let actor = Actor::Agent(original.name.clone());
    store.rename_agent(&original.id, "renamed").unwrap();
    let replacement = store
        .create_agent(anyagents_core::NewAgent::new("original", "opencode"))
        .unwrap();
    let receiver = store
        .create_agent(anyagents_core::NewAgent::new("recipient", "opencode"))
        .unwrap();
    let body = SendRequest {
        to: vec![receiver.name.clone()],
        text: "identity sentinel".into(),
        ..Default::default()
    };
    let plan = plan_send(&store, &principal, &actor, &body)
        .unwrap_or_else(|error| panic!("{}", error.message));
    assert_eq!(plan.sender_id.as_ref(), Some(&original.id));
    assert_ne!(plan.sender_id.as_ref(), Some(&replacement.id));
    let _ = send_sync(&store, &actor, body, plan, None)
        .unwrap_or_else(|error| panic!("{}", error.message));
    let received = store.inbox(&receiver.id, None).unwrap();
    assert_eq!(
        received[0].sender.as_deref(),
        Some(format!("agent:{}", original.id).as_str())
    );
}

#[cfg(target_os = "linux")]
#[test]
fn local_sender_identity_survives_rename_and_refuses_deletion_while_journal_waits() {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    use std::time::{Duration, Instant};
    for delete in [false, true] {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let original = store
            .create_agent(anyagents_core::NewAgent::new("author", "opencode"))
            .unwrap();
        store
            .set_agent_role(&original.id, Some("admitted role"))
            .unwrap();
        let mut child = anyagents_core::NewAgent::new("recipient", "opencode");
        child.parent_id = Some(original.id.clone());
        let recipient = store.create_agent(child).unwrap();
        let actor = Actor::Agent(original.name.clone());
        let body = SendRequest {
            to: vec![recipient.name.clone()],
            text: "queued identity sentinel".into(),
            ..Default::default()
        };
        let plan =
            SendPlan::parse(&store, &actor, &body).unwrap_or_else(|e| panic!("{}", e.message));
        let key = "identity-lock-test";
        let filename: String = format!("{key}:0")
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let journal = dir.path().join("outbound-history");
        std::fs::create_dir_all(&journal).unwrap();
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(journal.join(format!("{filename}.lock")))
            .unwrap();
        lock.lock().unwrap();
        let inode = lock.metadata().unwrap().ino();
        std::thread::scope(|scope| {
            let sending = scope.spawn(|| {
                send_sync(&store, &actor, body, plan, Some(key.into())).map_err(|e| e.message)
            });
            let deadline = Instant::now() + Duration::from_secs(10);
            let waiting = loop {
                let waiting = std::fs::read_to_string("/proc/locks")
                    .unwrap()
                    .lines()
                    .any(|line| {
                        line.contains("-> FLOCK")
                            && line.contains("WRITE")
                            && line
                                .split_whitespace()
                                .any(|field| field.ends_with(&format!(":{inode}")))
                    });
                if waiting || Instant::now() >= deadline {
                    break waiting;
                }
                std::thread::sleep(Duration::from_millis(5));
            };
            // Release even on a failed observation, so the fixture cannot deadlock.
            if waiting {
                if delete {
                    // Preserve the recipient while removing its parent; the
                    // production delete path separately refuses live children.
                    store.set_agent_parent(&recipient.id, None).unwrap();
                    store.delete_agent(&original.id).unwrap();
                } else {
                    store.rename_agent(&original.id, "renamed").unwrap();
                }
                let replacement = store
                    .create_agent(anyagents_core::NewAgent::new("author", "opencode"))
                    .unwrap();
                store
                    .set_agent_role(&replacement.id, Some("wrong role"))
                    .unwrap();
            }
            drop(lock);
            let result = sending.join().unwrap();
            if delete {
                let error = result.expect_err("deleted admitted author cannot publish");
                assert!(error.contains("agent not found"), "{error}");
            } else {
                let _ = result.unwrap();
            }
            assert!(waiting, "actual journal-lock waiter must be observed");
        });
        let inbox = store.inbox(&recipient.id, None).unwrap();
        if delete {
            assert!(inbox.is_empty());
            continue;
        }
        assert_eq!(inbox.len(), 1);
        assert_eq!(
            inbox[0].sender.as_deref(),
            Some(format!("agent:{}", original.id).as_str())
        );
        assert_eq!(
            inbox[0].context.sender_role.as_deref(),
            Some("admitted role")
        );
        assert_eq!(inbox[0].context.sender_rel.as_deref(), Some("parent"));
    }
}

#[test]
fn keyed_local_fanout_recovers_partial_delivery_without_duplicates() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let sender = store
        .create_agent(anyagents_core::NewAgent::new("sender", "opencode"))
        .unwrap();
    let first = store
        .create_agent(anyagents_core::NewAgent::new("first", "opencode"))
        .unwrap();
    let second = store
        .create_agent(anyagents_core::NewAgent::new("second", "opencode"))
        .unwrap();
    for agent in [&sender, &first, &second] {
        store.set_agent_state(&agent.id, "running").unwrap();
        store.join_group("room", &agent.id).unwrap();
    }
    let db = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
    db.execute_batch(&format!("CREATE TRIGGER fail_recipient BEFORE INSERT ON messages WHEN NEW.agent_id = '{}' BEGIN SELECT RAISE(ABORT, 'injected second recipient failure'); END;", second.id)).unwrap();
    let actor = Actor::Agent(sender.name.clone());
    let body = SendRequest {
        sender: Some(actor.as_address()),
        to: vec!["group:room".into()],
        text: "fanout sentinel".into(),
        ..Default::default()
    };
    let plan =
        SendPlan::parse(&store, &actor, &body).unwrap_or_else(|error| panic!("{}", error.message));
    assert!(
        send_sync(
            &store,
            &actor,
            body.clone(),
            plan,
            Some("fanout-call".into())
        )
        .is_err()
    );
    assert_eq!(store.inbox(&first.id, None).unwrap().len(), 1);
    assert!(store.inbox(&second.id, None).unwrap().is_empty());
    db.execute_batch("DROP TRIGGER fail_recipient;").unwrap();
    let plan =
        SendPlan::parse(&store, &actor, &body).unwrap_or_else(|error| panic!("{}", error.message));
    let _ = send_sync(&store, &actor, body, plan, Some("fanout-call".into()))
        .unwrap_or_else(|error| panic!("{}", error.message));
    assert_eq!(store.inbox(&first.id, None).unwrap().len(), 1);
    assert_eq!(store.inbox(&second.id, None).unwrap().len(), 1);
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM outbound_messages", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn missing_sender_cannot_bypass_private_room_checks() {
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let sender = store
        .create_agent(anyagents_core::NewAgent::new("sender", "opencode"))
        .unwrap();
    let recipient = store
        .create_agent(anyagents_core::NewAgent::new("recipient", "opencode"))
        .unwrap();
    store.join_group("private-room", &recipient.id).unwrap();
    let actor = Actor::Agent(sender.name.clone());
    let body = SendRequest {
        to: vec!["group:private-room".into()],
        text: "blocked".into(),
        ..Default::default()
    };
    let plan = SendPlan::parse(&store, &actor, &body).unwrap_or_else(|e| panic!("{}", e.message));
    store.delete_agent(&sender.id).unwrap();
    // A replacement with the same name must not acquire the old plan.
    store
        .create_agent(anyagents_core::NewAgent::new(&sender.name, "opencode"))
        .unwrap();
    let denied = check_targets(&store, &actor, &body, &plan)
        .expect_err("missing actor must not pass room authorization");
    assert_eq!(denied.status, StatusCode::FORBIDDEN);
    assert!(store.inbox(&recipient.id, None).unwrap().is_empty());
}

#[test]
fn reset_session_requires_management_authority_and_quiescence_before_any_pin_change() {
    use anyagents_core::host_session;
    let dir = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("reset-parent", "opencode"))
        .unwrap();
    let mut new_child = anyagents_core::NewAgent::new("reset-child", "opencode");
    new_child.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new_child).unwrap();
    let peer = store
        .create_agent(anyagents_core::NewAgent::new("reset-peer", "opencode"))
        .unwrap();
    store.set_agent_state(&child.id, "paused").unwrap();
    let root = dir.path().join("agents").join(child.id.as_str());
    std::fs::create_dir_all(root.join("config")).unwrap();
    std::fs::create_dir_all(root.join("memory")).unwrap();
    std::fs::write(root.join("memory/MEMORY.md"), "before reset").unwrap();
    let old = host_session::ensure_active(&root, "opencode").unwrap();
    std::fs::write(root.join("config/opencode-session-id"), "old-id\n").unwrap();
    std::fs::write(root.join("config/raw.jsonl"), "raw retained").unwrap();
    std::fs::write(root.join("memory/MEMORY.md"), "after reset").unwrap();
    let before = std::fs::read(root.join("sessions/registry.json")).unwrap();
    let pins = || std::fs::read_dir(root.join("sessions")).unwrap().count();
    let before_pins = pins();
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let principal = |a: &Agent| Principal::Agent {
            agent_id: a.id.clone(),
            name: a.name.clone(),
        };
        let unchanged = || {
            assert_eq!(
                std::fs::read(root.join("sessions/registry.json")).unwrap(),
                before
            );
            assert_eq!(
                std::fs::read(root.join("config/opencode-session-id")).unwrap(),
                b"old-id\n"
            );
            assert_eq!(pins(), before_pins);
        };
        let denied = reset_session_route(
            principal(&peer),
            State(state.clone()),
            AxumPath(child.name.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(denied.status, StatusCode::FORBIDDEN);
        unchanged();
        let process = store.try_lock_agent(&child.id).unwrap();
        let busy = reset_session_route(
            principal(&parent),
            State(state.clone()),
            AxumPath(child.name.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(busy.status, StatusCode::CONFLICT);
        unchanged();
        drop(process);
        let lifecycle = store.lock_agent_lifecycle(&child.id).unwrap();
        let busy = reset_session_route(
            principal(&parent),
            State(state.clone()),
            AxumPath(child.name.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(busy.status, StatusCode::CONFLICT);
        unchanged();
        drop(lifecycle);
        store.set_agent_state(&child.id, "running").unwrap();
        let busy = reset_session_route(
            principal(&parent),
            State(state.clone()),
            AxumPath(child.name.clone()),
        )
        .await
        .unwrap_err();
        assert_eq!(busy.status, StatusCode::CONFLICT);
        unchanged();
        store.set_agent_state(&child.id, "paused").unwrap();
        let Json(result) = reset_session_route(
            principal(&parent),
            State(state),
            AxumPath(child.name.clone()),
        )
        .await
        .unwrap_or_else(|e| panic!("{}", e.message));
        let active = host_session::active(&root).unwrap().unwrap();
        assert_eq!(result["generation"], active.generation);
        assert_ne!(active.generation, old.generation);
        assert!(active.memory.section.contains("after reset"));
        assert_eq!(
            std::fs::read(root.join("config/raw.jsonl")).unwrap(),
            b"raw retained"
        );
        assert_eq!(
            host_session::resume_vendor(&root, "opencode", "old-id").unwrap(),
            old
        );
    });
}

/// A search over more new history than one request may index answers
/// at once from what it indexed, says the rest is pending, and queues it;
/// after the background indexer runs, the same history is found.
#[test]
fn a_search_past_its_index_budget_answers_partial_and_the_background_completes_it() {
    let dir = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("large-history", "spiron"))
        .unwrap();
    let path = dir
        .path()
        .join("agents")
        .join(agent.id.as_str())
        .join("transcripts/history.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut text = String::from("{\"role\":\"user\",\"content\":\"earlyneedle\"}\n");
    let filler = format!(
        "{{\"role\":\"assistant\",\"content\":\"{}\"}}\n",
        "filler ".repeat(36)
    );
    while (text.len() as u64) < anyagents_core::host_retrieval::REQUEST_INDEX_BYTES * 3 / 2 {
        text.push_str(&filler);
    }
    text.push_str("{\"role\":\"user\",\"content\":\"lateneedle\"}\n");
    std::fs::write(&path, &text).unwrap();
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let search = |term: &str| {
        runtime.block_on(async {
            let principal = Principal::Agent {
                agent_id: agent.id.clone(),
                name: agent.name.clone(),
            };
            let Json(response) = conversation_route(
                principal,
                State(state.clone()),
                AxumPath(agent.name.clone()),
                Query(
                    serde_json::from_value(serde_json::json!({"scope":"transcripts","query":term}))
                        .unwrap(),
                ),
            )
            .await
            .unwrap_or_else(|e| panic!("{}", e.message));
            response
        })
    };
    // The first answer comes from the first 4 MiB: the late record is not
    // indexed yet, and the response says so.
    let first = search("lateneedle");
    assert!(first.turns.iter().all(|t| !t.text.contains("lateneedle")));
    let retrieval = first.retrieval.clone().unwrap();
    let pending = retrieval.pending.clone().expect("a partial search says so");
    assert_eq!(pending.sources, 1);
    assert!(pending.bytes > 0);
    assert!(
        retrieval.degraded.iter().any(|d| d.starts_with("partial:")),
        "{retrieval:?}"
    );
    assert_eq!(store.host_history_backlog(), 1);
    while store.host_history_backlog() > 0 {
        store.index_host_history_step(u64::MAX).unwrap();
    }
    let done = search("lateneedle");
    assert!(done.turns.iter().any(|t| t.text.contains("lateneedle")));
    assert!(done.retrieval.and_then(|r| r.pending).is_none());
    assert!(
        search("earlyneedle")
            .turns
            .iter()
            .any(|t| t.text.contains("earlyneedle"))
    );
}

#[test]
fn hybrid_search_fuses_cited_dense_ranges_reranks_and_never_widens_access() {
    use agent_memory::protocol::{self, DenseHit, Reply, Request, Status};
    use std::io::{BufRead, Write as _};
    let dir = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("hybrid-parent", "spiron"))
        .unwrap();
    let peer = store
        .create_agent(anyagents_core::NewAgent::new("hybrid-peer", "spiron"))
        .unwrap();
    let lexical = "{\"role\":\"user\",\"content\":\"lexicalneedle about deploys\"}\n";
    let semantic = "{\"role\":\"assistant\",\"content\":\"完全唔同嘅講法 semantic target\"}\n";
    let write = |agent: &Agent, text: &str| {
        let path = dir
            .path()
            .join("agents")
            .join(agent.id.as_str())
            .join("transcripts/history.jsonl");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, text).unwrap();
        path
    };
    let parent_file = write(&parent, &format!("{lexical}{semantic}"));
    let peer_file = write(&peer, "{\"content\":\"peer secret target\"}\n");
    let doc = |agent: &Agent, path: &std::path::Path, seq: u64| {
        format!(
            "transcript:{}:{}:{seq}",
            agent.id,
            anyagents_core::learned_memory::sha256(path.as_os_str().as_encoded_bytes())
        )
    };
    let cite = |document_id: String, agent: &Agent, offset: usize, text: &str| DenseHit {
        document_id,
        agent_id: agent.id.to_string(),
        offset: offset as u64,
        length: text.len() as u64,
        sha256: anyagents_core::learned_memory::sha256(text.as_bytes()),
        score: 0.9,
    };
    let target = semantic.find("semantic target").unwrap() + lexical.len();
    let hits = vec![
        cite(
            doc(&parent, &parent_file, 2),
            &parent,
            target,
            "semantic target",
        ),
        // Another owner's document, a range outside its document and a
        // range whose bytes do not match: none may surface.
        cite(doc(&peer, &peer_file, 1), &peer, 13, "peer secret"),
        cite(doc(&parent, &parent_file, 1), &parent, 0, &"x".repeat(9999)),
        DenseHit {
            sha256: "0".repeat(64),
            ..cite(doc(&parent, &parent_file, 1), &parent, 1, "\"role")
        },
    ];
    let listener =
        std::os::unix::net::UnixListener::bind(protocol::socket_path(dir.path())).unwrap();
    let seen = Arc::new(Mutex::new(Vec::<Request>::new()));
    let recorded = Arc::clone(&seen);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let stream = stream.unwrap();
            let mut line = String::new();
            std::io::BufReader::new(&stream)
                .read_line(&mut line)
                .unwrap();
            let request = protocol::decode_request(line.trim()).unwrap();
            recorded.lock().unwrap().push(request.clone());
            let reply = match request {
                Request::Search { .. } => Reply::Search {
                    hits: hits.clone(),
                    status: Status::default(),
                },
                // The cross-encoder prefers the semantic answer.
                Request::Rerank { passages, .. } => Reply::Rerank {
                    scores: passages
                        .iter()
                        .map(|p| {
                            if p.contains("semantic target") {
                                5.0
                            } else {
                                -1.0
                            }
                        })
                        .collect(),
                },
                Request::Status => Reply::Status {
                    status: Status::default(),
                },
            };
            (&stream)
                .write_all(protocol::encode_reply(&Ok(reply)).as_bytes())
                .unwrap();
        }
    });
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let principal = Principal::Agent {
            agent_id: parent.id.clone(),
            name: parent.name.clone(),
        };
        let Json(response) = conversation_route(
            principal,
            State(state.clone()),
            AxumPath("all".into()),
            Query(
                serde_json::from_value(
                    serde_json::json!({"scope":"transcripts","query":"lexicalneedle"}),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("{}", e.message));
        let retrieval = response.retrieval.clone().unwrap();
        assert_eq!(
            retrieval.stages,
            vec!["lexical", "dense", "rerank"],
            "{retrieval:?}"
        );
        assert!(retrieval.degraded.is_empty());
        assert_eq!(
            response.turns.len(),
            2,
            "{:?}",
            response.turns.iter().map(|t| &t.text).collect::<Vec<_>>()
        );
        // Reranked first: the dense-only answer, cited to its exact bytes.
        let first = &response.turns[0];
        assert_eq!(first.text, "semantic target");
        let window = first.window.as_ref().unwrap();
        assert_eq!(
            (window.offset, window.length),
            (target as u64, "semantic target".len() as u64)
        );
        assert!(response.turns[1].text.contains("lexicalneedle"));
        assert!(
            response
                .turns
                .iter()
                .all(|t| !t.text.contains("peer secret"))
        );
    });
    let requests = seen.lock().unwrap();
    let Some(Request::Search { owners, .. }) = requests.first() else {
        panic!("no dense search: {requests:?}");
    };
    assert_eq!(
        owners,
        &vec![parent.id.to_string()],
        "only authorized owners reach the service"
    );
}

#[test]
fn canonical_raw_ranges_bind_owned_digest_and_return_bounded_exact_bytes() {
    let dir = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("range-parent", "spiron"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("range-child", "spiron");
    new.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new).unwrap();
    let peer = store
        .create_agent(anyagents_core::NewAgent::new("range-peer", "spiron"))
        .unwrap();
    let bytes: Vec<u8> = [
        b"rangeneedle ".as_slice(),
        &[0, 0xff, 0xfe],
        "繁體".as_bytes(),
        &vec![b'x'; 100_000],
    ]
    .concat();
    for a in [&parent, &child, &peer] {
        let root = dir.path().join("agents").join(a.id.as_str()).join("memory");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("MEMORY.md"), &bytes).unwrap();
    }
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let principal = || Principal::Agent { agent_id: parent.id.clone(), name: parent.name.clone() };
        let Json(all) = conversation_route(Principal::Operator, State(state.clone()), AxumPath("all".into()),
            Query(serde_json::from_value(serde_json::json!({"scope":"memory","query":"rangeneedle"})).unwrap())).await.unwrap_or_else(|e| panic!("{}", e.message));
        assert_eq!(all.turns.len(), 3);
        let find = |owner: &Agent| all.turns.iter().find(|t| t.raw.as_ref().unwrap().segment.contains(owner.id.as_str())).unwrap();
        let target = find(&child);
        let request = |id: &str, sha: &str, offset: u64, length: u64| serde_json::from_value(serde_json::json!({"scope":"memory","raw_id":id,"raw_sha256":sha,"raw_offset":offset,"raw_length":length})).unwrap();
        let Json(response) = conversation_route(principal(), State(state.clone()), AxumPath("all".into()),
            Query(request(&target.id, &target.raw.as_ref().unwrap().sha256, 11, 65536))).await.unwrap_or_else(|e| panic!("{}", e.message));
        let turn = &response.turns[0];
        assert_eq!(response.agent_id, child.id.as_str());
        assert_eq!(turn.raw_bytes.as_deref(), Some(&bytes[11..65547]));
        assert_eq!(turn.raw.as_ref().unwrap().length, bytes.len() as u64);
        assert_eq!(turn.window.as_ref().unwrap().length, 65536);
        assert_eq!(turn.window.as_ref().unwrap().sha256, anyagents_core::learned_memory::sha256(&bytes[11..65547]));
        assert!(turn.text.contains('�'), "text is explicitly a lossy preview");
        let peer_hit = find(&peer);
        // Make the hidden file fail if opened. Its ID must still be refused
        // as not found, not reach descriptor validation or reveal the path.
        std::fs::hard_link(&peer_hit.raw.as_ref().unwrap().segment, dir.path().join("hidden-hardlink")).unwrap();
        let denied = conversation_route(principal(), State(state.clone()), AxumPath("all".into()),
            Query(request(&peer_hit.id, &peer_hit.raw.as_ref().unwrap().sha256, 0, 32))).await.unwrap_err();
        assert_eq!(denied.status, StatusCode::NOT_FOUND);
        let denied = conversation_route(principal(), State(state.clone()), AxumPath(peer.name.clone()),
            Query(request(&peer_hit.id, &peer_hit.raw.as_ref().unwrap().sha256, 0, 32))).await.unwrap_err();
        assert_eq!(denied.status, StatusCode::FORBIDDEN);
        let denied = conversation_route(principal(), State(state.clone()), AxumPath(parent.name.clone()),
            Query(request(&target.id, &target.raw.as_ref().unwrap().sha256, 0, 32))).await.unwrap_err();
        assert_eq!(denied.status, StatusCode::NOT_FOUND, "explicit owner selector must bind the document");
        for (offset,length) in [(0,0),(0,65537),(u64::MAX,1),(bytes.len() as u64+1,1)] {
            let error = conversation_route(principal(), State(state.clone()), AxumPath("all".into()),
                Query(request(&target.id, &target.raw.as_ref().unwrap().sha256, offset, length))).await.unwrap_err();
            assert_eq!(error.status, StatusCode::BAD_REQUEST);
        }
        let Json(tail) = conversation_route(principal(), State(state.clone()), AxumPath("all".into()),
            Query(request(&target.id, &target.raw.as_ref().unwrap().sha256, bytes.len() as u64-5, 32))).await.unwrap_or_else(|e| panic!("{}", e.message));
        assert_eq!(tail.turns[0].raw_bytes.as_deref(), Some(&bytes[bytes.len()-5..]));
        let error = conversation_route(principal(), State(state.clone()), AxumPath("all".into()),
            Query(request(&target.id, &"0".repeat(64), 0, 32))).await.unwrap_err();
        assert_eq!(error.status, StatusCode::CONFLICT);
        let error = conversation_route(principal(), State(state.clone()), AxumPath("all".into()),
            Query(request(&target.raw.as_ref().unwrap().segment, &target.raw.as_ref().unwrap().sha256, 0, 32))).await.unwrap_err();
        assert_eq!(error.status, StatusCode::NOT_FOUND, "a caller path is not a document ID");
        let mut edited = bytes.clone(); edited[99_000] = b'y';
        std::fs::write(&target.raw.as_ref().unwrap().segment, edited).unwrap();
        let error = conversation_route(principal(), State(state), AxumPath("all".into()),
            Query(request(&target.id, &target.raw.as_ref().unwrap().sha256, 0, 32))).await.unwrap_err();
        assert_eq!(error.status, StatusCode::CONFLICT, "mutation outside requested range must fail whole-event SHA");
    });
}

#[test]
fn canonical_conversation_scopes_files_under_real_ancestor_authority() {
    let dir = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("history-parent", "spiron"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("history-child", "spiron");
    new.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new).unwrap();
    let peer = store
        .create_agent(anyagents_core::NewAgent::new("history-peer", "spiron"))
        .unwrap();
    for a in [&parent, &child, &peer] {
        let root = dir.path().join("agents").join(a.id.as_str());
        std::fs::create_dir_all(root.join("memory")).unwrap();
        std::fs::create_dir_all(root.join("config")).unwrap();
        std::fs::write(
            root.join("memory/MEMORY.md"),
            format!("{} canonicalneedle {}", "padding ".repeat(1700), a.name),
        )
        .unwrap();
        std::fs::write(root.join("config/sylphx-transcript.jsonl"),serde_json::to_string(&serde_json::json!({"role":"tool","name":"browser","timestamp_ms":100,"content":format!("canonicalneedle {}",a.name)})).unwrap()+"\n").unwrap();
    }
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let principal=||Principal::Agent {agent_id:parent.id.clone(),name:parent.name.clone()};
        for (scope,count) in [("all",4),("memory",2),("transcripts",2)] {
            let query=serde_json::from_value(serde_json::json!({"scope":scope,"query":"canonicalneedle"})).unwrap();
            let Json(response)=conversation_route(principal(),State(state.clone()),AxumPath("all".into()),Query(query)).await.unwrap_or_else(|e|panic!("{}",e.message));
            assert_eq!(response.turns.len(),count,"{scope}");assert!(response.turns.iter().all(|t|!t.text.contains("history-peer")));
            assert!(response.turns.iter().all(|t|t.raw.is_some() && t.window.is_some()));
            for t in &response.turns {let raw=t.raw.as_ref().unwrap();let bytes=std::fs::read(&raw.segment).unwrap();assert_eq!(anyagents_core::learned_memory::sha256(&bytes[raw.offset as usize..(raw.offset+raw.length) as usize]),raw.sha256);}
        }
        // Torn external strings must not prevent the canonical selector
        // from reading valid neighbors, including large escaped payloads.
        let native = dir.path().join("agents").join(child.id.as_str()).join("config/sylphx-transcript.jsonl");
        let payload = "largevalidneedle \" 中文 ".repeat(1000);
        let valid = serde_json::to_string(&serde_json::json!({"role":"assistant","direction":"out","content":payload})).unwrap();
        assert!(valid.len() > 4096);
        assert_eq!(serde_json::from_str::<serde_json::Value>(&valid).unwrap()["content"], payload);
        std::fs::OpenOptions::new().append(true).open(&native).unwrap().write_all(
            format!("{{\"content\":\"{}\n{valid}\n", "torntoken ".repeat(1000)).as_bytes()
        ).unwrap();
        let Json(large)=conversation_route(principal(),State(state.clone()),AxumPath(child.name.clone()),Query(serde_json::from_value(serde_json::json!({"scope":"transcripts","query":"largevalidneedle","direction":"out","limit":6})).unwrap())).await.unwrap_or_else(|e|panic!("{}", e.message));
        assert_eq!(large.turns.len(), 1);
        let raw = large.turns[0].raw.as_ref().unwrap();
        let bytes = std::fs::read(&raw.segment).unwrap();
        let record = &bytes[raw.offset as usize..(raw.offset+raw.length) as usize];
        assert_eq!(serde_json::from_slice::<serde_json::Value>(record).unwrap()["content"], payload);
        // Native records lacking time must remain null at the canonical
        // surface, never inherit a recent filesystem timestamp.
        let native = dir.path().join("agents").join(child.id.as_str()).join("config/sylphx-transcript.jsonl");
        use std::io::Write;
        std::fs::OpenOptions::new().append(true).open(native).unwrap().write_all(b"{\"role\":\"user\",\"content\":\"untimedcanonical\"}\n").unwrap();
        let Json(untimed)=conversation_route(principal(),State(state.clone()),AxumPath(child.name.clone()),Query(serde_json::from_value(serde_json::json!({"scope":"transcripts","query":"untimedcanonical"})).unwrap())).await.unwrap_or_else(|e|panic!("{}", e.message));
        assert_eq!(untimed.turns.len(), 1);
        assert!(untimed.turns[0].timestamp_ms.is_none());
        assert!(serde_json::to_value(&untimed).unwrap()["turns"][0]["timestamp_ms"].is_null());
        // Source-shaped synthetic Claude blocks, exercised through the
        // canonical selector rather than only direct index queries.
        let native = dir.path().join("agents").join(child.id.as_str()).join("config/sylphx-transcript.jsonl");
        std::fs::OpenOptions::new().append(true).open(&native).unwrap().write_all(
            "{\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"canon-shell\",\"name\":\"shell\",\"input\":{}},{\"type\":\"tool_use\",\"id\":\"canon-files\",\"name\":\"files\",\"input\":{}}]}}\n"
        .as_bytes()).unwrap();
        let Json(multi)=conversation_route(principal(),State(state.clone()),AxumPath(child.name.clone()),Query(serde_json::from_value(serde_json::json!({"scope":"transcripts","tool_name":"files","include_tools":false})).unwrap())).await.unwrap_or_else(|e|panic!("{}", e.message));
        assert_eq!(multi.turns.len(), 1);
        assert_eq!(multi.turns[0].tool_names, ["files", "shell"]);
        assert_eq!(serde_json::to_value(&multi).unwrap()["turns"][0]["tool_names"], serde_json::json!(["files", "shell"]));
        std::fs::OpenOptions::new().append(true).open(native).unwrap().write_all(b"{\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"tool_result\",\"tool_use_id\":\"canon-shell\",\"content\":\"canonicaltoolresult\"}]}}\n").unwrap();
        let Json(result)=conversation_route(principal(),State(state.clone()),AxumPath(child.name.clone()),Query(serde_json::from_value(serde_json::json!({"scope":"transcripts","tool_name":"shell","query":"canonicaltoolresult"})).unwrap())).await.unwrap_or_else(|e|panic!("{}", e.message));
        assert_eq!(result.turns.len(), 1);
        assert_eq!(result.turns[0].tool_names, ["shell"]);
        let denied=conversation_route(principal(),State(state.clone()),AxumPath(peer.name.clone()),Query(serde_json::from_value(serde_json::json!({"scope":"memory"})).unwrap())).await.unwrap_err();assert_eq!(denied.status,StatusCode::FORBIDDEN);
        let query=serde_json::from_value(serde_json::json!({"scope":"all","only_tools":true,"tool_name":"browser","from_seq":1,"to_seq":1})).unwrap();
        let Json(response)=conversation_route(principal(),State(state),AxumPath("all".into()),Query(query)).await.unwrap_or_else(|e|panic!("{}",e.message));assert_eq!(response.turns.len(),2);assert!(response.turns.iter().all(|t|t.tool_name.as_deref()==Some("browser")));
    });
}

/// Answers one sign-in with two lines and records where it ran.
struct Scripted {
    dir: Arc<Mutex<Option<PathBuf>>>,
}

impl CommandRunner for Scripted {
    fn run(
        &mut self,
        _program: &str,
        _args: &[String],
        dir: &Path,
        env: &[(&str, &str)],
        on_line: &mut dyn LineSink,
    ) -> Result<(), InstallError> {
        *self.dir.lock().unwrap() = Some(dir.to_path_buf());
        assert!(env.iter().any(|(key, _)| *key == "CODEX_HOME"));
        on_line.line("LINK https://auth.example/device");
        on_line.line("SIGNED_IN ada@example");
        Ok(())
    }
}

/// These tests start a server in this process, so the environment they set
/// is read under this lock and put back before the next one runs.
static ENV: Mutex<()> = Mutex::new(());

/// One lock for every test that sets this process's environment, so a
/// server started by one never reads another's settings.
pub(crate) fn env_lock() -> MutexGuard<'static, ()> {
    ENV.lock().unwrap_or_else(|err| err.into_inner())
}

/// A send's local media is read with the sending agent's own access,
/// once, before any Telegram post: the operator token, a link to it and
/// a path outside the agent's directories are refused; the agent's own
/// file is read, including internal-only sends; URLs and file IDs read
/// nothing. The operator reads with its own access.
#[test]
fn send_media_is_read_with_the_senders_access_never_the_servers() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("ada", "opencode"))
        .unwrap();
    let secrets = data.join("secrets");
    std::fs::create_dir_all(&secrets).unwrap();
    let token = secrets.join("operator-token");
    std::fs::write(&token, b"operator-secret").unwrap();
    let work = anyagents_core::agent_work_dir(data, &agent.id);
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(work.join("photo.png"), b"png").unwrap();
    std::os::unix::fs::symlink(&token, work.join("photo-link.png")).unwrap();
    store
        .create_agent(anyagents_core::NewAgent::new("bob", "opencode"))
        .unwrap();
    let plan = |principal: Option<&anyagents_core::AgentId>, to: &str| {
        let request = SendRequest {
            to: vec![to.into()],
            text: "x".into(),
            ..Default::default()
        };
        let mut plan = SendPlan::parse(&store, &Actor::Agent("ada".into()), &request)
            .unwrap_or_else(|error| panic!("{}", error.message));
        plan.principal_id = principal.cloned();
        plan
    };
    let body = |source: &str| SendRequest {
        to: vec!["telegram:chat:100".into()],
        media: Some(crate::api::SendMedia {
            kind: "photo".into(),
            source: source.into(),
            title: None,
            performer: None,
        }),
        ..Default::default()
    };
    let as_agent = plan(Some(&agent.id), "telegram:chat:100");
    for refused in [
        token.clone(),
        work.join("photo-link.png"),
        work.join("../../../secrets/operator-token"),
        data.join("state.db"),
    ] {
        let error = media_upload(&store, &body(refused.to_str().unwrap()), &as_agent)
            .err()
            .unwrap_or_else(|| panic!("{} must be refused", refused.display()));
        assert_eq!(error.status, StatusCode::FORBIDDEN, "{}", error.message);
        assert!(!error.message.contains("operator-secret"));
    }
    let relative = media_upload(&store, &body("./photo.png"), &as_agent)
        .err()
        .unwrap();
    assert!(
        relative.message.contains("absolute"),
        "{}",
        relative.message
    );
    let own = media_upload(
        &store,
        &body(work.join("photo.png").to_str().unwrap()),
        &as_agent,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        (own.file_name.as_str(), own.bytes.as_slice()),
        ("photo.png", &b"png"[..])
    );
    for reference in ["https://example.test/a.png", "AgACAgUAAxkBAAIB"] {
        assert!(
            media_upload(&store, &body(reference), &as_agent)
                .unwrap()
                .is_none()
        );
    }
    let internal = plan(Some(&agent.id), "agent:bob");
    assert!(media_upload(&store, &body(token.to_str().unwrap()), &internal).is_err());
    let operator = plan(None, "telegram:chat:100");
    let read = media_upload(&store, &body(token.to_str().unwrap()), &operator)
        .unwrap()
        .unwrap();
    assert_eq!(read.bytes, b"operator-secret");
}

/// Process-local environment locks do not isolate forked children or
/// process-global PATH. Keep one-tick lifecycle assertions in one worker.
fn single_test_worker(name: &str) -> bool {
    let name = format!("server::tests::{name}");
    if std::env::var("ANYAGENTS_SINGLE_TEST_WORKER").as_deref() == Ok(name.as_str()) {
        return false;
    }
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &name, "--nocapture"])
        .env("ANYAGENTS_SINGLE_TEST_WORKER", &name)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("1 passed; 0 failed;"),
        "{name} worker failed or did not execute: {stdout}{}",
        String::from_utf8_lossy(&out.stderr)
    );
    true
}

#[test]
fn an_interrupted_spawn_gets_its_prompt_once_at_the_next_start() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("child", "codex");
    new.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new).unwrap();
    let finish = SpawnFinish {
        workdir: Some("/allocated/worktree".into()),
        actor: "agent:parent".into(),
        caller: Some(parent.id.to_string()),
        scope: None,
        public: None,
        lifecycle: Some("task".into()),
        groups: Vec::new(),
        prompt: Some("build the thing".into()),
    };
    // The server stopped after admitting the child, before its prompt.
    let marker = spawn_finish_marker(data, &child.id);
    std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
    record_spawn_finish(&marker, &finish).unwrap();

    assert_eq!(finish_interrupted_spawns(&store, data), 1);
    assert!(!marker.exists());
    let inbox = store.inbox(&child.id, None).unwrap();
    assert_eq!(inbox.len(), 1);
    assert_eq!(inbox[0].text, "build the thing");
    assert_eq!(
        inbox[0].sender.as_deref(),
        Some(format!("agent:{}", parent.id).as_str())
    );
    assert_eq!(
        store.agent(&child.id).unwrap().lifecycle().to_string(),
        "task"
    );

    assert_eq!(
        store.agent(&child.id).unwrap().workdir.as_deref(),
        Some("/allocated/worktree")
    );
    // A later authoritative assignment must survive a stale finish record.
    store
        .set_agent_running(&child.id, "/current/worktree")
        .unwrap();
    // Stopped again after the prompt landed but before the record went:
    // the next start finishes without a second prompt.
    record_spawn_finish(&marker, &finish).unwrap();
    assert_eq!(finish_interrupted_spawns(&store, data), 1);
    assert_eq!(store.inbox(&child.id, None).unwrap().len(), 1);
    assert_eq!(finish_interrupted_spawns(&store, data), 0);
    assert_eq!(
        store.agent(&child.id).unwrap().workdir.as_deref(),
        Some("/current/worktree")
    );
    // Records written before workdir snapshots remain readable.
    let mut legacy = serde_json::to_value(&finish).unwrap();
    legacy.as_object_mut().unwrap().remove("workdir");
    let legacy: SpawnFinish = serde_json::from_value(legacy).unwrap();
    assert!(legacy.workdir.is_none());
    record_spawn_finish(&marker, &legacy).unwrap();
    assert_eq!(finish_interrupted_spawns(&store, data), 1);
    assert_eq!(
        store.agent(&child.id).unwrap().workdir.as_deref(),
        Some("/current/worktree")
    );
}

#[test]
fn an_interrupted_spawn_that_cannot_finish_keeps_its_record_for_the_next_start() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    let child = store
        .create_agent(anyagents_core::NewAgent::new("child", "codex"))
        .unwrap();
    let mut finish = SpawnFinish {
        workdir: None,
        actor: "operator".into(),
        caller: None,
        scope: None,
        public: None,
        // Refused by the store: this start cannot finish the spawn.
        lifecycle: Some("not-a-lifecycle".into()),
        groups: Vec::new(),
        prompt: Some("build the thing".into()),
    };
    let marker = spawn_finish_marker(data, &child.id);
    std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
    record_spawn_finish(&marker, &finish).unwrap();

    assert_eq!(finish_interrupted_spawns(&store, data), 0);
    assert!(
        marker.exists(),
        "a failed finish dropped the spawn's record"
    );
    assert!(store.inbox(&child.id, None).unwrap().is_empty());

    // The next start can: the prompt lands and only then the record goes.
    finish.lifecycle = Some("task".into());
    record_spawn_finish(&marker, &finish).unwrap();
    assert_eq!(finish_interrupted_spawns(&store, data), 1);
    assert!(!marker.exists());
    assert_eq!(store.inbox(&child.id, None).unwrap().len(), 1);
}

#[tokio::test]
async fn every_listener_stops_taking_calls_when_the_server_is_told_to_stop() {
    let (stopping, _) = tokio::sync::watch::channel(false);
    let socket = stopped(&stopping);
    let public = stopped(&stopping);
    tokio::pin!(socket, public);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), &mut socket)
            .await
            .is_err(),
        "stopped before the stop"
    );
    let _ = stopping.send(true);
    tokio::time::timeout(std::time::Duration::from_secs(1), socket)
        .await
        .expect("the socket listener kept taking calls");
    tokio::time::timeout(std::time::Duration::from_secs(1), public)
        .await
        .expect("a TCP listener kept taking calls");
    // A listener started after the stop stops at once; so does one whose
    // server is gone.
    tokio::time::timeout(std::time::Duration::from_secs(1), stopped(&stopping))
        .await
        .unwrap();
    let (gone, _) = tokio::sync::watch::channel(false);
    let orphan = stopped(&gone);
    drop(gone);
    tokio::time::timeout(std::time::Duration::from_secs(1), orphan)
        .await
        .unwrap();
}

/// A write arriving on either TCP listener (public or local webhook)
/// counts toward the stop until it is done, as on the socket.
#[test]
fn writes_on_the_tcp_listeners_hold_the_stop() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let state = AppState {
        store,
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        use tokio::io::AsyncWriteExt;
        for (name, router) in [
            ("public", build_public_router(state.clone())),
            ("webhook", build_webhook_router(state.clone())),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let served = tokio::spawn(async move {
                let _ = axum::serve(listener, router).await;
            });
            // A webhook whose body is still arriving: the call is running.
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(
                    b"POST /v1/webhooks/telegram/some-channel HTTP/1.1\r\nhost: x\r\n\
                      content-type: application/json\r\ncontent-length: 100\r\n\r\n{",
                )
                .await
                .unwrap();
            let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while in_flight::MUTATIONS.running() == 0 && std::time::Instant::now() < until {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            assert!(
                in_flight::MUTATIONS.running() > 0,
                "a write on the {name} listener does not hold the stop"
            );
            drop(stream);
            served.abort();
            in_flight::MUTATIONS
                .settle(std::time::Duration::from_secs(5))
                .await;
        }
    });
}

/// A streaming sign-in returns its response headers before the blocking
/// vendor command finishes, so its carried `in_flight` token must keep the
/// stop waiting until the blocking work and capability rotation return.
#[test]
fn sign_in_route_carries_in_flight_token_until_blocking_work_finishes() {
    struct HeldSignIn {
        entered: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    }
    impl CommandRunner for HeldSignIn {
        fn run(
            &mut self,
            _program: &str,
            _args: &[String],
            _dir: &Path,
            _env: &[(&str, &str)],
            on_line: &mut dyn LineSink,
        ) -> Result<(), InstallError> {
            let _ = self.entered.send(());
            let _ = self.release.recv();
            on_line.line("SIGNED_IN ada@example");
            Ok(())
        }
    }
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let state = AppState {
        store,
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(HeldSignIn {
            entered: entered_tx,
            release: release_rx,
        })),
        auth: auth::Mode::Confined {
            operator_digest: Some(auth::operator_digest("test-op-token")),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        use hyper::service::Service as _;
        let router = build_router(state);
        let service = hyper_util::service::TowerToHyperService::new(router);
        let req = hyper::Request::builder()
            .method("POST")
            .uri("http://localhost/v1/harnesses/codex/sign-in")
            .header(crate::operator::HEADER, "test-op-token")
            .body(http_body_util::Full::new(bytes::Bytes::new()))
            .unwrap();
        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        tokio::task::spawn_blocking(move || {
            entered_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("blocking sign-in started");
        })
        .await
        .unwrap();
        // Even after the handler future returned its streaming response
        // (and even if the caller drops the body), the blocking task still
        // holds its carried `in_flight` token until `sign_in` finishes.
        drop(response);
        assert!(
            in_flight::MUTATIONS.running() > 0,
            "streaming sign-in dropped its in_flight token before blocking work finished"
        );
        release_tx.send(()).unwrap();
        assert_eq!(
            in_flight::MUTATIONS
                .settle(std::time::Duration::from_secs(5))
                .await,
            0
        );
    });
}

/// An admitted HTTP MCP call runs against the server's in-process router
/// instead of dialing `anyagents.sock`: when `stopping` has already closed
/// the external socket listener (leaving a refusing socket file on disk),
/// an in-flight `POST /v1/webhooks/mcp` finishes at once instead of
/// self-deadlocking the mutation drain on `ECONNREFUSED`.
#[test]
fn http_mcp_post_completes_in_process_while_external_socket_refuses() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let cloud = state
        .store
        .create_agent(anyagents_core::NewAgent::new("cloud-drain", "claude-cloud"))
        .unwrap();
    let peer = state
        .store
        .create_agent(anyagents_core::NewAgent::new("peer-drain", "claude-code"))
        .unwrap();
    crate::remote::begin_session(&state.store, &cloud).unwrap();
    let cloud_cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &cloud.id);
    let remote = crate::remote::session_token(&cloud_cfg).unwrap();

    // Simulate shutdown after `stopping`: `anyagents.sock` stays on disk
    // while its external listener is closed (`ECONNREFUSED`).
    let sock = socket_path(dir.path());
    drop(std::os::unix::net::UnixListener::bind(&sock).unwrap());
    assert!(sock.exists());

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        use http_body_util::BodyExt as _;
        use hyper::service::Service as _;
        let public = build_public_router(state.clone());
        let service = hyper_util::service::TowerToHyperService::new(public);
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "send",
                "arguments": {
                    "to": ["peer-drain"],
                    "text": "draining cleanly"
                }
            }
        });
        let req = hyper::Request::builder()
            .method("POST")
            .uri("http://localhost/v1/webhooks/mcp")
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {remote}"),
            )
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(http_body_util::Full::new(bytes::Bytes::from(
                serde_json::to_vec(&payload).unwrap(),
            )))
            .unwrap();
        let started = std::time::Instant::now();
        let response = service.call(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "HTTP MCP call stalled on closed external socket: {:?}",
            started.elapsed()
        );
        let body: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_ne!(body["result"]["isError"], serde_json::json!(true), "{body}");
        let messages = state.store.inbox(&peer.id, None).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].text, "draining cleanly");
    });
}

#[test]
fn a_deleted_agents_name_frees_only_after_its_profile_and_config_are_gone() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("shopper", "codex"))
        .unwrap();
    let agent_dir = data.join("agents").join(agent.id.as_str());
    let profile = data.join("browser-profiles").join("shopper");
    for path in [
        agent_dir.join("config"),
        agent_dir.join("memory"),
        profile.join("user-data"),
    ] {
        std::fs::create_dir_all(&path).unwrap();
    }
    std::fs::write(profile.join("user-data").join("Cookies"), b"logged-in").unwrap();
    // The delete route removes these before the row; the row still holds
    // the name, so no agent can take it while they exist.
    remove_agent_identity_files(data, &agent, &agent_dir, None);
    assert!(!profile.exists());
    assert!(!agent_dir.join("config").exists());
    assert!(!agent_dir.join("memory").exists());
    assert!(
        store
            .create_agent(anyagents_core::NewAgent::new("shopper", "codex"))
            .is_err()
    );
    // Run again after a stop part way: nothing left, nothing fails.
    remove_agent_identity_files(data, &agent, &agent_dir, None);
    store.delete_agent(&agent.id).unwrap();
    let reused = store
        .create_agent(anyagents_core::NewAgent::new("shopper", "codex"))
        .unwrap();
    assert_ne!(reused.id, agent.id);
    assert!(!profile.join("user-data").join("Cookies").exists());
}

#[test]
fn describe_preserves_scope_and_audits_authorized_policy_updates() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("mind", "codex");
    new.parent_id = Some(parent.id.clone());
    new.role = Some("Mind".into());
    let mind = store.create_agent(new).unwrap();
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().to_path_buf(),
        },
    };
    let caller = |agent: &Agent| Principal::Agent {
        agent_id: agent.id.clone(),
        name: agent.name.clone(),
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut expected_audit = Vec::new();
        for (body, before, after, source) in [
            (serde_json::json!({"scope":"siblings"}), "private", "siblings", "scope"),
            (serde_json::json!({"description":"Public charter", "public":false}), "siblings", "siblings", "public"),
            (serde_json::json!({"instructions":"Private instructions"}), "siblings", "siblings", ""),
            (serde_json::json!({"role":"Updated Mind"}), "siblings", "siblings", ""),
            (serde_json::json!({"public":true}), "siblings", "public", "public"),
            (serde_json::json!({"public":false}), "public", "private", "public"),
            (serde_json::json!({"scope":"isolated", "public":true}), "private", "isolated", "scope"),
            (serde_json::json!({"public":false}), "isolated", "isolated", "public"),
            (serde_json::json!({"scope":"private"}), "isolated", "private", "scope"),
            (serde_json::json!({"public":false}), "private", "private", "public"),
            (serde_json::json!({"scope":"siblings"}), "private", "siblings", "scope"),
        ] {
            let Json(updated) = set_description_route(
                caller(&parent), State(state.clone()), AxumPath("mind".into()),
                Json(serde_json::from_value(body).unwrap()),
            ).await.unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
            assert_eq!(updated["scope"], after);
            if !source.is_empty() {
                expected_audit.push(("agent:parent".to_string(), before.to_string(), after.to_string(), source.to_string()));
            }
        }
        let before = store.agent(&mind.id).unwrap();
        let outsider = store.create_agent(anyagents_core::NewAgent::new("outsider", "codex")).unwrap();
        let mut sibling_new = anyagents_core::NewAgent::new("sibling", "codex");
        sibling_new.parent_id = Some(parent.id.clone());
        let sibling = store.create_agent(sibling_new).unwrap();
        for agent in [&outsider, &sibling] {
            for body in [serde_json::json!({"scope":"public"}), serde_json::json!({"description":"Overwrite", "instructions":"Overwrite"})] {
                let err = set_description_route(caller(agent), State(state.clone()), AxumPath("mind".into()), Json(serde_json::from_value(body).unwrap()))
                    .await.expect_err("unrelated caller must be refused");
                assert_eq!(err.status, StatusCode::FORBIDDEN);
                assert_eq!(store.agent(&mind.id).unwrap(), before);
            }
        }
        for self_caller in [&mind, &parent] {
            for body in [
                serde_json::json!({"scope":"public"}),
                serde_json::json!({"description":"Self-modified description"}),
                serde_json::json!({"instructions":"Self-modified instructions"}),
            ] {
                let err = set_description_route(caller(self_caller), State(state.clone()), AxumPath("self".into()), Json(serde_json::from_value(body).unwrap()))
                    .await.expect_err("non-admin self cannot mutate description, instructions, or scope");
                assert_eq!(err.status, StatusCode::FORBIDDEN);
            }
            let Json(read_self) = get_description_route(caller(self_caller), State(state.clone()), AxumPath("self".into()))
                .await.expect("self can read own description");
            assert_eq!(read_self["name"], self_caller.name);
        }
        assert_eq!(store.agent(&mind.id).unwrap(), before);

        // An admin's self update is allowed and has the same atomic audit.
        store.set_agent_admin(&parent.id, true).unwrap();
        let _ = set_description_route(caller(&parent), State(state.clone()), AxumPath("self".into()), Json(serde_json::from_value(serde_json::json!({"scope":"isolated", "description":"Admin self charter"})).unwrap()))
            .await.unwrap_or_else(|e| panic!("{}", e.message));
        let conn = rusqlite::Connection::open(dir.path().join("state.db")).unwrap();
        let mut stmt = conn.prepare("SELECT actor,before_scope,after_scope,source FROM admin_audit WHERE target='mind' AND source IS NOT NULL ORDER BY id").unwrap();
        let rows: Vec<(String,String,String,String)> = stmt.query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap().map(Result::unwrap).collect();
        assert_eq!(rows, expected_audit);
        let self_audit: (String,String,String,String) = conn.query_row("SELECT actor,before_scope,after_scope,source FROM admin_audit WHERE target='parent' AND source IS NOT NULL", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
        assert_eq!(self_audit, ("agent:parent".into(),"public".into(),"isolated".into(),"scope".into()));
        assert_eq!(store.agent(&mind.id).unwrap().description.as_deref(), Some("Public charter"));
        // Every runtime-config pass reads the store's policy; none owns it.
        for _ in 0..2 {
            let current = store.agent(&mind.id).unwrap();
            let config = anyagents_core::agent_config_dir(dir.path(), &mind.id);
            let work = anyagents_core::agent_work_dir(dir.path(), &mind.id);
            create::reconcile_agent_runtime_config(&store, dir.path(), &current, &config, &work).unwrap();
            let reloaded = Store::open(dir.path().join("state.db")).unwrap().agent(&mind.id).unwrap();
            assert_eq!(reloaded.scope(), "siblings");
            assert_eq!(reloaded.description, current.description);
        }
    });
}

#[test]
fn spawn_defaults_to_private_regardless_of_role() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().into(),
        },
    };
    std::fs::create_dir_all(dir.path().join("roles")).unwrap();
    std::fs::write(
        dir.path().join("roles/researcher.md"),
        "---\nname: researcher\ndescription: Research specialist\n---\nResearch deeply.\n",
    )
    .unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for (name, role, scope) in [("labelled", Some("Researcher"), "private"), ("task", None, "private")] {
            let body = serde_json::from_value(serde_json::json!({"name":name,"role":role,"harness":"codex","model":"test-model","workdir":"private"})).unwrap();
            let (_, Json(created)) = create_agent_route(Principal::Agent { agent_id: parent.id.clone(), name: parent.name.clone() }, State(state.clone()), axum::http::HeaderMap::new(), Json(body)).await.unwrap_or_else(|e| panic!("{}: {}",e.status,e.message));
            assert_eq!(created.scope, scope);
            let _ = set_description_route(Principal::Agent { agent_id: parent.id.clone(), name: parent.name.clone() }, State(state.clone()), AxumPath(name.into()), Json(serde_json::from_value(serde_json::json!({"public":true})).unwrap())).await.unwrap_or_else(|e| panic!("{}",e.message));
            let _ = set_description_route(Principal::Agent { agent_id: parent.id.clone(), name: parent.name.clone() }, State(state.clone()), AxumPath(name.into()), Json(serde_json::from_value(serde_json::json!({"public":false})).unwrap())).await.unwrap_or_else(|e| panic!("{}",e.message));
            assert_eq!(store.agent_by_name(name).unwrap().scope(),scope);
        }
    });
}

struct SingleUidEnvironment(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl SingleUidEnvironment {
    /// Call only while env_lock is held, before starting the fixture server.
    fn new() -> Self {
        let keys = [
            "ANYAGENTS_PRINCIPALS",
            "ANYAGENTS_PRINCIPAL_REGISTRY",
            "ANYAGENTS_AGENTS",
            "ANYAGENTS_AGENT_UID",
            "ANYAGENTS_AGENT_GID",
            "ANYAGENTS_SOCKET_GID",
        ];
        let saved = keys
            .iter()
            .map(|key| (*key, std::env::var_os(key)))
            .collect();
        for key in keys {
            // SAFETY: these server fixtures hold env_lock through shutdown.
            unsafe { std::env::remove_var(key) };
        }
        assert!(!crate::sandbox::seats_configured());
        assert!(crate::sandbox::launch_boundary(&[]));
        Self(saved)
    }
}

impl Drop for SingleUidEnvironment {
    fn drop(&mut self) {
        for (key, value) in &self.0 {
            // SAFETY: the fixture server has stopped and env_lock is held.
            unsafe {
                if let Some(value) = value {
                    std::env::set_var(key, value);
                } else {
                    std::env::remove_var(key);
                }
            }
        }
    }
}

#[test]
fn readiness_answers_while_the_once_only_image_hash_is_blocked() {
    if single_test_worker("readiness_answers_while_the_once_only_image_hash_is_blocked") {
        return;
    }
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};
    let _env = env_lock();
    let _single_uid = SingleUidEnvironment::new();
    let data = tempfile::tempdir().unwrap();
    let token = data.path().join("operator-token");
    std::fs::write(&token, "an-operator-token-0123456789abcdef\n").unwrap();
    let settings = [
        (
            "ANYAGENTS_OPERATOR_TOKEN_FILE",
            token.to_string_lossy().into_owned(),
        ),
        ("ANYAGENTS_FAKE_MODE", "1".into()),
    ];
    struct Restore(Vec<(&'static str, Option<std::ffi::OsString>)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            for (key, value) in &self.0 {
                // SAFETY: this fixture holds env_lock until its server stops.
                unsafe {
                    if let Some(value) = value {
                        std::env::set_var(key, value);
                    } else {
                        std::env::remove_var(key);
                    }
                }
            }
        }
    }
    let _restore = Restore(
        settings
            .iter()
            .map(|(key, _)| (*key, std::env::var_os(key)))
            .collect(),
    );
    for (key, value) in settings {
        // SAFETY: every server fixture uses env_lock.
        unsafe {
            std::env::set_var(key, value);
        }
    }
    let image = data.path().join("image");
    std::fs::write(&image, b"abc").unwrap();
    let (release, blocked) = std::sync::mpsc::channel();
    let (entered, running) = std::sync::mpsc::channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let identity = Arc::new(ServingIdentity::with_hasher(move || {
        use sha2::{Digest, Sha256};
        observed.fetch_add(1, Ordering::SeqCst);
        entered.send(()).unwrap();
        blocked.recv().unwrap();
        Ok(format!("{:x}", Sha256::digest(std::fs::read(image)?)))
    }));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let path = data.path().to_path_buf();
    let serving = Arc::clone(&identity);
    let server = std::thread::spawn(move || {
        run_with_identity(
            &path,
            Arc::new(Mutex::new(ProcessRunner)),
            stopped,
            serving,
            HarnessHost::InProcess,
        )
    });
    struct Stop(
        Option<tokio::sync::oneshot::Sender<()>>,
        Option<std::thread::JoinHandle<Result<(), Error>>>,
    );
    impl Drop for Stop {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
            self.1.take().unwrap().join().unwrap().unwrap();
        }
    }
    let _server = Stop(Some(stop), Some(server));
    let deadline = Instant::now() + Duration::from_secs(5);
    let socket = socket_path(data.path());
    loop {
        if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "readiness waited for the image hash"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    running.recv_timeout(Duration::from_secs(5)).unwrap();
    let health = || {
        let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        write!(
            stream,
            "GET /v1/webhooks/healthz HTTP/1.1\r\nHost: local\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        serde_json::from_str::<serde_json::Value>(response.split_once("\r\n\r\n").unwrap().1)
            .unwrap()
    };
    let computing = health();
    assert_eq!(computing["binary_sha256_state"], "computing");
    assert!(computing["binary_sha256"].is_null());
    assert_eq!(computing["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(computing["source_sha"], env!("ANYAGENTS_SOURCE_SHA"));
    identity.start_hash();
    identity.clone().start_hash();
    release.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let ready = health();
        if ready["binary_sha256_state"] == "ready" {
            // SHA-256 of the exact bytes written to the fixture image.
            assert_eq!(
                ready["binary_sha256"],
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            );
            break;
        }
        assert!(Instant::now() < deadline, "{ready}");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn a_sign_in_streams_the_link_before_the_outcome() {
    if single_test_worker("a_sign_in_streams_the_link_before_the_outcome") {
        return;
    }
    let _env = env_lock();
    let _single_uid = SingleUidEnvironment::new();
    let data = tempfile::tempdir().unwrap();
    let socket = socket_path(data.path());
    let dir = Arc::new(Mutex::new(None));
    let runner = Arc::new(Mutex::new(Scripted {
        dir: Arc::clone(&dir),
    }));
    let data_path = data.path().to_path_buf();
    // This deployment has an operator, so the sign-in presents one.
    let token_file = data.path().join("operator-token");
    std::fs::write(&token_file, "an-operator-token-0123456789abcdef\n").unwrap();
    // SAFETY: the test process sets these before the server thread reads
    // them, and sets them for its own duration only.
    unsafe {
        std::env::set_var("ANYAGENTS_OPERATOR_TOKEN_FILE", &token_file);
        // Authentication is independent of optional multi-UID seats.
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let started = Arc::new(Mutex::new(None));
    let outcome = Arc::clone(&started);
    let server = std::thread::spawn(move || {
        let result = run_with(&data_path, runner, stop_rx);
        if let Err(err) = &result {
            *outcome.lock().unwrap_or_else(|e| e.into_inner()) = Some(err.to_string());
        }
        result
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !socket.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        socket.exists(),
        "the server did not start: {:?}",
        started.lock().unwrap_or_else(|e| e.into_inner())
    );
    let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/harnesses/codex/sign-in HTTP/1.1\r\nHost: local\r\n{}: an-operator-token-0123456789abcdef\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                crate::operator::HEADER
            )
            .as_bytes(),
        )
        .unwrap();
    let mut buf = String::new();
    let mut chunk = [0u8; 256];
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline && !buf.contains("\"kind\":\"done\"") {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => buf.push_str(&String::from_utf8_lossy(&chunk[..n])),
            Err(_) => break,
        }
    }
    let link = buf.find("\"kind\":\"link\"").expect(&buf);
    let done = buf.find("\"kind\":\"done\"").expect(&buf);
    assert!(link < done, "{buf}");
    assert!(buf.contains("https://auth.example/device"), "{buf}");
    assert!(buf.contains("ada@example"), "{buf}");
    let recorded = dir.lock().unwrap().clone().unwrap();
    assert!(recorded.ends_with("config/codex/default"), "{recorded:?}");
    drop(stream);
    let _ = stop_tx.send(());
    let _ = server.join();
    // SAFETY: as above, for this test only.
    unsafe {
        std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
        std::env::remove_var("ANYAGENTS_AGENT_UID");
        std::env::remove_var("ANYAGENTS_AGENT_GID");
        std::env::remove_var("ANYAGENTS_SOCKET_GID");
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
        std::env::remove_var("ANYAGENTS_LAUNCHER");
    }
}

/// A sign-in call from an agent's own capability is refused, and no vendor
/// command runs.
#[test]
fn an_agent_capability_cannot_sign_a_harness_in() {
    if single_test_worker("an_agent_capability_cannot_sign_a_harness_in") {
        return;
    }
    let _env = env_lock();
    let _single_uid = SingleUidEnvironment::new();
    let data = tempfile::tempdir().unwrap();
    let socket = socket_path(data.path());
    let dir = Arc::new(Mutex::new(None));
    let runner = Arc::new(Mutex::new(Scripted {
        dir: Arc::clone(&dir),
    }));
    let data_path = data.path().to_path_buf();
    let token_file = data.path().join("operator-token");
    std::fs::write(&token_file, "an-operator-token-0123456789abcdef\n").unwrap();
    // SAFETY: the test process sets these before the server thread reads
    // them, and sets them for its own duration only.
    unsafe {
        std::env::set_var("ANYAGENTS_OPERATOR_TOKEN_FILE", &token_file);
        std::env::set_var(
            "ANYAGENTS_CAPABILITY_KEY",
            "0123456789abcdef0123456789abcdef",
        );
    }
    // SAFETY: as above, for this test's own duration under the lock.
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let store = Store::open(data.path().join("state.db")).unwrap();
    let configured = "0123456789abcdef0123456789abcdef";
    store.hold_capability_key(configured).unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
        .unwrap();
    drop(store);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let started = Arc::new(Mutex::new(None));
    let outcome = Arc::clone(&started);
    let server = std::thread::spawn(move || {
        let result = run_with(&data_path, runner, stop_rx);
        if let Err(err) = &result {
            *outcome.lock().unwrap_or_else(|e| e.into_inner()) = Some(err.to_string());
        }
        result
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !socket.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    assert!(
        socket.exists(),
        "the server did not start: {:?}",
        started.lock().unwrap_or_else(|e| e.into_inner())
    );
    // The server mints each agent's capability when it starts, so this test
    // reads the one it left and the server recognises.
    let capability = std::fs::read_to_string(
        data.path()
            .join("agents")
            .join(agent.id.as_str())
            .join("config")
            .join(crate::operator::CAPABILITY_FILE),
    )
    .unwrap()
    .trim()
    .to_string();
    let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(
            format!(
                "POST /v1/harnesses/codex/sign-in HTTP/1.1\r\nHost: local\r\n{}: {capability}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                crate::operator::AGENT_HEADER
            )
            .as_bytes(),
        )
        .unwrap();
    let mut buf = String::new();
    let _ = stream.read_to_string(&mut buf);
    assert!(buf.starts_with("HTTP/1.1 403"), "{buf}");
    assert!(buf.contains("cannot administer"), "{buf}");
    assert!(dir.lock().unwrap().is_none(), "the vendor command ran");
    drop(stream);
    let _ = stop_tx.send(());
    let _ = server.join();
    // SAFETY: as above, under `ENV`.
    unsafe {
        std::env::remove_var("ANYAGENTS_OPERATOR_TOKEN_FILE");
        std::env::remove_var("ANYAGENTS_CAPABILITY_KEY");
        std::env::remove_var("ANYAGENTS_AGENT_UID");
        std::env::remove_var("ANYAGENTS_AGENT_GID");
        std::env::remove_var("ANYAGENTS_SOCKET_GID");
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
        std::env::remove_var("ANYAGENTS_LAUNCHER");
    }
}

fn archived_reap_fixture(store: &Store, name: &str, parent: &Agent) -> Agent {
    let mut new = anyagents_core::NewAgent::new(name, "opencode");
    new.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new).unwrap();
    let _lifecycle = store.lock_agent_lifecycle(&child.id).unwrap();
    let child = store.set_agent_state(&child.id, "archived").unwrap();
    mark_archive_generation_locked(store, store.data_dir(), &child);
    child
}

#[test]
fn archive_reap_marker_survives_restart_and_is_invalidated_by_wake_and_rearchive() {
    if single_test_worker(
        "archive_reap_marker_survives_restart_and_is_invalidated_by_wake_and_rearchive",
    ) {
        return;
    }
    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.db");
    let store = Store::open(&db).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    let generation = std::fs::read(cfg.join(".anyagents-archived")).unwrap();
    std::fs::write(cfg.join(".anyagents-reaped"), b"foreign generation").unwrap();
    std::fs::create_dir_all(cfg.join(".cache")).unwrap();
    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    assert!(!cfg.join(".cache").exists());
    assert_eq!(
        std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
        generation
    );
    // A fresh process must use durable completion, not rescan retained dirs.
    std::fs::create_dir_all(cfg.join("cache-home")).unwrap();
    let restarted = Store::open(&db).unwrap();
    reap_archived_agents(&restarted, dir.path(), &HealthBoard::default());
    assert!(cfg.join("cache-home").exists());
    let stale_child = restarted.agent(&child.id).unwrap();
    authored_send(
        &store,
        &Actor::Agent(parent.name.clone()),
        "worker",
        "continue",
        "wake-reaped",
    );
    assert!(!cfg.join(".anyagents-reaped").exists());
    assert!(!cfg.join(".anyagents-archived").exists());
    {
        let _lifecycle = restarted.lock_agent_lifecycle(&stale_child.id).unwrap();
        reap_archived_agent_locked(&restarted, dir.path(), &stale_child.id);
    }
    assert!(
        cfg.join("cache-home").exists(),
        "stale cleanup deleted running config"
    );
    assert_eq!(store.agent(&child.id).unwrap().state, "running");
    mark_explicitly_archived(&store, dir.path(), &child);
    assert_ne!(
        std::fs::read(cfg.join(".anyagents-archived")).unwrap(),
        generation
    );
    assert!(!cfg.join(".anyagents-reaped").exists());
    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    assert!(!cfg.join("cache-home").exists());
    assert_eq!(
        std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
        std::fs::read(cfg.join(".anyagents-archived")).unwrap()
    );
    unsafe {
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
    }
}

#[test]
fn archive_reap_repairs_empty_legacy_marker_and_retries_partial_failure() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let other = archived_reap_fixture(&store, "other", &parent);
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    let other_cfg = anyagents_core::agent_config_dir(dir.path(), &other.id);
    std::fs::write(cfg.join(".anyagents-archived"), b"").unwrap();
    let blocked = cfg.join(".cache");
    std::fs::create_dir_all(&blocked).unwrap();
    std::fs::write(blocked.join("keep"), b"partial cleanup").unwrap();
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o000)).unwrap();
    let failed_at = now_ms();
    reap_archived_agents_at(&store, dir.path(), &HealthBoard::default(), failed_at);
    // Restore access before asserting so failed tests don't leak fixtures.
    std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(
        !std::fs::read(cfg.join(".anyagents-archived"))
            .unwrap()
            .is_empty()
    );
    assert!(
        !cfg.join(".anyagents-reaped").exists(),
        "failed cleanup was marked complete"
    );
    assert!(
        other_cfg.join(".anyagents-reaped").exists(),
        "failure starved an independent archive"
    );
    // A failed cleanup waits out its backoff instead of the next minute.
    reap_archived_agents_at(
        &store,
        dir.path(),
        &HealthBoard::default(),
        failed_at + 30_000,
    );
    assert!(blocked.exists(), "retried before its backoff");
    assert!(!cfg.join(".anyagents-reaped").exists());
    reap_archived_agents_at(
        &store,
        dir.path(),
        &HealthBoard::default(),
        now_ms() + 60_000,
    );
    assert!(!blocked.exists());
    assert_eq!(
        std::fs::read(cfg.join(".anyagents-reaped")).unwrap(),
        std::fs::read(cfg.join(".anyagents-archived")).unwrap()
    );
}

#[test]
fn reap_queue_backfills_once_and_idle_maintenance_visits_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    // Finished under an earlier binary: its marker matches its archive.
    let finished = archived_reap_fixture(&store, "finished", &parent);
    let finished_cfg = anyagents_core::agent_config_dir(dir.path(), &finished.id);
    std::fs::copy(
        finished_cfg.join(".anyagents-archived"),
        finished_cfg.join(".anyagents-reaped"),
    )
    .unwrap();
    // Archived by an earlier binary that wrote no marker at all.
    let mut new = anyagents_core::NewAgent::new("legacy", "opencode");
    new.parent_id = Some(parent.id.clone());
    let legacy = store.create_agent(new).unwrap();
    store.set_agent_state(&legacy.id, "archived").unwrap();
    let legacy_cfg = anyagents_core::agent_config_dir(dir.path(), &legacy.id);
    // Forget what this binary queued, as if the archives predated the queue.
    std::fs::remove_dir_all(dir.path().join("reap-pending")).unwrap();
    for cfg in [&finished_cfg, &legacy_cfg] {
        std::fs::create_dir_all(cfg.join(".cache")).unwrap();
    }

    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    assert!(
        finished_cfg.join(".cache").exists(),
        "finished archive revisited"
    );
    assert!(
        !legacy_cfg.join(".cache").exists(),
        "legacy archive not backfilled"
    );
    assert_eq!(
        std::fs::read(legacy_cfg.join(".anyagents-reaped")).unwrap(),
        std::fs::read(legacy_cfg.join(".anyagents-archived")).unwrap()
    );

    // Nothing is owed now, so maintenance visits no agent at all.
    std::fs::create_dir_all(legacy_cfg.join(".cache")).unwrap();
    std::fs::remove_file(finished_cfg.join(".anyagents-reaped")).unwrap();
    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    assert!(legacy_cfg.join(".cache").exists());
    assert!(finished_cfg.join(".cache").exists());
    assert!(reap_queue::due(dir.path(), i64::MAX, usize::MAX).is_empty());
}

#[test]
fn roots_and_role_bearing_children_can_self_archive_without_losing_browser_profiles() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().into(),
        },
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for (name, parent_id, role) in [
            ("root", None, Some("Researcher")),
            (
                "labelled-child",
                Some(parent.id.clone()),
                Some("Researcher"),
            ),
            ("plain-child", Some(parent.id.clone()), None),
        ] {
            let mut new = anyagents_core::NewAgent::new(name, "codex");
            new.parent_id = parent_id;
            new.role = role.map(str::to_string);
            let agent = store.create_agent(new).unwrap();
            let profile = dir.path().join("browser-profiles").join(name);
            std::fs::create_dir_all(&profile).unwrap();
            std::fs::write(profile.join("login"), b"preserved session").unwrap();
            let caller = Principal::Agent {
                agent_id: agent.id.clone(),
                name: name.into(),
            };
            let Json(archived) =
                archive_agent_route(caller, State(state.clone()), AxumPath("self".into()))
                    .await
                    .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
            assert_eq!(archived.state, "archived");
            assert_eq!(store.agent(&agent.id).unwrap().role, agent.role);
            assert_eq!(
                std::fs::read(profile.join("login")).unwrap(),
                b"preserved session"
            );
            let _ = delete_agent_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(name.into()),
            )
            .await
            .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
            assert!(!profile.exists(), "delete must remove the profile");
        }
    });
}

#[test]
fn archive_reap_waits_for_self_archived_process_to_stop() {
    if single_test_worker("archive_reap_waits_for_self_archived_process_to_stop") {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    std::fs::create_dir_all(cfg.join(".cache")).unwrap();
    let health = HealthBoard::default();
    let mut live = anyagents_supervisor::LiveState {
        process_alive: true,
        turn_state: anyagents_harness::TurnState::Idle,
        delivery: Some(crate::vendor::declared_delivery(&child.harness)),
        quota_reset_at_ms: None,
        restart_at_ms: None,
        pid: None,
        live_harness: Some(child.harness.clone()),
        live_model: None,
        process_started_at_ms: None,
        last_error: None,
        retry_at_ms: None,
    };
    health.publish(&child.id, live.clone());
    reap_archived_agents(&store, dir.path(), &health);
    assert!(cfg.join(".cache").exists());
    assert!(!cfg.join(".anyagents-reaped").exists());
    live.process_alive = false;
    health.publish(&child.id, live);
    reap_queue::enqueue(dir.path(), &child.id);
    reap_archived_agents(&store, dir.path(), &health);
    assert!(!cfg.join(".cache").exists());
    assert!(cfg.join(".anyagents-reaped").exists());
}

#[test]
fn archive_reap_failed_protection_read_and_delete_preserve_unknown_work() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(work.join("keep"), b"protected").unwrap();
    // Simulate an unreadable/corrupt protection database, not an empty list.
    let failed = tempfile::tempdir().unwrap();
    let failed_work = anyagents_core::agent_work_dir(failed.path(), &child.id);
    std::fs::create_dir_all(&failed_work).unwrap();
    std::fs::write(failed_work.join("keep"), b"unknown references").unwrap();
    std::fs::write(failed.path().join("state.db"), b"not a database").unwrap();
    assert!(!reap_subagent_ephemeral_dirs(failed.path(), &child.id));
    assert!(failed_work.join("keep").exists());
    // A live sibling sharing this workdir protects it. Deleting the target
    // row still invokes raw cleanup, but must retain surviving references.
    let sibling = store
        .create_agent(anyagents_core::NewAgent::new("sibling", "opencode"))
        .unwrap();
    store
        .set_agent_running(&sibling.id, work.to_str().unwrap())
        .unwrap();
    store.delete_agent(&child.id).unwrap();
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert!(work.join("keep").exists());
    store.delete_agent(&sibling.id).unwrap();
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert_eq!(std::fs::read(work.join("keep")).unwrap(), b"protected");
}

#[test]
fn archive_reap_requires_process_custody_even_when_health_is_absent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    std::fs::create_dir_all(cfg.join(".cache")).unwrap();
    let custody = store.try_lock_agent(&child.id).unwrap();
    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    assert!(cfg.join(".cache").exists());
    assert!(!cfg.join(".anyagents-reaped").exists());
    drop(custody);
    reap_queue::enqueue(dir.path(), &child.id);
    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    assert!(!cfg.join(".cache").exists());
    assert!(cfg.join(".anyagents-reaped").exists());
}

#[test]
fn archive_reap_preserves_native_homes_memory_sessions_and_unknown_work() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
    let files: Vec<_> = [
        ".claude/projects/session.json",
        ".codex/history.jsonl",
        "state-home/learned.md",
        ".config/settings",
        "data-home/sessions/a",
    ]
    .map(|p| cfg.join(p))
    .into_iter()
    .chain(
        [
            "memory/MEMORY.md",
            "sessions/transcript.json",
            "target/report",
        ]
        .map(|p| work.join(p)),
    )
    .collect();
    for file in &files {
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, b"only copy").unwrap();
    }
    std::fs::create_dir_all(cfg.join(".cache")).unwrap();
    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    for file in &files {
        assert_eq!(std::fs::read(file).unwrap(), b"only copy");
    }
    assert!(!cfg.join(".cache").exists());
    assert!(cfg.join(".anyagents-reaped").exists());
}

#[test]
fn archive_reap_drains_at_most_32_per_pass() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let children: Vec<_> = (0..65)
        .map(|i| archived_reap_fixture(&store, &format!("worker-{i}"), &parent))
        .collect();
    for remaining in [33, 1, 0] {
        reap_archived_agents(&store, dir.path(), &HealthBoard::default());
        assert_eq!(
            reap_queue::due(dir.path(), i64::MAX, usize::MAX).len(),
            remaining
        );
        let done = children
            .iter()
            .filter(|a| {
                anyagents_core::agent_config_dir(dir.path(), &a.id)
                    .join(".anyagents-reaped")
                    .exists()
            })
            .count();
        assert_eq!(done, 65 - remaining);
    }
}

#[test]
fn cleanup_preserves_unpushed_head_and_other_branch_then_removes_pushed() {
    use crate::repo::tests::git;
    fn snapshot(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
        fn walk(root: &Path, path: &Path, out: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>) {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                let meta = std::fs::symlink_metadata(&path).unwrap();
                if meta.is_dir() {
                    walk(root, &path, out);
                } else if meta.is_file() {
                    out.insert(
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        std::fs::read(&path).unwrap(),
                    );
                } else {
                    panic!("unexpected fixture object: {}", path.display());
                }
            }
        }
        let mut out = std::collections::BTreeMap::new();
        walk(root, root, &mut out);
        out
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
    let fixture = dir.path().join("fixture");
    crate::repo::tests::seed_remote(&fixture);
    crate::repo::tests::pool_checkouts(&fixture)
        .remote("ExampleOrg/demo", &work, "worker", None)
        .unwrap();
    let commit = |args: &[&str]| {
        let mut all = vec!["-c", "user.name=t", "-c", "user.email=t@example.test"];
        all.extend_from_slice(args);
        git(&work, &all)
    };
    assert!(
        is_git_worktree_safe_to_reap(&work, None),
        "published baseline must be eligible"
    );
    std::fs::write(work.join("src/unique"), b"unique committed source\n").unwrap();
    git(&work, &["add", "src/unique"]);
    commit(&["commit", "-m", "Unpushed source"]);
    // A child-controlled marker must not turn an unpushed HEAD into a GC proof.
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    std::fs::write(
        cfg.join(".anyagents-managed-worktree.json"),
        serde_json::to_vec(
            &serde_json::json!({"path": work, "initial_head": git(&work, &["rev-parse", "HEAD"])}),
        )
        .unwrap(),
    )
    .unwrap();
    let before_work = snapshot(&work);
    assert!(!is_git_worktree_safe_to_reap(&work, None));
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert_eq!(snapshot(&work), before_work);
    // A checkout whose .git is gone or replaced by a pointer elsewhere is
    // never judged through it, nor treated as a plain directory.
    let own = work.join(".git");
    let aside = dir.path().join("aside.git");
    std::fs::rename(&own, &aside).unwrap();
    for pointer in [None, Some(b"gitdir: /nonexistent/broken\n".as_slice())] {
        if let Some(bytes) = pointer {
            std::fs::write(&own, bytes).unwrap();
        }
        std::fs::create_dir_all(work.join("target")).unwrap();
        std::fs::write(work.join("target/uncertain-report"), b"retain\n").unwrap();
        let missing_work = snapshot(&work);
        assert!(!is_git_worktree_safe_to_reap(&work, None));
        assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
        assert_eq!(snapshot(&work), missing_work, "lost .git deleted source");
        let _ = std::fs::remove_file(&own);
    }
    std::fs::rename(&aside, &own).unwrap();
    std::fs::remove_dir_all(work.join("target")).unwrap();
    git(&work, &["push", "-q", "origin", "HEAD:refs/heads/worker"]);
    assert!(is_git_worktree_safe_to_reap(&work, None));
    let pushed_head = git(&work, &["rev-parse", "HEAD"]);
    git(&work, &["checkout", "-q", "-b", "unpublished-side"]);
    commit(&["commit", "-q", "--allow-empty", "-m", "Side branch only"]);
    git(&work, &["checkout", "-q", "--detach", &pushed_head]);
    let before_work = snapshot(&work);
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "published HEAD does not cover another unpushed local branch"
    );
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert_eq!(snapshot(&work), before_work);
    git(
        &work,
        &["push", "-q", "origin", "refs/heads/unpublished-side"],
    );
    assert!(is_git_worktree_safe_to_reap(&work, None));
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert!(!work.exists());
}

/// Archive keeps anything only the checkout holds: a stash, a tag or a
/// note the remote lacks. What the remote advertises is the proof, so a
/// branch published by URL (no remote-tracking ref moves) is reaped.
/// A clean checkout of a fresh pool clone whose every commit is on the
/// remote: the baseline each refusal below changes one thing in.
fn published_checkout(dir: &Path) -> (PathBuf, PathBuf) {
    let fixture = dir.join("fixture");
    let remote = crate::repo::tests::seed_remote(&fixture);
    let work = dir.join("work");
    crate::repo::tests::pool_checkouts(&fixture)
        .remote("ExampleOrg/demo", &work, "worker", None)
        .unwrap();
    crate::repo::tests::git(&work, &["push", "-q", "origin", "worker"]);
    assert!(is_git_worktree_safe_to_reap(&work, None), "baseline");
    (work, remote)
}

#[test]
fn cleanup_preserves_detached_reflog_and_in_progress_git_state() {
    use crate::repo::tests::git;
    let dir = tempfile::tempdir().unwrap();
    let (work, _) = published_checkout(dir.path());
    git(&work, &["checkout", "--detach"]);
    git_as_author(&work, &["commit", "--allow-empty", "-m", "detached-only"]);
    assert!(!is_git_worktree_safe_to_reap(&work, None), "detached HEAD");
    git(&work, &["checkout", "worker"]);
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "reflog-only commit"
    );
    git(&work, &["reflog", "expire", "--expire=now", "--all"]);
    assert!(is_git_worktree_safe_to_reap(&work, None));
    for name in [
        "index.lock",
        "HEAD.lock",
        "MERGE_HEAD",
        "CHERRY_PICK_HEAD",
        "REVERT_HEAD",
        "REBASE_HEAD",
        "rebase-apply",
        "rebase-merge",
        "sequencer",
        "BISECT_START",
        "AUTO_MERGE",
    ] {
        let path = work.join(".git").join(name);
        std::fs::write(&path, "recovery state").unwrap();
        assert!(!is_git_worktree_safe_to_reap(&work, None), "{name}");
        std::fs::remove_file(path).unwrap();
    }
    assert!(is_git_worktree_safe_to_reap(&work, None));
}

fn git_as_author(dir: &Path, args: &[&str]) -> String {
    let mut all = vec!["-c", "user.name=t", "-c", "user.email=t@example.test"];
    all.extend_from_slice(args);
    crate::repo::tests::git(dir, &all)
}

/// Ignored files are often the only copy of something (`.env`, data, a
/// gitignored memory file). Build-like names do not prove disposability.
#[test]
fn cleanup_keeps_all_ignored_files_including_build_names() {
    let dir = tempfile::tempdir().unwrap();
    let (work, _) = published_checkout(dir.path());
    std::fs::create_dir_all(work.join(".git/info")).unwrap();
    std::fs::write(
        work.join(".git/info/exclude"),
        "node_modules/\ntarget/\n.env\nmemory/\nbench/results/\n",
    )
    .unwrap();
    for dir in ["node_modules/pkg", "target/debug", "crates/x/target/debug"] {
        std::fs::create_dir_all(work.join(dir)).unwrap();
    }
    std::fs::write(work.join("node_modules/pkg/index.js"), "x").unwrap();
    std::fs::write(
        work.join("target/CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55",
    )
    .unwrap();
    std::fs::write(work.join("target/debug/bin"), "x").unwrap();
    std::fs::write(
        work.join("crates/x/target/CACHEDIR.TAG"),
        "Signature: 8a477f597d28d172789f06886806bc55",
    )
    .unwrap();
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "build output may hide unique work"
    );
    for path in ["node_modules", "target", "crates"] {
        std::fs::remove_dir_all(work.join(path)).unwrap();
    }
    assert!(is_git_worktree_safe_to_reap(&work, None));
    for (path, body) in [
        (".env", "SECRET=only-copy\n"),
        ("memory/MEMORY.md", "# the agent's only memory\n"),
        ("bench/results/run.json", "{}\n"),
    ] {
        let file = work.join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, body).unwrap();
        assert!(
            !is_git_worktree_safe_to_reap(&work, None),
            "ignored {path} was judged disposable"
        );
        std::fs::remove_file(&file).unwrap();
    }
    // A directory merely named target is not build output.
    std::fs::create_dir_all(work.join("notes/target")).unwrap();
    std::fs::write(work.join("notes/target/plan.md"), "x").unwrap();
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "an untagged target/"
    );
}

/// An edit to a tracked file is never incidental, whatever the file is
/// called; and AnyAgents writes no instruction file into a workdir, so an
/// untracked one is the agent's too.
#[test]
fn cleanup_keeps_tracked_and_untracked_instruction_file_edits() {
    let dir = tempfile::tempdir().unwrap();
    let (work, _) = published_checkout(dir.path());
    for path in ["AGENTS.md", "CLAUDE.md", ".claude/settings.json"] {
        let file = work.join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, "committed\n").unwrap();
    }
    crate::repo::tests::git(&work, &["add", "."]);
    git_as_author(&work, &["commit", "-qm", "instructions"]);
    crate::repo::tests::git(&work, &["push", "-q", "origin", "worker"]);
    assert!(is_git_worktree_safe_to_reap(&work, None));
    for path in ["AGENTS.md", "CLAUDE.md", ".claude/settings.json"] {
        std::fs::write(work.join(path), "an edit\n").unwrap();
        assert!(
            !is_git_worktree_safe_to_reap(&work, None),
            "tracked {path} edit"
        );
        crate::repo::tests::git(&work, &["checkout", "-q", "--", path]);
    }
    std::fs::write(work.join("GEMINI.md"), "written by the agent\n").unwrap();
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "untracked GEMINI.md"
    );
}

/// An orphan HEAD has no commit, so nothing about it is proven.
#[test]
fn cleanup_keeps_an_unborn_head() {
    let dir = tempfile::tempdir().unwrap();
    let (work, _) = published_checkout(dir.path());
    crate::repo::tests::git(&work, &["switch", "-q", "--orphan", "scratch"]);
    assert!(!is_git_worktree_safe_to_reap(&work, None));
}

/// A submodule's own Git directory, absorbed or embedded, is history the
/// check cannot read: keep.
#[test]
fn cleanup_keeps_submodules_with_their_own_git_directory() {
    let dir = tempfile::tempdir().unwrap();
    let (work, _) = published_checkout(dir.path());
    let library = dir.path().join("library");
    std::fs::create_dir_all(&library).unwrap();
    crate::repo::tests::git(&library, &["init", "-q", "-b", "main"]);
    git_as_author(
        &library,
        &["commit", "-q", "--allow-empty", "-m", "library"],
    );
    // Embedded: a repository inside the checkout, staged as a gitlink.
    let embedded = work.join("vendor/embedded");
    std::fs::create_dir_all(&embedded).unwrap();
    crate::repo::tests::git(&embedded, &["init", "-q", "-b", "main"]);
    git_as_author(
        &embedded,
        &["commit", "-q", "--allow-empty", "-m", "unpushed"],
    );
    crate::repo::tests::git(&work, &["add", "vendor/embedded"]);
    git_as_author(&work, &["commit", "-qm", "gitlink"]);
    crate::repo::tests::git(&work, &["push", "-q", "origin", "worker"]);
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "embedded gitlink"
    );
    // Absorbed: `.git/modules` holds the submodule's repository.
    git_as_author(
        &work,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            library.to_str().unwrap(),
            "lib",
        ],
    );
    assert!(work.join(".git/modules").is_dir());
    assert!(!is_git_worktree_safe_to_reap(&work, None), ".git/modules");
}

/// Every ref namespace counts, not only branches: a filter-branch backup
/// in `refs/original` is the only copy of the history it names.
#[test]
fn cleanup_counts_every_ref_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let (work, _) = published_checkout(dir.path());
    // A commit no branch, tag or reflog names yet.
    let only_copy = git_as_author(
        &work,
        &[
            "commit-tree",
            "HEAD^{tree}",
            "-p",
            "HEAD",
            "-m",
            "rewritten away",
        ],
    );
    assert!(is_git_worktree_safe_to_reap(&work, None));
    for name in [
        "refs/original/refs/heads/worker",
        "refs/anyagents/backup",
        "refs/pull/1/head",
    ] {
        crate::repo::tests::git(&work, &["update-ref", name, &only_copy]);
        assert!(!is_git_worktree_safe_to_reap(&work, None), "{name}");
        crate::repo::tests::git(&work, &["update-ref", "-d", name]);
    }
}

/// A live agent working inside an archived agent's directory, or in a
/// directory holding it, keeps that directory whole.
#[test]
fn cleanup_keeps_a_directory_a_live_agent_works_in_or_around() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let archived = archived_reap_fixture(&store, "old-worker", &parent);
    let work = anyagents_core::agent_work_dir(dir.path(), &archived.id);
    let inside = work.join("closure/live-worker");
    std::fs::create_dir_all(&inside).unwrap();
    std::fs::write(inside.join("progress.md"), "live work\n").unwrap();
    let live = store
        .create_agent(anyagents_core::NewAgent::new("live-worker", "codex"))
        .unwrap();
    store
        .set_agent_running(&live.id, &inside.display().to_string())
        .unwrap();
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &archived.id));
    assert_eq!(
        std::fs::read_to_string(inside.join("progress.md")).unwrap(),
        "live work\n",
        "a live agent's workdir inside the reaped one was deleted"
    );
    // The other way round: the live agent works in a directory that holds it.
    store
        .set_agent_running(&live.id, &work.parent().unwrap().display().to_string())
        .unwrap();
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &archived.id));
    assert!(inside.join("progress.md").is_file());
    assert!(workdirs_overlap(&work, &inside) && workdirs_overlap(&inside, &work));
    assert!(!workdirs_overlap(
        &work,
        &dir.path().join("agents/other/work")
    ));
    assert!(!workdirs_overlap(
        &work,
        &work.with_file_name(format!("{}2", work.file_name().unwrap().to_string_lossy()))
    ));
    assert!(!workdirs_overlap(&work, Path::new("")));
}

#[test]
fn cleanup_keeps_a_stash_and_unpublished_tags_or_notes() {
    use crate::repo::tests::git;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
    let fixture = dir.path().join("fixture");
    let remote = crate::repo::tests::seed_remote(&fixture);
    crate::repo::tests::pool_checkouts(&fixture)
        .remote("ExampleOrg/demo", &work, "worker", None)
        .unwrap();
    let as_author = |args: &[&str]| {
        let mut all = vec!["-c", "user.name=t", "-c", "user.email=t@example.test"];
        all.extend_from_slice(args);
        git(&work, &all)
    };
    assert!(is_git_worktree_safe_to_reap(&work, None));
    // A stash is the only copy of an edit.
    std::fs::write(work.join("README.md"), "precious edit\n").unwrap();
    as_author(&["stash", "-q"]);
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "a stash was judged disposable"
    );
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert_eq!(git(&work, &["stash", "list"]).lines().count(), 1);
    git(&work, &["stash", "drop", "-q"]);
    // A tag the remote lacks, then one it has.
    git(&work, &["tag", "local-only"]);
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "an unpublished tag"
    );
    git(&work, &["push", "-q", "origin", "refs/tags/local-only"]);
    assert!(is_git_worktree_safe_to_reap(&work, None));
    // A note the remote lacks, then one it has.
    as_author(&["notes", "add", "-m", "remember this"]);
    assert!(
        !is_git_worktree_safe_to_reap(&work, None),
        "an unpublished note"
    );
    git(&work, &["push", "-q", "origin", "refs/notes/commits"]);
    assert!(is_git_worktree_safe_to_reap(&work, None));
    // A commit published by URL, as a publishing tool does: no
    // remote-tracking ref records it, the remote still has it.
    as_author(&["commit", "-q", "--allow-empty", "-m", "Published by URL"]);
    assert!(!is_git_worktree_safe_to_reap(&work, None));
    git(
        &work,
        &[
            "push",
            "-q",
            remote.to_str().unwrap(),
            "HEAD:refs/heads/published",
        ],
    );
    assert!(git(&work, &["for-each-ref", "refs/remotes/origin/published"]).is_empty());
    assert!(is_git_worktree_safe_to_reap(&work, None));
    // With no answer from the remote, nothing is proven.
    git(
        &work,
        &[
            "remote",
            "set-url",
            "origin",
            "https://example.invalid/x.git",
        ],
    );
    assert!(!is_git_worktree_safe_to_reap(&work, None));
    git(
        &work,
        &["remote", "set-url", "origin", remote.to_str().unwrap()],
    );
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert!(!work.exists());
}

/// An archived agent's linked worktree from before per-agent repositories
/// is converted, then judged like any checkout: dirty work stays, and the
/// shared store no longer registers it.
#[test]
fn cleanup_converts_a_legacy_worktree_and_keeps_its_work() {
    use crate::repo::tests::git;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let dirty = archived_reap_fixture(&store, "dirty", &parent);
    let clean = archived_reap_fixture(&store, "clean", &parent);
    let fixture = dir.path().join("fixture");
    let remote = crate::repo::tests::seed_remote(&fixture);
    let hub = fixture.join("hub");
    git(
        &fixture,
        &[
            "clone",
            "-q",
            remote.to_str().unwrap(),
            hub.to_str().unwrap(),
        ],
    );
    git(&hub, &["config", "gc.pruneExpire", "never"]);
    git(&hub, &["config", "gc.cruftPacks", "true"]);
    let mut checkouts = Vec::new();
    for agent in [&dirty, &clean] {
        let work = anyagents_core::agent_work_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(work.parent().unwrap()).unwrap();
        git(
            &hub,
            &["worktree", "add", "-q", "--detach", work.to_str().unwrap()],
        );
        checkouts.push(work);
    }
    std::fs::write(checkouts[0].join("README.md"), "work in progress\n").unwrap();
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &dirty.id));
    assert!(checkouts[0].join(".git").is_dir(), "not converted");
    assert_eq!(
        std::fs::read_to_string(checkouts[0].join("README.md")).unwrap(),
        "work in progress\n"
    );
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &clean.id));
    assert!(!checkouts[1].exists(), "a clean converted checkout stays");
    let registered = git(&hub, &["worktree", "list", "--porcelain"]);
    assert!(
        !registered.contains(checkouts[0].to_str().unwrap())
            && !registered.contains(checkouts[1].to_str().unwrap()),
        "{registered}"
    );
    // Preserve both pool recovery references, even after successful reaping.
    // Cleanup must not drop recovery protection before directory removal.
    let keeps = git(
        &hub,
        &["for-each-ref", "--format=%(refname)", "refs/anyagents/keep"],
    );
    assert_eq!(keeps.lines().count(), 2, "{keeps}");
}

#[test]
fn archive_reap_preserves_unpushed_and_external_worktrees() {
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
    std::fs::create_dir_all(&work).unwrap();
    let git = |args: &[&str]| {
        let output = std::process::Command::new("git")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .args([
                "-c",
                "user.name=Example Author",
                "-c",
                "user.email=author@example.org",
                "-C",
            ])
            .arg(&work)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    git(&["init", "-q"]);
    std::fs::write(work.join("source.rs"), b"unique committed source").unwrap();
    std::fs::write(work.join(".gitignore"), b"/target/\n").unwrap();
    git(&["add", "source.rs", ".gitignore"]);
    git(&["commit", "-qm", "Initial source"]);
    std::fs::create_dir_all(work.join("target/debug")).unwrap();
    std::fs::write(work.join("target/debug/report"), b"unique report").unwrap();
    // A clean commit with no remote is not safe to delete.
    assert!(!is_git_worktree_safe_to_reap(&work, None));
    reap_archived_agents(&store, dir.path(), &HealthBoard::default());
    assert!(work.join("source.rs").exists());
    assert!(work.join(".git").exists());
    assert_eq!(
        std::fs::read(work.join("target/debug/report")).unwrap(),
        b"unique report"
    );
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    assert!(
        cfg.join(".anyagents-reaped").exists(),
        "retained source is not pending cleanup"
    );
    let external = dir.path().join("borrowed-worktree");
    std::fs::create_dir_all(external.join("target/debug")).unwrap();
    std::fs::write(external.join("source.rs"), b"borrowed source").unwrap();
    std::fs::write(
        cfg.join(".anyagents-managed-worktree.json"),
        serde_json::to_vec(&serde_json::json!({"path": external})).unwrap(),
    )
    .unwrap();
    assert!(reap_subagent_ephemeral_dirs(dir.path(), &child.id));
    assert!(external.join("source.rs").exists());
    assert!(external.join("target/debug").exists());
}

#[test]
fn archive_reap_blocked_in_git_does_not_delay_other_mail() {
    if single_test_worker("archive_reap_blocked_in_git_does_not_delay_other_mail") {
        return;
    }
    let _env = env_lock();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.db");
    let store = Store::open(&db).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    let child = archived_reap_fixture(&store, "slow-worker", &parent);
    let work = anyagents_core::agent_work_dir(dir.path(), &child.id);
    let fixture = dir.path().join("fixture");
    crate::repo::tests::seed_remote(&fixture);
    crate::repo::tests::pool_checkouts(&fixture)
        .remote("ExampleOrg/demo", &work, "worker", None)
        .unwrap();
    let recipient = store
        .create_agent(anyagents_core::NewAgent::new("recipient", "opencode"))
        .unwrap();
    store
        .receive(&recipient.id, anyagents_core::Incoming::new("pending mail"))
        .unwrap();
    store.set_agent_state(&recipient.id, "running").unwrap();
    let entered = dir.path().join("git-entered");
    let release = dir.path().join("git-release");
    let bin = dir.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    // Delegate to the same Git that prepared the fixture (Homebrew on
    // macOS), preserving all arguments and stdin for validation probes.
    let real_git = std::process::Command::new("/bin/sh")
        .args(["-c", "command -v git"])
        .output()
        .unwrap();
    assert!(real_git.status.success());
    let real_git = String::from_utf8(real_git.stdout).unwrap();
    let real_git = real_git.trim();
    assert!(Path::new(real_git).is_absolute());
    let git = bin.join("git");
    crate::repo::tests::write_executable(
        &git,
        &format!(
            r#"#!/bin/sh
# Parse in a subshell so non-barrier calls retain the original argv.
if (
while [ "$#" -gt 0 ]; do
    case "$1" in
        -c|--git-dir) shift 2 ;;
        --no-optional-locks) shift ;;
        --work-tree|-C) cwd="$2"; shift 2 ;;
        *) break ;;
    esac
done
[ "$cwd" = '{}' ] && [ "$1" = status ]
); then
touch '{}'
n=0
while [ ! -e '{}' ] && [ "$n" -lt 1000 ]; do sleep 0.01; n=$((n+1)); done
exit 1
fi
exec '{}' "$@"
"#,
            work.display(),
            entered.display(),
            release.display(),
            real_git
        ),
    );
    let previous_path = std::env::var_os("PATH");
    let path =
        std::env::join_paths([bin, PathBuf::from("/usr/bin"), PathBuf::from("/bin")]).unwrap();
    unsafe {
        std::env::set_var("PATH", path);
    }
    let worker_store = Store::open(&db).unwrap();
    let worker_data = dir.path().to_path_buf();
    let worker = std::thread::spawn(move || {
        reap_archived_agents(&worker_store, &worker_data, &HealthBoard::default())
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !entered.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let was_entered = entered.exists();
    let owns_lifecycle = matches!(
        store.try_lock_agent_lifecycle(&child.id),
        Err(Error::AgentBusy)
    );
    let rec_store = Store::open(&db).unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let scanner = std::thread::spawn(move || {
        authored_send(
            &rec_store,
            &Actor::Operator("operator".into()),
            "recipient",
            "unrelated mail",
            "during-reap",
        );
        done_tx.send(()).unwrap();
    });
    let progressed_while_blocked = done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .is_ok();
    let recipient_running = store.agent(&recipient.id).unwrap().state == "running";
    std::fs::write(&release, b"release").unwrap();
    worker.join().unwrap();
    scanner.join().unwrap();
    unsafe {
        if let Some(path) = previous_path {
            std::env::set_var("PATH", path);
        } else {
            std::env::remove_var("PATH");
        }
    }
    assert!(was_entered, "reaper never reached the git barrier");
    assert!(
        owns_lifecycle,
        "barrier did not observe reaper lifecycle ownership"
    );
    assert!(
        progressed_while_blocked && recipient_running,
        "unrelated mail delivery waited for blocked cleanup"
    );
}

#[test]
fn reap_subagent_ephemeral_dirs_preserves_dirty_worktree_including_build_names() {
    let data = tempfile::tempdir().unwrap();
    let agent_id = anyagents_core::AgentId::generate();
    let agent_root = data.path().join("agents").join(agent_id.as_str());
    let config_dir = agent_root.join("config");
    let work_dir = agent_root.join("work");
    std::fs::create_dir_all(config_dir.join(".gemini/antigravity-cli/brain")).unwrap();
    std::fs::write(
        config_dir.join(".gemini/antigravity-cli/brain/transcript.jsonl"),
        b"large transcript",
    )
    .unwrap();
    std::fs::create_dir_all(config_dir.join(".cache/sub")).unwrap();
    std::fs::create_dir_all(agent_root.join("tmp/sub")).unwrap();
    std::fs::create_dir_all(work_dir.join(".git")).unwrap();
    std::fs::create_dir_all(work_dir.join("target/debug")).unwrap();
    std::fs::create_dir_all(work_dir.join("apps/web/node_modules/pkg")).unwrap();
    std::fs::write(work_dir.join("dirty.rs"), b"fn main() {}").unwrap();

    reap_subagent_ephemeral_dirs(data.path(), &agent_id);

    assert_eq!(
        std::fs::read(config_dir.join(".gemini/antigravity-cli/brain/transcript.jsonl")).unwrap(),
        b"large transcript"
    );
    assert!(!config_dir.join(".cache").exists());
    assert!(!agent_root.join("tmp").exists());
    // Dirty worktree contents are preserved regardless of build-like names:
    assert!(work_dir.join("dirty.rs").exists());
    assert!(work_dir.join("target").exists());
    assert!(work_dir.join("apps/web/node_modules").exists());
}

#[cfg(unix)]
#[test]
fn reap_and_delete_tolerate_symlinked_host_mount_roots_and_reject_inner_symlinks() {
    use std::os::unix::fs::symlink;
    let phys = tempfile::tempdir().unwrap();
    let real_data = phys.path().join("private/var/folders/data");
    let outside = phys.path().join("outside-keep");
    std::fs::create_dir_all(&real_data).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("precious"), b"must stay").unwrap();
    let var_alias = phys.path().join("var");
    symlink(phys.path().join("private/var"), &var_alias).unwrap();
    let data = var_alias.join("folders/data");

    let store = Store::open(data.join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent", "codex"))
        .unwrap();
    let child = archived_reap_fixture(&store, "worker", &parent);
    let cfg = anyagents_core::agent_config_dir(&data, &child.id);
    std::fs::create_dir_all(cfg.join(".cache")).unwrap();
    std::fs::create_dir_all(cfg.join("cache-home")).unwrap();
    std::fs::create_dir_all(cfg.join("tmp")).unwrap();
    reap_archived_agents(&store, &data, &HealthBoard::default());
    assert!(!cfg.join(".cache").exists());
    assert!(!cfg.join("cache-home").exists());
    assert!(!cfg.join("tmp").exists());
    assert!(cfg.join(".anyagents-reaped").exists());

    // A symlink planted beneath `data_dir` is still rejected fail-closed.
    let tampered = archived_reap_fixture(&store, "tampered", &parent);
    let tampered_root = data.join("agents").join(tampered.id.as_str());
    std::fs::remove_dir_all(&tampered_root).unwrap_or(());
    std::fs::create_dir_all(&tampered_root).unwrap();
    std::fs::create_dir_all(outside.join(".cache")).unwrap();
    symlink(&outside, tampered_root.join("config")).unwrap();
    assert!(!reap_subagent_ephemeral_dirs(&data, &tampered.id));
    assert!(outside.join(".cache").exists());
    assert_eq!(
        std::fs::read(outside.join("precious")).unwrap(),
        b"must stay"
    );

    // Identity removal also works under symlinked host mount roots.
    let shopper = store
        .create_agent(anyagents_core::NewAgent::new("shopper", "codex"))
        .unwrap();
    let shopper_dir = data.join("agents").join(shopper.id.as_str());
    let profile = data.join("browser-profiles").join("shopper");
    std::fs::create_dir_all(shopper_dir.join("config")).unwrap();
    std::fs::create_dir_all(profile.join("user-data")).unwrap();
    remove_agent_identity_files(&data, &shopper, &shopper_dir, None);
    assert!(!profile.exists());
    assert!(!shopper_dir.join("config").exists());
}
fn mark_explicitly_archived(store: &Store, data_dir: &Path, agent: &Agent) {
    let Ok(_lifecycle) = store.lock_agent_lifecycle(&agent.id) else {
        return;
    };
    let _ = store.set_agent_state(&agent.id, "archived");
    mark_explicitly_archived_locked(store, data_dir, agent).unwrap();
}

fn authored_send(store: &Store, actor: &Actor, to: &str, text: &str, key: &str) -> SendResponse {
    let body = SendRequest {
        sender: Some(actor.as_address()),
        to: vec![to.to_string()],
        text: text.to_string(),
        idempotency_key: Some(key.to_string()),
        ..Default::default()
    };
    let key = send_key(&body);
    let plan = SendPlan::parse(store, actor, &body).unwrap_or_else(|e| panic!("{}", e.message));
    send_sync(store, actor, body, plan, key)
        .unwrap_or_else(|e| panic!("{}", e.message))
        .0
}

#[test]
fn a_named_bot_is_part_of_the_send_key() {
    let body = |bot: Option<&str>| SendRequest {
        sender: Some("agent:ada".into()),
        to: vec!["telegram:chat:-1007".into()],
        text: "hello".into(),
        bot: bot.map(str::to_string),
        idempotency_key: Some("call-1".into()),
        ..Default::default()
    };
    let unnamed = send_key(&body(None)).unwrap();
    assert_eq!(send_key(&body(Some("  "))).unwrap(), unnamed);
    let desk = send_key(&body(Some("@ada_desk_bot"))).unwrap();
    assert_eq!(send_key(&body(Some("@ADA_DESK_BOT"))).unwrap(), desk);
    for other in ["@ada_promo_bot", "@bea_bot", "@nobody"] {
        let key = send_key(&body(Some(other))).unwrap();
        assert_ne!(key, desk, "{other}");
        assert_ne!(key, unnamed, "{other}");
    }
}

/// A send that cannot wake an archived recipient (through an alias, or to
/// several recipients) is refused for it with who answers for it, and
/// nothing is stored unread. A send with no live recipient fails.
#[test]
fn a_send_an_archived_agent_would_never_read_is_refused_with_its_parent() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let root = store
        .create_agent(anyagents_core::NewAgent::new("root", "opencode"))
        .unwrap();
    // Running, as an agent that answers for its archived child.
    store.set_agent_state(&root.id, "running").unwrap();
    let mut new = anyagents_core::NewAgent::new("lead", "opencode");
    new.parent_id = Some(root.id.clone());
    let lead = store.create_agent(new).unwrap();
    let mut new = anyagents_core::NewAgent::new("worker", "opencode");
    new.parent_id = Some(lead.id.clone());
    let worker = store.create_agent(new).unwrap();
    let peer = store
        .create_agent(anyagents_core::NewAgent::new("peer", "opencode"))
        .unwrap();
    // A public sub-agent may also write to a root agent.
    store.set_agent_scope(&worker.id, "public").unwrap();
    mark_explicitly_archived(&store, dir.path(), &lead);
    let actor = Actor::Agent(worker.name.clone());
    let send = |to: Vec<&str>| {
        let body = SendRequest {
            sender: Some(actor.as_address()),
            to: to.into_iter().map(str::to_string).collect(),
            text: "status report".to_string(),
            ..Default::default()
        };
        let plan = SendPlan::parse(&store, &actor, &body)?;
        send_sync(&store, &actor, body, plan, None).map(|response| response.0)
    };

    let refused = send(vec!["parent"]).expect_err("no live recipient");
    assert_eq!(refused.status, StatusCode::CONFLICT);
    assert!(
        refused
            .message
            .contains("agent `lead` is archived; its parent is `root`"),
        "{}",
        refused.message
    );
    assert!(
        store.inbox(&lead.id, None).unwrap().is_empty(),
        "nothing stored unread"
    );
    assert_eq!(store.agent(&lead.id).unwrap().state, "archived");

    let mixed =
        send(vec!["parent", "agent:peer"]).unwrap_or_else(|error| panic!("{}", error.message));
    assert_eq!(mixed.deliveries.len(), 1);
    assert_eq!(mixed.deliveries[0].to, "agent:peer");
    assert_eq!(mixed.skipped.len(), 1);
    assert_eq!(mixed.skipped[0].to, "agent:lead");
    assert_eq!(
        mixed.skipped[0].reason,
        "agent `lead` is archived; its parent is `root`"
    );
    assert!(store.inbox(&lead.id, None).unwrap().is_empty());
    assert_eq!(store.inbox(&peer.id, None).unwrap().len(), 1);
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

#[test]
fn closing_sends_do_not_store_messages_receipts_or_wake_archived_targets() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let sender = store
        .create_agent(anyagents_core::NewAgent::new("sender", "opencode"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("target", "opencode");
    new.parent_id = Some(sender.id.clone());
    let target = store.create_agent(new).unwrap();
    store.set_agent_state(&target.id, "archived").unwrap();
    let actor = Actor::Agent(sender.name.clone());
    for text in [
        "NO_REPLY",
        "End.",
        "停止。",
        "No more.",
        "。",
        "Done.",
        "終了。",
        "Final!",
    ] {
        let response = authored_send(&store, &actor, "agent:target", text, "closing-only");
        assert!(response.deliveries.is_empty());
        assert!(response.skipped.is_empty());
        assert!(store.inbox(&target.id, None).unwrap().is_empty());
        let key = send_key(&SendRequest {
            sender: Some(actor.as_address()),
            to: vec!["agent:target".into()],
            text: text.into(),
            idempotency_key: Some("closing-only".into()),
            ..Default::default()
        })
        .unwrap();
        assert!(store.send_receipt(&key).unwrap().is_none());
        assert_eq!(store.agent(&target.id).unwrap().state, "archived");
    }
    // A real message containing a closing word must still be delivered.
    store.set_agent_state(&target.id, "running").unwrap();
    let response = authored_send(
        &store,
        &actor,
        "agent:target",
        "Done. Build passed.",
        "real-message",
    );
    assert_eq!(response.deliveries.len(), 1);
    assert_eq!(store.inbox(&target.id, None).unwrap().len(), 1);
}

#[test]
fn direct_authored_sends_wake_archived_agents_once_in_mailbox_order_and_survive_reopen() {
    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.db");
    let store = Store::open(&db).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent-agent", "opencode"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("worker", "opencode");
    new.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new).unwrap();
    let actor = Actor::Agent(parent.name.clone());
    store
        .receive(&child.id, Incoming::new("queued before archive"))
        .unwrap();
    store.set_agent_state(&child.id, "archived").unwrap();
    mark_explicitly_archived(&store, dir.path(), &child);
    let cfg = anyagents_core::agent_config_dir(dir.path(), &child.id);
    for file in [
        ".anyagents-last-error.json",
        ".anyagents-notified-parent-error.json",
    ] {
        std::fs::write(cfg.join(file), "old error").unwrap();
    }
    // An authored quotation of a lifecycle prefix is still a direct send:
    // bodies are not the wake discriminator.
    let sent = authored_send(
        &store,
        &actor,
        "agent:worker",
        "[Sub-Agent Completed] quoted in a follow-up",
        "direct-1",
    );
    assert_eq!(sent.deliveries[0].seq, 2);
    assert_eq!(store.agent(&child.id).unwrap().state, "running");
    assert!(!has_explicit_archive_marker(dir.path(), &child.id));
    assert!(!cfg.join(".anyagents-last-error.json").exists());
    assert!(!cfg.join(".anyagents-notified-parent-error.json").exists());
    assert!(anyagents_core::agent_work_dir(dir.path(), &child.id).exists());
    let retried = authored_send(
        &store,
        &actor,
        "agent:worker",
        "[Sub-Agent Completed] quoted in a follow-up",
        "direct-1",
    );
    assert_eq!(retried.deliveries[0].id, sent.deliveries[0].id);
    assert_eq!(store.inbox(&child.id, None).unwrap().len(), 2);
    // Replay after a new archive is not a fresh wake instruction.
    store.set_agent_state(&child.id, "archived").unwrap();
    mark_explicitly_archived(&store, dir.path(), &child);
    authored_send(
        &store,
        &actor,
        "agent:worker",
        "[Sub-Agent Completed] quoted in a follow-up",
        "direct-1",
    );
    assert_eq!(store.agent(&child.id).unwrap().state, "archived");
    assert!(has_explicit_archive_marker(dir.path(), &child.id));
    // Operator-authored direct sends also work by immutable id.
    authored_send(
        &store,
        &Actor::Operator("operator".into()),
        child.id.as_str(),
        "operator follow-up",
        "direct-2",
    );
    drop(store);
    let store = Store::open(&db).unwrap();
    assert_eq!(store.agent(&child.id).unwrap().state, "running");
    assert!(!has_explicit_archive_marker(dir.path(), &child.id));
    let inbox = store.inbox(&child.id, None).unwrap();
    assert_eq!(
        inbox.iter().map(|m| m.seq).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(inbox[2].text, "operator follow-up");
    mark_explicitly_archived(&store, dir.path(), &child);
    let replayed = authored_send(
        &store,
        &Actor::Operator("operator".into()),
        child.id.as_str(),
        "operator follow-up",
        "direct-2",
    );
    assert_eq!(replayed.deliveries[0].seq, 3);
    assert_eq!(store.agent(&child.id).unwrap().state, "archived");
    assert!(has_explicit_archive_marker(dir.path(), &child.id));
    // Simulate a stop after mailbox commit but before whole-send receipt.
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute("DELETE FROM sends", [])
        .unwrap();
    std::fs::write(
        cfg.join(".anyagents-last-error.json"),
        "new error after archive",
    )
    .unwrap();
    let replayed = authored_send(
        &store,
        &Actor::Operator("operator".into()),
        child.id.as_str(),
        "operator follow-up",
        "direct-2",
    );
    assert!(replayed.deliveries[0].duplicate);
    assert_eq!(store.agent(&child.id).unwrap().state, "archived");
    assert!(has_explicit_archive_marker(dir.path(), &child.id));
    assert!(cfg.join(".anyagents-last-error.json").exists());
    assert_eq!(store.inbox(&child.id, None).unwrap().len(), 3);
    unsafe {
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
    }
}

#[test]
fn archived_agents_stay_retired_on_channel_senders_aliases_groups_and_deleted_targets() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent-agent", "opencode"))
        .unwrap();
    store.set_agent_state(&parent.id, "running").unwrap();
    let mut new = anyagents_core::NewAgent::new("worker", "opencode");
    new.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new).unwrap();
    store.set_agent_state(&child.id, "archived").unwrap();
    mark_explicitly_archived(&store, dir.path(), &child);
    store.join_group("crew", &parent.id).unwrap();
    store.join_group("crew", &child.id).unwrap();
    let operator = Actor::Operator("operator".into());
    for to in ["all", "group:all", "group:crew"] {
        authored_send(&store, &operator, to, "broadcast", to);
        assert_eq!(store.agent(&child.id).unwrap().state, "archived");
    }
    let actor = Actor::Agent(parent.name.clone());
    for to in ["children", "group:children", "siblings"] {
        let body = SendRequest {
            to: vec![to.into()],
            ..Default::default()
        };
        assert!(SendPlan::parse(&store, &actor, &body).is_err());
    }
    for from in ["telegram:user:123", "telegram:chat:-456"] {
        // A channel sender cannot wake it, so the send is refused with
        // the reason instead of stored unread.
        let channel = Actor::parse(from).unwrap_or_else(|e| panic!("{}", e.message));
        let body = SendRequest {
            sender: Some(channel.as_address()),
            to: vec!["worker".into()],
            text: "channel update".into(),
            idempotency_key: Some(from.into()),
            ..Default::default()
        };
        let key = send_key(&body);
        let plan =
            SendPlan::parse(&store, &channel, &body).unwrap_or_else(|e| panic!("{}", e.message));
        let refused = send_sync(&store, &channel, body, plan, key).expect_err("archived");
        assert_eq!(refused.status, StatusCode::CONFLICT);
        assert!(
            refused
                .message
                .contains("agent `worker` is archived; its parent is `parent-agent`"),
            "{}",
            refused.message
        );
        assert_eq!(store.agent(&child.id).unwrap().state, "archived");
        assert!(has_explicit_archive_marker(dir.path(), &child.id));
    }
    assert!(
        store
            .inbox(&child.id, None)
            .unwrap()
            .iter()
            .all(|message| message.text != "channel update"),
        "nothing is stored unread"
    );
    let deleted_id = child.id.clone();
    store.delete_agent(&deleted_id).unwrap();
    let body = SendRequest {
        to: vec![deleted_id.to_string()],
        ..Default::default()
    };
    let plan = SendPlan::parse(&store, &actor, &body).unwrap_or_else(|e| panic!("{}", e.message));
    let error = send_sync(&store, &actor, body, plan, None).expect_err("deleted target");
    assert_eq!(error.status, StatusCode::NOT_FOUND);
    assert!(store.agent(&deleted_id).is_err());
}
#[test]
fn direct_send_and_archive_serialize_across_store_connections() {
    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("state.db");
    let store = Store::open(&db).unwrap();
    let recipient = store
        .create_agent(anyagents_core::NewAgent::new("recipient", "opencode"))
        .unwrap();
    store.set_agent_state(&recipient.id, "running").unwrap();
    let guard = store.lock_agent_lifecycle(&recipient.id).unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let other = Store::open(&db).unwrap();
    let sender = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let receipt = authored_send(
            &other,
            &Actor::Operator("operator".into()),
            "recipient",
            "follow-up",
            "interleave",
        );
        done_tx.send(receipt).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_millis(30))
            .is_err()
    );
    store.set_agent_state(&recipient.id, "archived").unwrap();
    mark_explicitly_archived_locked(&store, dir.path(), &recipient).unwrap();
    drop(guard);
    let receipt = done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    sender.join().unwrap();
    assert_eq!(receipt.deliveries[0].seq, 1);
    assert_eq!(store.agent(&recipient.id).unwrap().state, "running");
    assert!(!has_explicit_archive_marker(dir.path(), &recipient.id));
    // The converse ordering leaves a coherent archive, not running+marker.
    let guard = store.lock_agent_lifecycle(&recipient.id).unwrap();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let other = Store::open(&db).unwrap();
    let archived_id = recipient.id.clone();
    let archive = std::thread::spawn(move || {
        let recipient = other.agent(&archived_id).unwrap();
        mark_explicitly_archived(&other, other.data_dir(), &recipient);
        done_tx.send(()).unwrap();
    });
    assert!(
        done_rx
            .recv_timeout(std::time::Duration::from_millis(30))
            .is_err()
    );
    drop(guard);
    done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    archive.join().unwrap();
    assert_eq!(store.agent(&recipient.id).unwrap().state, "archived");
    assert!(has_explicit_archive_marker(dir.path(), &recipient.id));
    unsafe {
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
    }
}
#[test]
fn archetype_templates_apply_frontmatter_defaults_and_prepend_instructions() {
    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    let parent_work = dir.path().join("parent-work");
    let agents_dir = parent_work.join(".agents").join("agents");
    std::fs::create_dir_all(&agents_dir).unwrap();
    std::fs::write(
        agents_dir.join("writer.md"),
        "---\ndescription: Ephemeral isolated-worktree sole writer\nscope: isolated\nworkdir: private\nharness: opencode\nmodel: gpt-5.4-mini\n---\nAlways run cargo test before opening a PR.",
    )
    .unwrap();
    std::fs::write(
        agents_dir.join("code-reviewer.md"),
        "---\ndescription: Read-only code reviewer\nscope: siblings\nharness: claude-code\n---\nVerify all invariants before approving.",
    )
    .unwrap();

    let data_templates = dir.path().join("agents-templates");
    std::fs::create_dir_all(&data_templates).unwrap();
    std::fs::write(
        data_templates.join("auditor.md"),
        "---\ndescription: Global template auditor\n---\nAudit template body.",
    )
    .unwrap();

    let mut new_parent = anyagents_core::NewAgent::new("calliope", "claude-code");
    new_parent.role = Some("Mind".into());
    new_parent.workdir = Some(parent_work.to_string_lossy().into_owned());
    new_parent.description = Some("Parent mind".into());
    let parent = store.create_agent(new_parent).unwrap();
    let state = AppState {
        store: Arc::clone(&store),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        auth: auth::Mode::Trusted {
            data_dir: dir.path().into(),
        },
    };

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (status, Json(created)) = create_agent_route(
            Principal::Agent {
                agent_id: parent.id.clone(),
                name: parent.name.clone(),
            },
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "writer-sub-1",
                    "type": "writer",
                    "instructions": "Fix issue #42."
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            created.description.as_deref(),
            Some("Ephemeral isolated-worktree sole writer")
        );
        assert_eq!(created.scope, "isolated");
        assert_eq!(created.harness, "opencode");
        assert_eq!(created.model.as_deref(), Some("gpt-5.4-mini"));
        let stored = store.agent_by_name("writer-sub-1").unwrap();
        assert_eq!(stored.instructions.as_deref(), Some("Fix issue #42."));
        assert_eq!(
            created.instructions.as_deref(),
            Some(
                format!(
                    "{}\n\nAlways run cargo test before opening a PR.\n\nFix issue #42.",
                    create::BUILTIN_GENERAL_ROLE_BODY
                )
                .as_str()
            )
        );

        // Role matching a template slug (`role: "code-reviewer"`) applies template defaults
        // when `archetype` is omitted, projecting template instructions dynamically.
        let (_, Json(reviewer)) = create_agent_route(
            Principal::Agent {
                agent_id: parent.id.clone(),
                name: parent.name.clone(),
            },
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "reviewer-sub-1",
                    "role": "code-reviewer"
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
        assert_eq!(
            reviewer.description.as_deref(),
            Some("Read-only code reviewer")
        );
        assert_eq!(reviewer.scope, "siblings");
        let reviewer_stored = store.agent_by_name("reviewer-sub-1").unwrap();
        assert!(
            reviewer_stored.instructions.is_none(),
            "template body must not be baked into sqlite: {:?}",
            reviewer_stored.instructions
        );
        assert_eq!(
            reviewer.instructions.as_deref(),
            Some(
                format!(
                    "{}\n\nVerify all invariants before approving.",
                    create::BUILTIN_GENERAL_ROLE_BODY
                )
                .as_str()
            )
        );

        // `<data_dir>/agents-templates/<name>.md` is also searched.
        let (_, Json(auditor)) = create_agent_route(
            Principal::Agent {
                agent_id: parent.id.clone(),
                name: parent.name.clone(),
            },
            State(state),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "auditor-sub-1",
                    "template": "auditor"
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
        assert_eq!(
            auditor.description.as_deref(),
            Some("Global template auditor")
        );
    });
    unsafe {
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
    }
}

#[test]
fn archetype_absence_preserves_http_harness_defaults_and_explicit_inheritance() {
    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let dir = tempfile::tempdir().unwrap();
    let state = archetype_validation_state(dir.path());
    let parent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("parent", "opencode"))
        .unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for (name, parent_ref, harness, expected) in [
            ("root-default", "root", None, "claude-code"),
            ("child-default", parent.id.as_str(), None, "claude-code"),
            ("child-empty", parent.id.as_str(), Some(""), "opencode"),
            ("child-inherit", parent.id.as_str(), Some("inherit"), "opencode"),
            ("child-explicit", parent.id.as_str(), Some("codex"), "codex"),
        ] {
            let mut body = serde_json::json!({"name": name, "parent": parent_ref, "workdir": dir.path().join(name), "scope": "team"});
            if let Some(harness) = harness { body["harness"] = serde_json::json!(harness); }
            let (status, Json(created)) = create_agent_route(
                Principal::Operator, State(state.clone()), axum::http::HeaderMap::new(), Json(serde_json::from_value(body).unwrap()),
            ).await.unwrap_or_else(|e| panic!("{}", e.message));
            assert_eq!(status, StatusCode::CREATED);
            assert_eq!(created.harness, expected, "{name}");
            assert_eq!(created.scope, "siblings");
        }
        // An explicit unknown role must fail closed even when no custom catalog exists on disk.
        let before_count = state.store.list_agents().unwrap().len();
        let unknown_body = serde_json::json!({
            "name": "unmatched-label",
            "parent": parent.id.as_str(),
            "role": "nonexistent-display-label-134",
            "workdir": dir.path().join("unmatched-label"),
        });
        let err = create_agent_route(
            Principal::Operator,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(serde_json::from_value(unknown_body).unwrap()),
        )
        .await
        .expect_err("unknown explicit role must fail closed");
        assert_eq!(err.status, StatusCode::BAD_REQUEST, "{}", err.message);
        assert!(
            err.message
                .contains("unknown role 'nonexistent-display-label-134'"),
            "{}",
            err.message
        );
        assert!(
            err.message.contains("available roles: [general]"),
            "{}",
            err.message
        );
        assert_eq!(state.store.list_agents().unwrap().len(), before_count);
    });
    unsafe {
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
    }
}

fn archetype_validation_state(data: &Path) -> AppState {
    let store = Arc::new(Store::open(data.join("state.db")).unwrap());
    store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    AppState {
        store,
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
        auth: auth::Mode::Trusted {
            data_dir: data.into(),
        },
    }
}

async fn assert_archetype_create_refused(
    state: &AppState,
    body: serde_json::Value,
    expected_error: &str,
) {
    let result = create_agent_route(
        Principal::Operator,
        State(state.clone()),
        axum::http::HeaderMap::new(),
        Json(serde_json::from_value(body).unwrap()),
    )
    .await;
    let error = match result {
        Ok(_) => panic!("invalid archetype unexpectedly created an agent"),
        Err(error) => error,
    };
    assert_eq!(error.status, StatusCode::BAD_REQUEST, "{}", error.message);
    assert!(error.message.contains(expected_error), "{}", error.message);
    assert!(!error.message.contains("OUTSIDE_TEMPLATE_SENTINEL"));
    assert!(state.store.list_agents().unwrap().is_empty());
    let Json(listed) = list_agents(
        Principal::Operator,
        State(state.clone()),
        Query(ListAgentsQuery::default()),
    )
    .await
    .unwrap_or_else(|error| panic!("{}", error.message));
    assert!(listed.agents.is_empty());
    assert!(
        !serde_json::to_string(&listed)
            .unwrap()
            .contains("OUTSIDE_TEMPLATE_SENTINEL")
    );
    let description = get_description_route(
        Principal::Operator,
        State(state.clone()),
        AxumPath("rejected-template-agent".into()),
    )
    .await;
    match description {
        Ok(_) => panic!("rejected template must not have a description"),
        Err(error) => assert_eq!(error.status, StatusCode::NOT_FOUND),
    }
    // Validation runs before creation, including config/SYSTEM_PROMPT.md writes.
    let agents_dir = state.store.data_dir().join("agents");
    assert!(
        !agents_dir.exists() || std::fs::read_dir(agents_dir).unwrap().next().is_none(),
        "a rejected template must not leave a SYSTEM_PROMPT.md or agent directory"
    );
}

#[test]
fn create_agent_route_defaults_omitted_and_empty_role_to_builtin_general() {
    let _env = env_lock();
    let prev_home = std::env::var_os("HOME");
    let prev_owner = std::env::var_os("ANYAGENTS_OWNER_WORKDIR");
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let work = dir.path().join("work");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&work).unwrap();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        std::env::set_var("HOME", &home);
        std::env::remove_var("ANYAGENTS_OWNER_WORKDIR");
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let state = archetype_validation_state(dir.path());
        for (name, extra) in [
            ("http-omitted", serde_json::json!({})),
            ("http-empty", serde_json::json!({ "role": "" })),
            ("http-general", serde_json::json!({ "role": "general" })),
        ] {
            let mut body = serde_json::json!({
                "name": name,
                "workdir": work,
            });
            if let Some(obj) = extra.as_object() {
                for (k, v) in obj {
                    body[k] = v.clone();
                }
            }
            let (status, Json(agent)) = create_agent_route(
                Principal::Operator,
                State(state.clone()),
                axum::http::HeaderMap::new(),
                Json(serde_json::from_value(body).unwrap()),
            )
            .await
            .unwrap_or_else(|e| panic!("{name}: {}: {}", e.status, e.message));
            assert_eq!(status, StatusCode::CREATED, "{name}");
            assert_eq!(agent.role.as_deref(), Some("general"), "{name}");
            assert_eq!(agent.scope, "public", "{name}");
            assert_eq!(
                agent.instructions.as_deref(),
                Some(create::BUILTIN_GENERAL_ROLE_BODY),
                "{name}"
            );
        }

        // Sub-agent with omitted role gets built-in `general` with `private` scope.
        let (sub_status, Json(sub)) = create_agent_route(
            Principal::Operator,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "http-subagent",
                    "parent": "http-omitted",
                    "workdir": work,
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("subagent: {}: {}", e.status, e.message));
        assert_eq!(sub_status, StatusCode::CREATED);
        assert_eq!(sub.role.as_deref(), Some("general"));
        assert_eq!(sub.scope, "private");
        assert_eq!(
            sub.instructions.as_deref(),
            Some(create::BUILTIN_GENERAL_ROLE_BODY)
        );

        // Explicit `public: true` / `public: false` without `scope` on a sub-agent.
        for (public_flag, expected_scope) in [(true, "public"), (false, "private")] {
            let (flag_status, Json(flagged)) = create_agent_route(
                Principal::Operator,
                State(state.clone()),
                axum::http::HeaderMap::new(),
                Json(
                    serde_json::from_value(serde_json::json!({
                        "name": format!("http-sub-public-{public_flag}"),
                        "parent": "http-omitted",
                        "public": public_flag,
                        "workdir": work,
                    }))
                    .unwrap(),
                ),
            )
            .await
            .unwrap_or_else(|e| panic!("public={public_flag}: {}: {}", e.status, e.message));
            assert_eq!(flag_status, StatusCode::CREATED);
            assert_eq!(flagged.role.as_deref(), Some("general"));
            assert_eq!(flagged.scope, expected_scope);
        }

        // Caller `description` without `instructions` composes with built-in `general`.
        let (legacy_status, Json(legacy_sub)) = create_agent_route(
            Principal::Operator,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "http-sub-legacy-desc",
                    "parent": "http-omitted",
                    "description": "Legacy HTTP brief",
                    "workdir": work,
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("legacy: {}: {}", e.status, e.message));
        assert_eq!(legacy_status, StatusCode::CREATED);
        assert_eq!(legacy_sub.role.as_deref(), Some("general"));
        assert_eq!(legacy_sub.scope, "private");
        assert_eq!(
            legacy_sub.description.as_deref(),
            Some("Legacy HTTP brief")
        );
        assert_eq!(
            legacy_sub.instructions.as_deref(),
            Some(
                format!("{}\n\nLegacy HTTP brief", create::BUILTIN_GENERAL_ROLE_BODY)
                    .as_str()
            )
        );

        // Workspace .agents/agents/general.md overrides the built-in template even for root.
        std::fs::create_dir_all(work.join(".agents/agents")).unwrap();
        std::fs::write(
            work.join(".agents/agents/general.md"),
            "---\nname: general\ndescription: HTTP General Override\nscope: private\n---\nHTTP general override body.\n",
        )
        .unwrap();
        let (status, Json(overridden)) = create_agent_route(
            Principal::Operator,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "http-overridden",
                    "workdir": work,
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("override: {}: {}", e.status, e.message));
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(overridden.role.as_deref(), Some("general"));
        assert_eq!(overridden.scope, "private");
        assert_eq!(
            overridden.description.as_deref(),
            Some("HTTP General Override")
        );
        assert!(
            overridden
                .instructions
                .as_deref()
                .is_some_and(|text| text.contains("HTTP general override body."))
        );
    });

    unsafe {
        match prev_home {
            Some(v) => std::env::set_var("HOME", v),
            None => std::env::remove_var("HOME"),
        }
        match prev_owner {
            Some(v) => std::env::set_var("ANYAGENTS_OWNER_WORKDIR", v),
            None => std::env::remove_var("ANYAGENTS_OWNER_WORKDIR"),
        }
    }
}

#[cfg(unix)]
#[test]
fn archetype_templates_reject_file_and_directory_symlinks_without_leaking() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for variant in [
            "file",
            "agents-dir",
            "dot-agents-dir",
            "nested-dir",
            "data-dir",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let state = archetype_validation_state(dir.path());
            let work = dir.path().join("work");
            let outside = dir.path().join("outside");
            std::fs::create_dir_all(&outside).unwrap();
            let secret = outside.join("escape.md");
            std::fs::write(&secret, "OUTSIDE_TEMPLATE_SENTINEL").unwrap();
            std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::write(outside.join("agent.md"), "OUTSIDE_TEMPLATE_SENTINEL").unwrap();
            match variant {
                "file" => {
                    std::fs::create_dir_all(work.join(".agents/agents")).unwrap();
                    symlink(&secret, work.join(".agents/agents/escape.md")).unwrap();
                }
                "agents-dir" => {
                    std::fs::create_dir_all(work.join(".agents")).unwrap();
                    symlink(&outside, work.join(".agents/agents")).unwrap();
                }
                "dot-agents-dir" => {
                    std::fs::create_dir_all(&work).unwrap();
                    std::fs::create_dir_all(outside.join("agents")).unwrap();
                    std::fs::write(
                        outside.join("agents/escape.md"),
                        "OUTSIDE_TEMPLATE_SENTINEL",
                    )
                    .unwrap();
                    symlink(&outside, work.join(".agents")).unwrap();
                }
                "nested-dir" => {
                    std::fs::create_dir_all(work.join(".agents/agents")).unwrap();
                    symlink(&outside, work.join(".agents/agents/escape")).unwrap();
                }
                "data-dir" => {
                    std::fs::create_dir_all(&work).unwrap();
                    symlink(&outside, dir.path().join("agents-templates")).unwrap();
                }
                _ => unreachable!(),
            }
            // A lower-priority valid template must never mask a hostile path.
            std::fs::create_dir_all(dir.path().join("archetypes")).unwrap();
            std::fs::write(dir.path().join("archetypes/escape.md"), "Safe fallback.").unwrap();
            for selector in ["archetype", "role"] {
                let mut body = serde_json::json!({
                    "name": "rejected-template-agent",
                    "workdir": work,
                    "harness": "codex",
                });
                body[selector] = serde_json::json!("escape");
                assert_archetype_create_refused(&state, body, "symlink").await;
            }
        }
    });
    unsafe {
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
    }
}

#[test]
fn archetype_templates_reject_unknown_explicit_names_and_malformed_frontmatter() {
    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let dir = tempfile::tempdir().unwrap();
    let state = archetype_validation_state(dir.path());
    let work = dir.path().join("work");
    let templates = work.join(".agents/agents");
    std::fs::create_dir_all(&templates).unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for selector in ["archetype", "template", "type", "preset"] {
            let mut body =
                serde_json::json!({"name": "rejected-template-agent", "workdir": work});
            body[selector] = serde_json::json!("nonexistent-role");
            assert_archetype_create_refused(
                &state,
                body,
                "unknown archetype template \"nonexistent-role\"",
            )
            .await;
        }
        assert_archetype_create_refused(
            &state,
            serde_json::json!({"name": "rejected-template-agent", "scope": "invalid-request-scope", "workdir": work}),
            "invalid scope `invalid-request-scope`",
        ).await;
        for name in [
            ".",
            "..",
            "../escape",
            "escape.md",
            "escape/agent",
            "escape\\agent",
        ] {
            assert_archetype_create_refused(
                &state,
                serde_json::json!({
                    "name": "rejected-template-agent", "archetype": name, "workdir": work,
                }),
                "invalid archetype template name",
            )
            .await;
        }
        for (raw, expected) in [
            ("---\ndescription: unclosed", "unclosed YAML frontmatter"),
            (
                "---\nscpoe: isolated\n---\nUNKNOWN_KEY_BODY",
                "unknown archetype template frontmatter key \"scpoe\"",
            ),
            (
                "---\nscope: invalid-review-scope\n---\nINVALID_SCOPE_BODY",
                "invalid scope `invalid-review-scope`",
            ),
            (
                "---\ndescription broken\n---\nBody.",
                "malformed YAML frontmatter",
            ),
            (
                "---\ndescription: [broken\n---\nBody.",
                "malformed YAML frontmatter",
            ),
            (
                "---\ndescription: broken\n---not-a-delimiter\nBody.",
                "unclosed YAML frontmatter",
            ),
        ] {
            std::fs::write(templates.join("broken.md"), raw).unwrap();
            for selector in ["archetype", "role"] {
                let mut body =
                    serde_json::json!({"name": "rejected-template-agent", "workdir": work});
                body[selector] = serde_json::json!("broken");
                assert_archetype_create_refused(&state, body, expected).await;
            }
        }
    });
    unsafe {
        std::env::remove_var("ANYAGENTS_FAKE_MODE");
    }
}

#[test]
fn memory_topic_upsert_syncs_index_and_supports_direct_o1_topic_read() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let vulcan = state
        .store
        .create_agent(anyagents_core::NewAgent::new("vulcan", "agy"))
        .unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        // 1. Seed initial index so the 60s refresh cache is populated before upsert.
        let _ = state.store.sync_agent_memory_index(&vulcan);

        // 2. Upsert a topic via conversation_route.
        let Json(upserted) = conversation_route(
            Principal::Agent {
                agent_id: vulcan.id.clone(),
                name: vulcan.name.clone(),
            },
            State(state.clone()),
            AxumPath(vulcan.name.clone()),
            Query(ConversationQuery {
                scope: anyagents_core::host_index::MemoryScope::Memory,
                topic: Some("test-invariants".into()),
                description: Some("Vulcan system invariants and verified receipts".into()),
                content: Some("# Test Invariants\n\nVerified invariant alpha-beta-gamma.\n".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));

        assert_eq!(upserted.turns.len(), 1);
        let turn = &upserted.turns[0];
        assert!(
            turn.raw_bytes.is_none(),
            "upsert response must not bloat payload with raw_bytes"
        );
        let raw = turn.raw.as_ref().expect("raw coordinate");
        assert_eq!(raw.seq, 1);

        // 3. Immediate raw_id + raw_sha256 readback (Read-Your-Own-Writes within 60s window).
        let Json(raw_read) = conversation_route(
            Principal::Agent {
                agent_id: vulcan.id.clone(),
                name: vulcan.name.clone(),
            },
            State(state.clone()),
            AxumPath(vulcan.name.clone()),
            Query(ConversationQuery {
                scope: anyagents_core::host_index::MemoryScope::Memory,
                raw_id: Some(turn.id.clone()),
                raw_sha256: Some(raw.sha256.clone()),
                raw_offset: Some(0),
                raw_length: Some(32),
                ..Default::default()
            }),
        )
        .await
        .unwrap_or_else(|e| {
            panic!(
                "immediate raw_id readback failed: {}: {}",
                e.status, e.message
            )
        });
        assert_eq!(raw_read.turns.len(), 1);
        assert!(
            raw_read.turns[0].raw_bytes.is_some(),
            "explicit raw_id slice read must include raw_bytes"
        );

        // 4. Direct O(1) read by topic (without content or delete).
        let Json(topic_read) = conversation_route(
            Principal::Agent {
                agent_id: vulcan.id.clone(),
                name: vulcan.name.clone(),
            },
            State(state.clone()),
            AxumPath(vulcan.name.clone()),
            Query(ConversationQuery {
                scope: anyagents_core::host_index::MemoryScope::Memory,
                topic: Some("test-invariants".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap_or_else(|e| panic!("direct topic read failed: {}: {}", e.status, e.message));
        assert_eq!(topic_read.turns.len(), 1);
        assert!(
            topic_read.turns[0]
                .text
                .contains("Verified invariant alpha-beta-gamma.")
        );
        assert!(topic_read.turns[0].raw_bytes.is_none());
        assert_eq!(
            topic_read.turns[0].reply_to.as_deref(),
            Some("test-invariants.md")
        );

        // 5. Lexical search in scope="memory" returns the topic file with reply_to filename
        // and does not return duplicate MEMORY.md index hits.
        let Json(searched) = conversation_route(
            Principal::Agent {
                agent_id: vulcan.id.clone(),
                name: vulcan.name.clone(),
            },
            State(state.clone()),
            AxumPath(vulcan.name.clone()),
            Query(ConversationQuery {
                scope: anyagents_core::host_index::MemoryScope::Memory,
                query: Some("invariants".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap_or_else(|e| panic!("memory search failed: {}: {}", e.status, e.message));
        assert_eq!(searched.turns.len(), 1);
        assert_eq!(
            searched.turns[0].reply_to.as_deref(),
            Some("test-invariants.md")
        );
    });
}

#[test]
fn active_seat_ids_excludes_archived_agents_from_startup_boundary() {
    let data = crate::test_tempdir();
    let store = Store::open(data.path().join("state.db")).unwrap();
    let active = store
        .create_agent(anyagents_core::NewAgent::new("active-root", "codex"))
        .unwrap();
    let archived = store
        .create_agent(anyagents_core::NewAgent::new("archived-root", "codex"))
        .unwrap();
    store.set_agent_state(&archived.id, "archived").unwrap();
    let seats = active_seat_ids(&store).unwrap();
    assert_eq!(seats, vec![active.id.to_string()]);
}

#[test]
fn streamable_http_mcp_enforces_bearer_auth_and_three_mode_profiles() {
    use http_body_util::BodyExt as _;

    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let cloud_agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new(
            "cloud-worker",
            "claude-cloud",
        ))
        .unwrap();
    let spiron_agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new(
            "spiron-worker",
            "spiron-cloud",
        ))
        .unwrap();
    // HTTP MCP takes remote session tokens only; a local capability is
    // refused below.
    let remote_token = |agent: &Agent| {
        crate::remote::begin_session(&state.store, agent).unwrap();
        crate::remote::session_token(&anyagents_core::agent_config_dir(
            state.store.data_dir(),
            &agent.id,
        ))
        .unwrap()
    };
    let cloud_cap = remote_token(&cloud_agent);
    let spiron_cap = remote_token(&spiron_agent);
    let local_cap = state.store.issue_capability(&cloud_agent.id).unwrap();

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        // Verify both `build_router` and `build_webhook_router` compile and bind `/mcp`.
        let _main_app = build_router(state.clone());
        let _webhook_app = build_webhook_router(state.clone());

        // 1. Missing or invalid bearer token returns 401 Unauthorized.
        let unauth_err = mcp_post_route(
            State(state.clone()),
            axum::http::HeaderMap::new(),
            axum::body::Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {}
                }))
                .unwrap(),
            ),
        )
        .await
        .expect_err("missing auth must fail with 401");
        assert_eq!(unauth_err.status, StatusCode::UNAUTHORIZED);

        let mut bad_headers = axum::http::HeaderMap::new();
        bad_headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer invalid-capability-token".parse().unwrap(),
        );
        let bad_err = mcp_post_route(
            State(state.clone()),
            bad_headers,
            axum::body::Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {}
                }))
                .unwrap(),
            ),
        )
        .await
        .expect_err("invalid token must fail with 401");
        assert_eq!(bad_err.status, StatusCode::UNAUTHORIZED);

        // A valid local capability is refused with 403: local agents use
        // stdio MCP, which runs as the agent, never HTTP inside the server.
        for header in [
            axum::http::header::AUTHORIZATION.as_str(),
            crate::operator::AGENT_HEADER,
        ] {
            let mut local_headers = axum::http::HeaderMap::new();
            let value = if header == "authorization" {
                format!("Bearer {local_cap}")
            } else {
                local_cap.clone()
            };
            local_headers.insert(
                axum::http::HeaderName::from_static(header),
                value.parse().unwrap(),
            );
            let refused = mcp_post_route(
                State(state.clone()),
                local_headers,
                axum::body::Bytes::from(
                    serde_json::to_vec(&serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/list",
                        "params": {}
                    }))
                    .unwrap(),
                ),
            )
            .await
            .expect_err("a local capability must be refused over HTTP");
            assert_eq!(refused.status, StatusCode::FORBIDDEN, "{header}");
            assert!(
                refused.message.contains("only remote session tokens"),
                "{}",
                refused.message
            );
        }

        // Helper to call `mcp_post_route` with a Bearer token.
        let post_mcp = |token: &str, body: serde_json::Value| {
            let state = state.clone();
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {token}").parse().unwrap(),
            );
            let raw = axum::body::Bytes::from(serde_json::to_vec(&body).unwrap());
            async move {
                let resp = mcp_post_route(State(state), headers, raw).await.unwrap();
                let status = resp.status();
                let bytes = resp.into_body().collect().await.unwrap().to_bytes();
                let json: serde_json::Value = if bytes.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&bytes).unwrap()
                };
                (status, json)
            }
        };

        // 2. GET /mcp returns 200 OK text/event-stream by default and JSON when Accept: application/json.
        let mut get_headers = axum::http::HeaderMap::new();
        get_headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {cloud_cap}").parse().unwrap(),
        );
        let sse_resp = mcp_get_route(State(state.clone()), get_headers)
            .await
            .unwrap();
        assert_eq!(sse_resp.status(), StatusCode::OK);
        assert!(
            sse_resp
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("text/event-stream")
        );

        // 3. POST /mcp with valid Bearer token exposes the exact 7 universal MCP tools.
        let (status, init_json) = post_mcp(
            &cloud_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(init_json["result"]["serverInfo"]["name"], "anyagents");

        let (status, list_json) = post_mcp(
            &cloud_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let remote_tools: Vec<String> = list_json["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect();
        let expected_universal: Vec<String> = crate::mcp::UNIVERSAL_MCP_TOOLS
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        assert_eq!(remote_tools, expected_universal);

        // Calling `send`, `inbox`, `agents`, `memory` succeeds on the universal surface.
        let (_, send_res) = post_mcp(
            &cloud_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "send",
                    "arguments": {
                        "to": ["spiron-worker"],
                        "text": "hello from remote mcp"
                    }
                }
            }),
        )
        .await;
        assert_ne!(
            send_res["result"]["isError"],
            serde_json::json!(true),
            "{send_res}"
        );

        let (_, inbox_res) = post_mcp(
            &spiron_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "inbox",
                    "arguments": { "limit": 10 }
                }
            }),
        )
        .await;
        assert_ne!(
            inbox_res["result"]["isError"],
            serde_json::json!(true),
            "{inbox_res}"
        );
        assert_eq!(
            inbox_res["result"]["structuredContent"]["messages"][0]["text"],
            "hello from remote mcp"
        );

        let (_, agents_res) = post_mcp(
            &cloud_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "agents",
                    "arguments": { "action": "list" }
                }
            }),
        )
        .await;
        assert_ne!(
            agents_res["result"]["isError"],
            serde_json::json!(true),
            "{agents_res}"
        );

        let (_, mem_res) = post_mcp(
            &cloud_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "memory",
                    "arguments": {
                        "action": "write",
                        "topic": "remote-note",
                        "description": "Note written over remote MCP",
                        "content": "# Remote Note\nVerified.\n"
                    }
                }
            }),
        )
        .await;
        assert_ne!(
            mem_res["result"]["isError"],
            serde_json::json!(true),
            "{mem_res}"
        );

        // 4. `spiron` receives the exact same 7-tool universal MCP surface as every other harness,
        // and removed/non-core tools are blocked uniformly.
        let (_, spiron_default_json) = post_mcp(
            &spiron_cap,
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/list",
                "params": {}
            }),
        )
        .await;
        let spiron_default_tools: Vec<String> = spiron_default_json["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(spiron_default_tools, expected_universal);

        for omitted in [
            "code",
            "files",
            "skills",
            "web",
            "web_search",
            "web_fetch",
            "browser",
            "computer",
            "device",
            "media",
        ] {
            assert!(!spiron_default_tools.contains(&omitted.to_string()));
            let (_, err_json) = post_mcp(
                &spiron_cap,
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "tools/call",
                    "params": {
                        "name": omitted,
                        "arguments": {}
                    }
                }),
            )
            .await;
            assert!(
                err_json["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("not available on the AnyAgents MCP surface"),
                "expected `{omitted}` to be blocked on universal MCP surface, got {err_json}"
            );
        }
    });
}

/// One MCP POST with a bearer token.
async fn post_mcp_as(
    state: &AppState,
    token: &str,
    _profile: Option<&str>,
    body: serde_json::Value,
) -> Result<(StatusCode, serde_json::Value), ApiError> {
    use http_body_util::BodyExt as _;
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {token}").parse().unwrap(),
    );
    let raw = axum::body::Bytes::from(serde_json::to_vec(&body).unwrap());
    let resp = mcp_post_route(State(state.clone()), headers, raw).await?;
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    Ok((status, json))
}

fn listed_tools(list: &serde_json::Value) -> Vec<String> {
    list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect()
}

fn tools_list() -> serde_json::Value {
    serde_json::json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {} })
}

fn tool_call(name: &str, arguments: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": { "name": name, "arguments": arguments }
    })
}

#[tokio::test]
async fn fleet_schedules_enforce_owner_authority_and_reject_all_mutations() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let store = &state.store;
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("fleet-parent", "codex"))
        .unwrap();
    let child = store
        .create_agent(anyagents_core::NewAgent {
            parent_id: Some(parent.id.clone()),
            ..anyagents_core::NewAgent::new("fleet-child", "codex")
        })
        .unwrap();
    let unrelated = store
        .create_agent(anyagents_core::NewAgent::new("fleet-unrelated", "codex"))
        .unwrap();
    for agent in [&parent, &child, &unrelated] {
        store
            .create_schedule(anyagents_core::NewSchedule {
                agent_id: agent.id.clone(),
                kind: "watch".into(),
                spec: "file:/tmp/fleet-fixture".into(),
                prompt: format!("private prompt for {}", agent.name),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: None,
            })
            .unwrap();
    }
    let principal = Principal::Agent {
        agent_id: parent.id.clone(),
        name: parent.name.clone(),
    };
    let child_principal = Principal::Agent {
        agent_id: child.id.clone(),
        name: child.name.clone(),
    };
    assert_eq!(
        authorize_schedule_target(store, &principal, Some(unrelated.id.as_str()))
            .unwrap_err()
            .status,
        StatusCode::FORBIDDEN
    );
    for (actor, expected) in [
        (&principal, 2),
        (&child_principal, 1),
        (&Principal::Operator, 3),
    ] {
        for target in ["all", "ALL", " agent:All "] {
            let request: ScheduleRequest =
                serde_json::from_value(serde_json::json!({"action": "list", "agent": target}))
                    .unwrap();
            let result = handle_schedule_action(actor.clone(), state.clone(), request)
                .await
                .unwrap()
                .0;
            let rows = result["schedules"].as_array().unwrap();
            assert_eq!(rows.len(), expected);
            for row in rows {
                assert!(row["agent_name"].is_string());
                assert!(row["agent_id"].is_string());
                assert!(row.get("next_fire_at_ms").unwrap().is_null());
                assert!(row.get("last_observed").unwrap().is_null());
                if expected < 3 {
                    assert_ne!(row["agent_name"], unrelated.name);
                }
            }
        }
    }
    // Both HTTP GET and the MCP-backed action share the same authorized population.
    let get = list_schedules_route(
        principal.clone(),
        State(state.clone()),
        AxumPath("all".into()),
    )
    .await
    .unwrap()
    .0;
    assert_eq!(get["schedules"].as_array().unwrap().len(), 2);
    for actor in [&principal, &Principal::Operator] {
        for payload in [
            serde_json::json!({"action": "create", "at": "1h", "prompt": "no"}),
            serde_json::json!({"action": "at", "at": "1h", "prompt": "no"}),
            serde_json::json!({"action": "every", "every": "1h", "prompt": "no"}),
            serde_json::json!({"action": "watch", "watch": "file:/tmp/no", "prompt": "no"}),
            serde_json::json!({"action": "cancel", "id": "all"}),
            serde_json::json!({"at": "1h", "prompt": "no"}),
        ] {
            let mut payload = payload;
            payload["agent"] = serde_json::json!("all");
            let request = serde_json::from_value(payload).unwrap();
            assert_eq!(
                handle_schedule_action(actor.clone(), state.clone(), request)
                    .await
                    .unwrap_err()
                    .status,
                StatusCode::BAD_REQUEST
            );
        }
    }
    assert_eq!(store.list_schedules(None, true).unwrap().len(), 3);
    assert!(authorize_schedule_target(store, &Principal::Operator, None).is_err());
    // Only top-level agents can hold administrative authority.
    assert!(store.set_agent_admin(&child.id, true).is_err());
    assert_eq!(
        list_fleet_schedules(store, &child_principal).unwrap()["schedules"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    store.set_agent_admin(&parent.id, true).unwrap();
    assert_eq!(
        list_fleet_schedules(store, &principal).unwrap()["schedules"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    store.set_agent_admin(&parent.id, false).unwrap();
    assert_eq!(
        list_fleet_schedules(store, &principal).unwrap()["schedules"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    // An unreadable caller is an error, not a fleet-wide visibility grant.
    let absent = Principal::Agent {
        agent_id: anyagents_core::AgentId::generate(),
        name: "missing".into(),
    };
    assert!(list_fleet_schedules(store, &absent).is_err());
}

#[test]
fn remote_session_tokens_get_universal_mcp_surface() {
    use http_body_util::BodyExt as _;
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let cloud = state
        .store
        .create_agent(anyagents_core::NewAgent::new(
            "cloud-remote",
            "claude-cloud",
        ))
        .unwrap();
    let peer = state
        .store
        .create_agent(anyagents_core::NewAgent::new("local-peer", "claude-code"))
        .unwrap();
    let peer_cap = state.store.issue_capability(&peer.id).unwrap();
    crate::remote::begin_session(&state.store, &cloud).unwrap();
    let cloud_cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &cloud.id);
    let remote = crate::remote::session_token(&cloud_cfg).unwrap();
    let expected_universal: Vec<String> = crate::mcp::UNIVERSAL_MCP_TOOLS
        .iter()
        .map(|s| (*s).to_string())
        .collect();
    assert_eq!(
        expected_universal,
        [
            "agents",
            "send",
            "inbox",
            "conversation",
            "memory",
            "schedules",
            "groups",
        ]
    );

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (status, list) = post_mcp_as(&state, &remote, None, tools_list())
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed_tools(&list), expected_universal);

        // Nothing outside the 7 canonical tools can be called, even by vendor prefix.
        for host_tool in [
            "browser",
            "computer",
            "device",
            "media",
            "files",
            "web",
            "web_search",
            "web_fetch",
            "code",
            "skills",
            "mcp__anyagents__browser",
            "mcp__anyagents__computer",
        ] {
            let (_, refused) = post_mcp_as(
                &state,
                &remote,
                None,
                tool_call(host_tool, serde_json::json!({})),
            )
            .await
            .unwrap();
            assert!(
                refused["error"]["message"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("not available on the AnyAgents MCP surface"),
                "{host_tool}: {refused}"
            );
        }

        // Coordination works, as the agent the token names.
        let (_, sent) = post_mcp_as(
            &state,
            &remote,
            None,
            tool_call(
                "send",
                serde_json::json!({ "to": ["local-peer"], "text": "from the cloud" }),
            ),
        )
        .await
        .unwrap();
        assert_ne!(sent["result"]["isError"], serde_json::json!(true), "{sent}");
        let inbox = state.store.inbox(&peer.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].text, "from the cloud");
        assert!(
            inbox[0]
                .sender
                .as_deref()
                .is_some_and(|sender| sender.contains(cloud.id.as_str())),
            "{:?}",
            inbox[0].sender
        );

        // The GET route names the session, its expiry, and the 7 universal tools.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {remote}").parse().unwrap(),
        );
        headers.insert(
            axum::http::header::ACCEPT,
            "application/json".parse().unwrap(),
        );
        let described = mcp_get_route(State(state.clone()), headers).await.unwrap();
        let bytes = described.into_body().collect().await.unwrap().to_bytes();
        let described: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(described["agent"], "cloud-remote");
        assert_eq!(
            described["tools"],
            serde_json::json!(crate::mcp::UNIVERSAL_MCP_TOOLS)
        );
        let grant = state.store.authenticate_remote_session(&remote).unwrap();
        assert_eq!(described["remote_session"]["session_id"], grant.session_id);
        assert_eq!(
            described["remote_session"]["expires_at_ms"],
            grant.expires_at_ms
        );

        // Over HTTP a local capability is refused outright.
        let refused = post_mcp_as(&state, &peer_cap, None, tools_list())
            .await
            .unwrap_err();
        assert_eq!(refused.status, StatusCode::FORBIDDEN);
    });
}

#[test]
fn remote_session_tokens_stop_at_expiry_revocation_relaunch_and_stop() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let cloud = state
        .store
        .create_agent(anyagents_core::NewAgent::new(
            "cloud-expiring",
            "claude-cloud",
        ))
        .unwrap();
    let cloud_cfg = anyagents_core::agent_config_dir(state.store.data_dir(), &cloud.id);
    let begin = || {
        crate::remote::begin_session(&state.store, &cloud).unwrap();
        crate::remote::session_token(&cloud_cfg).unwrap()
    };
    let set_expiry = |at_ms: i64| {
        rusqlite::Connection::open(dir.path().join("state.db"))
            .unwrap()
            .execute(
                "UPDATE remote_session_tokens SET expires_at_ms = ?1",
                [at_ms],
            )
            .unwrap();
    };

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let accepted = |token: String| {
            let state = state.clone();
            async move {
                post_mcp_as(&state, &token, None, tools_list())
                    .await
                    .map(|(status, _)| status)
                    .map_err(|err| (err.status, err.message))
            }
        };
        let refused = |result: Result<StatusCode, (StatusCode, String)>| {
            let (status, message) = result.unwrap_err();
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert!(message.contains("remote session token"), "{message}");
        };

        // Expiry.
        let first = begin();
        assert_eq!(accepted(first.clone()).await.unwrap(), StatusCode::OK);
        set_expiry(anyagents_core::now_ms() - 1);
        refused(accepted(first.clone()).await);

        // On demand, through the operator route.
        let second = begin();
        assert_eq!(accepted(second.clone()).await.unwrap(), StatusCode::OK);
        let revoked = revoke_remote_sessions_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath("cloud-expiring".into()),
            Json(RevokeRemoteSessionsRequest::default()),
        )
        .await
        .unwrap();
        assert_eq!(revoked.0["revoked"], 1);
        refused(accepted(second.clone()).await);
        assert!(!cloud_cfg.join(crate::remote::TOKEN_FILE).exists());

        // A relaunch supersedes the previous session.
        let third = begin();
        let fourth = begin();
        refused(accepted(third).await);
        assert_eq!(accepted(fourth.clone()).await.unwrap(), StatusCode::OK);

        // Pausing (stop) and archiving revoke in the store commit itself.
        state.store.set_agent_state(&cloud.id, "paused").unwrap();
        refused(accepted(fourth.clone()).await);
        state.store.set_agent_state(&cloud.id, "running").unwrap();
        refused(accepted(fourth).await);
        let fifth = begin();
        assert_eq!(accepted(fifth.clone()).await.unwrap(), StatusCode::OK);
        state.store.set_agent_state(&cloud.id, "archived").unwrap();
        refused(accepted(fifth).await);

        // A remote token is never a credential on the internal API.
        state.store.set_agent_state(&cloud.id, "running").unwrap();
        let sixth = begin();
        let principal = state.auth.principal(&state.store, &{
            let request = axum::http::Request::builder()
                .header(crate::operator::AGENT_HEADER, sixth.as_str())
                .body(())
                .unwrap();
            let (parts, ()) = request.into_parts();
            crate::auth::Presented::of(&parts)
        });
        assert!(principal.is_err(), "{principal:?}");
    });
}

#[test]
fn event_webhook_routes_authenticate_original_bytes() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let watcher = state
        .store
        .create_agent(anyagents_core::NewAgent::new("deploy-bot", "codex"))
        .unwrap();
    state.store.set_agent_running(&watcher.id, "/work").unwrap();
    crate::schedule::create_agent_schedule(
        &state.store,
        &state.health,
        &watcher,
        None,
        None,
        Some("webhook:deploy/prod"),
        "verify deployment",
    )
    .unwrap();
    let config = state.store.data_dir().join("anyagents.toml");
    std::fs::write(&config, "[server]\nwebhook_secret = 'secret'\n").unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        use hyper::service::Service as _;
        for router in [
            build_router(state.clone()),
            build_public_router(state.clone()),
            build_webhook_router(state.clone()),
        ] {
            let service = hyper_util::service::TowerToHyperService::new(router);
            for (body, signed, expected) in [
                (r#"{"release":"v2"}"#, true, StatusCode::ACCEPTED),
                ("{", false, StatusCode::UNAUTHORIZED),
                ("{", true, StatusCode::BAD_REQUEST),
            ] {
                let signature = if signed {
                    format!(
                        "sha256={}",
                        anyagents_core::hmac_sha256_hex(b"secret", body.as_bytes())
                    )
                } else {
                    "sha256=invalid".into()
                };
                let request = hyper::Request::builder()
                    .method("POST")
                    .uri("/v1/webhooks/events/deploy/prod")
                    .header("x-anyagents-signature-256", signature)
                    .header("x-anyagents-delivery", format!("route-{}", body.len()))
                    .body(http_body_util::Full::new(bytes::Bytes::copy_from_slice(
                        body.as_bytes(),
                    )))
                    .unwrap();
                assert_eq!(service.call(request).await.unwrap().status(), expected);
            }
        }
        assert_eq!(state.store.inbox(&watcher.id, None).unwrap().len(), 1);
        std::fs::write(&config, "[server]\nwebhook_secret = ''\n").unwrap();
        let service = hyper_util::service::TowerToHyperService::new(build_webhook_router(state));
        let request = hyper::Request::builder()
            .method("POST")
            .uri("/v1/webhooks/events/deploy/prod")
            .body(http_body_util::Full::new(bytes::Bytes::new()))
            .unwrap();
        assert_eq!(
            service.call(request).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    });
}

#[test]
fn the_public_listener_answers_readiness_only_and_the_socket_keeps_the_identity() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let socket_identity = state.identity.status();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let public = build_webhook_router(state.clone());
        let served = tokio::spawn(async move {
            let _ = axum::serve(listener, public).await;
        });
        for path in ["/", "/healthz", "/v1/webhooks/healthz"] {
            let raw = tokio::task::spawn_blocking(move || {
                use std::io::{Read as _, Write as _};
                let mut stream = std::net::TcpStream::connect(addr).unwrap();
                write!(
                    stream,
                    "GET {path} HTTP/1.1\r\nHost: public\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                let mut raw = String::new();
                stream.read_to_string(&mut raw).unwrap();
                raw
            })
            .await
            .unwrap();
            assert!(raw.starts_with("HTTP/1.1 200"), "{path}: {raw}");
            assert!(
                raw.to_ascii_lowercase().contains("cache-control: no-store"),
                "{path}: {raw}"
            );
            let body: serde_json::Value =
                serde_json::from_str(raw.split_once("\r\n\r\n").unwrap().1).unwrap();
            assert_eq!(body, serde_json::json!({ "ok": true }), "{path}");
        }
        served.abort();
    });
    // The operator's socket still serves the full serving identity.
    for field in [
        "version",
        "source_sha",
        "binary_sha256",
        "binary_sha256_state",
    ] {
        assert!(socket_identity.get(field).is_some(), "{field}");
    }
}

#[test]
fn remote_agents_cannot_be_created_without_a_public_url() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let refused = create_agent_route(
            Principal::Operator,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(
                serde_json::from_value(serde_json::json!({
                    "name": "cloud-unconfigured",
                    "harness": "claude-cloud",
                    "parent": "root",
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(refused.status, StatusCode::BAD_REQUEST);
        assert!(
            refused.message.contains("public_url"),
            "{}",
            refused.message
        );
        assert!(
            refused.message.contains(crate::remote::CONFIG_FILE),
            "{}",
            refused.message
        );
        assert!(state.store.agent_by_name("cloud-unconfigured").is_err());

        // Moving an existing agent onto a remote harness is refused too.
        state
            .store
            .create_agent(anyagents_core::NewAgent::new("local-first", "claude-code"))
            .unwrap();
        let moved = harness_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath("local-first".into()),
            axum::http::HeaderMap::new(),
            Json(HarnessRequest {
                harness: "openai-cloud".into(),
                model: None,
                profile: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(moved.status, StatusCode::BAD_REQUEST);
        assert!(moved.message.contains("public_url"), "{}", moved.message);
        assert_eq!(
            state.store.agent_by_name("local-first").unwrap().harness,
            "claude-code"
        );
    });
}

/// One HTTP/1.1 exchange with a TCP listener: the status and the JSON body
/// (`Null` when there is none).
fn raw_http(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, serde_json::Value) {
    use std::io::{Read as _, Write as _};
    let mut stream = std::net::TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(30)))
        .unwrap();
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: public.example\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if !body.is_empty() {
        request.push_str("Content-Type: application/json\r\n");
    }
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes()).unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).unwrap();
    let (head, rest) = raw.split_once("\r\n\r\n").unwrap();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    (
        status,
        serde_json::from_str(rest).unwrap_or(serde_json::Value::Null),
    )
}

#[test]
fn the_public_listener_serves_only_the_public_surface_and_remote_tokens() {
    use anyagents_core::CredentialStore as _;
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\npublic_listen = \"127.0.0.1:0\"\n",
    )
    .unwrap();
    let cloud = state
        .store
        .create_agent(anyagents_core::NewAgent::new(
            "cloud-public",
            "claude-cloud",
        ))
        .unwrap();
    let local = state
        .store
        .create_agent(anyagents_core::NewAgent::new("local-public", "claude-code"))
        .unwrap();
    let local_cap = state.store.issue_capability(&local.id).unwrap();
    crate::remote::begin_session(&state.store, &cloud).unwrap();
    let remote =
        crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &cloud.id))
            .unwrap();
    // A Telegram channel, so the webhook route reaches its secret check.
    let channel = state
        .store
        .create_channel(anyagents_core::NewChannel {
            kind: "telegram".into(),
            identity: None,
            config: "{}".into(),
            credential_ref: None,
        })
        .unwrap();
    let cred_ref = format!("channels/{}", channel.id);
    anyagents_core::FileCredentialStore::new(dir.path())
        .write(&cred_ref, b"777:fake-token-not-real")
        .unwrap();
    state
        .store
        .set_credential_ref(&channel.id, &cred_ref)
        .unwrap();
    state.store.attach_channel(&channel.id, &local.id).unwrap();
    let telegram_path = format!("/v1/webhooks/telegram/{}", channel.id);

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let public = build_public_router(state.clone());
        let served = tokio::spawn(async move {
            let _ = axum::serve(listener, public).await;
        });
        let call = move |method: &'static str,
                         path: String,
                         headers: Vec<(&'static str, String)>,
                         body: String| {
            tokio::task::spawn_blocking(move || {
                let headers: Vec<(&str, &str)> =
                    headers.iter().map(|(k, v)| (*k, v.as_str())).collect();
                raw_http(addr, method, &path, &headers, &body)
            })
        };
        let list = tools_list().to_string();

        for path in ["/", "/healthz", "/v1/webhooks/healthz"] {
            let (status, body) = call("GET", path.into(), vec![], String::new())
                .await
                .unwrap();
            assert_eq!(status, 200, "{path}");
            assert_eq!(body, serde_json::json!({ "ok": true }), "{path}");
        }

        // Nothing of the local API, nor the MCP aliases, is reachable here,
        // whatever credential is presented.
        for (method, path) in [
            ("GET", "/v1/agents"),
            ("GET", "/v1/whoami"),
            ("POST", "/v1/send"),
            ("POST", "/v1/browser"),
            ("POST", "/v1/operator/agents/local-public/capability"),
            (
                "POST",
                "/v1/operator/agents/cloud-public/remote-sessions/revoke",
            ),
            ("POST", "/v1/mcp"),
            ("POST", "/mcp"),
            ("GET", "/v1/webhooks/unknown"),
        ] {
            // No body: a 404 closes without reading one, and unread bytes
            // would turn that close into a reset on some platforms.
            let (status, _) = call(
                method,
                path.into(),
                vec![("x-anyagents-agent", local_cap.clone())],
                String::new(),
            )
            .await
            .unwrap();
            assert_eq!(status, 404, "{method} {path}");
        }

        // MCP takes remote session tokens only. A local capability is
        // refused in either header, and no forwarding header changes that.
        for headers in [
            vec![("authorization", format!("Bearer {local_cap}"))],
            vec![("x-anyagents-agent", local_cap.clone())],
            vec![
                ("authorization", format!("Bearer {local_cap}")),
                ("x-forwarded-for", "127.0.0.1".to_string()),
                ("forwarded", "for=127.0.0.1".to_string()),
            ],
        ] {
            let (status, body) = call("POST", "/v1/webhooks/mcp".into(), headers, list.clone())
                .await
                .unwrap();
            assert_eq!(status, 401, "{body}");
            assert!(
                body["error"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("only remote session tokens"),
                "{body}"
            );
        }
        let (status, body) = call(
            "GET",
            "/v1/webhooks/mcp".into(),
            vec![("authorization", format!("Bearer {local_cap}"))],
            String::new(),
        )
        .await
        .unwrap();
        assert_eq!(status, 401, "{body}");

        let (status, body) = call(
            "POST",
            "/v1/webhooks/mcp".into(),
            vec![("authorization", format!("Bearer {remote}"))],
            list.clone(),
        )
        .await
        .unwrap();
        assert_eq!(status, 200, "{body}");
        assert_eq!(listed_tools(&body), crate::mcp::UNIVERSAL_MCP_TOOLS);

        // Telegram's webhook is verified by the channel's secret token here
        // too: a missing or wrong secret is refused before any delivery.
        let update = serde_json::json!({ "update_id": 1 }).to_string();
        for headers in [
            vec![],
            vec![("x-telegram-bot-api-secret-token", "wrong".to_string())],
        ] {
            let (status, _) = call("POST", telegram_path.clone(), headers, update.clone())
                .await
                .unwrap();
            assert_eq!(status, 401);
        }
        assert!(state.store.inbox(&local.id, None).unwrap().is_empty());
        served.abort();
    });

    // The local listener refuses a local capability too (#272), with 403
    // there, and serves the remote token.
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let refused = post_mcp_as(&state, &local_cap, None, tools_list())
            .await
            .unwrap_err();
        assert_eq!(refused.status, StatusCode::FORBIDDEN);
        let (status, _) = post_mcp_as(&state, &remote, None, tools_list())
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
    });
}

#[test]
fn the_local_listener_keeps_its_default_until_a_public_listener_exists() {
    // Unset public listener: today's behaviour.
    assert_eq!(
        local_listen_addr(None, false, false).as_deref(),
        Some("0.0.0.0:8788")
    );
    // A configured public listener moves the local default to loopback.
    assert_eq!(
        local_listen_addr(None, false, true).as_deref(),
        Some("127.0.0.1:8788")
    );
    // An explicit address or `off` always wins; test doubles bind nothing.
    assert_eq!(
        local_listen_addr(Some(" 127.0.0.1:9000 "), false, true).as_deref(),
        Some("127.0.0.1:9000")
    );
    assert_eq!(local_listen_addr(Some("off"), false, false), None);
    assert_eq!(local_listen_addr(None, true, false), None);
    assert!(binds_loopback("127.0.0.1:8788"));
    assert!(binds_loopback("[::1]:8788"));
    assert!(binds_loopback("localhost:8788"));
    assert!(!binds_loopback("0.0.0.0:8788"));
    assert!(!binds_loopback("10.0.0.5:8788"));
}

#[test]
fn serverless_listen_addr_honors_port_precedence_and_binds_all_interfaces() {
    // Explicit `ANYAGENTS_WEBHOOK_LISTEN` always takes priority.
    assert_eq!(
        local_listen_addr_with_port(
            Some("127.0.0.1:9999"),
            Some(9090),
            Some(8081),
            Some(8080),
            true,
            false,
            true,
        )
        .as_deref(),
        Some("127.0.0.1:9999")
    );
    // CLI `--port` beats `ANYAGENTS_PORT` and `PORT`.
    assert_eq!(
        local_listen_addr_with_port(
            None,
            Some(9090),
            Some(8081),
            Some(8080),
            false,
            false,
            false
        )
        .as_deref(),
        Some("0.0.0.0:9090")
    );
    // `ANYAGENTS_PORT` beats `PORT`.
    assert_eq!(
        local_listen_addr_with_port(None, None, Some(8081), Some(8080), false, false, false)
            .as_deref(),
        Some("0.0.0.0:8081")
    );
    // Standard `PORT` fallback binds `0.0.0.0:${PORT}` even if `public_configured`.
    assert_eq!(
        local_listen_addr_with_port(None, None, None, Some(8080), false, false, true).as_deref(),
        Some("0.0.0.0:8080")
    );
    // `ANYAGENTS_SERVERLESS=true` binds `0.0.0.0:${PORT}` even when `public_configured`.
    assert_eq!(
        local_listen_addr_with_port(None, Some(8080), None, None, true, false, true).as_deref(),
        Some("0.0.0.0:8080")
    );
    assert_eq!(
        local_listen_addr_with_port(None, None, None, None, true, false, true).as_deref(),
        Some("0.0.0.0:8788")
    );
}

#[test]
fn a_remote_token_stops_authenticating_once_its_agent_runs_on_the_host() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let cloud = state
        .store
        .create_agent(anyagents_core::NewAgent::new("cloud-moved", "claude-cloud"))
        .unwrap();
    let sandboxed = state
        .store
        .create_agent(anyagents_core::NewAgent::new("sandboxed", "claude-code"))
        .unwrap();
    let sandbox_cfg = anyagents_core::agent_config_dir(dir.path(), &sandboxed.id);
    agent_tools::SandboxConfig {
        endpoint: "https://sandbox.example.com".into(),
        sandbox_id: "sbx-1".into(),
        token: None,
        workdir: "/workspace".into(),
        remote_spawn: true,
    }
    .write_to_config_dir(&sandbox_cfg)
    .unwrap();
    assert!(crate::remote::runs_off_host(&sandboxed, &sandbox_cfg));
    assert!(crate::remote::runs_off_host(
        &cloud,
        &anyagents_core::agent_config_dir(dir.path(), &cloud.id)
    ));

    crate::remote::begin_session(&state.store, &cloud).unwrap();
    let remote =
        crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &cloud.id))
            .unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (status, _) = post_mcp_as(&state, &remote, None, tools_list())
            .await
            .unwrap();
        assert_eq!(status, StatusCode::OK);
        state
            .store
            .set_agent_harness(&cloud.id, "claude-code")
            .unwrap();
        let err = post_mcp_as(&state, &remote, None, tools_list())
            .await
            .unwrap_err();
        assert_eq!(err.status, StatusCode::UNAUTHORIZED);
    });
}

#[test]
fn http_mcp_serves_only_universal_seven_tools_and_blocks_host_tools() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let local = state
        .store
        .create_agent(anyagents_core::NewAgent::new("seat-agent", "claude-cloud"))
        .unwrap();
    crate::remote::begin_session(&state.store, &local).unwrap();
    let cap =
        crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &local.id))
            .unwrap();
    let cfg = anyagents_core::agent_config_dir(dir.path(), &local.id);
    std::fs::create_dir_all(&cfg).unwrap();
    std::fs::write(cfg.join("cloud.json"), r#"{"disable_builtin_tools":true}"#).unwrap();
    agent_tools::SandboxConfig {
        endpoint: "https://sandbox.example.com".into(),
        sandbox_id: "sbx-1".into(),
        token: None,
        workdir: "/workspace".into(),
        remote_spawn: false,
    }
    .write_to_config_dir(&cfg)
    .unwrap();
    let marker = dir.path().join("exec-ran");

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let (status, list) = post_mcp_as(&state, &cap, None, tools_list()).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed_tools(&list), crate::mcp::UNIVERSAL_MCP_TOOLS);

        let exec = serde_json::json!({
            "action": "exec",
            "command": format!("touch {}", marker.display()),
        });
        for (tool, args) in [
            ("files", exec.clone()),
            ("mcp__anyagents__files", exec.clone()),
            ("browser", serde_json::json!({ "action": "status" })),
            ("computer", serde_json::json!({ "action": "screenshot" })),
            ("device", serde_json::json!({ "action": "screenshot" })),
            ("media", serde_json::json!({ "action": "speech" })),
            ("web", serde_json::json!({ "url": "https://example.com" })),
            ("web_search", serde_json::json!({ "query": "x" })),
            (
                "web_fetch",
                serde_json::json!({ "url": "https://example.com" }),
            ),
            ("code", serde_json::json!({ "code": "return 1;" })),
            ("skills", serde_json::json!({ "action": "list" })),
        ] {
            let (_, refused) = post_mcp_as(&state, &cap, None, tool_call(tool, args))
                .await
                .unwrap();
            assert!(
                refused["error"]["message"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("not available on the AnyAgents MCP surface"),
                "{tool}: {refused}"
            );
        }
        assert!(!marker.exists(), "an HTTP call ran a host command");
    });
}

#[test]
fn calls_over_http_mcp_cannot_start_or_read_on_the_host() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let local = state
        .store
        .create_agent(anyagents_core::NewAgent::new("http-local", "claude-code"))
        .unwrap();
    let local_cap = state.store.issue_capability(&local.id).unwrap();
    let cloud = state
        .store
        .create_agent(anyagents_core::NewAgent::new("http-cloud", "claude-cloud"))
        .unwrap();
    crate::remote::begin_session(&state.store, &cloud).unwrap();
    let remote =
        crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &cloud.id))
            .unwrap();
    let mut children = Vec::new();
    {
        let parent = &cloud;
        let child = state
            .store
            .create_agent(anyagents_core::NewAgent {
                parent_id: Some(parent.id.clone()),
                ..anyagents_core::NewAgent::new(format!("{}-child", parent.name), "claude-cloud")
            })
            .unwrap();
        let sleeper = state
            .store
            .create_agent(anyagents_core::NewAgent {
                parent_id: Some(parent.id.clone()),
                ..anyagents_core::NewAgent::new(format!("{}-sleeper", parent.name), "claude-code")
            })
            .unwrap();
        state
            .store
            .set_agent_state(&sleeper.id, "archived")
            .unwrap();
        children.push((child, sleeper));
    }
    let secret = dir.path().join("host-secret");
    std::fs::write(&secret, "not for a remote caller").unwrap();

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        // A local capability never reaches these tools over HTTP.
        let refused_local = post_mcp_as(
            &state,
            &local_cap,
            None,
            tool_call("agents", serde_json::json!({ "action": "list" })),
        )
        .await
        .unwrap_err();
        assert_eq!(refused_local.status, StatusCode::FORBIDDEN);
        {
            let token = &remote;
            let (child, sleeper) = &children[0];
            let call = |tool: &str, args: serde_json::Value| {
                let state = state.clone();
                let token = token.to_string();
                let request = tool_call(tool, args);
                async move {
                    post_mcp_as(&state, &token, None, request)
                        .await
                        .unwrap()
                        .1
                }
            };
            let refused = |result: &serde_json::Value, why: &str| {
                assert_eq!(result["result"]["isError"], true, "{result}");
                assert!(result.to_string().contains(why), "{why}: {result}");
            };

            // Spawning a host-executed agent, through either tool.
            let prefix = token.chars().take(6).collect::<String>();
            let named = |n: &str| format!("host-{prefix}-{n}");
            let attempts = vec![
                (
                    "agents",
                    serde_json::json!({ "action": "spawn", "name": named("a"), "harness": "claude-code" }),
                    named("a"),
                ),
                (
                    "spawn",
                    serde_json::json!({ "name": named("c"), "harness": "codex" }),
                    named("c"),
                ),
            ];
            for (tool, args, name) in attempts {
                refused(&call(tool, args).await, "only on a cloud harness");
                assert!(state.store.agent_by_name(&name).is_err(), "{name}");
            }
            // Moving a child onto a host harness.
            refused(
                &call(
                    "agents",
                    serde_json::json!({ "action": "harness", "name": child.name, "harness": "claude-code" }),
                )
                .await,
                "only on a cloud harness",
            );
            assert_eq!(state.store.agent(&child.id).unwrap().harness, "claude-cloud");
            // Waking an archived host agent, explicitly or by a direct send.
            refused(
                &call("wake", serde_json::json!({ "agent": sleeper.name })).await,
                "cannot wake it",
            );
            let _ = call(
                "send",
                serde_json::json!({ "to": [sleeper.name], "text": "wake up" }),
            )
            .await;
            assert_eq!(state.store.agent(&sleeper.id).unwrap().state, "archived");
            // Attaching a host file as media, by path or by bare name.
            for source in [secret.display().to_string(), "operator-token".to_string()] {
                refused(
                    &call(
                        "send",
                        serde_json::json!({
                            "to": [child.name],
                            "text": "here",
                            "media": { "kind": "document", "source": source },
                        }),
                    )
                    .await,
                    "by `https://` URL only",
                );
            }
            // A URL attachment and an ordinary send still work.
            let sent = call(
                "send",
                serde_json::json!({
                    "to": [child.name],
                    "text": "with a link",
                    "media": { "kind": "photo", "source": "https://example.com/a.png" },
                }),
            )
            .await;
            assert_ne!(sent["result"]["isError"], true, "{sent}");
            assert!(
                state
                    .store
                    .inbox(&child.id, None)
                    .unwrap()
                    .iter()
                    .any(|message| message.text.contains("with a link"))
            );
        }
    });
}

/// The review's probe, kept as a regression: a seat plants links in the
/// memory and skill directories it can write, then asks over HTTP MCP,
/// where the tools run as the server. A local capability is refused
/// outright; a remote session token's reads never follow the links.
#[test]
fn http_mcp_never_reads_a_link_a_seat_planted() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.com\"\n",
    )
    .unwrap();
    let canary = "OPERATOR-SECRET-CANARY-7731";
    let secrets = dir.path().join("secrets");
    std::fs::create_dir_all(&secrets).unwrap();
    let secret = secrets.join("operator-token");
    std::fs::write(&secret, canary).unwrap();
    // Makes the linked directory look like a skill to discovery.
    std::fs::write(secrets.join("SKILL.md"), "---\nname: x\n---\n").unwrap();

    let seat = state
        .store
        .create_agent(anyagents_core::NewAgent::new("mem-seat", "claude-code"))
        .unwrap();
    let seat_cap = state.store.issue_capability(&seat.id).unwrap();
    let cloud = state
        .store
        .create_agent(anyagents_core::NewAgent::new("mem-cloud", "claude-cloud"))
        .unwrap();
    for agent in [&seat, &cloud] {
        let config = anyagents_core::agent_config_dir(dir.path(), &agent.id);
        std::fs::create_dir_all(&config).unwrap();
        let root = config.parent().unwrap().to_path_buf();
        let memory = root.join("memory");
        std::fs::create_dir_all(&memory).unwrap();
        std::os::unix::fs::symlink(&secret, memory.join("MEMORY.md")).unwrap();
        std::os::unix::fs::symlink(&secret, memory.join("leak.md")).unwrap();
        let overlay = root.join("skills").join("leaky");
        std::fs::create_dir_all(&overlay).unwrap();
        std::os::unix::fs::symlink(&secret, overlay.join("SKILL.md")).unwrap();
        let view = root.join("skills-view");
        std::fs::create_dir_all(&view).unwrap();
        std::os::unix::fs::symlink(&secrets, view.join("borrowed")).unwrap();
    }
    crate::remote::begin_session(&state.store, &cloud).unwrap();
    let remote =
        crate::remote::session_token(&anyagents_core::agent_config_dir(dir.path(), &cloud.id))
            .unwrap();

    let probes = [
        ("memory", serde_json::json!({ "action": "list" })),
        (
            "memory",
            serde_json::json!({ "action": "read", "topic": "leak" }),
        ),
        (
            "memory",
            serde_json::json!({ "action": "read", "topic": "MEMORY" }),
        ),
        ("memory", serde_json::json!({ "action": "status" })),
        ("skills", serde_json::json!({ "action": "list" })),
        (
            "skills",
            serde_json::json!({ "action": "read", "name": "leaky" }),
        ),
        (
            "skills",
            serde_json::json!({ "action": "read", "name": "borrowed", "file": "operator-token" }),
        ),
        (
            "skills",
            serde_json::json!({ "action": "search", "query": "CANARY" }),
        ),
    ];
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for (tool, args) in &probes {
            let refused = post_mcp_as(&state, &seat_cap, None, tool_call(tool, args.clone()))
                .await
                .unwrap_err();
            assert_eq!(refused.status, StatusCode::FORBIDDEN, "{tool} {args}");
            assert!(!refused.message.contains(canary));

            let (status, body) = post_mcp_as(&state, &remote, None, tool_call(tool, args.clone()))
                .await
                .unwrap();
            assert_eq!(status, StatusCode::OK, "{tool} {args}");
            assert!(
                !body.to_string().contains(canary),
                "{tool} {args} leaked: {body}"
            );
        }
    });
}

#[test]
fn spiron_and_sylphx_share_compatible_harnesses_and_seed_auth_json_paths() {
    let dir = crate::test_tempdir();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let sylphx_profile = data.join("profiles/sylphx/default/.config/sylphx");
    std::fs::create_dir_all(&sylphx_profile).unwrap();
    std::fs::write(
        sylphx_profile.join("auth.json"),
        r#"{"access_token":"spiron-shared-token-123","org_id":"org_shared"}"#,
    )
    .unwrap();

    assert_eq!(
        crate::create::compatible_harnesses_for("spiron"),
        &["spiron", "spiron-cloud", "sylphx"]
    );
    assert_eq!(
        crate::create::compatible_harnesses_for("spiron-cloud"),
        &["spiron", "spiron-cloud", "sylphx"]
    );
    assert_eq!(
        crate::create::compatible_harnesses_for("sylphx"),
        &["spiron", "spiron-cloud", "sylphx"]
    );

    let agent = store
        .create_agent(anyagents_core::NewAgent::new("spiron-seat", "spiron"))
        .unwrap();
    let target_cfg = data.join("agents").join(agent.id.as_str());
    std::fs::create_dir_all(&target_cfg).unwrap();
    crate::create::seed_harness_credentials(&store, data, &agent, &target_cfg).unwrap();
    crate::spiron::write_spiron_config(
        &target_cfg,
        agent.model.as_deref(),
        Path::new("/usr/bin/anyagents"),
        &["mcp".to_string()],
    )
    .unwrap();

    for rel in ["auth.json", ".config/sylphx/auth.json", ".spiron/auth.json"] {
        let raw = std::fs::read_to_string(target_cfg.join(rel))
            .unwrap_or_else(|e| panic!("missing {rel}: {e}"));
        let val: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(val["access_token"], "spiron-shared-token-123", "in {rel}");
    }
}

fn usage_fixture() -> (
    tempfile::TempDir,
    AppState,
    Agent,
    Agent,
    Agent,
    Agent,
    Agent,
) {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let root = state
        .store
        .create_agent(anyagents_core::NewAgent::new("usage-root", "opencode"))
        .unwrap();
    let mut cn = anyagents_core::NewAgent::new("usage-child", "opencode");
    cn.parent_id = Some(root.id.clone());
    let child = state.store.create_agent(cn).unwrap();
    let mut gn = anyagents_core::NewAgent::new("usage-grandchild", "opencode");
    gn.parent_id = Some(child.id.clone());
    let grandchild = state.store.create_agent(gn).unwrap();
    let mut sn = anyagents_core::NewAgent::new("usage-sibling", "opencode");
    sn.parent_id = Some(root.id.clone());
    let sibling = state.store.create_agent(sn).unwrap();
    let other_root = state
        .store
        .create_agent(anyagents_core::NewAgent::new(
            "usage-other-root",
            "opencode",
        ))
        .unwrap();
    for (agent, tokens) in [
        (&root, 100),
        (&child, 200),
        (&grandchild, 400),
        (&sibling, 800),
        (&other_root, 1600),
    ] {
        state
            .store
            .record_usage(anyagents_core::NewUsage {
                agent_id: agent.id.clone(),
                harness: "opencode".into(),
                model: Some("m1".into()),
                input_tokens: tokens,
                output_tokens: tokens / 2,
                cache_read_tokens: 1,
                cache_write_tokens: 2,
                reported_cost_micros: Some(tokens),
                estimated_cost_micros: None,
                billing_mode: anyagents_core::BillingMode::Subscription,
                quota_state: None,
                day: "2026-11-01".into(),
            })
            .unwrap();
    }
    (dir, state, root, child, grandchild, sibling, other_root)
}

async fn usage_q(
    state: AppState,
    principal: Principal,
    q: UsageQuery,
) -> Result<anyagents_core::UsageReport, ApiError> {
    usage_report(principal, State(state), Query(q))
        .await
        .map(|Json(r)| r)
}

#[tokio::test]
async fn agents_report_to_the_operator_and_only_the_operator_reads_it() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let root = state
        .store
        .create_agent(anyagents_core::NewAgent::new("report-root", "opencode"))
        .unwrap();
    state.store.set_agent_admin(&root.id, true).unwrap();
    let mut new = anyagents_core::NewAgent::new("report-child", "opencode");
    new.parent_id = Some(root.id.clone());
    let child = state.store.create_agent(new).unwrap();
    let as_agent = |agent: &Agent| Principal::Agent {
        agent_id: agent.id.clone(),
        name: agent.name.clone(),
    };
    let report = |principal: Principal, to: &[&str], text: &str| {
        send(
            principal,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(SendRequest {
                to: to.iter().map(|to| to.to_string()).collect(),
                text: text.to_string(),
                ..SendRequest::default()
            }),
        )
    };
    let read = |principal: Principal, since: Option<i64>| {
        operator_reports(
            principal,
            State(state.clone()),
            Query(Since {
                since,
                limit: None,
                unread: None,
                sender: None,
            }),
        )
    };

    // Delivery: the report lands in the operator's inbox, keyed to the
    // sender's immutable id, and in the sender's own history.
    let text = "worktree add failed: EACCES on .git/worktrees; tried a fresh clone";
    let Json(sent) = report(as_agent(&child), &["operator"], text).await.unwrap();
    assert_eq!(sent.deliveries.len(), 1);
    let delivery = &sent.deliveries[0];
    assert_eq!(delivery.to, "operator");
    assert!(!delivery.duplicate);
    let Json(inbox_view) = read(Principal::Operator, None).await.unwrap();
    assert_eq!(inbox_view.messages.len(), 1);
    let stored = &inbox_view.messages[0];
    assert_eq!(stored.id, delivery.id);
    assert_eq!(stored.text, text);
    assert_eq!(stored.sender, format!("agent:{}", child.id));
    assert_eq!(stored.sender_name.as_deref(), Some("report-child"));
    let history = state
        .store
        .conversation(&child.id, Some("operator"), None, None)
        .unwrap();
    assert!(
        history
            .iter()
            .any(|turn| turn.direction == "out" && turn.text == text),
        "{history:?}"
    );
    assert!(
        read(Principal::Operator, Some(stored.seq))
            .await
            .unwrap()
            .0
            .messages
            .is_empty()
    );

    // An agent may only write: neither the operator route nor an agent
    // mailbox route reaches it, for a root admin too.
    for principal in [as_agent(&child), as_agent(&root)] {
        let err = read(principal.clone(), None).await.unwrap_err();
        assert_eq!(err.status, StatusCode::FORBIDDEN, "{}", err.message);
        let err = inbox(
            principal.clone(),
            State(state.clone()),
            AxumPath("operator".into()),
            Query(Since {
                since: None,
                limit: None,
                unread: None,
                sender: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::FORBIDDEN, "{}", err.message);
        let err = operator_inbox(
            principal,
            State(state.clone()),
            AxumPath("operator".into()),
            Query(Since {
                since: None,
                limit: None,
                unread: None,
                sender: None,
            }),
        )
        .await
        .unwrap_err();
        assert_ne!(err.status, StatusCode::OK, "{}", err.message);
    }

    // Deduplication: the same report again is the stored one.
    let Json(again) = report(as_agent(&child), &["operator"], text).await.unwrap();
    assert!(again.deliveries[0].duplicate);
    assert_eq!(again.deliveries[0].id, delivery.id);
    let Json(counted) = read(Principal::Operator, None).await.unwrap();
    assert_eq!(counted.messages[0].repeats, 1);

    // Rate limit: after the burst the sender is refused with 429, before
    // any other address of the same send is written.
    for i in 1..anyagents_core::OPERATOR_BURST {
        let Json(sent) = report(as_agent(&child), &["operator"], &format!("defect {i}"))
            .await
            .unwrap();
        assert!(!sent.deliveries[0].duplicate);
    }
    let parent_mail = state.store.inbox(&root.id, None).unwrap().len();
    let err = report(as_agent(&child), &["parent", "operator"], "one more")
        .await
        .unwrap_err();
    assert_eq!(err.status, StatusCode::TOO_MANY_REQUESTS, "{}", err.message);
    assert!(err.message.contains("Retry in"), "{}", err.message);
    assert_eq!(
        state.store.inbox(&root.id, None).unwrap().len(),
        parent_mail
    );
    // Another sender has its own limit.
    let Json(other) = report(as_agent(&root), &["operator"], "one more")
        .await
        .unwrap();
    assert!(!other.deliveries[0].duplicate);
    let Json(all) = read(Principal::Operator, None).await.unwrap();
    assert_eq!(
        all.messages.len(),
        usize::try_from(anyagents_core::OPERATOR_BURST).unwrap() + 1
    );
}

#[test]
fn concurrent_reports_never_leave_a_mixed_send_half_delivered() {
    let dir = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("race-parent", "opencode"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("race-child", "opencode");
    new.parent_id = Some(parent.id.clone());
    store.create_agent(new).unwrap();
    let senders = 8;
    let barrier = Arc::new(std::sync::Barrier::new(senders));
    let workers: Vec<_> = (0..senders)
        .map(|i| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let actor = Actor::Agent("race-child".into());
                let body = SendRequest {
                    sender: Some(actor.as_address()),
                    to: vec!["parent".into(), "operator".into()],
                    text: format!("defect {i}"),
                    ..SendRequest::default()
                };
                let plan = SendPlan::parse(&store, &actor, &body).unwrap();
                barrier.wait();
                send_sync(&store, &actor, body, plan, None)
                    .map(|_| ())
                    .map_err(|err| err.status)
            })
        })
        .collect();
    let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert!(
        results
            .iter()
            .all(|r| matches!(r, Ok(()) | Err(StatusCode::TOO_MANY_REQUESTS))),
        "{results:?}"
    );
    let sent = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(
        sent,
        usize::try_from(anyagents_core::OPERATOR_BURST).unwrap()
    );
    // Every refused send wrote nothing: the parent got exactly the sends
    // the operator inbox took.
    assert_eq!(store.operator_inbox(None, None).unwrap().len(), sent);
    assert_eq!(store.inbox(&parent.id, None).unwrap().len(), sent);
}

async fn http(
    socket: &Path,
    method: &str,
    path: &str,
    body: Option<serde_json::Value>,
    capability: Option<&str>,
    operator: Option<&str>,
) -> (hyper::StatusCode, serde_json::Value) {
    let body = body.map(|body| serde_json::to_vec(&body).unwrap());
    let (status, bytes) = crate::mcp::request(socket, method, path, body, capability, operator)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, value)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_http_only_operator_root_creation_can_request_a_named_seat() {
    let dir = crate::test_tempdir();
    let mut state = archetype_validation_state(dir.path());
    let operator_token = "operator-token-0123456789abcdef0123456789abcdef";
    state.auth = auth::Mode::Confined {
        operator_digest: Some(auth::operator_digest(operator_token)),
    };
    let owner = state
        .store
        .create_agent(anyagents_core::NewAgent::new("owner", "spiron"))
        .unwrap();
    let capability = state.store.issue_capability(&owner.id).unwrap();
    let uid = (42001..42010)
        .find(|&uid| uid != crate::sandbox::server_uid())
        .unwrap();
    let data = state.store.data_dir();
    let registry = data.join("execution-principals.json");
    std::fs::write(
        data.join("test-passwd"),
        format!(
            "seat-claimed:x:{uid}:42010::/:/bin/false\nseat-fresh:x:{}:42010::/:/bin/false\n",
            uid + 100
        ),
    )
    .unwrap();
    let saved = serde_json::json!({owner.id.as_str(): {
        "user":"seat-claimed", "uid":uid, "gid":42010, "socket_gid":42011
    }})
    .to_string();
    std::fs::write(&registry, &saved).unwrap();
    let socket = dir.path().join("admission.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let app = build_router(state.clone());
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let workdir = dir.path().join("work");
    std::fs::create_dir(&workdir).unwrap();
    let explicit = serde_json::json!({
        "name":"fresh", "harness":"spiron", "scope":"isolated", "workdir":workdir,
        "seat_user":"seat-fresh", "seat_uid":uid + 100
    });
    for (mut body, agent_token, operator, want, problem) in [
        (
            explicit.clone(),
            None,
            None,
            StatusCode::UNAUTHORIZED,
            "authenticated",
        ),
        (
            explicit.clone(),
            Some(capability.as_str()),
            None,
            StatusCode::FORBIDDEN,
            "operator-created root",
        ),
        (
            explicit.clone(),
            None,
            Some(operator_token),
            StatusCode::FORBIDDEN,
            "operator-created root",
        ),
        (
            serde_json::json!({"name":"claimed","harness":"spiron","scope":"isolated","workdir":workdir}),
            None,
            Some(operator_token),
            StatusCode::FORBIDDEN,
            "already claimed",
        ),
    ] {
        if operator.is_some() && body["name"] == "fresh" {
            body["parent"] = serde_json::json!(owner.name);
        }
        let (status, reply) = http(
            &socket,
            "POST",
            "/v1/agents",
            Some(body),
            agent_token,
            operator,
        )
        .await;
        assert_eq!(status, want, "{reply}");
        assert!(
            reply["error"].as_str().unwrap().contains(problem),
            "{reply}"
        );
    }
    let mut mismatch = explicit;
    mismatch["seat_uid"] = serde_json::json!(uid + 101);
    let (status, reply) = http(
        &socket,
        "POST",
        "/v1/agents",
        Some(mismatch),
        None,
        Some(operator_token),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{reply}");
    assert!(
        reply["error"].as_str().unwrap().contains("does not match"),
        "{reply}"
    );
    assert_eq!(std::fs::read_to_string(registry).unwrap(), saved);
    assert!(state.store.agent_by_name("fresh").is_err());
    assert!(state.store.agent_by_name("claimed").is_err());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_http_a_report_is_capped_at_16_kib_and_only_the_operator_token_reads_it() {
    let dir = crate::test_tempdir();
    let mut state = archetype_validation_state(dir.path());
    let operator_token = "operator-token-0123456789abcdef0123456789abcdef";
    state.auth = auth::Mode::Confined {
        operator_digest: Some(auth::operator_digest(operator_token)),
    };
    let parent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("http-parent", "opencode"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("http-child", "opencode");
    new.parent_id = Some(parent.id.clone());
    let child = state.store.create_agent(new).unwrap();
    let capability = state.store.issue_capability(&child.id).unwrap();
    let socket = dir.path().join("operator-inbox.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let app = build_router(state.clone());
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let report = |text: String| serde_json::json!({ "to": ["parent", "operator"], "text": text });

    // One byte over the cap is refused before any target is written.
    let max = 16 * 1024;
    let (status, refused) = http(
        &socket,
        "POST",
        "/v1/send",
        Some(report("x".repeat(max + 1))),
        Some(&capability),
        None,
    )
    .await;
    assert_eq!(status, hyper::StatusCode::BAD_REQUEST, "{refused}");
    assert!(
        refused["error"].as_str().unwrap().contains("16 KiB"),
        "{refused}"
    );
    assert!(state.store.inbox(&parent.id, None).unwrap().is_empty());
    assert!(state.store.operator_inbox(None, None).unwrap().is_empty());
    // Exactly the cap is delivered to both.
    let (status, sent) = http(
        &socket,
        "POST",
        "/v1/send",
        Some(report("y".repeat(max))),
        Some(&capability),
        None,
    )
    .await;
    assert_eq!(status, hyper::StatusCode::OK, "{sent}");
    assert_eq!(sent["deliveries"].as_array().unwrap().len(), 2, "{sent}");

    // The agent's capability, no credential and a wrong token cannot read.
    for (capability, operator, expected) in [
        (
            Some(capability.as_str()),
            None,
            hyper::StatusCode::FORBIDDEN,
        ),
        (None, None, hyper::StatusCode::UNAUTHORIZED),
        (None, Some("not-the-token"), hyper::StatusCode::UNAUTHORIZED),
    ] {
        let (status, body) = http(
            &socket,
            "GET",
            "/v1/operator/inbox",
            None,
            capability,
            operator,
        )
        .await;
        assert_eq!(status, expected, "{body}");
    }
    let (status, read) = http(
        &socket,
        "GET",
        "/v1/operator/inbox?limit=5",
        None,
        None,
        Some(operator_token),
    )
    .await;
    assert_eq!(status, hyper::StatusCode::OK, "{read}");
    let messages = read["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1, "{read}");
    assert_eq!(messages[0]["text"].as_str().unwrap().len(), max);
    assert_eq!(messages[0]["sender_name"], "http-child");
    server.abort();
}

#[tokio::test]
async fn usage_rpc_enforces_causal_authorization_boundary() {
    let (_dir, state, root, child, grandchild, sibling, _other) = usage_fixture();
    let op = Principal::Operator;
    // Operator: whole workspace, explicit sibling, `all` all allowed.
    let r = usage_q(state.clone(), op.clone(), UsageQuery::default())
        .await
        .unwrap();
    assert_eq!(r.totals.turns, 5);
    assert_eq!(r.totals.input_tokens, 3100);
    let r = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            agent: Some("usage-sibling".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.input_tokens, 800);
    let r = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            agent: Some("all".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.turns, 5);
    // Operator + subtree flag on root child widens to its subtree.
    let r = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            agent: Some("usage-child".into()),
            include_descendants: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.input_tokens, 600);
    // Root caller (parent: null) is privileged like the operator.
    let root_p = Principal::Agent {
        agent_id: root.id.clone(),
        name: root.name.clone(),
    };
    let r = usage_q(
        state.clone(),
        root_p,
        UsageQuery {
            agent: Some("usage-sibling".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.input_tokens, 800);
    // Private child: omitted target = self subtree (child + grandchild).
    let child_p = Principal::Agent {
        agent_id: child.id.clone(),
        name: child.name.clone(),
    };
    let r = usage_q(state.clone(), child_p.clone(), UsageQuery::default())
        .await
        .unwrap();
    assert_eq!(r.totals.input_tokens, 600);
    assert_eq!(r.totals.turns, 2);
    // Explicit descendant allowed; subtree flag widens to self subtree.
    let r = usage_q(
        state.clone(),
        child_p.clone(),
        UsageQuery {
            agent: Some("usage-grandchild".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.input_tokens, 400);
    // Parent, sibling, `all` and unrelated roots are fail-closed.
    for target in ["usage-root", "usage-sibling", "all", "usage-other-root"] {
        let err = usage_q(
            state.clone(),
            child_p.clone(),
            UsageQuery {
                agent: Some(target.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::FORBIDDEN, "target {target}");
    }
    // Unknown target is a client error, never a leak.
    let err = usage_q(
        state.clone(),
        child_p.clone(),
        UsageQuery {
            agent: Some("nope".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(err.status.is_client_error());
    // Public-scope caller regains the whole workspace.
    state.store.set_agent_scope(&child.id, "public").unwrap();
    let r = usage_q(
        state.clone(),
        child_p.clone(),
        UsageQuery {
            agent: Some("usage-sibling".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.input_tokens, 800);
    let r = usage_q(
        state.clone(),
        child_p.clone(),
        UsageQuery {
            agent: Some("all".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.turns, 5);
    let _ = (grandchild, sibling);
}

#[tokio::test]
async fn usage_rpc_grouping_and_window_accuracy() {
    let (_dir, state, _root, _child, _gc, _sib, _other) = usage_fixture();
    let op = Principal::Operator;
    for group_by in ["agent", "day", "model", "harness"] {
        let r = usage_q(
            state.clone(),
            op.clone(),
            UsageQuery {
                group_by: Some(group_by.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(!r.buckets.is_empty(), "group_by {group_by}");
        assert_eq!(
            r.buckets.iter().map(|b| b.totals.turns).sum::<i64>(),
            5,
            "bucket turns sum for {group_by}"
        );
        assert_eq!(r.totals.turns, 5);
        assert_eq!(r.totals.input_tokens, 3100);
        assert_eq!(r.totals.output_tokens, 1550);
        assert_eq!(r.totals.cache_read_tokens, 5);
        assert_eq!(r.totals.cache_write_tokens, 10);
        assert_eq!(r.totals.reported_cost_micros, Some(3100));
    }
    // Time windows: far-future lower bound excludes everything, days=36500 keeps all.
    let r = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            since_ms: Some(anyagents_core::now_ms() + 86_400_000),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.turns, 0);
    let r = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            days: Some(36500),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.turns, 5);
    let r = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            until_ms: Some(0),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.turns, 0);
    // Invalid grouping / inverted window are client errors.
    let err = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            group_by: Some("bogus".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
    let err = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            since_ms: Some(2),
            until_ms: Some(1),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn mcp_usage_action_forwards_to_daemon_usage_route() {
    // The MCP tool serializes the same query the daemon route accepts:
    // build the query string the way the dispatch does and confirm the
    // route parses it (group_by + window + subtree).
    let (_dir, state, _root, _child, _gc, _sib, _other) = usage_fixture();
    let r = usage_q(
        state.clone(),
        Principal::Operator,
        UsageQuery {
            agent: Some("usage-child".into()),
            include_descendants: Some(true),
            group_by: Some("day".into()),
            days: Some(36500),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.input_tokens, 600);
    assert_eq!(r.buckets.len(), 2);
}

#[tokio::test]
async fn pause_agent_route_waits_on_lifecycle_and_rejects_archived() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let live = state
        .store
        .create_agent(anyagents_core::NewAgent::new("pause-live", "opencode"))
        .unwrap();
    let Json(paused) = pause_agent_route(
        Principal::Operator,
        State(state.clone()),
        AxumPath(live.name.clone()),
    )
    .await
    .unwrap();
    assert_eq!(paused.state, "paused");

    let archived = state
        .store
        .create_agent(anyagents_core::NewAgent::new("pause-archived", "opencode"))
        .unwrap();
    let held = state.store.lock_agent_lifecycle(&archived.id).unwrap();
    let route_state = state.clone();
    let route_name = archived.name.clone();
    let pending = tokio::spawn(async move {
        pause_agent_route(
            Principal::Operator,
            State(route_state),
            AxumPath(route_name),
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    assert!(
        !pending.is_finished(),
        "pause ran while lifecycle lock was held"
    );
    state
        .store
        .set_agent_state(&archived.id, "archived")
        .unwrap();
    drop(held);
    let err = pending.await.unwrap().unwrap_err();
    assert_eq!(err.status, StatusCode::CONFLICT);
    assert_eq!(
        state.store.agent(&archived.id).unwrap().state,
        "archived",
        "pause overwrote archived state"
    );
}

struct RouteWakeHarness {
    generation: usize,
    alive: bool,
    in_turn: bool,
    diagnostic: Option<String>,
    allow_verified_stop: Arc<std::sync::atomic::AtomicBool>,
    stop_entered_tx: Option<std::sync::mpsc::Sender<()>>,
    stop_release_rx: Option<Arc<Mutex<std::sync::mpsc::Receiver<()>>>>,
    delivered: Arc<Mutex<Vec<(usize, String)>>>,
    active_processes: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for RouteWakeHarness {
    fn drop(&mut self) {
        if self.alive {
            self.active_processes
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            self.alive = false;
        }
    }
}

impl anyagents_harness::Harness for RouteWakeHarness {
    fn deliver(
        &mut self,
        message: &anyagents_harness::OutboundMessage,
    ) -> Result<anyagents_harness::DeliverOutcome, anyagents_harness::HarnessError> {
        self.delivered
            .lock()
            .unwrap()
            .push((self.generation, message.text.clone()));
        self.in_turn = false;
        Ok(anyagents_harness::DeliverOutcome::Accepted {
            turn_finished: true,
        })
    }

    fn proven(&self, _message_id: &str) -> Option<anyagents_harness::Proof> {
        Some(anyagents_harness::Proof::Taken {
            turn_finished: true,
        })
    }

    fn stop_verified(&mut self) -> Result<(), anyagents_harness::HarnessError> {
        if let Some(tx) = self.stop_entered_tx.take() {
            let _ = tx.send(());
        }
        if let Some(rx) = &self.stop_release_rx {
            let _ = rx
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(5));
        }
        if !self
            .allow_verified_stop
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(anyagents_harness::HarnessError::Failed(
                "checked harness retirement is unsupported".into(),
            ));
        }
        if self.alive {
            self.active_processes
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            self.alive = false;
        }
        self.in_turn = false;
        Ok(())
    }

    fn background(&mut self) -> Result<(), anyagents_harness::HarnessError> {
        Ok(())
    }
    fn resume(&mut self, _session: &str) -> Result<(), anyagents_harness::HarnessError> {
        Ok(())
    }
    fn cancel(&mut self) -> Result<(), anyagents_harness::HarnessError> {
        Ok(())
    }
    fn exit_diagnostic(&self) -> Option<String> {
        self.diagnostic.clone()
    }
    fn health(&self) -> anyagents_harness::HarnessHealth {
        anyagents_harness::HarnessHealth {
            process_alive: self.alive,
            turn_state: if self.in_turn {
                anyagents_harness::TurnState::InTurn
            } else {
                anyagents_harness::TurnState::Idle
            },
            delivery: anyagents_harness::Delivery {
                level: anyagents_harness::DeliveryLevel::NextStep,
                idempotent: true,
                proof: "test".into(),
                peer: None,
            },
            quota_reset_at_ms: None,
            sign_in_fault: None,
        }
    }
}

struct FakeModeGuard(Option<std::ffi::OsString>);
impl FakeModeGuard {
    fn enter() -> Self {
        let prev = std::env::var_os("ANYAGENTS_FAKE_MODE");
        unsafe {
            std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
        }
        Self(prev)
    }
}
impl Drop for FakeModeGuard {
    fn drop(&mut self) {
        unsafe {
            match self.0.take() {
                Some(v) => std::env::set_var("ANYAGENTS_FAKE_MODE", v),
                None => std::env::remove_var("ANYAGENTS_FAKE_MODE"),
            }
        }
    }
}

#[test]
fn wake_route_recovers_delivery_error_in_one_step_and_keeps_repeat_wake_idempotent() {
    let _env = env_lock();
    let _fake = FakeModeGuard::enter();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = crate::test_tempdir();
        let mut state = archetype_validation_state(dir.path());
        let launches = Arc::new(AtomicUsize::new(0));
        let active_processes = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let delivered = Arc::new(Mutex::new(Vec::<(usize, String)>::new()));

        let launches_l = Arc::clone(&launches);
        let active_l = Arc::clone(&active_processes);
        let max_l = Arc::clone(&max_concurrent);
        let delivered_l = Arc::clone(&delivered);

        let sup = anyagents_supervisor::Supervisor::start(
            Arc::clone(&state.store),
            Arc::new(anyagents_supervisor::SystemClock::new()),
            Arc::new(move |_| {
                let gen_no = launches_l.fetch_add(1, Ordering::SeqCst) + 1;
                let cur = active_l.fetch_add(1, Ordering::SeqCst) + 1;
                max_l.fetch_max(cur, Ordering::SeqCst);
                Ok(RouteWakeHarness {
                    generation: gen_no,
                    alive: true,
                    in_turn: false,
                    // Generation 1 surfaces a terminal delivery_error; generation 2+ is healthy.
                    diagnostic: (gen_no == 1).then(|| "broken transport pipe".into()),
                    allow_verified_stop: Arc::new(AtomicBool::new(true)),
                    stop_entered_tx: None,
                    stop_release_rx: None,
                    delivered: Arc::clone(&delivered_l),
                    active_processes: Arc::clone(&active_l),
                })
            }),
            Vec::new(),
        )
        .unwrap();
        state.health = sup.board();

        let created = state
            .store
            .create_agent(anyagents_core::NewAgent::new("wake-target", "claude-code"))
            .unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &created.id);
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(
            cfg.join(".anyagents-last-error.json"),
            r#"{"error":"broken transport pipe","timestamp_ms":9999999999999}"#,
        )
        .unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let running = state
            .store
            .set_agent_running(&created.id, &work.display().to_string())
            .unwrap();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while sup.live(&running.id).and_then(|l| l.last_error).as_deref()
            != Some("broken transport pipe")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "fault never published"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let before = agent_view(&state.store, &state.health, &running).unwrap();
        assert_eq!(before.block_reason.as_deref(), Some("delivery_error"));

        let Json(woken) = wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(running.name.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(woken.state, "running");
        assert_eq!(woken.block_reason, None);
        assert_eq!(woken.last_error, None);
        assert!(!cfg.join(".anyagents-last-error.json").exists());

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while launches.load(Ordering::SeqCst) < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "replacement generation never launched"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // Repeat wake while the healthy replacement worker is running must be
        // idempotent and must not rotate markers or spawn a third generation.
        let Json(repeat) = wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(running.name.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(repeat.state, "running");
        assert_eq!(repeat.block_reason, None);
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert_eq!(
            launches.load(Ordering::SeqCst),
            2,
            "repeat wake restarted healthy replacement worker"
        );

        state
            .store
            .receive(
                &running.id,
                anyagents_core::Incoming::new("post-wake message"),
            )
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !delivered
            .lock()
            .unwrap()
            .iter()
            .any(|(_, text)| text.contains("post-wake message"))
        {
            assert!(
                std::time::Instant::now() < deadline,
                "replacement generation never delivered post-wake mail"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(launches.load(Ordering::SeqCst), 2);
        assert_eq!(
            max_concurrent.load(Ordering::SeqCst),
            1,
            "old generation must retire before replacement launches"
        );
        assert_eq!(delivered.lock().unwrap()[0].0, 2);
        drop(sup);
    });
}

#[test]
fn wake_route_refuses_unproven_termination_and_cancellation_without_clearing_faults() {
    let _env = env_lock();
    let _fake = FakeModeGuard::enter();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = crate::test_tempdir();
        let mut state = archetype_validation_state(dir.path());
        let launches = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let allow_stop = Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let entered_slot = Arc::new(Mutex::new(Some(entered_tx)));

        let launches_l = Arc::clone(&launches);
        let active_l = Arc::clone(&active);
        let allow_l = Arc::clone(&allow_stop);
        let entered_l = Arc::clone(&entered_slot);
        let release_l = Arc::clone(&release_rx);

        let sup = anyagents_supervisor::Supervisor::start(
            Arc::clone(&state.store),
            Arc::new(anyagents_supervisor::SystemClock::new()),
            Arc::new(move |_| {
                let gen_no = launches_l.fetch_add(1, Ordering::SeqCst) + 1;
                active_l.fetch_add(1, Ordering::SeqCst);
                Ok(RouteWakeHarness {
                    generation: gen_no,
                    alive: true,
                    in_turn: false,
                    diagnostic: (gen_no == 1).then(|| "remote keeper stop unproven".into()),
                    allow_verified_stop: Arc::clone(&allow_l),
                    stop_entered_tx: entered_l.lock().unwrap().take(),
                    stop_release_rx: (gen_no == 1).then(|| Arc::clone(&release_l)),
                    delivered: Arc::new(Mutex::new(Vec::new())),
                    active_processes: Arc::clone(&active_l),
                })
            }),
            Vec::new(),
        )
        .unwrap();
        state.health = sup.board();

        let created = state
            .store
            .create_agent(anyagents_core::NewAgent::new(
                "unproven-agent",
                "claude-code",
            ))
            .unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &created.id);
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(
            cfg.join(".anyagents-last-error.json"),
            r#"{"error":"remote keeper stop unproven","timestamp_ms":9999999999999}"#,
        )
        .unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let running = state
            .store
            .set_agent_running(&created.id, &work.display().to_string())
            .unwrap();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while sup.live(&running.id).and_then(|l| l.last_error).as_deref()
            != Some("remote keeper stop unproven")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "fault never published"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // 1. Cancellation while stop_for_wake is waiting must fail closed before
        // clearing any fault or starting a replacement.
        let cancel_state = state.clone();
        let cancel_name = running.name.clone();
        let wake_fut = wake_agent_route(
            Principal::Operator,
            State(cancel_state),
            AxumPath(cancel_name),
            axum::http::HeaderMap::new(),
        );
        let timed = tokio::time::timeout(std::time::Duration::from_millis(60), wake_fut).await;
        assert!(
            timed.is_err(),
            "wake should still be waiting on gated stop_verified"
        );

        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        release_tx.send(()).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;

        assert_eq!(state.store.agent(&running.id).unwrap().state, "paused");
        assert!(cfg.join(".anyagents-last-error.json").exists());
        assert_eq!(
            sup.live(&running.id).and_then(|l| l.last_error).as_deref(),
            Some("remote keeper stop unproven")
        );
        assert!(matches!(
            state.store.try_lock_agent(&running.id),
            Err(anyagents_core::Error::AgentBusy)
        ));
        assert_eq!(launches.load(Ordering::SeqCst), 1);

        // 2. Subsequent wake while stop_verified still returns Err refuses with
        // 409 Conflict and preserves both fault and quarantined custody.
        let _ = release_tx.send(());
        let err = wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(running.name.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert!(err.message.contains("unsupported"), "{}", err.message);
        assert!(cfg.join(".anyagents-last-error.json").exists());
        assert!(matches!(
            state.store.try_lock_agent(&running.id),
            Err(anyagents_core::Error::AgentBusy)
        ));
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        drop(sup);
    });
}

#[test]
fn wake_route_preserves_unrelated_auth_quota_and_migration_faults() {
    let _env = env_lock();
    let _fake = FakeModeGuard::enter();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let dir = crate::test_tempdir();
        let state = archetype_validation_state(dir.path());
        for (idx, (fault_text, quota_ms, expected_reason)) in [
            ("not logged in to provider", None, "sign_in_required"),
            (
                "rate_limit quota exceeded",
                Some(now_ms() + 600_000),
                "quota_exhausted",
            ),
            (
                "memory_migration_wait: indexing host memory",
                None,
                "memory_migration_wait",
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let msg = fault_text;
            let agent = state
                .store
                .create_agent(anyagents_core::NewAgent::new(
                    format!("unrelated-{idx}"),
                    "claude-code",
                ))
                .unwrap();
            let cfg = anyagents_core::agent_config_dir(dir.path(), &agent.id);
            std::fs::create_dir_all(&cfg).unwrap();
            std::fs::write(
                cfg.join(".anyagents-last-error.json"),
                serde_json::json!({
                    "error": msg,
                    "timestamp_ms": now_ms() + 1_000,
                })
                .to_string(),
            )
            .unwrap();
            let work = dir.path().join(format!("work-{idx}"));
            std::fs::create_dir_all(&work).unwrap();
            state.store.set_agent_state(&agent.id, "paused").unwrap();
            state.health.publish(
                &agent.id,
                anyagents_supervisor::LiveState {
                    process_alive: true,
                    turn_state: anyagents_harness::TurnState::Idle,
                    delivery: None,
                    quota_reset_at_ms: quota_ms,
                    restart_at_ms: None,
                    pid: Some(1000 + idx as u32),
                    live_harness: Some("claude-code".into()),
                    live_model: None,
                    process_started_at_ms: Some(now_ms()),
                    last_error: Some(msg.to_string()),
                    retry_at_ms: None,
                },
            );

            let Json(woken) = wake_agent_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(agent.name.clone()),
                axum::http::HeaderMap::new(),
            )
            .await
            .unwrap();
            assert_eq!(woken.state, "running");
            assert_eq!(
                woken.block_reason.as_deref(),
                Some(expected_reason),
                "unrelated fault cleared on paused->running wake for {expected_reason}"
            );
            assert!(cfg.join(".anyagents-last-error.json").exists());
            assert!(!cfg.join(".anyagents-quota-cleared-ms").exists());

            let Json(woken_again) = wake_agent_route(
                Principal::Operator,
                State(state.clone()),
                AxumPath(agent.name.clone()),
                axum::http::HeaderMap::new(),
            )
            .await
            .unwrap();
            assert_eq!(
                woken_again.block_reason.as_deref(),
                Some(expected_reason),
                "unrelated fault cleared on running wake for {expected_reason}"
            );
            assert!(cfg.join(".anyagents-last-error.json").exists());
        }
    });
}

/// A worker whose process died keeps the agent's process lock through its
/// restart backoff while the board shows `process: down`. A wake for the
/// persisted fault retires that generation and restarts in one step,
/// never beside it.
#[test]
fn wake_route_reclaims_the_lock_a_dead_process_worker_holds() {
    let _env = env_lock();
    let _fake = FakeModeGuard::enter();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = crate::test_tempdir();
        let mut state = archetype_validation_state(dir.path());
        let launches = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let delivered = Arc::new(Mutex::new(Vec::<(usize, String)>::new()));

        let (launches_l, active_l, max_l, delivered_l) = (
            Arc::clone(&launches),
            Arc::clone(&active),
            Arc::clone(&max_concurrent),
            Arc::clone(&delivered),
        );
        let sup = anyagents_supervisor::Supervisor::start(
            Arc::clone(&state.store),
            Arc::new(anyagents_supervisor::SystemClock::new()),
            Arc::new(move |request: anyagents_supervisor::LaunchRequest| {
                let gen_no = launches_l.fetch_add(1, Ordering::SeqCst) + 1;
                // Until the wake, every process the worker starts is
                // already gone: it waits out its backoff holding the lock.
                // The wake writes this marker only after it retired that
                // worker, so every later launch is healthy.
                let alive = request
                    .config_dir
                    .join(".anyagents-profile-rotated-ms")
                    .exists();
                if alive {
                    let cur = active_l.fetch_add(1, Ordering::SeqCst) + 1;
                    max_l.fetch_max(cur, Ordering::SeqCst);
                }
                Ok(RouteWakeHarness {
                    generation: gen_no,
                    alive,
                    in_turn: false,
                    diagnostic: None,
                    allow_verified_stop: Arc::new(AtomicBool::new(true)),
                    stop_entered_tx: None,
                    stop_release_rx: None,
                    delivered: Arc::clone(&delivered_l),
                    active_processes: Arc::clone(&active_l),
                })
            }),
            Vec::new(),
        )
        .unwrap();
        state.health = sup.board();

        let created = state
            .store
            .create_agent(anyagents_core::NewAgent::new("dead-worker", "claude-code"))
            .unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &created.id);
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(
            cfg.join(".anyagents-last-error.json"),
            r#"{"error":"agy_empty_turn: turn ended without model output","timestamp_ms":9999999999999}"#,
        )
        .unwrap();
        let work = dir.path().join("work");
        std::fs::create_dir_all(&work).unwrap();
        let running = state
            .store
            .set_agent_running(&created.id, &work.display().to_string())
            .unwrap();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let down = sup
                .live(&running.id)
                .is_some_and(|l| !l.process_alive && l.last_error.is_none());
            let held = matches!(
                state.store.try_lock_agent(&running.id),
                Err(anyagents_core::Error::AgentBusy)
            );
            if down && held {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "worker never held the lock over a dead process"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let before = agent_view(&state.store, &state.health, &running).unwrap();
        assert_eq!(before.state, "running");
        assert_eq!(before.process, "down");
        assert_eq!(before.turn_state, "errored");
        assert_eq!(before.block_reason.as_deref(), Some("delivery_error"));
        let dead_generations = launches.load(Ordering::SeqCst);

        let Json(woken) = wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(running.name.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(woken.state, "running");
        assert_eq!(woken.block_reason, None);
        assert_eq!(woken.last_error, None);
        assert!(!cfg.join(".anyagents-last-error.json").exists());
        assert!(cfg.join(".anyagents-profile-rotated-ms").exists());

        state
            .store
            .receive(&running.id, anyagents_core::Incoming::new("post-wake message"))
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !delivered
            .lock()
            .unwrap()
            .iter()
            .any(|(_, text)| text.contains("post-wake message"))
        {
            assert!(
                std::time::Instant::now() < deadline,
                "replacement generation never delivered post-wake mail: launches={} \
                 (dead before wake: {dead_generations}), state={}, live={:?}",
                launches.load(Ordering::SeqCst),
                state.store.agent(&running.id).unwrap().state,
                sup.live(&running.id),
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(delivered.lock().unwrap()[0].0 > dead_generations);
        assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
        drop(sup);
    });
}

/// A wake never takes the lock from a process that is alive, or from a
/// holder that is not this server's worker: it refuses and keeps the fault.
#[test]
fn wake_route_refuses_a_lock_held_by_a_live_process_or_another_holder() {
    let _env = env_lock();
    let _fake = FakeModeGuard::enter();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        let dir = crate::test_tempdir();
        let fault = r#"{"error":"broken transport pipe","timestamp_ms":9999999999999}"#;

        // A foreign holder: no worker of this server owns the lock.
        let state = archetype_validation_state(dir.path());
        let created = state
            .store
            .create_agent(anyagents_core::NewAgent::new("foreign-lock", "claude-code"))
            .unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &created.id);
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(cfg.join(".anyagents-last-error.json"), fault).unwrap();
        let work = dir.path().join("work-foreign");
        std::fs::create_dir_all(&work).unwrap();
        let running = state
            .store
            .set_agent_running(&created.id, &work.display().to_string())
            .unwrap();
        let held = state.store.try_lock_agent(&running.id).unwrap();
        let err = wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(running.name.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert!(
            err.message.contains("lock is still held"),
            "{}",
            err.message
        );
        assert!(cfg.join(".anyagents-last-error.json").exists());
        assert_eq!(state.store.agent(&running.id).unwrap().state, "running");
        drop(held);

        let dir = crate::test_tempdir();
        // A live process: its worker holds the lock and it is never stopped
        // for a persisted fault.
        let mut state = archetype_validation_state(dir.path());
        let launches = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let (launches_l, active_l) = (Arc::clone(&launches), Arc::clone(&active));
        let sup = anyagents_supervisor::Supervisor::start(
            Arc::clone(&state.store),
            Arc::new(anyagents_supervisor::SystemClock::new()),
            Arc::new(move |_| {
                let gen_no = launches_l.fetch_add(1, Ordering::SeqCst) + 1;
                active_l.fetch_add(1, Ordering::SeqCst);
                Ok(RouteWakeHarness {
                    generation: gen_no,
                    alive: true,
                    in_turn: false,
                    diagnostic: None,
                    allow_verified_stop: Arc::new(AtomicBool::new(true)),
                    stop_entered_tx: None,
                    stop_release_rx: None,
                    delivered: Arc::new(Mutex::new(Vec::new())),
                    active_processes: Arc::clone(&active_l),
                })
            }),
            Vec::new(),
        )
        .unwrap();
        state.health = sup.board();
        let created = state
            .store
            .create_agent(anyagents_core::NewAgent::new("live-lock", "claude-code"))
            .unwrap();
        let cfg = anyagents_core::agent_config_dir(dir.path(), &created.id);
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::write(cfg.join(".anyagents-last-error.json"), fault).unwrap();
        let work = dir.path().join("work-live");
        std::fs::create_dir_all(&work).unwrap();
        let running = state
            .store
            .set_agent_running(&created.id, &work.display().to_string())
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !sup.live(&running.id).is_some_and(|l| l.process_alive) {
            assert!(
                std::time::Instant::now() < deadline,
                "process never came up"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let err = wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath(running.name.clone()),
            axum::http::HeaderMap::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::CONFLICT);
        assert!(
            err.message.contains("lock is still held"),
            "{}",
            err.message
        );
        assert!(cfg.join(".anyagents-last-error.json").exists());
        assert_eq!(state.store.agent(&running.id).unwrap().state, "running");
        assert_eq!(launches.load(Ordering::SeqCst), 1);
        assert_eq!(active.load(Ordering::SeqCst), 1);
        assert!(sup.live(&running.id).is_some_and(|l| l.process_alive));
        drop(sup);
    });
}

#[test]
fn describe_instruction_update_marks_rotation_and_sets_pending_switch() {
    let dir = tempfile::tempdir().unwrap();
    let state = archetype_validation_state(dir.path());
    let ws = dir.path().join("ws");
    std::fs::create_dir_all(ws.join(".agents/agents")).unwrap();
    std::fs::write(
        ws.join(".agents/agents/specialist.md"),
        "---\nname: specialist\ndescription: Role summary\n---\nRole SSOT instructions.",
    )
    .unwrap();
    let mut new = anyagents_core::NewAgent::new("spiron-worker", "spiron");
    new.role = Some("specialist".into());
    new.workdir = Some(ws.display().to_string());
    let created = state.store.create_agent(new).unwrap();
    let running = state
        .store
        .set_agent_running(&created.id, &ws.display().to_string())
        .unwrap();
    let cfg = anyagents_core::agent_config_dir(dir.path(), &running.id);
    create::sync_agent_description_files(&state.store, &running, &cfg).unwrap();
    let started_ms = now_ms();
    state.health.publish(
        &running.id,
        anyagents_supervisor::LiveState {
            process_alive: true,
            turn_state: anyagents_harness::TurnState::Idle,
            delivery: Some(crate::vendor::declared_delivery("spiron")),
            quota_reset_at_ms: None,
            restart_at_ms: None,
            pid: Some(4242),
            live_harness: Some("spiron".to_string()),
            live_model: running.model.clone(),
            process_started_at_ms: Some(started_ms),
            last_error: None,
            retry_at_ms: None,
        },
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let Json(resp) = rt
        .block_on(set_description_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath("spiron-worker".to_string()),
            Json(DescriptionRequest {
                description: None,
                instructions: Some("Custom task v2.".to_string()),
                role: None,
                scope: None,
                lifecycle: None,
                public: None,
                clear: None,
            }),
        ))
        .unwrap_or_else(|e| panic!("{}: {}", e.status, e.message));
    assert_eq!(
        resp["instructions"].as_str(),
        Some(
            format!(
                "{}\n\nRole SSOT instructions.\n\nCustom task v2.",
                create::BUILTIN_GENERAL_ROLE_BODY
            )
            .as_str()
        )
    );
    let prompt = std::fs::read_to_string(cfg.join("SYSTEM_PROMPT.md")).unwrap();
    assert!(prompt.contains("Role SSOT instructions."));
    assert!(prompt.contains("Custom task v2."));
    let marker: i64 = std::fs::read_to_string(cfg.join(create::INSTRUCTIONS_ROTATED_MARKER))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(marker > started_ms, "marker={marker} started={started_ms}");
    let updated = state.store.agent(&running.id).unwrap();
    assert_eq!(updated.instructions.as_deref(), Some("Custom task v2."));
    file_cache::cache().catch_up();
    let view = agent_view(&state.store, &state.health, &updated).unwrap();
    assert!(view.pending_switch);
    assert_eq!(
        view.instructions.as_deref(),
        Some(
            format!(
                "{}\n\nRole SSOT instructions.\n\nCustom task v2.",
                create::BUILTIN_GENERAL_ROLE_BODY
            )
            .as_str()
        )
    );
}

#[test]
fn webhook_router_serves_mcp_profile_full_for_sandboxed_only_and_gateway_prefix() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    std::fs::write(
        dir.path().join(crate::remote::CONFIG_FILE),
        "[server]\npublic_url = \"https://agents.example.test\"\n",
    )
    .unwrap();

    let sbx_agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("sbx-codex", "codex"))
        .unwrap();
    let sbx_cfg = anyagents_core::agent_config_dir(dir.path(), &sbx_agent.id);
    std::fs::create_dir_all(&sbx_cfg).unwrap();
    crate::sandbox_providers::sylphx::SylphxLeaseConfig {
        endpoint: "https://sandbox.example.test".into(),
        token: None,
        parent: "orgs/o1/projects/p1/envs/e1".into(),
        template: "agent".into(),
        shape: "linux-m".into(),
        volume_size_gib: Some(10),
        mount_path: "/home/user/agent-state".into(),
        workdir: "/home/user/workspace".into(),
        volume_id: "vol_1".into(),
        volume_resource: "orgs/o1/projects/p1/envs/e1/volumes/vol_1".into(),
        lease_id: Some("lease_1".into()),
        lease_resource: Some("orgs/o1/projects/p1/envs/e1/leases/lease_1".into()),
        data_plane_resource: Some("orgs/o1/projects/p1/envs/e1/leases/lease_1".into()),
        envd_url: None,
        envd_token: None,
        expires_at: None,
    }
    .save_to_config_dir(&sbx_cfg)
    .unwrap();
    crate::remote::begin_session(&state.store, &sbx_agent).unwrap();
    let sbx_remote = crate::remote::session_token(&sbx_cfg).unwrap();

    let cloud_agent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("cloud-only", "claude-cloud"))
        .unwrap();
    let cloud_cfg = anyagents_core::agent_config_dir(dir.path(), &cloud_agent.id);
    std::fs::create_dir_all(&cloud_cfg).unwrap();
    crate::remote::begin_session(&state.store, &cloud_agent).unwrap();
    let cloud_remote = crate::remote::session_token(&cloud_cfg).unwrap();

    // Start a minimal mock upstream responses server and add gateway route.
    let upstream = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    std::thread::spawn(move || {
        for stream in upstream.incoming().take(2) {
            let mut s = stream.unwrap();
            let mut buf = [0u8; 4096];
            let n = std::io::Read::read(&mut s, &mut buf).unwrap();
            let req_str = String::from_utf8_lossy(&buf[..n]);
            assert!(req_str.contains("upstream-secret-token"), "{req_str}");
            let body = r#"{"id":"resp_1","object":"response","status":"completed"}"#;
            let reply = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = std::io::Write::write_all(&mut s, reply.as_bytes());
        }
    });
    crate::gateway::ProviderImage::store(
        dir.path(),
        &sbx_agent,
        &format!("http://{upstream_addr}/v1"),
        "gpt-test",
        "upstream-secret-token",
    )
    .unwrap();
    let gw_token = crate::gateway::add_route(dir.path(), &sbx_agent)
        .unwrap()
        .unwrap();

    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let app = build_webhook_router(state.clone());
        let served = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let list = tools_list().to_string();

        // 1) Both sandboxed and cloud agents receive the identical 7-tool universal MCP surface
        // on `/mcp`, `/v1/webhooks/mcp`, and legacy `?profile=...` URLs.
        for (tok, path) in [
            (sbx_remote.clone(), "/mcp"),
            (sbx_remote.clone(), "/v1/webhooks/mcp"),
            (sbx_remote.clone(), "/mcp?profile=full"),
            (cloud_remote.clone(), "/mcp?profile=full"),
        ] {
            let list_body = list.clone();
            let (status, body) = tokio::task::spawn_blocking(move || {
                let hdrs = [("authorization", format!("Bearer {tok}"))];
                let refs: Vec<(&str, &str)> = hdrs.iter().map(|(k, v)| (*k, v.as_str())).collect();
                raw_http(addr, "POST", path, &refs, &list_body)
            })
            .await
            .unwrap();
            assert_eq!(status, 200, "{path}: {body}");
            assert_eq!(
                listed_tools(&body),
                crate::mcp::UNIVERSAL_MCP_TOOLS,
                "{path}"
            );
        }

        // 3) Both `/v1/responses` and `/gateway/v1/responses` work on the webhook router.
        for path in ["/gateway/v1/responses", "/v1/responses"] {
            let tok = gw_token.clone();
            let (status, body) = tokio::task::spawn_blocking(move || {
                let hdrs = [("authorization", format!("Bearer {tok}"))];
                let refs: Vec<(&str, &str)> = hdrs.iter().map(|(k, v)| (*k, v.as_str())).collect();
                raw_http(
                    addr,
                    "POST",
                    path,
                    &refs,
                    r#"{"model":"gpt-test","input":"hi"}"#,
                )
            })
            .await
            .unwrap();
            assert_eq!(status, 200, "{path}: {body}");
            assert_eq!(body["id"], "resp_1", "{path}: {body}");
        }

        served.abort();
    });
}

#[test]
fn reap_subagent_ephemeral_dirs_removes_browser_profiles_for_reaped_agent() {
    let dir = crate::test_tempdir();
    let data = dir.path();
    let store = Store::open(data.join("state.db")).unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new(
            "ephem-browser",
            "claude-code",
        ))
        .unwrap();
    let bp = data.join("browser-profiles").join(&agent.name);
    std::fs::create_dir_all(&bp).unwrap();
    std::fs::write(bp.join("cookies.sqlite"), b"db").unwrap();
    store.set_agent_state(&agent.id, "archived").unwrap();

    reap_subagent_ephemeral_dirs_in(&store, data, &agent.id);
    assert!(
        !bp.exists(),
        "browser-profiles/{} must be reaped",
        agent.name
    );
}

#[test]
fn stateless_remote_sandbox_workdir_rejects_host_launcher_paths() {
    let _env = env_lock();
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(anyagents_core::NewAgent::new("sbx-wd", "claude-code"))
        .unwrap();

    // Default logical workspace when nothing is configured (never a coordinator host path).
    let default_wd = resolve_remote_sandbox_workdir(&store, &agent);
    assert_eq!(
        default_wd,
        anyagents_core::stateless::DEFAULT_REMOTE_SANDBOX_WORKDIR
    );

    // Host `<data>/agents/<id>/launcher` path in `agent.workdir` or launch_env MUST be rejected!
    let host_launcher = dir
        .path()
        .join("agents")
        .join(agent.id.as_str())
        .join("launcher")
        .display()
        .to_string();
    store
        .set_agent_launch_env(
            &agent.id,
            &[("ANYAGENTS_SANDBOX_WORKDIR".into(), host_launcher)],
        )
        .unwrap();
    let agent_host = store.agent(&agent.id).unwrap();
    assert_eq!(
        resolve_remote_sandbox_workdir(&store, &agent_host),
        anyagents_core::stateless::DEFAULT_REMOTE_SANDBOX_WORKDIR
    );

    // Explicit logical remote workspace in `agent_sandbox_bindings` is honored.
    let mut binding = anyagents_core::stateless::SandboxBinding::new(agent.id.clone(), "sylphx");
    binding.remote_workdir = "/home/user/agent-data/workspace/repo".to_string();
    store.sandbox_binding_store().upsert(&binding).unwrap();
    assert_eq!(
        resolve_remote_sandbox_workdir(&store, &agent_host),
        "/home/user/agent-data/workspace/repo"
    );
}

#[test]
fn stateless_in_process_remote_harness_delivers_turn_without_os_child_and_keeper_host_is_in_process()
 {
    use anyagents_harness::{Context, DeliverOutcome, Harness, OutboundMessage, Proof};
    use anyagents_keeper::Hosted as _;
    use std::io::{BufRead as _, Write as _};

    let _env = env_lock();
    let prev_mode = std::env::var_os("ANYAGENTS_SANDBOX_MODE");
    unsafe {
        std::env::set_var("ANYAGENTS_SANDBOX_MODE", "remote_required");
    }
    assert!(matches!(
        crate::keeper::host(),
        Ok(crate::vendor::HarnessHost::InProcess)
    ));
    match prev_mode {
        Some(v) => unsafe { std::env::set_var("ANYAGENTS_SANDBOX_MODE", v) },
        None => unsafe { std::env::remove_var("ANYAGENTS_SANDBOX_MODE") },
    }

    // Exercise `InProcessRemoteHarness` end-to-end with an in-memory stream-json worker.
    let mut harness = crate::vendor::InProcessRemoteHarness::start_with_worker(
        true,
        "test",
        |reader, mut writer| {
            for line in reader.lines() {
                let Ok(line) = line else { break };
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let Ok(ev) = serde_json::from_str::<serde_json::Value>(trimmed) else {
                    continue;
                };
                if ev.get("type").and_then(|v| v.as_str()) == Some("user") {
                    writeln!(writer, "{trimmed}")?;
                    let res = serde_json::json!({
                        "type": "result",
                        "subtype": "success",
                        "is_error": false,
                        "session_id": "inproc-sess-42",
                        "result": "ok",
                        "usage": { "input_tokens": 11, "output_tokens": 7 }
                    });
                    writeln!(writer, "{res}")?;
                    writer.flush()?;
                }
            }
            Ok(())
        },
    )
    .expect("start InProcessRemoteHarness");

    assert!(
        harness.process_id().is_none(),
        "InProcessRemoteHarness must spawn ZERO OS child processes"
    );
    let msg = OutboundMessage {
        id: "msg-inproc-1".to_string(),
        seq: 1,
        text: "ping".to_string(),
        sender: Some("operator".to_string()),
        reply_to: None,
        source_time_ms: None,
        context: Context::default(),
    };
    let outcome = harness.deliver(&msg).expect("deliver to in-process worker");
    assert!(
        matches!(outcome, DeliverOutcome::Accepted { .. }),
        "unexpected deliver outcome: {outcome:?}"
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while harness.proven("msg-inproc-1")
        != Some(Proof::Taken {
            turn_finished: true,
        })
        && std::time::Instant::now() < deadline
    {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(
        harness.proven("msg-inproc-1"),
        Some(Proof::Taken {
            turn_finished: true
        })
    );
    assert_eq!(harness.session_id().as_deref(), Some("inproc-sess-42"));
    assert_eq!(harness.turns_served(), 1);
    assert_eq!(harness.served_usage().input_tokens, 11);
    assert_eq!(harness.served_usage().output_tokens, 7);
    harness.stop();
}

#[test]
fn stateless_dockerfile_sets_anyagents_data_not_data_dir() {
    let dockerfile = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Dockerfile");
    let body = std::fs::read_to_string(&dockerfile)
        .unwrap_or_else(|e| panic!("read {}: {e}", dockerfile.display()));
    assert!(
        body.contains("ANYAGENTS_DATA=/var/lib/anyagents"),
        "Dockerfile must export ANYAGENTS_DATA=/var/lib/anyagents"
    );
    assert!(
        !body.contains("ANYAGENTS_DATA_DIR"),
        "Dockerfile must not reference dead ANYAGENTS_DATA_DIR env var"
    );
    for heavy in [
        "chromium",
        "sway",
        "wayvnc",
        "fonts-noto-core",
        "nodejs",
        "oven/bun",
    ] {
        assert!(
            !body.contains(heavy),
            "slim coordinator Dockerfile must not install `{heavy}`"
        );
    }
}

#[test]
fn stateless_two_replicas_mcp_and_gateway_never_revoke_capabilities_or_overwrite_launch_env() {
    use anyagents_gateway::RouteResolver as _;
    use http_body_util::BodyExt as _;

    let url = match std::env::var("ANYAGENTS_TEST_POSTGRES_URL")
        .or_else(|_| std::env::var("TEST_DATABASE_URL"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        Some(u) => u,
        None => {
            eprintln!(
                "skipping stateless_two_replicas_mcp_and_gateway test: ANYAGENTS_TEST_POSTGRES_URL not set"
            );
            return;
        }
    };
    let _env = env_lock();
    let tag = format!("{}_{}", std::process::id(), anyagents_core::now_ms());
    let dir_a = crate::test_tempdir();
    let dir_b = crate::test_tempdir();
    let store_a = Arc::new(
        Store::open_postgres_isolated(&url, dir_a.path(), format!("srv-a-{tag}"))
            .expect("open replica A store"),
    );
    let store_b = Arc::new(
        Store::open_postgres_isolated(&url, dir_b.path(), format!("srv-b-{tag}"))
            .expect("open replica B store"),
    );
    let shared_cap_key = "a".repeat(64);
    store_a.hold_capability_key(&shared_cap_key).unwrap();
    store_b.hold_capability_key(&shared_cap_key).unwrap();

    let state_a = AppState {
        store: Arc::clone(&store_a),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: dir_a.path().into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
    };
    let state_b = AppState {
        store: Arc::clone(&store_b),
        health: HealthBoard::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: auth::Mode::Trusted {
            data_dir: dir_b.path().into(),
        },
        identity: Arc::new(ServingIdentity::capture()),
        profiles: Default::default(),
    };

    // 1. Replica A creates a remote-sandbox-backed agent with a live local capability,
    //    a PG `agent_sandbox_bindings` row, a remote session token, a PG gateway route,
    //    and a public cloud `launch_env`.
    let agent = store_a
        .create_agent(anyagents_core::NewAgent::new(
            format!("rep_sbx_{tag}"),
            "claude-code",
        ))
        .unwrap();
    let cap_a = store_a.issue_capability(&agent.id).unwrap();
    let mut binding = anyagents_core::stateless::SandboxBinding::new(agent.id.clone(), "sylphx");
    binding.volume_id = format!("vol-srv-{tag}");
    binding.lease_id = Some(format!("lease-srv-{tag}"));
    binding.lease_state = "ready".to_string();
    store_a.sandbox_binding_store().upsert(&binding).unwrap();

    let remote_tok = store_a
        .begin_remote_session(&agent.id, 3_600_000)
        .unwrap()
        .token;

    let key = anyagents_core::stateless::MasterKey::from_bytes(&[0x55u8; 32]).unwrap();
    let gw_tok = crate::gateway::store_key_in(
        &store_a,
        &agent,
        "https://api.example.test/v1",
        "gpt-5",
        "sk-srv-synthetic",
        Some(key.clone()),
    )
    .expect("store gateway route in PG");
    let cloud_env = vec![
        (
            "OPENAI_BASE_URL".to_string(),
            "https://public-gateway.example.test/v1".to_string(),
        ),
        ("OPENAI_API_KEY".to_string(), gw_tok.clone()),
    ];
    store_a.set_agent_launch_env(&agent.id, &cloud_env).unwrap();
    let expected_launch_env = store_a.agent(&agent.id).unwrap().launch_env;

    // 2. Verify replica B has ZERO local sandbox/capability/provider files on disk!
    let cfg_b = anyagents_core::agent_config_dir(dir_b.path(), &agent.id);
    assert!(!cfg_b.exists());

    // 3. Replica B serves HTTP MCP using PG `agent_sandbox_bindings`
    //    and direct `Principal::Agent` injection WITHOUT calling `operator::ensure()`.
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {remote_tok}").parse().unwrap(),
        );
        let resp = mcp_post_route(
            State(state_b.clone()),
            headers.clone(),
            axum::body::Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/list",
                    "params": {}
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| {
            panic!(
                "replica B MCP tools/list failed: {}: {}",
                e.status, e.message
            )
        });
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            listed_tools(&parsed),
            crate::mcp::UNIVERSAL_MCP_TOOLS,
            "replica B must expose the universal 7-tool MCP surface"
        );

        // Also call an MCP tool (`inbox`) to verify the in-process router accepts
        // `InjectedPrincipal` cleanly with zero local capability file on replica B!
        let call_resp = mcp_post_route(
            State(state_b.clone()),
            headers,
            axum::body::Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "inbox",
                        "arguments": {}
                    }
                }))
                .unwrap(),
            ),
        )
        .await
        .unwrap_or_else(|e| {
            panic!(
                "replica B MCP inbox call failed: {}: {}",
                e.status, e.message
            )
        });
        assert_eq!(call_resp.status(), StatusCode::OK);
    });

    // 4. Replica A's original capability token MUST STILL BE VALID in PG (never revoked by replica B!).
    let authed = store_a
        .authenticate_agent(&cap_a)
        .expect("replica A capability MUST NOT be revoked when replica B serves MCP");
    assert_eq!(authed.id, agent.id);
    let _ = state_a;

    // 5. Replica B resolves replica A's gateway route directly from PG `gateway_routes`!
    let resolver_b =
        crate::gateway::PgGatewayRoutes::with_master_key(Arc::clone(&store_b), Some(key));
    let route_b = resolver_b
        .resolve(&gw_tok)
        .expect("replica B resolves gateway token from PG");
    assert_eq!(route_b.agent, agent.name);
    assert_eq!(route_b.api_key, "sk-srv-synthetic");

    // 6. Under `ANYAGENTS_SANDBOX_MODE=remote_required`, `gateway::start` on replica B
    //    MUST NOT bind a random loopback port and MUST NOT overwrite `agents.launch_env` in PG!
    std::fs::write(dir_b.path().join("database-url"), format!("{url}\n")).unwrap();
    let prev_mode = std::env::var_os("ANYAGENTS_SANDBOX_MODE");
    unsafe {
        std::env::set_var("ANYAGENTS_SANDBOX_MODE", "remote_required");
    }
    let started_b = crate::gateway::start(dir_b.path()).expect("gateway::start in remote_required");
    match prev_mode {
        Some(v) => unsafe { std::env::set_var("ANYAGENTS_SANDBOX_MODE", v) },
        None => unsafe { std::env::remove_var("ANYAGENTS_SANDBOX_MODE") },
    }
    assert!(!started_b.moved);
    assert!(
        !dir_b.path().join("gateway.port").exists(),
        "stateless remote_required replica MUST NOT bind pod-local gateway.port"
    );
    let reloaded = store_a.agent(&agent.id).unwrap();
    assert_eq!(
        reloaded.launch_env, expected_launch_env,
        "stateless replica gateway::start MUST NOT overwrite agents.launch_env in PG"
    );
    assert!(
        reloaded.launch_env.iter().any(|(k, v)| {
            k == "OPENAI_BASE_URL" && v == "https://public-gateway.example.test/v1"
        }),
        "public gateway URL must remain intact: {:?}",
        reloaded.launch_env
    );

    // Clean up test agent in PG.
    store_a.delete_agent(&agent.id).unwrap();
}

#[test]
fn decode_mcp_request_payload_supports_plain_json_and_base64_waf_framing() {
    use base64::Engine as _;

    let raw = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/call",
        "params": {
            "name": "send",
            "arguments": { "message": "ls -la | grep foo && cat /etc/hosts; SELECT * FROM t" }
        }
    });
    let raw_bytes = serde_json::to_vec(&raw).unwrap();

    // Plain JSON works without any encoding header.
    let plain_headers = axum::http::HeaderMap::new();
    let decoded_plain =
        decode_mcp_request_payload(&plain_headers, axum::body::Bytes::from(raw_bytes.clone()))
            .unwrap();
    assert_eq!(decoded_plain, raw);

    // Opaque base64 framing decodes to the identical JSON-RPC value.
    let mut b64_headers = axum::http::HeaderMap::new();
    b64_headers.insert(
        axum::http::header::HeaderName::from_static(MCP_PAYLOAD_ENCODING_HEADER),
        axum::http::HeaderValue::from_static("base64"),
    );
    let b64_body = base64::engine::general_purpose::STANDARD.encode(&raw_bytes);
    let decoded_b64 =
        decode_mcp_request_payload(&b64_headers, axum::body::Bytes::from(b64_body)).unwrap();
    assert_eq!(decoded_b64, raw);
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
