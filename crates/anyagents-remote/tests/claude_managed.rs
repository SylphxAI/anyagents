//! Contract tests of the remote-session executor over the Claude Managed
//! Agents adapter, against a scripted Managed Agents API on loopback: create,
//! deliver, steer while running, the event stream, reconnect with dedup,
//! interrupt, the budget cap, pause and stop. No request leaves the machine.

mod support;

use anyagents_core::remote_execution_status as status;
use anyagents_harness::{DeliverOutcome, DeliveryLevel, Harness, OutboundMessage, TurnState};
use anyagents_remote::Executor;
use support::{KEY, MCP_URL, Vendor, config, store_with_agent, transcript_lines, wait_until};

fn message(id: &str, text: &str) -> OutboundMessage {
    OutboundMessage {
        id: id.into(),
        seq: 1,
        text: text.into(),
        sender: None,
        reply_to: None,
        source_time_ms: None,
        context: Default::default(),
    }
}

fn accepted() -> DeliverOutcome {
    DeliverOutcome::Accepted {
        turn_finished: false,
    }
}

#[test]
fn create_sends_the_documented_shapes_and_grants_a_revocable_token() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("created");

    // A metered agent never starts without a cap, and nothing is created.
    let mut uncapped = config(&vendor, &store, &agent, 1);
    uncapped.budget_cents = None;
    let refused = Executor::launch(uncapped).err().unwrap();
    assert!(refused.contains("spend cap"), "{refused}");
    assert!(vendor.lock().requests.is_empty());

    let executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    for request in &vendor.lock().requests {
        assert_eq!(request.api_key.as_deref(), Some(KEY), "{}", request.path);
        assert_eq!(request.beta.as_deref(), Some("managed-agents-2026-04-01"));
        assert_eq!(request.version.as_deref(), Some("2026-10-21"));
    }
    let environment = &vendor.requests("POST", "/v1/environments")[0].body;
    assert_eq!(environment["config"]["type"], "cloud");

    let definition = &vendor.requests("POST", "/v1/agents")[0].body;
    assert_eq!(definition["model"], "claude-opus-5-5");
    assert_eq!(definition["system"], "Answer briefly.");
    assert_eq!(definition["mcp_servers"][0]["type"], "url");
    assert_eq!(definition["mcp_servers"][0]["url"], MCP_URL);
    let toolsets: Vec<&str> = definition["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["type"].as_str().unwrap())
        .collect();
    assert_eq!(toolsets, ["agent_toolset_20260401", "mcp_toolset"]);
    assert_eq!(
        definition["tools"][1]["default_config"]["permission_policy"]["type"],
        "always_allow"
    );

    // The coordination token lives only in the vault, as a static bearer
    // for the public MCP URL, and authenticates as this agent.
    let credential = vendor
        .requests("POST", "/v1/vaults/")
        .into_iter()
        .find(|r| r.path.ends_with("/credentials"))
        .unwrap()
        .body;
    assert_eq!(credential["auth"]["type"], "static_bearer");
    assert_eq!(credential["auth"]["mcp_server_url"], MCP_URL);
    let token = credential["auth"]["token"].as_str().unwrap().to_string();
    assert_eq!(
        store.authenticate_remote_session(&token).unwrap().agent_id,
        agent.id
    );
    assert!(!definition.to_string().contains(&token));

    let session = &vendor.requests("POST", "/v1/sessions")[0].body;
    assert_eq!(session["budget"]["type"], "limit");
    assert_eq!(session["budget"]["max_list_cost"]["amount"], "2500");
    assert_eq!(session["budget"]["max_list_cost"]["currency"], "USD");
    assert_eq!(session["vault_ids"].as_array().unwrap().len(), 1);
    assert_eq!(session["agent"]["type"], "agent");
    assert_eq!(session["agent"]["version"], 1);
    assert_eq!(session["metadata"]["anyagents_agent"], agent.id.as_str());
    assert!(!session.to_string().contains(&token));

    let row = store.remote_execution(&agent.id).unwrap().unwrap();
    assert_eq!(row.status, status::ATTACHED);
    assert_eq!(row.adapter, "claude-managed");
    assert_eq!(
        row.vendor_session.as_deref(),
        Some(vendor.only_session().as_str())
    );
    assert_eq!(row.budget_cents, Some(2500));
    assert!(!row.adapter_state.to_string().contains(&token));

    let health = executor.health();
    assert!(health.process_alive);
    assert_eq!(health.turn_state, TurnState::Idle);
    assert_eq!(health.delivery.level, DeliveryLevel::MidTurn);
    assert!(health.delivery.idempotent);
}

#[test]
fn delivered_mail_runs_a_turn_whose_events_become_history_usage_and_cost() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("created");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();

    assert_eq!(
        executor.deliver(&message("msg_1", "hello")).unwrap(),
        accepted()
    );
    wait_until("the turn ends", || {
        executor.turns_served() == 1 && executor.health().turn_state == TurnState::Idle
    });
    let sent = vendor.sent("user.message");
    assert_eq!(sent.len(), 1);
    let text = sent[0]["content"][0]["text"].as_str().unwrap();
    assert!(text.starts_with("anyagents-message:msg_1\n"), "{text}");
    assert!(text.contains("hello"));

    let usage = executor.served_usage();
    assert_eq!((usage.input_tokens, usage.output_tokens), (100, 10));
    assert_eq!(
        usage.reported_cost_micros,
        Some(70_000),
        "7 cents at list price"
    );

    // Offered again, it is proven, not written again.
    assert_eq!(
        executor.deliver(&message("msg_1", "hello")).unwrap(),
        accepted()
    );
    assert_eq!(vendor.sent("user.message").len(), 1);

    let lines = transcript_lines(&store, &agent.id);
    let ids: Vec<&str> = lines.iter().map(|l| l["id"].as_str().unwrap()).collect();
    let unique: std::collections::HashSet<&&str> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "each event once: {ids:?}");
    assert!(
        lines
            .iter()
            .any(|l| l["role"] == "user" && l["content"].as_str().unwrap().contains("hello"))
    );
    assert!(
        lines.iter().any(
            |l| l["role"] == "assistant" && l["content"].as_str().unwrap().starts_with("done:")
        )
    );

    let row = store.remote_execution(&agent.id).unwrap().unwrap();
    assert_eq!(row.totals.turns, 1);
    assert_eq!(row.cost_cents, Some(7));
    assert!(row.resume.cursor.is_some());
    assert_eq!(row.resume.held, vec!["msg_1".to_string()]);
}

#[test]
fn mail_for_a_running_turn_is_queued_into_it() {
    let vendor = Vendor::start();
    vendor.set_hold(true);
    let (_dir, store, agent) = store_with_agent("created");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();

    assert_eq!(
        executor.deliver(&message("msg_1", "start")).unwrap(),
        accepted()
    );
    let session = vendor.only_session();
    wait_until("the turn runs", || vendor.status(&session) == "running");
    wait_until("the executor sees it", || {
        executor.health().turn_state == TurnState::InTurn
    });
    // Steering: both are admitted while the turn runs, and queued in order.
    assert_eq!(
        executor.deliver(&message("msg_2", "also this")).unwrap(),
        accepted()
    );
    assert_eq!(
        executor.deliver(&message("msg_3", "and this")).unwrap(),
        accepted()
    );
    assert_eq!(vendor.sent("user.message").len(), 3);
    assert_eq!(vendor.lock().sessions[&session].queue.len(), 2);
    assert_eq!(executor.turns_served(), 0);

    vendor.finish(&session);
    wait_until("the one turn ends", || {
        executor.turns_served() == 1 && executor.health().turn_state == TurnState::Idle
    });
    assert_eq!(executor.served_usage().input_tokens, 300);
    let inputs = transcript_lines(&store, &agent.id)
        .into_iter()
        .filter(|l| l["role"] == "user")
        .count();
    assert_eq!(inputs, 3);
}

#[test]
fn a_dropped_stream_reconnects_and_replays_what_it_missed_once() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("created");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    assert_eq!(
        executor.deliver(&message("msg_1", "one")).unwrap(),
        accepted()
    );
    wait_until("the first turn ends", || executor.turns_served() == 1);
    let session = vendor.only_session();
    let streams_before = vendor
        .requests("GET", &format!("/v1/sessions/{session}/events/stream"))
        .len();

    // The stream drops; a whole turn runs before the client is back.
    vendor.drop_streams(&session);
    assert_eq!(
        executor.deliver(&message("msg_2", "two")).unwrap(),
        accepted()
    );
    assert_eq!(
        vendor.status(&session),
        "idle",
        "the turn ran with no stream open"
    );

    wait_until("the missed turn is replayed", || {
        executor.turns_served() == 2 && executor.health().turn_state == TurnState::Idle
    });
    assert!(
        vendor
            .requests("GET", &format!("/v1/sessions/{session}/events/stream"))
            .len()
            > streams_before
    );
    // The history was read in pages to cover the gap.
    assert!(
        vendor
            .requests("GET", &format!("/v1/sessions/{session}/events"))
            .iter()
            .any(|r| r.body.get("page").is_some())
    );
    let usage = executor.served_usage();
    assert_eq!(
        (usage.input_tokens, usage.output_tokens),
        (200, 20),
        "each turn once"
    );
    let lines = transcript_lines(&store, &agent.id);
    let ids: Vec<&str> = lines.iter().map(|l| l["id"].as_str().unwrap()).collect();
    let unique: std::collections::HashSet<&&str> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "no event twice: {ids:?}");
    assert_eq!(lines.iter().filter(|l| l["role"] == "user").count(), 2);
}

#[test]
fn an_interrupt_stops_the_running_turn() {
    let vendor = Vendor::start();
    vendor.set_hold(true);
    let (_dir, store, agent) = store_with_agent("created");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    executor.deliver(&message("msg_1", "long job")).unwrap();
    let session = vendor.only_session();
    wait_until("the turn runs", || {
        executor.health().turn_state == TurnState::InTurn && vendor.status(&session) == "running"
    });

    executor.cancel().unwrap();
    assert_eq!(vendor.sent("user.interrupt").len(), 1);
    wait_until("the turn ends", || {
        executor.health().turn_state == TurnState::Idle && executor.turns_served() == 1
    });
    assert_eq!(vendor.status(&session), "idle");
    // The session stays: the next message opens a new turn in it.
    vendor.set_hold(false);
    assert_eq!(
        executor.deliver(&message("msg_2", "next")).unwrap(),
        accepted()
    );
    wait_until("the next turn ends", || executor.turns_served() == 2);
    assert_eq!(vendor.lock().sessions.len(), 1);
}

#[test]
fn at_the_budget_cap_mail_is_held_until_the_cap_is_raised() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("created");
    // Each model request costs 7 cents: the second turn reaches a 10 cent cap.
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 10)).unwrap();
    executor.deliver(&message("msg_1", "one")).unwrap();
    wait_until("the first turn ends", || executor.turns_served() == 1);
    executor.deliver(&message("msg_2", "two")).unwrap();
    wait_until("the session pauses at its cap", || {
        executor.health().quota_reset_at_ms.is_some()
    });
    let health = executor.health();
    assert_eq!(
        health.turn_state,
        TurnState::InTurn,
        "the paused turn is open"
    );
    assert!(health.process_alive);
    assert_eq!(executor.turns_served(), 1);
    let row = store.remote_execution(&agent.id).unwrap().unwrap();
    assert_eq!(row.vendor_status.as_deref(), Some("budget_reached"));
    assert_eq!(row.cost_cents, Some(14));

    // Mail waits as a quota hold; nothing is written to the paused session.
    assert!(matches!(
        executor.deliver(&message("msg_3", "three")).unwrap(),
        DeliverOutcome::Quota { .. }
    ));
    assert_eq!(vendor.sent("user.message").len(), 2);

    // Raising the cap resumes the paused work and releases the mail.
    executor.set_budget(100).unwrap();
    let session = vendor.only_session();
    let update = vendor
        .requests("POST", &format!("/v1/sessions/{session}"))
        .into_iter()
        .find(|r| r.path == format!("/v1/sessions/{session}"))
        .unwrap();
    assert_eq!(update.body["budget"]["max_list_cost"]["amount"], "100");
    wait_until("the resumed turn ends", || {
        executor.turns_served() == 2
            && executor.health().quota_reset_at_ms.is_none()
            && executor.health().turn_state == TurnState::Idle
    });
    assert_eq!(
        executor.deliver(&message("msg_3", "three")).unwrap(),
        accepted()
    );
    wait_until("the third turn ends", || executor.turns_served() == 3);
    assert_eq!(
        store
            .remote_execution(&agent.id)
            .unwrap()
            .unwrap()
            .budget_cents,
        Some(100)
    );
}

#[test]
fn pausing_interrupts_and_revokes_and_resuming_reconnects_to_the_same_session() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("running");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    executor.deliver(&message("msg_1", "one")).unwrap();
    wait_until("the turn ends", || executor.turns_served() == 1);
    let session = vendor.only_session();
    let first_token = vendor.token_of_session(&session).unwrap();

    store.set_agent_state(&agent.id, "paused").unwrap();
    drop(executor);
    assert_eq!(vendor.sent("user.interrupt").len(), 1);
    assert!(!vendor.lock().sessions[&session].archived);
    assert!(store.authenticate_remote_session(&first_token).is_err());
    assert_eq!(
        store.remote_execution(&agent.id).unwrap().unwrap().status,
        status::PARKED
    );

    store.set_agent_state(&agent.id, "running").unwrap();
    let agent = store.agent(&agent.id).unwrap();
    let executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    assert_eq!(
        vendor
            .requests("POST", "/v1/sessions")
            .iter()
            .filter(|r| r.path == "/v1/sessions")
            .count(),
        1
    );
    // A fresh token reached the vault in place, and authenticates.
    let rotated = vendor.token_of_session(&session).unwrap();
    assert_ne!(rotated, first_token);
    assert_eq!(
        store
            .authenticate_remote_session(&rotated)
            .unwrap()
            .agent_id,
        agent.id
    );
    // A new start begins a new accounting lineage: nothing is served twice.
    assert_eq!(executor.turns_served(), 0);
    assert_eq!(executor.served_usage().input_tokens, 0);
}

#[test]
fn stopping_an_archived_agent_ends_its_session_and_revokes_its_token() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("running");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    executor.deliver(&message("msg_1", "one")).unwrap();
    wait_until("the turn ends", || executor.turns_served() == 1);
    let session = vendor.only_session();
    let token = vendor.token_of_session(&session).unwrap();
    assert!(store.authenticate_remote_session(&token).is_ok());

    store.set_agent_state(&agent.id, "archived").unwrap();
    drop(executor);
    let inner = vendor.lock();
    assert!(inner.sessions[&session].archived, "the session is archived");
    let vault = inner.sessions[&session].vault.clone().unwrap();
    assert!(
        inner.archived_vaults.contains(&vault),
        "the vault's token is purged"
    );
    drop(inner);
    assert!(store.authenticate_remote_session(&token).is_err());
    assert_eq!(
        store.remote_execution(&agent.id).unwrap().unwrap().status,
        status::ENDED
    );
}

#[test]
fn a_running_agents_executor_dropped_by_a_stopping_kernel_leaves_the_session_attached() {
    let vendor = Vendor::start();
    vendor.set_hold(true);
    let (_dir, store, agent) = store_with_agent("running");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    executor.deliver(&message("msg_1", "long job")).unwrap();
    let session = vendor.only_session();
    wait_until("the turn runs", || vendor.status(&session) == "running");
    let token = vendor.token_of_session(&session).unwrap();

    drop(executor);
    assert!(vendor.sent("user.interrupt").is_empty());
    assert_eq!(vendor.status(&session), "running");
    assert!(store.authenticate_remote_session(&token).is_ok());
    let row = store.remote_execution(&agent.id).unwrap().unwrap();
    assert!(row.is_attached());
    assert_eq!(anyagents_remote::attached(&store), vec![agent.id.clone()]);
}
