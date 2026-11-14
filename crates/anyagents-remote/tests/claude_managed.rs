//! Contract tests of the remote-session executor over the Claude Managed
//! Agents adapter, against a scripted Managed Agents API on loopback: create,
//! deliver, steer while running, the event stream, reconnect with dedup,
//! interrupt, the budget cap, pause and stop, and the fault paths: lost and
//! throttled answers, a kernel stop while a turn closes, replaced and
//! terminated sessions, failed creates, and bounded history reads. No request
//! leaves the machine.

mod support;

use std::time::Duration;

use anyagents_core::remote_execution_status as status;
use anyagents_harness::{
    DeliverOutcome, DeliveryLevel, Harness, OutboundMessage, Proof, TurnState,
};
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

fn taken() -> Option<Proof> {
    Some(Proof::Taken {
        turn_finished: false,
    })
}

fn accepted() -> DeliverOutcome {
    DeliverOutcome::Accepted {
        turn_finished: false,
    }
}

fn unique_ids(lines: &[serde_json::Value]) -> bool {
    let ids: Vec<&str> = lines.iter().map(|l| l["id"].as_str().unwrap()).collect();
    let unique: std::collections::HashSet<&&str> = ids.iter().collect();
    unique.len() == ids.len()
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
    assert_eq!(health.delivery.level, DeliveryLevel::NextStep);
    assert!(health.delivery.idempotent);
}

#[test]
fn mail_counts_as_delivered_once_the_session_processed_it() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("created");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();

    assert_eq!(
        executor.deliver(&message("msg_1", "hello")).unwrap(),
        DeliverOutcome::InFlight
    );
    wait_until("the session processed it", || {
        executor.proven("msg_1") == taken()
    });
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
    wait_until("the turn's cost is read", || {
        executor.served_usage().reported_cost_micros == Some(70_000)
    });

    // Offered again, it is proven, not written again.
    assert_eq!(
        executor.deliver(&message("msg_1", "hello")).unwrap(),
        accepted()
    );
    assert_eq!(vendor.sent("user.message").len(), 1);

    let lines = transcript_lines(&store, &agent.id);
    assert!(unique_ids(&lines), "each event once: {lines:?}");
    assert_eq!(lines.iter().filter(|l| l["role"] == "user").count(), 1);
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
    assert!(row.resume.queued.is_empty());
}

#[test]
fn mail_for_a_running_turn_is_queued_into_it_and_proven_when_taken() {
    let vendor = Vendor::start();
    vendor.set_hold(true);
    let (_dir, store, agent) = store_with_agent("created");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();

    executor.deliver(&message("msg_1", "start")).unwrap();
    let session = vendor.only_session();
    wait_until("the turn runs", || vendor.status(&session) == "running");
    wait_until("the first is taken", || executor.proven("msg_1") == taken());
    // Steering: both are written while the turn runs, queued in order, and
    // not proven until the session takes them.
    for id in ["msg_2", "msg_3"] {
        assert_eq!(
            executor.deliver(&message(id, "more")).unwrap(),
            DeliverOutcome::InFlight
        );
    }
    assert_eq!(vendor.sent("user.message").len(), 3);
    assert_eq!(vendor.lock().sessions[&session].queue.len(), 2);
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(executor.proven("msg_2"), None);
    assert_eq!(executor.turns_served(), 0);

    vendor.finish(&session);
    wait_until("the one turn ends with all three taken", || {
        executor.turns_served() == 1
            && executor.proven("msg_2") == taken()
            && executor.proven("msg_3") == taken()
    });
    assert_eq!(executor.served_usage().input_tokens, 300);
    let inputs = transcript_lines(&store, &agent.id)
        .into_iter()
        .filter(|l| l["role"] == "user")
        .count();
    assert_eq!(inputs, 3);
}

#[test]
fn a_dropped_stream_reconnects_and_replays_what_it_missed_once_reading_back_to_the_cursor() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("created");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    for (n, id) in ["msg_1", "msg_2", "msg_3", "msg_4"].iter().enumerate() {
        executor.deliver(&message(id, "warm")).unwrap();
        wait_until("a turn ends", || executor.turns_served() as usize == n + 1);
    }
    let session = vendor.only_session();
    let streams_before = vendor
        .requests("GET", &format!("/v1/sessions/{session}/events/stream"))
        .len();
    let reads_before = vendor.history_reads(&session).len();

    // The stream drops; a whole turn runs before the client is back.
    vendor.drop_streams(&session);
    executor.deliver(&message("msg_5", "five")).unwrap();
    assert_eq!(
        vendor.status(&session),
        "idle",
        "the turn ran with no stream open"
    );

    wait_until("the missed turn is replayed", || {
        executor.turns_served() == 5 && executor.health().turn_state == TurnState::Idle
    });
    assert!(
        vendor
            .requests("GET", &format!("/v1/sessions/{session}/events/stream"))
            .len()
            > streams_before
    );
    // The replay read the history newest first and stopped at the cursor:
    // two pages of three cover the missed turn, not the whole history.
    let reads: Vec<_> = vendor.history_reads(&session)[reads_before..].to_vec();
    assert!(!reads.is_empty());
    assert!(reads.iter().all(|q| q["order"] == "desc"), "{reads:?}");
    assert!(
        reads.len() <= 3,
        "{} pages of {} events",
        reads.len(),
        vendor.lock().sessions[&session].events.len()
    );
    let usage = executor.served_usage();
    assert_eq!(
        (usage.input_tokens, usage.output_tokens),
        (500, 50),
        "each turn once"
    );
    let lines = transcript_lines(&store, &agent.id);
    assert!(unique_ids(&lines), "no event twice: {lines:?}");
    assert_eq!(lines.iter().filter(|l| l["role"] == "user").count(), 5);
    // The very first attach had no cursor: it read the history oldest first.
    assert_eq!(vendor.history_reads(&session)[0]["order"], "asc");
}

#[test]
fn an_interrupt_stops_the_turn_and_mail_it_never_took_is_offered_again() {
    let vendor = Vendor::start();
    vendor.set_hold(true);
    let (_dir, store, agent) = store_with_agent("created");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    executor.deliver(&message("msg_1", "long job")).unwrap();
    let session = vendor.only_session();
    wait_until("the turn runs", || {
        executor.proven("msg_1") == taken() && vendor.status(&session) == "running"
    });
    executor
        .deliver(&message("msg_2", "queued behind"))
        .unwrap();
    assert_eq!(executor.proven("msg_2"), None);

    executor.cancel().unwrap();
    assert_eq!(vendor.sent("user.interrupt").len(), 1);
    wait_until("the turn ends", || {
        executor.health().turn_state == TurnState::Idle && executor.turns_served() == 1
    });
    assert_eq!(vendor.status(&session), "idle");
    // The interrupt dropped msg_2 unread: it is not delivered, and the next
    // offer writes it again into the same session.
    assert_eq!(executor.proven("msg_2"), None);
    vendor.set_hold(false);
    assert_eq!(
        executor
            .deliver(&message("msg_2", "queued behind"))
            .unwrap(),
        DeliverOutcome::InFlight
    );
    wait_until("it is taken by the next turn", || {
        executor.proven("msg_2") == taken() && executor.turns_served() == 2
    });
    assert_eq!(vendor.sent("user.message").len(), 3);
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
    wait_until("the record says so", || {
        let row = store.remote_execution(&agent.id).unwrap().unwrap();
        row.vendor_status.as_deref() == Some("budget_reached") && row.cost_cents == Some(14)
    });

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
    executor.deliver(&message("msg_3", "three")).unwrap();
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
fn a_parked_session_of_an_agent_archived_later_is_ended_when_settled() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("running");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    executor.deliver(&message("msg_1", "one")).unwrap();
    wait_until("the turn ends", || executor.turns_served() == 1);
    store.set_agent_state(&agent.id, "idle").unwrap();
    drop(executor);
    assert_eq!(
        store.remote_execution(&agent.id).unwrap().unwrap().status,
        status::PARKED
    );
    store.set_agent_state(&agent.id, "archived").unwrap();
    let adapter = config(&vendor, &store, &agent, 2500).adapter;
    assert_eq!(
        anyagents_remote::settle(&store, &agent.id, adapter.as_ref()),
        anyagents_remote::Settled::Ended
    );
    let session = vendor.only_session();
    assert!(vendor.lock().sessions[&session].archived);
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

/// Review item 2: a 5xx, or a success whose body cannot be read, after the
/// vendor applied the message leaves its outcome unknown; the next offer asks
/// the session before writing again.
#[test]
fn an_answer_lost_after_the_vendor_applied_the_message_never_writes_it_twice() {
    for fault in ["504", "unreadable"] {
        let vendor = Vendor::start();
        vendor.set_hold(true);
        let (_dir, store, agent) = store_with_agent("created");
        let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
        let session = vendor.only_session();
        vendor.drop_streams(&session);
        match fault {
            "504" => vendor.lock().fail_after_apply = Some(504),
            _ => vendor.lock().unreadable_answer = true,
        }
        assert_eq!(
            executor.deliver(&message("msg_1", "do the thing")).unwrap(),
            DeliverOutcome::Deferred,
            "{fault}"
        );
        // Offered again once its backoff is over: settled from the history.
        let mut outcome = DeliverOutcome::Deferred;
        wait_until("the retry is due", || {
            outcome = executor.deliver(&message("msg_1", "do the thing")).unwrap();
            outcome != DeliverOutcome::Deferred
        });
        // Taken by this executor once the session shows it, never written
        // again.
        assert!(
            matches!(
                outcome,
                DeliverOutcome::InFlight | DeliverOutcome::Accepted { .. }
            ),
            "{fault}: {outcome:?}"
        );
        assert_eq!(vendor.sent("user.message").len(), 1, "{fault}");
        assert_eq!(vendor.stored_messages(&session), 1, "{fault}");
    }
}

/// Review items 2 and 7: a lost answer is settled by reading the history
/// newest first, stopping at the marker or at the executor's cursor; nothing
/// older is read.
#[test]
fn a_lost_answer_is_settled_from_the_newest_history_back_to_the_cursor() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("created");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    for (n, id) in ["msg_1", "msg_2", "msg_3"].iter().enumerate() {
        executor.deliver(&message(id, "x")).unwrap();
        wait_until("a turn ends", || executor.turns_served() as usize == n + 1);
    }
    let session = vendor.only_session();
    let handle = executor.handle().clone();
    let adapter = config(&vendor, &store, &agent, 2500).adapter;
    let cursor = store
        .remote_execution(&agent.id)
        .unwrap()
        .unwrap()
        .resume
        .cursor
        .unwrap();
    let reads = |vendor: &Vendor| vendor.history_reads(&session).len();

    // After the cursor: found, newest first, in the first page.
    let before = reads(&vendor);
    executor.deliver(&message("msg_4", "x")).unwrap();
    wait_until("a turn ends", || executor.turns_served() == 4);
    assert!(anyagents_remote::block_on(adapter.holds(&handle, "msg_4", Some(&cursor))).unwrap());
    let lookups = &vendor.history_reads(&session)[before..];
    assert!(lookups.iter().all(|q| q["order"] == "desc"), "{lookups:?}");
    assert!(lookups.len() <= 2, "{lookups:?}");

    // Before the cursor: the executor's own record answers; the lookup stops
    // at the cursor and reads nothing older.
    let before = reads(&vendor);
    assert!(!anyagents_remote::block_on(adapter.holds(&handle, "msg_1", Some(&cursor))).unwrap());
    assert!(reads(&vendor) - before <= 2, "read past the cursor");
    // Never written: not held, with the whole (short) history read.
    assert!(!anyagents_remote::block_on(adapter.holds(&handle, "msg_9", None)).unwrap());
}

/// Review item 1: mail the vendor does not take waits out a backoff, never
/// a hot loop, and never sooner than its `retry-after`.
#[test]
fn mail_the_vendor_refuses_backs_off_and_honours_retry_after() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("created");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    vendor.lock().fail_before_apply = Some(429);
    vendor.lock().retry_after_secs = Some(3);
    let started = std::time::Instant::now();
    let mut offers = 0;
    while started.elapsed() < Duration::from_secs(2) {
        offers += 1;
        assert_eq!(
            executor.deliver(&message("msg_1", "x")).unwrap(),
            DeliverOutcome::Deferred
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(offers > 50, "offered {offers} times");
    assert_eq!(
        vendor.sent("user.message").len(),
        1,
        "one POST, then nothing until its retry-after"
    );
    vendor.lock().fail_before_apply = None;
    wait_until("the retry after it goes through", || {
        executor.deliver(&message("msg_1", "x")).unwrap() == DeliverOutcome::InFlight
    });
    assert!(started.elapsed() >= Duration::from_secs(3));
    assert_eq!(vendor.sent("user.message").len(), 2);
}

/// Review item 3: the cursor never passes a turn's end the record does not
/// show, even when the kernel stops while it reads the turn's cost.
#[test]
fn a_kernel_stop_while_a_turn_closes_leaves_it_closed_in_the_record() {
    let vendor = Vendor::start();
    vendor.set_hold(true);
    let (_dir, store, agent) = store_with_agent("running");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    let session = vendor.only_session();
    executor.deliver(&message("msg_1", "one")).unwrap();
    wait_until("running", || {
        executor.health().turn_state == TurnState::InTurn && executor.proven("msg_1") == taken()
    });
    vendor.lock().snapshot_delay_ms = 3000;
    vendor.finish(&session);
    // The idle event is applied and the cost read is under way.
    wait_until("the turn closes", || {
        store
            .remote_execution(&agent.id)
            .unwrap()
            .unwrap()
            .totals
            .turns
            == 1
    });
    executor.detach();
    vendor.lock().snapshot_delay_ms = 0;
    let row = store.remote_execution(&agent.id).unwrap().unwrap();
    assert_eq!(row.totals.turns, 1);
    assert_eq!(row.vendor_status.as_deref(), Some("idle"));

    let (again, _lock) = anyagents_remote::take_over(config(&vendor, &store, &agent, 2500))
        .unwrap()
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(again.health().turn_state, TurnState::Idle);
    assert_eq!(again.turns_served(), 1, "the base of the lineage is kept");
}

fn sessions_created(vendor: &Vendor) -> usize {
    vendor
        .requests("POST", "/v1/sessions")
        .iter()
        .filter(|r| r.path == "/v1/sessions")
        .count()
}

/// Review item 4: the cap is the agent's. A replacement session gets what is
/// left of it, none starts once it is spent, a session the vendor keeps
/// ending is not replaced at once, and an old session is stopped before a
/// new one replaces it.
#[test]
fn the_cap_spans_sessions_and_a_replaced_session_is_stopped_first() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("running");
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 20)).unwrap();
    executor.deliver(&message("msg_1", "one")).unwrap();
    wait_until("the turn and its cost", || {
        executor.turns_served() == 1
            && store
                .remote_execution(&agent.id)
                .unwrap()
                .unwrap()
                .cost_cents
                == Some(7)
    });
    let first = vendor.only_session();

    // The vendor ends the session.
    vendor.terminate(&first);
    wait_until("the executor sees it", || !executor.health().process_alive);
    drop(executor);
    let row = store.remote_execution(&agent.id).unwrap().unwrap();
    assert_eq!(row.status, status::ENDED);
    assert_eq!(row.terminations, 1);
    assert!(
        vendor.lock().sessions[&first].archived,
        "stopped before any replacement"
    );

    // Not replaced at once.
    let refused = Executor::launch(config(&vendor, &store, &agent, 20))
        .err()
        .unwrap();
    assert!(refused.contains("no sooner"), "{refused}");
    assert_eq!(sessions_created(&vendor), 1);

    // Once the backoff is over, the replacement gets what is left of the cap.
    store
        .update_remote_execution(
            &agent.id,
            &anyagents_core::RemoteExecutionPatch {
                ended_at_ms: Some(anyagents_core::now_ms() - 3_600_000),
                ..Default::default()
            },
        )
        .unwrap();
    let mut executor = Executor::launch(config(&vendor, &store, &agent, 20)).unwrap();
    let creates: Vec<_> = vendor
        .requests("POST", "/v1/sessions")
        .into_iter()
        .filter(|r| r.path == "/v1/sessions")
        .collect();
    let second = &creates[1].body;
    assert_eq!(
        second["budget"]["max_list_cost"]["amount"], "13",
        "20 less the 7 spent"
    );
    let row = store.remote_execution(&agent.id).unwrap().unwrap();
    assert_eq!(row.spent_before_micros, 70_000);
    executor.deliver(&message("msg_2", "two")).unwrap();
    wait_until("a turn in the new session", || executor.turns_served() == 1);
    assert_eq!(
        store
            .remote_execution(&agent.id)
            .unwrap()
            .unwrap()
            .terminations,
        0,
        "a completed turn ends the run of terminations"
    );
    drop(executor);

    // The session's credential vanishes: the session is stopped, then
    // replaced, not left running beside a new one.
    let current = vendor
        .lock()
        .sessions
        .keys()
        .find(|id| **id != first)
        .cloned()
        .unwrap();
    vendor.forget_credentials(&current);
    let _executor = Executor::launch(config(&vendor, &store, &agent, 30)).unwrap();
    assert!(vendor.lock().sessions[&current].archived);
    assert_eq!(sessions_created(&vendor), 3);

    // Spent: no session starts, and nothing is asked of the vendor.
    drop(_executor);
    let before = vendor.lock().requests.len();
    store
        .update_remote_execution(
            &agent.id,
            &anyagents_core::RemoteExecutionPatch {
                status: Some(status::ENDED.into()),
                ..Default::default()
            },
        )
        .unwrap();
    let refused = Executor::launch(config(&vendor, &store, &agent, 14))
        .err()
        .unwrap();
    assert!(refused.contains("reached its spend cap"), "{refused}");
    assert_eq!(vendor.lock().requests.len(), before);
}

/// Review item 5: a create that fails halfway keeps what it made; the retry
/// reuses the vendor agent and environment.
#[test]
fn a_failed_create_keeps_the_vendor_agent_and_environment_for_the_retry() {
    let vendor = Vendor::start();
    let (_dir, store, agent) = store_with_agent("created");
    vendor.lock().fail_session_creates = 1;
    let failed = Executor::launch(config(&vendor, &store, &agent, 2500))
        .err()
        .unwrap();
    assert!(failed.contains("not created"), "{failed}");
    let state = store
        .remote_execution(&agent.id)
        .unwrap()
        .unwrap()
        .adapter_state;
    assert!(
        state["environment_id"].is_string() && state["agent_id"].is_string(),
        "{state}"
    );

    let _executor = Executor::launch(config(&vendor, &store, &agent, 2500)).unwrap();
    assert_eq!(vendor.requests("POST", "/v1/environments").len(), 1);
    assert_eq!(
        vendor
            .requests("POST", "/v1/agents")
            .iter()
            .filter(|r| r.path == "/v1/agents")
            .count(),
        1
    );
    assert_eq!(sessions_created(&vendor), 2);
    // The vault the failed create made was purged.
    assert_eq!(vendor.lock().archived_vaults.len(), 1);
}
