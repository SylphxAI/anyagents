use super::super::agents_lifecycle::wake_agent_route;
use super::super::agents_roster::list_agents_sync_for_principal;
use super::super::messaging::{
    SendPlan, can_actor_message_agent, can_sender_message_agent, check_targets, media_upload,
    plan_send, send_key, send_sync,
};
use super::super::*;
use super::helpers::{
    archetype_validation_state, authored_send, env_lock, mark_explicitly_archived,
    try_authored_send,
};
use crate::api::SendRequest;
use crate::create;
use anyagents_core::{Agent, Incoming};
use axum::Json;
use axum::http::StatusCode;

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

/// An archived recipient is skipped with who answers for it. A send
/// addressed only to archived recipients fails without storing anything.
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

/// No send unarchives its recipient: by name or `agt_...` id, from an agent
/// or the operator, alone or with a lifecycle-looking body, it fails closed
/// with `409`, stores nothing and leaves the agent archived. Only an explicit
/// wake revives it, after which the same send is delivered; live agents
/// still receive and wake as before.
#[test]
fn sends_never_wake_archived_agents_and_only_an_explicit_wake_does() {
    let _env = env_lock();
    unsafe {
        std::env::set_var("ANYAGENTS_FAKE_MODE", "1");
    }
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let store: &Store = &state.store;
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("parent-agent", "opencode"))
        .unwrap();
    store.set_agent_state(&parent.id, "running").unwrap();
    let mut new = anyagents_core::NewAgent::new("worker", "opencode");
    new.parent_id = Some(parent.id.clone());
    let child = store.create_agent(new).unwrap();
    store
        .receive(&child.id, Incoming::new("queued before archive"))
        .unwrap();
    mark_explicitly_archived(store, dir.path(), &child);
    let agent = Actor::Agent(parent.name.clone());
    let operator = Actor::Operator("operator".into());
    let by_id = child.id.to_string();
    let attempts = [
        (&agent, "agent:worker", "follow-up"),
        (&agent, "worker", "follow-up"),
        (&agent, by_id.as_str(), "follow-up"),
        (
            &agent,
            "worker",
            "[Sub-Agent Completed] quoted in a follow-up",
        ),
        (&operator, "worker", "operator follow-up"),
        (&operator, by_id.as_str(), "operator follow-up"),
    ];
    for (index, (actor, to, text)) in attempts.into_iter().enumerate() {
        let key = format!("refused-{index}");
        let refused = try_authored_send(store, actor, to, text, &key).expect_err("archived");
        assert_eq!(refused.status, StatusCode::CONFLICT, "{to}");
        assert!(
            refused
                .message
                .contains("agent `worker` is archived; its parent is `parent-agent`")
                && refused.message.contains("agents(action=\"wake\")"),
            "{}",
            refused.message
        );
        assert_eq!(store.agent(&child.id).unwrap().state, "archived", "{to}");
        assert!(has_explicit_archive_marker(dir.path(), &child.id), "{to}");
        assert_eq!(store.inbox(&child.id, None).unwrap().len(), 1, "{to}");
        let receipt = send_key(&SendRequest {
            sender: Some(actor.as_address()),
            to: vec![to.to_string()],
            text: text.to_string(),
            idempotency_key: Some(key),
            ..Default::default()
        })
        .unwrap();
        assert!(store.send_receipt(&receipt).unwrap().is_none(), "{to}");
    }
    // A store record left `archived` without the marker is refused alike.
    let mut new = anyagents_core::NewAgent::new("legacy", "opencode");
    new.parent_id = Some(parent.id.clone());
    let legacy = store.create_agent(new).unwrap();
    store.set_agent_state(&legacy.id, "archived").unwrap();
    let refused =
        try_authored_send(store, &agent, "legacy", "hello", "legacy-1").expect_err("archived");
    assert_eq!(refused.status, StatusCode::CONFLICT);
    assert_eq!(store.agent(&legacy.id).unwrap().state, "archived");
    assert!(store.inbox(&legacy.id, None).unwrap().is_empty());

    // The explicit wake is the one way back; the send then goes through.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let Json(woken) = rt
        .block_on(wake_agent_route(
            Principal::Operator,
            State(state.clone()),
            AxumPath("worker".into()),
            axum::http::HeaderMap::new(),
        ))
        .unwrap_or_else(|e| panic!("{}", e.message));
    assert_eq!(woken.state, "running");
    assert!(!has_explicit_archive_marker(dir.path(), &child.id));
    let sent = authored_send(store, &agent, by_id.as_str(), "after wake", "woken-1");
    assert_eq!(sent.deliveries.len(), 1);
    assert_eq!(sent.deliveries[0].seq, 2);
    assert_eq!(store.agent(&child.id).unwrap().state, "running");

    // Idle and created agents still receive and wake.
    for (name, start) in [("idle-peer", Some("idle")), ("fresh-peer", None)] {
        let mut new = anyagents_core::NewAgent::new(name, "opencode");
        new.parent_id = Some(parent.id.clone());
        let peer = store.create_agent(new).unwrap();
        if let Some(start) = start {
            store.set_agent_state(&peer.id, start).unwrap();
        }
        let sent = authored_send(store, &agent, name, "live follow-up", name);
        assert_eq!(sent.deliveries.len(), 1, "{name}");
        assert_eq!(store.agent(&peer.id).unwrap().state, "running", "{name}");
        assert_eq!(store.inbox(&peer.id, None).unwrap().len(), 1, "{name}");
    }
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
        let response = authored_send(&store, &operator, to, "broadcast", to);
        assert!(response.deliveries.iter().all(|d| d.to != "agent:worker"));
        if to == "group:crew" {
            assert!(
                response
                    .skipped
                    .iter()
                    .any(|s| s.to == "agent:worker" && s.reason.contains("is archived"))
            );
        }
        assert_eq!(store.agent(&child.id).unwrap().state, "archived");
        assert!(store.inbox(&child.id, None).unwrap().is_empty());
    }
    // A marker alone must exclude a named member from fan-out too.
    store.set_agent_state(&child.id, "running").unwrap();
    let mixed = authored_send(&store, &operator, "group:crew", "no unread mail", "mixed");
    assert_eq!(mixed.deliveries.len(), 1);
    assert_eq!(mixed.skipped[0].to, "agent:worker");
    assert!(store.inbox(&child.id, None).unwrap().is_empty());
    store.set_agent_state(&child.id, "archived").unwrap();
    store.join_group("retired", &child.id).unwrap();
    let refused = try_authored_send(
        &store,
        &operator,
        "group:retired",
        "no unread mail",
        "retired",
    )
    .expect_err("named group has only archived members");
    assert_eq!(refused.status, StatusCode::CONFLICT);
    assert!(refused.message.contains("agent `worker` is archived"));
    assert!(refused.message.contains("agents(action=\"wake\")"));
    assert!(store.inbox(&child.id, None).unwrap().is_empty());
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
fn send_and_archive_serialize_across_store_connections() {
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
        let outcome = try_authored_send(
            &other,
            &Actor::Operator("operator".into()),
            "recipient",
            "follow-up",
            "interleave",
        );
        done_tx.send(outcome).unwrap();
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
    // The send admitted before the archive is refused under the lifecycle
    // lock: it neither stores mail nor flips the recipient back to running.
    let refused = done_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap()
        .expect_err("archived while the send waited");
    sender.join().unwrap();
    assert_eq!(refused.status, StatusCode::CONFLICT);
    assert!(
        refused.message.contains("is archived"),
        "{}",
        refused.message
    );
    assert!(store.inbox(&recipient.id, None).unwrap().is_empty());
    assert_eq!(store.agent(&recipient.id).unwrap().state, "archived");
    assert!(has_explicit_archive_marker(dir.path(), &recipient.id));
    store.set_agent_state(&recipient.id, "running").unwrap();
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
