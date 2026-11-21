use super::super::bootstrap::consolidate_idle_agent_memory;
use super::super::messaging_conversation::ConversationQuery;
use super::super::*;
use super::helpers::{archetype_validation_state, env_lock};
use axum::Json;

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

    // A non-substantive turn still wakes the agent but never requires an
    // external sleep model or its cooldown to settle the backlog.
    store
        .receive(&agent.id, anyagents_core::Incoming::new("NO_REPLY"))
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
