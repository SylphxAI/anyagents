use super::super::*;

fn sleep_test_env_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

struct ScopedSleepModelEnv {
    _guard: std::sync::MutexGuard<'static, ()>,
}

impl ScopedSleepModelEnv {
    fn new(url: Option<&str>) -> Self {
        let guard = sleep_test_env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        unsafe {
            match url {
                Some(u) => std::env::set_var("ANYAGENTS_SLEEP_MODEL_URL", u),
                None => std::env::remove_var("ANYAGENTS_SLEEP_MODEL_URL"),
            }
            std::env::remove_var("ANYAGENTS_SLEEP_CONSOLIDATION_FORCE");
        }
        Self { _guard: guard }
    }
}

impl Drop for ScopedSleepModelEnv {
    fn drop(&mut self) {
        unsafe {
            std::env::remove_var("ANYAGENTS_SLEEP_MODEL_URL");
            std::env::remove_var("ANYAGENTS_SLEEP_CONSOLIDATION_FORCE");
        }
    }
}

fn spawn_mock_sleep_model_server(
    plan_json: String,
) -> (
    String,
    std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    std::thread::JoinHandle<()>,
) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let req_clone = std::sync::Arc::clone(&requests);
    let handle = std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(5)));
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            loop {
                match stream.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => {
                        buf.extend_from_slice(&chunk[..n]);
                        let s = String::from_utf8_lossy(&buf);
                        if let Some((hdrs, body)) = s.split_once("\r\n\r\n") {
                            let content_len = hdrs
                                .lines()
                                .find_map(|l| {
                                    let (k, v) = l.split_once(':')?;
                                    if k.trim().eq_ignore_ascii_case("content-length") {
                                        v.trim().parse::<usize>().ok()
                                    } else {
                                        None
                                    }
                                })
                                .unwrap_or(0);
                            if body.len() >= content_len {
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
            req_clone
                .lock()
                .unwrap()
                .push(String::from_utf8_lossy(&buf).into_owned());
            let resp_body = serde_json::json!({
                "choices": [
                    {
                        "message": {
                            "role": "assistant",
                            "content": plan_json
                        }
                    }
                ]
            })
            .to_string();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                resp_body.len(),
                resp_body
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        }
    });
    (
        format!("http://{addr}/v1/chat/completions"),
        requests,
        handle,
    )
}

#[test]
fn sleep_memory_maintenance_rebuilds_index_for_disk_edits_without_hardcoded_topics() {
    let _env = ScopedSleepModelEnv::new(None);
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("sleep-neutral", "spiron"))
        .unwrap();
    let mut msg = Incoming::new("Directive message body that exceeds thirty characters easily");
    msg.sender = Some("operator".into());
    store.receive(&agent.id, msg).unwrap();

    let root = dir.path().join("agents").join(agent.id.as_str());
    std::fs::create_dir_all(root.join("memory")).unwrap();
    std::fs::write(
        root.join("memory/custom-topic.md"),
        "# Custom Topic\n\nAgent-authored invariant fact.\n",
    )
    .unwrap();

    let modified = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
    assert!(modified);
    let mem_md = std::fs::read_to_string(root.join("memory/MEMORY.md")).unwrap();
    assert!(mem_md.contains("custom-topic"));
    let topics = crate::learned_memory::list_memory_topics(&root).unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].slug, "custom-topic");
    let sleep = crate::learned_memory::read_sleep_state(&root);
    assert_eq!(sleep.last_inbound_seq, 1);
    assert_eq!(sleep.consolidated_topics, 1);
}

#[test]
fn sleep_model_selector_matrix_reaches_chat_completions_without_route_or_fragment() {
    use crate::learned_memory::{SleepModelEndpoint, request_sleep_consolidation_plan};
    let root = crate::test_tempdir();
    let mut cases = Vec::new();
    for base in [
        "gpt-6.2-sol",
        "gpt-6.2-astra",
        "gpt-6.1-sol",
        "unknown-model",
    ] {
        for prefix in ["openai/", "sylphx/openai/"] {
            for effort in ["low", "medium", "high", "xhigh", "max"] {
                cases.push((
                    format!("{prefix}{base}#{effort}"),
                    format!("openai/{base}"),
                    Some(effort),
                ));
            }
        }
    }
    for opaque in [
        "vendor/custom#future",
        "sylphx/vendor/custom#high",
        "openai/custom#future",
        "openai/gpt-6.1-sol",
    ] {
        cases.push((opaque.into(), opaque.into(), None));
    }
    for (selector, model, effort) in cases {
        let (url, requests, handle) = spawn_mock_sleep_model_server(r#"{"operations":[]}"#.into());
        let endpoint = SleepModelEndpoint {
            url,
            bearer_key: None,
            model: selector.clone(),
        };
        request_sleep_consolidation_plan(&endpoint, root.path(), "worker", None, &[], &[]).unwrap();
        handle.join().unwrap();
        let captured = requests.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert!(captured[0].starts_with("POST /v1/chat/completions HTTP/1.1\r\n"));
        let (_, payload) = captured[0].split_once("\r\n\r\n").unwrap();
        let body: serde_json::Value = serde_json::from_str(payload).unwrap();
        assert_eq!(body["model"].as_str(), Some(model.as_str()), "{selector}");
        assert_eq!(
            body.get("reasoning_effort").and_then(|v| v.as_str()),
            effort
        );
        assert_eq!(body["messages"].as_array().unwrap().len(), 2);
        assert_eq!(
            endpoint.model, selector,
            "wire conversion must not rewrite the stored selector"
        );
    }
    let (url, _, handle) = spawn_mock_sleep_model_server("not a consolidation plan".into());
    let endpoint = SleepModelEndpoint {
        url,
        bearer_key: None,
        model: "sylphx/openai/gpt-6.2-sol#max".into(),
    };
    assert!(
        request_sleep_consolidation_plan(&endpoint, root.path(), "worker", None, &[], &[]).is_err()
    );
    handle.join().unwrap();
}

#[test]
fn sleep_consolidation_strips_quotes_and_rejects_noise() {
    let (url, requests, handle) = spawn_mock_sleep_model_server(r#"{"operations":[]}"#.to_string());
    let _env = ScopedSleepModelEnv::new(Some(&url));
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("sleep-noise", "spiron"))
        .unwrap();
    let root = dir.path().join("agents").join(agent.id.as_str());
    std::fs::create_dir_all(root.join("memory")).unwrap();

    let mut quoted_msg = Incoming::new("OK, checking now");
    quoted_msg.sender = Some("user:alex".into());
    quoted_msg.quote = Some(crate::quote::Quote {
        author: "Alex".into(),
        text: "quoted_echo_secret_should_be_stripped".into(),
        ..Default::default()
    });
    store.receive(&agent.id, quoted_msg).unwrap();

    let mut no_reply = Incoming::new("NO_REPLY");
    no_reply.sender = Some("agent:peer".into());
    store.receive(&agent.id, no_reply).unwrap();

    let modified = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
    handle.join().unwrap();
    assert!(!modified, "noise rejection should not create topic files");

    let captured = requests.lock().unwrap();
    assert_eq!(captured.len(), 1);
    assert!(captured[0].contains("OK, checking now"));
    assert!(
        !captured[0].contains("quoted_echo_secret_should_be_stripped"),
        "quoted block must be stripped before LLM consolidation"
    );
    assert!(
        !captured[0].contains("NO_REPLY"),
        "NO_REPLY turns must be filtered out"
    );

    let topics = crate::learned_memory::list_memory_topics(&root).unwrap();
    assert!(topics.is_empty());
    let sleep = crate::learned_memory::read_sleep_state(&root);
    assert_eq!(sleep.last_inbound_seq, 2);
    assert_eq!(sleep.consolidated_topics, 0);
}

#[test]
fn sleep_consolidation_upserts_existing_topic_in_place_and_resolves_conflicts() {
    let plan = serde_json::json!({
        "operations": [
            {
                "op": "upsert",
                "topic": "service-endpoint",
                "description": "Primary API endpoint migrated to port 9090 (replaced 8080)",
                "content": "# Service Endpoint\n\nPrimary API endpoint runs on port 9090 in production."
            }
        ]
    })
    .to_string();
    let (url, requests, handle) = spawn_mock_sleep_model_server(plan);
    let _env = ScopedSleepModelEnv::new(Some(&url));
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("sleep-upsert", "spiron"))
        .unwrap();
    let root = dir.path().join("agents").join(agent.id.as_str());
    crate::learned_memory::upsert_memory_topic(
        &root,
        &agent.name,
        None,
        "service-endpoint",
        Some("Primary API endpoint on port 8080"),
        "# Service Endpoint\n\nPrimary API endpoint runs on port 8080.",
        1700000000000,
    )
    .unwrap();

    let body = "We migrated the service-endpoint port from 8080 to 9090.";
    let mut msg = Incoming::new(body);
    msg.sender = Some("operator".into());
    let ReceiveOutcome::New(received) = store.receive(&agent.id, msg).unwrap() else {
        panic!("a new message");
    };

    let modified = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
    handle.join().unwrap();
    assert!(modified);

    let captured = requests.lock().unwrap();
    assert_eq!(captured.len(), 1);
    assert!(
        captured[0].contains("Primary API endpoint runs on port 8080."),
        "existing topic body must be included in consolidation prompt"
    );

    let topics = crate::learned_memory::list_memory_topics(&root).unwrap();
    assert_eq!(
        topics.len(),
        1,
        "must update topic in place without duplicating"
    );
    assert_eq!(topics[0].slug, "service-endpoint");
    assert_eq!(
        topics[0].description,
        "Primary API endpoint migrated to port 9090 (replaced 8080)"
    );
    assert!(topics[0].updated_at_ms > 1700000000000);

    let on_disk = std::fs::read_to_string(root.join("memory/service-endpoint.md")).unwrap();
    assert!(on_disk.contains("created_at_ms: 1700000000000"));
    assert!(on_disk.contains("port 9090 in production"));
    assert!(!on_disk.contains("runs on port 8080"));
    // The turns were numbered for the model, and the written topic cites
    // the immutable raw turn it was distilled from.
    assert!(captured[0].contains("- #0 [ts="), "{}", captured[0]);
    let (_, cited) = crate::learned_memory::split_topic_sources(&on_disk);
    assert_eq!(
        cited,
        vec![crate::learned_memory::RawCitation {
            raw_id: received.id.to_string(),
            raw_sha256: crate::learned_memory::sha256(body.as_bytes()),
            offset: 0,
            length: body.len() as u64,
            seq: received.seq,
            direction: "in".into(),
        }]
    );
    // Nothing was destroyed: the replaced version is archived history,
    // and the raw turn is byte-for-byte what it was.
    let archived: Vec<_> = std::fs::read_dir(root.join("history/archive/memory"))
        .unwrap()
        .map(|e| std::fs::read_to_string(e.unwrap().path()).unwrap())
        .collect();
    assert_eq!(archived.len(), 1);
    assert!(archived[0].contains("Primary API endpoint runs on port 8080."));
    assert_eq!(
        store
            .message(&agent.id, received.id.as_str())
            .unwrap()
            .unwrap()
            .text,
        body
    );
}

#[test]
fn sleep_consolidation_defragments_duplicate_topics_and_syncs_fts5() {
    let plan = serde_json::json!({
        "operations": [
            {
                "op": "upsert",
                "topic": "cache-ttl",
                "description": "Unified cache TTL policy (300s L1, 3600s L2)",
                "content": "# Unified Cache TTL\n\nCanonical merged token unifiedcachetoken."
            },
            {
                "op": "delete",
                "topic": "cache-ttl-v1"
            },
            {
                "op": "delete",
                "topic": "cache-ttl-v2"
            }
        ]
    })
    .to_string();
    let (url, _requests, handle) = spawn_mock_sleep_model_server(plan);
    let _env = ScopedSleepModelEnv::new(Some(&url));
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("sleep-defrag", "spiron"))
        .unwrap();
    let root = dir.path().join("agents").join(agent.id.as_str());
    crate::learned_memory::upsert_memory_topic(
        &root,
        &agent.name,
        None,
        "cache-ttl-v1",
        Some("Legacy L1 cache TTL"),
        "Fragment one oldfragmenttoken",
        1700000000000,
    )
    .unwrap();
    crate::learned_memory::upsert_memory_topic(
        &root,
        &agent.name,
        None,
        "cache-ttl-v2",
        Some("Legacy L2 cache TTL"),
        "Fragment two secondfragmenttoken",
        1700000001000,
    )
    .unwrap();
    store.sync_agent_memory_index(&agent).unwrap();

    let mut msg = Incoming::new("Consolidate cache TTL notes into a single topic.");
    msg.sender = Some("operator".into());
    store.receive(&agent.id, msg).unwrap();

    let modified = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
    handle.join().unwrap();
    assert!(modified);

    let topics = crate::learned_memory::list_memory_topics(&root).unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].slug, "cache-ttl");
    let mem_md = std::fs::read_to_string(root.join("memory/MEMORY.md")).unwrap();
    assert!(mem_md.contains("cache-ttl"));
    assert!(!mem_md.contains("cache-ttl-v1"));
    assert!(!mem_md.contains("cache-ttl-v2"));

    let ids = [agent.id.to_string()];
    let merged_hits = store
        .search_host_files(
            &ids,
            crate::host_index::MemoryScope::Memory,
            &HistorySearchQuery {
                query: Some("unifiedcachetoken".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(merged_hits.len(), 1);
    let old_hits = store
        .search_host_files(
            &ids,
            crate::host_index::MemoryScope::Memory,
            &HistorySearchQuery {
                query: Some("oldfragmenttoken".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert!(
        old_hits.is_empty(),
        "deleted fragmented topic must be evicted from HostIndex FTS5"
    );
    // Merged-away topics are retired to the history archive, not
    // destroyed: still searchable as history, never pinned memory.
    assert!(!root.join("memory/cache-ttl-v1.md").exists());
    let archived = std::fs::read_dir(root.join("history/archive/memory"))
        .unwrap()
        .count();
    assert_eq!(archived, 2);
    let history_hits = store
        .search_host_files(
            &ids,
            crate::host_index::MemoryScope::Transcripts,
            &HistorySearchQuery {
                query: Some("oldfragmenttoken".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(history_hits.len(), 1);
}

#[test]
fn sleep_consolidation_defers_turns_during_cooldown_without_data_loss() {
    let plan = serde_json::json!({
        "operations": [
            {
                "op": "upsert",
                "topic": "deferred-fact",
                "description": "Fact consolidated after cooldown expiry",
                "content": "Durable fact body after cooldown."
            }
        ]
    })
    .to_string();
    let (url, requests, handle) = spawn_mock_sleep_model_server(plan);
    let _env = ScopedSleepModelEnv::new(Some(&url));
    let dir = crate::test_tempdir();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store
        .create_agent(NewAgent::new("sleep-cooldown", "spiron"))
        .unwrap();
    let root = dir.path().join("agents").join(agent.id.as_str());
    std::fs::create_dir_all(root.join("memory")).unwrap();

    // Simulate a consolidation that just ran at `now_ms()`.
    let now = crate::now_ms();
    crate::learned_memory::write_sleep_state(
        &root,
        &crate::learned_memory::SleepState {
            last_consolidated_ms: now,
            last_inbound_seq: 0,
            last_outbound_ms: 0,
            consolidated_topics: 0,
        },
    )
    .unwrap();

    let mut msg = Incoming::new("Durable fact arriving during the 10-minute cooldown window.");
    msg.sender = Some("operator".into());
    store.receive(&agent.id, msg).unwrap();

    // First call during cooldown: must defer LLM call and preserve last_inbound_seq = 0.
    let modified_in_cooldown = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
    assert!(!modified_in_cooldown);
    assert!(requests.lock().unwrap().is_empty());
    let sleep_during = crate::learned_memory::read_sleep_state(&root);
    assert_eq!(
        sleep_during.last_inbound_seq, 0,
        "must not advance watermark while deferred by cooldown"
    );

    // Expire the 10-minute cooldown and run again: now it consolidates and advances watermark.
    crate::learned_memory::write_sleep_state(
        &root,
        &crate::learned_memory::SleepState {
            last_consolidated_ms: now - 605_000,
            last_inbound_seq: 0,
            last_outbound_ms: 0,
            consolidated_topics: 0,
        },
    )
    .unwrap();
    let modified_after = store.consolidate_agent_sleep_memory(&agent.id).unwrap();
    handle.join().unwrap();
    assert!(modified_after);
    assert_eq!(requests.lock().unwrap().len(), 1);
    let sleep_after = crate::learned_memory::read_sleep_state(&root);
    assert_eq!(sleep_after.last_inbound_seq, 1);
    assert_eq!(sleep_after.consolidated_topics, 1);
}
