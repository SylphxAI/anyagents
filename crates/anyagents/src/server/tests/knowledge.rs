use super::super::*;
use anyagents_core::Agent;
use axum::Json;
use axum::http::StatusCode;

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
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
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
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
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
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
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
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
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
