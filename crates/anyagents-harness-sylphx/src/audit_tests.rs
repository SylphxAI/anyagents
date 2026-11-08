//! Behavioral boundary regressions. Fixtures invoke real IO/HTTP consumers.
use super::*;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};

fn tool(cfg: &Path, work: &Path, name: &str, args: Value) -> String {
    execute_sylphx_tool(
        name,
        &args,
        cfg,
        work,
        &Vec::new().into(),
        Path::new("unused-mcp"),
        &[],
    )
}
fn binding() -> PersistedCloudSession {
    PersistedCloudSession {
        agent_id: "agent-a".into(),
        session_id: "session-a".into(),
        model_id: "example/model".into(),
        env_slug: "orgs/acme/projects/editor/envs/test".into(),
        last_seq: 2,
    }
}
fn request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut byte = [0];
        stream.read_exact(&mut byte).unwrap();
        bytes.push(byte[0]);
        if bytes.ends_with(b"\r\n\r\n") {
            break bytes.len();
        }
        assert!(bytes.len() < 32_000);
    };
    let header = String::from_utf8(bytes.clone()).unwrap();
    let len = header
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|v| v.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    bytes.resize(header_end + len, 0);
    stream.read_exact(&mut bytes[header_end..]).unwrap();
    String::from_utf8(bytes).unwrap()
}
fn serve(replies: Vec<(u16, Value)>) -> (String, std::thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        replies.into_iter().map(|(status, body)| {
        let (mut stream, _) = listener.accept().unwrap();
        let req = request(&mut stream);
        let body = body.to_string();
        write!(stream, "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        req
    }).collect()
    });
    (base, handle)
}

#[test]
fn optional_files_distinguish_absence_corruption_and_io_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("state.json");
    assert!(read_json(&path).unwrap().is_none());
    write_atomic(&path, b"{\"state\":\"active\"}").unwrap();
    assert_eq!(read_json(&path).unwrap().unwrap()["state"], "active");
    for bad in ["{", "[]", "null"] {
        std::fs::write(&path, bad).unwrap();
        assert!(read_json(&path).unwrap_err().contains("state.json"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), bad);
    }
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(read_json(&path).unwrap_err().contains("reading"));
    assert!(
        write_atomic(&path, b"{}")
            .unwrap_err()
            .contains("persisting")
    );
    assert!(remove_optional(&path).unwrap_err().contains("removing"));
    std::fs::remove_dir(&path).unwrap();
    remove_optional(&path).unwrap();
}

#[test]
fn shell_never_substitutes_a_failed_workspace() {
    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path().join("work");
    let env = Vec::new().into();
    let valid = execute_bash_with_timeout("pwd", &work, &env, 1000);
    assert!(valid.contains(work.to_str().unwrap()), "{valid}");
    std::fs::write(tmp.path().join("blocked"), "sentinel").unwrap();
    let bad = execute_bash_with_timeout(
        "touch should-not-exist",
        &tmp.path().join("blocked/work"),
        &env,
        1000,
    );
    assert!(bad.contains("error creating requested workspace"), "{bad}");
    assert_eq!(
        std::fs::read_to_string(tmp.path().join("blocked")).unwrap(),
        "sentinel"
    );
    assert!(!tmp.path().join("should-not-exist").exists());
}

#[test]
fn settings_and_binding_refusals_preserve_selected_state() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = tmp.path();
    let local = vec![("SYLPHX_EXECUTION_MODE".into(), "local_only".into())];
    assert_eq!(
        sdk::SylphxExecutionMode::from_env(cfg, &local).unwrap(),
        sdk::SylphxExecutionMode::LocalOnly
    );
    for (file, raw) in [
        ("sylphx-agent.json", "{"),
        (
            "sylphx-workspace-placement.json",
            "{\"placement\":\"unknown\"}",
        ),
        ("sylphx-cloud-session.json", "{"),
    ] {
        std::fs::write(cfg.join(file), raw).unwrap();
        let result = if file.contains("cloud-session") {
            load_persisted_cloud_session(cfg).map(|_| ())
        } else {
            sdk::SylphxExecutionMode::from_env(cfg, &local).map(|_| ())
        };
        assert!(result.is_err(), "{file}");
        assert_eq!(std::fs::read_to_string(cfg.join(file)).unwrap(), raw);
        std::fs::remove_file(cfg.join(file)).unwrap();
    }
    save_persisted_cloud_session(cfg, &binding()).unwrap();
    assert_eq!(
        load_persisted_cloud_session(cfg).unwrap().unwrap().last_seq,
        2
    );
    let mut invalid = binding();
    invalid.agent_id = " ".into();
    assert!(save_persisted_cloud_session(cfg, &invalid).is_err());
    assert_eq!(
        load_persisted_cloud_session(cfg).unwrap().unwrap().agent_id,
        "agent-a"
    );
    std::fs::remove_file(cfg.join("sylphx-cloud-session.json")).unwrap();
    std::fs::create_dir(cfg.join("sylphx-cloud-session.json")).unwrap();
    assert!(save_persisted_cloud_session(cfg, &binding()).is_err());
    assert!(clear_persisted_cloud_session(cfg).is_err());
}

#[test]
fn environment_resources_are_explicit_not_aliases() {
    for valid in [
        "orgs/acme/projects/editor/envs/test",
        "organizations/acme/projects/editor/environments/test",
        "acme/editor/test",
        "environments/env-a",
    ] {
        assert_eq!(normalize_cloud_env_slug(valid).unwrap(), valid);
    }
    for bad in [
        "",
        "default",
        "prod",
        "sylphx",
        "orgs/a/projects/b/envs/..",
        "a/b/c?token=x",
    ] {
        assert!(normalize_cloud_env_slug(bad).is_err(), "{bad}");
    }
    let tmp = tempfile::tempdir().unwrap();
    let env = vec![
        ("SYLPHX_EXECUTION_MODE".into(), "cloud_only".into()),
        ("SYLPHX_AGENT_ENV".into(), "".into()),
    ];
    assert!(resolve_cloud_agent_environment(tmp.path(), &env).is_err());
    assert!(
        sdk::SylphxSdkSession::new(
            tmp.path(),
            tmp.path(),
            None,
            vec![("SYLPHX_EXECUTION_MODE".into(), "invalid".into())]
        )
        .is_err()
    );
}

#[test]
fn tool_schema_validation_precedes_mutation() {
    let tmp = tempfile::tempdir().unwrap();
    assert!(
        tool(
            tmp.path(),
            tmp.path(),
            "write_file",
            json!({"path":"valid", "content":"kept"})
        )
        .starts_with("wrote")
    );
    for args in [
        json!({"path":"invalid","content":42}),
        json!({"path":"invalid"}),
        json!([]),
    ] {
        assert!(tool(tmp.path(), tmp.path(), "write_file", args).starts_with("error:"));
        assert!(!tmp.path().join("invalid").exists());
    }
    assert_eq!(tool_arguments("{}").unwrap(), json!({}));
    for args in ["{", "[]", "null"] {
        assert!(tool_arguments(args).is_err());
    }
}

#[test]
fn transcript_validation_rejects_fabricated_tool_payloads_and_keeps_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("sylphx-transcript.jsonl");
    for valid in [
        json!({"type":"function_call","call_id":"a","name":"bash","arguments":"{}"}),
        json!({"type":"function_call_output","call_id":"a","output":""}),
        json!({"role":"tool","tool_call_id":"b","content":""}),
    ] {
        std::fs::write(&path, format!("{valid}\n")).unwrap();
        memory::validate_transcript(&path).unwrap();
    }
    for bad in [
        json!({"type":"function_call","call_id":"a","name":"bash"}),
        json!({"type":"function_call_output","call_id":"a"}),
        json!({"role":"tool","tool_call_id":"b"}),
        json!({"role":"assistant","tool_calls":[{"id":"b","function":{"name":"bash"}}]}),
    ] {
        let raw = format!("{bad}\n");
        std::fs::write(&path, &raw).unwrap();
        assert!(memory::validate_transcript(&path).is_err());
        assert!(execute_memory_recall(&path, &json!({})).starts_with("error:"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), raw);
    }
}

#[test]
fn transcript_append_and_context_edits_require_durable_effects() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("transcript");
    append_transcript_message(&path, &json!({"role":"user","content":"retained"})).unwrap();
    assert!(
        execute_memory_recall(&path, &json!({"range":"transcript:0-0"}))
            .contains("Already present")
    );
    assert!(std::fs::read_to_string(&path).unwrap().contains("retained"));
    for range in ["transcript:oops-0", "transcript:0-no", "transcript:1-0"] {
        assert!(execute_memory_recall(&path, &json!({"range":range})).starts_with("error:"));
    }
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(append_transcript_message(&path, &json!({"content":"must not disappear"})).is_err());
    assert!(execute_context_edit(&path, &json!({"drop":["0-0"]})).starts_with("error:"));
}

#[test]
fn goal_lifecycle_and_continuation_are_durable_or_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = tmp.path();
    let path = cfg.join("sylphx-goal.json");
    assert!(load_active_goal_continuation_prompt(cfg).unwrap().is_none());
    let created: Value = serde_json::from_str(&tool(
        cfg,
        cfg,
        "goal",
        json!({"action":"create","title":"deliver","acceptance_criteria":["done"]}),
    ))
    .unwrap();
    assert_eq!(created["state"], "active");
    for _ in 0..12 {
        assert!(load_active_goal_continuation_prompt(cfg).unwrap().is_some());
    }
    assert!(load_active_goal_continuation_prompt(cfg).unwrap().is_none());
    assert_eq!(read_json(&path).unwrap().unwrap()["auto_continuations"], 12);
    for (action, state) in [
        ("complete", "completed"),
        ("pause", "paused"),
        ("cancel", "cancelled"),
    ] {
        let output = tool(cfg, cfg, "goal", json!({"action":action}));
        assert!(output.contains(state), "{output}");
        assert_eq!(read_json(&path).unwrap().unwrap()["state"], state);
    }
    std::fs::write(&path, "{").unwrap();
    for action in ["status", "create", "update", "complete", "pause", "cancel"] {
        assert!(tool(cfg, cfg, "goal", json!({"action":action})).starts_with("error:"));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{");
    }
    assert!(load_active_goal_continuation_prompt(cfg).is_err());
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    for action in ["create", "update", "complete"] {
        assert!(tool(cfg, cfg, "goal", json!({"action":action})).starts_with("error:"));
    }
}

#[test]
fn context_sources_are_explicit_workspace_owned_files() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = tmp.path().join("cfg");
    let work = tmp.path().join("work");
    std::fs::create_dir(&cfg).unwrap();
    std::fs::create_dir(&work).unwrap();
    std::fs::write(work.join("STATE.md"), "not selected").unwrap();
    assert!(load_explicit_context(&cfg, &work).unwrap().is_empty());
    std::fs::write(work.join("notes.md"), "selected context").unwrap();
    std::fs::write(
        cfg.join("sylphx-agent.json"),
        json!({"context_sources":["notes.md"]}).to_string(),
    )
    .unwrap();
    assert!(
        load_explicit_context(&cfg, &work)
            .unwrap()
            .contains("selected context")
    );
    for sources in [
        json!(["../cfg/sylphx-agent.json"]),
        json!(["missing.md"]),
        json!([false]),
        json!("notes.md"),
    ] {
        std::fs::write(
            cfg.join("sylphx-agent.json"),
            json!({"context_sources":sources}).to_string(),
        )
        .unwrap();
        assert!(load_explicit_context(&cfg, &work).is_err());
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(cfg.join("sylphx-agent.json"), work.join("escape")).unwrap();
        std::fs::write(
            cfg.join("sylphx-agent.json"),
            json!({"context_sources":["escape"]}).to_string(),
        )
        .unwrap();
        assert!(
            load_explicit_context(&cfg, &work)
                .unwrap_err()
                .contains("owned")
        );
    }
}

#[test]
fn skill_activation_reports_existing_but_unreadable_instruction() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join(".agents/skills/example");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("SKILL.md"), "instruction body").unwrap();
    assert_eq!(
        tool(
            tmp.path(),
            tmp.path(),
            "activate_skill",
            json!({"name":"example"})
        ),
        "instruction body"
    );
    std::fs::write(dir.join("SKILL.md"), [0xff]).unwrap();
    assert!(
        tool(
            tmp.path(),
            tmp.path(),
            "activate_skill",
            json!({"name":"example"})
        )
        .starts_with("error reading")
    );
    assert!(
        tool(
            tmp.path(),
            tmp.path(),
            "activate_skill",
            json!({"name":"../escape"})
        )
        .starts_with("error:")
    );
}

#[test]
fn reply_fulfillment_requires_exact_successful_delivery_receipt() {
    let dest = "telegram:chat:-123:9";
    assert!(reply_delivered(
        &json!({"deliveries":[{"to":dest,"id":"message-a"}]}).to_string(),
        Some(dest)
    ));
    for bad in [
        json!({"to":dest}),
        json!({"deliveries":[{"to":dest,"id":""}]}),
        json!({"deliveries":[{"to":dest,"id":"a","error":"refused"}]}),
        json!({"deliveries":[{"to":"telegram:chat:-123:8","id":"a"}]}),
        json!({"deliveries":[{"to":"telegram:chat:-789:9","id":"a"}]}),
    ] {
        assert!(!reply_delivered(&bad.to_string(), Some(dest)));
    }
    assert!(!reply_delivered("error: refused", Some(dest)));
    assert!(!reply_delivered("{}", None));
}

#[test]
fn event_identity_is_never_fabricated() {
    for valid in [json!(1), json!("2")] {
        assert!(sequence(Some(&valid), false).unwrap() > 0);
    }
    assert_eq!(sequence(Some(&json!(0)), true).unwrap(), 0);
    for bad in [
        json!(0),
        json!(-1),
        json!("not-a-number"),
        json!(null),
        json!(1.5),
    ] {
        assert!(sequence(Some(&bad), false).is_err());
    }
    assert!(sequence(None, true).is_err());
}

#[test]
fn cloud_interrupt_requires_http_ack_and_settled_readback() {
    let (base, server) = serve(vec![(200, json!({})), (200, json!({"state":"idle"}))]);
    interrupt_and_settle(&base, "sessions/a", "fixture-key").unwrap();
    let reqs = server.join().unwrap();
    assert!(reqs[0].starts_with("POST /v1/sessions/a:interrupt "));
    assert!(reqs[1].starts_with("GET /v1/sessions/a "));
    let (base, server) = serve(vec![(503, json!({"error":"unavailable"}))]);
    assert!(
        interrupt_and_settle(&base, "sessions/a", "fixture-key")
            .unwrap_err()
            .contains("HTTP 503")
    );
    assert_eq!(server.join().unwrap().len(), 1);
    let (base, server) = serve(vec![(200, json!({})), (200, json!({}))]);
    assert!(interrupt_and_settle(&base, "sessions/a", "fixture-key").is_err());
    server.join().unwrap();
}

#[test]
fn cloud_tool_outbox_retains_failed_submission_and_reconciles_without_execution() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = tmp.path();
    save_persisted_cloud_session(cfg, &binding()).unwrap();
    let path = cfg.join("sylphx-cloud-tool-receipt.json");
    let receipt = json!({"session_path":cloud_session_resource_path(&binding().env_slug,&binding().session_id),"call_id":"call-a","event_sequence":3,"state":"completed","output":"effect already completed","error":false});
    std::fs::write(&path, receipt.to_string()).unwrap();
    let (base, server) = serve(vec![(503, json!({}))]);
    assert!(flush_cloud_tool_receipt(cfg, &base, "fixture-key").is_err());
    server.join().unwrap();
    assert_eq!(read_json(&path).unwrap().unwrap(), receipt);
    assert_eq!(
        load_persisted_cloud_session(cfg).unwrap().unwrap().last_seq,
        2
    );
    let (base, server) = serve(vec![(200, json!({}))]);
    flush_cloud_tool_receipt(cfg, &base, "fixture-key").unwrap();
    let reqs = server.join().unwrap();
    assert!(reqs[0].contains("effect already completed"));
    assert!(!path.exists());
    assert_eq!(
        load_persisted_cloud_session(cfg).unwrap().unwrap().last_seq,
        3
    );
    std::fs::write(&path, json!({"state":"executing"}).to_string()).unwrap();
    assert!(
        flush_cloud_tool_receipt(cfg, "http://127.0.0.1:1", "fixture-key")
            .unwrap_err()
            .contains("unresolved")
    );
    assert!(path.exists());
}

#[test]
fn cloud_pull_preserves_binding_on_failed_transfer_and_commits_exact_event_identity() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = tmp.path();
    save_persisted_cloud_session(cfg, &binding()).unwrap();
    let (base, server) = serve(vec![
        (200, json!({})),
        (200, json!({"state":"idle","last_event_sequence":3})),
        (503, json!({})),
    ]);
    let env = vec![
        ("SYLPHX_API_URL".into(), base),
        ("SYLPHX_API_KEY".into(), "fixture-key".into()),
    ];
    assert!(
        sdk::pull_session_from_cloud(cfg, &env)
            .unwrap_err()
            .contains("HTTP 503")
    );
    server.join().unwrap();
    assert!(load_persisted_cloud_session(cfg).unwrap().is_some());
    let (base, server) = serve(vec![
        (200, json!({})),
        (200, json!({"state":"idle","last_event_sequence":3})),
        (
            200,
            json!({"events":[{"sequence":3,"message":{"parts":[{"text":"retained remote text"}]}}]}),
        ),
    ]);
    let env = vec![
        ("SYLPHX_API_URL".into(), base),
        ("SYLPHX_API_KEY".into(), "fixture-key".into()),
    ];
    let receipt = sdk::pull_session_from_cloud(cfg, &env).unwrap();
    server.join().unwrap();
    assert_eq!(receipt.synced_events, 1);
    assert!(load_persisted_cloud_session(cfg).unwrap().is_none());
    let transcript = cfg.join("sylphx-transcript.jsonl");
    assert!(
        std::fs::read_to_string(&transcript)
            .unwrap()
            .contains("\"cloud_sequence\":3")
    );
    assert!(
        load_transcript_input_items(&transcript)[0]["content"]
            .as_str()
            .unwrap()
            .contains("retained remote text")
    );
    for (settled, events) in [
        (json!({"state":"idle"}), None),
        (
            json!({"state":"idle","last_event_sequence":4}),
            Some(json!({"events":[{"sequence":3}]})),
        ),
        (
            json!({"state":"idle","last_event_sequence":3}),
            Some(json!({"events":[{"sequence":"invalid"}]})),
        ),
    ] {
        save_persisted_cloud_session(cfg, &binding()).unwrap();
        let mut replies = vec![(200, json!({})), (200, settled)];
        if let Some(events) = events {
            replies.push((200, events));
        }
        let (base, server) = serve(replies);
        let env = vec![
            ("SYLPHX_API_URL".into(), base),
            ("SYLPHX_API_KEY".into(), "fixture-key".into()),
        ];
        assert!(sdk::pull_session_from_cloud(cfg, &env).is_err());
        server.join().unwrap();
        assert_eq!(
            load_persisted_cloud_session(cfg)
                .unwrap()
                .unwrap()
                .session_id,
            "session-a"
        );
    }
}

#[test]
fn selected_model_corruption_is_not_a_default_upstream() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = tmp.path();
    assert!(
        auth::resolve_upstream_endpoint(
            cfg,
            &[("SYLPHX_API_KEY".into(), "fixture-key".into())],
            Some("example/model")
        )
        .is_ok()
    );
    for raw in ["{", "{\"model\":42}", "{\"model\":\"\"}"] {
        std::fs::write(cfg.join("sylphx-agent.json"), raw).unwrap();
        assert!(auth::resolve_upstream_endpoint(cfg, &[], None).is_err());
        assert_eq!(
            std::fs::read_to_string(cfg.join("sylphx-agent.json")).unwrap(),
            raw
        );
    }
}

#[test]
fn terminal_frames_propagate_write_and_flush_failure() {
    let mut bytes = Vec::new();
    write_terminal_frame(&mut bytes, &json!({"type":"done"})).unwrap();
    assert_eq!(bytes, b"{\"type\":\"done\"}\n");
    struct Broken(bool);
    impl Write for Broken {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.0 {
                Err(std::io::Error::other("write sentinel"))
            } else {
                Ok(buf.len())
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::other("flush sentinel"))
        }
    }
    assert!(
        write_terminal_frame(&mut Broken(true), &json!({}))
            .unwrap_err()
            .to_string()
            .contains("write sentinel")
    );
    assert!(
        write_terminal_frame(&mut Broken(false), &json!({}))
            .unwrap_err()
            .to_string()
            .contains("flush sentinel")
    );
}

#[test]
fn native_dispatch_refuses_bad_call_identity_or_arguments_before_side_effects() {
    let valid = json!({"type":"function_call","call_id":"call-a","name":"write_file","arguments":"{\"path\":\"effect\",\"content\":\"kept\"}"});
    let mut missing_id = valid.clone();
    missing_id.as_object_mut().unwrap().remove("call_id");
    let mut bad_args = valid.clone();
    bad_args["arguments"] = json!("{");
    let mut wrong_type = valid.clone();
    wrong_type["arguments"] = json!("{\"path\":\"effect\",\"content\":false}");
    for (calls, expected) in [
        (vec![missing_id], Some("missing tool call id")),
        (vec![bad_args], Some("invalid tool arguments")),
        (vec![wrong_type], Some("invalid type")),
        (
            vec![valid.clone(), valid.clone()],
            Some("duplicate tool call id"),
        ),
        (vec![valid], None),
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        let body = json!({"output":calls,"usage":{"input_tokens":1,"output_tokens":1}});
        let mut replies = vec![(200, body)];
        if expected.is_none() {
            replies.push((200, json!({"output":[],"usage":{}})));
        }
        let (base, server) = serve(replies);
        std::fs::write(
            cfg.join("model-catalog-direct.json"),
            r#"{"data":[{"id":"boundary/model","context_length":128000}]}"#,
        )
        .unwrap();
        let env = vec![
            ("SYLPHX_GATEWAY_URL".into(), format!("{base}/v1")),
            ("SYLPHX_GATEWAY_KEY".into(), "fixture-key".into()),
        ]
        .into();
        let result = run_native_gateway_turn(
            &Arc::new(Mutex::new(SharedState::default())),
            &Arc::new(AtomicBool::new(false)),
            cfg,
            cfg,
            Some("boundary/model"),
            &env,
            Path::new("unused-mcp"),
            &[],
            "perform action",
        );
        server.join().unwrap();
        if let Some(expected) = expected {
            let error = result.unwrap_err();
            assert!(error.contains(expected), "{error}");
            assert!(!cfg.join("effect").exists());
            let path = cfg.join("sylphx-transcript.jsonl");
            assert!(
                !load_pending_native_messages(&path, &load_transcript_input_items(&path))
                    .is_empty()
            );
        } else {
            result.unwrap();
            assert_eq!(std::fs::read_to_string(cfg.join("effect")).unwrap(), "kept");
        }
    }
}

#[test]
fn rejected_cloud_cancel_and_steer_retain_inflight_state() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = tmp.path();
    save_persisted_cloud_session(cfg, &binding()).unwrap();
    let make = |base: String| SylphxHarness {
        config_dir: cfg.into(),
        work_dir: cfg.into(),
        model: None,
        env: vec![
            ("SYLPHX_API_URL".into(), base),
            ("SYLPHX_API_KEY".into(), "fixture-key".into()),
            ("SYLPHX_AGENT_ENV".into(), binding().env_slug),
            ("SYLPHX_EXECUTION_MODE".into(), "cloud_only".into()),
        ]
        .into(),
        mcp_command: "unused-mcp".into(),
        mcp_args: vec![],
        fake_mode: false,
        fake_log: None,
        cancel_flag: Arc::new(AtomicBool::new(false)),
        state: Arc::new(Mutex::new(SharedState {
            in_turn: true,
            cloud_session_id: Some("session-a".into()),
            steer_queue: VecDeque::from(["retained".into()]),
            ..SharedState::default()
        })),
    };
    let (base, server) = serve(vec![(503, json!({}))]);
    let mut harness = make(base);
    assert!(harness.cancel().is_err());
    server.join().unwrap();
    assert!(harness.state.lock().unwrap().in_turn);
    assert_eq!(harness.state.lock().unwrap().steer_queue.len(), 1);
    assert!(!harness.cancel_flag.load(Ordering::SeqCst));
    assert!(load_persisted_cloud_session(cfg).unwrap().is_some());
    let (base, server) = serve(vec![(503, json!({}))]);
    let mut harness = make(base);
    let message = OutboundMessage {
        id: "message-a".into(),
        seq: 1,
        text: "retained inbound".into(),
        sender: Some("agent:worker-a".into()),
        reply_to: None,
        source_time_ms: None,
        context: Default::default(),
    };
    assert!(harness.deliver(&message).is_err());
    server.join().unwrap();
    assert!(harness.state.lock().unwrap().in_turn);
    let (base, server) = serve(vec![(200, json!({})), (200, json!({"state":"idle"}))]);
    let mut harness = make(base);
    harness.cancel().unwrap();
    server.join().unwrap();
    assert!(!harness.state.lock().unwrap().in_turn);
    assert!(harness.cancel_flag.load(Ordering::SeqCst));
}

#[test]
fn instruction_layering_depends_on_scope_not_document_heading() {
    for heading in [
        "# sample/project",
        "# Organization Operations",
        "# Any unrelated heading",
    ] {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("data");
        let cfg = data.join("agents/test/config");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir(&work).unwrap();
        std::fs::write(data.join("AGENTS.md"), format!("{heading}\nshared rule")).unwrap();
        std::fs::write(work.join("AGENTS.md"), format!("{heading}\nworkspace rule")).unwrap();
        let prompt = assemble_system_prompt(&cfg, &work);
        assert!(prompt.find("shared rule").unwrap() < prompt.find("workspace rule").unwrap());
    }
}

#[test]
fn unknown_cloud_mutation_effect_is_not_replayed_by_transport_fallback() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let req = request(&mut stream);
        assert!(req.contains("effect-a"));
        drop(stream);
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        listener.set_nonblocking(true).unwrap();
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    });
    let error = auth::post_cloud_json(
        &format!("{base}/v1/session:send"),
        Some("fixture-key"),
        &json!({"message":"effect-a"}),
    )
    .unwrap_err();
    assert!(error.contains("outcome unresolved"), "{error}");
    tx.send(()).unwrap();
    server.join().unwrap();
}

#[test]
fn retained_cloud_submission_refuses_a_second_dispatch_without_losing_input() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = tmp.path();
    let pending = cfg.join("sylphx-cloud-turn-pending.json");
    let receipt =
        json!({"session_path":"sessions/a","message":"retained input","state":"submitting"});
    std::fs::write(&pending, receipt.to_string()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let env = vec![
        (
            "SYLPHX_API_URL".into(),
            format!("http://{}", listener.local_addr().unwrap()),
        ),
        ("SYLPHX_API_KEY".into(), "fixture-key".into()),
    ]
    .into();
    let error = run_cloud_session_turn(
        &Arc::new(Mutex::new(SharedState::default())),
        &Arc::new(AtomicBool::new(false)),
        cfg,
        cfg,
        Some("boundary/model"),
        &env,
        Path::new("unused-mcp"),
        &[],
        &binding().env_slug,
        "retained input",
    )
    .unwrap_err();
    assert!(error.contains("unresolved retained submission"), "{error}");
    assert_eq!(read_json(&pending).unwrap().unwrap(), receipt);
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn teleport_requires_durable_instruction_revision_and_retains_created_binding_on_failure() {
    for blocked in [false, true] {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path();
        let fp = cfg.join(".sylphx-cloud-instructions-fingerprint");
        if blocked {
            std::fs::create_dir(&fp).unwrap();
        }
        let (base, server) = serve(vec![
            (200, json!({"name":"agent-a"})),
            (200, json!({"name":"session-a"})),
        ]);
        let env = vec![
            ("SYLPHX_API_URL".into(), base),
            ("SYLPHX_API_KEY".into(), "fixture-key".into()),
        ];
        let result = sdk::teleport_session_to_cloud(
            cfg,
            cfg,
            Some("example/model"),
            &env,
            Some(&binding().env_slug),
        );
        let requests = server.join().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            load_persisted_cloud_session(cfg)
                .unwrap()
                .unwrap()
                .session_id,
            "session-a"
        );
        if blocked {
            assert!(result.unwrap_err().contains("fingerprint"));
            assert!(fp.is_dir());
        } else {
            assert_eq!(result.unwrap().session_id.as_deref(), Some("session-a"));
            assert!(!std::fs::read_to_string(&fp).unwrap().trim().is_empty());
        }
    }
}
