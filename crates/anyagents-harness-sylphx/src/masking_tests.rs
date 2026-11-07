//! Exercise both native tool-batch paths through actual reads and HTTP dispatch.
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::{SharedState, execute_sylphx_tool, run_cloud_session_turn, run_native_gateway_turn};

fn next_http_request(listener: &TcpListener) -> (TcpStream, String, Value) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut stream = loop {
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(Instant::now() < deadline, "no native HTTP request arrived");
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(err) => panic!("loopback accept: {err}"),
        }
    };
    // macOS inherits O_NONBLOCK from the listener on accept.
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut bytes = Vec::new();
    let mut buf = [0; 4096];
    let (header_end, length, route) = loop {
        let n = stream.read(&mut buf).unwrap();
        assert_ne!(n, 0, "request ended before its headers");
        bytes.extend_from_slice(&buf[..n]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
            let route = headers.lines().next().unwrap().to_string();
            let length: usize = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length:")?.trim().parse().ok())
                .unwrap_or(0);
            break (end + 4, length, route);
        }
    };
    while bytes.len() < header_end + length {
        let n = stream.read(&mut buf).unwrap();
        assert_ne!(n, 0, "request body was truncated");
        bytes.extend_from_slice(&buf[..n]);
    }
    let value = if length == 0 {
        Value::Null
    } else {
        serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
    };
    (stream, route, value)
}

fn next_request(listener: &TcpListener) -> (TcpStream, Value) {
    let (stream, route, value) = next_http_request(listener);
    assert!(route.starts_with("post /v1/responses "));
    (stream, value)
}

fn respond(mut stream: TcpStream, body: Value) {
    let body = body.to_string();
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    stream.flush().unwrap();
}

#[test]
fn sequential_and_parallel_tool_results_are_masked_before_next_http_and_disk() {
    for count in [1, 2] {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("cfg");
        let work = tmp.path().join("work");
        std::fs::create_dir_all(&cfg).unwrap();
        std::fs::create_dir_all(&work).unwrap();
        std::fs::write(
            cfg.join("model-catalog-direct.json"),
            r#"{"data":[{"id":"mask-http/model","context_length":128000,"limits":{"max_output_tokens":2000}}]}"#,
        )
        .unwrap();
        for index in 0..count {
            std::fs::write(
                work.join(format!("result-{index}.txt")),
                "password=fixturecanarynativehttp keep\nnovel actionable diagnostic 測試🙂\n",
            )
            .unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let gateway = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (stream, _) = next_request(&listener);
            let calls: Vec<Value> = (0..count)
                .map(|index| {
                    json!({"type":"function_call", "call_id":format!("call_{index}"), "name":"read_file", "arguments":json!({"path":format!("result-{index}.txt")}).to_string()})
                })
                .collect();
            respond(stream, json!({"output":calls}));
            let (stream, request) = next_request(&listener);
            let raw = request.to_string();
            assert!(!raw.contains("fixturecanarynativehttp"));
            let results: Vec<_> = request["input"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|item| item["type"] == "function_call_output")
                .collect();
            assert_eq!(results.len(), count);
            for (index, item) in results.iter().enumerate() {
                assert_eq!(item["call_id"], format!("call_{index}"));
                let text = item["output"].as_str().unwrap();
                assert!(text.contains("password=[redacted] keep"), "{text}");
                assert!(text.contains("novel actionable diagnostic 測試🙂"));
            }
            respond(stream, json!({"output":[]}));
        });
        let state = Arc::new(Mutex::new(SharedState::default()));
        let cancel = Arc::new(AtomicBool::new(false));
        let env = vec![
            ("SYLPHX_GATEWAY_URL".into(), gateway),
            ("SYLPHX_GATEWAY_KEY".into(), "fixture-key".into()),
        ];
        let result = run_native_gateway_turn(
            &state,
            &cancel,
            &cfg,
            &work,
            Some("mask-http/model"),
            &env.clone().into(),
            Path::new("unused-mcp"),
            &[],
            "inspect the synthetic result files",
        );
        server.join().unwrap();
        result.unwrap();
        let raw = std::fs::read_to_string(cfg.join("sylphx-transcript.jsonl")).unwrap();
        assert!(!raw.contains("fixturecanarynativehttp"));
        let items: Vec<Value> = raw
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let outputs: Vec<_> = items
            .iter()
            .filter(|item| item["type"] == "function_call_output")
            .collect();
        assert_eq!(outputs.len(), count);
        assert!(outputs.iter().all(|item| {
            let text = item["output"].as_str().unwrap();
            text.contains("password=[redacted] keep")
                && text.contains("novel actionable diagnostic 測試🙂")
        }));
    }
}

#[test]
fn complete_tool_output_is_masked_before_bash_read_file_and_mcp_truncation() {
    let tmp = tempfile::tempdir().unwrap();
    let canary = "straddlingcanary".repeat(8_000);
    let raw = format!(
        "HEADER diagnostic\n{}password={canary}\nFOOTER actionable diagnostic 測試🙂\n",
        "safe filler\n".repeat(1_300)
    );
    std::fs::write(tmp.path().join("large.txt"), &raw).unwrap();
    let mcp_script = r#"import json,sys
for line in sys.stdin:
    request=json.loads(line)
    if request.get('id') == 0:
        print(json.dumps({'jsonrpc':'2.0','id':0,'result':{}}),flush=True)
    elif request.get('id') == 1:
        print(json.dumps({'jsonrpc':'2.0','id':1,'result':{'content':[{'type':'text','text':open(sys.argv[1]).read()}]}}),flush=True)
        break
"#;
    for (name, args, command, command_args) in [
        ("bash", json!({"command":"cat large.txt"}), "unused", vec![]),
        ("read_file", json!({"path":"large.txt"}), "unused", vec![]),
        (
            "browser",
            json!({"action":"snapshot"}),
            "python3",
            vec![
                "-c".into(),
                mcp_script.into(),
                tmp.path().join("large.txt").display().to_string(),
            ],
        ),
    ] {
        let output = execute_sylphx_tool(
            name,
            &args,
            tmp.path(),
            tmp.path(),
            &super::workspace::Environment::default(),
            Path::new(command),
            &command_args,
        );
        assert!(
            !output.contains("straddlingcanary"),
            "{name} leaked a credential fragment after truncation"
        );
        assert!(
            output.contains("HEADER diagnostic"),
            "{name}: missing header"
        );
        assert!(
            output.contains("FOOTER actionable diagnostic 測試🙂"),
            "{name}: missing footer"
        );
    }
}

#[test]
fn masked_results_keep_observation_limits_and_unrecognized_diagnostics() {
    let tmp = tempfile::tempdir().unwrap();
    // Still over every old observation limit after masking: retain useful head
    // and tail rather than avoiding truncation by replacing the entire output.
    let raw = format!(
        "HEADER diagnostic\n{}\npassword=budgetcanary\nFOOTER actionable diagnostic 測試🙂\n",
        "ordinary novel diagnostic ".repeat(4_000)
    );
    std::fs::write(tmp.path().join("large.txt"), raw).unwrap();
    for (name, args, maximum) in [
        ("bash", json!({"command":"cat large.txt"}), 30_000),
        ("read_file", json!({"path":"large.txt"}), 64_000),
    ] {
        let output = execute_sylphx_tool(
            name,
            &args,
            tmp.path(),
            tmp.path(),
            &super::workspace::Environment::default(),
            Path::new("unused"),
            &[],
        );
        assert!(output.len() <= maximum, "{name}: observation cap lost");
        assert!(output.contains("elided"), "{name}: truncation did not run");
        assert!(output.contains("HEADER diagnostic"));
        assert!(output.contains("ordinary novel diagnostic"));
        assert!(output.contains("password=[redacted]"));
        assert!(output.contains("FOOTER actionable diagnostic 測試🙂"));
        assert!(!output.contains("budgetcanary"));
    }
}

#[test]
fn cloud_bridge_submits_only_the_masked_tool_result() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = tmp.path().join("cfg");
    let work = tmp.path().join("work");
    std::fs::create_dir_all(&cfg).unwrap();
    let skill = work.join("skills/masking-fixture");
    std::fs::create_dir_all(&skill).unwrap();
    std::fs::write(
        skill.join("SKILL.md"),
        "password=cloudbridgecanary\nnovel cloud diagnostic 測試🙂",
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let gateway = format!("http://{}", listener.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (stream, route, _) = next_http_request(&listener);
        assert!(
            route.starts_with("post /v1/environments/masking/agent_sessions "),
            "{route}"
        );
        respond(
            stream,
            json!({"name":"environments/masking/agent_sessions/s"}),
        );
        let (mut stream, route, _) = next_http_request(&listener);
        assert!(route.contains("/agents:capture-credential "), "{route}");
        write!(
            stream,
            "HTTP/1.1 404 Not Found\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
        )
        .unwrap();
        drop(stream);
        let (stream, route, _) = next_http_request(&listener);
        assert!(route.contains(":send "), "{route}");
        respond(stream, json!({}));
        let (stream, route, _) = next_http_request(&listener);
        assert!(route.contains("/session_events "), "{route}");
        respond(
            stream,
            json!({"session_events":[{"sequence":1,"turn":1,"type":"TOOL_CALL","tool_call":{"id":"cloud_call","tool":"activate_skill","input":{"name":"masking-fixture"}}}]}),
        );
        let (stream, route, body) = next_http_request(&listener);
        assert!(route.contains(":submittoolresult "), "{route}");
        // Always answer before asserting, so a failing regression cannot hang the client.
        respond(stream, json!({}));
        let (stream, route, _) = next_http_request(&listener);
        assert!(
            route.starts_with("get /v1/environments/masking/agent_sessions/s "),
            "{route}"
        );
        respond(stream, json!({"state":"completed","last_event_sequence":1}));
        assert_eq!(body["call_id"], "cloud_call");
        assert_eq!(body["error"], false);
        assert_eq!(
            body["output"],
            "password=[redacted]\nnovel cloud diagnostic 測試🙂"
        );
        assert!(!body.to_string().contains("cloudbridgecanary"));
    });
    let state = Arc::new(Mutex::new(SharedState {
        cloud_agent_id: Some("environments/masking/agents/a".into()),
        ..Default::default()
    }));
    let env = vec![
        ("SYLPHX_API_URL".into(), gateway),
        ("SYLPHX_API_KEY".into(), "fixture-key".into()),
    ];
    let result = run_cloud_session_turn(
        &state,
        &Arc::new(AtomicBool::new(false)),
        &cfg,
        &work,
        Some("mask-http/model"),
        &env.clone().into(),
        Path::new("unused"),
        &[],
        "environments/masking",
        "inspect the fixture",
    );
    server.join().unwrap();
    result.unwrap();
    let stored = std::fs::read_to_string(cfg.join("sylphx-transcript.jsonl")).unwrap();
    assert!(!stored.contains("cloudbridgecanary"));
    assert!(stored.contains("novel cloud diagnostic 測試🙂"));
}
