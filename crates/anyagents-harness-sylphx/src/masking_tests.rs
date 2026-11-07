//! Exercise both native tool-batch paths through actual reads and HTTP dispatch.
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::{SharedState, run_native_gateway_turn};

fn next_request(listener: &TcpListener) -> (TcpStream, Value) {
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
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut bytes = Vec::new();
    let mut buf = [0; 4096];
    let (header_end, length) = loop {
        let n = stream.read(&mut buf).unwrap();
        assert_ne!(n, 0, "request ended before its headers");
        bytes.extend_from_slice(&buf[..n]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
            assert!(headers.starts_with("post /v1/responses "));
            let length: usize = headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length:")?.trim().parse().ok())
                .unwrap();
            break (end + 4, length);
        }
    };
    while bytes.len() < header_end + length {
        let n = stream.read(&mut buf).unwrap();
        assert_ne!(n, 0, "request body was truncated");
        bytes.extend_from_slice(&buf[..n]);
    }
    let value = serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap();
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
            &env,
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
