use super::super::tests::Mock;
use super::*;
use std::io::Cursor;

const AGENT: &str = "00000000-0000-4000-8000-000000000001";
const ROOT: &str = "00000000-0000-4000-8000-000000000002";
const STEER: &str = "00000000-0000-4000-8000-000000000003";
const FIRST: &str = "00000000-0000-4000-8000-000000000004";
const SECOND: &str = "00000000-0000-4000-8000-000000000005";

fn contract() -> Value {
    json!({"x-spiron-input-delivery":{"protocol":"session-entry-v1","level":"next-step","idempotent":true,"proof":"input_delivery"}})
}
fn receipt(task: &str, key: &str, status: &str, proof: bool, turn: &str) -> Value {
    json!({"id":task,"agent_id":AGENT,"provider_message_id":key,"status":status,"reply":"done",
        "input_delivery":if proof { json!({"session_id":AGENT,"entry_id":key,"turn_entry_id":turn,"recorded_at":"2026-11-17T12:00:00Z"}) } else { Value::Null }})
}
#[derive(Clone, Default)]
struct Sink(Arc<Mutex<Vec<u8>>>);
impl Write for Sink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn events(sink: &Sink) -> Vec<Value> {
    String::from_utf8(sink.0.lock().unwrap().clone())
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}
fn config(mock: &Mock) -> tempfile::TempDir {
    let config = mock.config();
    save(
        &config.path().join(session_filename_for(HARNESS)),
        AGENT.as_bytes(),
    )
    .unwrap();
    config
}
fn line(uuid: &str, text: &str) -> Value {
    json!({"type":"user","uuid":uuid,"message":{"role":"user","content":text}})
}

#[test]
fn submits_steer_before_root_finishes_and_proves_each_input_independently() {
    let mock = Mock::new(vec![
        (200, contract()),
        (202, receipt(FIRST, ROOT, "working", false, ROOT)),
        (202, receipt(SECOND, STEER, "working", true, ROOT)),
        (200, receipt(FIRST, ROOT, "completed", true, ROOT)),
        (200, receipt(SECOND, STEER, "completed", true, ROOT)),
    ]);
    let config = config(&mock);
    let sink = Sink::default();
    let input = format!("{}\n{}\n", line(ROOT, "root"), line(STEER, "steer"));
    serve(
        config.path(),
        None,
        Some(AGENT),
        Cursor::new(input),
        sink.clone(),
    )
    .unwrap();
    mock.consumed();
    let requests = mock.requests();
    assert_eq!(requests[1].method, "POST");
    assert_eq!(
        requests[2].method, "POST",
        "steer submitted while root is still working"
    );
    assert_eq!(requests[1].headers["idempotency-key"], ROOT);
    assert_eq!(requests[2].headers["idempotency-key"], STEER);
    assert_eq!(requests[2].body["content"], "steer");
    assert!(
        requests
            .iter()
            .all(|r| r.headers["authorization"] == "Bearer api-test-token")
    );
    let output = events(&sink);
    assert_eq!(output[0]["delivery"]["level"], "next-step");
    let echoes: Vec<_> = output.iter().filter(|e| e["isReplay"] == true).collect();
    assert_eq!(echoes.len(), 2);
    assert!(echoes.iter().all(|e| e["input_delivery"].is_object()));
    assert_eq!(
        echoes[0]["uuid"], STEER,
        "root cannot be acknowledged before its proof"
    );
    let finished: Vec<_> = output.iter().filter(|e| e["type"] == "result").collect();
    assert_eq!(finished[0]["completed_message_uuids"], json!([ROOT]));
    assert_eq!(finished[1]["completed_message_uuids"], json!([STEER]));
}

#[test]
fn restart_replays_cached_completion_without_a_second_mutation_and_rejects_changed_content() {
    let mock = Mock::new(vec![(202, receipt(FIRST, ROOT, "completed", true, ROOT))]);
    let config = config(&mock);
    let path = config.path().join("input.json");
    {
        let session = Session::open(config.path(), None, Some(AGENT)).unwrap();
        let mut input =
            Input::prepare(&session, config.path(), path.clone(), Some(ROOT), "root").unwrap();
        assert!(input.advance(&session, true).unwrap().is_some());
    }
    let session = Session::open(config.path(), None, Some(AGENT)).unwrap();
    let mut replay =
        Input::prepare(&session, config.path(), path.clone(), Some(ROOT), "root").unwrap();
    assert!(replay.advance(&session, true).unwrap().is_some());
    assert!(Input::prepare(&session, config.path(), path, Some(ROOT), "different").is_err());
    assert_eq!(mock.requests().len(), 1);
}

#[test]
fn wrong_key_or_missing_proof_never_produces_a_replay() {
    for response in [
        receipt(FIRST, STEER, "working", true, ROOT),
        receipt(FIRST, ROOT, "completed", false, ROOT),
    ] {
        let mock = Mock::new(vec![(200, contract()), (202, response)]);
        let config = config(&mock);
        let sink = Sink::default();
        assert!(
            serve(
                config.path(),
                None,
                Some(AGENT),
                Cursor::new(format!("{}\n", line(ROOT, "root"))),
                sink.clone()
            )
            .is_err()
        );
        assert!(!events(&sink).iter().any(|e| e["isReplay"] == true));
        assert!(
            config
                .path()
                .join(format!("spiron-cloud-inputs/{ROOT}.json"))
                .exists()
        );
    }
}

#[test]
fn only_exact_versioned_capability_enables_concurrent_next_step() {
    assert!(declares_next_step(&contract()));
    for value in [
        json!({}),
        json!({"x-spiron-input-delivery":{"protocol":"other","level":"next-step","idempotent":true,"proof":"input_delivery"}}),
        json!({"x-spiron-input-delivery":{"protocol":"session-entry-v1","level":"next-step","idempotent":false,"proof":"input_delivery"}}),
    ] {
        assert!(!declares_next_step(&value));
    }
}

#[test]
fn one_controller_owns_config_and_pins_api_key_across_restart() {
    let mock = Mock::new(vec![]);
    let config = config(&mock);
    let session = Session::open(config.path(), None, Some(AGENT)).unwrap();
    assert!(Session::open(config.path(), None, Some(AGENT)).is_err());
    drop(session);
    std::fs::write(
        config.path().join("cloud.json"),
        json!({"base_url":mock.base,"token":"rotated-test-token"}).to_string(),
    )
    .unwrap();
    assert!(Session::open(config.path(), None, Some(AGENT)).is_err());
}

#[test]
fn imported_unknown_legacy_request_keeps_key_body_and_receipt_across_restart() {
    let mock = Mock::new(vec![(200, receipt(SECOND, FIRST, "completed", true, ROOT))]);
    let config = config(&mock);
    let old = config.path().join("spiron-cloud-message.json");
    let body =
        json!({"content":"root", "mcp_servers":[{"bearer_token":"expired-test-capability"}]});
    save(
        &old,
        json!({"scope":format!("{}/{}",mock.base,AGENT),"key":FIRST,
        "body":body,"receipt":SECOND})
        .to_string()
        .as_bytes(),
    )
    .unwrap();
    let path = config.path().join("input.json");
    {
        let session = Session::open(config.path(), None, Some(AGENT)).unwrap();
        let input =
            Input::prepare(&session, config.path(), path.clone(), Some(ROOT), "root").unwrap();
        assert_eq!(input.request.key, FIRST);
        assert_eq!(input.request.body, body);
        assert_eq!(input.request.receipt.as_deref(), Some(SECOND));
        assert!(!old.exists());
    }
    let session = Session::open(config.path(), None, Some(AGENT)).unwrap();
    let mut input =
        Input::prepare(&session, config.path(), path.clone(), Some(ROOT), "root").unwrap();
    assert!(input.advance(&session, true).unwrap().is_some());
    assert_eq!(mock.requests()[0].method, "GET");
    assert!(
        !std::fs::read_to_string(path)
            .unwrap()
            .contains("expired-test-capability")
    );
    mock.consumed();
}

#[test]
fn legacy_peer_serializes_inputs_without_claiming_next_step() {
    let page = json!({"agent_id":AGENT,"data":[],"has_more":false,"next_cursor":null});
    let mock = Mock::new(vec![
        (200, json!({})),
        (202, receipt(FIRST, ROOT, "completed", false, ROOT)),
        (200, page.clone()),
        (202, receipt(SECOND, STEER, "completed", false, STEER)),
        (200, page),
    ]);
    let config = config(&mock);
    let sink = Sink::default();
    serve(
        config.path(),
        None,
        Some(AGENT),
        Cursor::new(format!(
            "{}\n{}\n",
            line(ROOT, "root"),
            line(STEER, "steer")
        )),
        sink.clone(),
    )
    .unwrap();
    let output = events(&sink);
    assert_eq!(output[0]["delivery"]["level"], "turn-end");
    assert_eq!(output[0]["delivery"]["idempotent"], false);
    let requests = mock.requests();
    assert_eq!(requests[1].method, "POST");
    assert!(requests[2].uri.contains("conversation"));
    assert_eq!(requests[3].method, "POST");
    mock.consumed();
}

#[test]
fn terminal_failure_does_not_acknowledge_or_finish_another_input() {
    let mock = Mock::new(vec![
        (200, contract()),
        (202, receipt(FIRST, ROOT, "failed", false, ROOT)),
        (202, receipt(SECOND, STEER, "completed", true, STEER)),
    ]);
    let config = config(&mock);
    let sink = Sink::default();
    serve(
        config.path(),
        None,
        Some(AGENT),
        Cursor::new(format!(
            "{}\n{}\n",
            line(ROOT, "root"),
            line(STEER, "steer")
        )),
        sink.clone(),
    )
    .unwrap();
    let output = events(&sink);
    assert!(
        !output
            .iter()
            .any(|e| e["uuid"] == ROOT && e["isReplay"] == true)
    );
    let results: Vec<_> = output.iter().filter(|e| e["type"] == "result").collect();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["completed_message_uuids"], json!([ROOT]));
    assert_eq!(results[0]["is_error"], true);
    assert!(results[0].get("user_message_uuids").is_none());
    assert_eq!(results[1]["completed_message_uuids"], json!([STEER]));
    mock.consumed();
}

#[test]
fn proof_is_immutable_across_restart_not_just_within_a_process() {
    let mut changed = receipt(FIRST, ROOT, "completed", true, ROOT);
    changed["input_delivery"]["entry_id"] = json!(STEER);
    let mock = Mock::new(vec![
        (202, receipt(FIRST, ROOT, "working", true, ROOT)),
        (200, changed),
    ]);
    let config = config(&mock);
    let path = config.path().join("input.json");
    {
        let session = Session::open(config.path(), None, Some(AGENT)).unwrap();
        let mut input =
            Input::prepare(&session, config.path(), path.clone(), Some(ROOT), "root").unwrap();
        assert!(input.advance(&session, true).unwrap().is_none());
        assert!(input.proof.is_some());
    }
    let session = Session::open(config.path(), None, Some(AGENT)).unwrap();
    let mut input = Input::prepare(&session, config.path(), path, Some(ROOT), "root").unwrap();
    assert!(
        input
            .advance(&session, true)
            .unwrap_err()
            .contains("proof changed")
    );
}

#[test]
fn response_lost_after_receipt_creation_reuses_exact_key_and_body() {
    use std::io::Read;
    use std::net::{Shutdown, TcpListener};
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let seen = recorded.clone();
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut operations = std::collections::HashSet::new();
        while seen.lock().unwrap().len() < 2 {
            let (mut stream, _) = match listener.accept() {
                Ok(s) => s,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < deadline,
                        "missing second reconciliation request"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(e) => panic!("{e}"),
            };
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut bytes = Vec::new();
            let (headers, body) = loop {
                let mut buf = [0u8; 2048];
                let n = stream.read(&mut buf).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") else {
                    continue;
                };
                let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                if bytes.len() >= end + 4 + length {
                    break (headers, bytes[end + 4..end + 4 + length].to_vec());
                }
            };
            let key = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("idempotency-key")
                        .then(|| value.trim().to_string())
                })
                .unwrap();
            operations.insert(key.clone());
            seen.lock().unwrap().push((key, body));
            if seen.lock().unwrap().len() == 1 {
                // Operation exists, but no status line or response body reaches the client.
                stream.shutdown(Shutdown::Both).unwrap();
            } else {
                let body = receipt(FIRST, ROOT, "completed", true, ROOT).to_string();
                write!(stream,"HTTP/1.1 202 Accepted\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",body.len(),body).unwrap();
            }
        }
        assert_eq!(operations.len(), 1, "one remote logical operation");
    });
    let config = tempfile::tempdir().unwrap();
    save(
        &config.path().join("cloud.json"),
        json!({"base_url":base,"token":"api-test-token"})
            .to_string()
            .as_bytes(),
    )
    .unwrap();
    let path = config.path().join("input.json");
    {
        let session = Session::open(config.path(), None, Some(AGENT)).unwrap();
        let mut input =
            Input::prepare(&session, config.path(), path.clone(), Some(ROOT), "root").unwrap();
        assert!(
            input
                .advance(&session, true)
                .unwrap_err()
                .contains("acknowledgement uncertain")
        );
    }
    let session = Session::open(config.path(), None, Some(AGENT)).unwrap();
    let mut input = Input::prepare(&session, config.path(), path, Some(ROOT), "root").unwrap();
    assert!(input.advance(&session, true).unwrap().is_some());
    server.join().unwrap();
    let requests = recorded.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0], requests[1]);
}

#[cfg(unix)]
#[test]
fn cancellation_keeps_known_receipts_and_eof_never_invents_completion() {
    use std::os::unix::net::UnixStream;
    struct ControlWriter {
        sink: Sink,
        commands: UnixStream,
        buffer: Vec<u8>,
        cancelled: bool,
    }
    impl Write for ControlWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.sink.write_all(bytes)?;
            self.buffer.extend_from_slice(bytes);
            while let Some(end) = self.buffer.iter().position(|b| *b == b'\n') {
                let line: Vec<_> = self.buffer.drain(..=end).collect();
                let event: Value = serde_json::from_slice(&line).unwrap();
                if event["isReplay"] == true && !self.cancelled {
                    writeln!(self.commands, "{}", json!({"type":"cancel"}))?;
                    self.cancelled = true;
                }
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mock = Mock::new(vec![
        (200, contract()),
        (202, receipt(FIRST, ROOT, "working", true, ROOT)),
        (200, receipt(FIRST, ROOT, "canceled", true, ROOT)),
    ]);
    let config = config(&mock);
    let sink = Sink::default();
    let (mut commands, input) = UnixStream::pair().unwrap();
    writeln!(commands, "{}", line(ROOT, "root")).unwrap();
    let error = serve(
        config.path(),
        None,
        Some(AGENT),
        std::io::BufReader::new(input),
        ControlWriter {
            sink: sink.clone(),
            commands,
            buffer: Vec::new(),
            cancelled: false,
        },
    )
    .unwrap_err();
    assert!(error.contains("cancellation requested"));
    let requests = mock.requests();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[2].uri,
        format!("/v1/message-receipts/{FIRST}:cancel")
    );
    assert!(
        config
            .path()
            .join(format!("spiron-cloud-inputs/{ROOT}.json"))
            .exists()
    );
    assert!(!events(&sink).iter().any(|e| e["type"] == "result"));
    mock.consumed();
}

#[test]
fn overlapping_same_mailbox_uuid_is_only_one_remote_operation() {
    let mock = Mock::new(vec![
        (200, contract()),
        (202, receipt(FIRST, ROOT, "completed", true, ROOT)),
    ]);
    let config = config(&mock);
    let sink = Sink::default();
    serve(
        config.path(),
        None,
        Some(AGENT),
        Cursor::new(format!("{}\n{}\n", line(ROOT, "root"), line(ROOT, "root"))),
        sink.clone(),
    )
    .unwrap();
    assert_eq!(
        mock.requests()
            .iter()
            .filter(|r| r.method == "POST")
            .count(),
        1
    );
    assert_eq!(
        events(&sink)
            .iter()
            .filter(|e| e["type"] == "result")
            .count(),
        1
    );
    mock.consumed();
}
