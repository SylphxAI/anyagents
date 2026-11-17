//! Presentation contracts: force human output without changing transport or JSON.
#![cfg(unix)]

use anyagents_core::{NewAgent, NewChannel, Store};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

fn command(data: &Path, pretty: bool) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_anyagents"));
    for key in [
        "ANYAGENTS_AGENT_ID",
        "ANYAGENTS_AGENT_CONFIG",
        "ANYAGENTS_DATA",
        "ANYAGENTS_OPERATOR_TOKEN_FILE",
        "ANYAGENTS_SOCKET",
        "FORCE_COLOR",
    ] {
        command.env_remove(key);
    }
    command
        .env("ANYAGENTS_PRETTY", if pretty { "1" } else { "0" })
        .env("ANYAGENTS_RAW", if pretty { "0" } else { "1" })
        .env("NO_COLOR", "1")
        .env("COLUMNS", "100")
        .arg("--data")
        .arg(data);
    command
}

fn output(command: &mut Command) -> String {
    let out = command.output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

/// One bounded HTTP exchange; return the request to prove rendering didn't change it.
fn remote(args: &[&str], pretty: bool, response: &Value) -> (String, String, Value) {
    let (out, route, request) = exchange(args, pretty, response, "200 OK");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    (String::from_utf8(out.stdout).unwrap(), route, request)
}
fn exchange(
    args: &[&str],
    pretty: bool,
    response: &Value,
    status: &str,
) -> (std::process::Output, String, Value) {
    let status = status.to_owned();
    let dir = tempfile::tempdir().unwrap();
    let token = dir.path().join("operator-token");
    std::fs::write(&token, "fixture-token").unwrap();
    let listener = UnixListener::bind(dir.path().join("anyagents.sock")).unwrap();
    listener.set_nonblocking(true).unwrap();
    let body = response.to_string();
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(err)
                    if err.kind() == std::io::ErrorKind::WouldBlock
                        && Instant::now() < deadline =>
                {
                    std::thread::sleep(Duration::from_millis(5))
                }
                Err(err) => panic!("no CLI request: {err}"),
            }
        };
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            stream.read_exact(&mut byte).unwrap();
            headers.push(byte[0]);
        }
        let headers = String::from_utf8(headers).unwrap();
        let length = headers
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut request = vec![0; length];
        stream.read_exact(&mut request).unwrap();
        write!(stream, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        (
            headers.lines().next().unwrap().to_owned(),
            if request.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&request).unwrap()
            },
        )
    });
    let stdout = command(dir.path(), pretty)
        .env("ANYAGENTS_OPERATOR_TOKEN_FILE", token)
        .args(args)
        .output()
        .unwrap();
    let (route, request) = server.join().unwrap();
    (stdout, route, request)
}

fn message() -> Value {
    json!({"id":"message-fixture", "seq":7, "text":"first line\nsecond line", "sender":"agent:beta", "reply_to":null, "source_time_ms":null, "received_at_ms":0, "ack":"received"})
}

fn turn() -> Value {
    json!({"seq":7, "id":"turn-fixture", "direction":"out", "from":"alpha", "to":"beta", "text":"first line\nsecond line", "timestamp_ms":null})
}

#[test]
fn server_send_receipts_keep_raw_bytes_and_request() {
    let response = json!({"deliveries":[{"to":"alpha","id":"msg-new","seq":7,"duplicate":false},{"to":"beta","id":"msg-old","seq":8,"duplicate":true}]});
    let args = ["send", "alpha,beta", "hello", "--from", "operator"];
    let (raw, route, request) = remote(&args, false, &response);
    assert_eq!(raw, "msg-new\t7\nmsg-old\t8\tduplicate\n");
    assert_eq!(route, "POST http://localhost/v1/send HTTP/1.1");
    let (human, human_route, human_request) = remote(&args, true, &response);
    assert_eq!(human_route, route);
    assert_eq!(human_request, request);
    assert!(human.contains("Sent to alpha"));
    assert!(human.contains("Already delivered to beta"));
    assert!(!human.contains("msg-new"));
    assert!(!human.contains('\t'));
    let (verbose, _, _) = remote(&["--verbose", "send", "alpha", "hello"], true, &response);
    assert!(verbose.contains("msg-new · sequence 7"));
}

#[test]
fn inbox_keeps_filters_json_and_full_body() {
    let response = json!({"agent":"alpha","agent_id":"fixture","messages":[message()]});
    let args = [
        "inbox",
        "alpha",
        "--unread",
        "--sender",
        "agent:beta",
        "--since",
        "3",
        "-n",
        "2",
    ];
    let (raw, route, _) = remote(&args, false, &response);
    assert!(raw.contains("[#7]"));
    assert!(raw.ends_with("first line\nsecond line\n\n"));
    assert!(
        route.contains("unread=true&since=3&limit=2&sender=agent%3Abeta"),
        "{route}"
    );
    let (human, human_route, _) = remote(&args, true, &response);
    assert_eq!(human_route, route);
    assert!(human.contains("Inbox · alpha"));
    assert!(human.contains("queued"));
    assert!(human.contains("first line ↵ second line"));
    assert!(!human.contains("message-fixture"));
    let (full, _, _) = remote(&["inbox", "alpha", "--full"], true, &response);
    assert!(full.contains("    first line\n    second line"));
    let args = ["inbox", "alpha", "--json"];
    assert_eq!(
        remote(&args, true, &response).0,
        remote(&args, false, &response).0
    );
}

#[test]
fn conversation_and_search_preserve_query_envelopes_and_transcripts() {
    let response = json!({"agent":"alpha","agent_id":"fixture","turns":[turn()]});
    let args = [
        "conversation",
        "alpha",
        "--query",
        "first line",
        "--out",
        "--sender",
        "alpha",
        "--from-seq",
        "2",
        "--to-seq",
        "9",
        "-n",
        "3",
        "--full",
    ];
    let (raw, route, _) = remote(&args, false, &response);
    assert!(raw.contains("OUT"));
    assert!(raw.ends_with("first line\nsecond line\n\n"));
    for filter in [
        "query=first%20line",
        "sender=alpha",
        "direction=out",
        "from_seq=2",
        "to_seq=9",
        "limit=3",
        "include_tools=false",
    ] {
        assert!(route.contains(filter), "{route}");
    }
    let (human, human_route, _) = remote(&args, true, &response);
    assert_eq!(human_route, route);
    assert!(human.contains("Search · alpha · “first line”"));
    assert!(human.contains("→ out"));
    assert!(human.contains("    first line\n    second line"));
    assert!(!human.contains("turn-fixture"));
    let args = ["search", "first line", "--agent", "alpha", "--json"];
    assert_eq!(
        remote(&args, true, &response).0,
        remote(&args, false, &response).0
    );
}

#[test]
fn channel_mutations_render_bound_and_pending_receipts_without_ids_or_secrets() {
    let bound = json!({"status":"bound","kind":"telegram","agent":"alpha","agent_id":"private-agent-id","channel_id":"private-channel-id","bot_username":"@alpha_bot"});
    let args = [
        "channels", "add", "telegram", "--agent", "alpha", "--bot-id", "42",
    ];
    let (raw, route, request) = remote(&args, false, &bound);
    assert_eq!(raw, format!("{bound}\n"));
    assert_eq!(route, "POST http://localhost/v1/channels HTTP/1.1");
    assert_eq!(request["bot_id"], 42);
    let (human, _, human_request) = remote(&args, true, &bound);
    assert_eq!(human_request, request);
    assert!(human.contains("Channel bound · @alpha_bot → alpha"));
    assert!(!human.contains("private-"));
    let pending = json!({"status":"pending_creation","agent":"alpha","manager_bot":"@manager_bot","suggested_bot_username":"@alpha_bot","newbot_url":"https://t.me/newbot/manager_bot/alpha_bot?name=Alpha","agent_id":"private-agent-id"});
    let (human, _, _) = remote(
        &["channels", "request-bot", "--agent", "alpha"],
        true,
        &pending,
    );
    assert!(human.contains("Bot creation requested"));
    assert!(human.contains("Manager: @manager_bot"));
    assert!(human.contains("https://t.me/newbot/manager_bot/alpha_bot?name=Alpha"));
    assert!(!human.contains("private-"));
}

#[test]
fn local_channel_list_keeps_chat_membership_and_raw_columns() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let agent = store.create_agent(NewAgent::new("alpha", "codex")).unwrap();
    let channel = store
        .create_channel(NewChannel {
            kind: "telegram".into(),
            identity: Some("42:alpha_bot".into()),
            config: "{}".into(),
            credential_ref: Some("secret-reference".into()),
        })
        .unwrap();
    store.attach_channel(&channel.id, &agent.id).unwrap();
    store
        .set_channel_state(
            &channel.id,
            &json!({"chats":[{"chat_id":-7,"title":"Team room"},{"chat_id":10}]}).to_string(),
        )
        .unwrap();
    let raw = output(command(dir.path(), false).args(["channels", "list"]));
    assert_eq!(
        raw,
        format!("{}\ttelegram\tsecret-reference\talpha\n", channel.id)
    );
    let human = output(command(dir.path(), true).args(["channels", "list"]));
    for text in ["Channels", "@alpha_bot", "alpha", "Team room", "1 DM"] {
        assert!(human.contains(text), "{human}");
    }
    assert!(!human.contains("secret-reference"));
    assert!(!human.contains(channel.id.as_str()));
    assert!(!human.contains('\t'));
    let verbose = output(command(dir.path(), true).args(["--verbose", "channels", "list"]));
    assert!(verbose.contains(channel.id.as_str()));
}

#[test]
fn local_operator_receipts_repeats_and_json_are_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("state.db")).unwrap();
    store.create_agent(NewAgent::new("alpha", "codex")).unwrap();
    let args = ["send", "operator", "build failed", "--from", "agent:alpha"];
    let raw = output(command(dir.path(), false).args(args));
    assert_eq!(raw.trim().split('\t').count(), 2);
    let duplicate = output(command(dir.path(), false).args(args));
    assert!(duplicate.ends_with("\tduplicate\n"));
    let human = output(command(dir.path(), true).args(args));
    assert!(human.contains("Already delivered to operator"));
    assert!(!human.contains('\t'));
    let args = ["inbox", "--operator", "--json"];
    let raw = output(command(dir.path(), false).args(args));
    assert_eq!(raw, output(command(dir.path(), true).args(args)));
    let human = output(command(dir.path(), true).args(["inbox", "--operator", "--full"]));
    assert!(human.contains("Inbox · operator"));
    assert!(human.contains("agent:alpha"));
    assert!(human.contains("build failed [repeated 2x]"));
}

#[test]
fn empty_human_reads_are_explicit_and_raw_stays_empty() {
    let inbox = json!({"agent":"alpha","agent_id":"fixture","messages":[]});
    assert!(
        remote(&["inbox", "alpha"], true, &inbox)
            .0
            .contains("No messages matched.")
    );
    assert_eq!(remote(&["inbox", "alpha"], false, &inbox).0, "");
    let conversation = json!({"agent":"alpha","agent_id":"fixture","turns":[]});
    assert!(
        remote(&["conversation", "alpha"], true, &conversation)
            .0
            .contains("No conversation turns matched.")
    );
    let dir = tempfile::tempdir().unwrap();
    assert!(
        output(command(dir.path(), true).args(["channels", "list"]))
            .contains("No channels recorded.")
    );
}

#[test]
fn fleet_aliases_preserve_requests_raw_bytes_and_explicit_json() {
    let response = json!({"schedules":[{"agent_name":"工作者","id":"schedule-private","kind":"watch","spec":"github:pr:example/repo#1","state":"active","prompt":"Review change"}]});
    let (raw, route, request) = remote(&["schedules", "--all"], false, &response);
    assert_eq!(
        raw,
        "schedule-private\twatch\tgithub:pr:example/repo#1\tactive\tReview change\n"
    );
    assert_eq!(route, "POST http://localhost/v1/schedules HTTP/1.1");
    assert_eq!(request["agent"], "all");
    assert_eq!(request["action"], "list");
    for args in [
        vec!["automations"],
        vec!["schedules", "--all"],
        vec!["schedule", "all"],
    ] {
        let (human, human_route, human_request) = remote(&args, true, &response);
        assert_eq!(human_route, route);
        assert_eq!(human_request, request);
        assert!(human.contains("工作者"));
        assert!(human.contains("AGENT"));
        assert!(!human.contains("schedule-private"));
        assert!(!human.contains('\t'));
        let mut args = args;
        args.push("--json");
        let (json, _, json_request) = remote(&args, true, &response);
        assert_eq!(serde_json::from_str::<Value>(&json).unwrap(), response);
        assert_eq!(json_request, request);
    }
}

#[test]
fn fleet_errors_never_fall_back_to_offline_state() {
    for args in [
        vec!["automations"],
        vec!["schedules", "--all"],
        vec!["schedule", "all"],
    ] {
        let (out, _, _) = exchange(
            &args,
            true,
            &json!({"error":"fleet permission denied"}),
            "403 Forbidden",
        );
        assert!(!out.status.success());
        assert!(out.stdout.is_empty());
        let err = String::from_utf8(out.stderr).unwrap();
        assert!(err.contains("fleet permission denied"), "{err}");
        let data = tempfile::tempdir().unwrap();
        let out = command(data.path(), true).args(&args).output().unwrap();
        assert!(!out.status.success());
        assert!(String::from_utf8_lossy(&out.stderr).contains("require a running server"));
        assert!(!data.path().join("state.db").exists());
    }
}

#[test]
fn fleet_mutations_are_refused_before_io() {
    for args in [
        vec!["schedules", "--all", "--cancel", "schedule-private"],
        vec!["schedule", "all", "--cancel", "schedule-private"],
        vec![
            "schedule",
            "agent:all",
            "--every",
            "1h",
            "--prompt",
            "unsafe",
        ],
        vec!["schedule", "all", "--prompt", "unsafe"],
    ] {
        let data = tempfile::tempdir().unwrap();
        let out = command(data.path(), true).args(&args).output().unwrap();
        assert!(!out.status.success(), "{args:?}");
        assert!(out.stdout.is_empty());
        assert!(!data.path().join("state.db").exists());
    }
}
