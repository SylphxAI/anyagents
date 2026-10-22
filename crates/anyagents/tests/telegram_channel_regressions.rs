//! Offline transport regressions: all requests go to a loopback fake Telegram.
#[allow(dead_code)]
#[path = "../src/telegram/channel.rs"]
mod telegram;

use anyagents_core::{CredentialStore, FileCredentialStore, NewAgent, NewChannel, Store};
use serde_json::{Value, json};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::time::Duration;

fn setup(path: &std::path::Path) -> Store {
    let store = Store::open(path.join("state.db")).unwrap();
    for (name, chat) in [("ada", -1007), ("bea", -1007), ("cy", -2000)] {
        let agent = store.create_agent(NewAgent::new(name, "codex")).unwrap();
        let reference = format!("channels/{name}");
        let channel = store
            .create_channel(NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: Some(reference.clone()),
            })
            .unwrap();
        store.attach_channel(&channel.id, &agent.id).unwrap();
        store
            .set_channel_state(
                &channel.id,
                &json!({"chats":[{
                    "agent":name,"chat_id":chat,"chat_type":"supergroup"
                }]})
                .to_string(),
            )
            .unwrap();
        FileCredentialStore::new(path)
            .write(&reference, b"777:synthetic-token")
            .unwrap();
    }
    store
}

fn fake(status: u16, response: Value) -> (String, std::thread::JoinHandle<Value>) {
    let body = response.to_string();
    fake_wire(Some(format!(
        "HTTP/1.1 {status} Synthetic\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )))
}

// None consumes the complete request, then loses the acknowledgement.
fn fake_wire(response: Option<String>) -> (String, std::thread::JoinHandle<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let task = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut bytes = Vec::new();
        let (start, length) = loop {
            let mut byte = [0];
            socket.read_exact(&mut byte).unwrap();
            bytes.push(byte[0]);
            if bytes.ends_with(b"\r\n\r\n") {
                let header = String::from_utf8(bytes.clone()).unwrap();
                let length: usize = header
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                break (bytes.len(), length);
            }
        };
        bytes.resize(start + length, 0);
        socket.read_exact(&mut bytes[start..]).unwrap();
        let request = serde_json::from_slice(&bytes[start..]).unwrap();
        if let Some(response) = response {
            socket.write_all(response.as_bytes()).unwrap();
        }
        drop(socket);
        // A forbidden response or lost acknowledgement must not cause an
        // automatic second connection carrying another sendMessage request.
        listener.set_nonblocking(true).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            matches!(listener.accept(), Err(err) if err.kind() == std::io::ErrorKind::WouldBlock)
        );
        request
    });
    (address, task)
}

static API_ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());

// Environment changes are serialized within this standalone test process.
#[test]
fn offline_provider_errors() {
    let _lock = API_ENV.lock().unwrap_or_else(|err| err.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let store = setup(dir.path());
    for (status, description, class) in [
        (403, "Forbidden: bot was blocked by the user", "bot_blocked"),
        (
            403,
            "Forbidden: bot can't initiate conversation with a user",
            "human_start_required",
        ),
        (400, "Bad Request: chat not found", "chat_not_found"),
        (403, "Bad Request: chat not found", "chat_not_found"),
        (403, "Forbidden: user is deactivated", "user_deactivated"),
        (
            403,
            "Forbidden: bot can't send messages to bots",
            "bot_destination_forbidden",
        ),
        (
            403,
            "Forbidden: bot is not a member of the channel chat",
            "not_member",
        ),
        (
            403,
            "Forbidden: bot was kicked from the group chat",
            "not_member",
        ),
        (
            403,
            "Forbidden: bot was kicked from the supergroup chat",
            "not_member",
        ),
        (400, "Bad Request: MESSAGE_THREAD_INVALID", "invalid_topic"),
        (400, "Bad Request: TOPIC_CLOSED", "topic_closed"),
        (400, "Bad Request: reply message not found", "invalid_reply"),
        (400, "Bad Request: REPLY_MESSAGE_NOT_FOUND", "invalid_reply"),
        (400, "Bad Request: CHAT_WRITE_FORBIDDEN", "send_forbidden"),
        (
            400,
            "Bad Request: not enough rights to send text messages to the chat",
            "send_forbidden",
        ),
        (
            409,
            "Conflict: terminated by other getUpdates request; make sure that only one bot instance is running",
            "poll_conflict",
        ),
        (
            409,
            "Conflict: can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first",
            "poll_conflict",
        ),
        (
            400,
            "Bad Request: message to be replied not found https://api.telegram.org/bot777:synthetic-token/sendMessage",
            "provider_refused",
        ),
        (
            403,
            "Forbidden: bot was blocked by the user\nprivate response body",
            "provider_refused",
        ),
        (403, "forbidden: BOT WAS BLOCKED BY THE USER", "bot_blocked"),
        (
            403,
            "Forbidden: bot is not a member of the supergroup chat",
            "not_member",
        ),
        (
            400,
            "Bad Request: message thread not found",
            "invalid_topic",
        ),
        (
            400,
            "Bad Request: message to be replied not found",
            "invalid_reply",
        ),
        (429, "Too Many Requests: retry after 8", "rate_limited"),
        (401, "Unauthorized", "unauthorized"),
        (200, "Forbidden: bot was blocked by the user", "bot_blocked"),
        (
            500,
            "Forbidden: bot was blocked by the user extra private data",
            "provider_refused",
        ),
        (
            403,
            "personal message text https://api.telegram.org/bot777:synthetic-token/sendMessage",
            "provider_refused",
        ),
    ] {
        // HTTP status and Telegram error_code are independent diagnostics.
        let code = if status == 500 { 400 } else { status };
        let (address, task) = fake(
            status,
            json!({"ok":false,"error_code":code,
            "description":description,"parameters":{"retry_after":8},
            "result":{"text":"private response body"}}),
        );
        unsafe {
            std::env::set_var("ANYAGENTS_TELEGRAM_API", address);
        }
        let err = telegram::post_text(
            &store,
            dir.path(),
            "ada",
            -1007,
            "private outbound text",
            Some(55),
            Some(3),
        )
        .unwrap_err()
        .to_string();
        let request = task.join().unwrap();
        let details: Value =
            serde_json::from_str(err.strip_prefix("telegram ").expect(&err)).expect(&err);
        assert_eq!(details["class"], class);
        assert_eq!(
            details["channel_id"],
            telegram::outbound_call(&store, "ada", -1007, "", None, None)
                .unwrap()
                .channel_id
                .as_str()
        );
        assert_eq!(details["status"], status);
        assert_eq!(details["error_code"], code);
        if class == "chat_not_found" {
            assert_ne!(details["class"], "human_start_required");
        }
        if class == "provider_refused" {
            assert_eq!(details["description"], "provider description redacted");
        }
        assert_eq!(request["chat_id"], -1007);
        assert!(err.contains(class), "expected {class}: {err}");
        assert!(err.contains(&format!("\"status\":{status}")), "{err}");
        assert!(err.contains("\"method\":\"sendMessage\""), "{err}");
        assert!(err.contains(&format!("\"error_code\":{code}")), "{err}");
        if status == 429 {
            assert!(err.contains("\"retry_after\":8"), "{err}");
        }
        if class == "human_start_required" {
            assert!(err.contains("can't initiate conversation"), "{err}");
        }
        for secret in [
            "synthetic-token",
            "https://",
            "personal message text",
            "private response body",
            "private outbound text",
        ] {
            assert!(!err.contains(secret), "unsafe error: {err}");
        }
        assert!(
            store
                .inbox(&store.agent_by_name("bea").unwrap().id, None)
                .unwrap()
                .is_empty()
        );
    }
    let closed = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    unsafe {
        std::env::set_var("ANYAGENTS_TELEGRAM_API", address);
    }
    let err = telegram::post_text(
        &store,
        dir.path(),
        "ada",
        -1007,
        "private outbound text",
        None,
        None,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("network"), "{err}");
    assert!(!err.contains("synthetic-token"));

    unsafe {
        std::env::remove_var("ANYAGENTS_TELEGRAM_API");
    }
}

#[test]
fn offline_topic_quote_delivery_survives_restart_and_replay() {
    let _lock = API_ENV.lock().unwrap_or_else(|err| err.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let store = setup(dir.path());
    let (address, task) = fake(
        200,
        json!({"ok":true,"result":{
            "message_id":901,"chat":{"id":-1007,"type":"supergroup"},
            "message_thread_id":7,"date":1700000000,"text":"topic answer",
            "reply_to_message":{"message_id":55,"chat":{"id":-1007,"type":"supergroup"},
                "date":1700000000,"from":{"id":42,"is_bot":false,"first_name":"Synthetic Human"},
                "text":"synthetic original"}
        }}),
    );
    unsafe {
        std::env::set_var("ANYAGENTS_TELEGRAM_API", address);
    }
    assert_eq!(
        telegram::post_text(
            &store,
            dir.path(),
            "ada",
            -1007,
            "topic answer",
            Some(55),
            Some(3)
        )
        .unwrap(),
        901
    );
    let request = task.join().unwrap();
    assert_eq!(request["message_thread_id"], 3);
    assert_eq!(request["reply_to_message_id"], 55);
    drop(store);
    let store = Store::open(dir.path().join("state.db")).unwrap();
    let bea = store.agent_by_name("bea").unwrap();
    let inbox = store.inbox(&bea.id, None).unwrap();
    assert_eq!(inbox.len(), 1);
    let message = &inbox[0];
    assert_eq!(message.origin.unwrap().thread_id, Some(7));
    assert_eq!(message.origin.unwrap().message_id, Some(901));
    assert_eq!(message.reply_to.as_deref(), Some("telegram:chat:-1007:7"));
    assert!(message.text.contains("Synthetic Human"));
    assert!(message.text.contains("synthetic original"));
    // Replaying a local receipt never sends to Telegram or duplicates the row.
    telegram::fan_out(&store, "ada", -1007, 901, "topic answer").unwrap();
    let replayed = store.inbox(&bea.id, None).unwrap();
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].text, message.text);
    assert_eq!(replayed[0].reply_to, message.reply_to);
    assert_eq!(replayed[0].origin, message.origin);
    for name in ["ada", "cy"] {
        assert!(
            store
                .inbox(&store.agent_by_name(name).unwrap().id, None)
                .unwrap()
                .is_empty()
        );
    }
    unsafe {
        std::env::remove_var("ANYAGENTS_TELEGRAM_API");
    }
}

#[test]
fn offline_lost_or_invalid_acknowledgement_is_uncertain_and_never_retried() {
    let _lock = API_ENV.lock().unwrap_or_else(|err| err.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let store = setup(dir.path());
    for (wire, expected_class, expected_status) in [
        (None, "transport", Value::Null),
        (Some("HTTP/1.1 200 OK\r\nContent-Length: 80\r\nConnection: close\r\n\r\n{\"ok\":true".to_string()), "response_read", json!(200)),
        (Some("HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nsecret".to_string()), "invalid_response", json!(200)),
        (Some("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}".to_string()), "invalid_response", json!(200)),
        (Some("HTTP/1.1 200 OK\r\nContent-Length: 23\r\nConnection: close\r\n\r\n{\"ok\":true,\"result\":{}}".to_string()), "invalid_response", json!(200)),
    ] {
        let (address, task) = fake_wire(wire);
        unsafe { std::env::set_var("ANYAGENTS_TELEGRAM_API", address); }
        let err = telegram::post_text(&store, dir.path(), "ada", -1007,
            "private outbound text", Some(55), Some(3)).unwrap_err().to_string();
        let request = task.join().unwrap();
        assert_eq!(request["text"], "private outbound text");
        let details: Value = serde_json::from_str(err.strip_prefix("telegram ").unwrap()).unwrap();
        // A complete request followed by EOF is an HTTP protocol/transport
        // failure; it cannot establish whether Telegram committed the send.
        if expected_status.is_null() {
            assert!(matches!(details["class"].as_str(), Some("network" | "transport")));
        } else {
            assert_eq!(details["class"], expected_class);
        }
        assert_eq!(details["status"], expected_status);
        assert_eq!(details["error_code"], Value::Null);
        assert!(details["description"].as_str().unwrap().contains("delivery may be uncertain"));
        for secret in ["synthetic-token", "private outbound text", "secret", "http://"] {
            assert!(!err.contains(secret), "{err}");
        }
        assert!(store.inbox(&store.agent_by_name("bea").unwrap().id, None).unwrap().is_empty());
    }
    unsafe {
        std::env::remove_var("ANYAGENTS_TELEGRAM_API");
    }
}

#[test]
fn offline_unknown_fields_and_redirect_do_not_expose_or_forward_secrets() {
    let _lock = API_ENV.lock().unwrap_or_else(|err| err.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let store = setup(dir.path());
    let redirect_target = TcpListener::bind("127.0.0.1:0").unwrap();
    redirect_target.set_nonblocking(true).unwrap();
    let body = json!({"ok":false,"error_code":"777:synthetic-token",
        "description":"Unauthorized https://api.telegram.org/bot777:synthetic-token/sendMessage private text",
        "parameters":{"retry_after":"777:synthetic-token"}}).to_string();
    let wire = format!(
        "HTTP/1.1 302 Found\r\nLocation: http://{}/bot777:synthetic-token/sendMessage\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        redirect_target.local_addr().unwrap(),
        body.len()
    );
    let (address, task) = fake_wire(Some(wire));
    unsafe {
        std::env::set_var("ANYAGENTS_TELEGRAM_API", address);
    }
    let err = telegram::post_text(
        &store,
        dir.path(),
        "ada",
        -1007,
        "private outbound text",
        None,
        None,
    )
    .unwrap_err()
    .to_string();
    task.join().unwrap();
    assert!(
        matches!(redirect_target.accept(), Err(err) if err.kind() == std::io::ErrorKind::WouldBlock)
    );
    let details: Value = serde_json::from_str(err.strip_prefix("telegram ").unwrap()).unwrap();
    assert_eq!(details["class"], "provider_refused");
    assert_eq!(details["status"], 302);
    assert!(details["error_code"].is_null());
    assert!(details["retry_after"].is_null());
    assert_eq!(details["description"], "provider description redacted");
    for secret in ["synthetic-token", "http", "private", "Unauthorized"] {
        assert!(!err.contains(secret), "{err}");
    }
    unsafe {
        std::env::remove_var("ANYAGENTS_TELEGRAM_API");
    }
}

#[test]
fn offline_invalid_identity_and_updates_never_echo_provider_data() {
    let _lock = API_ENV.lock().unwrap_or_else(|err| err.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let store = setup(dir.path());
    let channel = store
        .list_channels()
        .unwrap()
        .into_iter()
        .find(|channel| channel.credential_ref.as_deref() == Some("channels/ada"))
        .unwrap();
    for (method, response) in [
        (
            "getMe",
            json!({"ok":true,"result":{"id":"777:synthetic-token private identity","username":"synthetic"}}),
        ),
        (
            "getUpdates",
            json!({"ok":true,"result":[{"update_id":1,"message":{
                "message_id":"777:synthetic-token private update","chat":{"id":-1007,"type":"supergroup"},
                "date":1,"text":"private message body"
            }}]}),
        ),
    ] {
        if method == "getUpdates" {
            let mut memory = anyagents_channel_telegram::Memory::from_state(&channel.state);
            memory.bot = Some("777:synthetic_bot".into());
            store
                .set_channel_state(&channel.id, &memory.to_state())
                .unwrap();
        }
        let channel = store
            .list_channels()
            .unwrap()
            .into_iter()
            .find(|one| one.id == channel.id)
            .unwrap();
        let before = channel.state.clone();
        let (address, task) = fake(200, response);
        unsafe {
            std::env::set_var("ANYAGENTS_TELEGRAM_API", address);
        }
        let err =
            telegram::poll_channel_once(&store, &FileCredentialStore::new(dir.path()), &channel, 0)
                .unwrap_err()
                .to_string();
        task.join().unwrap();
        let details: Value = serde_json::from_str(err.strip_prefix("telegram ").unwrap()).unwrap();
        assert_eq!(details["class"], "invalid_response");
        assert_eq!(details["method"], method);
        for private in ["synthetic-token", "private", "http"] {
            assert!(!err.contains(private), "{err}");
        }
        assert_eq!(
            store
                .list_channels()
                .unwrap()
                .into_iter()
                .find(|one| one.id == channel.id)
                .unwrap()
                .state,
            before
        );
    }
    unsafe {
        std::env::remove_var("ANYAGENTS_TELEGRAM_API");
    }
}
