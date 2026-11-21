use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::thread::JoinHandle;
use std::time::Duration;

use anyagents_core::{Agent, CredentialStore, FileCredentialStore, NewAgent, NewChannel};
use serde_json::{Value, json};

use super::super::interact_sync;
use super::*;

const TARGET: &str = "telegram:chat:-1001234";
const TOKEN: &str = "777:synthetic-token";
const API_ENV: &str = "ANYAGENTS_TELEGRAM_API";

struct Fixture {
    dir: tempfile::TempDir,
    store: Store,
    owner: Agent,
    peer: Agent,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let owner = store.create_agent(NewAgent::new("owner", "codex")).unwrap();
        let peer = store.create_agent(NewAgent::new("peer", "codex")).unwrap();
        let channel = store
            .create_channel(NewChannel {
                kind: "telegram".into(),
                identity: Some("owner_bot".into()),
                config: "{}".into(),
                credential_ref: Some("channels/owner".into()),
            })
            .unwrap();
        store.attach_channel(&channel.id, &owner.id).unwrap();
        FileCredentialStore::new(dir.path())
            .write("channels/owner", TOKEN.as_bytes())
            .unwrap();
        Self {
            dir,
            store,
            owner,
            peer,
        }
    }

    fn interact(&self, principal: &Principal, body: Value) -> Result<InteractResponse, ApiError> {
        // Even an actor name outside the principal cannot choose another agent's bot.
        interact_sync(
            &self.store,
            principal,
            &self.owner.name,
            serde_json::from_value(body).unwrap(),
        )
        .map(|result| result.0)
    }

    fn history(&self, agent: &Agent) -> Vec<anyagents_core::ConversationTurn> {
        self.store
            .conversation(&agent.id, None, None, None)
            .unwrap()
    }
}

fn principal(agent: &Agent) -> Principal {
    Principal::Agent {
        agent_id: agent.id.clone(),
        name: agent.name.clone(),
    }
}

struct Override(Option<std::ffi::OsString>);

impl Override {
    fn new(url: &str) -> Self {
        let original = std::env::var_os(API_ENV);
        // Held by the caller for the lifetime of the override.
        unsafe { std::env::set_var(API_ENV, url) };
        Self(original)
    }
}

impl Drop for Override {
    fn drop(&mut self) {
        unsafe {
            if let Some(original) = &self.0 {
                std::env::set_var(API_ENV, original);
            } else {
                std::env::remove_var(API_ENV);
            }
        }
    }
}

fn wire(
    status: u16,
    mime: &'static str,
    reply: Vec<u8>,
) -> (String, JoinHandle<(String, Vec<u8>)>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(&mut socket);
        let mut headers = String::new();
        while !headers.ends_with("\r\n\r\n") {
            assert_ne!(reader.read_line(&mut headers).unwrap(), 0);
        }
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().unwrap())
            })
            .unwrap_or(0);
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body).unwrap();
        write!(socket, "HTTP/1.1 {status} Synthetic\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", reply.len()).unwrap();
        socket.write_all(&reply).unwrap();
        (headers, body)
    });
    (url, task)
}

fn telegram(status: u16, reply: Value) -> (Override, JoinHandle<(String, Vec<u8>)>) {
    let (url, task) = wire(status, "application/json", reply.to_string().into_bytes());
    (Override::new(&url), task)
}

fn png() -> Vec<u8> {
    let mut bytes = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut bytes, image::ImageFormat::Png)
        .unwrap();
    bytes.into_inner()
}

#[test]
fn text_updates_use_bot_api_and_record_acknowledged_changes() {
    let _lock = crate::telegram::channel::tests::env_lock();
    let fixture = Fixture::new();
    for (action, method, field, text) in [
        ("set_chat_title", "setChatTitle", "title", "工程 *Telegram*"),
        (
            "set_chat_description",
            "setChatDescription",
            "description",
            "Hello **team**",
        ),
        (
            "set_chat_description",
            "setChatDescription",
            "description",
            "",
        ),
    ] {
        let (_env, request) = telegram(200, json!({"ok":true,"result":true}));
        let result = fixture
            .interact(
                &principal(&fixture.owner),
                json!({
                    "action":action,"target":TARGET,"text":text,"bot":"@owner_bot"
                }),
            )
            .unwrap();
        assert!(result.ok);
        assert_eq!(result.target, TARGET);
        let (headers, body) = request.join().unwrap();
        assert_eq!(
            headers.lines().next().unwrap(),
            format!("POST /bot{TOKEN}/{method} HTTP/1.1")
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"chat_id":-1001234, field: text})
        );
        let history = fixture.history(&fixture.owner);
        assert!(
            history
                .iter()
                .any(|turn| turn.to == TARGET && turn.text == format!("{action} on {TARGET}"))
        );
    }
    assert_eq!(fixture.history(&fixture.owner).len(), 3);
}

#[test]
fn photos_from_agent_files_and_http_use_multipart() {
    let _lock = crate::telegram::channel::tests::env_lock();
    let fixture = Fixture::new();
    let workdir = anyagents_core::agent_work_dir(fixture.dir.path(), &fixture.owner.id);
    std::fs::create_dir_all(&workdir).unwrap();
    let local = workdir.join("input.png");
    let image = png();
    std::fs::write(&local, &image).unwrap();
    let (origin, download) = wire(200, "image/png", image.clone());
    for source in [
        local.to_string_lossy().into_owned(),
        format!("{origin}/input.png"),
    ] {
        let (_env, request) = telegram(200, json!({"ok":true,"result":true}));
        assert!(fixture.interact(&principal(&fixture.owner), json!({
            "action":"set_chat_photo","target":TARGET,"media":{"kind":"photo","source":source}
        })).unwrap().ok);
        let (headers, body) = request.join().unwrap();
        assert_eq!(
            headers.lines().next().unwrap(),
            format!("POST /bot{TOKEN}/setChatPhoto HTTP/1.1")
        );
        let boundary = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .starts_with("content-type: multipart/form-data;")
                    .then(|| line.split_once("boundary=").unwrap().1)
            })
            .unwrap();
        let expected = [
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"chat_id\"\r\n\r\n-1001234\r\n").into_bytes(),
            format!("--{boundary}\r\nContent-Disposition: form-data; name=\"photo\"; filename=\"chat-photo.png\"\r\nContent-Type: image/png\r\n\r\n").into_bytes(),
            image.clone(),
            format!("\r\n--{boundary}--\r\n").into_bytes(),
        ].concat();
        assert_eq!(body, expected);
    }
    let (headers, _) = download.join().unwrap();
    assert_eq!(headers.lines().next().unwrap(), "GET /input.png HTTP/1.1");
    let history = fixture.history(&fixture.owner);
    assert_eq!(history.len(), 2);
    assert!(
        history
            .iter()
            .all(|turn| turn.text == format!("set_chat_photo on {TARGET}"))
    );
}

#[test]
fn missing_rights_and_false_acknowledgement_do_not_record_success() {
    let _lock = crate::telegram::channel::tests::env_lock();
    let fixture = Fixture::new();
    let description = "Bad Request: not enough rights to change chat title";
    for (status, reply, expected) in [
        (
            400,
            json!({"ok":false,"error_code":400,"description":description}),
            description,
        ),
        (
            200,
            json!({"ok":true,"result":false}),
            "Telegram did not acknowledge the chat change",
        ),
        (
            400,
            json!({"ok":false,"error_code":400,"description":TOKEN}),
            "provider description redacted",
        ),
    ] {
        let (_env, request) = telegram(status, reply);
        let error = fixture
            .interact(
                &principal(&fixture.owner),
                json!({
                    "action":"set_chat_title","target":TARGET,"text":"New title"
                }),
            )
            .unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains(expected), "{}", error.message);
        assert!(!error.message.contains(TOKEN));
        request.join().unwrap();
        assert!(fixture.history(&fixture.owner).is_empty());
    }
}

#[test]
fn foreign_bot_operator_and_isolated_agent_are_refused_before_reading_media() {
    let fixture = Fixture::new();
    let body = json!({
        "action":"set_chat_photo", "target":TARGET, "sender":"agent:owner", "bot":"@owner_bot",
        "media":{"kind":"photo","source":"file:///should-not-be-touched.png"}
    });
    for (actor, expected) in [
        (principal(&fixture.peer), "is not"),
        (Principal::Operator, "requires an agent's own Telegram bot"),
    ] {
        let error = fixture.interact(&actor, body.clone()).unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
        assert!(error.message.contains(expected), "{}", error.message);
    }
    fixture
        .store
        .set_agent_scope(&fixture.owner.id, "isolated")
        .unwrap();
    let error = fixture
        .interact(&principal(&fixture.owner), body)
        .unwrap_err();
    assert_eq!(error.status, StatusCode::FORBIDDEN);
    assert!(error.message.contains("isolated agents"));
    assert!(fixture.history(&fixture.owner).is_empty());
    assert!(fixture.history(&fixture.peer).is_empty());
}

#[test]
fn chat_wide_coordinates_and_action_payloads_are_validated() {
    for invalid in [
        "telegram:chat:42",
        "telegram:chat:-4:7",
        "telegram:chat:-4:7:1",
        "telegram:user:4",
        "anyagents:reply:msg_1",
        "agent:owner",
    ] {
        assert_eq!(
            chat_id(invalid).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(chat_id(TARGET).unwrap(), -1001234);
    assert!(title(None).is_err());
    assert!(title(Some("   ")).is_err());
    assert!(title(Some(&"界".repeat(128))).is_ok());
    assert!(title(Some(&"界".repeat(129))).is_err());
    assert!(description(None).is_err());
    assert_eq!(description(Some("")).unwrap(), "");
    assert!(description(Some(&"界".repeat(255))).is_ok());
    assert!(description(Some(&"界".repeat(256))).is_err());
    let fixture = Fixture::new();
    assert!(photo(&fixture.store, &fixture.owner.id, None).is_err());
    let mut truncated = png();
    truncated.truncate(16);
    assert!(Photo::from_bytes(truncated).is_err());
}

#[tokio::test]
async fn chat_admin_http_route_checks_authentication_target_and_host_file_access() {
    use std::sync::{Arc, Mutex};

    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt as _;

    use crate::server::types::{AppState, ProcessRunner};

    let fixture = Fixture::new();
    fixture
        .store
        .hold_capability_key("test-capability-key-0123456789abcdef")
        .unwrap();
    let capability = fixture.store.issue_capability(&fixture.owner.id).unwrap();
    let app = crate::server::messaging::router().with_state(AppState {
        store: Arc::new(fixture.store),
        health: Default::default(),
        sign_in: Arc::new(Mutex::new(ProcessRunner)),
        auth: crate::auth::Mode::Confined {
            operator_digest: None,
        },
        identity: Arc::new(crate::identity::ServingIdentity::capture()),
        profiles: Default::default(),
        timer_backend: Arc::new(crate::timer::NoopTimerBackend),
    });
    let mut photo = json!({
        "action": "set_chat_photo", "target": TARGET,
        "sender": "agent:owner", "media": {"kind": "photo", "source": "/tmp/team.png"}
    });
    for (token, expected, message) in [
        (None, StatusCode::UNAUTHORIZED, ""),
        (
            Some(&capability),
            StatusCode::FORBIDDEN,
            "private media references",
        ),
        (
            Some(&capability),
            StatusCode::BAD_REQUEST,
            "telegram:chat:<chat_id>",
        ),
    ] {
        if expected == StatusCode::BAD_REQUEST {
            photo["target"] = json!("agent:owner");
            photo["media"]["source"] = json!("https://example.org/logo.png");
        }
        let mut request = Request::post("/v1/interact")
            .header("content-type", "application/json")
            .header(crate::mcp::HTTP_MCP_HEADER, "1");
        if let Some(token) = token {
            request = request.header(crate::operator::AGENT_HEADER, token);
        }
        let result = app
            .clone()
            .oneshot(request.body(Body::from(photo.to_string())).unwrap())
            .await
            .unwrap();
        assert_eq!(result.status(), expected);
        let bytes = axum::body::to_bytes(result.into_body(), 4096)
            .await
            .unwrap();
        let error: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(error["error"].as_str().unwrap().contains(message));
    }
}
