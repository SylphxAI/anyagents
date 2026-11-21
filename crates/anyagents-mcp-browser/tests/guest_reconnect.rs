#![cfg(target_os = "linux")]

use std::{path::Path, process::Stdio, time::Duration};

use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::{Router, routing::get};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};

struct Client {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    id: u64,
}

impl Client {
    async fn start(dir: &Path, api: Option<&str>) -> Self {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_anyagents-mcp-browser"));
        cmd.arg("--guest-runtime-dir")
            .arg(dir)
            .env_remove("ANYAGENTS_API_URL")
            .env_remove("ANYAGENTS_TAKEOVER_PUBLIC_URL")
            .env_remove("ANYAGENTS_BROWSER_PROFILE")
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        if let Some(api) = api {
            cmd.env("ANYAGENTS_API_URL", api)
                .env("ANYAGENTS_AGENT_TOKEN", "test-agent-token");
        }
        let mut child = cmd.spawn().unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut c = Self {
            child,
            stdin,
            stdout,
            id: 0,
        };
        c.call(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "guest-test", "version": "0"}
            }),
        )
        .await;
        c.write(json!({"jsonrpc":"2.0", "method":"notifications/initialized"}))
            .await;
        c
    }

    async fn write(&mut self, msg: Value) {
        self.stdin
            .write_all(format!("{msg}\n").as_bytes())
            .await
            .unwrap();
    }

    async fn call(&mut self, method: &str, params: Value) -> Value {
        self.id += 1;
        self.write(json!({"jsonrpc":"2.0", "id":self.id, "method":method, "params":params}))
            .await;
        let mut line = String::new();
        loop {
            line.clear();
            assert!(
                timeout(Duration::from_secs(55), self.stdout.read_line(&mut line))
                    .await
                    .unwrap()
                    .unwrap()
                    > 0
            );
            let resp: Value = serde_json::from_str(&line).unwrap();
            if resp["id"] == self.id {
                assert!(resp.get("error").is_none(), "{resp}");
                return resp["result"].clone();
            }
        }
    }

    async fn tool(&mut self, name: &str, args: Value) -> Value {
        let result = self
            .call("tools/call", json!({"name":name,"arguments":args}))
            .await;
        assert!(!result["isError"].as_bool().unwrap_or(false), "{result}");
        result["structuredContent"].clone()
    }

    async fn eof(self) {
        let Self {
            stdin, mut child, ..
        } = self;
        drop(stdin);
        assert!(
            timeout(Duration::from_secs(15), child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
}

#[tokio::test]
async fn idle_stdio_eof_exits_worker_and_removes_socket() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("runtime");
    let first = Client::start(&dir, None).await;
    assert!(dir.join("mcp.sock").exists());
    first.eof().await;
    assert!(!dir.join("mcp.sock").exists());
    Client::start(&dir, None).await.eof().await;
    assert!(!dir.join("mcp.sock").exists());
}

// Run in a Linux sandbox with the image's Chromium, sway, wayvnc and noVNC stack:
// cargo test -p anyagents-mcp-browser --test guest_reconnect -- --include-ignored
#[tokio::test]
#[ignore = "requires the sandbox desktop stack"]
async fn takeover_retains_url_and_tabs_across_stdio_eof_and_expires() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("runtime");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", listener.local_addr().unwrap());
    let (registered, mut registrations) = mpsc::channel(4);
    let (expire, close) = tokio::sync::watch::channel(false);
    let app =
        Router::new().route(
            "/v1/takeover/{id}/tunnel",
            get(move |ws: WebSocketUpgrade| {
                let registered = registered.clone();
                let mut close = close.clone();
                async move {
                    ws.on_upgrade(move |mut ws| async move {
                let Some(Ok(Message::Text(frame))) = ws.recv().await else { return };
                let registration: Value = serde_json::from_str(&frame).unwrap();
                assert_eq!(registration["type"], "register");
                registered.send(registration).await.unwrap();
                ws.send(Message::Text(json!({
                    "type":"registered", "session_id":"desktop-test",
                    "public_base_url":"http://browser.example/takeover/desktop-test/",
                    "public_url":"http://browser.example/takeover/desktop-test/?token=test-viewer",
                    "ttl_ms":60000
                }).to_string().into())).await.unwrap();
                loop {
                    tokio::select! {
                        () = async { let _ = close.wait_for(|sent| *sent).await; } => {
                            let _ = ws.send(Message::Close(Some(axum::extract::ws::CloseFrame {
                                code: 4410, reason: "expired".into(),
                            }))).await;
                            break;
                        }
                        msg = ws.recv() => if !matches!(msg, Some(Ok(_))) { break; },
                    }
                }
            })
                }
            }),
        );
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut first = Client::start(&dir, Some(&api)).await;
    let initial = first.tool("browser_takeover", json!({})).await;
    assert_eq!(initial["active"], true, "{initial}");
    assert!(
        initial["takeover_url"]
            .as_str()
            .unwrap()
            .contains("browser.example"),
        "{initial}"
    );
    let before = first
        .tool("browser_tabs", json!({"action":"new", "url":"about:blank"}))
        .await;
    assert!(before["tabs"].as_array().is_some_and(|t| t.len() > 1));
    assert_eq!(registrations.recv().await.unwrap()["reconnect"], false);
    first.eof().await;
    assert!(
        dir.join("mcp.sock").exists(),
        "active desktop must outlive relay EOF"
    );

    let mut next = Client::start(&dir, Some(&api)).await;
    let status = next.tool("browser_takeover_status", json!({})).await;
    assert_eq!(status["active"], true, "{status}");
    assert_eq!(status["takeover_url"], initial["takeover_url"]);
    let after = next.tool("browser_tabs", json!({})).await;
    assert_eq!(
        after["tabs"], before["tabs"],
        "tab targets must not be recreated"
    );
    assert!(
        registrations.try_recv().is_err(),
        "same tunnel must still be registered"
    );
    next.eof().await;
    expire.send(true).unwrap();
    timeout(Duration::from_secs(10), async {
        while dir.join("mcp.sock").exists() {
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("expired detached worker should close and remove its socket");
    server.abort();
}
