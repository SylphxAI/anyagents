#![cfg(target_os = "linux")]

use std::{
    fs,
    path::{Path as FsPath, PathBuf},
    process::Stdio,
    time::Duration,
};

use axum::extract::{
    Path,
    ws::{CloseFrame, Message, WebSocketUpgrade},
};
use axum::{Router, routing::get};
use futures_util::StreamExt as _;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, timeout};

struct Client {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    id: u64,
}

impl Client {
    async fn start(dir: &FsPath, api: Option<&str>) -> Self {
        let mut cmd = command(dir, api);
        cmd.stdin(Stdio::piped()).stdout(Stdio::piped());
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
                "clientInfo": {"name": "takeover-test", "version": "0"}
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

fn command(dir: &FsPath, api: Option<&str>) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_anyagents-mcp-browser"));
    cmd.arg("--guest-runtime-dir")
        .arg(dir)
        .env_remove("ANYAGENTS_API_URL")
        .env_remove("ANYAGENTS_TAKEOVER_PUBLIC_URL")
        .env_remove("ANYAGENTS_BROWSER_PROFILE")
        .env("ANYAGENTS_TAKEOVER_BIND", "127.0.0.1:0")
        .kill_on_drop(true);
    if let Some(api) = api {
        cmd.env("ANYAGENTS_API_URL", api)
            .env("ANYAGENTS_AGENT_TOKEN", "test-agent-token");
    }
    cmd
}

async fn wait_until(mut ready: impl FnMut() -> bool) {
    timeout(Duration::from_secs(8), async {
        while !ready() {
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("desktop state did not converge");
}

async fn start_worker(dir: &FsPath, api: &str) -> Child {
    let worker = command(dir, Some(api))
        .arg("--guest-runtime-worker")
        .spawn()
        .unwrap();
    wait_until(|| dir.join("mcp.sock").exists()).await;
    worker
}

struct Viewer {
    id: String,
    token: String,
    origin: String,
    end: Option<oneshot::Sender<u16>>,
    closed: Option<oneshot::Receiver<bool>>,
}

impl Viewer {
    fn request(&self, method: &str, path: &str, token: &str) -> Result<u16, ureq::Error> {
        let config = ureq::Agent::config_builder()
            .proxy(None)
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(2)))
            .build();
        let agent = ureq::Agent::new_with_config(config);
        let url = format!("{}{path}?token={token}", self.origin);
        let response = if method == "POST" {
            agent.post(&url).send_empty()?
        } else {
            agent.get(&url).call()?
        };
        Ok(response.status().as_u16())
    }
}

async fn coordinator() -> (String, mpsc::Receiver<Viewer>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}", listener.local_addr().unwrap());
    let (registered, registrations) = mpsc::channel(4);
    let app =
        Router::new().route(
            "/v1/takeover/{id}/tunnel",
            get(move |Path(id): Path<String>, ws: WebSocketUpgrade| {
                let registered = registered.clone();
                async move {
                    ws.on_upgrade(move |mut ws| async move {
                        let Some(Ok(Message::Text(frame))) = ws.recv().await else {
                            panic!("missing registration");
                        };
                        let registration: Value = serde_json::from_str(&frame).unwrap();
                        assert_eq!(registration["type"], "register");
                        assert_eq!(registration["reconnect"], false);
                        let token = registration["token"].as_str().unwrap().to_owned();
                        let origin = registration["local_origin"].as_str().unwrap().to_owned();
                        let base = format!("https://viewer.example/v1/takeover/{id}/");
                        ws.send(Message::Text(json!({
                        "type":"registered", "session_id":id,
                        "public_base_url":base, "public_url":format!("{base}?token={token}"),
                        "ttl_ms":60_000,
                    }).to_string().into())).await.unwrap();
                        let (end, mut terminal) = oneshot::channel();
                        let (report, closed) = oneshot::channel();
                        registered
                            .send(Viewer {
                                id,
                                token,
                                origin,
                                end: Some(end),
                                closed: Some(closed),
                            })
                            .await
                            .unwrap();
                        let mut unregister = false;
                        loop {
                            tokio::select! {
                                code = &mut terminal => {
                                    if let Ok(code) = code {
                                        ws.send(Message::Close(Some(CloseFrame {
                                            code, reason: "takeover ended".into(),
                                        }))).await.unwrap();
                                    }
                                    break;
                                }
                                msg = ws.recv() => match msg {
                                    Some(Ok(Message::Text(frame))) => {
                                        let frame: Value = serde_json::from_str(&frame).unwrap();
                                        unregister |= frame["type"] == "unregister";
                                    }
                                    Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                                    _ => {},
                                },
                            }
                        }
                        let _ = report.send(unregister);
                    })
                }
            }),
        );
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (api, registrations, task)
}

// Only inspect direct children of this test's MCP process; other desktop tests
// or orphaned Chromium helper processes cannot satisfy these assertions.
fn child(pid: u32, command: &str) -> (u32, Vec<String>) {
    fs::read_dir("/proc")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let child = entry.file_name().to_str()?.parse::<u32>().ok()?;
            let stat = fs::read_to_string(entry.path().join("stat")).ok()?;
            // The comm field in parentheses can contain spaces and parentheses.
            let (_, tail) = stat.rsplit_once(") ")?;
            let parent = tail.split_whitespace().nth(1)?.parse::<u32>().ok()?;
            (parent == pid).then_some(child)
        })
        .find_map(|child| {
            let args = fs::read(format!("/proc/{child}/cmdline")).ok()?;
            let args: Vec<_> = String::from_utf8(args)
                .ok()?
                .split_terminator('\0')
                .map(str::to_owned)
                .collect();
            let found = if command == "chromium" {
                args.iter().any(|arg| arg == "--remote-debugging-port=0")
                    && args.iter().any(|arg| arg.starts_with("--user-data-dir="))
            } else {
                args.first()
                    .is_some_and(|arg| arg.rsplit('/').next() == Some(command))
            };
            if !found {
                return None;
            }
            Some((child, args))
        })
        .unwrap_or_else(|| panic!("missing {command} child of {pid}"))
}

fn proc_exists(pid: u32) -> bool {
    PathBuf::from(format!("/proc/{pid}")).exists()
}

async fn viewer(registrations: &mut mpsc::Receiver<Viewer>) -> Viewer {
    timeout(Duration::from_secs(5), registrations.recv())
        .await
        .unwrap()
        .unwrap()
}

async fn assert_document(client: &mut Client, before: &Value, target: &str) {
    let tabs = client.tool("browser_tabs", json!({})).await;
    assert_eq!(&tabs["tabs"], &before["tabs"], "tab targets were recreated");
    let state = client
        .tool(
            "browser_eval",
            json!({
                "target_id": target, "script": "[window.__takeoverState, document.body.textContent]"
            }),
        )
        .await;
    assert_eq!(state["target_id"], target);
    assert_eq!(state["result"], json!(["solved", "checkout-ready"]));
}

// Run with the Linux sandbox desktop stack (Chromium, sway, WayVNC):
// cargo test -p anyagents-mcp-browser --test guest_reconnect -- --include-ignored
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the sandbox desktop stack"]
async fn handback_reclaims_same_chromium_across_stdio_eof() {
    let (api, mut registrations, coordinator) = coordinator().await;
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("runtime");
    let mut worker = start_worker(&dir, &api).await;
    let pid = worker.id().unwrap();
    let mut client = Client::start(&dir, Some(&api)).await;
    let first_status = client.tool("browser_takeover", json!({})).await;
    assert_eq!(first_status["tunnel_active"], true, "{first_status}");
    let mut first = viewer(&mut registrations).await;
    let (chromium, args) = child(pid, "chromium");
    let (sway, _) = child(pid, "sway");
    let (vnc, _) = child(pid, "wayvnc");
    let port_file = PathBuf::from(
        args.iter()
            .find_map(|arg| arg.strip_prefix("--user-data-dir="))
            .unwrap(),
    )
    .join("DevToolsActivePort");
    let devtools = fs::read_to_string(&port_file).unwrap();
    let before = client
        .tool("browser_tabs", json!({"action":"new", "url":"about:blank"}))
        .await;
    assert!(before["tabs"].as_array().unwrap().len() > 1);
    let solved = client.tool("browser_eval", json!({
        "script":"window.__takeoverState = 'solved'; document.body.textContent = 'checkout-ready'; true"
    })).await;
    let target = solved["target_id"].as_str().unwrap();
    assert_eq!(first.request("POST", "/take", &first.token).unwrap(), 200);
    let local_ws = first.origin.replacen("http://", "ws://", 1);
    let (mut rfb, _) =
        tokio_tungstenite::connect_async(format!("{local_ws}/ws?token={}", first.token))
            .await
            .unwrap();
    let greeting = timeout(Duration::from_secs(3), rfb.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(greeting.into_data().starts_with(b"RFB "));
    assert_eq!(
        client.tool("browser_takeover_status", json!({})).await["holder"],
        "human"
    );
    client.eof().await;
    assert!(
        dir.join("mcp.sock").exists(),
        "active viewer must outlive relay EOF"
    );

    // The Coordinator forwards /release before ending the tunnel. Exercise
    // both signals with no MCP peer permanently attached to the worker.
    assert_eq!(
        first.request("POST", "/release", &first.token).unwrap(),
        200
    );
    first.end.take().unwrap().send(4411).unwrap();
    timeout(Duration::from_secs(5), first.closed.take().unwrap())
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(5), async {
        while let Some(Ok(message)) = rfb.next().await {
            if message.is_close() {
                break;
            }
        }
    })
    .await
    .expect("the authenticated RFB stream survived hand-back");
    wait_until(|| !proc_exists(vnc)).await;
    assert!(
        first.request("GET", "/", &first.token).is_err(),
        "old viewer must stop serving"
    );
    assert!(dir.join("mcp.sock").exists());
    assert!(proc_exists(chromium) && proc_exists(sway));
    assert_eq!(fs::read_to_string(&port_file).unwrap(), devtools);

    let mut client = Client::start(&dir, Some(&api)).await;
    let completed = client.tool("browser_takeover_status", json!({})).await;
    assert_eq!(completed["active"], false);
    assert_eq!(completed["holder"], "agent");
    assert!(completed["takeover_url"].is_null());
    assert_document(&mut client, &before, target).await;
    let second_status = client.tool("browser_takeover", json!({})).await;
    assert_eq!(second_status["tunnel_active"], true);
    let mut second = viewer(&mut registrations).await;
    assert_ne!(first.id, second.id);
    assert_ne!(first.token, second.token);
    assert_eq!(second.request("GET", "/", &first.token).unwrap(), 403);
    assert_document(&mut client, &before, target).await;
    let (second_vnc, _) = child(pid, "wayvnc");
    assert_ne!(vnc, second_vnc);
    assert_eq!(second.request("POST", "/take", &second.token).unwrap(), 200);

    let released = client
        .tool("browser_takeover_status", json!({"action":"release"}))
        .await;
    assert_eq!(released["released"], true);
    assert_eq!(released["browser_running"], true);
    assert!(
        timeout(Duration::from_secs(5), second.closed.take().unwrap())
            .await
            .unwrap()
            .unwrap(),
        "MCP release must unregister the old Coordinator tunnel"
    );
    assert!(second.request("GET", "/", &second.token).is_err());
    assert!(!proc_exists(second_vnc));
    client.eof().await;
    assert!(dir.join("mcp.sock").exists());
    let mut client = Client::start(&dir, Some(&api)).await;
    assert_document(&mut client, &before, target).await;
    assert_eq!(fs::read_to_string(&port_file).unwrap(), devtools);

    let close = client.tool("browser_close", json!({})).await;
    assert_eq!(close["was_running"], true);
    assert!(!proc_exists(chromium) && !proc_exists(sway));
    client.eof().await;
    assert!(
        timeout(Duration::from_secs(5), worker.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(!dir.join("mcp.sock").exists());
    coordinator.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires the sandbox desktop stack"]
async fn terminal_tunnel_without_handback_closes_detached_chromium() {
    let (api, mut registrations, coordinator) = coordinator().await;
    for code in [4410, 4411] {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("runtime");
        let mut worker = start_worker(&dir, &api).await;
        let pid = worker.id().unwrap();
        let mut client = Client::start(&dir, Some(&api)).await;
        assert_eq!(
            client.tool("browser_takeover", json!({})).await["tunnel_active"],
            true
        );
        let mut viewer = viewer(&mut registrations).await;
        let (chromium, _) = child(pid, "chromium");
        let (sway, _) = child(pid, "sway");
        let (vnc, _) = child(pid, "wayvnc");
        assert_eq!(viewer.request("POST", "/take", &viewer.token).unwrap(), 200);
        client.eof().await;
        viewer.end.take().unwrap().send(code).unwrap();
        assert!(
            timeout(Duration::from_secs(8), worker.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        assert!(!dir.join("mcp.sock").exists());
        assert!(![chromium, sway, vnc].into_iter().any(proc_exists));
        assert!(viewer.request("GET", "/", &viewer.token).is_err());
    }
    coordinator.abort();
}
