//! End-to-end desktop session through the tool surface, on a real headless
//! Wayland desktop with a headful browser.
//!
//! Runs only with `ANYAGENTS_DESKTOP_HEADFUL=1`; needs `sway`, `wayvnc` and a
//! Chromium-family browser (`ANYAGENTS_CHROMIUM_BIN`). The profile is first
//! written the way the previous implementation launched its browser (a plain
//! Chromium process on the profile's `user-data/`, by default the same binary;
//! `ANYAGENTS_LEGACY_CHROMIUM_BIN` names a different one), so the test proves
//! that an existing profile opens with its cookies and local storage intact.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::browser::{BrowserRequest, execute};

const PAGE: &str = r#"<!doctype html><title>Fixture</title>
<h1>Sign in</h1>
<p id="state"></p>
<form method="post" action="/login">
  <label>Email <input name="email" aria-label="Email"></label>
  <label>Password <input name="password" type="password" aria-label="Password"></label>
  <button>Continue</button>
</form>
<script>
document.getElementById('state').textContent =
  'cookie=' + document.cookie + ' local=' + (localStorage.getItem('k') || 'none');
</script>"#;

const SET: &str = r#"<!doctype html><title>Set</title><script>
localStorage.setItem('k', 'legacy-ok');
// Closing the last window shuts the browser down gracefully, flushing cookies.
fetch('/stored').then(() => window.close());
</script>"#;

#[derive(Default)]
struct Seen {
    stored: bool,
    login_body: Option<String>,
}

async fn serve(seen: Arc<Mutex<Seen>>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = listener.accept().await {
            let seen = seen.clone();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 65536];
                let mut n = 0;
                loop {
                    let Ok(r) = s.read(&mut buf[n..]).await else {
                        return;
                    };
                    if r == 0 {
                        break;
                    }
                    n += r;
                    let text = String::from_utf8_lossy(&buf[..n]);
                    if let Some(end) = text.find("\r\n\r\n") {
                        let len = text[..end]
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                            })
                            .unwrap_or(0);
                        if n >= end + 4 + len {
                            break;
                        }
                    }
                }
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let path = req.split_whitespace().nth(1).unwrap_or("/").to_string();
                let (extra, body) = match path.as_str() {
                    "/set" => (
                        "Set-Cookie: legacy=yes; Max-Age=2592000; Path=/\r\n",
                        SET.to_string(),
                    ),
                    "/stored" => {
                        seen.lock().unwrap().stored = true;
                        ("", String::new())
                    }
                    "/login" => {
                        let body = req.split_once("\r\n\r\n").map(|x| x.1).unwrap_or("");
                        seen.lock().unwrap().login_body = Some(body.to_string());
                        (
                            "",
                            "<!doctype html><title>Welcome</title><h1>Signed in</h1>".into(),
                        )
                    }
                    _ => ("", PAGE.to_string()),
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = s.write_all(resp.as_bytes()).await;
            });
        }
    });
    format!("http://127.0.0.1:{}/", addr.port())
}

/// Write the profile the way the previous implementation's browser did.
async fn write_legacy_profile(user_data: &Path, url: &str, seen: &Arc<Mutex<Seen>>) {
    let bin = std::env::var("ANYAGENTS_LEGACY_CHROMIUM_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| super::session::browser_binary());
    std::fs::create_dir_all(user_data).unwrap();
    let mut child = tokio::process::Command::new(bin)
        .args([
            "--headless=new",
            "--no-sandbox",
            "--no-first-run",
            "--password-store=basic",
            "--use-mock-keychain",
        ])
        .arg(format!("--user-data-dir={}", user_data.display()))
        .arg(format!("{url}set"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    for _ in 0..200 {
        if seen.lock().unwrap().stored {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        seen.lock().unwrap().stored,
        "legacy browser stored its state"
    );
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("legacy browser exits after its last window closes")
        .unwrap();
    assert!(status.success(), "legacy browser exit: {status}");
}

async fn call(data: &Path, req: Value) -> Value {
    let req: BrowserRequest = serde_json::from_value(req).unwrap();
    let action = req.action.clone();
    execute(data, "agent-a", req)
        .await
        .unwrap_or_else(|e| panic!("{action}: {e}"))
}

fn line_ref(snapshot: &Value, name: &str) -> String {
    let text = snapshot["snapshot"].as_str().unwrap();
    let id = snapshot["snapshot_id"].as_str().unwrap();
    let line = text
        .lines()
        .find(|l| l.contains(name))
        .unwrap_or_else(|| panic!("{name} in {text}"));
    let short = line.rsplit("[@").next().unwrap().trim_end_matches(']');
    format!("@{id}:{short}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn existing_profile_opens_with_logins_and_every_operation_works() {
    if std::env::var("ANYAGENTS_DESKTOP_HEADFUL").as_deref() != Ok("1") {
        eprintln!("skipped: set ANYAGENTS_DESKTOP_HEADFUL=1");
        return;
    }
    let data = tempfile::tempdir().unwrap();
    let data = data.path();
    let seen = Arc::new(Mutex::new(Seen::default()));
    let url = serve(seen.clone()).await;
    let dir = super::profile_dir(data, "agent-a");
    write_legacy_profile(&dir.join("user-data"), &url, &seen).await;
    // The previous implementation's bookkeeping, as an upgraded host has it.
    std::fs::write(dir.join("runtime.json"), r#"{"pid": 0}"#).unwrap();
    use anyagents_core::CredentialStore as _;
    anyagents_core::FileCredentialStore::new(data)
        .write("fixture/password", b"s3cret-value")
        .unwrap();

    // The same user-data directory opens with its cookie and local storage.
    let opened = call(data, json!({"action": "open", "url": url})).await;
    assert!(!dir.join("runtime.json").exists(), "legacy state retired");
    let read = call(data, json!({"action": "read"})).await;
    let text = read["text"].as_str().unwrap();
    assert!(text.contains("cookie=legacy=yes"), "{text}");
    assert!(text.contains("local=legacy-ok"), "{text}");
    assert!(opened["takeover_url"].as_str().is_some());

    // Snapshot refs, a plain fill, and a secret_ref fill with submit.
    let snap = call(data, json!({"action": "snapshot"})).await;
    assert!(
        snap["snapshot"]
            .as_str()
            .unwrap()
            .contains("heading \"Sign in\"")
    );
    let email = line_ref(&snap, "textbox \"Email\"");
    let password = line_ref(&snap, "textbox \"Password\"");
    call(
        data,
        json!({"action": "fill", "selector": email, "text": "a@example.com"}),
    )
    .await;
    let filled = call(
        data,
        json!({"action": "fill", "selector": password, "secret_ref": "secret://fixture/password"}),
    )
    .await;
    assert!(
        !filled.to_string().contains("s3cret-value"),
        "secret never returned"
    );
    let go = line_ref(&snap, "button \"Continue\"");
    call(
        data,
        json!({"action": "click", "selector": go, "wait_ms": 1500}),
    )
    .await;
    let body = seen
        .lock()
        .unwrap()
        .login_body
        .clone()
        .expect("form submitted");
    assert!(body.contains("email=a%40example.com"), "{body}");
    assert!(body.contains("password=s3cret-value"), "{body}");

    // Refs from before the navigation are stale.
    let stale: BrowserRequest =
        serde_json::from_value(json!({"action": "click", "selector": email})).unwrap();
    let err = execute(data, "agent-a", stale)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("STALE_REF"), "{err}");

    // Full-desktop screenshot through the computer surface, same desktop.
    let shot = super::desktop::execute(data, "agent-a", json!({"action": "screenshot"}))
        .await
        .unwrap();
    assert_eq!(shot["mime_type"], "image/png");
    assert!(shot["width"].as_u64().unwrap() >= 320);
    let zoom = super::desktop::execute(
        data,
        "agent-a",
        json!({"action": "zoom", "region": {"x": 0, "y": 0, "width": 200, "height": 100}}),
    )
    .await
    .unwrap();
    assert_eq!(
        (zoom["width"].as_u64(), zoom["height"].as_u64()),
        (Some(200), Some(100))
    );

    // Desktop keyboard reaches the browser chrome: type a URL into the bar.
    super::desktop::execute(data, "agent-a", json!({"action": "key", "key": "ctrl+l"}))
        .await
        .unwrap();
    super::desktop::execute(
        data,
        "agent-a",
        json!({"action": "type", "text": format!("{url}?typed=1\n")}),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let read = call(data, json!({"action": "read"})).await;
    assert!(
        read["url"].as_str().unwrap().ends_with("?typed=1"),
        "{read}"
    );

    // The takeover page is served behind its token; a person holding
    // control suspends the agent until they hand back.
    let takeover = call(data, json!({"action": "takeover", "reason": "fixture"})).await;
    let page = url::Url::parse(takeover["takeover_url"].as_str().unwrap()).unwrap();
    let live = super::session::running(data, "agent-a").unwrap();
    live.session.gate().take(Some("tester".into()));
    let blocked: BrowserRequest = serde_json::from_value(json!({"action": "read"})).unwrap();
    let err = execute(data, "agent-a", blocked)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("HUMAN_IN_CONTROL"), "{err}");
    let waiter = {
        let data = data.to_path_buf();
        tokio::spawn(async move {
            call(&data, json!({"action": "wait_for_user", "wait_ms": 20000})).await
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(live.session.gate().release(Some("done".into())));
    assert_eq!(waiter.await.unwrap()["completed"], true);
    drop(live);
    let addr = page.socket_addrs(|| None).unwrap()[0];
    let mut tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let q = page.query().unwrap();
    tcp.write_all(format!("GET /?{q} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut resp = String::new();
    tcp.read_to_string(&mut resp).await.unwrap();
    assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");

    // Tabs.
    let tabs = call(
        data,
        json!({"action": "tabs", "tab_action": "new", "url": url}),
    )
    .await;
    assert_eq!(tabs["tabs"].as_array().unwrap().len(), 2);
    let id = tabs["target_id"].as_str().unwrap().to_string();
    let tabs = call(
        data,
        json!({"action": "tabs", "tab_action": "close", "target_id": id}),
    )
    .await;
    assert_eq!(tabs["tabs"].as_array().unwrap().len(), 1);

    // Close and reopen: the profile keeps its login.
    let closed = call(data, json!({"action": "close"})).await;
    assert_eq!(closed["was_running"], true);
    call(data, json!({"action": "open", "url": url})).await;
    let read = call(data, json!({"action": "read"})).await;
    assert!(
        read["text"].as_str().unwrap().contains("cookie=legacy=yes"),
        "{read}"
    );
    call(data, json!({"action": "close"})).await;
}
