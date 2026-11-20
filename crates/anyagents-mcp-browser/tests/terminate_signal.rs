//! `SIGTERM` (how sandboxes and MCP hosts stop servers) must trigger the same
//! graceful shutdown as closing stdin: browser closed cleanly (cookie store
//! flushed) and the active profile synced, instead of dying mid-session.
#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

struct Server {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl Server {
    fn spawn(args: &[&str], envs: &[(&str, &str)]) -> Self {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_anyagents-mcp-browser"));
        cmd.args(args)
            .env_remove("ANYAGENTS_API_URL")
            .envs(envs.iter().copied())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn anyagents-mcp-browser");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        let mut server = Self {
            child,
            stdin,
            stdout,
            next_id: 0,
        };
        server.call(
            "initialize",
            serde_json::json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "signal-test", "version": "0"},
            }),
        );
        server.send(serde_json::json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        server
    }

    fn send(&mut self, msg: serde_json::Value) {
        writeln!(self.stdin, "{msg}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn call(&mut self, method: &str, params: serde_json::Value) -> serde_json::Value {
        self.next_id += 1;
        let id = self.next_id;
        self.send(
            serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}),
        );
        loop {
            let mut line = String::new();
            assert!(
                self.stdout.read_line(&mut line).unwrap() > 0,
                "server closed stdout"
            );
            let resp: serde_json::Value = serde_json::from_str(&line).unwrap();
            if resp["id"] == id {
                return resp;
            }
        }
    }

    fn tool(&mut self, name: &str, args: serde_json::Value) -> serde_json::Value {
        let resp = self.call(
            "tools/call",
            serde_json::json!({"name": name, "arguments": args}),
        );
        resp["result"]["structuredContent"].clone()
    }

    /// Send `SIGTERM` and wait for exit, returning `(exit code, stderr)`.
    fn terminate(mut self) -> (Option<i32>, String) {
        let pid = self.child.id().to_string();
        assert!(
            Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(60);
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "server did not exit after SIGTERM"
            );
            std::thread::sleep(Duration::from_millis(50));
        };
        let mut stderr = String::new();
        let _ = self
            .child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut stderr);
        (status.code(), stderr)
    }
}

#[test]
fn sigterm_shuts_down_gracefully_instead_of_dying() {
    let server = Server::spawn(&[], &[]);
    let (code, stderr) = server.terminate();
    assert_eq!(code, Some(0), "expected clean exit, stderr: {stderr}");
    assert!(stderr.contains("received SIGTERM"), "{stderr}");
}

/// Live check against a real local browser (opt-in: `ANYAGENTS_BROWSER_LIVE_TEST=1`).
#[test]
fn sigterm_flushes_cookies_of_a_live_browser_to_the_profile() {
    if std::env::var("ANYAGENTS_BROWSER_LIVE_TEST").ok().as_deref() != Some("1") {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf);
            let body = "<html><title>signal-lab</title><body>ok</body></html>";
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: text/html\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let profile_dir = dir.path().to_str().unwrap();
    let mode = std::env::var("ANYAGENTS_BROWSER_MODE").unwrap_or_else(|_| "headless".into());
    let mut server = Server::spawn(
        &["--profile-dir", profile_dir, "--browser-mode", &mode],
        &[],
    );
    let url = format!("http://127.0.0.1:{port}/");
    let nav = server.tool("browser_navigate", serde_json::json!({"url": url}));
    assert_eq!(nav["title"], "signal-lab", "{nav}");
    let set = server.tool(
        "browser_eval",
        serde_json::json!({"script": "document.cookie='sigterm_probe=1; max-age=31536000; path=/'; document.cookie"}),
    );
    assert_eq!(set["result"], "sigterm_probe=1", "{set}");
    let (code, stderr) = server.terminate();
    assert_eq!(code, Some(0), "{stderr}");

    // Cookie names are stored in plaintext columns of the SQLite cookie store.
    let cookies = dir.path().join("user-data/Default/Cookies");
    let bytes = std::fs::read(&cookies).unwrap();
    assert!(
        bytes.windows(13).any(|w| w == b"sigterm_probe"),
        "cookie was not flushed to {}",
        cookies.display()
    );
    assert!(
        dir.path()
            .join("user-data/SingletonLock")
            .symlink_metadata()
            .is_err(),
        "browser left running after SIGTERM"
    );
}
