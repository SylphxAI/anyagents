//! `anyagents harness <config>`: a vendor double that obeys the adapter contract
//! end to end, so a repository test can prove the whole server path for a
//! harness without starting the vendor tool itself.
//!
//! The config file is the `adapter.json` the agent was created with:
//! `{"harness": "agy", "reply_with": "<binary> --data <dir> mcp --config <dir>"}`. Each user line is answered through the tool server
//! `reply_with` names, as an agent's model does — to whoever sent the message —
//! and the turn then ends with a `result` line.

use std::io::{BufRead, Write};

use anyagents_core::Error;

/// A line for whoever is watching the run, when `ANYAGENTS_SHARED_LOG` names a
/// file. Never a credential.
fn note(text: &str) {
    let Some(path) = std::env::var_os("ANYAGENTS_SHARED_LOG") else {
        return;
    };
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        use std::io::Write as _;
        let _ = writeln!(file, "harness: {text}");
    }
}

pub fn run(config: &std::path::Path) -> Result<(), Error> {
    note(&format!("start {}", config.display()));
    let text = std::fs::read_to_string(config).map_err(|err| Error::Message(err.to_string()))?;
    let value: serde_json::Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(err) => {
            note(&format!("bad config: {err}: {text}"));
            return Err(Error::Message(err.to_string()));
        }
    };
    let reply_with = value
        .get("reply_with")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let agent_file = value
        .get("agent_file")
        .and_then(serde_json::Value::as_str)
        .map(std::path::PathBuf::from);
    // Who the mailbox records as the sender, so the reply reaches it.
    let sender = value
        .get("sender")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    // A name the mailbox resolves; the double sends to it for a reply.
    let reply_target = value
        .get("reply_to")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let stdout = std::io::stdout();
    for line in std::io::stdin().lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(err) => {
                note(&format!("stdin ended: {err}"));
                break;
            }
        };
        let Ok(message) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        if message.get("type").and_then(serde_json::Value::as_str) != Some("user") {
            continue;
        }
        let user = message
            .get("text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let id = user
            .lines()
            .next()
            .and_then(|first| first.strip_prefix(anyagents_harness::MESSAGE_MARKER_PREFIX))
            .unwrap_or("")
            .to_string();
        let agent = agent_file
            .as_ref()
            .and_then(|path| std::fs::read_to_string(path).ok())
            .map(|name| name.trim().to_string())
            .unwrap_or_default();
        // The agent answers through the tool server, and the id proves which turn.
        let sent = tool_server::send(&reply_with, &agent, &sender, &reply_target, &id);
        note(&format!("answer {sent:?}"));
        let result = match sent {
            Ok(()) => serde_json::json!({"type": "result", "session_id": "sess-1"}),
            Err(reason) => {
                serde_json::json!({
                    "type": "result",
                    "session_id": "sess-1",
                    "subtype": "error_during_execution",
                    "is_error": true,
                    "result": reason
                })
            }
        };
        let mut out = stdout.lock();
        let _ = writeln!(out, "{result}");
        let _ = out.flush();
    }
    Ok(())
}

/// The least a model needs of an MCP client: start the tool server, initialize,
/// then call `send` once. Reads no mailbox and holds no credential.
mod tool_server {
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Command, Stdio};

    pub fn send(
        server: &str,
        agent: &str,
        sender: &str,
        target: &str,
        reply_to: &str,
    ) -> Result<(), String> {
        if server.is_empty() {
            return Err("no tool server".to_string());
        }
        let _ = agent;
        let mut parts = server.split_whitespace();
        let program = parts.next().ok_or("no program")?;
        // The tool server reads its capability from the deployment the server
        // exported to this process; a name would only be a check.
        let mut child = Command::new(program)
            .args(parts)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|err| err.to_string())?;
        let mut stdin = child.stdin.take().ok_or("no stdin")?;
        let mut stdout = BufReader::new(child.stdout.take().ok_or("no stdout")?);
        exchange(
            &mut stdin,
            &mut stdout,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"contract","version":"0"}}}"#,
        )?;
        writeln!(
            stdin,
            r#"{{"jsonrpc":"2.0","method":"notifications/initialized"}}"#
        )
        .map_err(|err| err.to_string())?;
        let call = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "send",
                "arguments": {"to": [target], "text": "pong", "sender": sender, "reply_to": reply_to}
            }
        });
        let done = exchange(&mut stdin, &mut stdout, &call.to_string())?;
        let _ = child.kill();
        let _ = child.wait();
        if done.to_string().contains("\"isError\":true") {
            return Err(done.to_string());
        }
        Ok(())
    }

    fn exchange(
        stdin: &mut impl Write,
        stdout: &mut impl BufRead,
        line: &str,
    ) -> Result<serde_json::Value, String> {
        writeln!(stdin, "{line}").map_err(|err| err.to_string())?;
        stdin.flush().map_err(|err| err.to_string())?;
        let mut reply = String::new();
        let read = stdout
            .read_line(&mut reply)
            .map_err(|err| err.to_string())?;
        if read == 0 {
            return Err("tool server closed".to_string());
        }
        serde_json::from_str(&reply).map_err(|err| err.to_string())
    }
}
