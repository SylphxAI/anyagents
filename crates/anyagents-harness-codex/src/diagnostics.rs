//! Bounded stderr diagnostics, assistant message extraction, and Codex fault/usage helpers.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::ChildStderr;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use anyagents_harness::TurnUsage;
use serde_json::{Value, json};

const MAX_STDERR_BYTES: usize = 4096;

#[derive(Clone, Default)]
pub(crate) struct StderrBuf(Arc<Mutex<String>>);

impl StderrBuf {
    pub(crate) fn spawn(&self, stderr: ChildStderr) -> Option<JoinHandle<()>> {
        let buf = Arc::clone(&self.0);
        thread::Builder::new()
            .name("anyagents-codex-stderr".into())
            .spawn(move || {
                let mut reader = BufReader::new(stderr);
                let mut line = String::new();
                while let Ok(n) = reader.read_line(&mut line) {
                    if n == 0 {
                        break;
                    }
                    let mut g = buf.lock().unwrap_or_else(|e| e.into_inner());
                    if g.len() < MAX_STDERR_BYTES {
                        let mut end = (MAX_STDERR_BYTES - g.len()).min(line.len());
                        while end > 0 && !line.is_char_boundary(end) {
                            end -= 1;
                        }
                        g.push_str(&line[..end]);
                    }
                    line.clear();
                }
            })
            .ok()
    }

    pub(crate) fn tail(&self) -> Option<String> {
        let g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let trimmed = g.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    }
}

pub(crate) fn extract_agent_message_text(item: &Value) -> Option<String> {
    let kind = item.get("type").and_then(Value::as_str).unwrap_or("");
    if kind != "agentMessage" && kind != "agent_message" {
        return None;
    }
    if let Some(t) = item.get("text").and_then(Value::as_str)
        && !t.trim().is_empty()
    {
        return Some(t.to_string());
    }
    let arr = item.get("content").and_then(Value::as_array)?;
    let out: String = arr
        .iter()
        .filter_map(|p| p.get("text").and_then(Value::as_str))
        .collect();
    (!out.trim().is_empty()).then_some(out)
}

pub(crate) fn persist_codex_fault_error(config_dir: Option<&Path>, err: Option<&str>) {
    let Some(dir) = config_dir else {
        return;
    };
    let path = dir.join(".anyagents-last-error.json");
    match err.map(str::trim).filter(|s| !s.is_empty()) {
        Some(msg) => {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| i64::try_from(d.as_millis()).unwrap_or(0))
                .unwrap_or(0);
            let payload = json!({
                "error": msg,
                "quota_reset_at_ms": null,
                "timestamp_ms": ts,
            });
            if let Err(err) = anyagents_harness::atomic_write_in_existing_dir(
                &path,
                payload.to_string().as_bytes(),
                0o664,
            ) {
                eprintln!("cannot persist {}: {err}", path.display());
            }
        }
        None => {
            let _ = std::fs::remove_file(&path);
        }
    }
}

pub(crate) fn usage_from_codex(params: &Value) -> TurnUsage {
    let last = params
        .pointer("/tokenUsage/last")
        .or_else(|| params.pointer("/tokenUsage/total"));
    let u64_f = |k: &str| {
        last.and_then(|v| v.get(k))
            .and_then(Value::as_u64)
            .unwrap_or(0)
    };
    TurnUsage {
        input_tokens: u64_f("inputTokens"),
        output_tokens: u64_f("outputTokens"),
        cache_read_tokens: u64_f("cachedInputTokens"),
        cache_write_tokens: u64_f("cacheWriteInputTokens"),
        reported_cost_micros: None,
    }
}

pub(crate) fn extract_turn_error(params: &Value) -> Option<String> {
    for ptr in [
        "/turn/error/message",
        "/error/message",
        "/turn/error",
        "/error",
        "/message",
    ] {
        if let Some(s) = params
            .pointer(ptr)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return Some(s.to_string());
        }
    }
    for ptr in ["/turn/error", "/error"] {
        if let Some(obj) = params.pointer(ptr).filter(|v| v.is_object()) {
            let raw = obj.to_string();
            if raw != "{}" {
                return Some(raw);
            }
        }
    }
    None
}
