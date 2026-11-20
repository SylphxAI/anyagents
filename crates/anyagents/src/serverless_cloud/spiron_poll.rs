//! Bounded receipt event replay for the Spiron HTTP driver.

use std::io::{BufRead, BufReader, Read};
use std::time::Duration;

use serde_json::Value;

use super::{
    CloudTurnState, build_http_agent, extract_error_string, extract_result_string, map_status_str,
};

const MAX_EVENT_BYTES: u64 = 2 * 1024 * 1024;
// The coordinator gives the whole receipt poll four seconds. Do not wait for
// an open event stream to close before returning a durable or terminal result.
const EVENT_TIMEOUT: Duration = Duration::from_millis(750);

pub(super) struct PollEvents {
    pub(super) cursor: Option<String>,
    pub(super) terminal: Option<CloudTurnState>,
    pub(super) reply: Option<String>,
    pub(super) error: Option<String>,
    pub(super) usage: Option<Value>,
    pub(super) deltas: Vec<String>,
    pub(super) snapshot: bool,
}

impl PollEvents {
    pub(super) fn new(cursor: Option<String>) -> Self {
        Self {
            cursor,
            terminal: None,
            reply: None,
            error: None,
            usage: None,
            deltas: Vec::new(),
            snapshot: false,
        }
    }

    pub(super) fn read(&mut self, url: &str, headers: &[(String, String)]) {
        let agent = build_http_agent(EVENT_TIMEOUT);
        let mut req = agent
            .get(url)
            .header("accept", "text/event-stream, application/json");
        for (k, v) in headers {
            if !k.eq_ignore_ascii_case("accept") {
                req = req.header(k.as_str(), v.as_str());
            }
        }
        if let Some(cursor) = self.cursor.as_deref().filter(|c| !c.is_empty()) {
            // Spiron resumes SSE with Last-Event-ID; legacy JSON snapshots use after.
            req = req.query("after", cursor);
            if cursor.parse::<usize>().is_ok() {
                req = req.header("last-event-id", cursor);
            }
        }
        let Ok(mut resp) = req.call() else { return };
        if !resp.status().is_success() {
            return;
        }
        let is_sse = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(';').next())
            .is_some_and(|v| v.trim().eq_ignore_ascii_case("text/event-stream"));
        let body = resp.body_mut().as_reader();
        if is_sse {
            self.read_sse(BufReader::new(body.take(MAX_EVENT_BYTES)));
        } else {
            // Retain compatibility with earlier finite JSON event snapshots.
            let mut bytes = Vec::new();
            if body
                .take(MAX_EVENT_BYTES + 1)
                .read_to_end(&mut bytes)
                .is_ok()
                && bytes.len() as u64 <= MAX_EVENT_BYTES
                && let Ok(doc) = serde_json::from_slice::<Value>(&bytes)
            {
                self.snapshot = true;
                for item in doc
                    .get("events")
                    .or_else(|| doc.get("items"))
                    .and_then(Value::as_array)
                    .or_else(|| doc.as_array())
                    .into_iter()
                    .flatten()
                {
                    self.apply(None, None, item);
                    if self.terminal.is_some() {
                        return;
                    }
                }
                if let Some(cursor) = field(&doc, "next_cursor").or_else(|| field(&doc, "cursor")) {
                    self.cursor = Some(cursor.into());
                }
            }
        }
    }

    fn read_sse(&mut self, mut body: impl BufRead) {
        let mut line = String::new();
        let mut event = String::new();
        let mut id = String::new();
        let mut data = String::new();
        while body.read_line(&mut line).is_ok_and(|n| n > 0) {
            let field_line = line.trim_end_matches(['\r', '\n']);
            if field_line.is_empty() {
                if let Ok(item) = serde_json::from_str::<Value>(&data) {
                    self.apply(Some(&event), Some(&id), &item);
                    if self.terminal.is_some() {
                        return;
                    }
                }
                event.clear();
                id.clear();
                data.clear();
            } else if let Some((key, val)) = field_line.split_once(':') {
                let val = val.strip_prefix(' ').unwrap_or(val);
                match key {
                    "event" => {
                        event.clear();
                        event.push_str(val);
                    }
                    "id" if !val.contains('\0') => {
                        id.clear();
                        id.push_str(val);
                    }
                    "data" => {
                        data.push_str(val);
                        data.push('\n');
                    }
                    _ => {}
                }
            }
            line.clear();
        }
    }

    fn apply(&mut self, event: Option<&str>, id: Option<&str>, item: &Value) {
        let payload = item.get("data").filter(|v| v.is_object()).unwrap_or(item);
        let cursor = match id {
            Some(sse_id) => Some(sse_id).filter(|s| !s.is_empty()),
            None => field(item, "id").or_else(|| field(item, "cursor")),
        };
        if let Some(cursor) = cursor {
            self.cursor = Some(cursor.into());
        }
        let event = event
            .filter(|s| !s.is_empty())
            .or_else(|| field(item, "event"))
            .or_else(|| field(item, "type"));
        match event {
            Some("message.completed") => {
                // A completion lacking its own reply should not mask a later failure.
                if let Some(reply) = reply_text(payload)
                    .or_else(|| payload.get("message").and_then(reply_text))
                    .or_else(|| reply_text(item))
                {
                    self.reply = Some(reply);
                    self.terminal = Some(CloudTurnState::Completed);
                }
            }
            Some("task.updated") => {
                let task = payload.get("task").unwrap_or(payload);
                let status =
                    map_status_str(field(task, "status").or_else(|| field(payload, "status")));
                if matches!(status, CloudTurnState::Failed | CloudTurnState::Cancelled) {
                    self.terminal = Some(status);
                    self.error = error_text(task)
                        .or_else(|| error_text(payload))
                        .or_else(|| error_text(item));
                } else if status == CloudTurnState::Completed
                    && let Some(reply) = reply_text(task)
                {
                    self.terminal = Some(status);
                    self.reply = Some(reply);
                }
            }
            _ => {
                if let Some(delta) = field(payload, "delta").or_else(|| field(payload, "text")) {
                    self.deltas.push(delta.into());
                }
                if let Some(reply) = field(payload, "reply").or_else(|| field(payload, "result")) {
                    self.reply = Some(reply.into());
                }
            }
        }
        if let Some(usage) = payload.get("usage").or_else(|| item.get("usage")) {
            self.usage = Some(usage.clone());
        }
    }
}

fn field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

pub(super) fn reply_text(value: &Value) -> Option<String> {
    extract_result_string(value).or_else(|| {
        let output = value.get("output")?;
        if field(output, "type") != Some("text") {
            return None;
        }
        nonblank(output, "text").map(str::to_owned)
    })
}

pub(super) fn error_text(value: &Value) -> Option<String> {
    // Keep exact provider text (including newlines and surrounding whitespace).
    let failure = value.get("failure").unwrap_or(&Value::Null);
    nonblank(value, "error")
        .or_else(|| nonblank(failure, "message"))
        .or_else(|| {
            value
                .get("error")
                .and_then(|error| nonblank(error, "message"))
        })
        .or_else(|| nonblank(value, "error_text"))
        .or_else(|| nonblank(failure, "reason"))
        .map(str::to_owned)
        .or_else(|| extract_error_string(value))
}

fn nonblank<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    field(value, key).filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests;
