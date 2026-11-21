//! Cloud harness transcript event normalization for poll and webhook payloads.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::spiron_poll;

/// Normalized unmasked transcript event captured from a cloud harness poll or webhook.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloudTranscriptEvent {
    pub seq: i64,
    pub timestamp_ms: i64,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    pub text: String,
    pub raw_json: String,
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

fn cloud_non_empty<'a>(val: &'a Value, key: &str) -> Option<&'a str> {
    val.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

fn cloud_val_to_text(val: &Value) -> String {
    match val {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn cloud_extract_ts_ms(item: &Value, payload: &Value) -> i64 {
    for src in [payload, item] {
        for key in ["timestamp_ms", "created_at_ms", "timestamp", "created_at"] {
            if let Some(v) = src.get(key) {
                if let Some(n) = v.as_i64()
                    && n > 0
                {
                    return if n < 10_000_000_000 { n * 1000 } else { n };
                }
                if let Some(s) = v.as_str()
                    && let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s.trim())
                {
                    return dt.timestamp_millis();
                }
            }
        }
    }
    anyagents_core::now_ms()
}

/// Normalize one Spiron or generic cloud event payload into an unmasked
/// [`CloudTranscriptEvent`].
pub(crate) fn parse_cloud_transcript_event(
    event: Option<&str>,
    id: Option<&str>,
    next_seq: &mut i64,
    call_names: &mut HashMap<String, String>,
    item: &Value,
) -> Option<CloudTranscriptEvent> {
    let payload = item.get("data").filter(|v| v.is_object()).unwrap_or(item);
    let kind = event
        .filter(|s| !s.is_empty())
        .or_else(|| cloud_non_empty(item, "event"))
        .or_else(|| cloud_non_empty(item, "event_type"))
        .or_else(|| cloud_non_empty(item, "type"))
        .or_else(|| cloud_non_empty(payload, "type"))
        .unwrap_or("raw")
        .trim()
        .to_ascii_lowercase();

    let raw_json = item.to_string();
    let explicit_seq = id
        .and_then(|s| s.trim().parse::<i64>().ok())
        .filter(|&n| n > 0)
        .or_else(|| {
            item.get("seq")
                .or_else(|| item.get("idx"))
                .or_else(|| payload.get("seq"))
                .or_else(|| payload.get("idx"))
                .and_then(Value::as_i64)
                .filter(|&n| n > 0)
        });
    let seq = if let Some(n) = explicit_seq {
        *next_seq = (*next_seq).max(n);
        n
    } else {
        *next_seq = next_seq.saturating_add(1);
        *next_seq
    };
    let timestamp_ms = cloud_extract_ts_ms(item, payload);

    let (role, tool_name, tool_call_id, text) = match kind.as_str() {
        "thinking" | "reasoning" | "thought" | "thinking.delta" | "reasoning.delta" => {
            let txt = cloud_non_empty(payload, "thinking")
                .or_else(|| cloud_non_empty(payload, "delta"))
                .or_else(|| cloud_non_empty(payload, "text"))
                .or_else(|| cloud_non_empty(payload, "content"))
                .map(str::to_string)
                .unwrap_or_else(|| raw_json.clone());
            ("thinking", None, None, txt)
        }
        "tool_call" | "tool_use" | "tool.started" | "tool.call" | "function_call" => {
            let name = cloud_non_empty(payload, "tool_name")
                .or_else(|| cloud_non_empty(payload, "toolName"))
                .or_else(|| cloud_non_empty(payload, "name"))
                .or_else(|| cloud_non_empty(payload, "tool"))
                .map(str::to_string);
            let cid = cloud_non_empty(payload, "tool_call_id")
                .or_else(|| cloud_non_empty(payload, "call_id"))
                .or_else(|| cloud_non_empty(payload, "callId"))
                .or_else(|| cloud_non_empty(payload, "id"))
                .map(str::to_string);
            if let (Some(c), Some(n)) = (&cid, &name) {
                call_names.insert(c.clone(), n.clone());
            }
            let args = payload
                .get("arguments")
                .or_else(|| payload.get("args"))
                .or_else(|| payload.get("input"))
                .map(cloud_val_to_text)
                .unwrap_or_default();
            let label = name.as_deref().unwrap_or("tool");
            let txt = if args.is_empty() {
                format!("[tool_call:{label}]")
            } else {
                format!("[tool_call:{label}] {args}")
            };
            ("tool_call", name, cid, txt)
        }
        "tool_result" | "tool.completed" | "tool.result" | "function_call_output" => {
            let cid = cloud_non_empty(payload, "tool_call_id")
                .or_else(|| cloud_non_empty(payload, "call_id"))
                .or_else(|| cloud_non_empty(payload, "callId"))
                .or_else(|| cloud_non_empty(payload, "tool_use_id"))
                .map(str::to_string);
            let name = cloud_non_empty(payload, "tool_name")
                .or_else(|| cloud_non_empty(payload, "toolName"))
                .or_else(|| cloud_non_empty(payload, "name"))
                .or_else(|| cloud_non_empty(payload, "tool"))
                .map(str::to_string)
                .or_else(|| cid.as_deref().and_then(|c| call_names.get(c).cloned()));
            let body = payload
                .get("output")
                .or_else(|| payload.get("result"))
                .or_else(|| payload.get("content"))
                .or_else(|| payload.get("text"))
                .map(cloud_val_to_text)
                .unwrap_or_default();
            let txt = if let Some(ref tname) = name {
                if body.is_empty() {
                    format!("[tool_result:{tname}]")
                } else {
                    format!("[tool_result:{tname}] {body}")
                }
            } else if body.is_empty() {
                raw_json.clone()
            } else {
                body
            };
            ("tool_result", name, cid, txt)
        }
        "text" | "delta" | "message.delta" | "message.completed" | "assistant" | "done" => {
            let txt = cloud_non_empty(payload, "delta")
                .map(str::to_string)
                .or_else(|| spiron_poll::reply_text(payload))
                .or_else(|| payload.get("message").and_then(spiron_poll::reply_text))
                .or_else(|| spiron_poll::reply_text(item))
                .unwrap_or_else(|| raw_json.clone());
            ("assistant", None, None, txt)
        }
        "error" | "message.failed" | "turn.failed" | "run.failed" | "turn_failed" => {
            let txt = spiron_poll::error_text(payload)
                .or_else(|| spiron_poll::error_text(item))
                .unwrap_or_else(|| raw_json.clone());
            ("system", None, None, txt)
        }
        "task.updated" | "message.updated" | "turn.completed" | "run.completed"
        | "turn_complete" => {
            let task = payload.get("task").unwrap_or(payload);
            if let Some(reply) = spiron_poll::reply_text(task)
                .or_else(|| spiron_poll::reply_text(payload))
                .or_else(|| spiron_poll::reply_text(item))
            {
                ("assistant", None, None, reply)
            } else if let Some(err) = spiron_poll::error_text(task)
                .or_else(|| spiron_poll::error_text(payload))
                .or_else(|| spiron_poll::error_text(item))
            {
                ("system", None, None, err)
            } else {
                let status = cloud_non_empty(task, "status")
                    .or_else(|| cloud_non_empty(payload, "status"))
                    .unwrap_or(&kind);
                ("system", None, None, format!("{kind}:{status}"))
            }
        }
        _ => {
            let txt = cloud_non_empty(payload, "delta")
                .or_else(|| cloud_non_empty(payload, "text"))
                .or_else(|| cloud_non_empty(payload, "content"))
                .map(str::to_string)
                .unwrap_or_else(|| raw_json.clone());
            ("raw", None, None, txt)
        }
    };

    Some(CloudTranscriptEvent {
        seq,
        timestamp_ms,
        role: role.to_string(),
        tool_name,
        tool_call_id,
        text,
        raw_json,
    })
}

pub(super) fn parse_webhook_transcript_events(
    event_type: &str,
    cursor: Option<&str>,
    payload: &Value,
) -> Vec<CloudTranscriptEvent> {
    let mut call_names = HashMap::new();
    let raw_bytes = payload.to_string();
    let stable_fallback = ((fnv1a64(
        cursor
            .filter(|s| !s.trim().is_empty())
            .unwrap_or(raw_bytes.as_str())
            .as_bytes(),
    ) % 899_999) as i64)
        + 100_000;
    let mut next_seq = stable_fallback - 1;
    let id = cursor.or_else(|| payload.get("id").and_then(Value::as_str));
    parse_cloud_transcript_event(
        Some(event_type),
        id,
        &mut next_seq,
        &mut call_names,
        payload,
    )
    .into_iter()
    .collect()
}
