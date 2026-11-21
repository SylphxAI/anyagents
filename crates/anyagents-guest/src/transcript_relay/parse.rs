//! Multi-harness JSONL event normalization (lossless & unmasked) and timestamp parsing
//! for [`super::TranscriptRelay`].

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use super::{GuestTranscriptEntry, SourceFormat};

const MAX_RAW_JSON_BYTES: usize = 1024 * 1024;
const MAX_TEXT_BYTES: usize = 64 * 1024;
const MAX_SUB_EVENTS_PER_LINE: usize = 1024;

struct DraftEvent {
    role: &'static str,
    tool_name: Option<String>,
    tool_call_id: Option<String>,
    text: String,
    session_id: Option<String>,
}

fn cap_text(text: String) -> String {
    if text.len() <= MAX_TEXT_BYTES {
        return text;
    }
    let mut end = MAX_TEXT_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...[truncated]", &text[..end])
}

fn cap_raw_json(raw_json: &str) -> String {
    if raw_json.len() <= MAX_RAW_JSON_BYTES {
        return raw_json.to_string();
    }
    let mut end = 4096.min(raw_json.len());
    while end > 0 && !raw_json.is_char_boundary(end) {
        end -= 1;
    }
    json!({
        "truncated": true,
        "original_bytes": raw_json.len(),
        "preview": &raw_json[..end],
    })
    .to_string()
}

pub(super) fn normalize_json_event(
    value: &Value,
    raw_json: &str,
    source: &str,
    format: SourceFormat,
    base_unit_seq: i64,
    active_session_id: Option<&str>,
    call_names: &mut HashMap<String, String>,
) -> Vec<GuestTranscriptEntry> {
    let ts = extract_timestamp_ms(value).unwrap_or_else(now_unix_ms);
    let entry_sid = extract_session_id(value).or_else(|| active_session_id.map(str::to_string));

    let mut drafts = match format {
        SourceFormat::Agy => normalize_agy(value, call_names),
        SourceFormat::Claude => normalize_claude_or_message(value, call_names),
        SourceFormat::Codex => normalize_codex(value, call_names),
        SourceFormat::SpironJsonl => normalize_spiron(value, call_names),
        SourceFormat::GenericJsonl => normalize_generic(value, call_names),
    };

    if drafts.is_empty() {
        let fallback_text = extract_fallback_text(value).unwrap_or_else(|| raw_json.to_string());
        drafts.push(DraftEvent {
            role: "raw",
            tool_name: None,
            tool_call_id: None,
            text: fallback_text,
            session_id: None,
        });
    }

    if drafts.len() > MAX_SUB_EVENTS_PER_LINE {
        drafts.truncate(MAX_SUB_EVENTS_PER_LINE);
    }

    let safe_raw = cap_raw_json(raw_json);
    let base_seq = base_unit_seq.saturating_mul(MAX_SUB_EVENTS_PER_LINE as i64);
    drafts
        .into_iter()
        .enumerate()
        .map(|(idx, d)| GuestTranscriptEntry {
            source: source.to_string(),
            seq: base_seq.saturating_add(idx as i64),
            timestamp_ms: ts,
            role: d.role.to_string(),
            session_id: d.session_id.or_else(|| entry_sid.clone()),
            tool_name: d.tool_name,
            tool_call_id: d.tool_call_id,
            text: cap_text(d.text),
            raw_json: safe_raw.clone(),
        })
        .collect()
}

fn normalize_agy(value: &Value, call_names: &mut HashMap<String, String>) -> Vec<DraftEvent> {
    let mut out = Vec::new();
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    let kind_upper = kind.to_ascii_uppercase();
    let source = value
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase();

    if kind_upper == "USER_INPUT" || kind_upper.ends_with("_USER_INPUT") {
        let text = non_empty_str(value, "content")
            .or_else(|| non_empty_str(value, "text"))
            .unwrap_or_default()
            .to_string();
        if !text.is_empty() {
            out.push(DraftEvent {
                role: "user",
                tool_name: None,
                tool_call_id: None,
                text,
                session_id: None,
            });
        }
        return out;
    }

    if kind_upper == "PLANNER_RESPONSE" || kind_upper.ends_with("_PLANNER_RESPONSE") {
        if let Some(thinking) = non_empty_str(value, "thinking") {
            out.push(DraftEvent {
                role: "thinking",
                tool_name: None,
                tool_call_id: None,
                text: thinking.to_string(),
                session_id: None,
            });
        }
        if let Some(calls) = value.get("tool_calls").and_then(Value::as_array) {
            for call in calls {
                let name = non_empty_str(call, "name")
                    .or_else(|| non_empty_str(call, "tool_name"))
                    .or_else(|| call.pointer("/function/name").and_then(Value::as_str))
                    .map(str::to_string);
                let call_id = non_empty_str(call, "id")
                    .or_else(|| non_empty_str(call, "call_id"))
                    .map(str::to_string);
                if let (Some(cid), Some(tname)) = (&call_id, &name) {
                    call_names.insert(cid.clone(), tname.clone());
                }
                let args = call
                    .get("arguments")
                    .or_else(|| call.get("args"))
                    .or_else(|| call.get("input"))
                    .or_else(|| call.pointer("/function/arguments"))
                    .map(value_to_text)
                    .unwrap_or_default();
                let label = name.as_deref().unwrap_or("tool");
                let text = if args.is_empty() {
                    format!("[tool_call:{label}]")
                } else {
                    format!("[tool_call:{label}] {args}")
                };
                out.push(DraftEvent {
                    role: "tool_call",
                    tool_name: name,
                    tool_call_id: call_id,
                    text,
                    session_id: None,
                });
            }
        }
        if let Some(content) = non_empty_str(value, "content") {
            out.push(DraftEvent {
                role: "assistant",
                tool_name: None,
                tool_call_id: None,
                text: content.to_string(),
                session_id: None,
            });
        }
        return out;
    }

    // Tool result / execution steps in AGY transcripts.
    let call_id = non_empty_str(value, "tool_call_id")
        .or_else(|| non_empty_str(value, "call_id"))
        .map(str::to_string);
    let direct_tool = non_empty_str(value, "tool_name")
        .or_else(|| non_empty_str(value, "tool"))
        .map(str::to_string);
    let is_tool_step = kind_upper.contains("TOOL")
        || kind_upper.contains("COMMAND")
        || kind_upper.contains("FILE")
        || kind_upper.contains("GREP")
        || kind_upper.contains("FIND")
        || kind_upper.contains("MCP")
        || call_id.is_some()
        || direct_tool.is_some();
    if is_tool_step {
        let tool_name = direct_tool
            .or_else(|| {
                call_id
                    .as_deref()
                    .and_then(|cid| call_names.get(cid).cloned())
            })
            .or_else(|| (!kind.is_empty()).then(|| kind.to_ascii_lowercase()));
        let text = non_empty_str(value, "output")
            .or_else(|| non_empty_str(value, "content"))
            .or_else(|| non_empty_str(value, "result"))
            .map(str::to_string)
            .unwrap_or_else(|| value.to_string());
        out.push(DraftEvent {
            role: "tool_result",
            tool_name,
            tool_call_id: call_id,
            text,
            session_id: None,
        });
        return out;
    }

    if source == "SYSTEM"
        && let Some(content) = non_empty_str(value, "content")
    {
        out.push(DraftEvent {
            role: "system",
            tool_name: None,
            tool_call_id: None,
            text: content.to_string(),
            session_id: None,
        });
    }
    out
}

fn normalize_claude_or_message(
    value: &Value,
    call_names: &mut HashMap<String, String>,
) -> Vec<DraftEvent> {
    let mut out = Vec::new();
    let msg = value
        .get("message")
        .filter(|v| v.is_object())
        .unwrap_or(value);
    let raw_role = non_empty_str(msg, "role")
        .or_else(|| non_empty_str(value, "role"))
        .or_else(|| non_empty_str(value, "type"))
        .unwrap_or("");
    let default_role = match raw_role.to_ascii_lowercase().as_str() {
        "user" | "human" => "user",
        "assistant" | "model" | "agent" => "assistant",
        "system" | "developer" | "summary" => "system",
        "thinking" | "reasoning" => "thinking",
        _ => "raw",
    };

    if let Some(thinking) =
        non_empty_str(value, "thinking").or_else(|| non_empty_str(msg, "thinking"))
    {
        out.push(DraftEvent {
            role: "thinking",
            tool_name: None,
            tool_call_id: None,
            text: thinking.to_string(),
            session_id: None,
        });
    }

    if let Some(content) = msg.get("content").or_else(|| value.get("content")) {
        match content {
            Value::String(s) if !s.is_empty() => {
                out.push(DraftEvent {
                    role: default_role,
                    tool_name: None,
                    tool_call_id: None,
                    text: s.clone(),
                    session_id: None,
                });
            }
            Value::Array(blocks) => {
                for block in blocks {
                    let btype = non_empty_str(block, "type").unwrap_or("");
                    match btype {
                        "thinking" | "reasoning" => {
                            if let Some(t) = non_empty_str(block, "thinking")
                                .or_else(|| non_empty_str(block, "text"))
                            {
                                out.push(DraftEvent {
                                    role: "thinking",
                                    tool_name: None,
                                    tool_call_id: None,
                                    text: t.to_string(),
                                    session_id: None,
                                });
                            }
                        }
                        "text" | "output_text" | "input_text" => {
                            if let Some(t) = non_empty_str(block, "text") {
                                out.push(DraftEvent {
                                    role: default_role,
                                    tool_name: None,
                                    tool_call_id: None,
                                    text: t.to_string(),
                                    session_id: None,
                                });
                            }
                        }
                        "tool_use" | "tool_call" => {
                            let name = non_empty_str(block, "name")
                                .or_else(|| non_empty_str(block, "tool_name"))
                                .map(str::to_string);
                            let call_id = non_empty_str(block, "id")
                                .or_else(|| non_empty_str(block, "call_id"))
                                .map(str::to_string);
                            if let (Some(cid), Some(tname)) = (&call_id, &name) {
                                call_names.insert(cid.clone(), tname.clone());
                            }
                            let input_str = block
                                .get("input")
                                .or_else(|| block.get("arguments"))
                                .map(value_to_text)
                                .unwrap_or_default();
                            let label = name.as_deref().unwrap_or("tool");
                            let text = if input_str.is_empty() {
                                format!("[tool_call:{label}]")
                            } else {
                                format!("[tool_call:{label}] {input_str}")
                            };
                            out.push(DraftEvent {
                                role: "tool_call",
                                tool_name: name,
                                tool_call_id: call_id,
                                text,
                                session_id: None,
                            });
                        }
                        "tool_result" => {
                            let call_id = non_empty_str(block, "tool_use_id")
                                .or_else(|| non_empty_str(block, "call_id"))
                                .map(str::to_string);
                            let tool_name = non_empty_str(block, "tool_name")
                                .or_else(|| non_empty_str(block, "name"))
                                .map(str::to_string)
                                .or_else(|| {
                                    call_id
                                        .as_deref()
                                        .and_then(|cid| call_names.get(cid).cloned())
                                });
                            let body = block
                                .get("content")
                                .or_else(|| block.get("output"))
                                .map(extract_content_text)
                                .unwrap_or_default();
                            let text = if let Some(ref tname) = tool_name {
                                if body.is_empty() {
                                    format!("[tool_result:{tname}]")
                                } else {
                                    format!("[tool_result:{tname}] {body}")
                                }
                            } else {
                                body
                            };
                            out.push(DraftEvent {
                                role: "tool_result",
                                tool_name,
                                tool_call_id: call_id,
                                text,
                                session_id: None,
                            });
                        }
                        _ => {
                            if let Some(t) = non_empty_str(block, "text") {
                                out.push(DraftEvent {
                                    role: default_role,
                                    tool_name: None,
                                    tool_call_id: None,
                                    text: t.to_string(),
                                    session_id: None,
                                });
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    out
}

fn normalize_codex(value: &Value, call_names: &mut HashMap<String, String>) -> Vec<DraftEvent> {
    let mut out = Vec::new();
    let outer_type = non_empty_str(value, "type").unwrap_or("");
    let payload = value
        .get("payload")
        .filter(|v| v.is_object())
        .unwrap_or(value);
    let inner_type = non_empty_str(payload, "type").unwrap_or(outer_type);

    match (outer_type, inner_type) {
        ("session_meta", _) => {
            let sid = non_empty_str(payload, "id").map(str::to_string);
            out.push(DraftEvent {
                role: "system",
                tool_name: None,
                tool_call_id: None,
                text: format!("codex session_meta {}", sid.as_deref().unwrap_or(""))
                    .trim()
                    .to_string(),
                session_id: sid,
            });
        }
        (_, "reasoning" | "agent_reasoning") => {
            let text = non_empty_str(payload, "text")
                .map(str::to_string)
                .or_else(|| {
                    payload.get("summary").and_then(Value::as_array).map(|arr| {
                        arr.iter()
                            .filter_map(|item| {
                                item.as_str()
                                    .or_else(|| item.get("text").and_then(Value::as_str))
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                })
                .filter(|s| !s.is_empty())
                .or_else(|| payload.get("content").map(extract_content_text))
                .filter(|s| !s.is_empty());
            if let Some(text) = text {
                out.push(DraftEvent {
                    role: "thinking",
                    tool_name: None,
                    tool_call_id: None,
                    text,
                    session_id: None,
                });
            }
        }
        (_, "function_call" | "custom_tool_call") => {
            let name = non_empty_str(payload, "name").map(str::to_string);
            let call_id = non_empty_str(payload, "call_id")
                .or_else(|| non_empty_str(payload, "id"))
                .map(str::to_string);
            if let (Some(cid), Some(tname)) = (&call_id, &name) {
                call_names.insert(cid.clone(), tname.clone());
            }
            let args = payload
                .get("arguments")
                .or_else(|| payload.get("input"))
                .map(value_to_text)
                .unwrap_or_default();
            let label = name.as_deref().unwrap_or("tool");
            let text = if args.is_empty() {
                format!("[tool_call:{label}]")
            } else {
                format!("[tool_call:{label}] {args}")
            };
            out.push(DraftEvent {
                role: "tool_call",
                tool_name: name,
                tool_call_id: call_id,
                text,
                session_id: None,
            });
        }
        (_, "function_call_output" | "custom_tool_call_output") => {
            let call_id = non_empty_str(payload, "call_id")
                .or_else(|| non_empty_str(payload, "id"))
                .map(str::to_string);
            let tool_name = call_id
                .as_deref()
                .and_then(|cid| call_names.get(cid).cloned());
            let body = payload
                .get("output")
                .or_else(|| payload.get("content"))
                .map(extract_content_text)
                .unwrap_or_default();
            let text = if let Some(ref tname) = tool_name {
                if body.is_empty() {
                    format!("[tool_result:{tname}]")
                } else {
                    format!("[tool_result:{tname}] {body}")
                }
            } else {
                body
            };
            out.push(DraftEvent {
                role: "tool_result",
                tool_name,
                tool_call_id: call_id,
                text,
                session_id: None,
            });
        }
        (_, "message") => {
            let role = match non_empty_str(payload, "role")
                .unwrap_or("")
                .to_ascii_lowercase()
                .as_str()
            {
                "user" => "user",
                "assistant" => "assistant",
                "developer" | "system" => "system",
                _ => "raw",
            };
            let text = payload
                .get("content")
                .map(extract_content_text)
                .filter(|s| !s.is_empty())
                .or_else(|| non_empty_str(payload, "text").map(str::to_string))
                .unwrap_or_default();
            if !text.is_empty() {
                out.push(DraftEvent {
                    role,
                    tool_name: None,
                    tool_call_id: None,
                    text,
                    session_id: None,
                });
            }
        }
        ("event_msg", "agent_message") => {
            if let Some(t) =
                non_empty_str(payload, "message").or_else(|| non_empty_str(payload, "text"))
            {
                out.push(DraftEvent {
                    role: "assistant",
                    tool_name: None,
                    tool_call_id: None,
                    text: t.to_string(),
                    session_id: None,
                });
            }
        }
        ("event_msg", "user_message") => {
            if let Some(t) =
                non_empty_str(payload, "message").or_else(|| non_empty_str(payload, "text"))
            {
                out.push(DraftEvent {
                    role: "user",
                    tool_name: None,
                    tool_call_id: None,
                    text: t.to_string(),
                    session_id: None,
                });
            }
        }
        _ => {}
    }
    out
}

fn normalize_spiron(value: &Value, call_names: &mut HashMap<String, String>) -> Vec<DraftEvent> {
    let mut out = Vec::new();
    let kind = non_empty_str(value, "type").unwrap_or("");
    let data = value.get("data").filter(|v| v.is_object()).unwrap_or(value);
    match kind.to_ascii_lowercase().as_str() {
        "thinking" | "reasoning" => {
            let text = non_empty_str(data, "text")
                .or_else(|| non_empty_str(data, "content"))
                .or_else(|| non_empty_str(data, "thinking"))
                .map(str::to_string)
                .unwrap_or_default();
            if !text.is_empty() {
                out.push(DraftEvent {
                    role: "thinking",
                    tool_name: None,
                    tool_call_id: None,
                    text,
                    session_id: None,
                });
            }
        }
        "tool_call" | "tool_use" => {
            let name = non_empty_str(data, "toolName")
                .or_else(|| non_empty_str(data, "tool_name"))
                .or_else(|| non_empty_str(data, "name"))
                .or_else(|| non_empty_str(data, "tool"))
                .map(str::to_string);
            let call_id = non_empty_str(data, "callId")
                .or_else(|| non_empty_str(data, "call_id"))
                .or_else(|| non_empty_str(data, "id"))
                .map(str::to_string);
            if let (Some(cid), Some(tname)) = (&call_id, &name) {
                call_names.insert(cid.clone(), tname.clone());
            }
            let args = data
                .get("args")
                .or_else(|| data.get("arguments"))
                .or_else(|| data.get("input"))
                .map(value_to_text)
                .unwrap_or_default();
            let label = name.as_deref().unwrap_or("tool");
            let text = if args.is_empty() {
                format!("[tool_call:{label}]")
            } else {
                format!("[tool_call:{label}] {args}")
            };
            out.push(DraftEvent {
                role: "tool_call",
                tool_name: name,
                tool_call_id: call_id,
                text,
                session_id: None,
            });
        }
        "tool_result" => {
            let call_id = non_empty_str(data, "callId")
                .or_else(|| non_empty_str(data, "call_id"))
                .or_else(|| non_empty_str(data, "tool_use_id"))
                .map(str::to_string);
            let name = non_empty_str(data, "toolName")
                .or_else(|| non_empty_str(data, "tool_name"))
                .or_else(|| non_empty_str(data, "name"))
                .or_else(|| non_empty_str(data, "tool"))
                .map(str::to_string)
                .or_else(|| {
                    call_id
                        .as_deref()
                        .and_then(|cid| call_names.get(cid).cloned())
                });
            let body = data
                .get("output")
                .or_else(|| data.get("result"))
                .or_else(|| data.get("content"))
                .map(extract_content_text)
                .unwrap_or_default();
            let text = if let Some(ref tname) = name {
                if body.is_empty() {
                    format!("[tool_result:{tname}]")
                } else {
                    format!("[tool_result:{tname}] {body}")
                }
            } else {
                body
            };
            out.push(DraftEvent {
                role: "tool_result",
                tool_name: name,
                tool_call_id: call_id,
                text,
                session_id: None,
            });
        }
        "text" | "message" => {
            let role = match non_empty_str(data, "role")
                .unwrap_or("assistant")
                .to_ascii_lowercase()
                .as_str()
            {
                "user" | "human" => "user",
                "system" => "system",
                _ => "assistant",
            };
            let text = data
                .get("content")
                .or_else(|| data.get("text"))
                .map(extract_content_text)
                .unwrap_or_default();
            if !text.is_empty() {
                out.push(DraftEvent {
                    role,
                    tool_name: None,
                    tool_call_id: None,
                    text,
                    session_id: None,
                });
            }
        }
        "error" => {
            let text = non_empty_str(data, "message")
                .or_else(|| non_empty_str(data, "error"))
                .map(str::to_string)
                .unwrap_or_else(|| data.to_string());
            out.push(DraftEvent {
                role: "system",
                tool_name: None,
                tool_call_id: None,
                text,
                session_id: None,
            });
        }
        _ => {
            // Fallback for legacy flat sylphx-transcript.jsonl rows (`role`, `tool`, `output`).
            out = normalize_generic(value, call_names);
        }
    }
    out
}

fn normalize_generic(value: &Value, call_names: &mut HashMap<String, String>) -> Vec<DraftEvent> {
    if value.get("message").is_some_and(Value::is_object) {
        let msgs = normalize_claude_or_message(value, call_names);
        if !msgs.is_empty() {
            return msgs;
        }
    }
    if value.get("payload").is_some_and(Value::is_object) {
        let codex = normalize_codex(value, call_names);
        if !codex.is_empty() {
            return codex;
        }
    }
    let role_raw = non_empty_str(value, "role")
        .or_else(|| non_empty_str(value, "type"))
        .unwrap_or("");
    let tool_name = non_empty_str(value, "tool_name")
        .or_else(|| non_empty_str(value, "tool"))
        .map(str::to_string);
    let call_id = non_empty_str(value, "tool_call_id")
        .or_else(|| non_empty_str(value, "call_id"))
        .map(str::to_string);
    if role_raw.eq_ignore_ascii_case("tool") || tool_name.is_some() {
        let text = value
            .get("output")
            .or_else(|| value.get("result"))
            .or_else(|| value.get("content"))
            .or_else(|| value.get("text"))
            .map(extract_content_text)
            .unwrap_or_else(|| value.to_string());
        let is_result = value.get("output").is_some() || value.get("result").is_some();
        return vec![DraftEvent {
            role: if is_result {
                "tool_result"
            } else {
                "tool_call"
            },
            tool_name,
            tool_call_id: call_id,
            text,
            session_id: None,
        }];
    }
    normalize_claude_or_message(value, call_names)
}

fn non_empty_str<'a>(val: &'a Value, key: &str) -> Option<&'a str> {
    val.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

fn value_to_text(val: &Value) -> String {
    match val {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn extract_content_text(val: &Value) -> String {
    match val {
        Value::String(s) => s.clone(),
        Value::Array(items) => items
            .iter()
            .filter_map(|item| match item {
                Value::String(s) if !s.is_empty() => Some(s.clone()),
                Value::Object(map) => map
                    .get("text")
                    .or_else(|| map.get("thinking"))
                    .or_else(|| map.get("content"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn extract_fallback_text(val: &Value) -> Option<String> {
    for key in ["text", "content", "thinking", "output", "message"] {
        if let Some(s) = val.get(key).and_then(Value::as_str)
            && !s.is_empty()
        {
            return Some(s.to_string());
        }
    }
    None
}

fn extract_session_id(val: &Value) -> Option<String> {
    for key in ["session_id", "sessionId", "conversation_id"] {
        if let Some(s) = val.get(key).and_then(Value::as_str)
            && !s.trim().is_empty()
        {
            return Some(s.trim().to_string());
        }
    }
    None
}

fn extract_timestamp_ms(val: &Value) -> Option<i64> {
    for key in ["timestamp_ms", "created_at_ms", "timestamp", "created_at"] {
        if let Some(v) = val.get(key) {
            if let Some(n) = v.as_i64()
                && n > 0
            {
                return Some(if n < 10_000_000_000 { n * 1000 } else { n });
            }
            if let Some(s) = v.as_str()
                && let Some(ms) = parse_rfc3339_ms(s)
            {
                return Some(ms);
            }
        }
    }
    None
}

/// Minimal pure-Rust RFC-3339 timestamp parser (`YYYY-MM-DDTHH:MM:SS(.frac)?(Z|+HH:MM|-HH:MM)`).
pub(crate) fn parse_rfc3339_ms(raw: &str) -> Option<i64> {
    let s = raw.trim();
    if s.len() < 20 {
        return None;
    }
    let bytes = s.as_bytes();
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || !matches!(bytes[10], b'T' | b't' | b' ')
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year: i64 = s[0..4].parse().ok()?;
    let month: u32 = s[5..7].parse().ok()?;
    let day: u32 = s[8..10].parse().ok()?;
    let hour: i64 = s[11..13].parse().ok()?;
    let min: i64 = s[14..16].parse().ok()?;
    let sec: i64 = s[17..19].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    let mut rest = &s[19..];
    let mut millis: i64 = 0;
    if let Some(after_dot) = rest.strip_prefix('.') {
        let digits_len = after_dot.bytes().take_while(|b| b.is_ascii_digit()).count();
        let frac = &after_dot[..digits_len];
        if !frac.is_empty() {
            let mut padded = [b'0'; 3];
            for (i, b) in frac.bytes().take(3).enumerate() {
                padded[i] = b;
            }
            millis = std::str::from_utf8(&padded).ok()?.parse().ok()?;
        }
        rest = &after_dot[digits_len..];
    }
    let tz_offset_secs: i64 = if rest.eq_ignore_ascii_case("Z") {
        0
    } else if rest.len() == 6 && (rest.starts_with('+') || rest.starts_with('-')) {
        let sign = if rest.starts_with('-') { -1 } else { 1 };
        let th: i64 = rest[1..3].parse().ok()?;
        let tm: i64 = rest[4..6].parse().ok()?;
        sign * (th * 3600 + tm * 60)
    } else {
        return None;
    };
    let days = days_from_civil(year, month as i64, day as i64);
    let total_secs = days * 86_400 + hour * 3_600 + min * 60 + sec - tz_offset_secs;
    Some(total_secs.saturating_mul(1_000).saturating_add(millis))
}

/// Howard Hinnant's civil-to-days algorithm (days since 1970-01-01).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub(super) fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
