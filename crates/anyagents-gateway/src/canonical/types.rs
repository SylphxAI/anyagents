use serde_json::{Map, Value, json};

use crate::error::Fail;
use crate::route::Protocol;
use crate::usage::{TokenUsage, patch_anthropic, patch_chat, patch_responses};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    EndTurn,
    MaxTokens,
    StopSequence,
    ToolUse,
    Refusal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Role {
    System,
    User,
    Assistant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ImageSource {
    Base64(String),
    Url(String),
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Block {
    Text(String),
    Image {
        media_type: String,
        source: ImageSource,
    },
    ToolCall {
        id: String,
        name: String,
        arguments: String,
    },
    ToolResult {
        id: String,
        name: Option<String>,
        output: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Tool {
    pub(super) name: String,
    pub(super) description: Option<String>,
    pub(super) parameters: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum ToolChoice {
    Auto,
    None,
    Required,
    Named(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Request {
    pub(super) system: Vec<String>,
    pub(super) messages: Vec<Message>,
    pub(super) tools: Vec<Tool>,
    pub(super) tool_choice: Option<ToolChoice>,
    pub(super) parallel_tool_calls: Option<bool>,
    pub(super) max_tokens: Option<u64>,
    pub(super) temperature: Option<Value>,
    pub(super) top_p: Option<Value>,
    pub(super) stop: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Message {
    pub(super) role: Role,
    pub(super) blocks: Vec<Block>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Completion {
    pub id: String,
    pub(super) blocks: Vec<Block>,
    pub stop: StopReason,
    pub(crate) usage: TokenUsage,
}
pub(super) fn promote(stop: StopReason, blocks: &[Block]) -> StopReason {
    let has_tool = blocks
        .iter()
        .any(|block| matches!(block, Block::ToolCall { .. }));
    if has_tool && stop == StopReason::EndTurn {
        StopReason::ToolUse
    } else {
        stop
    }
}

pub(super) fn usage_of(protocol: Protocol, usage: Option<&Value>) -> TokenUsage {
    let Some(usage) = usage.filter(|value| !value.is_null()) else {
        return TokenUsage::default();
    };
    let mut counts = TokenUsage::default();
    let patch = match protocol {
        Protocol::AnthropicMessages => patch_anthropic(usage),
        Protocol::OpenAiChat => patch_chat(usage),
        Protocol::OpenAiResponses => patch_responses(usage),
    };
    counts.apply(patch);
    counts
}

pub(crate) fn prefixed(id: &str, prefix: &str) -> String {
    if id.is_empty() {
        format!("{prefix}0")
    } else if id.starts_with(prefix) {
        id.to_string()
    } else {
        format!("{prefix}{id}")
    }
}
pub(super) fn insert_sampling(body: &mut Map<String, Value>, request: &Request) {
    if let Some(temperature) = &request.temperature {
        body.insert("temperature".into(), temperature.clone());
    }
    if let Some(top_p) = &request.top_p {
        body.insert("top_p".into(), top_p.clone());
    }
}

pub(super) fn system_value(parts: &[String], anthropic: bool) -> Option<Value> {
    if parts.is_empty() {
        return None;
    }
    if anthropic && parts.len() > 1 {
        return Some(Value::Array(
            parts
                .iter()
                .map(|text| json!({"type": "text", "text": text}))
                .collect(),
        ));
    }
    if anthropic {
        Some(Value::String(parts[0].clone()))
    } else {
        Some(Value::String(parts.join("\n\n")))
    }
}

pub(super) fn merge_turns(messages: &[Message]) -> Vec<Message> {
    let mut merged: Vec<Message> = Vec::new();
    for message in messages {
        if message.blocks.is_empty() {
            continue;
        }
        if let Some(last) = merged.last_mut()
            && last.role == message.role
        {
            last.blocks.extend(message.blocks.iter().cloned());
            continue;
        }
        merged.push(message.clone());
    }
    merged
}

pub(super) fn take_system(messages: Vec<Message>) -> Result<(Vec<String>, Vec<Message>), Fail> {
    let mut system = Vec::new();
    let mut rest = Vec::new();
    for message in messages {
        if message.role != Role::System {
            rest.push(message);
            continue;
        }
        for block in message.blocks {
            match block {
                Block::Text(text) => system.push(text),
                Block::Image { .. } => return Err(Fail::unsupported("image")),
                Block::ToolCall { .. } => return Err(Fail::unsupported("tool_use")),
                Block::ToolResult { .. } => return Err(Fail::unsupported("tool_result")),
            }
        }
    }
    Ok((system, rest))
}

pub(super) fn image_from_url(url: &str) -> Result<Block, Fail> {
    if let Some(rest) = url.strip_prefix("data:") {
        let Some((media_type, data)) = rest.split_once(";base64,") else {
            return Err(Fail::unsupported("image_encoding"));
        };
        if media_type.is_empty() || data.is_empty() {
            return Err(Fail::invalid("image"));
        }
        return Ok(Block::Image {
            media_type: media_type.to_string(),
            source: ImageSource::Base64(data.to_string()),
        });
    }
    Ok(Block::Image {
        media_type: String::new(),
        source: ImageSource::Url(url.to_string()),
    })
}

pub(super) fn image_string(media_type: &str, source: &ImageSource) -> String {
    match source {
        ImageSource::Url(url) => url.clone(),
        ImageSource::Base64(data) => format!("data:{media_type};base64,{data}"),
    }
}

pub(super) fn tool_text(value: &Value) -> Result<String, Fail> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Null => Ok(String::new()),
        Value::Array(parts) => {
            let mut out = String::new();
            for (index, part) in parts.iter().enumerate() {
                let obj = object(part, "content")?;
                reject_unknown(obj, &["type", "text"])?;
                let kind = required_string(obj, "type")?;
                if kind != "text" {
                    return Err(Fail::unsupported(kind));
                }
                if index > 0 {
                    out.push('\n');
                }
                out.push_str(&required_string(obj, "text")?);
            }
            Ok(out)
        }
        _ => Err(Fail::invalid("tool result")),
    }
}

pub(super) fn argument_string(value: Option<&Value>) -> Result<String, Fail> {
    match value {
        None | Some(Value::Null) => Ok("{}".to_string()),
        Some(Value::String(raw)) => {
            if raw.is_empty() {
                return Ok("{}".to_string());
            }
            let parsed: Value =
                serde_json::from_str(raw).map_err(|_| Fail::invalid("arguments"))?;
            if !parsed.is_object() {
                return Err(Fail::invalid("arguments"));
            }
            serde_json::to_string(&parsed).map_err(|_| Fail::invalid("arguments"))
        }
        Some(_) => Err(Fail::invalid("arguments")),
    }
}

pub(super) fn call_id(obj: &Map<String, Value>) -> Result<String, Fail> {
    optional_string(obj, "call_id")?
        .or(optional_string(obj, "id")?)
        .ok_or_else(|| Fail::invalid("call_id"))
}

pub(super) fn reject_detail(obj: &Map<String, Value>) -> Result<(), Fail> {
    match obj.get("detail").and_then(Value::as_str) {
        None | Some("auto") => Ok(()),
        Some(_) => Err(Fail::unsupported("image_detail")),
    }
}

pub(crate) fn reject_unknown(obj: &Map<String, Value>, allowed: &[&str]) -> Result<(), Fail> {
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(Fail::unsupported(key.clone()));
        }
    }
    Ok(())
}

pub(super) fn object<'a>(value: &'a Value, what: &str) -> Result<&'a Map<String, Value>, Fail> {
    value.as_object().ok_or_else(|| Fail::invalid(what))
}

pub(super) fn required_string(obj: &Map<String, Value>, key: &str) -> Result<String, Fail> {
    obj.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| Fail::invalid(key))
}

pub(super) fn optional_string(obj: &Map<String, Value>, key: &str) -> Result<Option<String>, Fail> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(Fail::invalid(key)),
    }
}

pub(super) fn require_string(obj: &Map<String, Value>, key: &str) -> Result<(), Fail> {
    match obj.get(key) {
        None => Ok(()),
        Some(Value::String(_)) => Ok(()),
        Some(_) => Err(Fail::invalid(key)),
    }
}

pub(super) fn required_array<'a>(
    obj: &'a Map<String, Value>,
    key: &str,
) -> Result<&'a Vec<Value>, Fail> {
    obj.get(key)
        .ok_or_else(|| Fail::invalid(key))?
        .as_array()
        .ok_or_else(|| Fail::invalid(key))
}

pub(super) fn optional_array<'a>(
    obj: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a Vec<Value>>, Fail> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => Ok(Some(value.as_array().ok_or_else(|| Fail::invalid(key))?)),
    }
}

pub(super) fn optional_bool(obj: &Map<String, Value>, key: &str) -> Result<Option<bool>, Fail> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(Fail::invalid(key)),
    }
}

pub(super) fn number_field(obj: &Map<String, Value>, key: &str) -> Result<Option<Value>, Fail> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) if value.is_number() => Ok(Some(value.clone())),
        Some(_) => Err(Fail::invalid(key)),
    }
}

pub(super) fn one_max(obj: &Map<String, Value>, keys: &[&str]) -> Result<Option<u64>, Fail> {
    let mut found = None;
    for key in keys {
        if let Some(value) = obj.get(*key) {
            let number = value
                .as_u64()
                .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
                .ok_or_else(|| Fail::invalid("max_tokens"))?;
            if let Some(previous) = found
                && previous != number
            {
                return Err(Fail::invalid("max_tokens"));
            }
            found = Some(number);
        }
    }
    Ok(found)
}

pub(super) fn optional_stop(obj: &Map<String, Value>, key: &str) -> Result<Vec<String>, Fail> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::String(text)) => Ok(vec![text.clone()]),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| Fail::invalid(key))
            })
            .collect(),
        Some(_) => Err(Fail::invalid(key)),
    }
}
