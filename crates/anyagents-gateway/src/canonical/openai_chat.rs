use serde_json::{Map, Value, json};

use crate::error::Fail;
use crate::route::Protocol;

use super::tools::{
    chat_tool_calls, choice_and_parallel, insert_tools, named_or_mode, parse_chat_tool_call,
    parse_tools,
};
use super::types::*;

const CHAT_KEYS: &[&str] = &[
    "model",
    "messages",
    "tools",
    "tool_choice",
    "stream",
    "temperature",
    "top_p",
    "stop",
    "max_tokens",
    "max_completion_tokens",
    "parallel_tool_calls",
];
pub(super) fn parse_chat_request(value: &Value) -> Result<Request, Fail> {
    let obj = object(value, "request")?;
    reject_unknown(obj, CHAT_KEYS)?;
    require_string(obj, "model")?;
    let mut messages = Vec::new();
    for message in required_array(obj, "messages")? {
        messages.push(parse_chat_message(message)?);
    }
    let (system, messages) = take_system(messages)?;
    let (choice, parallel) = choice_and_parallel(Protocol::OpenAiChat, obj)?;
    Ok(Request {
        system,
        messages,
        tools: parse_tools(obj, Protocol::OpenAiChat)?,
        tool_choice: choice,
        parallel_tool_calls: parallel,
        max_tokens: one_max(obj, &["max_tokens", "max_completion_tokens"])?,
        temperature: number_field(obj, "temperature")?,
        top_p: number_field(obj, "top_p")?,
        stop: optional_stop(obj, "stop")?,
    })
}
fn parse_chat_message(value: &Value) -> Result<Message, Fail> {
    let obj = object(value, "message")?;
    let role_name = required_string(obj, "role")?;
    if role_name == "tool" {
        reject_unknown(obj, &["role", "content", "tool_call_id", "name"])?;
        let output = match obj.get("content") {
            None | Some(Value::Null) => String::new(),
            Some(content) => tool_text(content)?,
        };
        return Ok(Message {
            role: Role::User,
            blocks: vec![Block::ToolResult {
                id: required_string(obj, "tool_call_id")?,
                name: optional_string(obj, "name")?,
                output,
            }],
        });
    }
    reject_unknown(obj, &["role", "content", "tool_calls", "refusal"])?;
    if let Some(refusal) = obj.get("refusal").filter(|value| !value.is_null()) {
        let text = refusal.as_str().ok_or_else(|| Fail::invalid("refusal"))?;
        if !text.is_empty() {
            return Err(Fail::unsupported("refusal"));
        }
    }
    let role = match role_name.as_str() {
        "system" | "developer" => Role::System,
        "user" => Role::User,
        "assistant" => Role::Assistant,
        other => return Err(Fail::unsupported(other)),
    };
    if obj.contains_key("tool_calls") && role != Role::Assistant {
        return Err(Fail::invalid("tool_calls"));
    }
    let mut blocks = match obj.get("content") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::String(text)) => vec![Block::Text(text.clone())],
        Some(Value::Array(parts)) => parts
            .iter()
            .map(parse_chat_part)
            .collect::<Result<_, _>>()?,
        Some(_) => return Err(Fail::invalid("content")),
    };
    if let Some(calls) = obj.get("tool_calls").filter(|value| !value.is_null()) {
        let calls = calls
            .as_array()
            .ok_or_else(|| Fail::invalid("tool_calls"))?;
        for call in calls {
            blocks.push(parse_chat_tool_call(call)?);
        }
    }
    Ok(Message { role, blocks })
}

fn parse_chat_part(value: &Value) -> Result<Block, Fail> {
    let obj = object(value, "content")?;
    let kind = required_string(obj, "type")?;
    match kind.as_str() {
        "text" => {
            reject_unknown(obj, &["type", "text"])?;
            Ok(Block::Text(required_string(obj, "text")?))
        }
        "image_url" => parse_chat_image(obj),
        other => Err(Fail::unsupported(other)),
    }
}

fn parse_chat_image(obj: &Map<String, Value>) -> Result<Block, Fail> {
    reject_unknown(obj, &["type", "image_url"])?;
    let image = object(
        obj.get("image_url")
            .ok_or_else(|| Fail::invalid("image_url"))?,
        "image_url",
    )?;
    reject_unknown(image, &["url", "detail"])?;
    reject_detail(image)?;
    image_from_url(&required_string(image, "url")?)
}
pub(super) fn encode_chat_request(
    request: &Request,
    model: &str,
    stream: bool,
) -> Result<Value, Fail> {
    let mut body = Map::new();
    body.insert("model".into(), json!(model));
    insert_sampling(&mut body, request);
    if let Some(max_tokens) = request.max_tokens {
        body.insert("max_tokens".into(), json!(max_tokens));
    }
    let mut messages = Vec::new();
    if let Some(system) = system_value(&request.system, false) {
        messages.push(json!({"role": "system", "content": system}));
    }
    for message in &request.messages {
        match message.role {
            Role::System => return Err(Fail::invalid("system")),
            Role::User => messages.extend(encode_chat_user(&message.blocks)?),
            Role::Assistant => messages.push(encode_chat_assistant(&message.blocks)?),
        }
    }
    body.insert("messages".into(), Value::Array(messages));
    insert_tools(&mut body, Protocol::OpenAiChat, &request.tools);
    if let Some(choice) = named_or_mode(request.tool_choice.as_ref(), true) {
        body.insert("tool_choice".into(), choice);
    }
    if let Some(parallel) = request.parallel_tool_calls {
        body.insert("parallel_tool_calls".into(), json!(parallel));
    }
    if !request.stop.is_empty() {
        body.insert("stop".into(), json!(request.stop));
    }
    body.insert("stream".into(), json!(stream));
    if stream {
        // The gateway asks for usage. A client-supplied stream_options is rejected
        // at parse time, so this is not a field that was dropped on the way in.
        body.insert("stream_options".into(), json!({"include_usage": true}));
    }
    Ok(Value::Object(body))
}
fn encode_chat_user(blocks: &[Block]) -> Result<Vec<Value>, Fail> {
    let mut out = Vec::new();
    let mut pending = Vec::new();
    for block in blocks {
        match block {
            Block::ToolCall { .. } => return Err(Fail::invalid("tool_use")),
            Block::ToolResult { id, name, output } => {
                flush_chat_user(&mut pending, &mut out)?;
                let mut message = json!({
                    "role": "tool",
                    "tool_call_id": id,
                    "content": output,
                });
                if let Some(name) = name {
                    message["name"] = json!(name);
                }
                out.push(message);
            }
            other => pending.push(other.clone()),
        }
    }
    flush_chat_user(&mut pending, &mut out)?;
    Ok(out)
}

fn flush_chat_user(pending: &mut Vec<Block>, out: &mut Vec<Value>) -> Result<(), Fail> {
    if pending.is_empty() {
        return Ok(());
    }
    let content = chat_content(pending)?;
    pending.clear();
    out.push(json!({"role": "user", "content": content}));
    Ok(())
}

fn encode_chat_assistant(blocks: &[Block]) -> Result<Value, Fail> {
    for block in blocks {
        if matches!(block, Block::ToolResult { .. }) {
            return Err(Fail::invalid("tool_result"));
        }
    }
    let mut message = json!({
        "role": "assistant",
        "content": chat_content(blocks)?,
    });
    let calls = chat_tool_calls(blocks);
    if !calls.is_empty() {
        message["tool_calls"] = Value::Array(calls);
    }
    Ok(message)
}

fn chat_content(blocks: &[Block]) -> Result<Value, Fail> {
    let mut parts = Vec::new();
    for block in blocks {
        match block {
            Block::Text(text) => parts.push(json!({"type": "text", "text": text})),
            Block::Image { media_type, source } => parts.push(json!({
                "type": "image_url",
                "image_url": {"url": image_string(media_type, source)},
            })),
            Block::ToolCall { .. } | Block::ToolResult { .. } => {}
        }
    }
    match parts.len() {
        0 => Ok(Value::Null),
        1 if parts[0].get("type").and_then(Value::as_str) == Some("text") => {
            Ok(parts[0].get("text").cloned().unwrap_or(Value::Null))
        }
        _ => Ok(Value::Array(parts)),
    }
}
pub(super) fn parse_chat_response(value: &Value) -> Result<Completion, Fail> {
    let obj = object(value, "response")?;
    let choices = obj
        .get("choices")
        .and_then(Value::as_array)
        .ok_or_else(|| Fail::invalid("choices"))?;
    if choices.len() != 1 {
        return Err(Fail::unsupported("n"));
    }
    let choice = object(&choices[0], "choice")?;
    if choice.get("logprobs").is_some_and(|value| !value.is_null()) {
        return Err(Fail::unsupported("logprobs"));
    }
    let message = parse_chat_message(
        choice
            .get("message")
            .ok_or_else(|| Fail::invalid("message"))?,
    )?;
    let reason = choice
        .get("finish_reason")
        .and_then(Value::as_str)
        .ok_or_else(|| Fail::invalid("finish_reason"))?;
    Ok(Completion {
        id: optional_string(obj, "id")?.unwrap_or_default(),
        stop: promote(stop_from_chat(reason)?, &message.blocks),
        blocks: message.blocks,
        usage: usage_of(Protocol::OpenAiChat, obj.get("usage")),
    })
}
pub(super) fn encode_chat_response(completion: &Completion, model: &str) -> Result<Value, Fail> {
    Ok(json!({
        "id": prefixed(&completion.id, "chatcmpl_"),
        "object": "chat.completion",
        "created": 0,
        "model": model,
        "choices": [{
            "index": 0,
            "message": encode_chat_assistant(&completion.blocks)?,
            "finish_reason": stop_to_chat(completion.stop),
        }],
        "usage": completion.usage.chat_json(),
    }))
}
pub(crate) fn stop_from_chat(reason: &str) -> Result<StopReason, Fail> {
    match reason {
        // Chat uses `stop` both for a normal end and for a stop sequence.
        "stop" => Ok(StopReason::EndTurn),
        "length" => Ok(StopReason::MaxTokens),
        "tool_calls" | "function_call" => Ok(StopReason::ToolUse),
        "content_filter" => Ok(StopReason::Refusal),
        other => Err(Fail::unsupported(other)),
    }
}
pub(crate) fn stop_to_chat(reason: StopReason) -> &'static str {
    match reason {
        StopReason::EndTurn | StopReason::StopSequence => "stop",
        StopReason::MaxTokens => "length",
        StopReason::ToolUse => "tool_calls",
        StopReason::Refusal => "content_filter",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::{encode_request, parse_request};
    use super::*;

    #[test]
    fn chat_image_url_becomes_an_anthropic_url() {
        let raw = json!({
            "model": "gpt-client",
            "max_tokens": 8,
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "image_url",
                    "image_url": {"url": "https://example.com/a.png", "detail": "auto"}
                }]
            }]
        });
        let request = parse_request(Protocol::OpenAiChat, &raw).unwrap();
        let encoded =
            encode_request(Protocol::AnthropicMessages, &request, "claude-route", false).unwrap();
        assert_eq!(
            encoded["messages"][0]["content"][0]["source"]["type"],
            "url"
        );
        assert_eq!(
            encoded["messages"][0]["content"][0]["source"]["url"],
            "https://example.com/a.png"
        );
    }
    #[test]
    fn developer_text_is_system_text() {
        let raw = json!({
            "model": "gpt-client",
            "messages": [
                {"role": "developer", "content": "Be brief."},
                {"role": "user", "content": "Hi"}
            ]
        });
        let request = parse_request(Protocol::OpenAiChat, &raw).unwrap();
        let encoded =
            encode_request(Protocol::OpenAiResponses, &request, "gpt-route", false).unwrap();
        assert_eq!(encoded["instructions"], "Be brief.");
        assert_eq!(encoded["input"][0]["content"], "Hi");
    }
}
