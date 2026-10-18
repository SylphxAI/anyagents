use serde_json::{Map, Value, json};

use crate::error::Fail;
use crate::route::Protocol;

use super::tools::{choice_and_parallel, insert_tools, named_or_mode, parse_tools};
use super::types::*;

const RESPONSES_KEYS: &[&str] = &[
    "model",
    "input",
    "instructions",
    "tools",
    "tool_choice",
    "stream",
    "temperature",
    "top_p",
    "max_output_tokens",
    "parallel_tool_calls",
];
pub(super) fn parse_responses_request(value: &Value) -> Result<Request, Fail> {
    let obj = object(value, "request")?;
    reject_unknown(obj, RESPONSES_KEYS)?;
    require_string(obj, "model")?;
    let mut messages = match obj.get("input") {
        None => return Err(Fail::invalid("input")),
        Some(input) => parse_responses_input(input)?,
    };
    if let Some(instructions) = optional_string(obj, "instructions")? {
        messages.insert(
            0,
            Message {
                role: Role::System,
                blocks: vec![Block::Text(instructions)],
            },
        );
    }
    let (system, messages) = take_system(messages)?;
    let (choice, parallel) = choice_and_parallel(Protocol::OpenAiResponses, obj)?;
    Ok(Request {
        system,
        messages,
        tools: parse_tools(obj, Protocol::OpenAiResponses)?,
        tool_choice: choice,
        parallel_tool_calls: parallel,
        max_tokens: one_max(obj, &["max_output_tokens"])?,
        temperature: number_field(obj, "temperature")?,
        top_p: number_field(obj, "top_p")?,
        stop: Vec::new(),
    })
}
fn parse_responses_input(value: &Value) -> Result<Vec<Message>, Fail> {
    match value {
        Value::String(text) => Ok(vec![Message {
            role: Role::User,
            blocks: vec![Block::Text(text.clone())],
        }]),
        Value::Array(items) => items.iter().map(parse_responses_item).collect(),
        _ => Err(Fail::invalid("input")),
    }
}

fn parse_responses_item(value: &Value) -> Result<Message, Fail> {
    if let Some(text) = value.as_str() {
        return Ok(Message {
            role: Role::User,
            blocks: vec![Block::Text(text.to_string())],
        });
    }
    let obj = object(value, "input")?;
    let kind = optional_string(obj, "type")?.unwrap_or_else(|| "message".to_string());
    match kind.as_str() {
        "message" => parse_responses_message(obj),
        "function_call" => {
            // `id` and `status` are provider bookkeeping, not a second copy of the call.
            reject_unknown(
                obj,
                &["type", "id", "call_id", "name", "arguments", "status"],
            )?;
            Ok(Message {
                role: Role::Assistant,
                blocks: vec![Block::ToolCall {
                    id: call_id(obj)?,
                    name: required_string(obj, "name")?,
                    arguments: argument_string(obj.get("arguments"))?,
                }],
            })
        }
        "function_call_output" => {
            reject_unknown(obj, &["type", "id", "call_id", "output", "status"])?;
            let output = match obj.get("output") {
                None | Some(Value::Null) => String::new(),
                Some(output) => tool_text(output)?,
            };
            Ok(Message {
                role: Role::User,
                blocks: vec![Block::ToolResult {
                    id: call_id(obj)?,
                    name: None,
                    output,
                }],
            })
        }
        other => Err(Fail::unsupported(other)),
    }
}

fn parse_responses_message(obj: &Map<String, Value>) -> Result<Message, Fail> {
    reject_unknown(obj, &["type", "role", "content", "id", "status"])?;
    let role = match optional_string(obj, "role")?.as_deref() {
        Some("system") | Some("developer") => Role::System,
        Some("assistant") => Role::Assistant,
        Some("user") | None => Role::User,
        Some(other) => return Err(Fail::unsupported(other)),
    };
    let blocks = match obj.get("content") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::String(text)) => vec![Block::Text(text.clone())],
        Some(Value::Array(parts)) => parts
            .iter()
            .map(parse_responses_part)
            .collect::<Result<_, _>>()?,
        Some(_) => return Err(Fail::invalid("content")),
    };
    Ok(Message { role, blocks })
}

fn parse_responses_part(value: &Value) -> Result<Block, Fail> {
    let obj = object(value, "content")?;
    let kind = required_string(obj, "type")?;
    match kind.as_str() {
        "input_text" | "output_text" | "text" => {
            reject_unknown(obj, &["type", "text", "annotations", "id"])?;
            if let Some(annotations) = obj.get("annotations") {
                let empty = annotations.as_array().is_some_and(|items| items.is_empty());
                if !empty {
                    return Err(Fail::unsupported("annotations"));
                }
            }
            Ok(Block::Text(required_string(obj, "text")?))
        }
        "input_image" => {
            // UNVERIFIED: image_url is accepted as a string. An object with `url` is
            // also accepted so either published shape can be read.
            reject_unknown(obj, &["type", "image_url", "detail"])?;
            reject_detail(obj)?;
            let url = match obj.get("image_url") {
                Some(Value::String(url)) => url.clone(),
                Some(Value::Object(image)) => {
                    reject_unknown(image, &["url", "detail"])?;
                    reject_detail(image)?;
                    required_string(image, "url")?
                }
                _ => return Err(Fail::invalid("image_url")),
            };
            image_from_url(&url)
        }
        other => Err(Fail::unsupported(other)),
    }
}
pub(super) fn encode_responses_request(
    request: &Request,
    model: &str,
    stream: bool,
) -> Result<Value, Fail> {
    let mut body = Map::new();
    body.insert("model".into(), json!(model));
    insert_sampling(&mut body, request);
    if let Some(max_tokens) = request.max_tokens {
        body.insert("max_output_tokens".into(), json!(max_tokens));
    }
    if let Some(system) = system_value(&request.system, false) {
        body.insert("instructions".into(), system);
    }
    let mut input = Vec::new();
    for message in &request.messages {
        if message.role == Role::System {
            return Err(Fail::invalid("system"));
        }
        append_responses(message.role, &message.blocks, &mut input, false)?;
    }
    body.insert("input".into(), Value::Array(input));
    insert_tools(&mut body, Protocol::OpenAiResponses, &request.tools);
    if let Some(choice) = named_or_mode(request.tool_choice.as_ref(), false) {
        body.insert("tool_choice".into(), choice);
    }
    if let Some(parallel) = request.parallel_tool_calls {
        body.insert("parallel_tool_calls".into(), json!(parallel));
    }
    body.insert("stream".into(), json!(stream));
    Ok(Value::Object(body))
}
fn append_responses(
    role: Role,
    blocks: &[Block],
    out: &mut Vec<Value>,
    as_output: bool,
) -> Result<(), Fail> {
    let role_name = match role {
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::System => return Err(Fail::invalid("system")),
    };
    let mut content = Vec::new();
    for block in blocks {
        match block {
            Block::Text(text) => {
                if as_output {
                    content.push(json!({"type": "output_text", "text": text, "annotations": []}));
                } else {
                    content.push(json!({"type": "input_text", "text": text}));
                }
            }
            Block::Image { media_type, source } => {
                if as_output {
                    return Err(Fail::unsupported("image"));
                }
                // UNVERIFIED: Responses image input takes image_url as a string.
                content.push(json!({
                    "type": "input_image",
                    "image_url": image_string(media_type, source),
                }));
            }
            Block::ToolCall {
                id,
                name,
                arguments,
            } => {
                flush_responses(role_name, as_output, &mut content, out);
                out.push(json!({
                    "type": "function_call",
                    "call_id": id,
                    "name": name,
                    "arguments": arguments,
                }));
            }
            Block::ToolResult { id, output, .. } => {
                flush_responses(role_name, as_output, &mut content, out);
                out.push(json!({
                    "type": "function_call_output",
                    "call_id": id,
                    "output": output,
                }));
            }
        }
    }
    flush_responses(role_name, as_output, &mut content, out);
    Ok(())
}

fn flush_responses(role: &str, as_output: bool, content: &mut Vec<Value>, out: &mut Vec<Value>) {
    if content.is_empty() {
        return;
    }
    let parts = std::mem::take(content);
    let single_text = !as_output
        && parts.len() == 1
        && parts[0].get("type").and_then(Value::as_str) == Some("input_text");
    if single_text {
        let text = parts[0].get("text").cloned().unwrap_or(Value::Null);
        out.push(json!({"role": role, "content": text}));
        return;
    }
    if as_output {
        out.push(json!({
            "type": "message",
            "role": role,
            "status": "completed",
            "content": parts,
        }));
        return;
    }
    out.push(json!({"type": "message", "role": role, "content": parts}));
}
pub(super) fn parse_responses_response(value: &Value) -> Result<Completion, Fail> {
    let obj = object(value, "response")?;
    let mut blocks = Vec::new();
    if let Some(output) = obj.get("output") {
        let items = output.as_array().ok_or_else(|| Fail::invalid("output"))?;
        for item in items {
            blocks.extend(parse_responses_item(item)?.blocks);
        }
    }
    let has_tool = blocks
        .iter()
        .any(|block| matches!(block, Block::ToolCall { .. }));
    Ok(Completion {
        id: optional_string(obj, "id")?.unwrap_or_default(),
        stop: promote(stop_from_responses(obj, has_tool)?, &blocks),
        blocks,
        usage: usage_of(Protocol::OpenAiResponses, obj.get("usage")),
    })
}
pub(super) fn encode_responses_response(
    completion: &Completion,
    model: &str,
) -> Result<Value, Fail> {
    let incomplete = match completion.stop {
        StopReason::StopSequence => return Err(Fail::unsupported("stop_sequence")),
        StopReason::MaxTokens => Some("max_output_tokens"),
        StopReason::Refusal => Some("content_filter"),
        StopReason::EndTurn | StopReason::ToolUse => None,
    };
    let mut output = Vec::new();
    append_responses(Role::Assistant, &completion.blocks, &mut output, true)?;
    let mut body = json!({
        "id": prefixed(&completion.id, "resp_"),
        "object": "response",
        "status": if incomplete.is_some() { "incomplete" } else { "completed" },
        "model": model,
        "output": output,
        "usage": completion.usage.responses_json(),
    });
    // UNVERIFIED: incomplete_details.reason uses max_output_tokens and content_filter.
    if let Some(reason) = incomplete {
        body["incomplete_details"] = json!({"reason": reason});
    }
    Ok(body)
}
pub(crate) fn stop_from_responses(
    value: &Map<String, Value>,
    has_tool: bool,
) -> Result<StopReason, Fail> {
    match value.get("status").and_then(Value::as_str) {
        Some("incomplete") => {
            // UNVERIFIED: incomplete_details.reason values.
            let reason = value
                .get("incomplete_details")
                .and_then(|details| details.get("reason"))
                .and_then(Value::as_str)
                .unwrap_or("");
            match reason {
                "max_output_tokens" => Ok(StopReason::MaxTokens),
                "content_filter" => Ok(StopReason::Refusal),
                "" => Err(Fail::unsupported("incomplete")),
                other => Err(Fail::unsupported(other)),
            }
        }
        Some("completed") | None => {
            if has_tool {
                Ok(StopReason::ToolUse)
            } else {
                Ok(StopReason::EndTurn)
            }
        }
        Some("failed") | Some("cancelled") | Some("canceled") => Err(Fail::Upstream),
        Some(other) => Err(Fail::unsupported(other)),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::{encode_response, parse_response};
    use super::*;

    #[test]
    fn responses_tool_call_becomes_anthropic() {
        let raw = json!({
            "id": "resp_1",
            "status": "completed",
            "output": [{
                "type": "function_call",
                "call_id": "call_2",
                "name": "get_weather",
                "arguments": "{\"city\":\"Paris\"}",
                "status": "completed"
            }],
            "usage": {"input_tokens": 20, "output_tokens": 10, "input_tokens_details": {"cached_tokens": 6}}
        });
        let completion = parse_response(Protocol::OpenAiResponses, &raw).unwrap();
        assert_eq!(completion.stop, StopReason::ToolUse);
        assert_eq!(completion.usage.cache_read, 6);
        assert_eq!(completion.usage.cache_write, 0);
        let encoded =
            encode_response(Protocol::AnthropicMessages, &completion, "claude-route").unwrap();
        assert_eq!(encoded["content"][0]["type"], "tool_use");
        assert_eq!(encoded["content"][0]["input"]["city"], "Paris");
        assert_eq!(encoded["stop_reason"], "tool_use");
        assert_eq!(encoded["usage"]["cache_read_input_tokens"], 6);
    }
}
