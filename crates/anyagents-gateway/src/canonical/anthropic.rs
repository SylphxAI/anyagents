use serde_json::{Map, Value, json};

use crate::error::Fail;
use crate::route::Protocol;

use super::tools::{
    anthropic_tool_choice, choice_and_parallel, insert_tools, parse_tool_result, parse_tools,
};
use super::types::*;

const ANTHROPIC_KEYS: &[&str] = &[
    "model",
    "max_tokens",
    "system",
    "messages",
    "tools",
    "tool_choice",
    "stream",
    "temperature",
    "top_p",
    "stop_sequences",
];
pub(super) fn parse_anthropic_request(value: &Value) -> Result<Request, Fail> {
    let obj = object(value, "request")?;
    reject_unknown(obj, ANTHROPIC_KEYS)?;
    require_string(obj, "model")?;
    let mut messages = Vec::new();
    for message in required_array(obj, "messages")? {
        messages.push(parse_anthropic_message(message)?);
    }
    let system = match obj.get("system") {
        None | Some(Value::Null) => Vec::new(),
        Some(system) => parse_anthropic_system(system)?,
    };
    let (choice, parallel) = choice_and_parallel(Protocol::AnthropicMessages, obj)?;
    Ok(Request {
        system,
        messages,
        tools: parse_tools(obj, Protocol::AnthropicMessages)?,
        tool_choice: choice,
        parallel_tool_calls: parallel,
        max_tokens: one_max(obj, &["max_tokens"])?,
        temperature: number_field(obj, "temperature")?,
        top_p: number_field(obj, "top_p")?,
        stop: optional_stop(obj, "stop_sequences")?,
    })
}
fn parse_anthropic_message(value: &Value) -> Result<Message, Fail> {
    let obj = object(value, "message")?;
    reject_unknown(obj, &["role", "content"])?;
    let role = match required_string(obj, "role")?.as_str() {
        "user" => Role::User,
        "assistant" => Role::Assistant,
        "system" => return Err(Fail::unsupported("system")),
        other => return Err(Fail::unsupported(other)),
    };
    let content = obj.get("content").ok_or_else(|| Fail::invalid("content"))?;
    Ok(Message {
        role,
        blocks: parse_anthropic_blocks(content)?,
    })
}

fn parse_anthropic_blocks(value: &Value) -> Result<Vec<Block>, Fail> {
    match value {
        Value::String(text) => Ok(vec![Block::Text(text.clone())]),
        Value::Null => Ok(Vec::new()),
        Value::Array(blocks) => blocks.iter().map(parse_anthropic_block).collect(),
        _ => Err(Fail::invalid("content")),
    }
}

fn parse_anthropic_block(value: &Value) -> Result<Block, Fail> {
    let obj = object(value, "content")?;
    let kind = required_string(obj, "type")?;
    match kind.as_str() {
        "text" => {
            reject_unknown(obj, &["type", "text"])?;
            Ok(Block::Text(required_string(obj, "text")?))
        }
        "image" => parse_anthropic_image(obj),
        "tool_use" => {
            reject_unknown(obj, &["type", "id", "name", "input"])?;
            let input = obj.get("input").ok_or_else(|| Fail::invalid("input"))?;
            if !input.is_object() {
                return Err(Fail::invalid("input"));
            }
            Ok(Block::ToolCall {
                id: required_string(obj, "id")?,
                name: required_string(obj, "name")?,
                arguments: serde_json::to_string(input).map_err(|_| Fail::invalid("input"))?,
            })
        }
        "tool_result" => parse_tool_result(obj),
        other => Err(Fail::unsupported(other)),
    }
}

fn parse_anthropic_image(obj: &Map<String, Value>) -> Result<Block, Fail> {
    reject_unknown(obj, &["type", "source"])?;
    let source = object(
        obj.get("source").ok_or_else(|| Fail::invalid("source"))?,
        "source",
    )?;
    match required_string(source, "type")?.as_str() {
        "base64" => {
            reject_unknown(source, &["type", "media_type", "data"])?;
            Ok(Block::Image {
                media_type: required_string(source, "media_type")?,
                source: ImageSource::Base64(required_string(source, "data")?),
            })
        }
        "url" => {
            reject_unknown(source, &["type", "url"])?;
            Ok(Block::Image {
                media_type: String::new(),
                source: ImageSource::Url(required_string(source, "url")?),
            })
        }
        other => Err(Fail::unsupported(other)),
    }
}
fn parse_anthropic_system(value: &Value) -> Result<Vec<String>, Fail> {
    match value {
        Value::String(text) => Ok(vec![text.clone()]),
        Value::Array(blocks) => {
            let mut system = Vec::new();
            for block in blocks {
                let obj = object(block, "system")?;
                reject_unknown(obj, &["type", "text"])?;
                if required_string(obj, "type")? != "text" {
                    return Err(Fail::unsupported(required_string(obj, "type")?));
                }
                system.push(required_string(obj, "text")?);
            }
            Ok(system)
        }
        _ => Err(Fail::invalid("system")),
    }
}
pub(super) fn encode_anthropic_request(
    request: &Request,
    model: &str,
    stream: bool,
) -> Result<Value, Fail> {
    let max_tokens = request
        .max_tokens
        .ok_or_else(|| Fail::invalid("max_tokens is required by the messages API"))?;
    let mut body = Map::new();
    body.insert("model".into(), json!(model));
    body.insert("max_tokens".into(), json!(max_tokens));
    insert_sampling(&mut body, request);
    if let Some(system) = system_value(&request.system, true) {
        body.insert("system".into(), system);
    }
    let mut messages = Vec::new();
    for message in merge_turns(&request.messages) {
        let role = match message.role {
            Role::User => "user",
            Role::Assistant => "assistant",
            Role::System => return Err(Fail::invalid("system")),
        };
        messages.push(json!({
            "role": role,
            "content": anthropic_content(&message.blocks, true)?,
        }));
    }
    body.insert("messages".into(), Value::Array(messages));
    insert_tools(&mut body, Protocol::AnthropicMessages, &request.tools);
    if let Some(choice) =
        anthropic_tool_choice(request.tool_choice.as_ref(), request.parallel_tool_calls)
    {
        body.insert("tool_choice".into(), choice);
    }
    if !request.stop.is_empty() {
        body.insert("stop_sequences".into(), json!(request.stop));
    }
    body.insert("stream".into(), json!(stream));
    Ok(Value::Object(body))
}
pub(super) fn parse_anthropic_response(value: &Value) -> Result<Completion, Fail> {
    let obj = object(value, "response")?;
    let blocks = match obj.get("content") {
        None => Vec::new(),
        Some(content) => parse_anthropic_blocks(content)?,
    };
    let reason = obj
        .get("stop_reason")
        .and_then(Value::as_str)
        .ok_or_else(|| Fail::invalid("stop_reason"))?;
    Ok(Completion {
        id: optional_string(obj, "id")?.unwrap_or_default(),
        stop: promote(stop_from_anthropic(reason)?, &blocks),
        blocks,
        usage: usage_of(Protocol::AnthropicMessages, obj.get("usage")),
    })
}
pub(super) fn encode_anthropic_response(
    completion: &Completion,
    model: &str,
) -> Result<Value, Fail> {
    Ok(json!({
        "id": prefixed(&completion.id, "msg_"),
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": anthropic_content(&completion.blocks, false)?,
        "stop_reason": stop_to_anthropic(completion.stop),
        "stop_sequence": null,
        "usage": completion.usage.anthropic_json(),
    }))
}
pub(crate) fn stop_from_anthropic(reason: &str) -> Result<StopReason, Fail> {
    match reason {
        "end_turn" => Ok(StopReason::EndTurn),
        "max_tokens" => Ok(StopReason::MaxTokens),
        "stop_sequence" => Ok(StopReason::StopSequence),
        "tool_use" => Ok(StopReason::ToolUse),
        "refusal" => Ok(StopReason::Refusal),
        other => Err(Fail::unsupported(other)),
    }
}
pub(crate) fn stop_to_anthropic(reason: StopReason) -> &'static str {
    match reason {
        StopReason::EndTurn => "end_turn",
        StopReason::MaxTokens => "max_tokens",
        StopReason::StopSequence => "stop_sequence",
        StopReason::ToolUse => "tool_use",
        StopReason::Refusal => "refusal",
    }
}
fn anthropic_content(blocks: &[Block], allow_string: bool) -> Result<Value, Fail> {
    if allow_string
        && blocks.len() == 1
        && let Block::Text(text) = &blocks[0]
    {
        return Ok(Value::String(text.clone()));
    }
    let mut content = Vec::new();
    for block in blocks {
        content.push(anthropic_block(block)?);
    }
    Ok(Value::Array(content))
}

fn anthropic_block(block: &Block) -> Result<Value, Fail> {
    match block {
        Block::Text(text) => Ok(json!({"type": "text", "text": text})),
        Block::Image { media_type, source } => Ok(anthropic_image(media_type, source)),
        Block::ToolCall {
            id,
            name,
            arguments,
        } => {
            let input: Value =
                serde_json::from_str(arguments).map_err(|_| Fail::invalid("input"))?;
            if !input.is_object() {
                return Err(Fail::invalid("input"));
            }
            Ok(json!({"type": "tool_use", "id": id, "name": name, "input": input}))
        }
        Block::ToolResult { id, output, .. } => Ok(json!({
            "type": "tool_result",
            "tool_use_id": id,
            "content": output,
        })),
    }
}

fn anthropic_image(media_type: &str, source: &ImageSource) -> Value {
    match source {
        ImageSource::Base64(data) => json!({
            "type": "image",
            "source": {"type": "base64", "media_type": media_type, "data": data},
        }),
        ImageSource::Url(url) => json!({
            "type": "image",
            "source": {"type": "url", "url": url},
        }),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::{encode_request, encode_response, parse_request, parse_response};
    use super::*;

    #[test]
    fn names_unsupported_features() {
        let thinking = json!({
            "model": "claude-client",
            "max_tokens": 8,
            "messages": [{"role": "user", "content": [{"type": "thinking", "thinking": "hidden"}]}]
        });
        let err = parse_request(Protocol::AnthropicMessages, &thinking).unwrap_err();
        assert!(err.to_string().contains("thinking"), "{err}");

        let detail = json!({
            "model": "gpt-client",
            "messages": [{"role": "user", "content": [{
                "type": "image_url",
                "image_url": {"url": "https://example.com/a.png", "detail": "high"}
            }]}]
        });
        let err = parse_request(Protocol::OpenAiChat, &detail).unwrap_err();
        assert!(err.to_string().contains("image_detail"), "{err}");

        let metadata = json!({"model": "gpt-client", "metadata": {}, "messages": []});
        let err = parse_request(Protocol::OpenAiChat, &metadata).unwrap_err();
        assert!(err.to_string().contains("metadata"), "{err}");

        assert!(
            stop_from_anthropic("pause_turn")
                .unwrap_err()
                .to_string()
                .contains("pause_turn")
        );
        let request = parse_request(
            Protocol::AnthropicMessages,
            &json!({
                "model": "claude-client",
                "max_tokens": 4,
                "stop_sequences": ["END"],
                "messages": [{"role": "user", "content": "Hi"}]
            }),
        )
        .unwrap();
        let err =
            encode_request(Protocol::OpenAiResponses, &request, "gpt-route", false).unwrap_err();
        assert!(err.to_string().contains("stop_sequences"), "{err}");
    }
    #[test]
    fn chat_without_max_tokens_cannot_become_messages() {
        let request = parse_request(
            Protocol::OpenAiChat,
            &json!({"model": "gpt-client", "messages": [{"role": "user", "content": "Hi"}]}),
        )
        .unwrap();
        let err = encode_request(Protocol::AnthropicMessages, &request, "claude-route", false)
            .unwrap_err();
        assert!(err.to_string().contains("max_tokens"), "{err}");
    }
    #[test]
    fn cache_write_survives_on_the_event_and_not_the_chat_body() {
        let raw = json!({
            "id": "msg_1",
            "content": [{"type": "text", "text": "Hi there"}],
            "stop_reason": "end_turn",
            "usage": {
                "input_tokens": 11,
                "output_tokens": 4,
                "cache_read_input_tokens": 2,
                "cache_creation_input_tokens": 3
            }
        });
        let completion = parse_response(Protocol::AnthropicMessages, &raw).unwrap();
        assert_eq!(completion.usage.cache_write, 3);
        let encoded = encode_response(Protocol::OpenAiChat, &completion, "gpt-route").unwrap();
        assert_eq!(
            encoded["usage"]["prompt_tokens_details"]["cached_tokens"],
            2
        );
        let text = encoded.to_string();
        assert!(!text.contains("cache_creation"));
        assert_eq!(encoded["choices"][0]["message"]["content"], "Hi there");
        assert_eq!(encoded["choices"][0]["finish_reason"], "stop");
    }
}
