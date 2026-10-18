use serde_json::{Map, Value, json};

use crate::error::Fail;
use crate::route::Protocol;

use super::types::*;

pub(super) fn parse_tool_result(obj: &Map<String, Value>) -> Result<Block, Fail> {
    reject_unknown(obj, &["type", "tool_use_id", "content", "is_error"])?;
    match obj.get("is_error") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => {}
        Some(Value::Bool(true)) => return Err(Fail::unsupported("is_error")),
        Some(_) => return Err(Fail::invalid("is_error")),
    }
    let output = match obj.get("content") {
        None | Some(Value::Null) => String::new(),
        Some(content) => tool_text(content)?,
    };
    Ok(Block::ToolResult {
        id: required_string(obj, "tool_use_id")?,
        name: None,
        output,
    })
}
pub(super) fn parse_chat_tool_call(value: &Value) -> Result<Block, Fail> {
    let obj = object(value, "tool_calls")?;
    reject_unknown(obj, &["id", "type", "function"])?;
    let kind = required_string(obj, "type")?;
    if kind != "function" {
        return Err(Fail::unsupported(kind));
    }
    let function = object(
        obj.get("function")
            .ok_or_else(|| Fail::invalid("function"))?,
        "function",
    )?;
    reject_unknown(function, &["name", "arguments"])?;
    Ok(Block::ToolCall {
        id: required_string(obj, "id")?,
        name: required_string(function, "name")?,
        arguments: argument_string(function.get("arguments"))?,
    })
}
pub(super) fn parse_tools(obj: &Map<String, Value>, protocol: Protocol) -> Result<Vec<Tool>, Fail> {
    let Some(tools) = optional_array(obj, "tools")? else {
        return Ok(Vec::new());
    };
    tools
        .iter()
        .map(|tool| parse_tool(protocol, tool))
        .collect()
}

fn parse_tool(protocol: Protocol, value: &Value) -> Result<Tool, Fail> {
    let obj = object(value, "tools")?;
    match protocol {
        Protocol::AnthropicMessages => {
            if let Some(kind) = obj.get("type").and_then(Value::as_str) {
                return Err(Fail::unsupported(kind));
            }
            reject_unknown(obj, &["name", "description", "input_schema"])?;
            let parameters = obj
                .get("input_schema")
                .filter(|value| value.is_object())
                .cloned()
                .ok_or_else(|| Fail::invalid("input_schema"))?;
            Ok(Tool {
                name: required_string(obj, "name")?,
                description: optional_string(obj, "description")?,
                parameters,
            })
        }
        Protocol::OpenAiChat => {
            let kind = required_string(obj, "type")?;
            if kind != "function" {
                return Err(Fail::unsupported(kind));
            }
            reject_unknown(obj, &["type", "function"])?;
            let function = object(
                obj.get("function")
                    .ok_or_else(|| Fail::invalid("function"))?,
                "function",
            )?;
            reject_unknown(function, &["name", "description", "parameters"])?;
            Ok(Tool {
                name: required_string(function, "name")?,
                description: optional_string(function, "description")?,
                parameters: function
                    .get("parameters")
                    .filter(|value| value.is_object())
                    .cloned()
                    .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
            })
        }
        Protocol::OpenAiResponses => {
            let kind = required_string(obj, "type")?;
            if kind != "function" {
                return Err(Fail::unsupported(kind));
            }
            reject_unknown(obj, &["type", "name", "description", "parameters"])?;
            Ok(Tool {
                name: required_string(obj, "name")?,
                description: optional_string(obj, "description")?,
                parameters: obj
                    .get("parameters")
                    .filter(|value| value.is_object())
                    .cloned()
                    .unwrap_or_else(|| json!({"type": "object", "properties": {}})),
            })
        }
    }
}

pub(super) fn choice_and_parallel(
    protocol: Protocol,
    obj: &Map<String, Value>,
) -> Result<(Option<ToolChoice>, Option<bool>), Fail> {
    let mut parallel = optional_bool(obj, "parallel_tool_calls")?;
    let choice = match obj.get("tool_choice") {
        None | Some(Value::Null) => None,
        Some(value) => {
            if let Some(from_choice) = parallel_from_choice(value)? {
                if parallel.is_some() && parallel != Some(from_choice) {
                    return Err(Fail::invalid("parallel_tool_calls"));
                }
                parallel = Some(from_choice);
            }
            Some(parse_tool_choice(protocol, value)?)
        }
    };
    Ok((choice, parallel))
}

fn parallel_from_choice(value: &Value) -> Result<Option<bool>, Fail> {
    let Some(obj) = value.as_object() else {
        return Ok(None);
    };
    match obj.get("disable_parallel_tool_use") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => Ok(None),
        Some(Value::Bool(true)) => Ok(Some(false)),
        Some(_) => Err(Fail::invalid("disable_parallel_tool_use")),
    }
}

fn parse_tool_choice(protocol: Protocol, value: &Value) -> Result<ToolChoice, Fail> {
    match value {
        Value::String(choice) => choice_name(choice),
        Value::Object(obj) => choice_object(protocol, obj),
        _ => Err(Fail::invalid("tool_choice")),
    }
}

fn choice_name(value: &str) -> Result<ToolChoice, Fail> {
    match value {
        "auto" => Ok(ToolChoice::Auto),
        "none" => Ok(ToolChoice::None),
        "required" | "any" => Ok(ToolChoice::Required),
        other => Err(Fail::unsupported(other)),
    }
}

fn choice_object(protocol: Protocol, obj: &Map<String, Value>) -> Result<ToolChoice, Fail> {
    let kind = required_string(obj, "type")?;
    match kind.as_str() {
        "auto" => {
            reject_unknown(obj, &["type", "disable_parallel_tool_use"])?;
            Ok(ToolChoice::Auto)
        }
        "none" => {
            reject_unknown(obj, &["type", "disable_parallel_tool_use"])?;
            Ok(ToolChoice::None)
        }
        "any" | "required" => {
            reject_unknown(obj, &["type", "disable_parallel_tool_use"])?;
            Ok(ToolChoice::Required)
        }
        "tool" => {
            reject_unknown(obj, &["type", "name", "disable_parallel_tool_use"])?;
            Ok(ToolChoice::Named(required_string(obj, "name")?))
        }
        "function" if protocol == Protocol::OpenAiResponses || obj.contains_key("name") => {
            reject_unknown(obj, &["type", "name"])?;
            Ok(ToolChoice::Named(required_string(obj, "name")?))
        }
        "function" => {
            reject_unknown(obj, &["type", "function"])?;
            let function = object(
                obj.get("function")
                    .ok_or_else(|| Fail::invalid("function"))?,
                "function",
            )?;
            reject_unknown(function, &["name"])?;
            Ok(ToolChoice::Named(required_string(function, "name")?))
        }
        other => Err(Fail::unsupported(other)),
    }
}
pub(super) fn chat_tool_calls(blocks: &[Block]) -> Vec<Value> {
    blocks
        .iter()
        .filter_map(|block| match block {
            Block::ToolCall {
                id,
                name,
                arguments,
            } => Some(json!({
                "id": id,
                "type": "function",
                "function": {"name": name, "arguments": arguments},
            })),
            _ => None,
        })
        .collect()
}
pub(super) fn anthropic_tool_choice(
    choice: Option<&ToolChoice>,
    parallel: Option<bool>,
) -> Option<Value> {
    let disable = parallel == Some(false);
    let choice = choice.unwrap_or(&ToolChoice::Auto);
    let mut value = match choice {
        ToolChoice::Auto if !disable => return None,
        ToolChoice::Auto => json!({"type": "auto"}),
        ToolChoice::None => json!({"type": "none"}),
        ToolChoice::Required => json!({"type": "any"}),
        ToolChoice::Named(name) => json!({"type": "tool", "name": name}),
    };
    if disable {
        value["disable_parallel_tool_use"] = json!(true);
    }
    Some(value)
}

pub(super) fn named_or_mode(choice: Option<&ToolChoice>, nested: bool) -> Option<Value> {
    match choice? {
        ToolChoice::Auto => None,
        ToolChoice::None => Some(json!("none")),
        ToolChoice::Required => Some(json!("required")),
        ToolChoice::Named(name) if nested => {
            Some(json!({"type": "function", "function": {"name": name}}))
        }
        ToolChoice::Named(name) => Some(json!({"type": "function", "name": name})),
    }
}

fn encode_tool(protocol: Protocol, tool: &Tool) -> Value {
    match protocol {
        Protocol::AnthropicMessages => {
            let mut value = json!({"name": tool.name, "input_schema": tool.parameters});
            if let Some(description) = &tool.description {
                value["description"] = json!(description);
            }
            value
        }
        Protocol::OpenAiChat => {
            let mut function = json!({"name": tool.name, "parameters": tool.parameters});
            if let Some(description) = &tool.description {
                function["description"] = json!(description);
            }
            json!({"type": "function", "function": function})
        }
        Protocol::OpenAiResponses => {
            let mut value = json!({
                "type": "function",
                "name": tool.name,
                "parameters": tool.parameters,
            });
            if let Some(description) = &tool.description {
                value["description"] = json!(description);
            }
            value
        }
    }
}

pub(super) fn insert_tools(body: &mut Map<String, Value>, protocol: Protocol, tools: &[Tool]) {
    if tools.is_empty() {
        return;
    }
    body.insert(
        "tools".into(),
        Value::Array(
            tools
                .iter()
                .map(|tool| encode_tool(protocol, tool))
                .collect(),
        ),
    );
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::{encode_response, parse_response};
    use super::*;

    #[test]
    fn tool_call_responses_translate_both_ways() {
        let anthropic = json!({
            "id": "msg_9",
            "role": "assistant",
            "type": "message",
            "model": "claude-client",
            "content": [
                {"type": "text", "text": "Checking."},
                {
                    "type": "tool_use",
                    "id": "call_9",
                    "name": "get_weather",
                    "input": {"city": "Paris"}
                }
            ],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 5, "output_tokens": 7}
        });
        let completion = parse_response(Protocol::AnthropicMessages, &anthropic).unwrap();
        assert_eq!(completion.stop, StopReason::ToolUse);
        let chat = encode_response(Protocol::OpenAiChat, &completion, "gpt-route").unwrap();
        assert_eq!(chat["object"], "chat.completion");
        assert_eq!(chat["choices"][0]["finish_reason"], "tool_calls");
        assert_eq!(chat["choices"][0]["message"]["content"], "Checking.");
        assert_eq!(
            chat["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
            "{\"city\":\"Paris\"}"
        );
        assert_eq!(chat["usage"]["prompt_tokens"], 5);
        assert_eq!(chat["usage"]["completion_tokens"], 7);

        let back = parse_response(Protocol::OpenAiChat, &chat).unwrap();
        let restored = encode_response(Protocol::AnthropicMessages, &back, "claude-route").unwrap();
        assert_eq!(restored["stop_reason"], "tool_use");
        assert_eq!(restored["content"][0]["text"], "Checking.");
        assert_eq!(restored["content"][1]["type"], "tool_use");
        assert_eq!(restored["content"][1]["id"], "call_9");
        assert_eq!(restored["content"][1]["name"], "get_weather");
        assert_eq!(restored["content"][1]["input"]["city"], "Paris");
        assert_eq!(restored["usage"]["input_tokens"], 5);
        assert_eq!(restored["usage"]["output_tokens"], 7);
    }
}
