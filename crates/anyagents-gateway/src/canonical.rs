//! One request and response shape, with a parser and emitter for each protocol.
//!
//! Same-protocol calls do not come through here: the forwarder replaces the model
//! and proxies the bytes. Translation uses this module so a feature one side
//! cannot represent is rejected by name instead of being dropped.

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
enum Role {
    System,
    User,
    Assistant,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ImageSource {
    Base64(String),
    Url(String),
}

#[derive(Clone, Debug, PartialEq)]
enum Block {
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
struct Tool {
    name: String,
    description: Option<String>,
    parameters: Value,
}

#[derive(Clone, Debug, PartialEq)]
enum ToolChoice {
    Auto,
    None,
    Required,
    Named(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Request {
    system: Vec<String>,
    messages: Vec<Message>,
    tools: Vec<Tool>,
    tool_choice: Option<ToolChoice>,
    parallel_tool_calls: Option<bool>,
    max_tokens: Option<u64>,
    temperature: Option<Value>,
    top_p: Option<Value>,
    stop: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct Message {
    role: Role,
    blocks: Vec<Block>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Completion {
    pub id: String,
    blocks: Vec<Block>,
    pub stop: StopReason,
    pub(crate) usage: TokenUsage,
}

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

pub fn parse_request(protocol: Protocol, value: &Value) -> Result<Request, Fail> {
    match protocol {
        Protocol::AnthropicMessages => parse_anthropic_request(value),
        Protocol::OpenAiChat => parse_chat_request(value),
        Protocol::OpenAiResponses => parse_responses_request(value),
    }
}

pub fn encode_request(
    protocol: Protocol,
    request: &Request,
    model: &str,
    stream: bool,
) -> Result<Value, Fail> {
    if protocol == Protocol::OpenAiResponses && !request.stop.is_empty() {
        return Err(Fail::unsupported("stop_sequences"));
    }
    match protocol {
        Protocol::AnthropicMessages => encode_anthropic_request(request, model, stream),
        Protocol::OpenAiChat => encode_chat_request(request, model, stream),
        Protocol::OpenAiResponses => encode_responses_request(request, model, stream),
    }
}

pub fn parse_response(protocol: Protocol, value: &Value) -> Result<Completion, Fail> {
    match protocol {
        Protocol::AnthropicMessages => parse_anthropic_response(value),
        Protocol::OpenAiChat => parse_chat_response(value),
        Protocol::OpenAiResponses => parse_responses_response(value),
    }
}

pub fn encode_response(
    protocol: Protocol,
    completion: &Completion,
    model: &str,
) -> Result<Value, Fail> {
    match protocol {
        Protocol::AnthropicMessages => encode_anthropic_response(completion, model),
        Protocol::OpenAiChat => encode_chat_response(completion, model),
        Protocol::OpenAiResponses => encode_responses_response(completion, model),
    }
}

fn parse_anthropic_request(value: &Value) -> Result<Request, Fail> {
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

fn parse_chat_request(value: &Value) -> Result<Request, Fail> {
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

fn parse_responses_request(value: &Value) -> Result<Request, Fail> {
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

fn parse_tool_result(obj: &Map<String, Value>) -> Result<Block, Fail> {
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

fn parse_chat_tool_call(value: &Value) -> Result<Block, Fail> {
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

fn parse_tools(obj: &Map<String, Value>, protocol: Protocol) -> Result<Vec<Tool>, Fail> {
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

fn choice_and_parallel(
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

fn encode_anthropic_request(request: &Request, model: &str, stream: bool) -> Result<Value, Fail> {
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

fn encode_chat_request(request: &Request, model: &str, stream: bool) -> Result<Value, Fail> {
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

fn encode_responses_request(request: &Request, model: &str, stream: bool) -> Result<Value, Fail> {
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

fn chat_tool_calls(blocks: &[Block]) -> Vec<Value> {
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

fn parse_anthropic_response(value: &Value) -> Result<Completion, Fail> {
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

fn parse_chat_response(value: &Value) -> Result<Completion, Fail> {
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

fn parse_responses_response(value: &Value) -> Result<Completion, Fail> {
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

fn encode_anthropic_response(completion: &Completion, model: &str) -> Result<Value, Fail> {
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

fn encode_chat_response(completion: &Completion, model: &str) -> Result<Value, Fail> {
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

fn encode_responses_response(completion: &Completion, model: &str) -> Result<Value, Fail> {
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

pub(crate) fn stop_to_anthropic(reason: StopReason) -> &'static str {
    match reason {
        StopReason::EndTurn => "end_turn",
        StopReason::MaxTokens => "max_tokens",
        StopReason::StopSequence => "stop_sequence",
        StopReason::ToolUse => "tool_use",
        StopReason::Refusal => "refusal",
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

fn promote(stop: StopReason, blocks: &[Block]) -> StopReason {
    let has_tool = blocks
        .iter()
        .any(|block| matches!(block, Block::ToolCall { .. }));
    if has_tool && stop == StopReason::EndTurn {
        StopReason::ToolUse
    } else {
        stop
    }
}

fn usage_of(protocol: Protocol, usage: Option<&Value>) -> TokenUsage {
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

fn anthropic_tool_choice(choice: Option<&ToolChoice>, parallel: Option<bool>) -> Option<Value> {
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

fn named_or_mode(choice: Option<&ToolChoice>, nested: bool) -> Option<Value> {
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

fn insert_tools(body: &mut Map<String, Value>, protocol: Protocol, tools: &[Tool]) {
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

fn insert_sampling(body: &mut Map<String, Value>, request: &Request) {
    if let Some(temperature) = &request.temperature {
        body.insert("temperature".into(), temperature.clone());
    }
    if let Some(top_p) = &request.top_p {
        body.insert("top_p".into(), top_p.clone());
    }
}

fn system_value(parts: &[String], anthropic: bool) -> Option<Value> {
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

fn merge_turns(messages: &[Message]) -> Vec<Message> {
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

fn take_system(messages: Vec<Message>) -> Result<(Vec<String>, Vec<Message>), Fail> {
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

fn image_from_url(url: &str) -> Result<Block, Fail> {
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

fn image_string(media_type: &str, source: &ImageSource) -> String {
    match source {
        ImageSource::Url(url) => url.clone(),
        ImageSource::Base64(data) => format!("data:{media_type};base64,{data}"),
    }
}

fn tool_text(value: &Value) -> Result<String, Fail> {
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

fn argument_string(value: Option<&Value>) -> Result<String, Fail> {
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

fn call_id(obj: &Map<String, Value>) -> Result<String, Fail> {
    optional_string(obj, "call_id")?
        .or(optional_string(obj, "id")?)
        .ok_or_else(|| Fail::invalid("call_id"))
}

fn reject_detail(obj: &Map<String, Value>) -> Result<(), Fail> {
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

fn object<'a>(value: &'a Value, what: &str) -> Result<&'a Map<String, Value>, Fail> {
    value.as_object().ok_or_else(|| Fail::invalid(what))
}

fn required_string(obj: &Map<String, Value>, key: &str) -> Result<String, Fail> {
    obj.get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| Fail::invalid(key))
}

fn optional_string(obj: &Map<String, Value>, key: &str) -> Result<Option<String>, Fail> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(Fail::invalid(key)),
    }
}

fn require_string(obj: &Map<String, Value>, key: &str) -> Result<(), Fail> {
    match obj.get(key) {
        None => Ok(()),
        Some(Value::String(_)) => Ok(()),
        Some(_) => Err(Fail::invalid(key)),
    }
}

fn required_array<'a>(obj: &'a Map<String, Value>, key: &str) -> Result<&'a Vec<Value>, Fail> {
    obj.get(key)
        .ok_or_else(|| Fail::invalid(key))?
        .as_array()
        .ok_or_else(|| Fail::invalid(key))
}

fn optional_array<'a>(
    obj: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a Vec<Value>>, Fail> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => Ok(Some(value.as_array().ok_or_else(|| Fail::invalid(key))?)),
    }
}

fn optional_bool(obj: &Map<String, Value>, key: &str) -> Result<Option<bool>, Fail> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(Fail::invalid(key)),
    }
}

fn number_field(obj: &Map<String, Value>, key: &str) -> Result<Option<Value>, Fail> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) if value.is_number() => Ok(Some(value.clone())),
        Some(_) => Err(Fail::invalid(key)),
    }
}

fn one_max(obj: &Map<String, Value>, keys: &[&str]) -> Result<Option<u64>, Fail> {
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

fn optional_stop(obj: &Map<String, Value>, key: &str) -> Result<Vec<String>, Fail> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tool_request() -> Value {
        json!({
            "model": "claude-client",
            "max_tokens": 64,
            "temperature": 0.2,
            "system": "Use tools.",
            "tools": [{
                "name": "get_weather",
                "description": "Weather",
                "input_schema": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}, "units": {"type": "string"}},
                    "required": ["city"]
                }
            }],
            "tool_choice": {"type": "auto", "disable_parallel_tool_use": true},
            "messages": [
                {"role": "user", "content": [
                    {"type": "text", "text": "Weather in Paris?"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "aaaa"}}
                ]},
                {"role": "assistant", "content": [{
                    "type": "tool_use",
                    "id": "call_1",
                    "name": "get_weather",
                    "input": {"city": "Paris", "units": "c"}
                }]},
                {"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": "call_1",
                    "content": "sunny"
                }]}
            ]
        })
    }

    #[test]
    fn anthropic_request_becomes_chat_and_responses() {
        let request = parse_request(Protocol::AnthropicMessages, &tool_request()).unwrap();
        let chat = encode_request(Protocol::OpenAiChat, &request, "gpt-route", false).unwrap();
        assert_eq!(chat["model"], "gpt-route");
        assert_eq!(chat["messages"][0]["content"], "Use tools.");
        assert_eq!(
            chat["messages"][1]["content"][1]["image_url"]["url"],
            "data:image/png;base64,aaaa"
        );
        assert_eq!(
            chat["messages"][2]["tool_calls"][0]["function"]["name"],
            "get_weather"
        );
        assert_eq!(
            chat["messages"][2]["tool_calls"][0]["function"]["arguments"],
            "{\"city\":\"Paris\",\"units\":\"c\"}"
        );
        assert_eq!(chat["messages"][3]["role"], "tool");
        assert_eq!(chat["messages"][3]["content"], "sunny");
        assert_eq!(chat["parallel_tool_calls"], false);
        assert_eq!(chat["temperature"], 0.2);

        let responses =
            encode_request(Protocol::OpenAiResponses, &request, "gpt-route", true).unwrap();
        assert_eq!(responses["instructions"], "Use tools.");
        assert_eq!(
            responses["input"][0]["content"][1]["image_url"],
            "data:image/png;base64,aaaa"
        );
        assert_eq!(responses["input"][1]["type"], "function_call");
        assert_eq!(responses["input"][2]["output"], "sunny");
        assert_eq!(responses["max_output_tokens"], 64);
        assert_eq!(responses["stream"], true);
    }

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

    #[test]
    fn anthropic_tool_turn_round_trips_through_chat() {
        let request = parse_request(Protocol::AnthropicMessages, &tool_request()).unwrap();
        let chat = encode_request(Protocol::OpenAiChat, &request, "gpt-route", false).unwrap();
        assert_eq!(chat["stream"], false);
        let back = parse_request(Protocol::OpenAiChat, &chat).unwrap();
        let anthropic =
            encode_request(Protocol::AnthropicMessages, &back, "claude-route", false).unwrap();
        assert_eq!(anthropic["model"], "claude-route");
        assert_eq!(anthropic["max_tokens"], 64);
        assert_eq!(anthropic["system"], "Use tools.");
        assert_eq!(anthropic["stream"], false);
        assert_eq!(anthropic["tools"][0]["name"], "get_weather");
        assert_eq!(
            anthropic["tools"][0]["input_schema"]["properties"]["city"]["type"],
            "string"
        );
        assert_eq!(
            anthropic["tool_choice"],
            json!({"type": "auto", "disable_parallel_tool_use": true})
        );
        assert_eq!(
            anthropic["messages"][0]["content"][0]["text"],
            "Weather in Paris?"
        );
        assert_eq!(
            anthropic["messages"][0]["content"][1]["source"]["data"],
            "aaaa"
        );
        assert_eq!(anthropic["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(anthropic["messages"][1]["content"][0]["id"], "call_1");
        assert_eq!(
            anthropic["messages"][1]["content"][0]["name"],
            "get_weather"
        );
        assert_eq!(
            anthropic["messages"][1]["content"][0]["input"],
            json!({"city": "Paris", "units": "c"})
        );
        assert_eq!(
            anthropic["messages"][2]["content"][0]["type"],
            "tool_result"
        );
        assert_eq!(
            anthropic["messages"][2]["content"][0]["tool_use_id"],
            "call_1"
        );
        assert_eq!(anthropic["messages"][2]["content"][0]["content"], "sunny");
    }

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
