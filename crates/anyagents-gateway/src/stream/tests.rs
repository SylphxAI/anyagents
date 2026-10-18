use serde_json::{Value, json};

use crate::route::Protocol;
use crate::sse::{ByteSse, sse_data, sse_done, sse_event};
use crate::usage::TokenUsage;

use super::StreamX;

fn run(from: Protocol, to: Protocol, model: &str, recorded: &str) -> (String, TokenUsage) {
    let mut stream = StreamX::between(from, to, model).unwrap();
    let mut out = stream.push(recorded.as_bytes()).unwrap();
    let (tail, usage) = stream.finish().unwrap();
    out.push_str(&tail);
    (out, usage)
}

fn json_frames(raw: &str) -> Vec<(Option<String>, Value)> {
    let mut parser = ByteSse::default();
    let mut frames = parser.push(raw.as_bytes());
    frames.extend(parser.finish());
    frames
        .into_iter()
        .map(|frame| {
            if frame.data.trim() == "[DONE]" {
                (frame.event, json!("[DONE]"))
            } else {
                let value = serde_json::from_str(&frame.data).expect("event json");
                (frame.event, value)
            }
        })
        .collect()
}

fn anthropic(kind: &str, value: Value) -> String {
    sse_event(kind, &value)
}

fn anthropic_text() -> String {
    let mut raw = String::new();
    raw.push_str(&anthropic(
        "message_start",
        json!({
            "type": "message_start",
            "message": {
                "id": "msg_01",
                "type": "message",
                "role": "assistant",
                "model": "claude-src",
                "content": [],
                "stop_reason": null,
                "stop_sequence": null,
                "usage": {
                    "input_tokens": 11,
                    "output_tokens": 1,
                    "cache_read_input_tokens": 2,
                    "cache_creation_input_tokens": 3
                }
            }
        }),
    ));
    raw.push_str(&anthropic(
        "content_block_start",
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }),
    ));
    raw.push_str(&anthropic("ping", json!({"type": "ping"})));
    raw.push_str(&anthropic(
        "content_block_delta",
        json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "Hello"}
        }),
    ));
    raw.push_str(&anthropic(
        "content_block_delta",
        json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": " there"}
        }),
    ));
    raw.push_str(&anthropic(
        "content_block_stop",
        json!({"type": "content_block_stop", "index": 0}),
    ));
    raw.push_str(&anthropic(
        "message_delta",
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": null},
            "usage": {"output_tokens": 4}
        }),
    ));
    raw.push_str(&anthropic("message_stop", json!({"type": "message_stop"})));
    raw
}

fn anthropic_tool() -> String {
    let mut raw = anthropic(
        "message_start",
        json!({
            "type": "message_start",
            "message": {
                "id": "msg_02",
                "type": "message",
                "role": "assistant",
                "model": "claude-src",
                "content": [],
                "stop_reason": null,
                "usage": {"input_tokens": 20, "output_tokens": 1}
            }
        }),
    );
    raw.push_str(&anthropic(
        "content_block_start",
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }),
    ));
    raw.push_str(&anthropic(
        "content_block_delta",
        json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "Checking."}
        }),
    ));
    raw.push_str(&anthropic(
        "content_block_stop",
        json!({"type": "content_block_stop", "index": 0}),
    ));
    raw.push_str(&anthropic(
        "content_block_start",
        json!({
            "type": "content_block_start",
            "index": 1,
            "content_block": {
                "type": "tool_use",
                "id": "toolu_01",
                "name": "get_weather",
                "input": {}
            }
        }),
    ));
    raw.push_str(&anthropic(
        "content_block_delta",
        json!({
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": "{\"city\":"}
        }),
    ));
    raw.push_str(&anthropic(
        "content_block_delta",
        json!({
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": "\"Paris\"}"}
        }),
    ));
    raw.push_str(&anthropic(
        "content_block_stop",
        json!({"type": "content_block_stop", "index": 1}),
    ));
    raw.push_str(&anthropic(
        "message_delta",
        json!({
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use", "stop_sequence": null},
            "usage": {"output_tokens": 12}
        }),
    ));
    raw.push_str(&anthropic("message_stop", json!({"type": "message_stop"})));
    raw
}

fn chat_piece(id: &str, delta: Value, finish: Value, usage: Option<Value>) -> String {
    let mut body = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": 1710000000,
        "model": "gpt-src",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]
    });
    if let Some(usage) = usage {
        body["usage"] = usage;
    }
    sse_data(&body)
}

fn chat_text() -> String {
    let mut raw = chat_piece(
        "chatcmpl-abc",
        json!({"role": "assistant", "content": ""}),
        Value::Null,
        None,
    );
    raw.push_str(&chat_piece(
        "chatcmpl-abc",
        json!({"content": "Hello"}),
        Value::Null,
        None,
    ));
    raw.push_str(&chat_piece(
        "chatcmpl-abc",
        json!({"content": " there"}),
        Value::Null,
        None,
    ));
    raw.push_str(&chat_piece(
        "chatcmpl-abc",
        json!({}),
        json!("stop"),
        Some(json!({
            "prompt_tokens": 11,
            "completion_tokens": 4,
            "total_tokens": 15,
            "prompt_tokens_details": {"cached_tokens": 2}
        })),
    ));
    raw.push_str(&sse_done());
    raw
}

fn chat_tool() -> String {
    let mut raw = chat_piece(
        "chatcmpl-tool",
        json!({"role": "assistant", "content": null}),
        Value::Null,
        None,
    );
    raw.push_str(&chat_piece(
        "chatcmpl-tool",
        json!({"content": "Checking."}),
        Value::Null,
        None,
    ));
    raw.push_str(&chat_piece(
        "chatcmpl-tool",
        json!({
            "tool_calls": [{
                "index": 0,
                "id": "call_abc",
                "type": "function",
                "function": {"name": "get_weather", "arguments": ""}
            }]
        }),
        Value::Null,
        None,
    ));
    raw.push_str(&chat_piece(
        "chatcmpl-tool",
        json!({
            "tool_calls": [{"index": 0, "function": {"arguments": "{\"city\":"}}]
        }),
        Value::Null,
        None,
    ));
    raw.push_str(&chat_piece(
        "chatcmpl-tool",
        json!({
            "tool_calls": [{"index": 0, "function": {"arguments": "\"Paris\"}"}}]
        }),
        Value::Null,
        None,
    ));
    raw.push_str(&chat_piece(
        "chatcmpl-tool",
        json!({}),
        json!("tool_calls"),
        Some(json!({
            "prompt_tokens": 20,
            "completion_tokens": 12,
            "total_tokens": 32
        })),
    ));
    raw.push_str(&sse_done());
    raw
}

fn events(frames: &[(Option<String>, Value)]) -> Vec<&str> {
    frames
        .iter()
        .map(|(event, _)| event.as_deref().unwrap_or("[DONE]"))
        .collect()
}

#[test]
fn anthropic_text_stream_becomes_chat_chunks() {
    let (out, usage) = run(
        Protocol::AnthropicMessages,
        Protocol::OpenAiChat,
        "gpt-route",
        &anthropic_text(),
    );
    let frames = json_frames(&out);
    assert_eq!(frames.len(), 5, "{out}");
    assert!(frames.iter().all(|(event, _)| event.is_none()));

    let role = &frames[0].1;
    assert_eq!(role["id"], "chatcmpl_msg_01");
    assert_eq!(role["object"], "chat.completion.chunk");
    assert_eq!(role["created"], 0);
    assert_eq!(role["model"], "gpt-route");
    assert_eq!(role["choices"][0]["delta"], json!({"role": "assistant"}));
    assert_eq!(role["choices"][0]["finish_reason"], Value::Null);
    assert!(role.get("usage").is_none());

    assert_eq!(frames[1].1["choices"][0]["delta"]["content"], "Hello");
    assert!(frames[1].1.get("usage").is_none());
    assert_eq!(frames[2].1["choices"][0]["delta"]["content"], " there");
    assert_eq!(frames[2].1["choices"][0]["finish_reason"], Value::Null);

    let finish = &frames[3].1;
    assert_eq!(finish["choices"][0]["delta"], json!({}));
    assert_eq!(finish["choices"][0]["finish_reason"], "stop");
    assert_eq!(finish["usage"]["prompt_tokens"], 11);
    assert_eq!(finish["usage"]["completion_tokens"], 4);
    assert_eq!(finish["usage"]["total_tokens"], 15);
    assert_eq!(finish["usage"]["prompt_tokens_details"]["cached_tokens"], 2);
    assert!(!finish.to_string().contains("cache_creation"));
    assert_eq!(frames[4].1, "[DONE]");

    assert_eq!(usage.input, 11);
    assert_eq!(usage.output, 4);
    assert_eq!(usage.cache_read, 2);
    assert_eq!(usage.cache_write, 3);
}

#[test]
fn anthropic_tool_stream_keeps_split_arguments() {
    let (out, usage) = run(
        Protocol::AnthropicMessages,
        Protocol::OpenAiChat,
        "gpt-route",
        &anthropic_tool(),
    );
    let frames = json_frames(&out);
    assert_eq!(frames.len(), 7, "{out}");
    assert_eq!(frames[1].1["choices"][0]["delta"]["content"], "Checking.");

    let start = &frames[2].1["choices"][0]["delta"]["tool_calls"][0];
    assert_eq!(start["index"], 0);
    assert_eq!(start["id"], "toolu_01");
    assert_eq!(start["type"], "function");
    assert_eq!(start["function"]["name"], "get_weather");
    assert_eq!(start["function"]["arguments"], "");

    let first = frames[3].1["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"]
        .as_str()
        .unwrap()
        .to_string();
    let second = frames[4].1["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(first, "{\"city\":");
    assert_eq!(second, "\"Paris\"}");
    assert!(
        frames[3].1["choices"][0]["delta"]["tool_calls"][0]
            .get("id")
            .is_none()
    );
    let arguments: Value = serde_json::from_str(&format!("{first}{second}")).unwrap();
    assert_eq!(arguments["city"], "Paris");

    assert_eq!(frames[5].1["choices"][0]["finish_reason"], "tool_calls");
    assert_eq!(frames[5].1["usage"]["prompt_tokens"], 20);
    assert_eq!(frames[5].1["usage"]["completion_tokens"], 12);
    assert!(frames[5].1.get("usage").is_some());
    assert!(frames[2].1.get("usage").is_none());
    assert_eq!(frames[6].1, "[DONE]");
    assert_eq!(usage.output, 12);
    assert_eq!(usage.input, 20);
}

#[test]
fn chat_text_stream_becomes_anthropic_events() {
    let (out, usage) = run(
        Protocol::OpenAiChat,
        Protocol::AnthropicMessages,
        "claude-route",
        &chat_text(),
    );
    let frames = json_frames(&out);
    assert_eq!(
        events(&frames),
        [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ],
        "{out}"
    );
    let message = &frames[0].1["message"];
    assert_eq!(message["id"], "msg_chatcmpl-abc");
    assert_eq!(message["type"], "message");
    assert_eq!(message["role"], "assistant");
    assert_eq!(message["model"], "claude-route");
    assert_eq!(message["content"], json!([]));
    assert_eq!(message["stop_reason"], Value::Null);
    assert_eq!(message["usage"]["input_tokens"], 0);

    assert_eq!(frames[1].1["index"], 0);
    assert_eq!(frames[1].1["content_block"]["type"], "text");
    assert_eq!(frames[2].1["delta"]["text"], "Hello");
    assert_eq!(frames[3].1["delta"]["text"], " there");
    assert_eq!(frames[4].1["index"], 0);

    assert_eq!(frames[5].1["delta"]["stop_reason"], "end_turn");
    assert_eq!(frames[5].1["delta"]["stop_sequence"], Value::Null);
    assert_eq!(frames[5].1["usage"]["input_tokens"], 11);
    assert_eq!(frames[5].1["usage"]["output_tokens"], 4);
    assert_eq!(frames[5].1["usage"]["cache_read_input_tokens"], 2);
    assert_eq!(frames[5].1["usage"]["cache_creation_input_tokens"], 0);
    assert_eq!(frames[6].1["type"], "message_stop");
    assert_eq!(usage.input, 11);
    assert_eq!(usage.output, 4);
    assert_eq!(usage.cache_read, 2);
    assert_eq!(usage.cache_write, 0);
}

#[test]
fn chat_tool_stream_keeps_split_arguments() {
    let (out, usage) = run(
        Protocol::OpenAiChat,
        Protocol::AnthropicMessages,
        "claude-route",
        &chat_tool(),
    );
    let frames = json_frames(&out);
    assert_eq!(
        events(&frames),
        [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "content_block_start",
            "content_block_delta",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop",
        ],
        "{out}"
    );
    assert_eq!(frames[0].1["message"]["id"], "msg_chatcmpl-tool");
    assert_eq!(frames[0].1["message"]["model"], "claude-route");
    assert_eq!(frames[2].1["delta"]["text"], "Checking.");
    assert_eq!(frames[3].1["index"], 0);

    let block = &frames[4].1["content_block"];
    assert_eq!(frames[4].1["index"], 1);
    assert_eq!(block["type"], "tool_use");
    assert_eq!(block["id"], "call_abc");
    assert_eq!(block["name"], "get_weather");
    assert_eq!(block["input"], json!({}));

    let first = frames[5].1["delta"]["partial_json"].as_str().unwrap();
    let second = frames[6].1["delta"]["partial_json"].as_str().unwrap();
    assert_eq!(first, "{\"city\":");
    assert_eq!(second, "\"Paris\"}");
    assert_eq!(frames[5].1["index"], 1);
    let arguments: Value = serde_json::from_str(&format!("{first}{second}")).unwrap();
    assert_eq!(arguments["city"], "Paris");

    assert_eq!(frames[7].1["index"], 1);
    assert_eq!(frames[8].1["delta"]["stop_reason"], "tool_use");
    assert_eq!(frames[8].1["usage"]["input_tokens"], 20);
    assert_eq!(frames[8].1["usage"]["output_tokens"], 12);
    assert_eq!(usage.input, 20);
    assert_eq!(usage.output, 12);
}
