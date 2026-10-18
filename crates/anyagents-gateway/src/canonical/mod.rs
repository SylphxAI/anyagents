//! One request and response shape, with a parser and emitter for each protocol.
//!
//! Same-protocol calls do not come through here: the forwarder replaces the model
//! and proxies the bytes. Translation uses this module so a feature one side
//! cannot represent is rejected by name instead of being dropped.

use serde_json::Value;

use crate::error::Fail;
use crate::route::Protocol;

mod anthropic;
mod openai_chat;
mod openai_responses;
mod tools;
mod types;

#[allow(unused_imports)]
pub(crate) use anthropic::{stop_from_anthropic, stop_to_anthropic};
#[allow(unused_imports)]
pub(crate) use openai_chat::{stop_from_chat, stop_to_chat};
#[allow(unused_imports)]
pub(crate) use openai_responses::stop_from_responses;
pub use types::{Completion, Request, StopReason};
#[allow(unused_imports)]
pub(crate) use types::{prefixed, reject_unknown};

pub fn parse_request(protocol: Protocol, value: &Value) -> Result<Request, Fail> {
    match protocol {
        Protocol::AnthropicMessages => anthropic::parse_anthropic_request(value),
        Protocol::OpenAiChat => openai_chat::parse_chat_request(value),
        Protocol::OpenAiResponses => openai_responses::parse_responses_request(value),
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
        Protocol::AnthropicMessages => anthropic::encode_anthropic_request(request, model, stream),
        Protocol::OpenAiChat => openai_chat::encode_chat_request(request, model, stream),
        Protocol::OpenAiResponses => {
            openai_responses::encode_responses_request(request, model, stream)
        }
    }
}

pub fn parse_response(protocol: Protocol, value: &Value) -> Result<Completion, Fail> {
    match protocol {
        Protocol::AnthropicMessages => anthropic::parse_anthropic_response(value),
        Protocol::OpenAiChat => openai_chat::parse_chat_response(value),
        Protocol::OpenAiResponses => openai_responses::parse_responses_response(value),
    }
}

pub fn encode_response(
    protocol: Protocol,
    completion: &Completion,
    model: &str,
) -> Result<Value, Fail> {
    match protocol {
        Protocol::AnthropicMessages => anthropic::encode_anthropic_response(completion, model),
        Protocol::OpenAiChat => openai_chat::encode_chat_response(completion, model),
        Protocol::OpenAiResponses => openai_responses::encode_responses_response(completion, model),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

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
}
