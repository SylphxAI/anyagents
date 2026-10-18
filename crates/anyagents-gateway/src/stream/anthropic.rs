use serde_json::{Value, json};

use crate::canonical::{StopReason, prefixed, stop_from_anthropic, stop_to_anthropic};
use crate::error::Fail;
use crate::sse::sse_event;
use crate::usage::patch_anthropic;

use super::{Open, Phase, StreamX, json_index, opt_str};

impl StreamX {
    pub(crate) fn on_anthropic(&mut self, value: &Value) -> Result<String, Fail> {
        let kind = value.get("type").and_then(Value::as_str).unwrap_or("");
        match kind {
            "ping" => Ok(String::new()),
            "message_start" => self.message_started(value),
            "content_block_start" => self.block_start(value),
            "content_block_delta" => self.block_delta(value),
            "content_block_stop" => Ok(String::new()),
            "message_delta" => self.message_delta(value),
            "message_stop" => self.end_chat(),
            "error" => Err(Fail::Upstream),
            "" => Err(Fail::invalid("event")),
            other => Err(Fail::unsupported(other)),
        }
    }

    fn message_started(&mut self, value: &Value) -> Result<String, Fail> {
        let message = value
            .get("message")
            .ok_or_else(|| Fail::invalid("message"))?;
        match message.get("id") {
            None | Some(Value::Null) => {}
            Some(Value::String(id)) => self.id = id.clone(),
            Some(_) => return Err(Fail::invalid("id")),
        }
        if let Some(usage) = message.get("usage").filter(|usage| !usage.is_null()) {
            self.usage.apply(patch_anthropic(usage));
        }
        self.phase = Phase::Live;
        Ok(self.chat_delta(json!({"role": "assistant"})))
    }

    fn block_start(&mut self, value: &Value) -> Result<String, Fail> {
        let index = json_index(value, "index")?;
        let block = value
            .get("content_block")
            .ok_or_else(|| Fail::invalid("content_block"))?;
        match block.get("type").and_then(Value::as_str).unwrap_or("") {
            "text" => {
                let text = opt_str(block, "text")?;
                if text.is_empty() {
                    Ok(String::new())
                } else {
                    Ok(self.chat_delta(json!({"content": text})))
                }
            }
            "tool_use" => self.tool_started(index, block),
            "thinking" | "redacted_thinking" => Err(Fail::unsupported("thinking")),
            "" => Err(Fail::invalid("content_block")),
            other => Err(Fail::unsupported(other)),
        }
    }

    fn tool_started(&mut self, index: usize, block: &Value) -> Result<String, Fail> {
        let id = opt_str(block, "id")?;
        let name = opt_str(block, "name")?;
        let tool = self.next_tool;
        self.next_tool += 1;
        self.from_block.push((index, tool));
        let mut out = self.chat_delta(json!({
            "tool_calls": [{
                "index": tool,
                "id": id,
                "type": "function",
                "function": {"name": name, "arguments": ""}
            }]
        }));
        let Some(input) = block.get("input").filter(|input| !input.is_null()) else {
            return Ok(out);
        };
        if !input.is_object() {
            return Err(Fail::invalid("input"));
        }
        if input.as_object().is_some_and(|fields| !fields.is_empty()) {
            let arguments = serde_json::to_string(input).map_err(|_| Fail::invalid("input"))?;
            out.push_str(&self.chat_delta(json!({
                "tool_calls": [{"index": tool, "function": {"arguments": arguments}}]
            })));
        }
        Ok(out)
    }

    fn block_delta(&mut self, value: &Value) -> Result<String, Fail> {
        let index = json_index(value, "index")?;
        let delta = value.get("delta").ok_or_else(|| Fail::invalid("delta"))?;
        match delta.get("type").and_then(Value::as_str).unwrap_or("") {
            "text_delta" => {
                let text = opt_str(delta, "text")?;
                if text.is_empty() {
                    Ok(String::new())
                } else {
                    Ok(self.chat_delta(json!({"content": text})))
                }
            }
            "input_json_delta" => self.argument_delta(index, delta),
            "thinking_delta" | "signature_delta" => Err(Fail::unsupported("thinking")),
            "citations_delta" => Err(Fail::unsupported("citations")),
            "" => Err(Fail::invalid("delta")),
            other => Err(Fail::unsupported(other)),
        }
    }

    fn argument_delta(&mut self, index: usize, delta: &Value) -> Result<String, Fail> {
        let partial = opt_str(delta, "partial_json")?;
        if partial.is_empty() {
            return Ok(String::new());
        }
        let tool = self
            .tool_for_block(index)
            .ok_or_else(|| Fail::invalid("tool_use"))?;
        Ok(self.chat_delta(json!({
            "tool_calls": [{"index": tool, "function": {"arguments": partial}}]
        })))
    }

    fn message_delta(&mut self, value: &Value) -> Result<String, Fail> {
        if let Some(usage) = value.get("usage").filter(|usage| !usage.is_null()) {
            self.usage.apply(patch_anthropic(usage));
        }
        match value.pointer("/delta/stop_reason") {
            None | Some(Value::Null) => Ok(String::new()),
            Some(Value::String(reason)) => {
                let stop = self.promoted(stop_from_anthropic(reason)?);
                self.finish_chat(stop)
            }
            Some(_) => Err(Fail::invalid("stop_reason")),
        }
    }

    pub(crate) fn close_anthropic(&mut self) -> Result<String, Fail> {
        if matches!(self.phase, Phase::Closed) {
            return Ok(String::new());
        }
        let mut out = self.ensure_started();
        out.push_str(&self.close_open());
        let reason = self
            .stop
            .unwrap_or_else(|| self.promoted(StopReason::EndTurn));
        self.stop = Some(reason);
        out.push_str(&sse_event(
            "message_delta",
            &json!({
                "type": "message_delta",
                "delta": {
                    "stop_reason": stop_to_anthropic(reason),
                    "stop_sequence": null
                },
                "usage": self.usage.anthropic_json(),
            }),
        ));
        out.push_str(&sse_event("message_stop", &json!({"type": "message_stop"})));
        self.phase = Phase::Closed;
        Ok(out)
    }

    pub(crate) fn ensure_started(&mut self) -> String {
        if !matches!(self.phase, Phase::New) {
            return String::new();
        }
        self.phase = Phase::Live;
        sse_event(
            "message_start",
            &json!({
                "type": "message_start",
                "message": {
                    "id": prefixed(&self.id, "msg_"),
                    "type": "message",
                    "role": "assistant",
                    "content": [],
                    "model": self.model.as_str(),
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": self.usage.anthropic_json(),
                }
            }),
        )
    }

    pub(crate) fn close_open(&mut self) -> String {
        let Some(open) = self.open.take() else {
            return String::new();
        };
        let index = match open {
            Open::Text(index) | Open::Tool(index) => index,
        };
        sse_event(
            "content_block_stop",
            &json!({"type": "content_block_stop", "index": index}),
        )
    }

    pub(crate) fn push_text(&mut self, text: &str) -> String {
        if text.is_empty() {
            return String::new();
        }
        let mut out = self.ensure_started();
        if !matches!(self.open, Some(Open::Text(_))) {
            out.push_str(&self.close_open());
            let index = self.alloc_block();
            self.open = Some(Open::Text(index));
            out.push_str(&sse_event(
                "content_block_start",
                &json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": {"type": "text", "text": ""}
                }),
            ));
        }
        let Some(Open::Text(index)) = self.open else {
            return out;
        };
        out.push_str(&sse_event(
            "content_block_delta",
            &json!({
                "type": "content_block_delta",
                "index": index,
                "delta": {"type": "text_delta", "text": text}
            }),
        ));
        out
    }

    pub(crate) fn push_tool(&mut self, call: &Value) -> Result<String, Fail> {
        let tool = json_index(call, "index")?;
        if let Some(kind) = call.get("type").filter(|kind| !kind.is_null()) {
            let kind = kind.as_str().ok_or_else(|| Fail::invalid("type"))?;
            if kind != "function" {
                return Err(Fail::unsupported(kind));
            }
        }
        let mut out = self.ensure_started();
        if self.block_for_tool(tool).is_none() {
            let id = opt_str(call, "id")?;
            let name = super::pointer_str(call, "/function/name", "name")?;
            out.push_str(&self.close_open());
            let block = self.alloc_block();
            self.from_tool.push((tool, block));
            self.open = Some(Open::Tool(block));
            out.push_str(&sse_event(
                "content_block_start",
                &json!({
                    "type": "content_block_start",
                    "index": block,
                    "content_block": {
                        "type": "tool_use",
                        "id": id,
                        "name": name,
                        "input": {}
                    }
                }),
            ));
        }
        let partial = super::pointer_str(call, "/function/arguments", "arguments")?;
        if !partial.is_empty() {
            let block = self
                .block_for_tool(tool)
                .ok_or_else(|| Fail::invalid("tool_use"))?;
            out.push_str(&sse_event(
                "content_block_delta",
                &json!({
                    "type": "content_block_delta",
                    "index": block,
                    "delta": {"type": "input_json_delta", "partial_json": partial}
                }),
            ));
        }
        Ok(out)
    }
}
