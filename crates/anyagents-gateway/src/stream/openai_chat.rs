use serde_json::{Value, json};

use crate::canonical::{StopReason, prefixed, stop_from_chat, stop_to_chat};
use crate::error::Fail;
use crate::sse::{sse_data, sse_done};
use crate::usage::patch_chat;

use super::{Phase, StreamX};

impl StreamX {
    pub(crate) fn on_chat(&mut self, value: &Value) -> Result<String, Fail> {
        if value.get("error").is_some_and(|error| !error.is_null()) {
            return Err(Fail::Upstream);
        }
        if self.id.is_empty() {
            match value.get("id") {
                None | Some(Value::Null) => {}
                Some(Value::String(id)) => self.id = id.clone(),
                Some(_) => return Err(Fail::invalid("id")),
            }
        }
        if let Some(usage) = value.get("usage").filter(|usage| !usage.is_null()) {
            self.usage.apply(patch_chat(usage));
        }
        let Some(choices) = value.get("choices").filter(|choices| !choices.is_null()) else {
            return Ok(String::new());
        };
        let choices = choices.as_array().ok_or_else(|| Fail::invalid("choices"))?;
        if choices.is_empty() {
            return Ok(String::new());
        }
        if choices.len() != 1 {
            return Err(Fail::unsupported("n"));
        }
        let choice = &choices[0];
        if !choice.is_object() {
            return Err(Fail::invalid("choice"));
        }
        if choice.get("logprobs").is_some_and(|value| !value.is_null()) {
            return Err(Fail::unsupported("logprobs"));
        }
        let mut out = String::new();
        if let Some(delta) = choice.get("delta").filter(|delta| !delta.is_null()) {
            out.push_str(&self.chat_fields(delta)?);
        }
        match choice.get("finish_reason") {
            None | Some(Value::Null) => {}
            Some(Value::String(reason)) => {
                self.stop = Some(self.promoted(stop_from_chat(reason)?));
            }
            Some(_) => return Err(Fail::invalid("finish_reason")),
        }
        Ok(out)
    }

    fn chat_fields(&mut self, delta: &Value) -> Result<String, Fail> {
        if !delta.is_object() {
            return Err(Fail::invalid("delta"));
        }
        if let Some(refusal) = delta.get("refusal").filter(|refusal| !refusal.is_null()) {
            let text = refusal.as_str().ok_or_else(|| Fail::invalid("refusal"))?;
            if !text.is_empty() {
                return Err(Fail::unsupported("refusal"));
            }
        }
        if delta
            .get("function_call")
            .is_some_and(|call| !call.is_null())
        {
            return Err(Fail::unsupported("function_call"));
        }
        let mut out = String::new();
        if let Some(role) = delta.get("role").filter(|role| !role.is_null()) {
            let role = role.as_str().ok_or_else(|| Fail::invalid("role"))?;
            if role != "assistant" {
                return Err(Fail::unsupported(role));
            }
            out.push_str(&self.ensure_started());
        }
        if let Some(content) = delta.get("content") {
            let text = match content {
                Value::Null => "",
                Value::String(text) => text.as_str(),
                _ => return Err(Fail::invalid("content")),
            };
            out.push_str(&self.push_text(text));
        }
        if let Some(calls) = delta.get("tool_calls").filter(|calls| !calls.is_null()) {
            let calls = calls
                .as_array()
                .ok_or_else(|| Fail::invalid("tool_calls"))?;
            for call in calls {
                out.push_str(&self.push_tool(call)?);
            }
        }
        Ok(out)
    }

    pub(crate) fn chat_delta(&self, delta: Value) -> String {
        self.chunk(delta, Value::Null, false)
    }

    pub(crate) fn finish_chat(&mut self, reason: StopReason) -> Result<String, Fail> {
        if self.stop.is_some() {
            return Ok(String::new());
        }
        let mut out = String::new();
        if matches!(self.phase, Phase::New) {
            self.phase = Phase::Live;
            out.push_str(&self.chat_delta(json!({"role": "assistant"})));
        }
        self.stop = Some(reason);
        out.push_str(&self.chunk(json!({}), json!(stop_to_chat(reason)), true));
        Ok(out)
    }

    pub(crate) fn end_chat(&mut self) -> Result<String, Fail> {
        if matches!(self.phase, Phase::Closed) {
            return Ok(String::new());
        }
        let mut out = String::new();
        if self.stop.is_none() {
            out.push_str(&self.finish_chat(self.promoted(StopReason::EndTurn))?);
        }
        out.push_str(&sse_done());
        self.phase = Phase::Closed;
        Ok(out)
    }

    fn chunk(&self, delta: Value, finish: Value, usage: bool) -> String {
        let mut body = json!({
            "id": prefixed(&self.id, "chatcmpl_"),
            "object": "chat.completion.chunk",
            "created": 0,
            "model": self.model.as_str(),
            "choices": [{
                "index": 0,
                "delta": delta,
                "finish_reason": finish,
            }],
        });
        if usage {
            body["usage"] = self.usage.chat_json();
        }
        sse_data(&body)
    }
}
