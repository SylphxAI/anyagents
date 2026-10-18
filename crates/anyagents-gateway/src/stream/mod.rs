//! Streaming translation between Anthropic Messages events and OpenAI chat chunks.
//!
//! Same-protocol streams do not come through here: the router forwards those bytes
//! unchanged. A feature with no equivalent is rejected by name instead of dropped.

#![cfg_attr(not(test), allow(dead_code))]

mod anthropic;
mod openai_chat;

use serde_json::Value;

use crate::canonical::StopReason;
use crate::error::Fail;
use crate::route::Protocol;
use crate::sse::{ByteSse, SseFrame};
use crate::usage::TokenUsage;

enum Way {
    ToChat,
    ToAnthropic,
}

enum Phase {
    New,
    Live,
    Closed,
}

enum Open {
    Text(usize),
    Tool(usize),
}

pub(crate) struct StreamX {
    way: Way,
    sse: ByteSse,
    usage: TokenUsage,
    id: String,
    model: String,
    /// Anthropic content-block index to OpenAI tool index.
    from_block: Vec<(usize, usize)>,
    next_tool: usize,
    /// OpenAI tool index to Anthropic content-block index.
    from_tool: Vec<(usize, usize)>,
    next_block: usize,
    open: Option<Open>,
    stop: Option<StopReason>,
    phase: Phase,
}

impl StreamX {
    pub(crate) fn to_chat(model: impl Into<String>) -> Self {
        Self::new(Way::ToChat, model)
    }

    pub(crate) fn to_anthropic(model: impl Into<String>) -> Self {
        Self::new(Way::ToAnthropic, model)
    }

    pub(crate) fn between(from: Protocol, to: Protocol, model: &str) -> Result<Self, Fail> {
        match (from, to) {
            (Protocol::AnthropicMessages, Protocol::OpenAiChat) => Ok(Self::to_chat(model)),
            (Protocol::OpenAiChat, Protocol::AnthropicMessages) => Ok(Self::to_anthropic(model)),
            _ => Err(Fail::unsupported("responses_stream")),
        }
    }

    fn new(way: Way, model: impl Into<String>) -> Self {
        Self {
            way,
            sse: ByteSse::default(),
            usage: TokenUsage::default(),
            id: String::new(),
            model: model.into(),
            from_block: Vec::new(),
            next_tool: 0,
            from_tool: Vec::new(),
            next_block: 0,
            open: None,
            stop: None,
            phase: Phase::New,
        }
    }

    pub(crate) fn push(&mut self, chunk: &[u8]) -> Result<String, Fail> {
        let frames = self.sse.push(chunk);
        self.frames(frames)
    }

    pub(crate) fn finish(&mut self) -> Result<(String, TokenUsage), Fail> {
        let frames = self.sse.finish();
        let mut out = self.frames(frames)?;
        out.push_str(&self.end()?);
        Ok((out, self.usage))
    }

    fn frames(&mut self, frames: Vec<SseFrame>) -> Result<String, Fail> {
        let mut out = String::new();
        for frame in frames {
            out.push_str(&self.one(&frame)?);
        }
        Ok(out)
    }

    fn one(&mut self, frame: &SseFrame) -> Result<String, Fail> {
        if matches!(self.phase, Phase::Closed) {
            return Ok(String::new());
        }
        if frame.data.trim() == "[DONE]" {
            return self.end();
        }
        let value: Value = serde_json::from_str(&frame.data).map_err(|_| Fail::invalid("event"))?;
        match self.way {
            Way::ToChat => self.on_anthropic(&value),
            Way::ToAnthropic => self.on_chat(&value),
        }
    }

    fn end(&mut self) -> Result<String, Fail> {
        if matches!(self.phase, Phase::Closed) {
            return Ok(String::new());
        }
        match self.way {
            Way::ToChat => self.end_chat(),
            Way::ToAnthropic => self.close_anthropic(),
        }
    }

    fn promoted(&self, stop: StopReason) -> StopReason {
        let saw_tool = !self.from_block.is_empty() || !self.from_tool.is_empty();
        if saw_tool && stop == StopReason::EndTurn {
            StopReason::ToolUse
        } else {
            stop
        }
    }

    fn alloc_block(&mut self) -> usize {
        let index = self.next_block;
        self.next_block += 1;
        index
    }

    fn block_for_tool(&self, tool: usize) -> Option<usize> {
        self.from_tool
            .iter()
            .find(|(index, _)| *index == tool)
            .map(|(_, block)| *block)
    }

    fn tool_for_block(&self, block: usize) -> Option<usize> {
        self.from_block
            .iter()
            .find(|(index, _)| *index == block)
            .map(|(_, tool)| *tool)
    }
}

fn json_index(value: &Value, key: &str) -> Result<usize, Fail> {
    let number = value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| Fail::invalid(key))?;
    usize::try_from(number).map_err(|_| Fail::invalid(key))
}

fn opt_str<'a>(value: &'a Value, key: &str) -> Result<&'a str, Fail> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(""),
        Some(Value::String(text)) => Ok(text),
        Some(_) => Err(Fail::invalid(key)),
    }
}

fn pointer_str<'a>(value: &'a Value, pointer: &str, key: &str) -> Result<&'a str, Fail> {
    match value.pointer(pointer) {
        None | Some(Value::Null) => Ok(""),
        Some(Value::String(text)) => Ok(text),
        Some(_) => Err(Fail::invalid(key)),
    }
}

#[cfg(test)]
mod tests;
