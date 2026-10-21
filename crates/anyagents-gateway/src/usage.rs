use std::sync::{Arc, Mutex};

use serde_json::Value;

/// One completed gateway call. The server records it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsageEvent {
    pub agent: String,
    pub provider: String,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
}

/// Receives one [`UsageEvent`] per completed request.
pub trait UsageSink: Send + Sync {
    fn record(&self, event: UsageEvent);
}

/// In-memory sink: for tests, and for a server that keeps its own records.
#[derive(Clone, Debug, Default)]
pub struct MemoryUsage {
    events: Arc<Mutex<Vec<UsageEvent>>>,
}

impl MemoryUsage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events(&self) -> Vec<UsageEvent> {
        self.events.lock().expect("usage").clone()
    }
}

impl UsageSink for MemoryUsage {
    fn record(&self, event: UsageEvent) {
        self.events.lock().expect("usage").push(event);
    }
}

/// Token counts gathered from a provider response.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct TokenUsage {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
}

/// Fields a provider actually sent. Absent fields do not overwrite earlier counts.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct UsagePatch {
    pub input: Option<u64>,
    pub output: Option<u64>,
    pub cache_read: Option<u64>,
    pub cache_write: Option<u64>,
}

impl TokenUsage {
    pub(crate) fn apply(&mut self, patch: UsagePatch) {
        if let Some(value) = patch.input {
            self.input = value;
        }
        if let Some(value) = patch.output {
            self.output = value;
        }
        if let Some(value) = patch.cache_read {
            self.cache_read = value;
        }
        if let Some(value) = patch.cache_write {
            self.cache_write = value;
        }
    }

    pub(crate) fn anthropic_json(self) -> Value {
        serde_json::json!({
            "input_tokens": self.input,
            "output_tokens": self.output,
            "cache_read_input_tokens": self.cache_read,
            "cache_creation_input_tokens": self.cache_write,
        })
    }

    pub(crate) fn chat_json(self) -> Value {
        // Chat Completions reports cache reads as prompt_tokens_details.cached_tokens.
        // The public usage object has no cache-write count, so that count stays on
        // the usage event only.
        serde_json::json!({
            "prompt_tokens": self.input,
            "completion_tokens": self.output,
            "total_tokens": self.input.saturating_add(self.output),
            "prompt_tokens_details": {"cached_tokens": self.cache_read},
        })
    }

    pub(crate) fn responses_json(self) -> Value {
        serde_json::json!({
            "input_tokens": self.input,
            "output_tokens": self.output,
            "total_tokens": self.input.saturating_add(self.output),
            "input_tokens_details": {"cached_tokens": self.cache_read},
            "output_tokens_details": {"reasoning_tokens": 0},
        })
    }
}

fn u64_at(value: &Value, key: &str) -> Option<u64> {
    let number = value.get(key)?;
    number
        .as_u64()
        .or_else(|| number.as_i64().and_then(|value| u64::try_from(value).ok()))
}

pub(crate) fn patch_anthropic(usage: &Value) -> UsagePatch {
    UsagePatch {
        input: u64_at(usage, "input_tokens"),
        output: u64_at(usage, "output_tokens"),
        cache_read: u64_at(usage, "cache_read_input_tokens"),
        cache_write: u64_at(usage, "cache_creation_input_tokens"),
    }
}

pub(crate) fn patch_chat(usage: &Value) -> UsagePatch {
    UsagePatch {
        input: u64_at(usage, "prompt_tokens"),
        output: u64_at(usage, "completion_tokens"),
        cache_read: usage
            .pointer("/prompt_tokens_details/cached_tokens")
            .and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
            }),
        // No public cache-write field.
        cache_write: None,
    }
}

pub(crate) fn patch_responses(usage: &Value) -> UsagePatch {
    UsagePatch {
        input: u64_at(usage, "input_tokens"),
        output: u64_at(usage, "output_tokens"),
        // UNVERIFIED: cache reads are taken from input_tokens_details.cached_tokens.
        cache_read: usage
            .pointer("/input_tokens_details/cached_tokens")
            .and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
            }),
        // No public cache-write field on the Responses usage object.
        cache_write: None,
    }
}

#[cfg(test)]
mod tests {
    use super::{TokenUsage, patch_anthropic};

    #[test]
    fn later_output_does_not_wipe_earlier_counts() {
        let mut usage = TokenUsage::default();
        usage.apply(patch_anthropic(&serde_json::json!({
            "input_tokens": 11,
            "output_tokens": 0,
            "cache_read_input_tokens": 2,
            "cache_creation_input_tokens": 3
        })));
        usage.apply(patch_anthropic(&serde_json::json!({"output_tokens": 4})));
        assert_eq!(usage.input, 11);
        assert_eq!(usage.output, 4);
        assert_eq!(usage.cache_read, 2);
        assert_eq!(usage.cache_write, 3);
    }
}
