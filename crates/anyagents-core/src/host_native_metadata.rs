//! Bounded, derivative-only metadata projection of native JSON events.
//! Oversized string tokens become empty strings only in this reader. The raw
//! source, its validation, search windows and exact coordinates are unchanged.
use serde::{
    Deserialize, Deserializer,
    de::{self, SeqAccess, Visitor},
};
use std::{
    fmt,
    io::{self, BufRead, BufReader, Read},
};

const STRING_BYTES: usize = 4096;
const EVENT_TOOLS: usize = 1024;

pub(crate) struct MetadataReader<R> {
    source: BufReader<R>,
    pending: Vec<u8>,
    position: usize,
}
impl<R: Read> MetadataReader<R> {
    pub(crate) fn new(source: R) -> Self {
        Self {
            source: BufReader::new(source),
            pending: Vec::new(),
            position: 0,
        }
    }
    fn byte(&mut self) -> io::Result<Option<u8>> {
        let byte = self.source.fill_buf()?.first().copied();
        if byte.is_some() {
            self.source.consume(1);
        }
        Ok(byte)
    }
    fn next_token(&mut self) -> io::Result<bool> {
        self.pending.clear();
        self.position = 0;
        let Some(byte) = self.byte()? else {
            return Ok(false);
        };
        self.pending.push(byte);
        if byte != b'"' {
            return Ok(true);
        }
        let mut escaped = false;
        let mut oversized = false;
        loop {
            let byte = self.byte()?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "unterminated metadata string")
            })?;
            if !oversized {
                if self.pending.len() < STRING_BYTES {
                    self.pending.push(byte);
                } else {
                    oversized = true;
                    self.pending.clear();
                }
            }
            if byte == b'"' && !escaped {
                break;
            }
            escaped = byte == b'\\' && !escaped;
        }
        if oversized {
            self.pending.extend_from_slice(b"\"\"");
        }
        Ok(true)
    }
}
impl<R: Read> Read for MetadataReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let mut written = 0;
        while written < output.len() {
            if self.position == self.pending.len() && !self.next_token()? {
                break;
            }
            let count = (output.len() - written).min(self.pending.len() - self.position);
            output[written..written + count]
                .copy_from_slice(&self.pending[self.position..self.position + count]);
            written += count;
            self.position += count;
        }
        Ok(written)
    }
}

#[derive(Default, Deserialize)]
pub(crate) struct EventHeader {
    pub role: Option<String>,
    pub message: Option<EventMessage>,
    pub tool_name: Option<String>,
    pub tool: Option<String>,
    pub name: Option<String>,
    pub direction: Option<String>,
    pub sender: Option<String>,
    pub destination: Option<String>,
    pub timestamp_ms: Option<i64>,
    pub timestamp: Option<String>,
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub call_id: Option<String>,
    #[serde(default)]
    pub content: ToolContent,
}
#[derive(Default, Deserialize)]
pub(crate) struct EventMessage {
    pub role: Option<String>,
    #[serde(default)]
    pub content: ToolContent,
}
#[derive(Default, Deserialize)]
pub(crate) struct ToolNode {
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub id: Option<String>,
    pub name: Option<String>,
    pub tool_use_id: Option<String>,
}
#[derive(Default)]
pub(crate) struct ToolContent(pub Vec<ToolNode>);
impl<'de> Deserialize<'de> for ToolContent {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ContentVisitor;
        impl<'de> Visitor<'de> for ContentVisitor {
            type Value = ToolContent;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("native string or content array")
            }
            fn visit_str<E: de::Error>(self, _: &str) -> Result<Self::Value, E> {
                Ok(ToolContent::default())
            }
            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(ToolContent::default())
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut nodes = Vec::new();
                while let Some(node) = seq.next_element::<ToolNode>()? {
                    if matches!(node.kind.as_deref(), Some("tool_use" | "tool_result")) {
                        if nodes.len() == EVENT_TOOLS {
                            return Err(de::Error::custom(
                                "native event exceeds tool metadata limit",
                            ));
                        }
                        nodes.push(node);
                    }
                }
                Ok(ToolContent(nodes))
            }
        }
        deserializer.deserialize_any(ContentVisitor)
    }
}

/// A result with no known definition stays explicitly unknown, not a guessed
/// tool name. Correlation IDs are scoped by owner and source in the index.
impl EventHeader {
    pub(crate) fn tool_records(&self) -> Vec<(Option<&str>, Option<&str>)> {
        let mut records = Vec::new();
        if matches!(
            self.kind.as_deref(),
            Some("function_call" | "function_call_output")
        ) {
            records.push((
                self.call_id.as_deref().filter(|s| !s.is_empty()),
                (self.kind.as_deref() == Some("function_call"))
                    .then_some(self.name.as_deref())
                    .flatten()
                    .filter(|s| !s.is_empty()),
            ));
        }
        for node in self
            .content
            .0
            .iter()
            .chain(self.message.iter().flat_map(|m| &m.content.0))
        {
            let definition = node.kind.as_deref() == Some("tool_use");
            records.push((
                if definition {
                    node.id.as_deref()
                } else {
                    node.tool_use_id.as_deref()
                }
                .filter(|s| !s.is_empty()),
                if definition {
                    node.name.as_deref().filter(|s| !s.is_empty())
                } else {
                    None
                },
            ));
        }
        records
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn projection_preserves_short_tokens_and_bounds_each_resident_token() {
        let short = serde_json::json!({"escaped":"\\\"☃\\", "array":["text", {"name":"browser"}], "null":null});
        let raw = short.to_string();
        let decoded: serde_json::Value =
            serde_json::from_reader(MetadataReader::new(raw.as_bytes())).unwrap();
        assert_eq!(decoded, short);
        let raw = serde_json::json!({"before":"x".repeat(1_000_000), "name":"trailing", "after":"y".repeat(1_000_000)}).to_string();
        let mut reader = MetadataReader::new(raw.as_bytes());
        let mut buffer = [0; 64];
        let mut projection = Vec::new();
        loop {
            let n = reader.read(&mut buffer).unwrap();
            if n == 0 {
                break;
            }
            assert!(reader.pending.len() <= STRING_BYTES);
            projection.extend_from_slice(&buffer[..n]);
        }
        let decoded: serde_json::Value = serde_json::from_slice(&projection).unwrap();
        assert_eq!(
            decoded,
            serde_json::json!({"before":"", "name":"trailing", "after":""})
        );
        assert!(raw.len() > 2_000_000); // The source is not the projection.
    }

    #[test]
    fn large_escaped_payloads_do_not_hide_trailing_metadata() {
        let large = "\\\"☃\\\\".repeat(100_000);
        let raw = serde_json::json!({"output": large, "type":"function_call_output", "call_id":"after-output", "content":"text"}).to_string();
        let value: EventHeader =
            serde_json::from_reader(MetadataReader::new(raw.as_bytes())).unwrap();
        assert_eq!(value.tool_records(), vec![(Some("after-output"), None)]);
        let raw = format!(
            "{{\"content\":[{{\"type\":\"text\",\"text\":{}}},{{\"type\":\"tool_use\",\"id\":\"c1\",\"input\":{},\"name\":\"browser\\u002epage\"}}]}}",
            serde_json::to_string(&large).unwrap(),
            serde_json::to_string(&large).unwrap()
        );
        let value: EventHeader =
            serde_json::from_reader(MetadataReader::new(raw.as_bytes())).unwrap();
        assert_eq!(
            value.tool_records(),
            vec![(Some("c1"), Some("browser.page"))]
        );
    }
}
