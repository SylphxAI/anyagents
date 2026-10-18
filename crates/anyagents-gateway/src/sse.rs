#![allow(dead_code)]

use serde_json::Value;

/// One Server-Sent Event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SseFrame {
    pub event: Option<String>,
    pub data: String,
}

/// Incremental SSE reader. Frames may arrive split across chunks.
#[derive(Debug, Default)]
pub(crate) struct ByteSse {
    buf: Vec<u8>,
}

impl ByteSse {
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Vec<SseFrame> {
        self.buf.extend_from_slice(chunk);
        let mut frames = Vec::new();
        while let Some((at, len)) = find_sep(&self.buf) {
            let mut raw: Vec<u8> = self.buf.drain(..at + len).collect();
            raw.truncate(at);
            if let Some(frame) = parse_frame(&raw) {
                frames.push(frame);
            }
        }
        frames
    }

    pub(crate) fn finish(&mut self) -> Vec<SseFrame> {
        if self.buf.iter().all(u8::is_ascii_whitespace) {
            self.buf.clear();
            return Vec::new();
        }
        let raw = std::mem::take(&mut self.buf);
        parse_frame(&raw).into_iter().collect()
    }
}

fn find_sep(buf: &[u8]) -> Option<(usize, usize)> {
    let mut index = 0;
    while index + 1 < buf.len() {
        if buf[index] == b'\n' && buf[index + 1] == b'\n' {
            return Some((index, 2));
        }
        if index + 3 < buf.len() && &buf[index..index + 4] == b"\r\n\r\n" {
            return Some((index, 4));
        }
        index += 1;
    }
    None
}

fn parse_frame(raw: &[u8]) -> Option<SseFrame> {
    let text = String::from_utf8_lossy(raw);
    let mut event = None;
    let mut data = Vec::new();
    for line in text.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("event:") {
            event = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            data.push(rest.strip_prefix(' ').unwrap_or(rest).to_string());
        }
    }
    if data.is_empty() {
        return None;
    }
    Some(SseFrame {
        event,
        data: data.join("\n"),
    })
}

pub(crate) fn sse_event(name: &str, value: &Value) -> String {
    format!(
        "event: {name}\ndata: {}\n\n",
        serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
    )
}

pub(crate) fn sse_data(value: &Value) -> String {
    format!(
        "data: {}\n\n",
        serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
    )
}

pub(crate) fn sse_done() -> String {
    "data: [DONE]\n\n".to_string()
}

#[cfg(test)]
mod tests {
    use super::ByteSse;

    #[test]
    fn splits_a_frame_across_chunks() {
        let mut parser = ByteSse::default();
        let first = parser.push(b"event: message_stop\ndata: {\"ty");
        assert!(first.is_empty());
        let second = parser.push(b"pe\":\"message_stop\"}\n\n");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].event.as_deref(), Some("message_stop"));
        assert_eq!(second[0].data, "{\"type\":\"message_stop\"}");
    }

    #[test]
    fn accepts_crlf_and_a_trailing_frame() {
        let mut parser = ByteSse::default();
        let frames = parser.push(b"data: {\"a\":1}\r\n\r\ndata: [DONE]\n\n");
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1].data, "[DONE]");
        assert!(parser.finish().is_empty());
    }

    #[test]
    fn writes_named_data_and_done_frames() {
        let value = serde_json::json!({"type": "ping"});
        assert_eq!(
            super::sse_event("ping", &value),
            "event: ping\ndata: {\"type\":\"ping\"}\n\n"
        );
        assert_eq!(super::sse_data(&value), "data: {\"type\":\"ping\"}\n\n");
        assert_eq!(super::sse_done(), "data: [DONE]\n\n");
    }
}
