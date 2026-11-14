//! Server-Sent Events framing: bytes in, one `data` payload per event out.
//! Comments (heartbeats), `event:`, `id:` and `retry:` fields are read past;
//! the payload carries its own type.

#[derive(Default)]
pub(crate) struct Decoder {
    line: Vec<u8>,
    data: Option<String>,
}

impl Decoder {
    /// Feed bytes as they arrive; returns the payloads of the events they
    /// completed, in order.
    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Vec<String> {
        let mut out = Vec::new();
        for &byte in bytes {
            if byte != b'\n' {
                self.line.push(byte);
                continue;
            }
            let mut line = std::mem::take(&mut self.line);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            self.line_done(&line, &mut out);
        }
        out
    }

    fn line_done(&mut self, line: &[u8], out: &mut Vec<String>) {
        if line.is_empty() {
            if let Some(data) = self.data.take() {
                out.push(data);
            }
            return;
        }
        if line.first() == Some(&b':') {
            return;
        }
        let text = String::from_utf8_lossy(line);
        let (field, value) = match text.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (text.as_ref(), ""),
        };
        if field == "data" {
            match &mut self.data {
                Some(data) => {
                    data.push('\n');
                    data.push_str(value);
                }
                None => self.data = Some(value.to_string()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_split_across_chunks_heartbeats_and_crlf_are_framed() {
        let mut decoder = Decoder::default();
        let mut out = decoder.feed(b": ping\n\nevent: agent.message\ndata: {\"a\"");
        assert!(out.is_empty());
        out.extend(decoder.feed(b":1}\r\n\r\ndata: x\ndata: y\n"));
        assert_eq!(out, vec!["{\"a\":1}".to_string()]);
        assert_eq!(decoder.feed(b"\n"), vec!["x\ny".to_string()]);
        assert!(decoder.feed(b"id: 7\nretry: 10\n\n").is_empty());
    }
}
