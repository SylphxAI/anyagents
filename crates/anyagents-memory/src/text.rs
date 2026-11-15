//! What a dense model reads from a raw event, and where each piece of it lives.
//!
//! A native event is a JSON record; its readable content is the string values
//! (message text, tool arguments and output), not the keys, ids, hashes and
//! timestamps around them. [`project`] extracts those values and remembers the
//! exact raw byte range each came from, so every piece embedded from them can
//! cite the bytes it stands for. A non-JSON document (a Markdown topic or an
//! archive page) is read as text, line by line.

/// One readable value and the raw bytes it was decoded from, as offsets
/// relative to the start of the event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
    pub text: String,
}

/// Values shorter than this many characters carry no meaning on their own.
const MIN_CHARS: usize = 2;
/// A single token this long without whitespace reads as an identifier or
/// encoded payload unless it contains CJK text.
const IDENTIFIER_CHARS: usize = 24;

/// The readable spans of one raw event, in byte order.
pub fn project(raw: &[u8]) -> Vec<Span> {
    let first = raw.iter().position(|b| !b.is_ascii_whitespace());
    match first.map(|i| raw[i]) {
        Some(b'{' | b'[') => {
            let mut spans = Vec::new();
            JsonScanner::new(raw).scan(&mut spans);
            if spans.is_empty() && !raw.is_empty() && !looks_complete_json(raw) {
                // A window cut from the middle of a record: read its text.
                return lines(raw);
            }
            spans
        }
        Some(_) if looks_like_json_fragment(raw) => fragment(raw),
        Some(_) => lines(raw),
        None => Vec::new(),
    }
}

/// A slice of a JSON record that starts somewhere inside it.
fn looks_like_json_fragment(raw: &[u8]) -> bool {
    raw.windows(3).any(|w| w == b"\":\"" || w == b"\",\"")
}

/// Read a fragment from its first quote. A quote followed by `,` `:` `}` or
/// `]` closes a string cut at the start; the bytes before it are that value.
fn fragment(raw: &[u8]) -> Vec<Span> {
    let Some(first) = raw.iter().position(|&b| b == b'"') else {
        return lines(raw);
    };
    let closes = raw[first + 1..]
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|b| matches!(b, b',' | b':' | b'}' | b']'));
    let mut spans = Vec::new();
    let resume = if closes {
        let text = String::from_utf8_lossy(&raw[..first]);
        if keep(&text) {
            spans.push(Span {
                start: 0,
                end: first + 1,
                text: text.trim().to_owned(),
            });
        }
        first + 1
    } else {
        first
    };
    let mut scanner = JsonScanner::new(raw);
    scanner.at = resume;
    scanner.scan(&mut spans);
    spans
}

/// Readable text of a passage that may be a raw JSON event, a slice of one,
/// or plain text: what a reranker should read.
pub fn readable(raw: &[u8]) -> String {
    let spans = project(raw);
    if spans.is_empty() {
        return String::from_utf8_lossy(raw).into_owned();
    }
    let mut out = String::new();
    for span in spans {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&span.text);
    }
    out
}

fn looks_complete_json(raw: &[u8]) -> bool {
    serde_json::from_slice::<serde::de::IgnoredAny>(raw).is_ok()
}

fn lines(raw: &[u8]) -> Vec<Span> {
    let mut spans = Vec::new();
    let mut start = 0;
    for (i, &b) in raw.iter().enumerate() {
        if b == b'\n' || i + 1 == raw.len() {
            let end = if b == b'\n' { i } else { i + 1 };
            let text = String::from_utf8_lossy(&raw[start..end]);
            let trimmed = text.trim();
            if keep(trimmed) {
                spans.push(Span {
                    start,
                    end,
                    text: trimmed.to_owned(),
                });
            }
            start = i + 1;
        }
    }
    spans
}

fn is_cjk(c: char) -> bool {
    matches!(c as u32,
        0x3040..=0x30FF | 0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xAC00..=0xD7AF
        | 0xF900..=0xFAFF | 0xFF00..=0xFFEF | 0x20000..=0x2FFFF)
}

/// Keys whose values describe an event rather than say anything: ids,
/// times, roles, kinds and routing. Their values are never embedded.
const METADATA_KEYS: &[&str] = &[
    "agentId",
    "callId",
    "call_id",
    "created_at",
    "cwd",
    "destination",
    "direction",
    "gitBranch",
    "hash",
    "id",
    "idx",
    "model",
    "parentUuid",
    "promptId",
    "requestId",
    "role",
    "sender",
    "sessionId",
    "sessionKey",
    "session_id",
    "sha256",
    "source",
    "status",
    "stop_reason",
    "talentId",
    "timestamp",
    "tool_use_id",
    "type",
    "uuid",
    "version",
];

/// Whether a decoded value is readable content rather than an identifier,
/// hash, timestamp or bare number.
fn keep(text: &str) -> bool {
    let trimmed = text.trim();
    let chars = trimmed.chars().count();
    if chars < MIN_CHARS {
        return false;
    }
    if trimmed.chars().any(is_cjk) {
        return true;
    }
    if !trimmed.chars().any(char::is_whitespace) {
        // One token: keep a short word, drop ids, digests, timestamps,
        // versions, URLs and encoded blobs.
        let digits = trimmed.chars().filter(char::is_ascii_digit).count();
        return chars < IDENTIFIER_CHARS
            && trimmed.chars().any(char::is_alphabetic)
            && digits * 2 < chars
            && !trimmed.chars().all(|c| c.is_ascii_hexdigit() || c == '-');
    }
    trimmed.chars().any(char::is_alphabetic)
}

/// A forgiving JSON scanner: it never fails, tolerates a slice cut at either
/// end, and reports each string value (not key) with its raw byte range.
struct JsonScanner<'a> {
    raw: &'a [u8],
    at: usize,
    /// The key whose value comes next, while it is known.
    key: Option<String>,
}

impl<'a> JsonScanner<'a> {
    fn new(raw: &'a [u8]) -> Self {
        Self {
            raw,
            at: 0,
            key: None,
        }
    }

    fn scan(&mut self, spans: &mut Vec<Span>) {
        while self.at < self.raw.len() {
            match self.raw[self.at] {
                b'"' => {
                    let start = self.at;
                    let (text, end) = self.string();
                    self.at = end;
                    if self.is_key() {
                        self.key = Some(text);
                        continue;
                    }
                    if self
                        .key
                        .take()
                        .is_none_or(|key| !METADATA_KEYS.contains(&key.as_str()))
                    {
                        self.value(start, end, text, spans);
                    }
                }
                // A value that is not a string, or the end of an object or
                // array, ends what the last key named.
                b',' | b'{' | b'[' | b'}' | b']' => {
                    if self.raw[self.at] != b'[' {
                        self.key = None;
                    }
                    self.at += 1;
                }
                _ => self.at += 1,
            }
        }
    }

    /// The decoded string starting at `self.at` (a quote) and the index just
    /// past its closing quote, or the end of input for a cut string.
    fn string(&self) -> (String, usize) {
        let mut out: Vec<u8> = Vec::new();
        let mut i = self.at + 1;
        let mut high: Option<u16> = None;
        while i < self.raw.len() {
            match self.raw[i] {
                b'"' => return (String::from_utf8_lossy(&out).into_owned(), i + 1),
                b'\\' if i + 1 < self.raw.len() => {
                    let escaped = self.raw[i + 1];
                    i += 2;
                    let ch = match escaped {
                        b'n' => Some('\n'),
                        b't' => Some('\t'),
                        b'r' => Some('\r'),
                        b'b' | b'f' => Some(' '),
                        b'u' => {
                            let code = self
                                .raw
                                .get(i..i + 4)
                                .and_then(|h| std::str::from_utf8(h).ok())
                                .and_then(|h| u16::from_str_radix(h, 16).ok());
                            i += 4;
                            match code {
                                Some(c @ 0xD800..=0xDBFF) => {
                                    high = Some(c);
                                    None
                                }
                                Some(c @ 0xDC00..=0xDFFF) => high.take().and_then(|h| {
                                    char::from_u32(
                                        0x10000
                                            + (((h as u32 - 0xD800) << 10) | (c as u32 - 0xDC00)),
                                    )
                                }),
                                Some(c) => char::from_u32(c as u32),
                                None => None,
                            }
                        }
                        other => Some(other as char),
                    };
                    if let Some(ch) = ch {
                        let mut buf = [0u8; 4];
                        out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
                    }
                }
                b => {
                    out.push(b);
                    i += 1;
                }
            }
        }
        (String::from_utf8_lossy(&out).into_owned(), self.raw.len())
    }

    /// A string followed by `:` is an object key.
    fn is_key(&self) -> bool {
        self.raw[self.at..]
            .iter()
            .find(|b| !b.is_ascii_whitespace())
            .is_some_and(|&b| b == b':')
    }

    fn value(&self, start: usize, end: usize, text: String, spans: &mut Vec<Span>) {
        let trimmed = text.trim_start();
        // A value that is itself a JSON document (tool arguments, nested
        // records) is read for its own values; all of them cite the outer
        // string, whose raw bytes are what the event holds.
        if (trimmed.starts_with('{') || trimmed.starts_with('['))
            && serde_json::from_str::<serde::de::IgnoredAny>(trimmed).is_ok()
        {
            let mut inner = Vec::new();
            JsonScanner::new(trimmed.as_bytes()).scan(&mut inner);
            let joined: Vec<String> = inner.into_iter().map(|s| s.text).collect();
            if !joined.is_empty() {
                spans.push(Span {
                    start,
                    end,
                    text: joined.join("\n"),
                });
            }
            return;
        }
        if keep(&text) {
            spans.push(Span {
                start,
                end,
                text: text.trim().to_owned(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_not_keys_ids_or_numbers_with_exact_raw_ranges() {
        let raw = r#"{"type":"text","sessionKey":"cli:01a1d448-658f-706f-8523-74a374b1b8ea","timestamp":"2026-11-13T16:48:58Z","data":{"content":"已核實 Cubeage key \"live\"\n😀 ok","role":"user","n":42}}"#.as_bytes();
        let spans = project(raw);
        let texts: Vec<&str> = spans.iter().map(|s| s.text.as_str()).collect();
        // Values of metadata keys (`type`, `role`, ids, times) are not read.
        assert_eq!(texts, vec!["已核實 Cubeage key \"live\"\n😀 ok"]);
        for span in &spans {
            assert_eq!(raw[span.start], b'"');
            assert_eq!(raw[span.end - 1], b'"');
        }
        let content = &spans[0];
        assert!(
            std::str::from_utf8(&raw[content.start..content.end])
                .unwrap()
                .starts_with(r#""已"#)
        );
    }

    #[test]
    fn nested_json_strings_are_read_and_cite_the_outer_value() {
        let raw =
            r#"{"record_json":"{\"payload\":{\"text\":\"你好 nested text here\"}}","id":"x"}"#
                .as_bytes();
        let spans = project(raw);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].text, "你好 nested text here");
        assert_eq!(
            &raw[spans[0].start..spans[0].start + 17],
            br#""{\"payload\":{\""#
        );
    }

    #[test]
    fn cut_windows_plain_text_and_identifiers() {
        // A slice that starts inside a value and ends inside another.
        let raw = br#"tail of a sentence here","next":"another readable sentence that is cut"#;
        let texts: Vec<String> = project(raw).into_iter().map(|s| s.text).collect();
        assert_eq!(
            texts,
            vec![
                "tail of a sentence here",
                "another readable sentence that is cut"
            ]
        );
        let doc = b"# Topic\n\nDeploy runbook for staging\n0123456789abcdef0123456789abcdef\n";
        let texts: Vec<String> = project(doc).into_iter().map(|s| s.text).collect();
        assert_eq!(texts, vec!["# Topic", "Deploy runbook for staging"]);
        for id in [
            "2026-11-13T16:48:58Z",
            "v1.31.0",
            "01a1d448-658f-706f-8523-74a374b1b8ea",
            "14472c6ca7b58f19c858f7f32388ed1c180f9641",
            "msg_01m76tjsgpeh0a1vn7hmt21kke",
            "42",
        ] {
            assert!(!keep(id), "{id}");
        }
        for word in ["user", "deploy", "粵語", "收到"] {
            assert!(keep(word), "{word}");
        }
        assert!(readable(b"{\"a\":\"hello world\"}").contains("hello world"));
    }
}
