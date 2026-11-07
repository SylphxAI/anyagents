//! Deterministic masking at durable tool-output boundaries. This recognizes
//! explicit credential assignments and provider token prefixes, not arbitrary
//! personal data or passwords without a label. Unchanged text is borrowed.
use std::borrow::Cow;

const MARKER: &str = "[redacted]";

fn identifier(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn credential_key(key: &str) -> bool {
    [
        "password",
        "passwd",
        "api_key",
        "apikey",
        "access_token",
        "refresh_token",
        "client_secret",
        "authorization",
    ]
    .iter()
    .any(|name| key.eq_ignore_ascii_case(name))
}

fn reference(value: &str) -> bool {
    (value.is_empty() || value == MARKER)
        || value.starts_with("op://")
        || value.starts_with("op-otp://")
        || value.starts_with("secret://")
}

pub fn mask_known_secrets(text: &str) -> Cow<'_, str> {
    let bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        if !bytes[pos].is_ascii_alphabetic() && bytes[pos] != b'_' {
            pos += 1;
            continue;
        }
        let start = pos;
        while pos < bytes.len() && identifier(bytes[pos]) {
            pos += 1;
        }
        let key = &text[start..pos];
        if !credential_key(key) {
            continue;
        }
        let mut value = pos;
        if start > 0
            && matches!(bytes[start - 1], b'\'' | b'"')
            && bytes.get(value) == bytes.get(start - 1)
        {
            value += 1;
        }
        while bytes.get(value).is_some_and(u8::is_ascii_whitespace) {
            value += 1;
        }
        if !matches!(bytes.get(value), Some(b':' | b'=')) {
            continue;
        }
        value += 1;
        while bytes.get(value).is_some_and(u8::is_ascii_whitespace) {
            value += 1;
        }
        if text[value..].starts_with(MARKER) || matches!(bytes.get(value), Some(b'{' | b'[')) {
            continue;
        }
        let quote = bytes
            .get(value)
            .copied()
            .filter(|b| matches!(b, b'\'' | b'"'));
        if quote.is_some() {
            value += 1;
        }
        let end = if let Some(quote) = quote {
            let mut end = value;
            while end < bytes.len() && bytes[end] != quote {
                end += if bytes[end] == b'\\' && end + 1 < bytes.len() {
                    2
                } else {
                    1
                };
            }
            end
        } else if key.eq_ignore_ascii_case("authorization") {
            let mut end = value;
            while end < bytes.len() && !matches!(bytes[end], b'\r' | b'\n' | b',' | b';' | b'}') {
                end += 1;
            }
            end
        } else {
            let mut end = value;
            while end < bytes.len()
                && !bytes[end].is_ascii_whitespace()
                && !matches!(bytes[end], b',' | b';' | b'}' | b']')
            {
                end += 1;
            }
            end
        };
        if quote.is_none() && matches!(&text[value..end], "null" | "true" | "false") {
            continue;
        }
        if end > value && !reference(&text[value..end]) {
            spans.push((value, end));
        }
    }
    for prefix in [
        "ghp_",
        "gho_",
        "ghu_",
        "ghs_",
        "github_pat_",
        "sk_live_",
        "sk_test_",
        "rk_live_",
        "sk-proj-",
        "xoxb-",
        "xoxp-",
    ] {
        for (start, _) in text.match_indices(prefix) {
            if start > 0 && (identifier(bytes[start - 1]) || bytes[start - 1] == b'-') {
                continue;
            }
            let mut end = start + prefix.len();
            while end < bytes.len() && (identifier(bytes[end]) || bytes[end] == b'-') {
                end += 1;
            }
            if end - start >= prefix.len() + 16 {
                spans.push((start, end));
            }
        }
    }
    if spans.is_empty() {
        return Cow::Borrowed(text);
    }
    spans.sort_unstable();
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in spans {
        if let Some(last) = merged.last_mut()
            && start <= last.1
        {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    for (start, end) in merged {
        out.push_str(&text[cursor..start]);
        out.push_str(MARKER);
        cursor = end;
    }
    out.push_str(&text[cursor..]);
    Cow::Owned(out)
}

/// Mask string values, never structural keys, while retaining the JSON shape.
/// This also covers persisted native-turn snapshots containing nested outputs.
pub fn mask_json_strings(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::String(text) => {
            if let Cow::Owned(masked) = mask_known_secrets(text) {
                *text = masked;
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(mask_json_strings),
        serde_json::Value::Object(fields) => {
            for (key, value) in fields {
                if credential_key(key)
                    && let serde_json::Value::String(text) = value
                    && !reference(text)
                {
                    *text = MARKER.to_string();
                } else {
                    mask_json_strings(value);
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_assignments_and_known_prefixes_are_masked_without_changing_surroundings() {
        for (input, expected) in [
            (
                "password=unlabelled-fixture keep",
                "password=[redacted] keep",
            ),
            (
                r#"{ "password": "quoted\"fixture", "ok": true }"#,
                r#"{ "password": "[redacted]", "ok": true }"#,
            ),
            (
                "CLIENT_SECRET='fixture value'\nexit_code: 7",
                "CLIENT_SECRET='[redacted]'\nexit_code: 7",
            ),
            (
                "Authorization: Bearer fixture-token\nstatus: 200",
                "Authorization: [redacted]\nstatus: 200",
            ),
            ("密碼 password=測試值 done", "密碼 password=[redacted] done"),
        ] {
            assert_eq!(mask_known_secrets(input), expected);
            assert_eq!(mask_known_secrets(expected), expected);
        }
        let token = format!("ghp_{}", "a".repeat(36));
        assert_eq!(
            mask_known_secrets(&format!("prefix {token} suffix")),
            "prefix [redacted] suffix"
        );
    }

    #[test]
    fn ordinary_output_and_credential_references_are_byte_identical() {
        for text in [
            "exit_code: 7\nfailed to open /novel/long-diagnostic-name.txt\n測試🙂",
            "password_ref=op://vault/item/password",
            "password=op://vault/item/password",
            "api_key=secret://service/key",
            "access_token=[redacted]",
            "token_count: 123",
            "ghp_short",
            "password file is missing",
            r#"{"password":null,"api_key":false}"#,
        ] {
            assert!(
                matches!(mask_known_secrets(text), Cow::Borrowed(_)),
                "{text}"
            );
        }
    }
}

#[cfg(test)]
mod persistence_tests {
    use crate::{HistorySearchQuery, NewAgent, Store};

    #[test]
    fn tool_history_and_search_persist_only_masked_output() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("mask-test", "sylphx"))
            .unwrap();
        let raw = "password=fixturecanarypersistence";
        store.record_tool_event(&agent.id, "bash", raw).unwrap();
        let rows = store
            .search_history(&HistorySearchQuery {
                agent: Some(agent.name.clone()),
                only_tools: true,
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].text, "[tool:bash] password=[redacted]");
        let hits = store
            .search_history(&HistorySearchQuery {
                query: Some("fixturecanarypersistence".into()),
                agent: Some(agent.name),
                only_tools: true,
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert!(hits.is_empty());
    }
}
