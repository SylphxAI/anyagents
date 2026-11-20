//! Pure HTTP/TLS helper utilities built on `ureq`.

use std::io::Read as _;
use std::time::Duration;

use serde_json::{Value as JsonValue, json};

/// Error produced by an upstream search/fetch provider call.
#[derive(Debug)]
pub struct EngineError {
    pub status: Option<u16>,
    pub message: String,
    pub retriable: bool,
}

/// Whether an HTTP error status code should trigger failover to the next configured provider.
pub fn is_retriable_status(status: u16) -> bool {
    status == 402 || status == 408 || status == 429 || status >= 500
}

/// Send an HTTP GET or POST request expecting a JSON response.
pub fn send_http_json(
    method: &str,
    url: &str,
    headers: &[(&str, String)],
    body: Option<&JsonValue>,
    timeout_ms: u64,
) -> Result<JsonValue, EngineError> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_millis(timeout_ms)))
        .build()
        .new_agent();

    let res = if method.eq_ignore_ascii_case("GET") {
        let mut req = agent.get(url);
        for (k, v) in headers {
            req = req.header(*k, v);
        }
        req.call()
    } else {
        let mut req = agent.post(url);
        for (k, v) in headers {
            req = req.header(*k, v);
        }
        let bytes = serde_json::to_vec(body.unwrap_or(&json!({}))).unwrap_or_default();
        req.send(bytes.as_slice())
    };

    match res {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let text = resp.into_body().read_to_string().map_err(|e| EngineError {
                status: Some(status),
                message: format!("failed reading response body: {e}"),
                retriable: true,
            })?;
            serde_json::from_str::<JsonValue>(&text).map_err(|e| EngineError {
                status: Some(status),
                message: format!("invalid JSON from provider (HTTP {status}): {e}"),
                retriable: is_retriable_status(status),
            })
        }
        Err(ureq::Error::StatusCode(code)) => Err(EngineError {
            status: Some(code),
            message: format!("HTTP {code}"),
            retriable: is_retriable_status(code),
        }),
        Err(err) => Err(EngineError {
            status: None,
            message: format!("transport error: {err}"),
            retriable: true,
        }),
    }
}

/// Fetch a bounded UTF-8 text body over HTTP(S) GET.
pub fn send_http_text(
    url: &str,
    headers: &[(&str, &str)],
    timeout_ms: u64,
    max_bytes: u64,
) -> Result<(u16, String), EngineError> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_millis(timeout_ms)))
        .http_status_as_error(false)
        .build()
        .new_agent();
    let mut req = agent.get(url);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let resp = req.call().map_err(|err| EngineError {
        status: None,
        message: format!("transport error: {err}"),
        retriable: true,
    })?;
    let status = resp.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(EngineError {
            status: Some(status),
            message: format!("HTTP {status}"),
            retriable: is_retriable_status(status),
        });
    }
    let mut bytes = Vec::new();
    resp.into_body()
        .into_reader()
        .take(max_bytes)
        .read_to_end(&mut bytes)
        .map_err(|e| EngineError {
            status: Some(status),
            message: format!("failed reading response body: {e}"),
            retriable: true,
        })?;
    Ok((status, String::from_utf8_lossy(&bytes).into_owned()))
}

/// Percent-encode a query parameter value (`application/x-www-form-urlencoded`).
pub fn urlencode_query(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else if byte == b' ' {
            out.push('+');
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urlencodes_spaces_and_reserved_characters() {
        assert_eq!(urlencode_query("rust mcp & sdk"), "rust+mcp+%26+sdk");
        assert_eq!(urlencode_query("hello-world_1.0~"), "hello-world_1.0~");
    }
}
