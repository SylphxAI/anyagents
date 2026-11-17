//! Raw-body authentication boundary; no unverified payload reaches schedules.
use axum::http::{HeaderMap, StatusCode};
use serde_json::Value;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

pub(crate) fn receive(
    secret: Option<&str>,
    headers: &HeaderMap,
    body: &[u8],
    apply: impl FnOnce(&str, &Value, &str) -> Result<bool, String>,
) -> Result<bool, StatusCode> {
    let secret = secret
        .filter(|secret| !secret.is_empty())
        .ok_or(StatusCode::NOT_FOUND)?;
    let signature = one_header(headers, "x-hub-signature-256").ok_or(StatusCode::UNAUTHORIZED)?;
    let expected = format!("sha256={}", hmac_sha256_hex(secret.as_bytes(), body));
    if !bool::from(expected.as_bytes().ct_eq(signature.as_bytes())) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let event = one_header(headers, "x-github-event").ok_or(StatusCode::BAD_REQUEST)?;
    let delivery = one_header(headers, "x-github-delivery")
        .filter(|id| !id.is_empty() && id.len() <= 200)
        .ok_or(StatusCode::BAD_REQUEST)?;
    // Authenticated unsupported events (including ping) are acknowledged no-ops.
    if !matches!(
        event,
        "pull_request" | "check_run" | "check_suite" | "workflow_run"
    ) {
        return Ok(false);
    }
    let payload: Value = serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)?;
    apply(event, &payload, delivery).map_err(|_| StatusCode::BAD_REQUEST)
}

fn one_header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let mut values = headers.get_all(name).iter();
    let value = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    Some(value)
}

// RFC 2104 HMAC-SHA256. Constant-time comparison is provided by subtle above.
pub(crate) fn hmac_sha256_hex(key: &[u8], body: &[u8]) -> String {
    let mut block = [0u8; 64];
    if key.len() > block.len() {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|b| b ^ 0x36));
    inner.update(body);
    let mut outer = Sha256::new();
    outer.update(block.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    outer
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn headers(body: &[u8]) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            "x-hub-signature-256",
            format!("sha256={}", hmac_sha256_hex(b"secret", body))
                .parse()
                .unwrap(),
        );
        h.insert("x-github-event", "check_run".parse().unwrap());
        h.insert("x-github-delivery", "delivery-1".parse().unwrap());
        h
    }
    #[test]
    fn rfc4231_vectors() {
        assert_eq!(
            hmac_sha256_hex(&[0x0b; 20], b"Hi There"),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        assert_eq!(
            hmac_sha256_hex(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            ),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }
    #[test]
    fn authentication_precedes_parse_and_mutation() {
        let body = b" {\n\"example\": true }\n";
        let h = headers(body);
        let never = |_: &str, _: &Value, _: &str| -> Result<bool, String> {
            panic!("unauthenticated mutation")
        };
        assert_eq!(receive(None, &h, body, never), Err(StatusCode::NOT_FOUND));
        assert_eq!(
            receive(Some("wrong"), &h, b"invalid json", never),
            Err(StatusCode::UNAUTHORIZED)
        );
        assert_eq!(
            receive(Some("secret"), &h, br#"{"example":true}"#, never),
            Err(StatusCode::UNAUTHORIZED)
        );
        assert_eq!(
            receive(Some("secret"), &h, body, |event, payload, id| {
                assert_eq!(event, "check_run");
                assert_eq!(id, "delivery-1");
                assert_eq!(payload["example"], true);
                Ok(true)
            }),
            Ok(true)
        );
        let mut duplicate = h.clone();
        duplicate.append("x-hub-signature-256", h["x-hub-signature-256"].clone());
        assert_eq!(
            receive(Some("secret"), &duplicate, body, never),
            Err(StatusCode::UNAUTHORIZED)
        );
        let invalid = b"{";
        assert_eq!(
            receive(Some("secret"), &headers(invalid), invalid, never),
            Err(StatusCode::BAD_REQUEST)
        );
    }
}
