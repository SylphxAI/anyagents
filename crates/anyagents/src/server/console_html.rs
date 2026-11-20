//! Shared HTML rendering, security headers, content negotiation, and canonical
//! origin redirect helpers for the embedded AnyAgents Web Console and OIDC auth.

use axum::body::Body;
use axum::http::header::{ACCEPT, CACHE_CONTROL, CONTENT_TYPE, HOST, SET_COOKIE};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use url::Url;

pub(crate) const SIGNIN_HTML: &str = include_str!("console/signin.html");
pub(crate) const AUTH_RECOVERY_HTML: &str = include_str!("console/auth_recovery.html");

pub(crate) const CONSOLE_CSP: &str =
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'";

/// Escape untrusted or interpolated text for safe inclusion inside HTML text or attributes.
pub(crate) fn html_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

/// Return `true` when the request explicitly asks for JSON (and does not accept
/// HTML), so automated API clients receive structured JSON while browsers and
/// default `GET` clients receive human-readable HTML.
pub(crate) fn accepts_json_only(headers: &HeaderMap) -> bool {
    let Some(raw) = headers.get(ACCEPT).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let lower = raw.to_ascii_lowercase();
    let wants_json = lower
        .split(',')
        .map(|part| part.split(';').next().unwrap_or("").trim())
        .any(|media| media == "application/json" || media.ends_with("+json"));
    let wants_html = lower
        .split(',')
        .map(|part| part.split(';').next().unwrap_or("").trim())
        .any(|media| media == "text/html" || media == "application/xhtml+xml" || media == "*/*");
    wants_json && !wants_html
}

/// Validate an optional post-login `next` redirect target strictly as a safe
/// same-origin relative path (blocking open redirects and auth loops).
pub(crate) fn sanitize_next_path(raw: Option<&str>) -> Option<String> {
    let trimmed = raw?.trim();
    if trimmed.is_empty() || trimmed.len() > 512 {
        return None;
    }
    if !trimmed.starts_with('/') || trimmed.starts_with("//") || trimmed.starts_with("/\\") {
        return None;
    }
    if trimmed
        .bytes()
        .any(|b| matches!(b, b'\r' | b'\n' | b'\0' | b'\\') || b < 0x20)
    {
        return None;
    }
    if trimmed.starts_with("/admin/auth/") || trimmed.starts_with("/auth/") {
        return None;
    }
    Some(trimmed.to_string())
}

fn percent_encode_query_value(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            use std::fmt::Write as _;
            let _ = write!(&mut out, "%{byte:02X}");
        }
    }
    out
}

/// Extract the effective client-facing host header (`X-Forwarded-Host` first, then `Host`).
pub(crate) fn request_host_header(headers: &HeaderMap) -> Option<String> {
    if let Some(xfh) = headers
        .get("x-forwarded-host")
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.split(',').next())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return Some(xfh.to_string());
    }
    headers
        .get(HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Compare the incoming request's `X-Forwarded-Host`/`Host` against `base_url`'s
/// canonical host and effective port. When they differ, return the canonical
/// `{base_url}/admin/auth/login` redirect URL (preserving a safe relative `next`
/// parameter if provided) so the pre-auth cookie is only ever minted on the
/// canonical origin that receives the OIDC callback.
pub(crate) fn canonical_login_redirect_url(
    base_url: &str,
    headers: &HeaderMap,
    next: Option<&str>,
) -> Option<String> {
    let incoming_raw = request_host_header(headers)?;
    let parsed_base = Url::parse(base_url).ok()?;
    let canonical_host = parsed_base.host_str()?.to_ascii_lowercase();
    let canonical_port = parsed_base.port_or_known_default();

    let matches_canonical = Url::parse(&format!("{}://{incoming_raw}", parsed_base.scheme()))
        .ok()
        .is_some_and(|parsed_in| {
            parsed_in.path() == "/"
                && parsed_in.query().is_none()
                && parsed_in.fragment().is_none()
                && parsed_in.username().is_empty()
                && parsed_in.password().is_none()
                && parsed_in
                    .host_str()
                    .is_some_and(|h| h.to_ascii_lowercase() == canonical_host)
                && parsed_in.port_or_known_default() == canonical_port
        });

    if matches_canonical {
        return None;
    }

    let mut location = format!("{}/admin/auth/login", base_url.trim_end_matches('/'));
    if let Some(safe_next) = sanitize_next_path(next) {
        location.push_str("?next=");
        location.push_str(&percent_encode_query_value(&safe_next));
    }
    Some(location)
}

/// Build an HTTP response with strict console security headers and `Cache-Control: no-store`.
pub(crate) fn console_response(
    status: StatusCode,
    content_type: &'static str,
    body: String,
) -> Response {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, content_type)
        .header(CACHE_CONTROL, "no-store")
        .header(
            HeaderName::from_static("content-security-policy"),
            HeaderValue::from_static(CONSOLE_CSP),
        )
        .header(
            HeaderName::from_static("x-frame-options"),
            HeaderValue::from_static("DENY"),
        )
        .header(
            HeaderName::from_static("x-content-type-options"),
            HeaderValue::from_static("nosniff"),
        )
        .header(
            HeaderName::from_static("referrer-policy"),
            HeaderValue::from_static("no-referrer"),
        )
        .body(Body::from(body))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

/// Render the clean AnyAgents Sign-In page (`signin.html`) for unauthenticated OIDC visitors.
pub(crate) fn render_signin_response(coordinator_url: Option<&str>) -> Response {
    let escaped_url = coordinator_url
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|u| html_escape(u.trim_end_matches('/')))
        .unwrap_or_default();
    let html = SIGNIN_HTML.replace("{{COORDINATOR_URL}}", &escaped_url);
    console_response(StatusCode::OK, "text/html; charset=utf-8", html)
}

/// Render the self-contained HTML callback recovery page (`auth_recovery.html`)
/// when the OIDC pre-auth state cookie is missing, expired, or mismatched.
pub(crate) fn render_auth_recovery_response(
    status: StatusCode,
    reason: &str,
    base_url: Option<&str>,
    clear_preauth_cookie: Option<&str>,
) -> Response {
    let normalized_base = base_url
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|u| u.trim_end_matches('/'));
    let login_url = match normalized_base {
        Some(base) => format!("{base}/admin/auth/login"),
        None => "/admin/auth/login".to_string(),
    };
    let home_url = match normalized_base {
        Some(base) => format!("{base}/"),
        None => "/".to_string(),
    };
    let html = AUTH_RECOVERY_HTML
        .replace("{{REASON}}", &html_escape(reason))
        .replace("{{LOGIN_URL}}", &html_escape(&login_url))
        .replace("{{HOME_URL}}", &html_escape(&home_url));

    let mut resp = console_response(status, "text/html; charset=utf-8", html);
    if let Some(cookie) = clear_preauth_cookie
        && let Ok(hdr) = HeaderValue::from_str(cookie)
    {
        resp.headers_mut().append(SET_COOKIE, hdr);
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_next_path_allows_safe_relative_and_blocks_open_redirects() {
        assert_eq!(sanitize_next_path(Some("/")), Some("/".to_string()));
        assert_eq!(
            sanitize_next_path(Some("/?tab=secrets")),
            Some("/?tab=secrets".to_string())
        );
        assert_eq!(
            sanitize_next_path(Some("/#fleet")),
            Some("/#fleet".to_string())
        );

        for rejected in [
            "",
            "   ",
            "https://evil.example/",
            "//evil.example/path",
            "/\\evil.example",
            "/foo\\bar",
            "/foo\r\nSet-Cookie: a=b",
            "/admin/auth/login",
            "/auth/callback",
        ] {
            assert_eq!(
                sanitize_next_path(Some(rejected)),
                None,
                "must reject `{rejected}`"
            );
        }
    }

    #[test]
    fn canonical_login_redirect_detects_alias_host_and_respects_xfh() {
        let base = "https://console.example.com";

        // Matching Host -> no redirect.
        let mut h_match = HeaderMap::new();
        h_match.insert(HOST, HeaderValue::from_static("console.example.com"));
        assert_eq!(canonical_login_redirect_url(base, &h_match, None), None);

        // Matching explicit default HTTPS port -> no redirect.
        let mut h_port443 = HeaderMap::new();
        h_port443.insert(HOST, HeaderValue::from_static("CONSOLE.EXAMPLE.COM:443"));
        assert_eq!(canonical_login_redirect_url(base, &h_port443, None), None);

        // Alias Host -> 302 redirect preserving safe next.
        let mut h_alias = HeaderMap::new();
        h_alias.insert(HOST, HeaderValue::from_static("alias.example.com"));
        assert_eq!(
            canonical_login_redirect_url(base, &h_alias, Some("/?tab=fleet")),
            Some("https://console.example.com/admin/auth/login?next=%2F%3Ftab%3Dfleet".to_string())
        );

        // Unsafe next is dropped on redirect.
        assert_eq!(
            canonical_login_redirect_url(base, &h_alias, Some("//evil.example")),
            Some("https://console.example.com/admin/auth/login".to_string())
        );

        // Reverse proxy: internal Host + canonical X-Forwarded-Host -> no redirect.
        let mut h_proxy_ok = HeaderMap::new();
        h_proxy_ok.insert(HOST, HeaderValue::from_static("127.0.0.1:8788"));
        h_proxy_ok.insert(
            HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_static("console.example.com"),
        );
        assert_eq!(canonical_login_redirect_url(base, &h_proxy_ok, None), None);

        // Reverse proxy: canonical Host + alias X-Forwarded-Host -> redirect.
        let mut h_proxy_alias = HeaderMap::new();
        h_proxy_alias.insert(HOST, HeaderValue::from_static("console.example.com"));
        h_proxy_alias.insert(
            HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_static("alt.example.com, edge.internal"),
        );
        assert_eq!(
            canonical_login_redirect_url(base, &h_proxy_alias, None),
            Some("https://console.example.com/admin/auth/login".to_string())
        );

        // Missing Host headers -> no redirect.
        assert_eq!(
            canonical_login_redirect_url(base, &HeaderMap::new(), None),
            None
        );
    }

    #[test]
    fn accepts_json_only_distinguishes_api_clients_from_browsers() {
        let mut h_none = HeaderMap::new();
        assert!(!accepts_json_only(&h_none));

        h_none.insert(
            ACCEPT,
            HeaderValue::from_static(
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            ),
        );
        assert!(!accepts_json_only(&h_none));

        let mut h_star = HeaderMap::new();
        h_star.insert(ACCEPT, HeaderValue::from_static("*/*"));
        assert!(!accepts_json_only(&h_star));

        let mut h_json = HeaderMap::new();
        h_json.insert(ACCEPT, HeaderValue::from_static("application/json"));
        assert!(accepts_json_only(&h_json));

        let mut h_json_mixed = HeaderMap::new();
        h_json_mixed.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/plain;q=0.5"),
        );
        assert!(accepts_json_only(&h_json_mixed));
    }
}
