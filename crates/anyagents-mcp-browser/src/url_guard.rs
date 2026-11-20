//! URL normalization and scheme safety validation for browser navigation.

/// Maximum text payload returned from `browser_snapshot`, `browser_eval`, or HTML reads.
pub const MAX_TEXT_BYTES: usize = 256 * 1024;

/// Normalize and validate a URL for browser navigation.
///
/// Permits `http://`, `https://`, and `about:blank`. Bare hostnames (such as
/// `example.com` or `localhost:3000/app`) default to `https://`. Rejects
/// dangerous schemes such as `file:`, `javascript:`, `chrome:`, and `data:`.
pub fn navigable_url(raw: &str) -> Result<String, String> {
    let cleaned: String = raw
        .trim_matches(|c: char| c <= ' ')
        .chars()
        .filter(|c| !matches!(c, '\t' | '\n' | '\r'))
        .collect();
    if cleaned.is_empty() {
        return Err("browser navigation requires a non-empty `url`".to_string());
    }
    if cleaned.eq_ignore_ascii_case("about:blank") {
        return Ok("about:blank".to_string());
    }
    match url_scheme(&cleaned).as_deref() {
        None => {
            let candidate = format!("https://{cleaned}");
            url::Url::parse(&candidate)
                .map_err(|e| format!("invalid navigation URL `{raw}`: {e}"))?;
            Ok(candidate)
        }
        Some("http" | "https") => {
            url::Url::parse(&cleaned)
                .map_err(|e| format!("invalid navigation URL `{raw}`: {e}"))?;
            Ok(cleaned)
        }
        Some(scheme) => Err(format!(
            "browser navigation allows only `http://`, `https://`, and `about:blank`, not `{scheme}:`"
        )),
    }
}

/// Whether the active page URL is safe to inspect or evaluate script against.
pub fn readable_page_url(url: &str) -> bool {
    let trimmed = url.trim();
    trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("about:blank")
        || trimmed
            .to_ascii_lowercase()
            .starts_with("chrome-error://chromewebdata/")
        || matches!(url_scheme(trimmed).as_deref(), Some("http" | "https"))
}

/// Extract the lowercased scheme prefix when present and not a `host:port`.
fn url_scheme(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once(':')?;
    let valid_scheme = scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    let after_port = rest.trim_start_matches(|c: char| c.is_ascii_digit());
    let is_port = (scheme.eq_ignore_ascii_case("localhost") || scheme.contains('.'))
        && after_port.len() < rest.len()
        && after_port
            .chars()
            .next()
            .is_none_or(|c| matches!(c, '/' | '?' | '#'));
    (valid_scheme && !is_port).then(|| scheme.to_ascii_lowercase())
}

/// Truncate UTF-8 text safely at `max_bytes`, returning `(text, truncated)`.
pub fn truncate_utf8(mut text: String, max_bytes: usize) -> (String, bool) {
    if text.len() <= max_bytes {
        return (text, false);
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    (text, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn navigable_url_accepts_http_https_about_blank_and_bare_hosts() {
        assert_eq!(navigable_url("about:blank").unwrap(), "about:blank");
        assert_eq!(
            navigable_url("https://example.com/path").unwrap(),
            "https://example.com/path"
        );
        assert_eq!(
            navigable_url("http://127.0.0.1:8080/health").unwrap(),
            "http://127.0.0.1:8080/health"
        );
        assert_eq!(
            navigable_url("example.com/docs").unwrap(),
            "https://example.com/docs"
        );
        assert_eq!(
            navigable_url("localhost:3000/login").unwrap(),
            "https://localhost:3000/login"
        );
    }

    #[test]
    fn navigable_url_rejects_disallowed_schemes_and_empty() {
        for bad in [
            "",
            "   ",
            "file:///etc/passwd",
            "javascript:alert(1)",
            "chrome://settings",
            "data:text/html,<h1>hi</h1>",
        ] {
            assert!(
                navigable_url(bad).is_err(),
                "expected `{bad}` to be rejected"
            );
        }
    }

    #[test]
    fn truncate_utf8_respects_multibyte_boundaries() {
        let s = "héllo 🌍 world".to_string();
        let (cut, truncated) = truncate_utf8(s, 8);
        assert!(truncated);
        assert!(cut.len() <= 8);
        assert!(std::str::from_utf8(cut.as_bytes()).is_ok());
    }
}
