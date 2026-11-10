//! Pure-Rust web utilities for SSRF IP validation and HTML-to-Markdown extraction.

use std::net::IpAddr;

/// Returns `true` if `ip` is a global unicast address safe for outbound HTTP fetches,
/// or `false` if it belongs to loopback, private, link-local, CGNAT, multicast,
/// broadcast, documentation, benchmarking, or unspecified ranges.
pub fn is_global_unicast_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_documentation()
                || v4.is_unspecified()
                || v4.is_multicast()
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0b1100_0000) == 0b0100_0000) // CGNAT 100.64.0.0/10
                || (v4.octets()[0] == 198 && (v4.octets()[1] & 0xfe) == 18)) // Benchmarking 198.18.0.0/15
        }
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_global_unicast_ip(IpAddr::V4(mapped));
            }
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (v6.segments()[0] & 0xfe00) == 0xfc00 // Unique local fc00::/7
                || (v6.segments()[0] & 0xffc0) == 0xfe80 // Link-local fe80::/10
                || (v6.segments()[0] == 0x2001 && v6.segments()[1] == 0x0db8)) // Documentation 2001:db8::/32
        }
    }
}

/// Extracts an optional `<title>` and converts an HTML document into clean Markdown,
/// stripping `<script>`, `<style>`, `<noscript>`, `<svg>`, and `<head>` blocks.
pub fn html_to_markdown(raw: &str) -> (Option<String>, String) {
    let trimmed = raw.trim();
    if !trimmed.contains('<')
        || (!trimmed.to_ascii_lowercase().contains("<html")
            && !trimmed.to_ascii_lowercase().contains("<body")
            && !trimmed.to_ascii_lowercase().contains("<p")
            && !trimmed.to_ascii_lowercase().contains("<div")
            && !trimmed.to_ascii_lowercase().contains("<h1")
            && !trimmed.to_ascii_lowercase().contains("<h2")
            && !trimmed.to_ascii_lowercase().contains("<article")
            && !trimmed.to_ascii_lowercase().contains("<main"))
    {
        return (None, trimmed.to_string());
    }

    let title = extract_tag_inner(trimmed, "title").map(|t| decode_html_entities(t.trim()));
    let mut working = strip_tag_blocks(trimmed, "script");
    working = strip_tag_blocks(&working, "style");
    working = strip_tag_blocks(&working, "noscript");
    working = strip_tag_blocks(&working, "svg");
    working = strip_tag_blocks(&working, "head");

    let mut out = String::with_capacity(working.len());
    let mut in_tag = false;
    let mut tag_buf = String::new();
    for ch in working.chars() {
        if ch == '<' {
            in_tag = true;
            tag_buf.clear();
        } else if ch == '>' && in_tag {
            in_tag = false;
            let tag_lower = tag_buf
                .trim()
                .trim_start_matches('/')
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_ascii_lowercase();
            let is_closing = tag_buf.trim().starts_with('/');
            match tag_lower.as_str() {
                "p" | "div" | "section" | "article" | "header" | "footer" | "main" | "tr"
                | "br" | "hr" | "blockquote" | "pre" => {
                    out.push('\n');
                }
                "h1" => {
                    out.push_str(if is_closing { "\n" } else { "\n# " });
                }
                "h2" => {
                    out.push_str(if is_closing { "\n" } else { "\n## " });
                }
                "h3" | "h4" | "h5" | "h6" => {
                    out.push_str(if is_closing { "\n" } else { "\n### " });
                }
                "li" => {
                    out.push_str(if is_closing { "\n" } else { "\n- " });
                }
                _ => {}
            }
        } else if in_tag {
            tag_buf.push(ch);
        } else {
            out.push(ch);
        }
    }

    let decoded = decode_html_entities(&out);
    let mut lines = Vec::new();
    let mut blank_run = 0usize;
    for raw_line in decoded.lines() {
        let collapsed = raw_line.split_whitespace().collect::<Vec<_>>().join(" ");
        if collapsed.is_empty() {
            blank_run += 1;
            if blank_run <= 1 && !lines.is_empty() {
                lines.push(String::new());
            }
        } else {
            blank_run = 0;
            lines.push(collapsed);
        }
    }
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    (title.filter(|t| !t.is_empty()), lines.join("\n"))
}

fn extract_tag_inner<'a>(html: &'a str, tag: &str) -> Option<&'a str> {
    let lower = html.to_ascii_lowercase();
    let open_pat = format!("<{tag}");
    let close_pat = format!("</{tag}>");
    let start = lower.find(&open_pat)?;
    let gt = start + lower[start..].find('>')? + 1;
    let end = gt + lower[gt..].find(&close_pat)?;
    Some(&html[gt..end])
}

fn strip_tag_blocks(html: &str, tag: &str) -> String {
    let open_pat = format!("<{tag}");
    let close_pat = format!("</{tag}>");
    let mut out = String::with_capacity(html.len());
    let mut cursor = 0usize;
    let lower = html.to_ascii_lowercase();
    while let Some(rel_start) = lower[cursor..].find(&open_pat) {
        let abs_start = cursor + rel_start;
        out.push_str(&html[cursor..abs_start]);
        if let Some(rel_end) = lower[abs_start..].find(&close_pat) {
            cursor = abs_start + rel_end + close_pat.len();
        } else {
            cursor = html.len();
            break;
        }
    }
    out.push_str(&html[cursor..]);
    out
}

/// Decodes common HTML character entities into UTF-8 text.
pub fn decode_html_entities(input: &str) -> String {
    input
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
        .replace("&nbsp;", " ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_unicast_ip_classifier_rejects_local_and_private_ranges() {
        for blocked in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "0.0.0.0",
            "::1",
            "fe80::1",
            "fc00::1",
            "::ffff:127.0.0.1",
        ] {
            let ip: IpAddr = blocked.parse().unwrap();
            assert!(
                !is_global_unicast_ip(ip),
                "expected {blocked} to be rejected"
            );
        }
        for public in [
            "93.184.216.34",
            "1.1.1.1",
            "2606:2800:220:1:248:1893:25c8:1946",
        ] {
            let ip: IpAddr = public.parse().unwrap();
            assert!(is_global_unicast_ip(ip), "expected {public} to be accepted");
        }
    }

    #[test]
    fn html_to_markdown_extracts_title_and_clean_body() {
        let html = r#"
            <html>
              <head><title>Docs &amp; Guide</title><style>body{color:red}</style></head>
              <body>
                <script>console.log("ignore")</script>
                <h1>Heading One</h1>
                <p>First paragraph with <strong>bold</strong> text.</p>
                <ul><li>Alpha</li><li>Beta</li></ul>
              </body>
            </html>
        "#;
        let (title, md) = html_to_markdown(html);
        assert_eq!(title.as_deref(), Some("Docs & Guide"));
        assert!(md.contains("# Heading One"));
        assert!(md.contains("First paragraph with bold text."));
        assert!(md.contains("- Alpha"));
        assert!(!md.contains("console.log"));
    }
}
