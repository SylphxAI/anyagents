//! Pure-Rust HTML-to-Markdown extraction, entity decoding, and DuckDuckGo HTML parsing.

use url::Url;

/// A single search hit parsed from HTML search results.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedSearchHit {
    pub url: String,
    pub title: Option<String>,
    pub snippet: Option<String>,
}

/// Extract an optional `<title>` and convert an HTML document into clean Markdown/text.
///
/// Non-HTML payloads (plain text, Markdown, JSON) are returned trimmed without alteration.
pub fn html_to_markdown(raw: &str) -> (Option<String>, String) {
    let trimmed = raw.trim();
    let lower_probe = trimmed.to_ascii_lowercase();
    if !trimmed.contains('<')
        || (!lower_probe.contains("<html")
            && !lower_probe.contains("<body")
            && !lower_probe.contains("<p")
            && !lower_probe.contains("<div")
            && !lower_probe.contains("<h1")
            && !lower_probe.contains("<h2")
            && !lower_probe.contains("<article")
            && !lower_probe.contains("<main"))
    {
        return (None, trimmed.to_string());
    }

    let title = extract_tag_inner(trimmed, "title")
        .map(|t| collapse_inline_whitespace(&decode_html_entities(t)));

    let mut working = strip_html_comments(trimmed);
    for tag in ["script", "style", "noscript", "svg", "head"] {
        working = strip_tag_blocks(&working, tag);
    }

    let mut out = String::with_capacity(working.len());
    let mut in_tag = false;
    let mut tag_buf = String::new();
    for ch in working.chars() {
        if ch == '<' {
            in_tag = true;
            tag_buf.clear();
        } else if ch == '>' && in_tag {
            in_tag = false;
            let trimmed_tag = tag_buf.trim();
            let is_closing = trimmed_tag.starts_with('/');
            let tag_name = trimmed_tag
                .trim_start_matches('/')
                .split(|c: char| c.is_ascii_whitespace() || c == '/')
                .next()
                .unwrap_or("")
                .to_ascii_lowercase();
            match tag_name.as_str() {
                "p" | "div" | "section" | "article" | "header" | "footer" | "main" | "tr"
                | "br" | "hr" | "blockquote" | "pre" => {
                    out.push('\n');
                }
                "h1" => out.push_str(if is_closing { "\n" } else { "\n# " }),
                "h2" => out.push_str(if is_closing { "\n" } else { "\n## " }),
                "h3" => out.push_str(if is_closing { "\n" } else { "\n### " }),
                "h4" | "h5" | "h6" => out.push_str(if is_closing { "\n" } else { "\n#### " }),
                "li" => out.push_str(if is_closing { "\n" } else { "\n- " }),
                "td" | "th" => {
                    if !is_closing {
                        out.push(' ');
                    }
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
        let collapsed = collapse_inline_whitespace(raw_line);
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

/// Parse search results from DuckDuckGo's HTML endpoint (`https://html.duckduckgo.com/html/`).
pub fn parse_duckduckgo_html(html: &str) -> Vec<ParsedSearchHit> {
    let mut hits = Vec::new();
    let mut cursor = 0usize;

    while let Some(rel) = html[cursor..].find("result__a") {
        let marker_pos = cursor + rel;
        let Some(tag_start) = html[..marker_pos].rfind('<') else {
            cursor = marker_pos + "result__a".len();
            continue;
        };
        let Some(tag_end_rel) = html[marker_pos..].find('>') else {
            break;
        };
        let tag_end = marker_pos + tag_end_rel;
        let open_tag = &html[tag_start..=tag_end];
        let body_start = tag_end + 1;
        let Some(close_rel) = html[body_start..].to_ascii_lowercase().find("</a>") else {
            cursor = body_start;
            continue;
        };
        let body_end = body_start + close_rel;
        let raw_title = strip_all_tags(&html[body_start..body_end]);
        let title = {
            let clean = collapse_inline_whitespace(&decode_html_entities(&raw_title));
            (!clean.is_empty()).then_some(clean)
        };

        let next_result_start = html[body_end..]
            .find("result__a")
            .map(|off| body_end + off)
            .unwrap_or(html.len());
        let region = &html[body_end..next_result_start];
        let snippet = extract_class_inner_text(region, "result__snippet");

        if let Some(raw_href) = extract_attr(open_tag, "href")
            && let Some(resolved_url) = decode_duckduckgo_href(&decode_html_entities(&raw_href))
        {
            hits.push(ParsedSearchHit {
                url: resolved_url,
                title,
                snippet,
            });
        }
        cursor = body_end + 4;
    }
    hits
}

fn decode_duckduckgo_href(raw_href: &str) -> Option<String> {
    let trimmed = raw_href.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = if trimmed.starts_with("//") {
        format!("https:{trimmed}")
    } else if trimmed.starts_with('/') {
        format!("https://duckduckgo.com{trimmed}")
    } else {
        trimmed.to_string()
    };
    let parsed = Url::parse(&normalized).ok()?;
    if parsed
        .host_str()
        .is_some_and(|h| h == "duckduckgo.com" || h.ends_with(".duckduckgo.com"))
    {
        if parsed.path().starts_with("/y.js") {
            return None;
        }
        for (k, v) in parsed.query_pairs() {
            if k == "uddg" {
                let candidate = v.trim().to_string();
                if let Ok(target) = Url::parse(&candidate)
                    && matches!(target.scheme(), "http" | "https")
                {
                    return Some(candidate);
                }
            }
        }
        return None;
    }
    matches!(parsed.scheme(), "http" | "https").then_some(normalized)
}

fn extract_class_inner_text(fragment: &str, class_name: &str) -> Option<String> {
    let marker = fragment.find(class_name)?;
    let gt = marker + fragment[marker..].find('>')? + 1;
    let rest = &fragment[gt..];
    let lower_rest = rest.to_ascii_lowercase();
    let end = lower_rest
        .find("</a>")
        .or_else(|| lower_rest.find("</div>"))
        .or_else(|| lower_rest.find("</span>"))?;
    let stripped = strip_all_tags(&rest[..end]);
    let clean = collapse_inline_whitespace(&decode_html_entities(&stripped));
    (!clean.is_empty()).then_some(clean)
}

fn extract_attr(tag: &str, attr: &str) -> Option<String> {
    let lower = tag.to_ascii_lowercase();
    let pattern = format!("{attr}=");
    let pos = lower.find(&pattern)?;
    let after = &tag[pos + pattern.len()..];
    let trimmed = after.trim_start();
    if let Some(rest) = trimmed.strip_prefix('"') {
        let end = rest.find('"')?;
        Some(rest[..end].to_string())
    } else if let Some(rest) = trimmed.strip_prefix('\'') {
        let end = rest.find('\'')?;
        Some(rest[..end].to_string())
    } else {
        let end = trimmed
            .find(|c: char| c.is_ascii_whitespace() || c == '>')
            .unwrap_or(trimmed.len());
        Some(trimmed[..end].to_string())
    }
}

fn strip_all_tags(fragment: &str) -> String {
    let mut out = String::with_capacity(fragment.len());
    let mut in_tag = false;
    for ch in fragment.chars() {
        if ch == '<' {
            in_tag = true;
        } else if ch == '>' && in_tag {
            in_tag = false;
        } else if !in_tag {
            out.push(ch);
        }
    }
    out
}

fn collapse_inline_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn strip_html_comments(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut cursor = 0usize;
    while let Some(rel) = html[cursor..].find("<!--") {
        let start = cursor + rel;
        out.push_str(&html[cursor..start]);
        if let Some(end_rel) = html[start + 4..].find("-->") {
            cursor = start + 4 + end_rel + 3;
        } else {
            return out;
        }
    }
    out.push_str(&html[cursor..]);
    out
}

fn extract_tag_inner<'a>(html: &'a str, tag: &str) -> Option<&'a str> {
    let lower = html.to_ascii_lowercase();
    let open_pat = format!("<{tag}");
    let close_pat = format!("</{tag}>");
    let mut search_from = 0usize;
    while let Some(rel) = lower[search_from..].find(&open_pat) {
        let start = search_from + rel;
        let after_name = start + open_pat.len();
        if is_tag_boundary(lower.as_bytes().get(after_name).copied()) {
            let gt = after_name + lower[after_name..].find('>')? + 1;
            let end = gt + lower[gt..].find(&close_pat)?;
            return Some(&html[gt..end]);
        }
        search_from = after_name;
    }
    None
}

fn strip_tag_blocks(html: &str, tag: &str) -> String {
    let open_pat = format!("<{tag}");
    let close_pat = format!("</{tag}>");
    let lower = html.to_ascii_lowercase();
    let mut out = String::with_capacity(html.len());
    let mut cursor = 0usize;
    let mut search_from = 0usize;

    while let Some(rel_start) = lower[search_from..].find(&open_pat) {
        let abs_start = search_from + rel_start;
        let after_name = abs_start + open_pat.len();
        if !is_tag_boundary(lower.as_bytes().get(after_name).copied()) {
            search_from = after_name;
            continue;
        }
        out.push_str(&html[cursor..abs_start]);
        if let Some(rel_end) = lower[after_name..].find(&close_pat) {
            cursor = after_name + rel_end + close_pat.len();
            search_from = cursor;
        } else {
            cursor = html.len();
            break;
        }
    }
    out.push_str(&html[cursor..]);
    out
}

fn is_tag_boundary(next_byte: Option<u8>) -> bool {
    matches!(
        next_byte,
        None | Some(b'>' | b'/' | b' ' | b'\t' | b'\n' | b'\r')
    )
}

/// Decode common named and numeric HTML character entities (`&amp;`, `&#39;`, `&#x27;`, etc.).
pub fn decode_html_entities(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        let after_amp = &rest[amp + 1..];
        let Some(semi) = after_amp.find(';') else {
            out.push_str(&rest[amp..]);
            return out;
        };
        let entity = &after_amp[..semi];
        if entity.len() <= 12
            && let Some(decoded) = decode_single_entity(entity)
        {
            out.push_str(&decoded);
            rest = &after_amp[semi + 1..];
            continue;
        }
        out.push('&');
        rest = after_amp;
    }
    out.push_str(rest);
    out
}

fn decode_single_entity(entity: &str) -> Option<String> {
    match entity {
        "amp" => Some("&".to_string()),
        "lt" => Some("<".to_string()),
        "gt" => Some(">".to_string()),
        "quot" => Some("\"".to_string()),
        "apos" => Some("'".to_string()),
        "nbsp" => Some(" ".to_string()),
        "ndash" => Some("–".to_string()),
        "mdash" => Some("—".to_string()),
        "hellip" => Some("…".to_string()),
        other => {
            let num = other.strip_prefix('#')?;
            let code = if let Some(hex) = num.strip_prefix('x').or_else(|| num.strip_prefix('X')) {
                u32::from_str_radix(hex, 16).ok()?
            } else {
                num.parse::<u32>().ok()?
            };
            char::from_u32(code).map(|c| c.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn html_to_markdown_keeps_header_tag_and_strips_head_scripts_and_comments() {
        let html = r#"
            <html>
              <head><title>Rust &amp; MCP &#x2014; Guide</title><style>body{color:red}</style></head>
              <body>
                <!-- hidden comment -->
                <header><h1>Overview</h1></header>
                <script>alert("xss")</script>
                <p>Hello <b>world</b> &#39;2026&#39;!</p>
                <ul><li>First</li><li>Second</li></ul>
              </body>
            </html>
        "#;
        let (title, md) = html_to_markdown(html);
        assert_eq!(title.as_deref(), Some("Rust & MCP — Guide"));
        assert!(
            md.contains("# Overview"),
            "header tag must be preserved: {md}"
        );
        assert!(md.contains("Hello world '2026'!"));
        assert!(md.contains("- First"));
        assert!(md.contains("- Second"));
        assert!(!md.contains("alert"));
        assert!(!md.contains("hidden comment"));
    }

    #[test]
    fn parses_duckduckgo_html_results_and_unwraps_uddg_redirects() {
        let sample = r##"
            <div class="result results_links">
              <h2 class="result__title">
                <a rel="nofollow" class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.rust-lang.org%2F&amp;rut=123">Rust <b>Programming</b> Language</a>
              </h2>
              <a class="result__snippet" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.rust-lang.org%2F">A language empowering everyone to build reliable software.</a>
            </div>
            <div class="result results_links">
              <h2 class="result__title">
                <a rel="nofollow" class="result__a" href="https://duckduckgo.com/y.js?ad_provider=test">Sponsored Ad</a>
              </h2>
              <a class="result__snippet" href="#">Ad snippet</a>
            </div>
            <div class="result results_links">
              <h2 class="result__title">
                <a rel="nofollow" class="result__a" href="https://docs.rs/rmcp">rmcp &mdash; Docs.rs</a>
              </h2>
              <div class="result__snippet">Official Rust SDK for Model Context Protocol.</div>
            </div>
        "##;

        let hits = parse_duckduckgo_html(sample);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].url, "https://www.rust-lang.org/");
        assert_eq!(hits[0].title.as_deref(), Some("Rust Programming Language"));
        assert_eq!(
            hits[0].snippet.as_deref(),
            Some("A language empowering everyone to build reliable software.")
        );
        assert_eq!(hits[1].url, "https://docs.rs/rmcp");
        assert_eq!(hits[1].title.as_deref(), Some("rmcp — Docs.rs"));
        assert_eq!(
            hits[1].snippet.as_deref(),
            Some("Official Rust SDK for Model Context Protocol.")
        );
    }
}
