//! SSRF-safe web fetch engine with optional provider cascade and direct HTTP/TLS fallback.

use std::collections::HashMap;
use std::io::Read as _;
use std::time::{Duration, Instant};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use url::Url;

use crate::html::html_to_markdown;
use crate::http::{EngineError, is_retriable_status, send_http_json};
use crate::ssrf::{url_allowed_by_domain_policy, validate_public_url};

const DEFAULT_FETCH_MAX_CHARS: usize = 12_000;
const HARD_FETCH_MAX_CHARS: usize = 65_536;
const MAX_REDIRECT_HOPS: usize = 5;
const MAX_DIRECT_FETCH_BYTES: u64 = 2 * 1024 * 1024;
const MAX_FAILOVER_ATTEMPTS: usize = 3;

/// Parameters for `web_fetch`.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct WebFetchRequest {
    /// Target `http://` or `https://` URL to fetch (private/local IPs are blocked by SSRF guard).
    #[serde(alias = "href", alias = "target")]
    pub url: String,
    /// Optional fetch engine: `"auto"` (default: configured extraction APIs then direct HTTP/TLS)
    /// or explicit `"direct" | "firecrawl" | "exa" | "parallel" | "tavily"`.
    #[serde(default)]
    pub engine: Option<String>,
    /// Maximum number of characters to return (`256..=65536`, default `12000`).
    #[serde(default, alias = "max_characters")]
    pub max_chars: Option<usize>,
    /// Return raw response text without HTML-to-Markdown conversion when using direct fetch.
    #[serde(default)]
    pub raw: Option<bool>,
    /// Optional domain allowlist.
    #[serde(default, alias = "include_domains")]
    pub allowed_domains: Vec<String>,
    /// Optional domain blocklist.
    #[serde(default, alias = "exclude_domains")]
    pub blocked_domains: Vec<String>,
    /// Optional request timeout in milliseconds (`1000..=60000`).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// Supported fetch backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchEngine {
    Auto,
    Firecrawl,
    Exa,
    Parallel,
    Tavily,
    Direct,
}

impl FetchEngine {
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        let cleaned = raw.unwrap_or("auto").trim().to_ascii_lowercase();
        match cleaned.as_str() {
            "" | "auto" => Ok(Self::Auto),
            "firecrawl" | "fc" => Ok(Self::Firecrawl),
            "exa" => Ok(Self::Exa),
            "parallel" => Ok(Self::Parallel),
            "tavily" => Ok(Self::Tavily),
            "direct" | "http" => Ok(Self::Direct),
            other => Err(format!(
                "unknown web fetch engine `{other}`; expected `auto`, `direct`, `firecrawl`, \
                 `exa`, `parallel`, or `tavily`"
            )),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Firecrawl => "firecrawl",
            Self::Exa => "exa",
            Self::Parallel => "parallel",
            Self::Tavily => "tavily",
            Self::Direct => "direct",
        }
    }
}

fn fetch_engine_has_key(engine: FetchEngine, keys: &HashMap<String, String>) -> bool {
    match engine {
        FetchEngine::Firecrawl => keys.contains_key("FIRECRAWL_API_KEY"),
        FetchEngine::Exa => keys.contains_key("EXA_API_KEY"),
        FetchEngine::Parallel => keys.contains_key("PARALLEL_API_KEY"),
        FetchEngine::Tavily => keys.contains_key("TAVILY_API_KEY"),
        FetchEngine::Direct => true,
        FetchEngine::Auto => false,
    }
}

/// Build the fetch execution plan for `selected` given available `keys`.
pub fn plan_fetch_engines(
    selected: FetchEngine,
    keys: &HashMap<String, String>,
) -> Vec<FetchEngine> {
    match selected {
        FetchEngine::Auto => {
            let mut plan: Vec<FetchEngine> = [
                FetchEngine::Firecrawl,
                FetchEngine::Exa,
                FetchEngine::Parallel,
                FetchEngine::Tavily,
            ]
            .into_iter()
            .filter(|e| fetch_engine_has_key(*e, keys))
            .take(MAX_FAILOVER_ATTEMPTS)
            .collect();
            plan.push(FetchEngine::Direct);
            plan
        }
        named if fetch_engine_has_key(named, keys) => vec![named],
        _ => Vec::new(),
    }
}

/// Execute a web fetch request synchronously with SSRF enforcement.
pub fn execute_fetch(
    keys: &HashMap<String, String>,
    req: &WebFetchRequest,
    default_timeout_ms: u64,
    user_agent: &str,
) -> Result<JsonValue, String> {
    let url = req.url.trim();
    if url.is_empty() {
        return Err("`web_fetch` requires a non-empty `url`".to_string());
    }
    validate_public_url(url)?;
    if !url_allowed_by_domain_policy(url, &req.allowed_domains, &req.blocked_domains) {
        return Err(format!(
            "URL `{url}` violates the requested `allowed_domains` / `blocked_domains` policy"
        ));
    }

    let selected = FetchEngine::parse(req.engine.as_deref())?;
    let plan = plan_fetch_engines(selected, keys);
    if plan.is_empty() {
        return Err(format!(
            "no API key configured for requested fetch engine `{}`",
            selected.as_str()
        ));
    }

    let max_chars = req
        .max_chars
        .unwrap_or(DEFAULT_FETCH_MAX_CHARS)
        .clamp(256, HARD_FETCH_MAX_CHARS);
    let timeout_ms = req
        .timeout_ms
        .unwrap_or(default_timeout_ms)
        .clamp(1_000, 60_000);

    let mut attempts = Vec::new();
    let mut last_err = String::new();

    let ctx = FetchContext {
        keys,
        req,
        max_chars,
        timeout_ms,
        user_agent,
    };

    for engine in plan {
        let started = Instant::now();
        match execute_single_fetch(engine, url, &ctx) {
            Ok((final_url, title, content_type, full_text)) => {
                let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(0);
                if !url_allowed_by_domain_policy(
                    &final_url,
                    &req.allowed_domains,
                    &req.blocked_domains,
                ) {
                    return Err(format!(
                        "fetched destination URL `{final_url}` violates `allowed_domains` / `blocked_domains` policy"
                    ));
                }
                let original_chars = full_text.chars().count();
                let truncated = original_chars > max_chars;
                let bounded: String = if truncated {
                    full_text.chars().take(max_chars).collect()
                } else {
                    full_text
                };
                let fallback_used = !attempts.is_empty();
                attempts.push(json!({
                    "engine": engine.as_str(),
                    "outcome": "succeeded",
                    "duration_ms": duration_ms,
                }));
                return Ok(json!({
                    "action": "fetch",
                    "engine": engine.as_str(),
                    "fallback_used": fallback_used,
                    "url": final_url,
                    "title": title,
                    "content_type": content_type,
                    "content": bounded,
                    "original_chars": original_chars,
                    "truncated": truncated,
                    "attempts": attempts,
                }));
            }
            Err(err) => {
                let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(0);
                attempts.push(json!({
                    "engine": engine.as_str(),
                    "outcome": "failed",
                    "http_status": err.status,
                    "duration_ms": duration_ms,
                    "error": err.message,
                }));
                last_err = format!("{}: {}", engine.as_str(), err.message);
                if !err.retriable && selected != FetchEngine::Auto {
                    break;
                }
            }
        }
    }

    Err(format!("web fetch failed ({last_err})"))
}

struct FetchContext<'a> {
    keys: &'a HashMap<String, String>,
    req: &'a WebFetchRequest,
    max_chars: usize,
    timeout_ms: u64,
    user_agent: &'a str,
}

fn execute_single_fetch(
    engine: FetchEngine,
    url: &str,
    ctx: &FetchContext<'_>,
) -> Result<(String, Option<String>, String, String), EngineError> {
    if engine == FetchEngine::Direct {
        return execute_direct_fetch(url, ctx.req, ctx.timeout_ms, ctx.user_agent);
    }

    let (key_name, endpoint, header, bearer, body) = match engine {
        FetchEngine::Firecrawl => (
            "FIRECRAWL_API_KEY",
            "https://api.firecrawl.dev/v2/scrape",
            "Authorization",
            true,
            json!({
                "url": url,
                "formats": ["markdown"],
                "skipTlsVerification": false,
            }),
        ),
        FetchEngine::Exa => (
            "EXA_API_KEY",
            "https://api.exa.ai/contents",
            "x-api-key",
            false,
            json!({
                "urls": [url],
                "text": { "maxCharacters": ctx.max_chars },
            }),
        ),
        FetchEngine::Parallel => (
            "PARALLEL_API_KEY",
            "https://api.parallel.ai/v1/extract",
            "x-api-key",
            false,
            json!({
                "urls": [url],
                "advanced_settings": { "full_content": true },
            }),
        ),
        FetchEngine::Tavily => (
            "TAVILY_API_KEY",
            "https://api.tavily.com/extract",
            "Authorization",
            true,
            json!({
                "urls": [url],
                "extract_depth": "advanced",
                "format": "markdown",
            }),
        ),
        FetchEngine::Auto | FetchEngine::Direct => {
            return Err(EngineError {
                status: None,
                message: "invalid API fetch engine".into(),
                retriable: false,
            });
        }
    };

    let key = ctx.keys.get(key_name).cloned().ok_or_else(|| EngineError {
        status: None,
        message: format!("missing {key_name}"),
        retriable: true,
    })?;
    let auth_val = if bearer { format!("Bearer {key}") } else { key };
    let headers = [
        (header, auth_val),
        ("Content-Type", "application/json".to_string()),
        ("Accept", "application/json".to_string()),
    ];
    let data = send_http_json("POST", endpoint, &headers, Some(&body), ctx.timeout_ms)?;
    let (final_url, title, content) = parse_provider_fetch(engine, url, &data)?;
    Ok((final_url, title, "text/markdown".to_string(), content))
}

fn parse_provider_fetch(
    engine: FetchEngine,
    requested_url: &str,
    data: &JsonValue,
) -> Result<(String, Option<String>, String), EngineError> {
    match engine {
        FetchEngine::Firecrawl => {
            if data.get("success") != Some(&JsonValue::Bool(true)) {
                return Err(EngineError {
                    status: None,
                    message: "firecrawl scrape returned success=false".into(),
                    retriable: true,
                });
            }
            let page = data.get("data").ok_or_else(|| EngineError {
                status: None,
                message: "firecrawl response missing data object".into(),
                retriable: true,
            })?;
            let text = page
                .get("markdown")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| EngineError {
                    status: None,
                    message: "firecrawl response missing markdown".into(),
                    retriable: true,
                })?;
            let meta = page.get("metadata");
            let final_url = meta
                .and_then(|m| m.get("url"))
                .and_then(JsonValue::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(requested_url)
                .to_string();
            let title = meta
                .and_then(|m| m.get("title"))
                .and_then(JsonValue::as_str)
                .map(str::to_string);
            Ok((final_url, title, text.to_string()))
        }
        FetchEngine::Exa => {
            let item = data
                .get("results")
                .and_then(JsonValue::as_array)
                .and_then(|arr| arr.first())
                .ok_or_else(|| EngineError {
                    status: None,
                    message: "exa contents returned empty results".into(),
                    retriable: true,
                })?;
            let text = item
                .get("text")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| EngineError {
                    status: None,
                    message: "exa contents missing text".into(),
                    retriable: true,
                })?;
            let final_url = item
                .get("url")
                .and_then(JsonValue::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(requested_url)
                .to_string();
            let title = item
                .get("title")
                .and_then(JsonValue::as_str)
                .map(str::to_string);
            Ok((final_url, title, text.to_string()))
        }
        FetchEngine::Parallel => {
            let item = data
                .get("results")
                .and_then(JsonValue::as_array)
                .and_then(|arr| arr.first())
                .ok_or_else(|| EngineError {
                    status: None,
                    message: "parallel extract returned empty results".into(),
                    retriable: true,
                })?;
            let text = if let Some(full) = item.get("full_content").and_then(JsonValue::as_str) {
                full.to_string()
            } else if let Some(excerpts) = item.get("excerpts").and_then(JsonValue::as_array) {
                excerpts
                    .iter()
                    .filter_map(JsonValue::as_str)
                    .collect::<Vec<_>>()
                    .join("\n\n")
            } else {
                return Err(EngineError {
                    status: None,
                    message: "parallel extract missing content".into(),
                    retriable: true,
                });
            };
            let final_url = item
                .get("url")
                .and_then(JsonValue::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(requested_url)
                .to_string();
            let title = item
                .get("title")
                .and_then(JsonValue::as_str)
                .map(str::to_string);
            Ok((final_url, title, text))
        }
        FetchEngine::Tavily => {
            let item = data
                .get("results")
                .and_then(JsonValue::as_array)
                .and_then(|arr| arr.first())
                .ok_or_else(|| EngineError {
                    status: None,
                    message: "tavily extract returned empty results".into(),
                    retriable: true,
                })?;
            let text = item
                .get("raw_content")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| EngineError {
                    status: None,
                    message: "tavily extract missing raw_content".into(),
                    retriable: true,
                })?;
            let final_url = item
                .get("url")
                .and_then(JsonValue::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(requested_url)
                .to_string();
            let title = item
                .get("title")
                .and_then(JsonValue::as_str)
                .map(str::to_string);
            Ok((final_url, title, text.to_string()))
        }
        FetchEngine::Auto | FetchEngine::Direct => Err(EngineError {
            status: None,
            message: "unsupported provider fetch parser".into(),
            retriable: false,
        }),
    }
}

fn execute_direct_fetch(
    url: &str,
    req: &WebFetchRequest,
    timeout_ms: u64,
    user_agent: &str,
) -> Result<(String, Option<String>, String, String), EngineError> {
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_millis(timeout_ms)))
        .max_redirects(0)
        .http_status_as_error(false)
        .build()
        .new_agent();

    let mut current_url = url.to_string();
    for hop in 0..=MAX_REDIRECT_HOPS {
        let base_parsed = validate_public_url(&current_url).map_err(|msg| EngineError {
            status: None,
            message: format!("SSRF check blocked URL on hop {hop}: {msg}"),
            retriable: false,
        })?;
        if !url_allowed_by_domain_policy(&current_url, &req.allowed_domains, &req.blocked_domains) {
            return Err(EngineError {
                status: None,
                message: format!(
                    "redirect target `{current_url}` violates `allowed_domains` / `blocked_domains` policy"
                ),
                retriable: false,
            });
        }

        let resp = http
            .get(&current_url)
            .header("User-Agent", user_agent)
            .header(
                "Accept",
                "text/html,application/xhtml+xml,text/plain,text/markdown;q=0.9,*/*;q=0.5",
            )
            .call()
            .map_err(|err| EngineError {
                status: None,
                message: format!("direct HTTP transport error: {err}"),
                retriable: true,
            })?;

        let status = resp.status().as_u16();
        if matches!(status, 301 | 302 | 303 | 307 | 308) {
            if hop == MAX_REDIRECT_HOPS {
                return Err(EngineError {
                    status: Some(status),
                    message: format!("exceeded maximum of {MAX_REDIRECT_HOPS} HTTP redirects"),
                    retriable: false,
                });
            }
            let location = resp
                .headers()
                .get("location")
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| EngineError {
                    status: Some(status),
                    message: format!("HTTP {status} redirect missing Location header"),
                    retriable: false,
                })?;
            current_url = resolve_redirect(&base_parsed, location).map_err(|e| EngineError {
                status: Some(status),
                message: e,
                retriable: false,
            })?;
            continue;
        }

        if !(200..300).contains(&status) {
            return Err(EngineError {
                status: Some(status),
                message: format!("direct HTTP fetch returned status {status}"),
                retriable: is_retriable_status(status),
            });
        }

        let content_type = resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("text/plain")
            .to_string();

        let mut body_bytes = Vec::new();
        resp.into_body()
            .into_reader()
            .take(MAX_DIRECT_FETCH_BYTES)
            .read_to_end(&mut body_bytes)
            .map_err(|e| EngineError {
                status: Some(status),
                message: format!("failed reading response body: {e}"),
                retriable: true,
            })?;

        let raw_text = String::from_utf8_lossy(&body_bytes).into_owned();
        let (title, cleaned) = if req.raw == Some(true) {
            (None, raw_text)
        } else {
            html_to_markdown(&raw_text)
        };
        return Ok((current_url, title, content_type, cleaned));
    }

    Err(EngineError {
        status: None,
        message: "direct HTTP fetch exhausted redirect hops".to_string(),
        retriable: false,
    })
}

/// Resolve a relative or absolute redirect `Location` against `base`.
pub fn resolve_redirect(base: &Url, location: &str) -> Result<String, String> {
    base.join(location.trim())
        .map(String::from)
        .map_err(|e| format!("invalid redirect Location `{location}`: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_fetch_plan_defaults_to_direct_without_api_keys() {
        let keys = HashMap::new();
        assert_eq!(
            plan_fetch_engines(FetchEngine::Auto, &keys),
            vec![FetchEngine::Direct]
        );
    }

    #[test]
    fn resolve_redirect_handles_absolute_root_relative_and_relative_paths() {
        let base = Url::parse("https://example.com/docs/intro.html").unwrap();
        assert_eq!(
            resolve_redirect(&base, "/v2/guide").unwrap(),
            "https://example.com/v2/guide"
        );
        assert_eq!(
            resolve_redirect(&base, "next.html").unwrap(),
            "https://example.com/docs/next.html"
        );
        assert_eq!(
            resolve_redirect(&base, "https://docs.rs/rmcp").unwrap(),
            "https://docs.rs/rmcp"
        );
    }

    #[test]
    fn execute_fetch_blocks_private_ip_before_network() {
        let keys = HashMap::new();
        let err = execute_fetch(
            &keys,
            &WebFetchRequest {
                url: "http://169.254.169.254/latest/meta-data/".into(),
                ..Default::default()
            },
            5_000,
            "test-agent",
        )
        .unwrap_err();
        assert!(err.contains("SSRF guard"), "got: {err}");
    }
}
