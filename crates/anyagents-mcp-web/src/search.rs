//! Multi-provider web search engine with bounded failover and DuckDuckGo zero-key fallback.

use std::collections::HashMap;
use std::time::Instant;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use url::Url;

use crate::config::brave_api_key;
use crate::html::parse_duckduckgo_html;
use crate::http::{EngineError, send_http_json, send_http_text, urlencode_query};
use crate::ssrf::{normalize_domain, url_allowed_by_domain_policy};

const DEFAULT_SEARCH_MAX_RESULTS: u32 = 8;
const DEFAULT_SEARCH_BUDGET_CHARS: usize = 12_000;
const MAX_FAILOVER_ATTEMPTS: usize = 4;
const MAX_DDG_HTML_BYTES: u64 = 1_048_576;

/// Parameters for `web_search`.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct WebSearchRequest {
    /// Search query string.
    #[serde(alias = "q")]
    pub query: String,
    /// Optional search engine: `"auto"` (default cascade across configured keys with DuckDuckGo fallback),
    /// or explicit `"brave" | "exa" | "tavily" | "parallel" | "firecrawl" | "perplexity" | "duckduckgo"`.
    #[serde(default)]
    pub engine: Option<String>,
    /// Optional result count cap (`1..=20`, default `8`).
    #[serde(default, alias = "limit", alias = "count")]
    pub max_results: Option<u32>,
    /// Optional search mode preset (`"auto"`, `"fast"`, `"deep"`, `"neural"`, `"keyword"`, `"answer"`).
    #[serde(default)]
    pub mode: Option<String>,
    /// Optional per-result snippet character budget.
    #[serde(default, alias = "max_characters")]
    pub max_chars: Option<usize>,
    /// Optional domain allowlist (e.g. `["rust-lang.org", "docs.rs"]`).
    #[serde(default, alias = "include_domains")]
    pub allowed_domains: Vec<String>,
    /// Optional domain blocklist (e.g. `["pinterest.com"]`).
    #[serde(default, alias = "exclude_domains")]
    pub blocked_domains: Vec<String>,
    /// Optional 2-letter ISO-3166-1 alpha-2 country code (e.g. `"US"`, `"GB"`).
    #[serde(default)]
    pub country: Option<String>,
    /// Optional request timeout in milliseconds (`1000..=60000`).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// Supported search providers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SearchEngine {
    Auto,
    Brave,
    Exa,
    Tavily,
    Parallel,
    Firecrawl,
    Perplexity,
    DuckDuckGo,
}

impl SearchEngine {
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        let cleaned = raw.unwrap_or("auto").trim().to_ascii_lowercase();
        match cleaned.as_str() {
            "" | "auto" => Ok(Self::Auto),
            "brave" => Ok(Self::Brave),
            "exa" => Ok(Self::Exa),
            "tavily" => Ok(Self::Tavily),
            "parallel" => Ok(Self::Parallel),
            "firecrawl" | "fc" => Ok(Self::Firecrawl),
            "perplexity" | "pplx" => Ok(Self::Perplexity),
            "duckduckgo" | "ddg" => Ok(Self::DuckDuckGo),
            other => Err(format!(
                "unknown web search engine `{other}`; expected `auto`, `brave`, `exa`, `tavily`, \
                 `parallel`, `firecrawl`, `perplexity`, or `duckduckgo`"
            )),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Brave => "brave",
            Self::Exa => "exa",
            Self::Tavily => "tavily",
            Self::Parallel => "parallel",
            Self::Firecrawl => "firecrawl",
            Self::Perplexity => "perplexity",
            Self::DuckDuckGo => "duckduckgo",
        }
    }

    /// Provider cascade ordering for `auto` mode.
    pub fn cascade_for_mode(self, mode: Option<&str>) -> Vec<Self> {
        match self {
            Self::Auto => match mode.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
                Some("fast") => vec![
                    Self::Brave,
                    Self::Tavily,
                    Self::Exa,
                    Self::Parallel,
                    Self::Firecrawl,
                    Self::Perplexity,
                    Self::DuckDuckGo,
                ],
                Some("answer") => vec![
                    Self::Perplexity,
                    Self::Tavily,
                    Self::Parallel,
                    Self::Exa,
                    Self::Brave,
                    Self::DuckDuckGo,
                ],
                _ => vec![
                    Self::Brave,
                    Self::Exa,
                    Self::Tavily,
                    Self::Parallel,
                    Self::Firecrawl,
                    Self::Perplexity,
                    Self::DuckDuckGo,
                ],
            },
            named => vec![named],
        }
    }
}

fn engine_has_credentials(engine: SearchEngine, keys: &HashMap<String, String>) -> bool {
    match engine {
        SearchEngine::Brave => brave_api_key(keys).is_some(),
        SearchEngine::Exa => keys.contains_key("EXA_API_KEY"),
        SearchEngine::Tavily => keys.contains_key("TAVILY_API_KEY"),
        SearchEngine::Parallel => keys.contains_key("PARALLEL_API_KEY"),
        SearchEngine::Firecrawl => keys.contains_key("FIRECRAWL_API_KEY"),
        SearchEngine::Perplexity => keys.contains_key("PERPLEXITY_API_KEY"),
        SearchEngine::DuckDuckGo => true,
        SearchEngine::Auto => false,
    }
}

/// Build the provider execution plan for a request given available `keys`.
pub fn plan_search_engines(
    selected: SearchEngine,
    mode: Option<&str>,
    keys: &HashMap<String, String>,
) -> Vec<SearchEngine> {
    let candidates = selected.cascade_for_mode(mode);
    let mut plan: Vec<SearchEngine> = candidates
        .iter()
        .copied()
        .filter(|e| *e != SearchEngine::DuckDuckGo && engine_has_credentials(*e, keys))
        .take(MAX_FAILOVER_ATTEMPTS)
        .collect();
    if selected == SearchEngine::DuckDuckGo
        || (selected == SearchEngine::Auto && !plan.contains(&SearchEngine::DuckDuckGo))
    {
        plan.push(SearchEngine::DuckDuckGo);
    }
    plan
}

/// Execute a web search request synchronously.
pub fn execute_search(
    keys: &HashMap<String, String>,
    req: &WebSearchRequest,
    default_timeout_ms: u64,
    user_agent: &str,
) -> Result<JsonValue, String> {
    execute_search_with_endpoints(keys, req, default_timeout_ms, user_agent, None)
}

fn execute_search_with_endpoints(
    keys: &HashMap<String, String>,
    req: &WebSearchRequest,
    default_timeout_ms: u64,
    user_agent: &str,
    endpoints: Option<&HashMap<SearchEngine, String>>,
) -> Result<JsonValue, String> {
    let query = req.query.trim();
    if query.is_empty() {
        return Err("`web_search` requires a non-empty `query`".to_string());
    }
    let selected = SearchEngine::parse(req.engine.as_deref())?;
    let plan = plan_search_engines(selected, req.mode.as_deref(), keys);
    if plan.is_empty() {
        return Err(format!(
            "no API key configured for requested search engine `{}`",
            selected.as_str()
        ));
    }

    let timeout_ms = req
        .timeout_ms
        .unwrap_or(default_timeout_ms)
        .clamp(1_000, 60_000);
    let max_res = req
        .max_results
        .unwrap_or(DEFAULT_SEARCH_MAX_RESULTS)
        .clamp(1, 20) as usize;

    let mut attempts = Vec::new();
    let mut last_err = String::new();

    for engine in plan {
        let started = Instant::now();
        match execute_single_engine(engine, query, req, keys, timeout_ms, user_agent, endpoints) {
            Ok(mut hits) => {
                let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(0);
                hits.retain(|h| {
                    h.get("url").and_then(JsonValue::as_str).is_some_and(|u| {
                        url_allowed_by_domain_policy(u, &req.allowed_domains, &req.blocked_domains)
                    })
                });
                if hits.len() > max_res {
                    hits.truncate(max_res);
                }
                let fallback_used = !attempts.is_empty();
                attempts.push(json!({
                    "engine": engine.as_str(),
                    "outcome": "succeeded",
                    "duration_ms": duration_ms,
                }));
                let count = hits.len();
                return Ok(json!({
                    "action": "search",
                    "engine": engine.as_str(),
                    "fallback_used": fallback_used,
                    "query": query,
                    "count": count,
                    "results": hits,
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
                if !err.retriable && selected != SearchEngine::Auto {
                    break;
                }
            }
        }
    }

    Err(format!("web search failed ({last_err})"))
}

fn execute_single_engine(
    engine: SearchEngine,
    query: &str,
    req: &WebSearchRequest,
    keys: &HashMap<String, String>,
    timeout_ms: u64,
    user_agent: &str,
    endpoints: Option<&HashMap<SearchEngine, String>>,
) -> Result<Vec<JsonValue>, EngineError> {
    if engine == SearchEngine::DuckDuckGo {
        return execute_duckduckgo_search(query, req, timeout_ms, user_agent);
    }

    let payload = build_search_payload(engine, query, req);
    let (key_opt, default_endpoint, header, bearer) = match engine {
        SearchEngine::Brave => (
            brave_api_key(keys).map(str::to_owned),
            "https://api.search.brave.com/res/v1/web/search",
            "X-Subscription-Token",
            false,
        ),
        SearchEngine::Exa => (
            keys.get("EXA_API_KEY").cloned(),
            "https://api.exa.ai/search",
            "x-api-key",
            false,
        ),
        SearchEngine::Tavily => (
            keys.get("TAVILY_API_KEY").cloned(),
            "https://api.tavily.com/search",
            "Authorization",
            true,
        ),
        SearchEngine::Parallel => (
            keys.get("PARALLEL_API_KEY").cloned(),
            "https://api.parallel.ai/v1/search",
            "x-api-key",
            false,
        ),
        SearchEngine::Firecrawl => (
            keys.get("FIRECRAWL_API_KEY").cloned(),
            "https://api.firecrawl.dev/v2/search",
            "Authorization",
            true,
        ),
        SearchEngine::Perplexity => (
            keys.get("PERPLEXITY_API_KEY").cloned(),
            "https://api.perplexity.ai/search",
            "Authorization",
            true,
        ),
        SearchEngine::Auto | SearchEngine::DuckDuckGo => {
            return Err(EngineError {
                status: None,
                message: "invalid API search engine".into(),
                retriable: false,
            });
        }
    };

    let endpoint = endpoints
        .and_then(|map| map.get(&engine))
        .map(String::as_str)
        .unwrap_or(default_endpoint);

    let key = key_opt.ok_or_else(|| EngineError {
        status: None,
        message: format!("missing API key for {}", engine.as_str()),
        retriable: true,
    })?;

    let auth_val = if bearer { format!("Bearer {key}") } else { key };
    let mut headers = vec![
        (header, auth_val),
        ("Accept", "application/json".to_string()),
    ];

    let parsed = if engine == SearchEngine::Brave {
        let count = req
            .max_results
            .unwrap_or(DEFAULT_SEARCH_MAX_RESULTS)
            .clamp(1, 20);
        let url = format!(
            "{endpoint}?q={}&count={count}&extra_snippets=true",
            urlencode_query(query)
        );
        if let Some(c) = req
            .country
            .as_deref()
            .map(str::trim)
            .filter(|s| s.len() == 2 && s.bytes().all(|b| b.is_ascii_alphabetic()))
        {
            headers.push(("X-Loc-Country", c.to_ascii_uppercase()));
        }
        send_http_json("GET", &url, &headers, None, timeout_ms)?
    } else {
        headers.push(("Content-Type", "application/json".to_string()));
        send_http_json("POST", endpoint, &headers, Some(&payload), timeout_ms)?
    };

    parse_provider_hits(engine, &parsed)
}

fn execute_duckduckgo_search(
    query: &str,
    req: &WebSearchRequest,
    timeout_ms: u64,
    user_agent: &str,
) -> Result<Vec<JsonValue>, EngineError> {
    let mut effective_query = query.to_string();
    let allowed: Vec<String> = req
        .allowed_domains
        .iter()
        .map(|d| normalize_domain(d))
        .filter(|d| !d.is_empty())
        .take(3)
        .collect();
    if !allowed.is_empty() {
        let clauses = allowed
            .iter()
            .map(|d| format!("site:{d}"))
            .collect::<Vec<_>>()
            .join(" OR ");
        effective_query = format!("{effective_query} ({clauses})");
    }

    let url = format!(
        "https://html.duckduckgo.com/html/?q={}",
        urlencode_query(&effective_query)
    );
    let headers = [
        ("User-Agent", user_agent),
        ("Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8"),
        ("Accept-Language", "en-US,en;q=0.9"),
    ];
    let (_, html) = send_http_text(&url, &headers, timeout_ms, MAX_DDG_HTML_BYTES)?;
    let parsed = parse_duckduckgo_html(&html);
    let hits: Vec<JsonValue> = parsed
        .into_iter()
        .filter_map(|h| make_hit(Some(&h.url), h.title.as_deref(), h.snippet.as_deref()))
        .collect();
    Ok(hits)
}

fn build_search_payload(engine: SearchEngine, query: &str, req: &WebSearchRequest) -> JsonValue {
    let max_results = req
        .max_results
        .unwrap_or(DEFAULT_SEARCH_MAX_RESULTS)
        .clamp(1, 20);
    let per_result_cap = req
        .max_chars
        .unwrap_or_else(|| DEFAULT_SEARCH_BUDGET_CHARS / (max_results.max(1) as usize));
    let allowed: Vec<String> = req
        .allowed_domains
        .iter()
        .map(|d| normalize_domain(d))
        .filter(|d| !d.is_empty())
        .collect();
    let blocked: Vec<String> = req
        .blocked_domains
        .iter()
        .map(|d| normalize_domain(d))
        .filter(|d| !d.is_empty())
        .collect();
    let country = req
        .country
        .as_deref()
        .map(str::trim)
        .filter(|s| s.len() == 2 && s.bytes().all(|b| b.is_ascii_alphabetic()))
        .map(str::to_ascii_uppercase);

    let mut payload = match engine {
        SearchEngine::Exa => json!({
            "query": query,
            "numResults": max_results,
            "contents": {
                "text": { "maxCharacters": per_result_cap }
            }
        }),
        SearchEngine::Tavily => json!({
            "query": query,
            "max_results": max_results,
            "include_answer": false,
        }),
        SearchEngine::Parallel => {
            let mode = req
                .mode
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|m| {
                    let l = m.to_ascii_lowercase();
                    if l == "auto" { "fast".to_string() } else { l }
                })
                .unwrap_or_else(|| "fast".to_string());
            json!({
                "mode": mode,
                "search_queries": [query],
                "max_chars_total": DEFAULT_SEARCH_BUDGET_CHARS,
            })
        }
        SearchEngine::Firecrawl => json!({
            "query": query,
            "limit": max_results,
        }),
        SearchEngine::Perplexity => json!({
            "query": query,
            "max_results": max_results,
            "search_context_size": "medium",
        }),
        SearchEngine::Brave | SearchEngine::DuckDuckGo | SearchEngine::Auto => json!({}),
    };

    if engine == SearchEngine::Exa
        && let Some(m) = req.mode.as_deref().map(str::trim).filter(|s| !s.is_empty())
    {
        payload["type"] = json!(m.to_ascii_lowercase());
    }
    if engine == SearchEngine::Parallel {
        if req.max_results.is_some() {
            payload["advanced_settings"]["max_results"] = json!(max_results);
        }
        if req.max_chars.is_some() {
            payload["advanced_settings"]["excerpt_settings"]["max_chars_per_result"] =
                json!(per_result_cap);
        }
    }
    if !allowed.is_empty() {
        match engine {
            SearchEngine::Exa | SearchEngine::Firecrawl => {
                payload["includeDomains"] = json!(allowed);
            }
            SearchEngine::Tavily => {
                payload["include_domains"] = json!(allowed);
            }
            SearchEngine::Parallel => {
                payload["advanced_settings"]["source_policy"]["include_domains"] = json!(allowed);
            }
            SearchEngine::Perplexity => {
                payload["search_domain_filter"] = json!(allowed);
            }
            _ => {}
        }
    }
    if !blocked.is_empty() {
        match engine {
            SearchEngine::Exa | SearchEngine::Firecrawl => {
                payload["excludeDomains"] = json!(blocked);
            }
            SearchEngine::Tavily => {
                payload["exclude_domains"] = json!(blocked);
            }
            SearchEngine::Parallel => {
                payload["advanced_settings"]["source_policy"]["exclude_domains"] = json!(blocked);
            }
            SearchEngine::Perplexity if allowed.is_empty() => {
                let neg: Vec<String> = blocked.iter().map(|d| format!("-{d}")).collect();
                payload["search_domain_filter"] = json!(neg);
            }
            _ => {}
        }
    }
    if let Some(c) = country {
        match engine {
            SearchEngine::Exa => payload["userLocation"] = json!(c),
            SearchEngine::Perplexity => payload["country"] = json!(c),
            _ => {}
        }
    }
    payload
}

fn parse_provider_hits(
    engine: SearchEngine,
    data: &JsonValue,
) -> Result<Vec<JsonValue>, EngineError> {
    if data.get("success") == Some(&JsonValue::Bool(false))
        || data.get("error").is_some_and(|v| !v.is_null() && v != "")
    {
        return Err(EngineError {
            status: None,
            message: "provider returned error in response body".into(),
            retriable: true,
        });
    }

    let mut out = Vec::new();
    match engine {
        SearchEngine::Brave => {
            if let Some(arr) = data.pointer("/web/results").and_then(JsonValue::as_array) {
                for item in arr {
                    let mut snippets: Vec<&str> = item
                        .get("description")
                        .and_then(JsonValue::as_str)
                        .into_iter()
                        .collect();
                    if let Some(extras) = item.get("extra_snippets").and_then(JsonValue::as_array) {
                        for extra in extras.iter().filter_map(JsonValue::as_str) {
                            if !snippets.contains(&extra) {
                                snippets.push(extra);
                            }
                        }
                    }
                    let joined = snippets.join("\n\n");
                    if let Some(h) = make_hit(
                        item.get("url").and_then(JsonValue::as_str),
                        item.get("title").and_then(JsonValue::as_str),
                        Some(&joined),
                    ) {
                        out.push(h);
                    }
                }
            }
        }
        SearchEngine::Exa => {
            if let Some(arr) = data.get("results").and_then(JsonValue::as_array) {
                for item in arr {
                    let highlights =
                        item.get("highlights")
                            .and_then(JsonValue::as_array)
                            .map(|items| {
                                items
                                    .iter()
                                    .filter_map(JsonValue::as_str)
                                    .collect::<Vec<_>>()
                                    .join("\n\n")
                            });
                    let snippet = item
                        .get("text")
                        .and_then(JsonValue::as_str)
                        .or(highlights.as_deref());
                    if let Some(h) = make_hit(
                        item.get("url").and_then(JsonValue::as_str),
                        item.get("title").and_then(JsonValue::as_str),
                        snippet,
                    ) {
                        out.push(h);
                    }
                }
            }
        }
        SearchEngine::Tavily => {
            if let Some(arr) = data.get("results").and_then(JsonValue::as_array) {
                for item in arr {
                    if let Some(h) = make_hit(
                        item.get("url").and_then(JsonValue::as_str),
                        item.get("title").and_then(JsonValue::as_str),
                        item.get("content").and_then(JsonValue::as_str),
                    ) {
                        out.push(h);
                    }
                }
            }
        }
        SearchEngine::Parallel => {
            if let Some(arr) = data.get("results").and_then(JsonValue::as_array) {
                for item in arr {
                    let snippet = item
                        .get("excerpts")
                        .and_then(JsonValue::as_array)
                        .map(|ex| {
                            ex.iter()
                                .filter_map(JsonValue::as_str)
                                .collect::<Vec<_>>()
                                .join("\n\n")
                        })
                        .or_else(|| {
                            item.get("excerpt")
                                .and_then(JsonValue::as_str)
                                .map(str::to_string)
                        });
                    if let Some(h) = make_hit(
                        item.get("url").and_then(JsonValue::as_str),
                        item.get("title").and_then(JsonValue::as_str),
                        snippet.as_deref(),
                    ) {
                        out.push(h);
                    }
                }
            }
        }
        SearchEngine::Firecrawl => {
            if let Some(arr) = data
                .pointer("/data/web")
                .or_else(|| data.get("data"))
                .and_then(JsonValue::as_array)
            {
                for item in arr {
                    let snippet = item
                        .get("description")
                        .or_else(|| item.get("markdown"))
                        .and_then(JsonValue::as_str);
                    if let Some(h) = make_hit(
                        item.get("url").and_then(JsonValue::as_str),
                        item.get("title").and_then(JsonValue::as_str),
                        snippet,
                    ) {
                        out.push(h);
                    }
                }
            }
        }
        SearchEngine::Perplexity => {
            if let Some(arr) = data.get("results").and_then(JsonValue::as_array) {
                for item in arr {
                    if let Some(h) = make_hit(
                        item.get("url").and_then(JsonValue::as_str),
                        item.get("title").and_then(JsonValue::as_str),
                        item.get("snippet").and_then(JsonValue::as_str),
                    ) {
                        out.push(h);
                    }
                }
            }
        }
        SearchEngine::Auto | SearchEngine::DuckDuckGo => {}
    }
    Ok(out)
}

fn make_hit(url: Option<&str>, title: Option<&str>, snippet: Option<&str>) -> Option<JsonValue> {
    let u = url?.trim();
    let parsed = Url::parse(u).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }
    Some(json!({
        "url": u,
        "title": title.map(str::trim).filter(|s| !s.is_empty()),
        "snippet": snippet.map(str::trim).filter(|s| !s.is_empty()),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_search_plan_falls_back_to_duckduckgo_with_zero_api_keys() {
        let keys = HashMap::new();
        let plan = plan_search_engines(SearchEngine::Auto, None, &keys);
        assert_eq!(plan, vec![SearchEngine::DuckDuckGo]);
    }

    #[test]
    fn auto_search_plan_prefers_configured_providers_before_duckduckgo() {
        let mut keys = HashMap::new();
        keys.insert("EXA_API_KEY".into(), "exa_1".into());
        keys.insert("BRAVE_API_KEY".into(), "brave_1".into());
        let plan = plan_search_engines(SearchEngine::Auto, None, &keys);
        assert_eq!(
            plan,
            vec![
                SearchEngine::Brave,
                SearchEngine::Exa,
                SearchEngine::DuckDuckGo
            ]
        );
    }

    #[test]
    fn parses_brave_and_exa_provider_payloads() {
        let brave = json!({
            "web": {
                "results": [{
                    "url": "https://docs.rs/rmcp",
                    "title": "rmcp",
                    "description": "Rust MCP SDK",
                    "extra_snippets": ["Full stdio support"]
                }]
            }
        });
        let hits = parse_provider_hits(SearchEngine::Brave, &brave).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0]["url"], "https://docs.rs/rmcp");
        assert!(
            hits[0]["snippet"]
                .as_str()
                .unwrap()
                .contains("Full stdio support")
        );

        let exa = json!({
            "results": [{
                "url": "https://www.rust-lang.org/",
                "title": "Rust",
                "text": "Systems programming language"
            }]
        });
        let exa_hits = parse_provider_hits(SearchEngine::Exa, &exa).unwrap();
        assert_eq!(exa_hits.len(), 1);
        assert_eq!(exa_hits[0]["title"], "Rust");
    }

    fn spawn_mock_json_server(status_line: &'static str, body: &'static str) -> String {
        use std::io::{BufRead as _, BufReader, Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            if let Ok((stream, _)) = listener.accept() {
                let mut reader = BufReader::new(stream);
                let mut content_len = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0
                        || line == "\r\n"
                        || line == "\n"
                    {
                        break;
                    }
                    if let Some((k, v)) = line.split_once(':')
                        && k.trim().eq_ignore_ascii_case("content-length")
                    {
                        content_len = v.trim().parse().unwrap_or(0);
                    }
                }
                if content_len > 0 {
                    let mut req_body = vec![0u8; content_len];
                    let _ = reader.read_exact(&mut req_body);
                }
                let mut stream = reader.into_inner();
                let resp = format!(
                    "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(resp.as_bytes());
                let _ = stream.flush();
                let _ = stream.shutdown(std::net::Shutdown::Write);
            }
        });
        format!("http://{addr}/search")
    }

    #[test]
    fn auto_search_fails_over_from_parallel_502_to_exa() {
        let parallel_url = spawn_mock_json_server("502 Bad Gateway", r#"{"error":"upstream 502"}"#);
        let exa_url = spawn_mock_json_server(
            "200 OK",
            r#"{"results":[{"url":"https://docs.rs/anyagents","title":"AnyAgents","text":"OSS coordinator"}]}"#,
        );
        let mut keys = HashMap::new();
        keys.insert("PARALLEL_API_KEY".into(), "par_key".into());
        keys.insert("EXA_API_KEY".into(), "exa_key".into());
        let mut endpoints = HashMap::new();
        endpoints.insert(SearchEngine::Parallel, parallel_url);
        endpoints.insert(SearchEngine::Exa, exa_url);

        // `answer` cascade orders Parallel before Exa when Perplexity & Tavily are unset.
        let req = WebSearchRequest {
            query: "anyagents".into(),
            engine: Some("auto".into()),
            mode: Some("answer".into()),
            ..Default::default()
        };
        let out =
            execute_search_with_endpoints(&keys, &req, 5_000, "test-ua", Some(&endpoints)).unwrap();
        assert_eq!(out["engine"], "exa");
        assert_eq!(out["fallback_used"], true);
        assert_eq!(out["count"], 1);
        assert_eq!(out["attempts"][0]["engine"], "parallel");
        assert_eq!(out["attempts"][0]["outcome"], "failed");
        assert_eq!(out["attempts"][0]["http_status"], 502);
        assert_eq!(out["attempts"][1]["engine"], "exa");
        assert_eq!(out["attempts"][1]["outcome"], "succeeded");
    }

    #[test]
    fn auto_search_fails_over_from_exa_502_to_tavily() {
        let exa_url = spawn_mock_json_server("502 Bad Gateway", r#"{"error":"bad gateway"}"#);
        let tavily_url = spawn_mock_json_server(
            "200 OK",
            r#"{"results":[{"url":"https://rust-lang.org/","title":"Rust","content":"Systems language"}]}"#,
        );
        let mut keys = HashMap::new();
        keys.insert("EXA_API_KEY".into(), "exa_key".into());
        keys.insert("TAVILY_API_KEY".into(), "tav_key".into());
        let mut endpoints = HashMap::new();
        endpoints.insert(SearchEngine::Exa, exa_url);
        endpoints.insert(SearchEngine::Tavily, tavily_url);

        let req = WebSearchRequest {
            query: "rust".into(),
            engine: Some("auto".into()),
            ..Default::default()
        };
        let out =
            execute_search_with_endpoints(&keys, &req, 5_000, "test-ua", Some(&endpoints)).unwrap();
        assert_eq!(out["engine"], "tavily");
        assert_eq!(out["fallback_used"], true);
        assert_eq!(out["attempts"][0]["http_status"], 502);
        assert_eq!(out["attempts"][1]["engine"], "tavily");
        assert_eq!(out["attempts"][1]["outcome"], "succeeded");
    }
}
