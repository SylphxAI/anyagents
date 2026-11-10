//! Multi-provider policy-aware Web Search and Web Fetch engine for AnyAgents Code Mode (`tools.web`).
//!
//! Adheres strictly to Mechanism vs. Policy:
//! - Provider API keys (`PARALLEL_API_KEY`, `EXA_API_KEY`, `FIRECRAWL_API_KEY`,
//!   `PERPLEXITY_API_KEY`, `TAVILY_API_KEY`, `BRAVE_SEARCH_API_KEY`,
//!   `ANYSEARCH_API_KEY`, `AA_API_KEY`) are resolved from the server process
//!   environment or `<data_dir>/.web-keys.env` (`0600`) and never printed or
//!   exposed to agent sandboxes.
//! - `auto` mode filters candidate engines by policy capability (`allowed_domains`,
//!   `blocked_domains`, `country`, `max_results`, `mode`, `external_web_access`)
//!   and available credentials, with bounded failover (up to 3 attempts) on
//!   retriable upstream statuses (`402`, `408`, `429`, `5xx`).

use std::collections::HashMap;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::Path;
use std::time::{Duration, Instant};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

const DEFAULT_SEARCH_MAX_RESULTS: u32 = 8;
const DEFAULT_SEARCH_BUDGET_CHARS: usize = 12_000;
const DEFAULT_FETCH_MAX_CHARS: usize = 12_000;
const HARD_FETCH_MAX_CHARS: usize = 65_536;
const DEFAULT_ENGINE_TIMEOUT_MS: u64 = 15_000;
const MAX_FAILOVER_ATTEMPTS: usize = 3;

const ANYSEARCH_MCP_ENDPOINT: &str = "https://api.anysearch.com/mcp";
const ANYSEARCH_EXTRACT_ENDPOINT: &str = "https://api.anysearch.com/v1/extract";
const ANYSEARCH_CLIENT_HEADER: &str = "ai-gateway/1.0.0";

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct WebRequest {
    /// Operation: `"search"` (default when `query` is set) or `"fetch"` (default when `url` is set).
    #[serde(default)]
    pub action: Option<String>,
    /// Search query string when `action = "search"`.
    #[serde(default, alias = "q")]
    pub query: Option<String>,
    /// Target `http://` or `https://` URL when `action = "fetch"`.
    #[serde(default, alias = "href", alias = "target")]
    pub url: Option<String>,
    /// Engine selection:
    /// - For `search`: `"auto"` (default cascade: `parallel -> exa -> tavily -> firecrawl -> perplexity -> brave -> anysearch`)
    ///   or strict named engine (`"parallel" | "exa" | "tavily" | "firecrawl" | "perplexity" | "brave" | "anysearch"`).
    /// - For `fetch`: `"auto"` (default cascade: `firecrawl -> exa -> parallel -> tavily -> anysearch -> direct`)
    ///   or strict named engine (`"firecrawl" | "exa" | "parallel" | "tavily" | "anysearch" | "direct"`).
    #[serde(default)]
    pub engine: Option<String>,
    /// Optional domain allowlist (e.g. `["rust-lang.org", "docs.rs"]`). Preserved both at the provider request boundary and via post-filtering.
    #[serde(
        default,
        alias = "include_domains",
        deserialize_with = "crate::api::deserialize_string_or_vec"
    )]
    pub allowed_domains: Vec<String>,
    /// Optional domain blocklist (e.g. `["pinterest.com"]`).
    #[serde(
        default,
        alias = "exclude_domains",
        deserialize_with = "crate::api::deserialize_string_or_vec"
    )]
    pub blocked_domains: Vec<String>,
    /// Optional 2-letter ISO-3166-1 alpha-2 country code (e.g. `"US"`, `"HK"`, `"GB"`).
    #[serde(default)]
    pub country: Option<String>,
    /// Optional result count cap for `search` (`1..=20`, default `8`).
    #[serde(default, alias = "limit", alias = "count")]
    pub max_results: Option<u32>,
    /// Optional search depth/mode preset (`"fast"`, `"auto"`, `"deep"`, `"neural"`, `"keyword"`, `"one-shot"`, `"agentic"`).
    #[serde(default)]
    pub mode: Option<String>,
    /// Optional character budget for `fetch` or per-result `search` snippets (default `12000`, max `65536`).
    #[serde(default, alias = "max_characters")]
    pub max_chars: Option<usize>,
    /// Whether live external web access is permitted (`true` by default; `false` restricts to cache-only providers such as `exa` with `maxAgeHours: -1` or `firecrawl` with `lockdown: true`).
    #[serde(default)]
    pub external_web_access: Option<bool>,
    /// Optional timeout in milliseconds (default `20000`, max `60000`).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchEngine {
    Auto,
    Parallel,
    Exa,
    Tavily,
    Firecrawl,
    Perplexity,
    Brave,
    AnySearch,
}

impl SearchEngine {
    pub fn parse(raw: Option<&str>) -> Result<Self, String> {
        let cleaned = raw.unwrap_or("auto").trim().to_ascii_lowercase();
        match cleaned.as_str() {
            "" | "auto" => Ok(Self::Auto),
            "parallel" => Ok(Self::Parallel),
            "exa" => Ok(Self::Exa),
            "tavily" => Ok(Self::Tavily),
            "firecrawl" | "fc" => Ok(Self::Firecrawl),
            "perplexity" | "pplx" => Ok(Self::Perplexity),
            "brave" => Ok(Self::Brave),
            "anysearch" | "any" => Ok(Self::AnySearch),
            other => Err(format!(
                "unknown web search engine `{other}`; expected `auto`, `parallel`, `exa`, `tavily`, `firecrawl`, `perplexity`, `brave`, or `anysearch`"
            )),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Parallel => "parallel",
            Self::Exa => "exa",
            Self::Tavily => "tavily",
            Self::Firecrawl => "firecrawl",
            Self::Perplexity => "perplexity",
            Self::Brave => "brave",
            Self::AnySearch => "anysearch",
        }
    }

    pub fn cascade_for_mode(self, mode: Option<&str>) -> Vec<Self> {
        match self {
            Self::Auto => match mode.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
                Some("fast") => vec![
                    Self::Brave,
                    Self::Tavily,
                    Self::Exa,
                    Self::Parallel,
                    Self::Perplexity,
                    Self::Firecrawl,
                    Self::AnySearch,
                ],
                Some("answer") => vec![Self::Perplexity, Self::Tavily, Self::Parallel, Self::Exa],
                _ => vec![
                    Self::Parallel,
                    Self::Exa,
                    Self::Tavily,
                    Self::Firecrawl,
                    Self::Perplexity,
                    Self::Brave,
                    Self::AnySearch,
                ],
            },
            named => vec![named],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchEngine {
    Auto,
    Firecrawl,
    Exa,
    Parallel,
    Tavily,
    AnySearch,
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
            "anysearch" | "any" => Ok(Self::AnySearch),
            "direct" | "curl" | "http" => Ok(Self::Direct),
            other => Err(format!(
                "unknown web fetch engine `{other}`; expected `auto`, `firecrawl`, `exa`, `parallel`, `tavily`, `anysearch`, or `direct`"
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
            Self::AnySearch => "anysearch",
            Self::Direct => "direct",
        }
    }

    pub fn cascade(self) -> Vec<Self> {
        match self {
            Self::Auto => vec![
                Self::Firecrawl,
                Self::Exa,
                Self::Parallel,
                Self::Tavily,
                Self::AnySearch,
                Self::Direct,
            ],
            named => vec![named],
        }
    }
}

pub fn load_web_keys(data_dir: Option<&Path>) -> HashMap<String, String> {
    let mut keys = HashMap::new();
    let mut env_files = Vec::new();
    if let Some(dir) = data_dir {
        env_files.push(dir.join(".web-keys.env"));
    }
    if let Some(env_data) = std::env::var_os("ANYAGENTS_DATA") {
        env_files.push(std::path::PathBuf::from(env_data).join(".web-keys.env"));
    }
    for path in env_files {
        if let Ok(text) = std::fs::read_to_string(&path) {
            parse_dotenv_into(&text, &mut keys);
        }
    }
    for name in [
        "SYLPHX_API_KEY",
        "SYLPHX_PLATFORM_KEY",
        "SYLPHX_BASE_URL",
        "SYLPHX_AI_BASE_URL",
        "PARALLEL_API_KEY",
        "EXA_API_KEY",
        "FIRECRAWL_API_KEY",
        "PERPLEXITY_API_KEY",
        "TAVILY_API_KEY",
        "BRAVE_SEARCH_API_KEY",
        "ANYSEARCH_API_KEY",
        "AA_API_KEY",
    ] {
        if let Ok(val) = std::env::var(name) {
            let trimmed = val.trim();
            if !trimmed.is_empty() {
                keys.insert(name.to_string(), trimmed.to_string());
            }
        }
    }
    keys
}

pub fn sylphx_baas_config(keys: &HashMap<String, String>) -> Option<(String, String)> {
    let api_key = keys
        .get("SYLPHX_API_KEY")
        .or_else(|| keys.get("SYLPHX_PLATFORM_KEY"))
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())?
        .to_string();
    let raw_base = keys
        .get("SYLPHX_BASE_URL")
        .or_else(|| keys.get("SYLPHX_AI_BASE_URL"))
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .unwrap_or("https://api.sylphx.com");
    let trimmed = raw_base.trim_end_matches('/');
    let normalized = trimmed
        .strip_suffix("/v1")
        .unwrap_or(trimmed)
        .trim_end_matches('/')
        .to_string();
    if normalized.is_empty() {
        return None;
    }
    Some((normalized, api_key))
}

pub fn parse_dotenv_into(text: &str, out: &mut HashMap<String, String>) {
    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let stripped = line.strip_prefix("export ").unwrap_or(line).trim();
        let Some((k, v)) = stripped.split_once('=') else {
            continue;
        };
        let key = k.trim();
        let mut val = v.trim();
        if (val.starts_with('"') && val.ends_with('"') && val.len() >= 2)
            || (val.starts_with('\'') && val.ends_with('\'') && val.len() >= 2)
        {
            val = &val[1..val.len() - 1];
        }
        if !key.is_empty() && !val.trim().is_empty() {
            out.entry(key.to_string())
                .or_insert_with(|| val.trim().to_string());
        }
    }
}

fn search_engine_has_key(engine: SearchEngine, keys: &HashMap<String, String>) -> bool {
    match engine {
        SearchEngine::Parallel => keys.contains_key("PARALLEL_API_KEY"),
        SearchEngine::Exa => keys.contains_key("EXA_API_KEY"),
        SearchEngine::Tavily => keys.contains_key("TAVILY_API_KEY"),
        SearchEngine::Firecrawl => keys.contains_key("FIRECRAWL_API_KEY"),
        SearchEngine::Perplexity => keys.contains_key("PERPLEXITY_API_KEY"),
        SearchEngine::Brave => keys.contains_key("BRAVE_SEARCH_API_KEY"),
        SearchEngine::AnySearch => {
            keys.contains_key("ANYSEARCH_API_KEY") || keys.contains_key("AA_API_KEY")
        }
        SearchEngine::Auto => false,
    }
}

fn fetch_engine_has_key(engine: FetchEngine, keys: &HashMap<String, String>) -> bool {
    match engine {
        FetchEngine::Firecrawl => keys.contains_key("FIRECRAWL_API_KEY"),
        FetchEngine::Exa => keys.contains_key("EXA_API_KEY"),
        FetchEngine::Parallel => keys.contains_key("PARALLEL_API_KEY"),
        FetchEngine::Tavily => keys.contains_key("TAVILY_API_KEY"),
        FetchEngine::AnySearch => {
            keys.contains_key("ANYSEARCH_API_KEY") || keys.contains_key("AA_API_KEY")
        }
        FetchEngine::Direct => true,
        FetchEngine::Auto => false,
    }
}

pub fn engine_supports_search_policy(engine: SearchEngine, req: &WebRequest) -> bool {
    let external_access = req.external_web_access.unwrap_or(true);
    if !external_access && engine != SearchEngine::Exa {
        return false;
    }
    if let Some(mode) = req.mode.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        let lower = mode.to_ascii_lowercase();
        let supported = match engine {
            SearchEngine::Exa => {
                matches!(
                    lower.as_str(),
                    "auto" | "fast" | "neural" | "keyword" | "deep" | "answer"
                )
            }
            SearchEngine::Parallel => {
                matches!(
                    lower.as_str(),
                    "auto" | "fast" | "one-shot" | "agentic" | "deep" | "answer"
                )
            }
            SearchEngine::Tavily | SearchEngine::Perplexity => {
                matches!(lower.as_str(), "auto" | "fast" | "deep" | "answer")
            }
            SearchEngine::Firecrawl => {
                matches!(lower.as_str(), "auto" | "fast" | "deep")
            }
            SearchEngine::Brave | SearchEngine::AnySearch => {
                matches!(lower.as_str(), "auto" | "fast")
            }
            SearchEngine::Auto => false,
        };
        if !supported {
            return false;
        }
    }
    if let Some(max_results) = req.max_results {
        let supported = match engine {
            SearchEngine::Parallel
            | SearchEngine::Exa
            | SearchEngine::Tavily
            | SearchEngine::Firecrawl => true,
            SearchEngine::Perplexity | SearchEngine::Brave | SearchEngine::AnySearch => {
                max_results <= 20
            }
            SearchEngine::Auto => false,
        };
        if !supported {
            return false;
        }
    }
    if !req.allowed_domains.is_empty()
        && !matches!(
            engine,
            SearchEngine::Parallel
                | SearchEngine::Exa
                | SearchEngine::Tavily
                | SearchEngine::Firecrawl
                | SearchEngine::Perplexity
        )
    {
        return false;
    }
    if !req.blocked_domains.is_empty() {
        if !matches!(
            engine,
            SearchEngine::Parallel
                | SearchEngine::Exa
                | SearchEngine::Tavily
                | SearchEngine::Firecrawl
                | SearchEngine::Perplexity
        ) {
            return false;
        }
        // Only Exa and Tavily preserve simultaneous include + exclude domain lists cleanly.
        if !req.allowed_domains.is_empty()
            && !matches!(engine, SearchEngine::Exa | SearchEngine::Tavily)
        {
            return false;
        }
    }
    if engine == SearchEngine::Perplexity
        && req.allowed_domains.len() + req.blocked_domains.len() > 20
    {
        return false;
    }
    if engine == SearchEngine::Parallel
        && req.allowed_domains.len() + req.blocked_domains.len() > 200
    {
        return false;
    }
    if let Some(country) = req
        .country
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        && (country.len() != 2
            || !country.bytes().all(|b| b.is_ascii_alphabetic())
            || !matches!(
                engine,
                SearchEngine::Exa | SearchEngine::Perplexity | SearchEngine::Brave
            ))
    {
        return false;
    }
    engine != SearchEngine::Auto
}

pub fn engine_supports_fetch_policy(engine: FetchEngine, req: &WebRequest) -> bool {
    let external_access = req.external_web_access.unwrap_or(true);
    if !external_access && !matches!(engine, FetchEngine::Exa | FetchEngine::Firecrawl) {
        return false;
    }
    engine != FetchEngine::Auto
}

pub fn execute_web_request(data_dir: Option<&Path>, req: WebRequest) -> Result<JsonValue, String> {
    let action = req
        .action
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| {
            if req
                .url
                .as_deref()
                .map(str::trim)
                .is_some_and(|s| !s.is_empty())
            {
                "fetch".to_string()
            } else {
                "search".to_string()
            }
        });
    let keys = load_web_keys(data_dir);
    match action.as_str() {
        "search" | "query" => execute_web_search(&keys, &req),
        "fetch" | "read" | "scrape" | "extract" => execute_web_fetch(&keys, &req),
        other => Err(format!(
            "unknown `web` action `{other}`; expected `search` or `fetch`"
        )),
    }
}

#[derive(Debug)]
struct EngineError {
    status: Option<u16>,
    message: String,
    retriable: bool,
}

fn is_retriable_status(status: u16) -> bool {
    status == 402 || status == 408 || status == 429 || status >= 500
}

fn normalize_domain(raw: &str) -> String {
    let trimmed = raw.trim().to_ascii_lowercase();
    let without_scheme = trimmed
        .strip_prefix("https://")
        .or_else(|| trimmed.strip_prefix("http://"))
        .unwrap_or(&trimmed);
    let host = without_scheme
        .split('/')
        .next()
        .unwrap_or(without_scheme)
        .split(':')
        .next()
        .unwrap_or(without_scheme)
        .trim_matches('.');
    host.to_string()
}

fn host_matches_domain(host: &str, domain: &str) -> bool {
    let h = host.trim_matches('.').to_ascii_lowercase();
    let d = normalize_domain(domain);
    if d.is_empty() {
        return false;
    }
    h == d || h.ends_with(&format!(".{d}"))
}

fn url_allowed_by_domain_policy(
    url_str: &str,
    allowed_domains: &[String],
    blocked_domains: &[String],
) -> bool {
    let Some((scheme, host, _)) = parse_http_url_parts(url_str) else {
        return false;
    };
    if !matches!(scheme.as_str(), "http" | "https") {
        return false;
    }
    if !blocked_domains.is_empty()
        && blocked_domains
            .iter()
            .any(|d| host_matches_domain(&host, d))
    {
        return false;
    }
    if !allowed_domains.is_empty()
        && !allowed_domains
            .iter()
            .any(|d| host_matches_domain(&host, d))
    {
        return false;
    }
    true
}

fn parse_http_url_parts(raw: &str) -> Option<(String, String, u16)> {
    let trimmed = raw.trim();
    let (scheme, rest) = if let Some(r) = trimmed.strip_prefix("https://") {
        ("https".to_string(), r)
    } else {
        let r = trimmed.strip_prefix("http://")?;
        ("http".to_string(), r)
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("").trim();
    if authority.is_empty() || authority.contains('@') {
        return None;
    }
    let default_port = if scheme == "https" { 443 } else { 80 };
    if let Some(bracket_end) = authority.find(']') {
        if !authority.starts_with('[') {
            return None;
        }
        let host = authority[1..bracket_end].to_ascii_lowercase();
        let after = &authority[bracket_end + 1..];
        let port = if let Some(p_str) = after.strip_prefix(':') {
            p_str.parse::<u16>().ok()?
        } else if after.is_empty() {
            default_port
        } else {
            return None;
        };
        if host.is_empty() {
            return None;
        }
        return Some((scheme, host, port));
    }
    if let Some((h, p_str)) = authority.rsplit_once(':') {
        if h.is_empty() || h.contains(':') {
            return None;
        }
        let port = p_str.parse::<u16>().ok()?;
        Some((scheme, h.to_ascii_lowercase(), port))
    } else {
        Some((scheme, authority.to_ascii_lowercase(), default_port))
    }
}

pub fn validate_fetch_url_ssrf(url_str: &str) -> Result<(), String> {
    validate_fetch_url_inner(url_str, true)
}

fn validate_fetch_url_inner(url_str: &str, resolve_dns: bool) -> Result<(), String> {
    let (_, host, port) = parse_http_url_parts(url_str).ok_or_else(|| {
        "URL must be a valid `http://` or `https://` URL without credentials".to_string()
    })?;
    let lower_host = host.trim_matches('.').to_ascii_lowercase();
    if lower_host.is_empty()
        || lower_host == "localhost"
        || lower_host.ends_with(".localhost")
        || lower_host.ends_with(".local")
        || lower_host.ends_with(".internal")
        || lower_host == "metadata.google.internal"
    {
        return Err(format!(
            "SSRF guard rejected local/internal host `{lower_host}`"
        ));
    }
    if let Ok(ip) = lower_host.parse::<IpAddr>() {
        if is_non_global_ip(ip) {
            return Err(format!("SSRF guard rejected non-public IP `{ip}`"));
        }
        return Ok(());
    }
    if resolve_dns {
        let addrs: Vec<_> = (lower_host.as_str(), port)
            .to_socket_addrs()
            .map_err(|e| format!("failed to resolve host `{lower_host}`: {e}"))?
            .collect();
        if addrs.is_empty() {
            return Err(format!("host `{lower_host}` resolved to no addresses"));
        }
        for sock_addr in addrs {
            if is_non_global_ip(sock_addr.ip()) {
                return Err(format!(
                    "SSRF guard rejected host `{lower_host}` resolving to non-public IP `{}`",
                    sock_addr.ip()
                ));
            }
        }
    }
    Ok(())
}

fn is_non_global_ip(ip: IpAddr) -> bool {
    !anyagents_tools::web::is_global_unicast_ip(ip)
}

fn execute_sylphx_baas_search(
    base_url: &str,
    api_key: &str,
    query: &str,
    selected: SearchEngine,
    req: &WebRequest,
    timeout_ms: u64,
) -> Result<JsonValue, EngineError> {
    let mut payload = json!({
        "query": query,
        "engine": selected.as_str(),
        "timeout_ms": timeout_ms,
    });
    if let Some(mode) = req.mode.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        payload["mode"] = json!(mode);
    }
    if let Some(max_results) = req.max_results {
        payload["max_results"] = json!(max_results);
    }
    if let Some(max_chars) = req.max_chars {
        payload["max_chars"] = json!(max_chars);
    }
    if !req.allowed_domains.is_empty() {
        payload["allowed_domains"] = json!(req.allowed_domains);
    }
    if !req.blocked_domains.is_empty() {
        payload["blocked_domains"] = json!(req.blocked_domains);
    }
    if let Some(country) = req
        .country
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        payload["country"] = json!(country);
    }
    if let Some(ext) = req.external_web_access {
        payload["external_web_access"] = json!(ext);
    }
    let endpoint = format!("{base_url}/v1/tools/web_search");
    let headers = [
        ("Authorization", format!("Bearer {api_key}")),
        ("Content-Type", "application/json".to_string()),
        ("Accept", "application/json".to_string()),
    ];
    send_http_json("POST", &endpoint, &headers, Some(&payload), timeout_ms)
}

fn execute_web_search(
    keys: &HashMap<String, String>,
    req: &WebRequest,
) -> Result<JsonValue, String> {
    let query = req
        .query
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "`web.search` requires a non-empty `query`".to_string())?;
    let selected = SearchEngine::parse(req.engine.as_deref())?;
    let policy_supported: Vec<SearchEngine> = selected
        .cascade_for_mode(req.mode.as_deref())
        .into_iter()
        .filter(|e| engine_supports_search_policy(*e, req))
        .collect();
    if policy_supported.is_empty() {
        return Err(
            "no web search engine supports the requested policy combination (`allowed_domains`, `blocked_domains`, `country`, `mode`, `max_results`, `external_web_access`)"
                .to_string(),
        );
    }
    let baas_cfg = sylphx_baas_config(keys);
    let admitted: Vec<SearchEngine> = policy_supported
        .into_iter()
        .filter(|e| search_engine_has_key(*e, keys))
        .collect();
    if baas_cfg.is_none() && admitted.is_empty() {
        return Err(format!(
            "no API key configured for eligible web search engine(s) (selected `{}`)",
            selected.as_str()
        ));
    }

    let timeout_ms = req
        .timeout_ms
        .unwrap_or(DEFAULT_ENGINE_TIMEOUT_MS)
        .clamp(1_000, 60_000);
    let max_res = req
        .max_results
        .unwrap_or(DEFAULT_SEARCH_MAX_RESULTS)
        .clamp(1, 20) as usize;
    let mut attempts = Vec::new();
    let mut last_err = String::new();

    if let Some((base_url, api_key)) = baas_cfg {
        let started = Instant::now();
        match execute_sylphx_baas_search(&base_url, &api_key, query, selected, req, timeout_ms) {
            Ok(resp) => {
                let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(0);
                let mut hits: Vec<JsonValue> = resp
                    .get("results")
                    .and_then(JsonValue::as_array)
                    .cloned()
                    .unwrap_or_default();
                hits.retain(|h| {
                    h.get("url").and_then(JsonValue::as_str).is_some_and(|u| {
                        url_allowed_by_domain_policy(u, &req.allowed_domains, &req.blocked_domains)
                    })
                });
                if hits.len() > max_res {
                    hits.truncate(max_res);
                }
                let engine_name = resp
                    .get("engine")
                    .and_then(JsonValue::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(selected.as_str());
                let fallback_used = resp
                    .get("fallback_used")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false);
                if let Some(remote_attempts) = resp.get("attempts").and_then(JsonValue::as_array)
                    && !remote_attempts.is_empty()
                {
                    attempts.extend(remote_attempts.iter().cloned());
                } else {
                    attempts.push(json!({
                        "engine": engine_name,
                        "via": "sylphx_baas",
                        "outcome": "succeeded",
                        "duration_ms": duration_ms,
                    }));
                }
                let count = hits.len();
                return Ok(json!({
                    "action": "search",
                    "engine": engine_name,
                    "via": "sylphx_baas",
                    "fallback_used": fallback_used,
                    "query": query,
                    "results": hits,
                    "count": count,
                    "attempts": attempts,
                }));
            }
            Err(err) => {
                let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(0);
                attempts.push(json!({
                    "engine": "sylphx_baas",
                    "outcome": "failed",
                    "http_status": err.status,
                    "duration_ms": duration_ms,
                    "error": err.message,
                }));
                last_err = format!("sylphx_baas: {}", err.message);
            }
        }
    }

    for engine in admitted.into_iter().take(MAX_FAILOVER_ATTEMPTS) {
        let started = Instant::now();
        match execute_single_search_engine(engine, query, req, keys, timeout_ms) {
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
                    "results": hits,
                    "count": count,
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
                if !err.retriable {
                    break;
                }
            }
        }
    }

    Err(format!("web search failed ({last_err})"))
}

fn build_search_payload(engine: SearchEngine, query: &str, req: &WebRequest) -> JsonValue {
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
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_uppercase);

    let mut payload = match engine {
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
        SearchEngine::Exa => {
            json!({
                "query": query,
                "numResults": max_results,
                "contents": {
                    "text": { "maxCharacters": per_result_cap }
                }
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
        SearchEngine::Tavily => json!({
            "query": query,
            "max_results": max_results,
            "include_usage": true,
            "include_answer": false,
        }),
        SearchEngine::Brave => json!({ "query": query }),
        SearchEngine::AnySearch => json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "search",
                "arguments": {
                    "query": query,
                    "max_results": max_results,
                }
            }
        }),
        SearchEngine::Auto => json!({}),
    };

    if engine == SearchEngine::Exa {
        if let Some(m) = req.mode.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            payload["type"] = json!(m.to_ascii_lowercase());
        }
        if req.external_web_access == Some(false) {
            payload["contents"]["maxAgeHours"] = json!(-1);
        }
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
            SearchEngine::Parallel => {
                payload["advanced_settings"]["source_policy"]["include_domains"] = json!(allowed);
            }
            SearchEngine::Exa | SearchEngine::Firecrawl => {
                payload["includeDomains"] = json!(allowed);
            }
            SearchEngine::Perplexity => {
                payload["search_domain_filter"] = json!(allowed);
            }
            SearchEngine::Tavily => {
                payload["include_domains"] = json!(allowed);
                payload["include_domains_mode"] = json!("filter");
            }
            _ => {}
        }
    }
    if !blocked.is_empty() {
        match engine {
            SearchEngine::Parallel => {
                payload["advanced_settings"]["source_policy"]["exclude_domains"] = json!(blocked);
            }
            SearchEngine::Exa | SearchEngine::Firecrawl => {
                payload["excludeDomains"] = json!(blocked);
            }
            SearchEngine::Tavily => {
                payload["exclude_domains"] = json!(blocked);
            }
            SearchEngine::Perplexity => {
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

fn execute_single_search_engine(
    engine: SearchEngine,
    query: &str,
    req: &WebRequest,
    keys: &HashMap<String, String>,
    timeout_ms: u64,
) -> Result<Vec<JsonValue>, EngineError> {
    let payload = build_search_payload(engine, query, req);
    let (key_name, endpoint, header, bearer) = match engine {
        SearchEngine::Parallel => (
            "PARALLEL_API_KEY",
            "https://api.parallel.ai/v1/search",
            "x-api-key",
            false,
        ),
        SearchEngine::Exa => (
            "EXA_API_KEY",
            "https://api.exa.ai/search",
            "x-api-key",
            false,
        ),
        SearchEngine::Firecrawl => (
            "FIRECRAWL_API_KEY",
            "https://api.firecrawl.dev/v2/search",
            "Authorization",
            true,
        ),
        SearchEngine::Perplexity => (
            "PERPLEXITY_API_KEY",
            "https://api.perplexity.ai/search",
            "Authorization",
            true,
        ),
        SearchEngine::Tavily => (
            "TAVILY_API_KEY",
            "https://api.tavily.com/search",
            "Authorization",
            true,
        ),
        SearchEngine::Brave => (
            "BRAVE_SEARCH_API_KEY",
            "https://api.search.brave.com/res/v1/web/search",
            "X-Subscription-Token",
            false,
        ),
        SearchEngine::AnySearch => (
            "ANYSEARCH_API_KEY",
            ANYSEARCH_MCP_ENDPOINT,
            "Authorization",
            true,
        ),
        SearchEngine::Auto => {
            return Err(EngineError {
                status: None,
                message: "invalid engine auto".into(),
                retriable: false,
            });
        }
    };

    let key = keys
        .get(key_name)
        .or_else(|| {
            if engine == SearchEngine::AnySearch {
                keys.get("AA_API_KEY")
            } else {
                None
            }
        })
        .cloned()
        .ok_or_else(|| EngineError {
            status: None,
            message: format!("missing {key_name}"),
            retriable: true,
        })?;

    let auth_val = if bearer { format!("Bearer {key}") } else { key };
    let mut headers = vec![
        (header, auth_val),
        ("Accept", "application/json".to_string()),
    ];
    if engine == SearchEngine::AnySearch {
        headers.push(("X-Anysearch-Client", ANYSEARCH_CLIENT_HEADER.to_string()));
    }

    let parsed = if engine == SearchEngine::Brave {
        let count = req
            .max_results
            .unwrap_or(DEFAULT_SEARCH_MAX_RESULTS)
            .clamp(1, 20);
        let url = format!(
            "{endpoint}?q={}&count={count}&extra_snippets=true",
            urlencoding_query(query)
        );
        if let Some(c) = req
            .country
            .as_deref()
            .map(str::trim)
            .filter(|s| s.len() == 2)
        {
            headers.push(("X-Loc-Country", c.to_ascii_uppercase()));
        }
        send_http_json("GET", &url, &headers, None, timeout_ms)?
    } else {
        headers.push(("Content-Type", "application/json".to_string()));
        send_http_json("POST", endpoint, &headers, Some(&payload), timeout_ms)?
    };

    parse_search_hits(engine, &parsed)
}

fn parse_search_hits(
    engine: SearchEngine,
    data: &JsonValue,
) -> Result<Vec<JsonValue>, EngineError> {
    if data.get("success") == Some(&JsonValue::Bool(false))
        || data.get("error").is_some_and(|v| !v.is_null() && v != "")
        || data.pointer("/result/isError") == Some(&JsonValue::Bool(true))
    {
        return Err(EngineError {
            status: None,
            message: "provider returned error in response body".into(),
            retriable: true,
        });
    }

    let mut out = Vec::new();
    match engine {
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
        SearchEngine::AnySearch => {
            if let Some(arr) = data
                .pointer("/result/content")
                .or_else(|| data.pointer("/result/results"))
                .or_else(|| data.get("results"))
                .and_then(JsonValue::as_array)
            {
                for item in arr {
                    if let Some(h) = make_hit(
                        item.get("url")
                            .or_else(|| item.get("link"))
                            .and_then(JsonValue::as_str),
                        item.get("title").and_then(JsonValue::as_str),
                        item.get("snippet")
                            .or_else(|| item.get("description"))
                            .or_else(|| item.get("text"))
                            .and_then(JsonValue::as_str),
                    ) {
                        out.push(h);
                    } else if let Some(text) = item.get("text").and_then(JsonValue::as_str) {
                        out.extend(parse_anysearch_markdown_hits(text));
                    }
                }
            }
        }
        SearchEngine::Auto => {}
    }
    Ok(out)
}

fn parse_anysearch_markdown_hits(text: &str) -> Vec<JsonValue> {
    let mut hits = Vec::new();
    let mut title: Option<String> = None;
    let mut url: Option<String> = None;
    let mut snippet: Option<String> = None;
    let flush = |hits: &mut Vec<JsonValue>,
                 title: &mut Option<String>,
                 url: &mut Option<String>,
                 snippet: &mut Option<String>| {
        if let Some(u) = url.take() {
            if let Some(h) = make_hit(Some(&u), title.take().as_deref(), snippet.take().as_deref())
            {
                hits.push(h);
            }
        } else {
            title.take();
            snippet.take();
        }
    };
    for raw in text.lines() {
        let line = raw.trim();
        if let Some(rest) = line.strip_prefix("### ") {
            flush(&mut hits, &mut title, &mut url, &mut snippet);
            let heading = rest.split_once(". ").map_or(rest, |(_, r)| r.trim());
            if !heading.is_empty() {
                title = Some(heading.to_string());
            }
        } else if let Some(u) = line
            .strip_prefix("- **URL**:")
            .or_else(|| line.strip_prefix("URL:"))
            .map(str::trim)
        {
            url = Some(u.to_string());
        } else if !line.is_empty() && url.is_some() && snippet.is_none() {
            snippet = Some(line.to_string());
        }
    }
    flush(&mut hits, &mut title, &mut url, &mut snippet);
    hits
}

fn make_hit(url: Option<&str>, title: Option<&str>, snippet: Option<&str>) -> Option<JsonValue> {
    let u = url?.trim();
    parse_http_url_parts(u)?;
    Some(json!({
        "url": u,
        "title": title.map(str::trim).filter(|s| !s.is_empty()),
        "snippet": snippet.map(str::trim).filter(|s| !s.is_empty()),
    }))
}

fn execute_sylphx_baas_fetch(
    base_url: &str,
    api_key: &str,
    url: &str,
    selected: FetchEngine,
    req: &WebRequest,
    max_chars: usize,
    timeout_ms: u64,
) -> Result<JsonValue, EngineError> {
    let mut payload = json!({
        "url": url,
        "engine": selected.as_str(),
        "max_chars": max_chars,
        "timeout_ms": timeout_ms,
    });
    if !req.allowed_domains.is_empty() {
        payload["allowed_domains"] = json!(req.allowed_domains);
    }
    if !req.blocked_domains.is_empty() {
        payload["blocked_domains"] = json!(req.blocked_domains);
    }
    if let Some(ext) = req.external_web_access {
        payload["external_web_access"] = json!(ext);
    }
    let endpoint = format!("{base_url}/v1/tools/web_fetch");
    let headers = [
        ("Authorization", format!("Bearer {api_key}")),
        ("Content-Type", "application/json".to_string()),
        ("Accept", "application/json".to_string()),
    ];
    send_http_json("POST", &endpoint, &headers, Some(&payload), timeout_ms)
}

fn execute_web_fetch(
    keys: &HashMap<String, String>,
    req: &WebRequest,
) -> Result<JsonValue, String> {
    let url = req
        .url
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "`web.fetch` requires a non-empty `url`".to_string())?;
    validate_fetch_url_ssrf(url)?;
    if !url_allowed_by_domain_policy(url, &req.allowed_domains, &req.blocked_domains) {
        return Err(format!(
            "URL `{url}` violates the requested `allowed_domains` / `blocked_domains` policy"
        ));
    }

    let selected = FetchEngine::parse(req.engine.as_deref())?;
    let policy_supported: Vec<FetchEngine> = selected
        .cascade()
        .into_iter()
        .filter(|e| engine_supports_fetch_policy(*e, req))
        .collect();
    if policy_supported.is_empty() {
        return Err(
            "no web fetch engine supports the requested policy (`external_web_access: false` requires `firecrawl` or `exa`)"
                .to_string(),
        );
    }
    let baas_cfg = if selected != FetchEngine::Direct {
        sylphx_baas_config(keys)
    } else {
        None
    };
    let admitted: Vec<FetchEngine> = policy_supported
        .into_iter()
        .filter(|e| fetch_engine_has_key(*e, keys))
        .collect();
    if baas_cfg.is_none() && admitted.is_empty() {
        return Err(format!(
            "no API key configured for eligible web fetch engine(s) (selected `{}`)",
            selected.as_str()
        ));
    }

    let max_chars = req
        .max_chars
        .unwrap_or(DEFAULT_FETCH_MAX_CHARS)
        .clamp(256, HARD_FETCH_MAX_CHARS);
    let timeout_ms = req
        .timeout_ms
        .unwrap_or(DEFAULT_ENGINE_TIMEOUT_MS)
        .clamp(1_000, 60_000);

    let mut attempts = Vec::new();
    let mut last_err = String::new();

    if let Some((base_url, api_key)) = baas_cfg {
        let started = Instant::now();
        match execute_sylphx_baas_fetch(
            &base_url, &api_key, url, selected, req, max_chars, timeout_ms,
        ) {
            Ok(resp) => {
                let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(0);
                let final_url = resp
                    .get("url")
                    .and_then(JsonValue::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(url)
                    .to_string();
                if !url_allowed_by_domain_policy(
                    &final_url,
                    &req.allowed_domains,
                    &req.blocked_domains,
                ) {
                    return Err(format!(
                        "fetched destination URL `{final_url}` violates `allowed_domains` / `blocked_domains` policy"
                    ));
                }
                let title = resp
                    .get("title")
                    .and_then(JsonValue::as_str)
                    .map(str::to_string);
                let full_text = resp
                    .get("content")
                    .or_else(|| resp.get("text"))
                    .and_then(JsonValue::as_str)
                    .unwrap_or("")
                    .to_string();
                let original_chars = full_text.chars().count();
                let truncated = original_chars > max_chars;
                let bounded: String = if truncated {
                    full_text.chars().take(max_chars).collect()
                } else {
                    full_text
                };
                let engine_name = resp
                    .get("engine")
                    .and_then(JsonValue::as_str)
                    .filter(|s| !s.is_empty())
                    .unwrap_or(selected.as_str());
                let fallback_used = resp
                    .get("fallback_used")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false);
                if let Some(remote_attempts) = resp.get("attempts").and_then(JsonValue::as_array)
                    && !remote_attempts.is_empty()
                {
                    attempts.extend(remote_attempts.iter().cloned());
                } else {
                    attempts.push(json!({
                        "engine": engine_name,
                        "via": "sylphx_baas",
                        "outcome": "succeeded",
                        "duration_ms": duration_ms,
                    }));
                }
                return Ok(json!({
                    "action": "fetch",
                    "engine": engine_name,
                    "via": "sylphx_baas",
                    "fallback_used": fallback_used,
                    "url": final_url,
                    "title": title,
                    "content": bounded,
                    "original_chars": original_chars,
                    "truncated": truncated,
                    "attempts": attempts,
                }));
            }
            Err(err) => {
                let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(0);
                attempts.push(json!({
                    "engine": "sylphx_baas",
                    "outcome": "failed",
                    "http_status": err.status,
                    "duration_ms": duration_ms,
                    "error": err.message,
                }));
                last_err = format!("sylphx_baas: {}", err.message);
            }
        }
    }

    // In `auto` mode, try up to MAX_FAILOVER_ATTEMPTS API engines, plus `Direct` as the final fallback if `external_web_access != false`.
    let mut plan: Vec<FetchEngine> = admitted
        .iter()
        .copied()
        .filter(|e| *e != FetchEngine::Direct)
        .take(MAX_FAILOVER_ATTEMPTS)
        .collect();
    if selected == FetchEngine::Direct
        || (selected == FetchEngine::Auto
            && req.external_web_access.unwrap_or(true)
            && !plan.contains(&FetchEngine::Direct))
    {
        plan.push(FetchEngine::Direct);
    }

    for engine in plan {
        let started = Instant::now();
        match execute_single_fetch_engine(engine, url, req, keys, max_chars, timeout_ms) {
            Ok((final_url, title, full_text)) => {
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
                if !err.retriable {
                    break;
                }
            }
        }
    }

    Err(format!("web fetch failed ({last_err})"))
}

fn execute_single_fetch_engine(
    engine: FetchEngine,
    url: &str,
    req: &WebRequest,
    keys: &HashMap<String, String>,
    max_chars: usize,
    timeout_ms: u64,
) -> Result<(String, Option<String>, String), EngineError> {
    if engine == FetchEngine::Direct {
        return execute_direct_http_fetch(url, req, timeout_ms);
    }

    let external_access = req.external_web_access.unwrap_or(true);
    let (key_name, endpoint, header, bearer, body) = match engine {
        FetchEngine::Firecrawl => {
            let mut b = json!({
                "url": url,
                "formats": ["markdown"],
                "skipTlsVerification": false,
            });
            if external_access {
                b["maxAge"] = json!(0);
            } else {
                b["lockdown"] = json!(true);
            }
            (
                "FIRECRAWL_API_KEY",
                "https://api.firecrawl.dev/v2/scrape",
                "Authorization",
                true,
                b,
            )
        }
        FetchEngine::Exa => {
            let b = json!({
                "urls": [url],
                "text": { "maxCharacters": max_chars },
                "maxAgeHours": if external_access { 0 } else { -1 },
            });
            (
                "EXA_API_KEY",
                "https://api.exa.ai/contents",
                "x-api-key",
                false,
                b,
            )
        }
        FetchEngine::Parallel => {
            let b = json!({
                "urls": [url],
                "advanced_settings": { "full_content": true },
            });
            (
                "PARALLEL_API_KEY",
                "https://api.parallel.ai/v1/extract",
                "x-api-key",
                false,
                b,
            )
        }
        FetchEngine::Tavily => {
            let b = json!({
                "urls": [url],
                "extract_depth": "advanced",
                "format": "markdown",
                "include_usage": true,
            });
            (
                "TAVILY_API_KEY",
                "https://api.tavily.com/extract",
                "Authorization",
                true,
                b,
            )
        }
        FetchEngine::AnySearch => {
            let b = json!({ "url": url });
            (
                "ANYSEARCH_API_KEY",
                ANYSEARCH_EXTRACT_ENDPOINT,
                "Authorization",
                true,
                b,
            )
        }
        FetchEngine::Auto | FetchEngine::Direct => {
            return Err(EngineError {
                status: None,
                message: "invalid fetch engine".into(),
                retriable: false,
            });
        }
    };

    let key = keys
        .get(key_name)
        .or_else(|| {
            if engine == FetchEngine::AnySearch {
                keys.get("AA_API_KEY")
            } else {
                None
            }
        })
        .cloned()
        .ok_or_else(|| EngineError {
            status: None,
            message: format!("missing {key_name}"),
            retriable: true,
        })?;

    let auth_val = if bearer { format!("Bearer {key}") } else { key };
    let mut headers = vec![
        (header, auth_val),
        ("Content-Type", "application/json".to_string()),
        ("Accept", "application/json".to_string()),
    ];
    if engine == FetchEngine::AnySearch {
        headers.push(("X-Anysearch-Client", ANYSEARCH_CLIENT_HEADER.to_string()));
    }

    let data = send_http_json("POST", endpoint, &headers, Some(&body), timeout_ms)?;
    parse_fetch_response(engine, url, &data)
}

fn parse_fetch_response(
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
                message: "firecrawl missing data object".into(),
                retriable: true,
            })?;
            let text = page
                .get("markdown")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| EngineError {
                    status: None,
                    message: "firecrawl missing markdown field".into(),
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
                    message: "parallel extract missing full_content and excerpts".into(),
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
        FetchEngine::AnySearch => {
            let item = data.get("data").ok_or_else(|| EngineError {
                status: None,
                message: "anysearch extract missing data".into(),
                retriable: true,
            })?;
            let text = item
                .get("content")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| EngineError {
                    status: None,
                    message: "anysearch extract missing content".into(),
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
            message: "unsupported parse_fetch_response engine".into(),
            retriable: false,
        }),
    }
}

fn resolve_redirect_url(base_url: &str, location: &str) -> Result<String, String> {
    let loc = location.trim();
    if loc.is_empty() {
        return Err("redirect response contained an empty Location header".to_string());
    }
    if loc.starts_with("http://") || loc.starts_with("https://") {
        return Ok(loc.to_string());
    }
    let Some((scheme, rest)) = base_url.split_once("://") else {
        return Err("invalid base URL scheme".to_string());
    };
    if let Some(protocol_rel) = loc.strip_prefix("//") {
        return Ok(format!("{scheme}://{protocol_rel}"));
    }
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..authority_end];
    if loc.starts_with('/') {
        return Ok(format!("{scheme}://{authority}{loc}"));
    }
    let path_part = &rest[authority_end..];
    let dir_end = path_part.rfind('/').map(|i| i + 1).unwrap_or(0);
    let base_dir = if dir_end == 0 {
        "/"
    } else {
        &path_part[..dir_end]
    };
    Ok(format!("{scheme}://{authority}{base_dir}{loc}"))
}

fn execute_direct_http_fetch(
    url: &str,
    req: &WebRequest,
    timeout_ms: u64,
) -> Result<(String, Option<String>, String), EngineError> {
    use std::io::Read as _;
    const MAX_REDIRECT_HOPS: usize = 5;
    const MAX_DIRECT_FETCH_BYTES: u64 = 1_048_576;

    let http = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_millis(timeout_ms.clamp(1_000, 60_000))))
        .max_redirects(0)
        .http_status_as_error(false)
        .build()
        .new_agent();

    let mut current_url = url.to_string();
    for hop in 0..=MAX_REDIRECT_HOPS {
        validate_fetch_url_ssrf(&current_url).map_err(|msg| EngineError {
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
            .header("User-Agent", "Mozilla/5.0 (compatible; AnyAgents/1.0)")
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
                .ok_or_else(|| EngineError {
                    status: Some(status),
                    message: format!("HTTP {status} redirect missing Location header"),
                    retriable: false,
                })?;
            current_url =
                resolve_redirect_url(&current_url, location).map_err(|e| EngineError {
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
        let raw = String::from_utf8_lossy(&body_bytes).into_owned();
        let (title, cleaned) = html_to_clean_markdown(&raw);
        return Ok((current_url, title, cleaned));
    }

    Err(EngineError {
        status: None,
        message: "direct HTTP fetch exhausted redirect hops".to_string(),
        retriable: false,
    })
}

pub fn html_to_clean_markdown(raw: &str) -> (Option<String>, String) {
    anyagents_tools::web::html_to_markdown(raw)
}

fn send_http_json(
    method: &str,
    url: &str,
    headers: &[(&str, String)],
    body: Option<&JsonValue>,
    timeout_ms: u64,
) -> Result<JsonValue, EngineError> {
    let http = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_millis(timeout_ms)))
        .build()
        .new_agent();

    let res = if method.eq_ignore_ascii_case("GET") {
        let mut req = http.get(url);
        for (k, v) in headers {
            req = req.header(*k, v);
        }
        req.call()
    } else {
        let mut req = http.post(url);
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

fn urlencoding_query(s: &str) -> String {
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
    fn dotenv_parser_loads_all_8_provider_keys_without_printing_values() {
        let sample = r#"
            # Comment line
            PARALLEL_API_KEY="pk_parallel_1"
            export EXA_API_KEY='exa_2'
            FIRECRAWL_API_KEY=fc_3
            PERPLEXITY_API_KEY=pplx_4
            TAVILY_API_KEY=tvly_5
            BRAVE_SEARCH_API_KEY=bsa_6
            ANYSEARCH_API_KEY=as_7
            AA_API_KEY=aa_8
        "#;
        let mut map = HashMap::new();
        parse_dotenv_into(sample, &mut map);
        assert_eq!(map.len(), 8);
        assert!(search_engine_has_key(SearchEngine::Parallel, &map));
        assert!(search_engine_has_key(SearchEngine::Exa, &map));
        assert!(search_engine_has_key(SearchEngine::Firecrawl, &map));
        assert!(search_engine_has_key(SearchEngine::Perplexity, &map));
        assert!(search_engine_has_key(SearchEngine::Tavily, &map));
        assert!(search_engine_has_key(SearchEngine::Brave, &map));
        assert!(search_engine_has_key(SearchEngine::AnySearch, &map));
    }

    #[test]
    fn search_and_fetch_policy_admission_enforces_constraints() {
        let cache_only_req = WebRequest {
            query: Some("rust".into()),
            external_web_access: Some(false),
            ..Default::default()
        };
        assert!(engine_supports_search_policy(
            SearchEngine::Exa,
            &cache_only_req
        ));
        assert!(!engine_supports_search_policy(
            SearchEngine::Parallel,
            &cache_only_req
        ));
        assert!(engine_supports_fetch_policy(
            FetchEngine::Exa,
            &cache_only_req
        ));
        assert!(engine_supports_fetch_policy(
            FetchEngine::Firecrawl,
            &cache_only_req
        ));
        assert!(!engine_supports_fetch_policy(
            FetchEngine::Direct,
            &cache_only_req
        ));

        let mixed_domains_req = WebRequest {
            query: Some("tokio".into()),
            allowed_domains: vec!["docs.rs".into()],
            blocked_domains: vec!["docs.rs".into()],
            ..Default::default()
        };
        assert!(engine_supports_search_policy(
            SearchEngine::Exa,
            &mixed_domains_req
        ));
        assert!(engine_supports_search_policy(
            SearchEngine::Tavily,
            &mixed_domains_req
        ));
        assert!(!engine_supports_search_policy(
            SearchEngine::Parallel,
            &mixed_domains_req
        ));
        assert!(!engine_supports_search_policy(
            SearchEngine::Perplexity,
            &mixed_domains_req
        ));
    }

    #[test]
    fn ssrf_guard_rejects_localhost_private_ips_and_non_http_schemes() {
        for bad in [
            "file:///etc/passwd",
            "ftp://example.com/file",
            "http://localhost:8080/admin",
            "http://127.0.0.1:3000/",
            "http://169.254.169.254/latest/meta-data/",
            "http://10.0.0.1/",
            "http://192.168.1.1/",
            "http://172.16.0.1/",
            "http://[::1]:8080/",
            "https://user:pass@example.com/",
        ] {
            assert!(
                validate_fetch_url_inner(bad, false).is_err(),
                "expected SSRF guard to reject {bad}"
            );
        }
        assert!(validate_fetch_url_inner("https://93.184.216.34/index.html", false).is_ok());
    }

    #[test]
    fn html_to_markdown_extracts_title_and_strips_scripts() {
        let html = r#"
            <html>
              <head><title>Example &amp; Co</title><script>alert(1)</script></head>
              <body>
                <h1>Welcome</h1>
                <p>Hello <b>world</b>!</p>
                <ul><li>Item 1</li><li>Item 2</li></ul>
              </body>
            </html>
        "#;
        let (title, md) = html_to_clean_markdown(html);
        assert_eq!(title.as_deref(), Some("Example & Co"));
        assert!(md.contains("# Welcome"));
        assert!(md.contains("Hello world!"));
        assert!(md.contains("- Item 1"));
        assert!(!md.contains("alert(1)"));
    }

    #[test]
    fn sylphx_baas_config_normalizes_base_url_and_accepts_api_or_platform_key() {
        let mut keys = HashMap::new();
        assert!(sylphx_baas_config(&keys).is_none());

        keys.insert("SYLPHX_API_KEY".into(), "sk_live_baas_1".into());
        assert_eq!(
            sylphx_baas_config(&keys),
            Some(("https://api.sylphx.com".into(), "sk_live_baas_1".into()))
        );

        keys.insert(
            "SYLPHX_BASE_URL".into(),
            "https://gateway.sylphx.com/v1/".into(),
        );
        assert_eq!(
            sylphx_baas_config(&keys),
            Some(("https://gateway.sylphx.com".into(), "sk_live_baas_1".into()))
        );

        keys.remove("SYLPHX_API_KEY");
        keys.insert("SYLPHX_PLATFORM_KEY".into(), "pk_platform_2".into());
        assert_eq!(
            sylphx_baas_config(&keys),
            Some(("https://gateway.sylphx.com".into(), "pk_platform_2".into()))
        );
    }

    #[test]
    fn web_search_and_fetch_route_through_sylphx_baas_primary_and_record_fallback_on_failure() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let handle = std::thread::spawn(move || {
            for idx in 0..3 {
                let (mut stream, _) = listener.accept().expect("accept");
                let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
                let mut data = Vec::new();
                let mut chunk = [0u8; 1024];
                loop {
                    match stream.read(&mut chunk) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            data.extend_from_slice(&chunk[..n]);
                            if let Some(hdr_end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                                let body_start = hdr_end + 4;
                                let hdr_str = String::from_utf8_lossy(&data[..hdr_end]);
                                let content_len = hdr_str
                                    .lines()
                                    .find_map(|line| {
                                        let (k, v) = line.split_once(':')?;
                                        k.trim()
                                            .eq_ignore_ascii_case("content-length")
                                            .then(|| v.trim().parse::<usize>().ok())
                                            .flatten()
                                    })
                                    .unwrap_or(0);
                                if data.len() >= body_start + content_len {
                                    break;
                                }
                            }
                        }
                    }
                }
                let req_str = String::from_utf8_lossy(&data);
                let response = match idx {
                    0 => {
                        assert!(req_str.starts_with("POST /v1/tools/web_search "));
                        assert!(
                            req_str
                                .to_ascii_lowercase()
                                .contains("authorization: bearer sk_test_baas")
                        );
                        let body = serde_json::json!({
                            "object": "web_search.result",
                            "action": "search",
                            "query": "sylphx baas",
                            "engine": "parallel",
                            "fallback_used": false,
                            "count": 1,
                            "results": [
                                {
                                    "url": "https://1.1.1.1/docs/baas",
                                    "title": "Sylphx BaaS Unified Web Engine",
                                    "snippet": "Canonical search and fetch via /v1/tools/web_search"
                                }
                            ]
                        })
                        .to_string();
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    }
                    1 => {
                        assert!(req_str.starts_with("POST /v1/tools/web_fetch "));
                        let body = serde_json::json!({
                            "object": "web_fetch.result",
                            "action": "fetch",
                            "url": "https://1.1.1.1/docs/baas",
                            "title": "Sylphx BaaS Doc",
                            "engine": "firecrawl",
                            "fallback_used": false,
                            "content": "# Sylphx BaaS\nUnified fetch content."
                        })
                        .to_string();
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    }
                    _ => {
                        let body = r#"{"error":{"message":"baas unavailable"}}"#;
                        format!(
                            "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    }
                };
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.flush();
                let _ = stream.shutdown(std::net::Shutdown::Write);
            }
        });

        let mut keys = HashMap::new();
        keys.insert("SYLPHX_API_KEY".into(), "sk_test_baas".into());
        keys.insert("SYLPHX_BASE_URL".into(), format!("http://{addr}/v1"));

        // 1. Primary BaaS search succeeds with zero local provider keys
        let search_res = execute_web_search(
            &keys,
            &WebRequest {
                action: Some("search".into()),
                query: Some("sylphx baas".into()),
                ..Default::default()
            },
        )
        .expect("baas search should succeed");
        assert_eq!(search_res["via"], "sylphx_baas");
        assert_eq!(search_res["engine"], "parallel");
        assert_eq!(search_res["count"], 1);
        assert_eq!(search_res["results"][0]["url"], "https://1.1.1.1/docs/baas");

        // 2. Primary BaaS fetch succeeds with zero local provider keys
        let fetch_res = execute_web_fetch(
            &keys,
            &WebRequest {
                action: Some("fetch".into()),
                url: Some("https://1.1.1.1/docs/baas".into()),
                ..Default::default()
            },
        )
        .expect("baas fetch should succeed");
        assert_eq!(fetch_res["via"], "sylphx_baas");
        assert_eq!(fetch_res["engine"], "firecrawl");
        assert_eq!(fetch_res["title"], "Sylphx BaaS Doc");
        assert!(
            fetch_res["content"]
                .as_str()
                .unwrap_or("")
                .contains("Unified fetch content.")
        );

        // 3. When BaaS returns 503 and no local search keys exist, error records sylphx_baas attempt
        let failed = execute_web_search(
            &keys,
            &WebRequest {
                action: Some("search".into()),
                query: Some("fallback test".into()),
                ..Default::default()
            },
        )
        .expect_err("baas 503 with no local keys should return error");
        assert!(failed.contains("sylphx_baas: HTTP 503"), "got: {failed}");

        handle.join().expect("server thread");
    }
}
