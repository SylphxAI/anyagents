//! Configuration and provider credential loading for `anyagents-mcp-web`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use clap::Parser;

/// Environment variables inspected for optional search/fetch provider keys.
pub const PROVIDER_KEY_VARS: &[&str] = &[
    "BRAVE_API_KEY",
    "BRAVE_SEARCH_API_KEY",
    "EXA_API_KEY",
    "TAVILY_API_KEY",
    "PARALLEL_API_KEY",
    "FIRECRAWL_API_KEY",
    "PERPLEXITY_API_KEY",
];

/// Default upstream request timeout in milliseconds.
pub const DEFAULT_WEB_TIMEOUT_MS: u64 = 15_000;

/// CLI arguments and environment configuration for `anyagents-mcp-web`.
#[derive(Clone, Debug, Parser)]
#[command(
    name = "anyagents-mcp-web",
    version,
    about = "Standalone stdio MCP server providing web search and SSRF-protected web fetch"
)]
pub struct WebServerConfig {
    /// Optional dotenv file containing search/fetch provider API keys.
    /// Process environment variables always take precedence over file entries.
    #[arg(long, env = "ANYAGENTS_WEB_KEYS_FILE")]
    pub env_file: Option<PathBuf>,

    /// Default request timeout in milliseconds (clamped to 1000..=60000).
    #[arg(
        long,
        env = "ANYAGENTS_WEB_TIMEOUT_MS",
        default_value_t = DEFAULT_WEB_TIMEOUT_MS
    )]
    pub default_timeout_ms: u64,

    /// Optional custom User-Agent sent on direct HTTP fetches.
    #[arg(long, env = "ANYAGENTS_WEB_USER_AGENT")]
    pub user_agent: Option<String>,
}

impl Default for WebServerConfig {
    fn default() -> Self {
        Self {
            env_file: None,
            default_timeout_ms: DEFAULT_WEB_TIMEOUT_MS,
            user_agent: None,
        }
    }
}

impl WebServerConfig {
    /// Load provider credentials from the optional dotenv file and process environment.
    /// Values are never logged or included in tool responses.
    pub fn load_keys(&self) -> HashMap<String, String> {
        load_provider_keys(self.env_file.as_deref())
    }

    /// Resolve the effective HTTP User-Agent string.
    pub fn effective_user_agent(&self) -> &str {
        self.user_agent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("Mozilla/5.0 (compatible; AnyAgentsWebMCP/1.0)")
    }
}

/// Load provider API keys from `env_file` (if readable) overlaid with live process env.
pub fn load_provider_keys(env_file: Option<&Path>) -> HashMap<String, String> {
    let mut keys = HashMap::new();
    if let Some(path) = env_file
        && let Ok(text) = std::fs::read_to_string(path)
    {
        parse_dotenv_into(&text, &mut keys);
    }
    for name in PROVIDER_KEY_VARS {
        if let Ok(val) = std::env::var(name) {
            let trimmed = val.trim();
            if !trimmed.is_empty() {
                keys.insert((*name).to_string(), trimmed.to_string());
            }
        }
    }
    keys
}

/// Parse simple `KEY=value` / `export KEY="value"` lines into `out` without overwriting existing entries.
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
        if ((val.starts_with('"') && val.ends_with('"'))
            || (val.starts_with('\'') && val.ends_with('\'')))
            && val.len() >= 2
        {
            val = &val[1..val.len() - 1];
        }
        let val = val.trim();
        if !key.is_empty() && !val.is_empty() {
            out.entry(key.to_string())
                .or_insert_with(|| val.to_string());
        }
    }
}

/// Look up the Brave Search API key (`BRAVE_API_KEY` or legacy `BRAVE_SEARCH_API_KEY`).
pub fn brave_api_key(keys: &HashMap<String, String>) -> Option<&str> {
    keys.get("BRAVE_API_KEY")
        .or_else(|| keys.get("BRAVE_SEARCH_API_KEY"))
        .map(String::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotenv_parser_loads_provider_keys_and_strips_quotes() {
        let sample = r#"
            # Provider keys
            BRAVE_API_KEY="brave_key_1"
            export EXA_API_KEY='exa_key_2'
            TAVILY_API_KEY=tvly_3
            PARALLEL_API_KEY=par_4
            FIRECRAWL_API_KEY=fc_5
            PERPLEXITY_API_KEY=pplx_6
        "#;
        let mut keys = HashMap::new();
        parse_dotenv_into(sample, &mut keys);
        assert_eq!(brave_api_key(&keys), Some("brave_key_1"));
        assert_eq!(
            keys.get("EXA_API_KEY").map(String::as_str),
            Some("exa_key_2")
        );
        assert_eq!(
            keys.get("TAVILY_API_KEY").map(String::as_str),
            Some("tvly_3")
        );
        assert_eq!(keys.len(), 6);
    }

    #[test]
    fn brave_api_key_accepts_both_env_names() {
        let mut keys = HashMap::new();
        keys.insert("BRAVE_SEARCH_API_KEY".into(), "legacy_brave".into());
        assert_eq!(brave_api_key(&keys), Some("legacy_brave"));
        keys.insert("BRAVE_API_KEY".into(), "preferred_brave".into());
        assert_eq!(brave_api_key(&keys), Some("preferred_brave"));
    }

    #[test]
    fn loads_keys_from_dotenv_file_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join("web-keys.env");
        std::fs::write(&env_path, "FIRECRAWL_API_KEY=fc_from_disk\n").unwrap();
        let keys = load_provider_keys(Some(&env_path));
        assert_eq!(
            keys.get("FIRECRAWL_API_KEY").map(String::as_str),
            Some("fc_from_disk")
        );
    }
}
