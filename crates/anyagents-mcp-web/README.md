# `anyagents-mcp-web`

Optional standalone stdio MCP server providing web search (`web_search`) and SSRF-protected web fetch (`web_fetch`) over pure Rust HTTP/TLS with zero dependency on the AnyAgents coordinator.

## Tools

| Tool | Purpose |
|---|---|
| `web_search` | Multi-provider web search with domain filters (`allowed_domains`, `blocked_domains`), country/mode options, bounded failover, and automatic zero-key DuckDuckGo HTML fallback. |
| `web_fetch` | SSRF-protected public `http://`/`https://` fetcher with DNS & redirect-hop IP verification and clean HTML-to-Markdown extraction (plus optional extraction API cascade). |

## Configuration

| Flag | Environment Variable | Default | Description |
|---|---|---|---|
| `--env-file` | `ANYAGENTS_WEB_KEYS_FILE` | None | Optional dotenv file containing search/fetch API keys |
| `--default-timeout-ms` | `ANYAGENTS_WEB_TIMEOUT_MS` | `15000` | Default upstream timeout in milliseconds |
| `--user-agent` | `ANYAGENTS_WEB_USER_AGENT` | `AnyAgentsWebMCP/1.0` | Custom User-Agent header for direct fetches |

Optional provider keys (all read from process environment or `--env-file`; process environment wins):

- `BRAVE_API_KEY` (also accepts `BRAVE_SEARCH_API_KEY`)
- `EXA_API_KEY`
- `TAVILY_API_KEY`
- `PARALLEL_API_KEY`
- `FIRECRAWL_API_KEY`
- `PERPLEXITY_API_KEY`

When no keys are configured, `web_search` uses DuckDuckGo HTML search and `web_fetch` uses direct SSRF-guarded HTTP/TLS fetch with HTML-to-Markdown conversion.

## Example MCP Configuration

```json
{
  "mcpServers": {
    "web": {
      "command": "anyagents-mcp-web",
      "env": {
        "BRAVE_API_KEY": "${BRAVE_API_KEY}",
        "EXA_API_KEY": "${EXA_API_KEY}"
      }
    }
  }
}
```
