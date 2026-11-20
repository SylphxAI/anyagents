# Optional Standalone MCP Capability Servers

Following the clean four-layer boundary, external capabilities live outside the AnyAgents control-plane binary as optional, standard stdio MCP servers. Any MCP-compatible agent harness (`agy`, `claude-code`, `codex`, `opencode`, `spiron`, or custom clients) can mount any subset of these servers independently based on per-agent grants.

| Crate / Binary | Capability | Tools | Coordinator Dependency |
|---|---|---|---|
| [`anyagents-mcp-browser`](../crates/anyagents-mcp-browser/README.md) | Headless Chromium CDP automation | `browser_navigate`, `browser_snapshot`, `browser_click`, `browser_type`, `browser_screenshot`, `browser_eval`, `browser_tabs`, `browser_close` | None |
| [`anyagents-mcp-web`](../crates/anyagents-mcp-web/README.md) | Web search & SSRF-protected fetch | `web_search`, `web_fetch` | None |
| [`anyagents-mcp-computer`](../crates/anyagents-mcp-computer/README.md) | Desktop GUI, screenshot & accessibility | `computer_screenshot`, `computer_click`, `computer_type`, `computer_key`, `computer_scroll`, `computer_snapshot` | None |

---

## Building the Binaries

Once the three crates are included in the root `Cargo.toml` `[workspace.members]`, build any or all of them with:

```bash
cargo build --release --locked \
  -p anyagents-mcp-browser \
  -p anyagents-mcp-web \
  -p anyagents-mcp-computer
```

Install or place `anyagents-mcp-browser`, `anyagents-mcp-web`, and `anyagents-mcp-computer` on `$PATH` (for example `/usr/local/bin/` inside a sandbox container or developer workstation).

---

## Mounting in Agent Harnesses

### Generic JSON (`mcp_config.json` / `.mcp.json` — `agy`, `claude-code`, `spiron`)

```json
{
  "mcpServers": {
    "browser": {
      "command": "anyagents-mcp-browser",
      "args": ["--profile-dir", "/home/user/.local/share/browser-profile"],
      "env": {
        "ANYAGENTS_CHROMIUM_BIN": "/usr/bin/chromium"
      }
    },
    "web": {
      "command": "anyagents-mcp-web",
      "env": {
        "BRAVE_API_KEY": "${BRAVE_API_KEY}",
        "EXA_API_KEY": "${EXA_API_KEY}"
      }
    },
    "computer": {
      "command": "anyagents-mcp-computer",
      "env": {
        "DISPLAY": ":0"
      }
    }
  }
}
```

### Codex (`~/.codex/config.toml`)

```toml
[mcp_servers.browser]
command = "anyagents-mcp-browser"
args = ["--profile-dir", "/home/user/.local/share/browser-profile"]

[mcp_servers.web]
command = "anyagents-mcp-web"

[mcp_servers.web.env]
BRAVE_API_KEY = "..."
EXA_API_KEY = "..."

[mcp_servers.computer]
command = "anyagents-mcp-computer"
```

### OpenCode (`opencode.json`)

```json
{
  "mcp": {
    "browser": {
      "type": "local",
      "command": ["anyagents-mcp-browser"]
    },
    "web": {
      "type": "local",
      "command": ["anyagents-mcp-web"]
    },
    "computer": {
      "type": "local",
      "command": ["anyagents-mcp-computer"]
    }
  }
}
```

---

## Server Details & Environment Variables

### 1. `anyagents-mcp-browser`

- **Transport:** MCP over stdio (`rmcp`).
- **Chromium Discovery Order:**
  1. `--chromium-bin` / `ANYAGENTS_CHROMIUM_BIN`
  2. `$PATH` (`chromium`, `chromium-browser`, `google-chrome-stable`, `google-chrome`, `chrome`)
  3. Standard system locations (`/usr/bin/chromium`, `/usr/bin/google-chrome`, macOS `.app` bundles, Windows Chrome)
  4. Playwright browser caches (`PLAYWRIGHT_BROWSERS_PATH`, `/opt/playwright`, `~/.cache/ms-playwright`, `~/Library/Caches/ms-playwright`)
- **Profiles:** Pass `--profile-dir <dir>` or `ANYAGENTS_BROWSER_PROFILE_DIR=<dir>` to persist cookies and site storage across sessions; omit for a clean ephemeral directory per session.
- **Navigation Safety:** Allows only `http://`, `https://`, and `about:blank` (bare hostnames default to `https://`). Explicit `target_id` values fail closed with `STALE_TARGET`, and stale accessibility refs fail closed with `STALE_REF`.

### 2. `anyagents-mcp-web`

- **Transport:** MCP over stdio (`rmcp`), pure Rust HTTP/TLS (`ureq` + `rustls`).
- **Zero-Key Default:** Works out of the box with no API keys (`web_search` falls back to DuckDuckGo HTML search; `web_fetch` performs direct HTTP/TLS fetch with HTML-to-Markdown conversion).
- **Optional Provider Keys:** Set any of `BRAVE_API_KEY` (or `BRAVE_SEARCH_API_KEY`), `EXA_API_KEY`, `TAVILY_API_KEY`, `PARALLEL_API_KEY`, `FIRECRAWL_API_KEY`, or `PERPLEXITY_API_KEY` via environment variables or `--env-file` / `ANYAGENTS_WEB_KEYS_FILE`.
- **SSRF Protection:** Rejects non-HTTP(S) schemes, embedded credentials, local/internal hostnames, private/loopback/link-local/CGNAT/metadata IPs (both literal and DNS-resolved), and re-validates every HTTP redirect hop.

### 3. `anyagents-mcp-computer`

- **Transport:** MCP over stdio (`rmcp`), backed by `agent-workstation` (`PlatformAdapter`) plus OS screenshot/input helpers.
- **Headless Safety:** In headless Linux containers where neither `DISPLAY` nor `WAYLAND_DISPLAY` is set, every tool fails immediately with a structured `UNSUPPORTED` MCP tool error rather than hanging or spawning GUI subprocesses.
- **Semantic Accessibility + Pixel Fallback:** Supports `computer_snapshot` for `@<snapshot_id>:e<N>` refs with `STALE_REF` protection alongside pixel coordinate input (`x`, `y`) on X11 hosts.
