# `anyagents-mcp-browser`

Optional standalone stdio MCP server providing headless Chromium CDP browser automation with zero dependency on the AnyAgents coordinator.

## Tools

| Tool | Purpose |
|---|---|
| `browser_navigate` | Navigate to a URL (`http://`, `https://`, `about:blank`) or step history (`back`, `forward`, `reload`). |
| `browser_snapshot` | Capture an accessibility outline with interactive `@eN` / `@<snapshot_id>:eN` refs, visible page text, or outer HTML. |
| `browser_click` | Click an element by snapshot ref or CSS selector. |
| `browser_type` | Focus, clear, and fill a field with `text` or in-memory `secret_ref` (`op://...`, `env:<VAR>`). |
| `browser_screenshot` | Capture viewport or `full_page` PNG screenshot as a native MCP image block (optional `path` export). |
| `browser_eval` | Evaluate a JavaScript expression in the active page (awaits promises). |
| `browser_tabs` | List, open, activate, or close tabs (`target_id` fails closed with `STALE_TARGET`). |
| `browser_profiles` | List capability-scoped coordinator profiles (`GET /v1/browser-profiles`) or local `--profile-dir` profiles. |
| `browser_takeover` | Start interactive human takeover (`sway` + `wayvnc` + tokenized noVNC URL, or visible headful window handoff). |
| `browser_takeover_status` | Inspect active takeover state (`status`), release control (`release`), or stop (`stop`). |
| `browser_close` | Shut down the running Chromium / takeover session cleanly (preserving/syncing profile data). |

## Configuration

| Flag | Environment Variable | Default | Description |
|---|---|---|---|
| `--chromium-bin` | `ANYAGENTS_CHROMIUM_BIN` | Auto-discovered | Explicit Chromium / Chrome / Playwright binary path |
| `--profile-dir` | `ANYAGENTS_BROWSER_PROFILE_DIR` | Ephemeral tempdir | Persistent Chromium profile directory |
| `--proxy` | `ANYAGENTS_BROWSER_PROXY` | Direct | Optional HTTP/SOCKS5 proxy URL |
| `--headful` | `ANYAGENTS_BROWSER_HEADFUL` | `false` | Launch with a visible window instead of `--headless=new` |
| `--extra-args` | `ANYAGENTS_BROWSER_ARGS` | None | Extra whitespace-separated Chromium flags |
| `--viewport-width` | — | `1280` | Initial viewport width in CSS pixels |
| `--viewport-height` | — | `800` | Initial viewport height in CSS pixels |

Auto-discovery checks `ANYAGENTS_CHROMIUM_BIN`, `$PATH` (`chromium`, `chromium-browser`, `google-chrome-stable`, `google-chrome`, `chrome`), standard OS paths, and Playwright browser caches (`PLAYWRIGHT_BROWSERS_PATH`, `/opt/playwright`, `~/.cache/ms-playwright`, `~/Library/Caches/ms-playwright`).

## Example MCP Configuration

```json
{
  "mcpServers": {
    "browser": {
      "command": "anyagents-mcp-browser",
      "args": ["--profile-dir", "/path/to/browser-profile"],
      "env": {
        "ANYAGENTS_CHROMIUM_BIN": "/usr/bin/chromium"
      }
    }
  }
}
```
