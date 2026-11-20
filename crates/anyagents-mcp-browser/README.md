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
| `--proxy` | `ANYAGENTS_BROWSER_PROXY` | Direct | Optional browser-only HTTP/HTTPS/SOCKS5/SOCKS5H proxy URL (also supports per-profile `ANYAGENTS_BROWSER_PROXY_<PROFILE>` and per-call `proxy`/`proxy_env`; never reads global `HTTP_PROXY`) |
| `--headful` | `ANYAGENTS_BROWSER_HEADFUL` | `false` | Launch directly with a visible window on the host display instead of `--headless=new` |
| `--browser-mode` | `ANYAGENTS_BROWSER_MODE` | `auto` | `desktop`: real headful browser inside a private `sway` Wayland session (no `--headless`/`--disable-gpu`; the same window is used for `browser_takeover`, so takeover never restarts the browser); fails if `sway` is missing. `headless`: direct launch. `auto`: `desktop` when Linux `sway` and a full (non `headless_shell`) browser are available and `--headful` is unset, else `headless` (reason logged to stderr) |
| `--timezone` | `ANYAGENTS_BROWSER_TIMEZONE` | `$TZ`, else `UTC` | IANA browser timezone (e.g. `Europe/London`); desktop mode records it in the profile's `fingerprint.json` when the profile is first created |
| `--locale` | `ANYAGENTS_BROWSER_LOCALE` | `en-US,en` | Comma-separated BCP 47 browser languages (`--lang` / `Accept-Language`) |
| `--extra-args` | `ANYAGENTS_BROWSER_ARGS` | None | Extra whitespace-separated Chromium flags |
| `--viewport-width` | — | `1280` | Initial viewport width in CSS pixels |
| `--viewport-height` | — | `800` | Initial viewport height in CSS pixels |

Auto-discovery checks `ANYAGENTS_CHROMIUM_BIN`, then standard binary directories preferring official Google Chrome (`google-chrome-stable`, `google-chrome`, `chrome`) over `chromium` / `chromium-browser`, standard OS paths, and Playwright browser caches (`PLAYWRIGHT_BROWSERS_PATH`, `/opt/playwright`, `~/.cache/ms-playwright`, `~/Library/Caches/ms-playwright`).

`SIGTERM`, `SIGINT`, and `SIGHUP` trigger the same graceful shutdown as closing stdin: the browser is closed cleanly (flushing its cookie store) and the active named profile is synced back before exit.

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
