# `anyagents-mcp-computer`

Optional standalone stdio MCP server providing desktop GUI screenshots, semantic accessibility snapshots, and input automation with zero dependency on the AnyAgents coordinator.

## Tools

| Tool | Purpose |
|---|---|
| `computer_screenshot` | Capture full-screen or cropped `region` PNG screenshot as a native MCP image block (optional `path` export). |
| `computer_click` | Click a semantic accessibility `ref` (`@<snapshot_id>:e<N>`) or logical `(x, y)` coordinates (`left`, `right`, `middle`, single/double-click). |
| `computer_type` | Type Unicode text into the focused control, a semantic `ref`, or after clicking `(x, y)`. |
| `computer_key` | Press a key or shortcut chord (e.g. `Return`, `Escape`, `ctrl+c`, `cmd+l`). |
| `computer_scroll` | Scroll `up`, `down`, `left`, or `right` at a semantic `ref`, `(x, y)`, or the current pointer. |
| `computer_snapshot` | Capture a structured desktop accessibility tree with `@<snapshot_id>:e<N>` refs (`STALE_REF` protected). |

## Headless Behavior

On headless Linux containers where neither `DISPLAY` nor `WAYLAND_DISPLAY` is set, every tool fails cleanly and immediately with a structured MCP tool error (`code: "UNSUPPORTED"`), never hanging or spawning broken GUI helpers.

## Configuration

| Flag | Environment Variable | Description |
|---|---|---|
| `--display` | `DISPLAY` | X11 display identifier (e.g. `:0`, `:99`) |
| `--wayland-display` | `WAYLAND_DISPLAY` | Wayland display socket name (e.g. `wayland-0`) |

## Example MCP Configuration

```json
{
  "mcpServers": {
    "computer": {
      "command": "anyagents-mcp-computer",
      "env": {
        "DISPLAY": ":0"
      }
    }
  }
}
```
