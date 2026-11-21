//! MCP `nodes` tool: list authorized edge nodes and execute scoped commands.

use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};

use super::{AgentMcp, urlencoding_simple};

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub(super) struct NodesArgs {
    /// Action: `"list"` (default), `"exec"`, `"files_list"`, `"files_read"`, or `"files_write"`.
    #[serde(default)]
    pub(super) action: Option<String>,
    /// Target node ID (`nd_...`) for actions other than `"list"`.
    #[serde(default)]
    pub(super) node_id: Option<String>,
    /// Executable or command name to run on the remote node for `"exec"`.
    #[serde(default)]
    pub(super) command: Option<String>,
    /// Optional command arguments for `"exec"`.
    #[serde(default)]
    pub(super) args: Vec<String>,
    /// Optional working directory on the remote node for `"exec"`.
    #[serde(default)]
    pub(super) cwd: Option<String>,
    /// Optional execution timeout in seconds (default 30, max 300) for `"exec"`.
    #[serde(default)]
    pub(super) timeout_secs: Option<u64>,
    /// File or directory path on the remote node for `"files_list"`, `"files_read"`, `"files_write"`.
    #[serde(default)]
    pub(super) path: Option<String>,
    /// Base64-encoded file content for `"files_write"`.
    #[serde(default)]
    pub(super) data_b64: Option<String>,
}

pub(super) async fn call(mcp: &AgentMcp, args: NodesArgs) -> Result<Value, String> {
    let action = args
        .action
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("list");

    match action {
        "list" => mcp.client.get("/v1/nodes", None).await,
        "exec" => {
            let node_id = args
                .node_id
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`nodes` exec requires `node_id`".to_string())?;
            let command = args
                .command
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`nodes` exec requires `command`".to_string())?;
            let body = serde_json::to_vec(&json!({
                "command": command,
                "args": args.args,
                "cwd": args.cwd,
                "timeout_secs": args.timeout_secs,
            }))
            .map_err(|err| err.to_string())?;
            let path = format!("/v1/nodes/{}/exec", urlencoding_simple(node_id));
            mcp.client.get(&path, Some(body)).await
        }
        "files_list" | "files_read" => {
            let node_id = args
                .node_id
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| format!("`nodes` {action} requires `node_id`"))?;
            let path = args
                .path
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| format!("`nodes` {action} requires `path`"))?;
            let body =
                serde_json::to_vec(&json!({ "path": path })).map_err(|err| err.to_string())?;
            let route = if action == "files_list" {
                "files/list"
            } else {
                "files/read"
            };
            let url = format!("/v1/nodes/{}/{route}", urlencoding_simple(node_id));
            mcp.client.get(&url, Some(body)).await
        }
        "files_write" => {
            let node_id = args
                .node_id
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`nodes` files_write requires `node_id`".to_string())?;
            let path = args
                .path
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`nodes` files_write requires `path`".to_string())?;
            let data_b64 = args
                .data_b64
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| "`nodes` files_write requires `data_b64`".to_string())?;
            let body = serde_json::to_vec(&json!({
                "path": path,
                "data_b64": data_b64,
            }))
            .map_err(|err| err.to_string())?;
            let url = format!("/v1/nodes/{}/files/write", urlencoding_simple(node_id));
            mcp.client.get(&url, Some(body)).await
        }
        other => Err(format!(
            "unsupported `nodes` action `{other}` \
             (expected `list`, `exec`, `files_list`, `files_read`, or `files_write`)"
        )),
    }
}
