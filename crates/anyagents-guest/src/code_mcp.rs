//! Lightweight stdio MCP server wrapper around `agent-code-mode`
//! (`anyagents-guest code-mcp`).

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use clap::Args;
use serde_json::{Value, json};

const DEFAULT_CODE_MODE_BIN: &str = "/usr/local/bin/agent-code-mode";

#[derive(Args, Clone, Debug)]
pub struct CodeMcpArgs {
    #[arg(long)]
    pub bin: Option<PathBuf>,
    #[arg(long)]
    pub workdir: Option<PathBuf>,
}

pub fn run_code_mcp(mut args: CodeMcpArgs) -> Result<(), String> {
    if args.bin.is_none() {
        args.bin = std::env::var_os("ANYAGENTS_CODE_MODE_BIN")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .or_else(|| Some(PathBuf::from(DEFAULT_CODE_MODE_BIN)));
    }
    if args.workdir.is_none() {
        args.workdir = Some(crate::guest::resolve_default_workdir(None));
    }
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut reader = BufReader::new(stdin.lock());
    let mut writer = std::io::BufWriter::new(stdout.lock());
    run_code_mcp_io(&args, &mut reader, &mut writer)
}

pub(crate) fn run_code_mcp_io<R: BufRead, W: Write>(
    args: &CodeMcpArgs,
    reader: &mut R,
    writer: &mut W,
) -> Result<(), String> {
    let bin = args
        .bin
        .as_deref()
        .unwrap_or_else(|| Path::new(DEFAULT_CODE_MODE_BIN));
    let workdir = args
        .workdir
        .clone()
        .unwrap_or_else(|| crate::guest::resolve_default_workdir(None));
    let mut line = String::new();
    while reader.read_line(&mut line).map_err(|e| e.to_string())? > 0 {
        let trimmed = line.trim();
        if !trimmed.is_empty()
            && let Ok(req) = serde_json::from_str::<Value>(trimmed)
            && let Some(resp) = handle_rpc(bin, &workdir, &req)
        {
            writeln!(writer, "{resp}").map_err(|e| e.to_string())?;
            writer.flush().map_err(|e| e.to_string())?;
        }
        line.clear();
    }
    Ok(())
}

fn handle_rpc(bin: &Path, workdir: &Path, req: &Value) -> Option<Value> {
    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
    let id = req.get("id").cloned()?;
    if method.starts_with("notifications/") {
        return None;
    }
    match method {
        "initialize" => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": req.pointer("/params/protocolVersion").and_then(Value::as_str).unwrap_or("2025-03-26"),
                "capabilities": { "tools": {} },
                "serverInfo": { "name": "code", "version": env!("CARGO_PKG_VERSION") }
            }
        })),
        "ping" => Some(json!({ "jsonrpc": "2.0", "id": id, "result": {} })),
        "tools/list" => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "tools": [
                    {
                        "name": "code_run",
                        "description": "Execute JavaScript/TypeScript in sandbox Code Mode (`agent-code-mode run`).",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "code": { "type": "string" },
                                "timeout_ms": { "type": "integer" },
                                "max_output_bytes": { "type": "integer" }
                            },
                            "required": ["code"]
                        }
                    },
                    {
                        "name": "code_types",
                        "description": "Return TypeScript declarations available in `code_run` (`agent-code-mode types`).",
                        "inputSchema": { "type": "object", "properties": {} }
                    }
                ]
            }
        })),
        "tools/call" => {
            let name = req
                .pointer("/params/name")
                .and_then(Value::as_str)
                .unwrap_or("");
            let args = req
                .pointer("/params/arguments")
                .cloned()
                .unwrap_or_else(|| json!({}));
            Some(
                json!({ "jsonrpc": "2.0", "id": id, "result": exec_tool(bin, workdir, name, &args) }),
            )
        }
        other => Some(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32601, "message": format!("method not found: {other}") }
        })),
    }
}

fn exec_tool(bin: &Path, workdir: &Path, name: &str, args: &Value) -> Value {
    let err_res =
        |m: String| json!({ "content": [{ "type": "text", "text": m }], "isError": true });
    let finish = |out: std::process::Output| {
        let s = String::from_utf8_lossy(&out.stdout);
        let e = String::from_utf8_lossy(&out.stderr);
        let text = if !s.trim().is_empty() {
            s.into_owned()
        } else if !e.trim().is_empty() {
            e.into_owned()
        } else {
            format!("exit {}", out.status)
        };
        json!({ "content": [{ "type": "text", "text": text }], "isError": !out.status.success() })
    };
    match name {
        "code_types" => Command::new(bin)
            .args(["types", "--workdir"])
            .arg(workdir)
            .stdin(Stdio::null())
            .output()
            .map(finish)
            .unwrap_or_else(|e| err_res(format!("failed to run {}: {e}", bin.display()))),
        "code_run" => {
            let Some(code) = args.get("code").and_then(Value::as_str) else {
                return err_res("missing required `code` string argument".into());
            };
            let mut cmd = Command::new(bin);
            cmd.args(["run", "--workdir"]).arg(workdir);
            if let Some(ms) = args.get("timeout_ms").and_then(Value::as_u64) {
                cmd.args(["--timeout-ms", &ms.to_string()]);
            }
            if let Some(max_b) = args.get("max_output_bytes").and_then(Value::as_u64) {
                cmd.args(["--max-output-bytes", &max_b.to_string()]);
            }
            let Ok(mut child) = cmd
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
            else {
                return err_res(format!("failed to spawn {}", bin.display()));
            };
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(code.as_bytes());
            }
            child
                .wait_with_output()
                .map(finish)
                .unwrap_or_else(|e| err_res(e.to_string()))
        }
        other => err_res(format!("unknown tool: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    #[test]
    fn mcp_initialize_list_and_call_work() {
        let tmp = tempfile::tempdir().unwrap();
        let fake = tmp.path().join("agent-code-mode");
        std::fs::write(&fake, "#!/bin/sh\nif [ \"$1\" = \"types\" ]; then echo 'declare const fs: any;'; else cat; fi\n").unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let init = handle_rpc(
            &fake,
            tmp.path(),
            &json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}),
        )
        .unwrap();
        assert_eq!(init["result"]["serverInfo"]["name"], "code");
        let list = handle_rpc(
            &fake,
            tmp.path(),
            &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        )
        .unwrap();
        assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 2);
        let run = handle_rpc(&fake, tmp.path(), &json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"code_run","arguments":{"code":"1+1"}}})).unwrap();
        assert_eq!(run["result"]["isError"], false);
        assert_eq!(run["result"]["content"][0]["text"], "1+1");
    }
}
