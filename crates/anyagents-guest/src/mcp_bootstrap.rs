//! Shared discovery and idempotent registration of installed local stdio MCP
//! servers (`browser`, `web`, `code`) across `agy`, `claude-code`, `spiron`,
//! `opencode`, `codex`, and `omp` (used by both coordinator and guest runtime).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value, json};

use crate::guest::GuestConfig;

const WEB_SEARCH_ENV_KEYS: &[&str] = &[
    "TAVILY_API_KEY",
    "BRAVE_API_KEY",
    "BRAVE_SEARCH_API_KEY",
    "EXA_API_KEY",
    "FIRECRAWL_API_KEY",
    "PARALLEL_API_KEY",
    "PERPLEXITY_API_KEY",
    "ANYSEARCH_API_KEY",
];

#[derive(Clone, Debug)]
pub struct LocalMcpServer {
    pub name: &'static str,
    pub command: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
}

fn env_val(key: &str, env: &[(String, String)]) -> Option<String> {
    env.iter()
        .rev()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .or_else(|| std::env::var(key).ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn find_bin(name: &str, dir_override: Option<&Path>) -> Option<PathBuf> {
    if let Some(d) = dir_override {
        let p = d.join(name);
        return p.is_file().then_some(p);
    }
    if let Ok(resolved) = anyagents_toolchain::tool(name, "anyagents-guest.mcp_bootstrap")
        && resolved.path.is_file()
    {
        return Some(resolved.path);
    }
    for d in ["/usr/local/bin", "/usr/bin", "/home/user/.local/bin"] {
        let p = Path::new(d).join(name);
        if p.is_file() {
            return Some(p);
        }
    }
    None
}

pub fn discover_local_mcp_servers(
    state_dir: Option<&Path>,
    work_dir: Option<&Path>,
    env: &[(String, String)],
    api_url: Option<&str>,
    agent_id: Option<&str>,
    token: Option<&str>,
) -> Vec<LocalMcpServer> {
    discover_in(state_dir, work_dir, env, api_url, agent_id, token, None)
}

fn discover_in(
    state_dir: Option<&Path>,
    work_dir: Option<&Path>,
    env: &[(String, String)],
    api_url: Option<&str>,
    agent_id: Option<&str>,
    token: Option<&str>,
    search_dir: Option<&Path>,
) -> Vec<LocalMcpServer> {
    let mut out = Vec::new();
    let api = api_url
        .map(str::to_string)
        .or_else(|| env_val("ANYAGENTS_API_URL", env));
    let aid = agent_id
        .map(str::to_string)
        .or_else(|| env_val("ANYAGENTS_AGENT_ID", env));
    let tok = token
        .map(str::to_string)
        .or_else(|| env_val("ANYAGENTS_AGENT_TOKEN", env));

    if let Some(bin) = find_bin("anyagents-mcp-browser", search_dir) {
        let mut senv = BTreeMap::new();
        for (k, v) in [
            ("ANYAGENTS_API_URL", api.clone()),
            ("ANYAGENTS_AGENT_ID", aid.clone()),
            ("ANYAGENTS_AGENT_TOKEN", tok),
        ] {
            if let Some(val) = v {
                senv.insert(k.into(), val);
            }
        }
        let shot = env_val("ANYAGENTS_BROWSER_SCREENSHOT_DIR", env).or_else(|| {
            state_dir.map(|sd| {
                let d = crate::media::sandbox_data_dir(sd, env).join("tmp/screenshots");
                let _ = std::fs::create_dir_all(&d);
                d.display().to_string()
            })
        });
        if let Some(s) = shot {
            senv.insert("ANYAGENTS_BROWSER_SCREENSHOT_DIR".into(), s);
        }
        for (k, v) in std::env::vars().chain(env.iter().cloned()) {
            let trimmed = v.trim();
            if !trimmed.is_empty()
                && (k == "ANYAGENTS_BROWSER_PROXY" || k.starts_with("ANYAGENTS_BROWSER_PROXY_"))
            {
                senv.insert(k, trimmed.to_string());
            }
        }
        out.push(LocalMcpServer {
            name: "browser",
            command: bin.display().to_string(),
            args: Vec::new(),
            env: senv,
        });
    }
    if let Some(bin) = find_bin("anyagents-mcp-web", search_dir) {
        let mut senv = BTreeMap::new();
        for k in WEB_SEARCH_ENV_KEYS {
            if let Some(v) = env_val(k, env) {
                senv.insert((*k).into(), v);
            }
        }
        if let Some(u) = api {
            senv.insert("ANYAGENTS_API_URL".into(), u);
        }
        if let Some(id) = aid {
            senv.insert("ANYAGENTS_AGENT_ID".into(), id);
        }
        out.push(LocalMcpServer {
            name: "web",
            command: bin.display().to_string(),
            args: Vec::new(),
            env: senv,
        });
    }
    if let Some(bin) = find_bin("agent-code-mode", search_dir)
        && let Some(guest) = find_bin("anyagents-guest", search_dir)
            .or_else(|| state_dir.and_then(|_| std::env::current_exe().ok()))
    {
        let mut args = vec!["code-mcp".into(), "--bin".into(), bin.display().to_string()];
        if let Some(wd) = work_dir {
            args.extend(["--workdir".into(), wd.display().to_string()]);
        }
        out.push(LocalMcpServer {
            name: "code",
            command: guest.display().to_string(),
            args,
            env: BTreeMap::new(),
        });
    }
    out
}

pub fn insert_stdio_servers_json(servers: &mut Map<String, Value>, local: &[LocalMcpServer]) {
    for s in local {
        let env: Map<String, Value> = s
            .env
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect();
        servers.insert(
            s.name.into(),
            json!({ "type": "stdio", "command": s.command, "args": s.args, "env": env }),
        );
    }
}

pub fn insert_opencode_servers_json(mcp: &mut Map<String, Value>, local: &[LocalMcpServer]) {
    for s in local {
        let mut cmd = vec![s.command.clone()];
        cmd.extend(s.args.iter().cloned());
        let mut entry = json!({ "type": "local", "command": cmd, "enabled": true });
        if !s.env.is_empty() {
            entry["environment"] = json!(s.env);
        }
        mcp.insert(s.name.into(), entry);
    }
}

pub fn upsert_codex_toml_servers(existing: &str, local: &[LocalMcpServer]) -> String {
    let mut text = existing.to_string();
    for s in local {
        let (hdr, ehdr) = (
            format!("[mcp_servers.{}]", s.name),
            format!("[mcp_servers.{}.env]", s.name),
        );
        text = strip_toml_table(&strip_toml_table(&text, &hdr), &ehdr);
        if !text.is_empty() && !text.ends_with('\n') {
            text.push('\n');
        }
        let cmd = serde_json::to_string(&s.command).unwrap_or_else(|_| "\"\"".into());
        let args = serde_json::to_string(&s.args).unwrap_or_else(|_| "[]".into());
        text.push_str(&format!("\n{hdr}\ncommand = {cmd}\nargs = {args}\n"));
        if !s.env.is_empty() {
            text.push_str(&format!("\n{ehdr}\n"));
            for (k, v) in &s.env {
                let ev = serde_json::to_string(v).unwrap_or_else(|_| "\"\"".into());
                text.push_str(&format!("{k} = {ev}\n"));
            }
        }
    }
    text
}

pub fn agy_allow_grants(config_dir: &Path) -> Vec<String> {
    let mut grants = vec![
        "mcp(anyagents/*)".into(),
        format!(
            "read_file({})",
            config_dir.join(".gemini/antigravity-cli/mcp").display()
        ),
    ];
    for s in discover_local_mcp_servers(None, None, &[], None, None, None) {
        grants.push(format!("mcp({}/*)", s.name));
    }
    grants
}

pub(crate) fn sync_local_mcp_configs(
    harness: &str,
    state_dir: &Path,
    work_dir: &Path,
    env: &[(String, String)],
    file_cfg: Option<&GuestConfig>,
) {
    let api = crate::media::resolve_api_base(env, file_cfg);
    let aid =
        env_val("ANYAGENTS_AGENT_ID", env).or_else(|| file_cfg.and_then(|c| c.agent_id.clone()));
    let tok = crate::media::resolve_auth_token(state_dir, env, file_cfg);
    let mut servers = discover_local_mcp_servers(
        Some(state_dir),
        Some(work_dir),
        env,
        api.as_deref(),
        aid.as_deref(),
        tok.as_deref(),
    );
    // Enable only in Linux sandbox images with the relay binary.
    if cfg!(target_os = "linux")
        && env_val("ANYAGENTS_BROWSER_RECONNECT", env).as_deref() == Some("1")
        && let Some(browser) = servers.iter_mut().find(|server| server.name == "browser")
    {
        browser.args = vec![
            "--guest-runtime-dir".to_string(),
            state_dir.join(".browser-runtime").display().to_string(),
        ];
    }
    if !servers.is_empty() {
        apply_servers(harness, state_dir, &servers);
    }
}

fn apply_servers(harness: &str, state_dir: &Path, servers: &[LocalMcpServer]) {
    let upsert_json = |path: PathBuf, key: &str, opencode: bool| {
        let mut root = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str::<Value>(&s).ok())
            .filter(Value::is_object)
            .unwrap_or_else(|| {
                if opencode {
                    json!({ "$schema": "https://opencode.ai/config.json" })
                } else {
                    json!({})
                }
            });
        let slot = root
            .as_object_mut()
            .unwrap()
            .entry(key)
            .or_insert_with(|| json!({}));
        if !slot.is_object() {
            *slot = json!({});
        }
        let map = slot.as_object_mut().unwrap();
        if opencode {
            insert_opencode_servers_json(map, servers);
        } else {
            insert_stdio_servers_json(map, servers);
        }
        write_json_atomic(&path, &root);
    };
    match harness {
        "agy" => {
            upsert_json(
                state_dir.join(".gemini/config/mcp_config.json"),
                "mcpServers",
                false,
            );
            let p = state_dir.join(".gemini/antigravity-cli/settings.json");
            let mut root = std::fs::read_to_string(&p)
                .ok()
                .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                .filter(Value::is_object)
                .unwrap_or_else(|| json!({}));
            let perms = root
                .as_object_mut()
                .unwrap()
                .entry("permissions")
                .or_insert_with(|| json!({}));
            if !perms.is_object() {
                *perms = json!({});
            }
            let allow = perms
                .as_object_mut()
                .unwrap()
                .entry("allow")
                .or_insert_with(|| json!([]));
            if !allow.is_array() {
                *allow = json!([]);
            }
            let arr = allow.as_array_mut().unwrap();
            for n in std::iter::once("anyagents").chain(servers.iter().map(|s| s.name)) {
                let g = format!("mcp({n}/*)");
                if !arr.iter().any(|v| v.as_str() == Some(g.as_str())) {
                    arr.push(Value::String(g));
                }
            }
            write_json_atomic(&p, &root);
        }
        "claude-code" => {
            for r in [".mcp.json", "mcp.json", ".claude.json"] {
                upsert_json(state_dir.join(r), "mcpServers", false);
            }
        }
        "spiron" => upsert_json(state_dir.join("spiron-mcp.json"), "mcpServers", false),
        "opencode" => {
            for r in [
                "opencode.json",
                ".config/opencode/opencode.json",
                "opencode/opencode.json",
            ] {
                upsert_json(state_dir.join(r), "mcp", true);
            }
        }
        "codex" => {
            let p = state_dir.join("config.toml");
            let updated = upsert_codex_toml_servers(
                &std::fs::read_to_string(&p).unwrap_or_default(),
                servers,
            );
            let _ = anyagents_harness::atomic_write_nonempty_file(&p, updated.as_bytes(), 0o600);
        }
        "omp" => upsert_json(state_dir.join(".omp/agent/mcp.json"), "mcpServers", false),
        _ => {}
    }
}

fn strip_toml_table(text: &str, header: &str) -> String {
    let mut skip = false;
    let kept: Vec<&str> = text
        .lines()
        .filter(|l| {
            let t = l.trim();
            if t.starts_with('[') && t.ends_with(']') {
                skip = t == header;
            }
            !skip
        })
        .collect();
    if kept.is_empty() {
        String::new()
    } else {
        format!("{}\n", kept.join("\n"))
    }
}

fn write_json_atomic(path: &Path, val: &Value) {
    if let Some(p) = path.parent() {
        let _ = std::fs::create_dir_all(p);
    }
    if let Ok(mut b) = serde_json::to_vec_pretty(val) {
        b.push(b'\n');
        let _ = anyagents_harness::atomic_write_nonempty_file(path, &b, 0o600);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_local_stdio_servers_across_all_harnesses_idempotently() {
        let tmp = tempfile::tempdir().unwrap();
        let (bin, state) = (tmp.path().join("bin"), tmp.path().join("state"));
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::create_dir_all(&state).unwrap();
        for b in [
            "anyagents-mcp-browser",
            "anyagents-mcp-web",
            "agent-code-mode",
            "anyagents-guest",
        ] {
            std::fs::write(bin.join(b), "#!/bin/sh\n").unwrap();
        }
        let env = vec![
            ("TAVILY_API_KEY".into(), "tv-key".into()),
            (
                "ANYAGENTS_BROWSER_PROXY".into(),
                "http://proxy.example.invalid:8080".into(),
            ),
            (
                "ANYAGENTS_BROWSER_PROXY_US".into(),
                "socks5://us.example.invalid:1080".into(),
            ),
            (
                "ANYAGENTS_AGENT_DATA_DIR".into(),
                tmp.path().display().to_string(),
            ),
        ];
        let s = discover_in(
            Some(&state),
            Some(tmp.path()),
            &env,
            Some("https://api.example.invalid"),
            Some("agt_1"),
            Some("tok-1"),
            Some(&bin),
        );
        assert_eq!(s.len(), 3);
        for h in ["agy", "claude-code", "spiron", "opencode", "codex"] {
            apply_servers(h, &state, &s);
            apply_servers(h, &state, &s);
        }
        let agy: Value = serde_json::from_str(
            &std::fs::read_to_string(state.join(".gemini/config/mcp_config.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            agy["mcpServers"]["browser"]["env"]["ANYAGENTS_AGENT_TOKEN"],
            "tok-1"
        );
        assert_eq!(
            agy["mcpServers"]["browser"]["env"]["ANYAGENTS_BROWSER_PROXY"],
            "http://proxy.example.invalid:8080"
        );
        assert_eq!(
            agy["mcpServers"]["browser"]["env"]["ANYAGENTS_BROWSER_PROXY_US"],
            "socks5://us.example.invalid:1080"
        );
        assert_eq!(agy["mcpServers"]["web"]["env"]["TAVILY_API_KEY"], "tv-key");
        assert_eq!(agy["mcpServers"]["code"]["args"][0], "code-mcp");
        let oc: Value =
            serde_json::from_str(&std::fs::read_to_string(state.join("opencode.json")).unwrap())
                .unwrap();
        assert_eq!(oc["mcp"]["browser"]["type"], "local");
        assert_eq!(oc["mcp"]["web"]["environment"]["TAVILY_API_KEY"], "tv-key");
        let toml = std::fs::read_to_string(state.join("config.toml")).unwrap();
        assert_eq!(toml.matches("[mcp_servers.browser]").count(), 1);
        assert_eq!(toml.matches("[mcp_servers.code]").count(), 1);
    }
}
