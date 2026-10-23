//! Per-agent MCP registration. Claude Code reads `.claude.json`. Codex reads
//! `config.toml`. Both files live in the agent's own config directory.

use std::path::Path;

use anyagents_core::Error;

pub fn write_claude(path: &Path, command: &Path, args: &[String]) -> Result<(), Error> {
    let mut root = read_json(path)?;
    let servers = root
        .as_object_mut()
        .ok_or_else(|| Error::Message("claude config is not an object".into()))?
        .entry("mcpServers")
        .or_insert_with(|| serde_json::json!({}));
    if !servers.is_object() {
        *servers = serde_json::json!({});
    }
    let Some(servers) = servers.as_object_mut() else {
        return Err(Error::Message("mcpServers is not an object".into()));
    };
    servers.insert(
        "anyagents".to_string(),
        serde_json::json!({
            "type": "stdio",
            "command": command.display().to_string(),
            "args": args,
            "env": {},
        }),
    );
    let bytes = serde_json::to_vec_pretty(&root).map_err(|err| Error::Message(err.to_string()))?;
    std::fs::write(path, bytes)?;
    Ok(())
}

#[allow(dead_code)]
pub fn write_codex(path: &Path, command: &Path, args: &[String]) -> Result<(), Error> {
    write_codex_with_model(path, None, command, args)
}

pub fn write_codex_with_model(
    path: &Path,
    model: Option<&str>,
    command: &Path,
    args: &[String],
) -> Result<(), Error> {
    let body = format!(
        "[mcp_servers.anyagents]\ncommand = \"{}\"\nargs = {}\n",
        toml_escape(&command.display().to_string()),
        toml_args(args),
    );
    let mut existing = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(err) => return Err(err.into()),
    };
    if let Some(raw_model) = model.map(str::trim).filter(|m| !m.is_empty()) {
        let normalized = raw_model
            .strip_prefix("sylphx/")
            .unwrap_or(raw_model)
            .strip_prefix("openai/")
            .unwrap_or(raw_model)
            .split('#')
            .next()
            .unwrap_or(raw_model);
        existing = upsert_top_key(
            &existing,
            "model",
            &format!("\"{}\"", toml_escape(normalized)),
        );
    }
    if !existing.contains("approval_policy") {
        existing = upsert_top_key(&existing, "approval_policy", "\"never\"");
    }
    if !existing.contains("sandbox_mode") {
        existing = upsert_top_key(&existing, "sandbox_mode", "\"danger-full-access\"");
    }
    let mut updated = upsert_table(&existing, &body);
    if !updated.contains("[projects.\"/\"]") {
        if !updated.ends_with('\n') {
            updated.push('\n');
        }
        updated.push_str("\n[projects.\"/\"]\ntrust_level = \"trusted\"\n");
    }
    std::fs::write(path, updated)?;
    Ok(())
}

fn upsert_top_key(text: &str, key: &str, value_literal: &str) -> String {
    let prefix = format!("{key} =");
    let mut lines: Vec<String> = Vec::new();
    let mut replaced = false;
    let mut in_section = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            if !in_section && !replaced {
                lines.push(format!("{key} = {value_literal}"));
                replaced = true;
            }
            in_section = true;
        }
        if !in_section && (trimmed.starts_with(&prefix) || trimmed.starts_with(&format!("{key}=")))
        {
            if !replaced {
                lines.push(format!("{key} = {value_literal}"));
                replaced = true;
            }
            continue;
        }
        lines.push(line.to_string());
    }
    if !replaced {
        lines.insert(0, format!("{key} = {value_literal}"));
    }
    let mut out = lines.join("\n");
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

fn read_json(path: &Path) -> Result<serde_json::Value, Error> {
    match std::fs::read(path) {
        Ok(bytes) if bytes.is_empty() => Ok(serde_json::json!({})),
        Ok(bytes) => serde_json::from_slice(&bytes).map_err(|err| Error::Message(err.to_string())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(serde_json::json!({})),
        Err(err) => Err(err.into()),
    }
}

fn upsert_table(text: &str, body: &str) -> String {
    let header = "[mcp_servers.anyagents]";
    let Some(start) = text.find(header) else {
        let mut out = text.to_string();
        if !out.is_empty() && !out.ends_with('\n') {
            out.push('\n');
        }
        out.push_str(body);
        return out;
    };
    let after = start + header.len();
    let end = text[after..]
        .find("\n[")
        .map(|index| after + index + 1)
        .unwrap_or(text.len());
    format!("{}{body}{}", &text[..start], &text[end..])
}

fn toml_args(args: &[String]) -> String {
    let parts: Vec<String> = args
        .iter()
        .map(|arg| format!("\"{}\"", toml_escape(arg)))
        .collect();
    format!("[{}]", parts.join(", "))
}

fn toml_escape(text: &str) -> String {
    text.replace('\\', "\\\\").replace('"', "\\\"")
}
