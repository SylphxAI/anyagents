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
        Err(err)
            if matches!(
                err.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            String::new()
        }
        Err(err) => return Err(err.into()),
    };
    let config_dir = path.parent();
    let profile_is_sylphx = config_dir
        .and_then(|dir| std::fs::read_to_string(dir.join(".anyagents-profile")).ok())
        .is_some_and(|s| {
            let trimmed = s.trim();
            trimmed == "sylphx" || trimmed.starts_with("sylphx/")
        });
    let model_is_sylphx = model
        .map(str::trim)
        .is_some_and(|m| m.starts_with("sylphx/"));
    let use_sylphx = profile_is_sylphx || model_is_sylphx;

    if let Some(raw_model) = model.map(str::trim).filter(|m| !m.is_empty()) {
        let after_sylphx = raw_model.strip_prefix("sylphx/").unwrap_or(raw_model);
        let stripped = after_sylphx.strip_prefix("openai/").unwrap_or(after_sylphx);
        let (normalized, effort) = match stripped.split_once('#') {
            Some((b, e)) if !b.trim().is_empty() && !e.trim().is_empty() => {
                (b.trim(), Some(e.trim()))
            }
            _ => (stripped.split('#').next().unwrap_or(raw_model), None),
        };
        existing = upsert_top_key(
            &existing,
            "model",
            &format!("\"{}\"", toml_escape(normalized)),
        );
        if let Some(eff) = effort {
            existing = upsert_top_key(
                &existing,
                "model_reasoning_effort",
                &format!("\"{}\"", toml_escape(eff)),
            );
        }
    }
    if use_sylphx {
        existing = upsert_top_key(&existing, "model_provider", "\"sylphx\"");
    }
    if !existing.contains("approval_policy") {
        existing = upsert_top_key(&existing, "approval_policy", "\"never\"");
    }
    if !existing.contains("sandbox_mode") {
        existing = upsert_top_key(&existing, "sandbox_mode", "\"danger-full-access\"");
    }
    let mut updated = upsert_table(&existing, &body);
    if use_sylphx {
        let (base_url, api_key) = resolve_sylphx_codex_provider(config_dir);
        let provider_table = match api_key {
            Some(key) if !key.is_empty() => format!(
                "[model_providers.sylphx]\nname = \"Sylphx AI\"\nbase_url = \"{}\"\nexperimental_bearer_token = \"{}\"\n",
                toml_escape(&base_url),
                toml_escape(&key),
            ),
            _ => format!(
                "[model_providers.sylphx]\nname = \"Sylphx AI\"\nbase_url = \"{}\"\nenv_key = \"SYLPHX_API_KEY\"\n",
                toml_escape(&base_url),
            ),
        };
        updated = upsert_named_table(&updated, "[model_providers.sylphx]", &provider_table);
    }
    if !updated.contains("[projects.\"/\"]") {
        if !updated.ends_with('\n') {
            updated.push('\n');
        }
        updated.push_str("\n[projects.\"/\"]\ntrust_level = \"trusted\"\n");
    }
    if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none()
        && !updated.contains("[features]")
    {
        if !updated.ends_with('\n') {
            updated.push('\n');
        }
        updated.push_str("\n[features]\nmulti_agent = false\n");
    }
    let tmp_path = path.with_extension("toml.new");
    std::fs::write(&tmp_path, &updated)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o664));
    }
    if std::fs::rename(&tmp_path, path).is_err() {
        let _ = std::fs::remove_file(path);
        std::fs::rename(&tmp_path, path)?;
    }
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
    upsert_named_table(text, "[mcp_servers.anyagents]", body)
}

fn upsert_named_table(text: &str, header: &str, body: &str) -> String {
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

fn resolve_sylphx_codex_provider(config_dir: Option<&Path>) -> (String, Option<String>) {
    let mut base_url = "https://api.models.sylphx.ai/v1".to_string();
    let mut key_candidates = Vec::new();
    if let Some(dir) = config_dir {
        for opencode_rel in [
            ".config/opencode/opencode.json",
            "opencode/opencode.json",
            "opencode.json",
        ] {
            if let Ok(text) = std::fs::read_to_string(dir.join(opencode_rel))
                && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                && let Some(url) = val
                    .pointer("/provider/sylphx/options/baseURL")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|u| !u.is_empty())
            {
                base_url = url.to_string();
                break;
            }
        }
        key_candidates.push(dir.join(".config/opencode/sylphx.key"));
        key_candidates.push(dir.join(".config/desk/sylphx-ai/key"));
        if let Some(data_dir) = dir.parent().and_then(Path::parent).and_then(Path::parent) {
            let prof_name = std::fs::read_to_string(dir.join(".anyagents-profile"))
                .ok()
                .and_then(|s| {
                    let t = s.trim();
                    t.strip_prefix("sylphx/").map(str::to_string)
                })
                .unwrap_or_else(|| "default".to_string());
            let prof_dir = data_dir.join("profiles").join("sylphx").join(&prof_name);
            key_candidates.push(prof_dir.join(".config/opencode/sylphx.key"));
            key_candidates.push(prof_dir.join(".config/desk/sylphx-ai/key"));
        }
    }
    if let Some(home) = crate::sandbox::host_home_dir() {
        key_candidates.push(home.join(".config/opencode/sylphx.key"));
        key_candidates.push(home.join(".config/desk/sylphx-ai/key"));
    }
    let api_key = key_candidates.into_iter().find_map(|p| {
        std::fs::read_to_string(p)
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    });
    (base_url, api_key)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_config_writes_native_sylphx_provider_and_reasoning_effort() {
        let dir = tempfile::tempdir().unwrap();
        let key_dir = dir.path().join(".config").join("desk").join("sylphx-ai");
        std::fs::create_dir_all(&key_dir).unwrap();
        std::fs::write(key_dir.join("key"), "test-sylphx-key\n").unwrap();
        let cfg_path = dir.path().join("config.toml");
        write_codex_with_model(
            &cfg_path,
            Some("sylphx/openai/gpt-6.1-sol#xhigh"),
            Path::new("/usr/local/bin/anyagents"),
            &["mcp".to_string()],
        )
        .unwrap();
        let text = std::fs::read_to_string(&cfg_path).unwrap();
        assert!(text.contains("model_provider = \"sylphx\""), "{text}");
        assert!(text.contains("model = \"gpt-6.1-sol\""), "{text}");
        assert!(
            text.contains("model_reasoning_effort = \"xhigh\""),
            "{text}"
        );
        assert!(text.contains("[model_providers.sylphx]"), "{text}");
        assert!(
            text.contains("base_url = \"https://api.models.sylphx.ai/v1\""),
            "{text}"
        );
        assert!(
            text.contains("experimental_bearer_token = \"test-sylphx-key\""),
            "{text}"
        );
    }
}
