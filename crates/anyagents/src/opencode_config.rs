//! OpenCode config for one agent. It lives in the agent's config directory,
//! which the server hands the child as `XDG_CONFIG_HOME`. It names the model and
//! registers the AnyAgents MCP server. Credentials stay in the user's OpenCode
//! state and are inherited; nothing here copies a provider or a secret.

use std::path::Path;

use anyagents_core::Error;

/// Write `opencode.json` and `opencode/opencode.json` in `dir`. When `XDG_CONFIG_HOME`
/// is set to `dir`, OpenCode v2.0.29 reads `$XDG_CONFIG_HOME/opencode/opencode.json`
/// (`<dir>/opencode/opencode.json`). Any existing `provider`, `agent`, or `$schema`
/// entries seeded from a donor seat are preserved instead of overwritten.
pub fn write(
    dir: &Path,
    model: Option<&str>,
    command: &Path,
    args: &[String],
) -> Result<(), Error> {
    let candidates = [
        dir.join("opencode").join("opencode.json"),
        dir.join(".config").join("opencode").join("opencode.json"),
        dir.join("opencode.json"),
    ];
    let mut root = candidates
        .iter()
        .find_map(|path| {
            let bytes = std::fs::read(path).ok()?;
            let val: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
            val.get("provider").is_some().then_some(val)
        })
        .or_else(|| {
            candidates.iter().find_map(|path| {
                let bytes = std::fs::read(path).ok()?;
                serde_json::from_slice::<serde_json::Value>(&bytes).ok()
            })
        })
        .unwrap_or_else(|| serde_json::json!({}));
    if !root.is_object() {
        root = serde_json::json!({});
    }
    if root.get("provider").is_none()
        && let Some(donor) = donor_provider_block(dir)
    {
        root["provider"] = donor;
    }
    let has_sylphx_provider = root.get("provider").and_then(|p| p.get("sylphx")).is_some();
    if let Some(raw_model) = model.map(str::trim).filter(|m| !m.is_empty()) {
        let normalized = normalize_model(raw_model, has_sylphx_provider);
        let (base_model, variant) = match normalized.split_once('#') {
            Some((b, v)) if !b.is_empty() && !v.is_empty() => (b.to_string(), Some(v.to_string())),
            _ => (normalized, None),
        };
        root["model"] = serde_json::Value::String(base_model.clone());
        let mut build_agent = serde_json::json!({
            "model": base_model,
        });
        if let Some(v) = variant {
            build_agent["variant"] = serde_json::Value::String(v);
        }
        root["agent"] = serde_json::json!({
            "build": build_agent,
        });
    } else if root.get("model").is_none() && has_sylphx_provider {
        root["model"] = serde_json::Value::String("sylphx/openai/gpt-6.1-sol".to_string());
        root["agent"] = serde_json::json!({
            "build": {
                "model": "sylphx/openai/gpt-6.1-sol",
                "variant": "xhigh",
            }
        });
    }
    if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none() {
        if !root.get("tools").is_some_and(|v| v.is_object()) {
            root["tools"] = serde_json::json!({});
        }
        root["tools"]["task"] = serde_json::Value::Bool(false);
        if let Some(build) = root.get_mut("agent").and_then(|a| a.get_mut("build"))
            && build.is_object()
        {
            if !build.get("tools").is_some_and(|v| v.is_object()) {
                build["tools"] = serde_json::json!({});
            }
            build["tools"]["task"] = serde_json::Value::Bool(false);
        }
    }
    root["mcp"] = serde_json::json!({
        "anyagents": {
            "type": "local",
            "command": command_args(command, args),
            "enabled": true,
        }
    });
    let bytes = serde_json::to_vec_pretty(&root).map_err(|err| Error::Message(err.to_string()))?;
    std::fs::write(dir.join("opencode.json"), &bytes)?;
    let xdg_opencode_dir = dir.join("opencode");
    std::fs::create_dir_all(&xdg_opencode_dir)?;
    std::fs::write(xdg_opencode_dir.join("opencode.json"), &bytes)?;
    let home_opencode_dir = dir.join(".config").join("opencode");
    std::fs::create_dir_all(&home_opencode_dir)?;
    std::fs::write(home_opencode_dir.join("opencode.json"), &bytes)?;
    Ok(())
}

fn donor_provider_block(dir: &Path) -> Option<serde_json::Value> {
    let mut candidates = Vec::new();
    if let Some(data_dir) = dir.parent().and_then(Path::parent) {
        candidates.push(data_dir.join("opencode-provider.json"));
    }
    if let Some(host_home) = crate::sandbox::host_home_dir() {
        candidates.push(
            host_home
                .join(".config")
                .join("opencode")
                .join("opencode.json"),
        );
    }
    for path in candidates {
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let Ok(val) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            continue;
        };
        if let Some(provider) = val.get("provider").cloned() {
            return Some(provider);
        }
    }
    None
}

fn normalize_model(raw: &str, has_sylphx_provider: bool) -> String {
    if !has_sylphx_provider {
        return raw.to_string();
    }
    if raw.starts_with("sylphx/")
        || raw.starts_with("minimax-code-plan/")
        || raw.starts_with("kimi-code-plan/")
        || raw.starts_with("glm-code-plan/")
        || raw.starts_with("qwen-token-plan/")
    {
        return raw.to_string();
    }
    if raw.starts_with("openai/")
        || raw.starts_with("anthropic/")
        || raw.starts_with("google/")
        || raw.starts_with("deepseek/")
        || raw.starts_with("x-ai/")
        || raw.starts_with("xiaomi/")
    {
        return format!("sylphx/{raw}");
    }
    if raw.starts_with("gpt-") {
        return format!("sylphx/openai/{raw}");
    }
    raw.to_string()
}

fn command_args(command: &Path, args: &[String]) -> Vec<String> {
    let mut all = Vec::with_capacity(args.len() + 1);
    all.push(command.display().to_string());
    all.extend(args.iter().cloned());
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_model_and_the_mcp_server_are_written() {
        let dir = tempfile::tempdir().unwrap();
        let args = vec![
            "--data".to_string(),
            "/tmp/d".to_string(),
            "mcp".to_string(),
        ];
        write(
            dir.path(),
            Some("sylphx/deepseek"),
            Path::new("/bin/anyagents"),
            &args,
        )
        .unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("opencode.json")).unwrap())
                .unwrap();
        assert_eq!(parsed["model"], "sylphx/deepseek");
        let server = &parsed["mcp"]["anyagents"];
        assert_eq!(server["type"], "local");
        assert_eq!(server["command"][0], "/bin/anyagents");
        assert_eq!(server["command"][3], "mcp");
    }

    #[test]
    fn no_model_leaves_the_model_out() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), None, Path::new("/bin/anyagents"), &[]).unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("opencode.json")).unwrap())
                .unwrap();
        assert!(parsed.get("model").is_none(), "{parsed}");
    }
}
