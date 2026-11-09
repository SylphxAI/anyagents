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
    let mut root = crate::mcp_config::read_candidates(&candidates)?;
    if root.get("provider").is_none()
        && let Some(donor) = donor_provider_block(dir)?
    {
        root["provider"] = donor;
    }
    let has_sylphx_provider = root.get("provider").and_then(|p| p.get("sylphx")).is_some();
    if let Some(sylphx_opts) = root
        .get_mut("provider")
        .and_then(|p| p.get_mut("sylphx"))
        .and_then(|s| s.get_mut("options"))
        && let Some(url) = sylphx_opts
            .get("baseURL")
            .and_then(serde_json::Value::as_str)
        && (url.contains("models.sylphx.ai") || url.contains("api.sylphx.ai"))
    {
        sylphx_opts["baseURL"] = serde_json::Value::String("https://api.sylphx.com/v1".to_string());
    }
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
        for tool in ["task", "webfetch", "websearch"] {
            root["tools"][tool] = serde_json::Value::Bool(false);
        }
        if let Some(build) = root.get_mut("agent").and_then(|a| a.get_mut("build"))
            && build.is_object()
        {
            if !build.get("tools").is_some_and(|v| v.is_object()) {
                build["tools"] = serde_json::json!({});
            }
            for tool in ["task", "webfetch", "websearch"] {
                build["tools"][tool] = serde_json::Value::Bool(false);
            }
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
    write_atomic(&dir.join("opencode.json"), &bytes)?;
    let xdg_opencode_dir = dir.join("opencode");
    std::fs::create_dir_all(&xdg_opencode_dir)?;
    write_atomic(&xdg_opencode_dir.join("opencode.json"), &bytes)?;
    let home_opencode_dir = dir.join(".config").join("opencode");
    std::fs::create_dir_all(&home_opencode_dir)?;
    write_atomic(&home_opencode_dir.join("opencode.json"), &bytes)?;
    Ok(())
}

static NEXT_TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let seq = NEXT_TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let pid = std::process::id();
    let tmp = path.with_file_name(format!(
        ".{}.{pid}.{seq}.anyagents-new",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("opencode.json")
    ));
    std::fs::write(&tmp, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o664));
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn donor_provider_block(dir: &Path) -> Result<Option<serde_json::Value>, Error> {
    let mut candidates = Vec::new();
    if let Some(data_dir) = dir.parent().and_then(Path::parent) {
        candidates.push(
            data_dir
                .join("profiles")
                .join("sylphx")
                .join("default")
                .join(".config")
                .join("opencode")
                .join("opencode.json"),
        );
        candidates.push(data_dir.join("opencode-provider.json"));
    }
    for path in candidates {
        let val = crate::mcp_config::read_candidates(&[path])?;
        if let Some(provider) = val.get("provider").cloned() {
            return Ok(Some(provider));
        }
    }
    Ok(None)
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
        for tools in [&parsed["tools"], &parsed["agent"]["build"]["tools"]] {
            for disabled in ["task", "webfetch", "websearch"] {
                assert_eq!(
                    tools.get(disabled).and_then(serde_json::Value::as_bool),
                    std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS")
                        .is_none()
                        .then_some(false),
                    "{disabled}: {tools}"
                );
            }
            for tool in [
                "read",
                "write",
                "edit",
                "multiedit",
                "patch",
                "glob",
                "grep",
                "list",
                "todowrite",
                "todoread",
            ] {
                assert!(tools.get(tool).is_none(), "{tool}: {tools}");
            }
        }
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
    #[test]
    fn invalid_existing_candidates_never_overwrite_valid_or_invalid_aliases() {
        for bad in [
            "opencode.json",
            "opencode/opencode.json",
            ".config/opencode/opencode.json",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let selected = dir.path().join("opencode.json");
            std::fs::create_dir_all(selected.parent().unwrap()).unwrap();
            std::fs::write(&selected, br#"{"custom":"keep"}"#).unwrap();
            let malformed = dir.path().join(bad);
            std::fs::create_dir_all(malformed.parent().unwrap()).unwrap();
            std::fs::write(&malformed, b"{broken").unwrap();
            let before = std::fs::read(&selected).unwrap();
            assert!(write(dir.path(), None, Path::new("/opt/bin/anyagents"), &[]).is_err());
            assert_eq!(std::fs::read(&selected).unwrap(), before);
            assert_eq!(std::fs::read(&malformed).unwrap(), b"{broken");
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("opencode.json")).unwrap();
        assert!(write(dir.path(), None, Path::new("/opt/bin/anyagents"), &[]).is_err());
    }

    #[test]
    fn absent_config_initializes_and_keeps_valid_custom_data() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), None, Path::new("/opt/bin/anyagents"), &[]).unwrap();
        let path = dir.path().join("opencode/opencode.json");
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["custom"] = serde_json::json!("keep");
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        write(dir.path(), None, Path::new("/opt/bin/anyagents"), &[]).unwrap();
        let after: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(after["custom"], "keep");
    }
}
