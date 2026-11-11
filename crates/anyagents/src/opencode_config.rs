//! OpenCode config for one agent. It lives in the agent's config directory,
//! which the server hands the child as `XDG_CONFIG_HOME`. It names the model,
//! validates that any bound provider profile maps cleanly to OpenCode (failing
//! closed instead of silently falling back to an unauthenticated free tier),
//! and registers the AnyAgents MCP server.

use std::path::{Path, PathBuf};

use anyagents_core::Error;

/// Write `opencode.json`, `opencode/opencode.json`, and `.config/opencode/opencode.json` in `dir`.
///
/// When `.anyagents-profile` binds this seat to a non-`opencode` provider profile,
/// `write` fails closed unless a concrete OpenCode mapping (donor `provider` block,
/// OpenCode `auth.json`, provider `.key` file, or explicit gateway `model`) exists.
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
    let bound_profile = read_bound_profile(dir);
    let profile_dir = resolve_bound_profile_dir(dir, bound_profile.as_deref());

    if root.get("provider").is_none()
        && let Some(donor) = donor_provider_block(dir, profile_dir.as_deref())?
    {
        root["provider"] = donor;
    }

    if root.get("provider").is_none()
        && let Some(synthesized) = synthesize_provider_from_profile_mapping(
            dir,
            profile_dir.as_deref(),
            bound_profile.as_deref(),
            model,
        )
    {
        root["provider"] = synthesized;
    }

    if let Some(ref bound) = bound_profile {
        let (prov, _) = bound.split_once('/').unwrap_or((bound.as_str(), "default"));
        if prov != "opencode" {
            let has_provider_block = root
                .get("provider")
                .and_then(serde_json::Value::as_object)
                .is_some_and(|m| !m.is_empty());
            let has_opencode_auth = has_opencode_native_auth(dir)
                || profile_dir.as_deref().is_some_and(has_opencode_native_auth);
            if !has_provider_block && !has_opencode_auth {
                return Err(Error::Message(format!(
                    "provider profile `{bound}` has no OpenCode provider or credential mapping; refusing to write unmapped opencode.json (fail-closed)"
                )));
            }
        }
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

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    anyagents_harness::atomic_write_nonempty_file(path, bytes, 0o664)?;
    Ok(())
}

fn read_bound_profile(dir: &Path) -> Option<String> {
    std::fs::read_to_string(dir.join(".anyagents-profile"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn resolve_data_dir_from_seat(dir: &Path) -> Option<PathBuf> {
    if dir.file_name().and_then(|n| n.to_str()) == Some("config") {
        dir.parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .map(Path::to_path_buf)
    } else {
        dir.parent().and_then(Path::parent).map(Path::to_path_buf)
    }
}

fn resolve_bound_profile_dir(dir: &Path, bound_profile: Option<&str>) -> Option<PathBuf> {
    let bound = bound_profile?;
    let (prov, prof) = bound.split_once('/').unwrap_or((bound, "default"));
    let data_dir = resolve_data_dir_from_seat(dir)?;
    Some(data_dir.join("profiles").join(prov).join(prof))
}

fn has_opencode_native_auth(base: &Path) -> bool {
    for rel in [
        "data-home/opencode/auth.json",
        ".local/share/opencode/auth.json",
    ] {
        if let Ok(raw) = std::fs::read_to_string(base.join(rel))
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
            && val.as_object().is_some_and(|m| !m.is_empty())
        {
            return true;
        }
    }
    false
}

fn donor_provider_block(
    dir: &Path,
    profile_dir: Option<&Path>,
) -> Result<Option<serde_json::Value>, Error> {
    let mut candidates = Vec::new();
    if let Some(pdir) = profile_dir {
        candidates.push(pdir.join("opencode").join("opencode.json"));
        candidates.push(pdir.join(".config").join("opencode").join("opencode.json"));
        candidates.push(pdir.join("opencode.json"));
    }
    if let Some(data_dir) = resolve_data_dir_from_seat(dir) {
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

fn read_nonempty_key(base: &Path, rel: &str) -> Option<String> {
    std::fs::read_to_string(base.join(rel))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn synthesize_provider_from_profile_mapping(
    dir: &Path,
    profile_dir: Option<&Path>,
    bound_profile: Option<&str>,
    model: Option<&str>,
) -> Option<serde_json::Value> {
    let bound = bound_profile?;
    let (prov, _) = bound.split_once('/').unwrap_or((bound, "default"));
    let key_candidates: Vec<String> = match prov {
        "sylphx" => vec![
            ".config/sylphx/key".to_string(),
            ".config/opencode/sylphx.key".to_string(),
        ],
        "qwen-token-plan" => vec![
            ".config/opencode/qwen-coding-plan.key".to_string(),
            ".config/opencode/qwen-token-plan.key".to_string(),
        ],
        other => vec![format!(".config/opencode/{other}.key")],
    };
    let api_key = key_candidates.iter().find_map(|rel| {
        read_nonempty_key(dir, rel)
            .or_else(|| profile_dir.and_then(|pd| read_nonempty_key(pd, rel)))
    });
    let explicit_sylphx_model = prov == "sylphx"
        && model
            .map(str::trim)
            .is_some_and(|m| m.starts_with("sylphx/"));
    if api_key.is_none() && !explicit_sylphx_model {
        return None;
    }
    let mut options = serde_json::Map::new();
    if prov == "sylphx" {
        options.insert(
            "baseURL".to_string(),
            serde_json::Value::String("https://api.sylphx.com/v1".to_string()),
        );
    } else if prov == "openrouter" {
        options.insert(
            "baseURL".to_string(),
            serde_json::Value::String("https://openrouter.ai/api/v1".to_string()),
        );
    }
    if let Some(k) = api_key {
        options.insert("apiKey".to_string(), serde_json::Value::String(k));
    }
    Some(serde_json::json!({
        prov: {
            "npm": "@ai-sdk/openai-compatible",
            "options": options,
        }
    }))
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
    #[test]
    fn write_fails_closed_when_bound_profile_has_no_opencode_mapping() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let agy_prof = data.join("profiles/google-antigravity/default/.gemini/antigravity-cli");
        std::fs::create_dir_all(&agy_prof).unwrap();
        std::fs::write(agy_prof.join("antigravity-oauth-token"), "oauth-only").unwrap();

        let seat_cfg = data.join("agents/agt_test/config");
        std::fs::create_dir_all(&seat_cfg).unwrap();
        std::fs::write(
            seat_cfg.join(".anyagents-profile"),
            "google-antigravity/default\n",
        )
        .unwrap();

        let err = write(
            &seat_cfg,
            None,
            Path::new("/bin/anyagents"),
            &["mcp".into()],
        )
        .expect_err(
            "unmapped subscription profile must fail closed instead of writing empty opencode.json",
        );
        assert!(err.to_string().contains("fail-closed"), "{err}");
        assert!(!seat_cfg.join("opencode.json").exists());
    }

    #[test]
    fn write_reads_donor_provider_from_bound_profile_at_agents_id_config_depth() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let prof = data.join("profiles/openrouter/team/.config/opencode");
        std::fs::create_dir_all(&prof).unwrap();
        std::fs::write(
            prof.join("opencode.json"),
            r#"{"provider":{"openrouter":{"options":{"baseURL":"https://openrouter.ai/api/v1","apiKey":"sk-or-1"}}}}"#,
        )
        .unwrap();

        let seat_cfg = data.join("agents/agt_or/config");
        std::fs::create_dir_all(&seat_cfg).unwrap();
        std::fs::write(seat_cfg.join(".anyagents-profile"), "openrouter/team\n").unwrap();

        write(
            &seat_cfg,
            Some("openrouter/anthropic/claude-sonnet-4.6"),
            Path::new("/bin/anyagents"),
            &["mcp".into()],
        )
        .unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(seat_cfg.join("opencode.json")).unwrap())
                .unwrap();
        assert_eq!(
            parsed["provider"]["openrouter"]["options"]["apiKey"],
            "sk-or-1"
        );
        assert_eq!(parsed["model"], "openrouter/anthropic/claude-sonnet-4.6");
    }
}
