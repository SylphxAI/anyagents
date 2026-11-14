//! Per-agent MCP registration. Claude Code reads `.claude.json`. Codex reads
//! `config.toml`. Both files live in the agent's own config directory.

use std::path::{Path, PathBuf};

use anyagents_core::Error;
use anyagents_core::agent_tree::AgentTree;

/// The largest harness config file the server reads to merge.
const CONFIG_LIMIT: u64 = 4 * 1024 * 1024;

/// `path`, below `root`, a directory the server chose (an agent's config
/// directory): read with no link at any component below `root`, at most
/// [`CONFIG_LIMIT`] bytes; `None` when absent. The agent's seat writes that
/// tree, so a link there never makes the server read a file of its own into
/// a config the seat then reads back (ADR 0010 D3).
pub(crate) fn read_below(root: &Path, path: &Path) -> Result<Option<Vec<u8>>, Error> {
    let relative = path.strip_prefix(root).map_err(|_| {
        Error::Message(format!(
            "{} is not below {}",
            path.display(),
            root.display()
        ))
    })?;
    AgentTree::open(root)?
        .read_optional(relative, CONFIG_LIMIT)
        .map_err(|err| Error::Message(format!("{}: {err}", path.display())))
}

/// Replace `path` below `root` with `bytes` at `mode`, creating the
/// directories above it; nothing below `root` is reached through a link.
pub(crate) fn write_below(root: &Path, path: &Path, bytes: &[u8], mode: u32) -> Result<(), Error> {
    if bytes.is_empty() {
        return Err(Error::Message(format!(
            "refusing to write an empty {}",
            path.display()
        )));
    }
    let relative = path.strip_prefix(root).map_err(|_| {
        Error::Message(format!(
            "{} is not below {}",
            path.display(),
            root.display()
        ))
    })?;
    // fs-ok: the config directory itself, which the server made and chose
    std::fs::create_dir_all(root)?;
    AgentTree::open(root)?
        .write(relative, bytes, mode, 0o777)
        .map_err(|err| Error::Message(format!("{}: {err}", path.display())))
}

/// The first of `paths` (each below `root`, the agent's config directory)
/// that exists, as a JSON object; read as [`read_below`] reads.
pub(crate) fn read_candidates_below(
    root: &Path,
    paths: &[PathBuf],
) -> Result<serde_json::Value, Error> {
    let mut selected = None;
    for path in paths {
        if let Some(bytes) = read_below(root, path)? {
            let value: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|err| Error::Message(format!("{}: {err}", path.display())))?;
            if !value.is_object() {
                return Err(Error::Message(format!(
                    "{} is not an object",
                    path.display()
                )));
            }
            if selected.is_none() {
                selected = Some(value);
            }
        }
    }
    Ok(selected.unwrap_or_else(|| serde_json::json!({})))
}

/// The first of `paths` that exists, as a JSON object, for files the server
/// owns (a provider profile, the data directory), read as named. Never for a
/// file in an agent's tree: use [`read_candidates_below`].
pub(crate) fn read_server_candidates(paths: &[PathBuf]) -> Result<serde_json::Value, Error> {
    let mut selected = None;
    for path in paths {
        // fs-ok: read_server_candidates reads files the server owns
        match std::fs::read(path) {
            Ok(bytes) => {
                let value: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|err| Error::Message(format!("{}: {err}", path.display())))?;
                if !value.is_object() {
                    return Err(Error::Message(format!(
                        "{} is not an object",
                        path.display()
                    )));
                }
                if selected.is_none() {
                    selected = Some(value);
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => (),
            Err(err) => return Err(err.into()),
        }
    }
    Ok(selected.unwrap_or_else(|| serde_json::json!({})))
}

pub fn write_claude(path: &Path, command: &Path, args: &[String]) -> Result<(), Error> {
    let config_dir = path
        .parent()
        .ok_or_else(|| Error::Message("claude config has no directory".into()))?;
    write_claude_below(config_dir, path, command, args)
}

/// [`write_claude`] for `path` below `config_dir`, the agent's config
/// directory: nothing below it is read or written through a link.
pub fn write_claude_below(
    config_dir: &Path,
    path: &Path,
    command: &Path,
    args: &[String],
) -> Result<(), Error> {
    // A malformed or non-object file of the agent's own is replaced; a link
    // (or anything but a plain file) is refused.
    let mut root = read_below(config_dir, path)?
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .filter(serde_json::Value::is_object)
        .unwrap_or_else(|| serde_json::json!({}));
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
    write_below(config_dir, path, &bytes, 0o664)
}

/// Write a clean, isolated `settings.json` for a `claude-code` agent in `config_dir`,
/// stripping any host-leaked `apiKeyHelper`, `ANTHROPIC_BASE_URL`, legacy hooks,
/// or `claudeMdExcludes` so the agent uses its bound `<provider>/<profile>` cleanly.
pub fn sanitize_claude_settings(config_dir: &Path) -> Result<(), Error> {
    let is_anthropic_oauth = read_text_below(config_dir, &config_dir.join(".anyagents-profile"))
        .map(|s| {
            let t = s.trim();
            t.is_empty() || t == "anthropic" || t.starts_with("anthropic/")
        })
        .unwrap_or(true);
    let settings_path = config_dir.join("settings.json");
    let mut root = read_json(config_dir, &settings_path)?;
    if !root.is_object() {
        return Err(Error::Message("settings.json is not an object".into()));
    }
    if let Some(obj) = root.as_object_mut() {
        obj.remove("hooks");
        obj.remove("claudeMdExcludes");
        if is_anthropic_oauth {
            obj.remove("apiKeyHelper");
            if let Some(env_obj) = obj
                .get_mut("env")
                .and_then(serde_json::Value::as_object_mut)
            {
                env_obj.remove("ANTHROPIC_BASE_URL");
                env_obj.remove("ANTHROPIC_API_KEY");
            }
        }
        let env_val = obj
            .entry("env".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(env_obj) = env_val.as_object_mut() {
            env_obj.insert(
                "CLAUDE_CODE_DISABLE_AUTO_MEMORY".to_string(),
                serde_json::Value::String("1".to_string()),
            );
        }
        obj.insert("autoMemoryEnabled".to_string(), serde_json::json!(false));
        obj.insert("includeCoAuthoredBy".to_string(), serde_json::json!(false));
        obj.insert(
            "attribution".to_string(),
            serde_json::json!({"commit": "", "pr": ""}),
        );
        obj.insert(
            "skipDangerousModePermissionPrompt".to_string(),
            serde_json::json!(true),
        );
        let perms = obj.entry("permissions".to_string()).or_insert_with(|| {
            serde_json::json!({
                "defaultMode": "bypassPermissions",
                "deny": ["SendFeedback"]
            })
        });
        if let Some(perms_obj) = perms.as_object_mut() {
            let deny = perms_obj
                .entry("deny".to_string())
                .or_insert_with(|| serde_json::json!(["SendFeedback"]));
            if let Some(deny_arr) = deny.as_array_mut()
                && std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none()
            {
                for tool in [
                    "SendFeedback",
                    "Task",
                    "Agent",
                    "TeamCreate",
                    "TeamDelete",
                    "SendMessage",
                    "ScheduleWakeup",
                    "Monitor",
                    "CronCreate",
                    "CronDelete",
                    "CronList",
                    "RemoteTrigger",
                    "PushNotification",
                    "WebSearch",
                    "WebFetch",
                ] {
                    if !deny_arr.iter().any(|v| v.as_str() == Some(tool)) {
                        deny_arr.push(serde_json::Value::String(tool.to_string()));
                    }
                }
            }
        }
    }
    let mut bytes =
        serde_json::to_vec_pretty(&root).map_err(|err| Error::Message(err.to_string()))?;
    bytes.push(b'\n');
    write_below(config_dir, &settings_path, &bytes, 0o664)?;
    // Only a regular file of the agent's own; a link there is left alone.
    let _ = AgentTree::open(config_dir)
        .and_then(|tree| tree.remove_file(Path::new(".claude/settings.json")));
    Ok(())
}

/// A small text file below `root`, trimmed of nothing; `None` when absent,
/// unreadable or a link.
fn read_text_below(root: &Path, path: &Path) -> Option<String> {
    read_below(root, path)
        .ok()
        .flatten()
        .and_then(|bytes| String::from_utf8(bytes).ok())
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
    let dir = path
        .parent()
        .ok_or_else(|| Error::Message("codex config has no directory".into()))?;
    let mut existing = read_toml_text(dir, path)?;
    let config_dir = Some(dir);
    let profile_is_sylphx =
        read_text_below(dir, &dir.join(".anyagents-profile")).is_some_and(|s| {
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
    if !existing.contains("project_doc_max_bytes") {
        existing = upsert_top_key(&existing, "project_doc_max_bytes", "131072");
    }
    if !std::env::var("ANYAGENTS_ALLOW_NATIVE_FILE_TOOLS")
        .ok()
        .is_some_and(|v| v.trim() == "1")
    {
        existing = upsert_top_key(&existing, "web_search", "\"disabled\"");
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
    updated = upsert_named_table(
        &updated,
        "[memories]",
        "[memories]\ngenerate_memories = false\nuse_memories = false\n",
    );
    if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none() {
        updated = upsert_named_table(
            &updated,
            "[features]",
            "[features]\napps = false\ncode_mode = false\ncode_mode_only = false\nmulti_agent = false\n",
        );
    }
    write_below(dir, path, updated.as_bytes(), 0o664)
}

pub fn sync_codex_developer_instructions(
    path: &Path,
    instructions: Option<&str>,
) -> Result<(), Error> {
    let dir = path
        .parent()
        .ok_or_else(|| Error::Message("codex config has no directory".into()))?;
    let existing = read_toml_text(dir, path)?;
    let updated = match instructions.map(str::trim).filter(|s| !s.is_empty()) {
        Some(text) => upsert_top_key(
            &existing,
            "developer_instructions",
            &format!("\"{}\"", toml_escape(text)),
        ),
        None => remove_top_key(&existing, "developer_instructions"),
    };
    if updated == existing && read_below(dir, path)?.is_some() {
        return Ok(());
    }
    write_below(dir, path, updated.as_bytes(), 0o664)
}

/// A TOML config below `dir` as text; empty when absent.
fn read_toml_text(dir: &Path, path: &Path) -> Result<String, Error> {
    match read_below(dir, path)? {
        Some(bytes) => String::from_utf8(bytes)
            .map_err(|err| Error::Message(format!("{}: {err}", path.display()))),
        None => Ok(String::new()),
    }
}

fn remove_top_key(text: &str, key: &str) -> String {
    let prefix = format!("{key} =");
    let tight = format!("{key}=");
    let mut lines: Vec<String> = Vec::new();
    let mut in_section = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_section = true;
        }
        if !in_section && (trimmed.starts_with(&prefix) || trimmed.starts_with(&tight)) {
            continue;
        }
        lines.push(line.to_string());
    }
    let mut out = lines.join("\n");
    if !out.ends_with('\n') && !out.is_empty() {
        out.push('\n');
    }
    out
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

fn read_json(root: &Path, path: &Path) -> Result<serde_json::Value, Error> {
    match read_below(root, path)? {
        Some(bytes) => serde_json::from_slice(&bytes)
            .map_err(|err| Error::Message(format!("{}: {err}", path.display()))),
        None => Ok(serde_json::json!({})),
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
    let mut base_url = "https://api.sylphx.com/v1".to_string();
    let mut key_candidates = Vec::new();
    if let Some(dir) = config_dir {
        for opencode_rel in [
            ".config/opencode/opencode.json",
            "opencode/opencode.json",
            "opencode.json",
        ] {
            if let Some(text) = read_text_below(dir, &dir.join(opencode_rel))
                && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                && let Some(url) = val
                    .pointer("/provider/sylphx/options/baseURL")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|u| !u.is_empty())
            {
                base_url = if url.contains("models.sylphx.ai") || url.contains("api.sylphx.ai") {
                    "https://api.sylphx.com/v1".to_string()
                } else {
                    url.to_string()
                };
                break;
            }
        }
        if let Some(data_dir) = dir.parent().and_then(Path::parent).and_then(Path::parent) {
            let prof_name = read_text_below(dir, &dir.join(".anyagents-profile"))
                .and_then(|s| {
                    let t = s.trim();
                    t.strip_prefix("sylphx/").map(str::to_string)
                })
                .unwrap_or_else(|| "default".to_string());
            let prof_dir = data_dir.join("profiles").join("sylphx").join(&prof_name);
            // The provider profile is the server's own: read as named.
            key_candidates.push((None, prof_dir.join(".config/opencode/sylphx.key")));
            key_candidates.push((None, prof_dir.join(".config/sylphx/key")));
        }
        // The agent's own keys: never through a link, which would put a
        // server file into config.toml as the bearer token.
        key_candidates.push((Some(dir), dir.join(".config/opencode/sylphx.key")));
        key_candidates.push((Some(dir), dir.join(".config/sylphx/key")));
    }
    let mut resolved_keys: Vec<String> = key_candidates
        .into_iter()
        .filter_map(|(below, p)| {
            match below {
                Some(root) => read_text_below(root, &p),
                // fs-ok: the provider profile is the server's own
                None => std::fs::read_to_string(p).ok(),
            }
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
        })
        .collect();
    let api_key = resolved_keys
        .iter()
        .find(|k| !k.starts_with("sylphx_sk_"))
        .cloned()
        .or_else(|| resolved_keys.drain(..).next());
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
    let mut out = String::with_capacity(text.len() + 16);
    for ch in text.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_config_writes_native_sylphx_provider_and_reasoning_effort() {
        let dir = tempfile::tempdir().unwrap();
        let key_dir = dir.path().join(".config").join("sylphx");
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
            text.contains("base_url = \"https://api.sylphx.com/v1\""),
            "{text}"
        );
        assert!(
            text.contains("experimental_bearer_token = \"test-sylphx-key\""),
            "{text}"
        );
    }

    #[test]
    fn sanitize_claude_settings_strips_host_gateway_and_hooks_for_oauth_profile() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(".anyagents-profile"),
            "anthropic/secondary\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("settings.json"),
            r#"{
  "apiKeyHelper": "/srv/workspace/example-project/.claude/gateway-key",
  "env": {
    "ANTHROPIC_BASE_URL": "https://api.sylphx.com",
    "OTHER": "1"
  },
  "hooks": {"SessionStart": []},
  "claudeMdExcludes": ["/srv/workspace/example/project/AGENTS.md"]
}"#,
        )
        .unwrap();
        sanitize_claude_settings(dir.path()).unwrap();
        let cleaned: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join("settings.json")).unwrap(),
        )
        .unwrap();
        assert!(cleaned.get("apiKeyHelper").is_none());
        assert!(cleaned.get("hooks").is_none());
        assert!(cleaned.get("claudeMdExcludes").is_none());
        assert!(cleaned["env"].get("ANTHROPIC_BASE_URL").is_none());
        assert_eq!(cleaned["env"]["OTHER"], "1");
        assert_eq!(cleaned["includeCoAuthoredBy"], false);
        assert_eq!(cleaned["skipDangerousModePermissionPrompt"], true);
        let denied = cleaned["permissions"]["deny"].as_array().unwrap();
        for tool in [
            "Read",
            "Write",
            "Edit",
            "MultiEdit",
            "Glob",
            "Grep",
            "LS",
            "NotebookEdit",
            "TodoRead",
            "TodoWrite",
        ] {
            assert!(
                !denied.iter().any(|value| value.as_str() == Some(tool)),
                "{tool}"
            );
        }
        if std::env::var_os("ANYAGENTS_ALLOW_NATIVE_SUBAGENTS").is_none() {
            for tool in [
                "Task",
                "Agent",
                "TeamCreate",
                "TeamDelete",
                "SendMessage",
                "WebSearch",
                "WebFetch",
            ] {
                assert!(
                    denied.iter().any(|value| value.as_str() == Some(tool)),
                    "{tool}"
                );
            }
        }
    }
    #[test]
    fn malformed_settings_and_non_notfound_reads_preserve_existing_bytes() {
        for bytes in [b"{broken".as_slice(), b"[]", &[0xff, 0xfe]] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("settings.json");
            std::fs::write(&path, bytes).unwrap();
            assert!(sanitize_claude_settings(dir.path()).is_err());
            assert_eq!(std::fs::read(path).unwrap(), bytes);
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("settings.json")).unwrap();
        assert!(sanitize_claude_settings(dir.path()).is_err());
        assert!(dir.path().join("settings.json").is_dir());
    }

    #[test]
    fn absent_settings_initialize_and_valid_settings_keep_unmanaged_values() {
        let dir = tempfile::tempdir().unwrap();
        sanitize_claude_settings(dir.path()).unwrap();
        let path = dir.path().join("settings.json");
        std::fs::write(&path, br#"{"theme":"night"}"#).unwrap();
        sanitize_claude_settings(dir.path()).unwrap();
        assert_eq!(read_json(dir.path(), &path).unwrap()["theme"], "night");
    }

    #[test]
    fn write_claude_self_heals_empty_or_corrupted_mcp_config_json() {
        let dir = tempfile::tempdir().unwrap();
        let mcp_path = dir.path().join(".gemini/config/mcp_config.json");
        std::fs::create_dir_all(mcp_path.parent().unwrap()).unwrap();
        for initial in [b"".as_slice(), b"{truncated", b"[]"] {
            std::fs::write(&mcp_path, initial).unwrap();
            write_claude(
                &mcp_path,
                Path::new("/usr/local/bin/anyagents"),
                &["mcp".to_string()],
            )
            .unwrap();
            let parsed = read_json(dir.path(), &mcp_path).unwrap();
            assert_eq!(
                parsed
                    .pointer("/mcpServers/anyagents/command")
                    .and_then(serde_json::Value::as_str),
                Some("/usr/local/bin/anyagents")
            );
        }
    }

    /// The agent's seat writes its config directory. A config file it links
    /// at a server file is never read into the merged config (where the seat
    /// would read it back) nor written through, and its own key file linked
    /// out is never copied into `config.toml` as the bearer token.
    #[cfg(unix)]
    #[test]
    fn planted_links_in_the_config_dir_are_never_read_or_written_through() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("agents/a1/config");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&config).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("secret"), "outside-secret").unwrap();
        let unchanged = || {
            assert_eq!(
                std::fs::read_to_string(outside.join("secret")).unwrap(),
                "outside-secret"
            );
            assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 1);
        };
        let command = Path::new("/usr/local/bin/anyagents");
        let args = ["mcp".to_string()];
        for (rel, write) in [
            ("config.toml", 0),
            (".claude.json", 1),
            ("settings.json", 2),
        ] {
            let path = config.join(rel);
            symlink(outside.join("secret"), &path).unwrap();
            let result = match write {
                0 => write_codex_with_model(&path, None, command, &args),
                1 => write_claude(&path, command, &args),
                _ => sanitize_claude_settings(&config),
            };
            assert!(result.is_err(), "{rel}");
            assert!(
                std::fs::symlink_metadata(&path)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            std::fs::remove_file(&path).unwrap();
            unchanged();
        }
        // A directory link above the file: nothing is created through it.
        symlink(&outside, config.join(".gemini")).unwrap();
        assert!(
            write_claude_below(
                &config,
                &config.join(".gemini/config/mcp_config.json"),
                command,
                &args
            )
            .is_err()
        );
        std::fs::remove_file(config.join(".gemini")).unwrap();
        unchanged();
        // The agent's own sylphx key linked out is not the bearer token.
        std::fs::write(config.join(".anyagents-profile"), "sylphx").unwrap();
        std::fs::create_dir_all(config.join(".config/sylphx")).unwrap();
        symlink(outside.join("secret"), config.join(".config/sylphx/key")).unwrap();
        write_codex_with_model(&config.join("config.toml"), None, command, &args).unwrap();
        let toml = std::fs::read_to_string(config.join("config.toml")).unwrap();
        assert!(!toml.contains("outside-secret"), "{toml}");
        unchanged();
    }
}
