//! Muse config for one agent, inside the agent's own config directory, which
//! the server hands the child as `MUSE_CONFIG_HOME`.
//!
//! `settings.json` holds the MCP server Muse reads at startup and the
//! permissions the agent runs with. An agent runs with `:unrestricted`
//! permissions, as Claude Code and Codex do under AnyAgents: nobody is at the
//! tool approval prompt, and the vendor CLI's own permissions are the agent's.
//! Credentials are not read or copied; `muse login` writes them into the same
//! directory.

use std::path::Path;

use anyagents_core::Error;

/// Point `muse` at the agent's config directory.
///
/// On Windows the vendor reads `%APPDATA%\meta\muse`, which no environment
/// variable moves. It is a link to `dir` (a junction or directory symlink), so the
/// vendor's own files land in the agent's directory and are never copied. A
/// directory that is already something else there is left alone and the error
/// says so; nothing inside it is ever removed.
pub fn point_vendor_home(dir: &Path) -> Result<(), Error> {
    if !cfg!(windows) {
        return Ok(());
    }
    let Some(app_data) = std::env::var_os("APPDATA") else {
        return Ok(());
    };
    let target = dir.join("muse");
    std::fs::create_dir_all(&target)?;
    let link = Path::new(&app_data).join("meta").join("muse");
    if let Some(parent) = link.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::read_link(&link) {
        Ok(existing) if existing == target => return Ok(()),
        Ok(existing) => {
            return Err(Error::Message(format!(
                "{} already links to {}; move it aside to give this agent its own Muse home",
                link.display(),
                existing.display()
            )));
        }
        Err(_) if link.is_dir() => {
            return Err(Error::Message(format!(
                "{} is a directory the user's own Muse setup uses; it is left untouched",
                link.display()
            )));
        }
        Err(_) => {}
    }
    if link.symlink_metadata().is_ok() {
        std::fs::remove_file(&link)?;
    }
    #[cfg(windows)]
    return std::os::windows::fs::symlink_dir(&target, &link)
        .map_err(|err| Error::Message(format!("linking the Muse home: {err}")));
    #[cfg(not(windows))]
    return Ok(());
}

/// Write `settings.json` in `dir`, keeping its own agent's settings: the MCP
/// server at the `anyagents` key, unrestricted permissions, and no contributor
/// mode. Nothing else is written.
pub fn write(
    dir: &Path,
    model: Option<&str>,
    command: &Path,
    args: &[String],
) -> Result<(), Error> {
    let path = dir.join("settings.json");
    let mut root: serde_json::Value = match std::fs::read(&path) {
        Ok(bytes) if !bytes.is_empty() => {
            serde_json::from_slice(&bytes).map_err(|err| Error::Message(err.to_string()))?
        }
        _ => serde_json::json!({}),
    };
    if !root.is_object() {
        return Err(Error::Message("settings.json is not an object".into()));
    }
    if let Some(model) = model.filter(|model| !model.is_empty()) {
        root["model"] = serde_json::Value::String(model.to_string());
    }
    root["contributor_mode_enabled"] = serde_json::Value::Bool(false);
    root["permissions"] = serde_json::json!({
        "default_profile": ":unrestricted",
        "schema_version": 1,
    });
    let mut all = Vec::with_capacity(args.len() + 1);
    all.push(command.display().to_string());
    all.extend(args.iter().cloned());
    root["mcpServers"] = serde_json::json!({
        "anyagents": {
            "mode": "required",
            "command": all[0],
            "args": all[1..].to_vec(),
        }
    });
    let bytes = serde_json::to_vec_pretty(&root).map_err(|err| Error::Message(err.to_string()))?;
    write_atomic(&path, &bytes)?;
    for sub in ["muse", ".config/muse"] {
        let sub_dir = dir.join(sub);
        if sub_dir.is_dir() {
            let _ = write_atomic(&sub_dir.join("settings.json"), &bytes);
        }
    }
    Ok(())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let tmp = path.with_extension("json.new");
    std::fs::write(&tmp, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o664));
    }
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mcp_server_and_model_are_written_and_other_keys_kept() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("settings.json"),
            r#"{"schema_version":1,"tui":{"x":true}}"#,
        )
        .unwrap();
        let args = vec![
            "--data".to_string(),
            "/tmp/d".to_string(),
            "mcp".to_string(),
        ];
        write(
            dir.path(),
            Some("muse-wonder-full-1.0"),
            Path::new("/bin/anyagents"),
            &args,
        )
        .unwrap();
        let parsed: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("settings.json")).unwrap())
                .unwrap();
        assert_eq!(parsed["model"], "muse-wonder-full-1.0");
        assert_eq!(parsed["tui"]["x"], true);
        assert_eq!(parsed["permissions"]["default_profile"], ":unrestricted");
        assert_eq!(parsed["contributor_mode_enabled"], false);
        let server = &parsed["mcpServers"]["anyagents"];
        assert_eq!(server["command"], "/bin/anyagents");
        assert_eq!(server["args"][2], "mcp");
        assert_eq!(server["mode"], "required");
    }
}
