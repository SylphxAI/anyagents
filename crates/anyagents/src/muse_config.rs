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
    let mut root: serde_json::Value = [
        path.clone(),
        dir.join("muse/settings.json"),
        dir.join(".config/muse/settings.json"),
    ]
    .into_iter()
    .find_map(|candidate| {
        std::fs::read(&candidate)
            .ok()
            .filter(|bytes| !bytes.is_empty())
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
    })
    .unwrap_or_else(|| serde_json::json!({}));
    if !root.is_object() {
        return Err(Error::Message("settings.json is not an object".into()));
    }
    if root.get("schema_version").is_none() {
        root["schema_version"] = serde_json::json!(1);
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
        let _ = std::fs::create_dir_all(&sub_dir);
        if sub_dir.is_dir() {
            let _ = write_atomic(&sub_dir.join("settings.json"), &bytes);
        }
    }
    Ok(())
}

/// Ensure `<config>/muse-session-id` points to a Muse session initialized with
/// `workspaceRoot` and `:unrestricted` permissions so `session/resume` attaches
/// the workspace toolset (`bash`, file tools) and starts `mcpServers.anyagents`.
pub fn ensure_workspace_session(
    data_dir: &Path,
    agent_id: &str,
    config_dir: &Path,
    work_dir: &Path,
    model: Option<&str>,
    seat_spec: Option<&str>,
) {
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() {
        return;
    }
    let sid_path = config_dir.join("muse-session-id");
    let marker_path = config_dir.join(".anyagents-muse-workspace-session");
    let expected_marker = |sid: &str| format!("{sid}\t{}\n", work_dir.display());
    if let (Ok(sid), Ok(marker)) = (
        std::fs::read_to_string(&sid_path),
        std::fs::read_to_string(&marker_path),
    ) {
        let sid = sid.trim();
        if !sid.is_empty() && marker == expected_marker(sid) {
            return;
        }
    }
    let program = std::env::var("ANYAGENTS_MUSE_BIN").unwrap_or_else(|_| "muse".to_string());
    let model_str = model
        .map(str::trim)
        .filter(|m| !m.is_empty())
        .unwrap_or("muse-wonder-full-1.0");
    let mut cmd = if let Some(spec) = seat_spec {
        let parts: Vec<&str> = spec.split('\t').collect();
        if parts.len() == 4 {
            let mut c = std::process::Command::new(parts[0]);
            c.args([
                "--reuid",
                parts[1],
                "--regid",
                parts[2],
                "--groups",
                parts[3],
                "--no-new-privs",
                "--",
                &program,
            ]);
            c
        } else {
            std::process::Command::new(&program)
        }
    } else {
        std::process::Command::new(&program)
    };
    cmd.args([
        "serve",
        "--provider",
        "meta",
        "--model",
        model_str,
        "--disable-request-user-input",
        "--disable-sandbox",
        "--trust-workspace",
        "-c",
        "context.foreign_personal_rules=false",
        "-c",
        "context.foreign_personal_skills=false",
    ])
    .current_dir(work_dir)
    .env("ANYAGENTS_DATA", data_dir)
    .env("ANYAGENTS_AGENT_ID", agent_id)
    .env("ANYAGENTS_AGENT_CONFIG", config_dir)
    .env("MUSE_CONFIG_HOME", config_dir)
    .env("XDG_CONFIG_HOME", config_dir)
    .env("XDG_DATA_HOME", config_dir.join("data-home"))
    .env("XDG_STATE_HOME", config_dir.join("state-home"))
    .env("XDG_CACHE_HOME", config_dir.join("cache-home"))
    .env("HOME", config_dir)
    .env("TMPDIR", config_dir.join("tmp"))
    .env("MUSE_NO_AUTO_UPDATE", "1")
    .stdin(std::process::Stdio::piped())
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::null());

    let Ok(mut child) = cmd.spawn() else {
        return;
    };
    let Some(mut stdin) = child.stdin.take() else {
        let _ = child.wait();
        return;
    };
    let Some(stdout) = child.stdout.take() else {
        drop(stdin);
        let _ = child.wait();
        return;
    };
    use std::io::{BufRead as _, BufReader, Write as _};
    let mut reader = BufReader::new(stdout);
    let init = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "clientInfo": {"name": "anyagents", "version": "0.1.0"},
            "capabilities": {
                "requestedCapabilities": ["userShell", "sessionMcp"],
                "userInputDialogs": false
            }
        }
    });
    if writeln!(stdin, "{init}").is_err() || stdin.flush().is_err() {
        drop(stdin);
        let _ = child.wait();
        return;
    }
    let mut line = String::new();
    if reader.read_line(&mut line).is_err() || line.is_empty() {
        drop(stdin);
        let _ = child.wait();
        return;
    }
    let _ = writeln!(
        stdin,
        r#"{{"jsonrpc":"2.0","method":"initialized","params":{{}}}}"#
    );
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let ms = (nanos / 1_000_000) & 0xFFFF_FFFF_FFFF;
    let rand_a = nanos & 0x0FFF;
    let rand_b = (nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 4) & 0x3FFF_FFFF_FFFF_FFFF;
    let val = (ms << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b;
    let hex = format!("{val:032x}");
    let cid = format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    );
    let start_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "session/start",
        "params": {
            "commandId": cid,
            "workspaceRoot": work_dir.display().to_string(),
            "permissionProfile": ":unrestricted"
        }
    });
    if writeln!(stdin, "{start_req}").is_err() || stdin.flush().is_err() {
        drop(stdin);
        let _ = child.wait();
        return;
    }
    let mut minted_sid = None;
    for _ in 0..32 {
        line.clear();
        if reader.read_line(&mut line).is_err() || line.is_empty() {
            break;
        }
        if let Ok(obj) = serde_json::from_str::<serde_json::Value>(&line)
            && obj.get("id").and_then(|v| v.as_i64()) == Some(2)
        {
            minted_sid = obj
                .pointer("/result/session/sessionId")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            break;
        }
    }
    drop(stdin);
    let _ = child.wait();
    if let Some(sid) = minted_sid {
        let _ = std::fs::write(&sid_path, format!("{sid}\n"));
        let _ = std::fs::write(&marker_path, expected_marker(&sid));
    }
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
        assert_eq!(parsed["schema_version"], 1);
        assert_eq!(parsed["model"], "muse-wonder-full-1.0");
        assert_eq!(parsed["tui"]["x"], true);
        assert_eq!(parsed["permissions"]["default_profile"], ":unrestricted");
        assert_eq!(parsed["contributor_mode_enabled"], false);
        let server = &parsed["mcpServers"]["anyagents"];
        assert_eq!(server["command"], "/bin/anyagents");
        assert_eq!(server["args"][2], "mcp");
        assert_eq!(server["mode"], "required");
        let muse_sub: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("muse/settings.json")).unwrap())
                .unwrap();
        assert_eq!(muse_sub["schema_version"], 1);
        assert_eq!(muse_sub["model"], "muse-wonder-full-1.0");
    }
}
