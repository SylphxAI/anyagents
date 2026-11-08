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
    let mut root = crate::mcp_config::read_candidates(&[
        path.clone(),
        dir.join("muse/settings.json"),
        dir.join(".config/muse/settings.json"),
    ])?;
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
    let clean_cmd = command.display().to_string();
    let clean_cmd = clean_cmd
        .strip_suffix(" (deleted)")
        .unwrap_or(clean_cmd.as_str())
        .to_string();
    all.push(clean_cmd);
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
        std::fs::create_dir_all(&sub_dir)?;
        write_atomic(&sub_dir.join("settings.json"), &bytes)?;
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
    seat_environment: &[(String, String)],
) -> Result<(), Error> {
    if std::env::var_os("ANYAGENTS_FAKE_MODE").is_some() {
        return Ok(());
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
            return Ok(());
        }
    }
    let program = std::env::var("ANYAGENTS_MUSE_BIN").unwrap_or_else(|_| "muse".to_string());
    let model_str = model
        .map(str::trim)
        .filter(|m| !m.is_empty())
        .unwrap_or("muse-wonder-full-1.0");
    let mut environment = seat_environment.to_vec();
    for (key, value) in [
        ("ANYAGENTS_DATA", data_dir.to_owned()),
        ("ANYAGENTS_AGENT_CONFIG", config_dir.to_owned()),
        ("MUSE_CONFIG_HOME", config_dir.to_owned()),
        ("XDG_CONFIG_HOME", config_dir.to_owned()),
        ("XDG_DATA_HOME", config_dir.join("data-home")),
        ("XDG_STATE_HOME", config_dir.join("state-home")),
        ("XDG_CACHE_HOME", config_dir.join("cache-home")),
        ("HOME", config_dir.to_owned()),
        ("TMPDIR", config_dir.join("tmp")),
    ] {
        environment.push((key.to_owned(), value.display().to_string()));
    }
    environment.push(("ANYAGENTS_AGENT_ID".into(), agent_id.into()));
    environment.push(("MUSE_NO_AUTO_UPDATE".into(), "1".into()));
    // This initializer is a real vendor process, including during explicit
    // wake reconciliation before the ordinary adapter exists. Use exactly the
    // same admission and authority stripping as every other vendor launch.
    let mut cmd = anyagents_harness::spawn_command(Path::new(&program), &environment)
        .map_err(|error| Error::Message(error.to_string()))?;
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
    .stdin(std::process::Stdio::piped())
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::null());

    finish_workspace_session(cmd.spawn()?, config_dir, work_dir, &environment)
}

/// Own the initializer through every error path, including malformed responses.
struct InitializerChild {
    child: std::process::Child,
    environment: Vec<(String, String)>,
}
impl Drop for InitializerChild {
    fn drop(&mut self) {
        anyagents_harness::kill_child(&mut self.child, &self.environment);
        let _ = self.child.wait();
    }
}

fn rpc_result(
    lines: &std::sync::mpsc::Receiver<std::io::Result<String>>,
    id: i64,
    deadline: std::time::Instant,
) -> Result<serde_json::Value, Error> {
    for _ in 0..32 {
        let line = lines
            .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
            .map_err(|_| {
                Error::Message(format!("Muse response {id} unavailable before deadline"))
            })??;
        let value: serde_json::Value = serde_json::from_str(&line)
            .map_err(|err| Error::Message(format!("invalid Muse response: {err}")))?;
        if value.get("id").and_then(|v| v.as_i64()) == Some(id) {
            if value.get("error").is_some() {
                return Err(Error::Message(format!("Muse request {id} rejected")));
            }
            return value
                .get("result")
                .filter(|v| v.is_object())
                .cloned()
                .ok_or_else(|| Error::Message(format!("Muse response {id} has no result object")));
        }
    }
    Err(Error::Message(format!("Muse response {id} not observed")))
}

fn finish_workspace_session(
    child: std::process::Child,
    config_dir: &Path,
    work_dir: &Path,
    environment: &[(String, String)],
) -> Result<(), Error> {
    use std::io::{BufRead as _, Write as _};
    let mut child = InitializerChild {
        child,
        environment: environment.to_vec(),
    };
    let mut stdin = child
        .child
        .stdin
        .take()
        .ok_or_else(|| Error::Message("Muse initializer has no stdin".into()))?;
    let stdout = child
        .child
        .stdout
        .take()
        .ok_or_else(|| Error::Message("Muse initializer has no stdout".into()))?;
    let (send, lines) = std::sync::mpsc::sync_channel(32);
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            if send.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let init = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
        "clientInfo":{"name":"anyagents","version":"0.1.0"},
        "capabilities":{"requestedCapabilities":["userShell","sessionMcp"],"userInputDialogs":false}
    }});
    writeln!(stdin, "{init}")?;
    stdin.flush()?;
    rpc_result(&lines, 1, deadline)?;
    writeln!(
        stdin,
        "{}",
        serde_json::json!({"jsonrpc":"2.0","method":"initialized","params":{}})
    )?;
    stdin.flush()?;
    let start = serde_json::json!({"jsonrpc":"2.0","id":2,"method":"session/start","params":{
        "commandId":format!("anyagents-{}-{}", std::process::id(), anyagents_core::now_ms()),
        "workspaceRoot":work_dir.display().to_string(),"permissionProfile":":unrestricted"
    }});
    writeln!(stdin, "{start}")?;
    stdin.flush()?;
    let result = rpc_result(&lines, 2, deadline)?;
    let sid = result
        .pointer("/session/sessionId")
        .and_then(|v| v.as_str())
        .filter(|v| !v.trim().is_empty())
        .ok_or_else(|| {
            Error::Message("Muse session/start did not return a session identity".into())
        })?;
    let marker = format!("{sid}\t{}\n", work_dir.display());
    // This recovery record survives either alias-write failure; the matching
    // final marker is published only after the selected launch pointer exists.
    let pending = config_dir.join(".anyagents-muse-workspace-session.pending");
    crate::create::write_required_file(&pending, marker.as_bytes(), 0o660)?;
    crate::create::write_required_file(
        &config_dir.join("muse-session-id"),
        format!("{sid}\n").as_bytes(),
        0o660,
    )?;
    crate::create::write_required_file(
        &config_dir.join(".anyagents-muse-workspace-session"),
        marker.as_bytes(),
        0o660,
    )?;
    std::fs::remove_file(pending)?;
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
            .unwrap_or("settings.json")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn fresh_session_initializer_propagates_admission_refusal_without_starting_vendor() {
        // Process isolation keeps the real initializer's fake-mode and binary
        // selectors independent of every other test's inherited environment.
        const ROOT: &str = "ANYAGENTS_TEST_MUSE_REFUSAL_ROOT";
        if let Some(root) = std::env::var_os(ROOT) {
            let data = std::path::PathBuf::from(root);
            let config = data.join("agents/one/config");
            let uid = crate::sandbox::server_uid();
            let environment = vec![(
                anyagents_harness::SEAT_LAUNCH_ENV.into(),
                format!("/bin/sh\t{uid}\t2100\t2100"),
            )];
            let error = ensure_workspace_session(&data, "one", &config, &data, None, &environment)
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("agent not started: the agent uid is root or the server's own uid"),
                "{error}"
            );
            for marker in [
                "vendor-started",
                "muse-session-id",
                ".anyagents-muse-workspace-session",
            ] {
                assert!(!config.join(marker).exists(), "refusal created {marker}");
            }
            return;
        }
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("agents/one/config")).unwrap();
        let probe = dir.path().join("muse-probe");
        std::fs::write(
            &probe,
            b"#!/bin/sh\nprintf started > \"$ANYAGENTS_AGENT_CONFIG/vendor-started\"\n",
        )
        .unwrap();
        std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o755)).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "muse_config::tests::fresh_session_initializer_propagates_admission_refusal_without_starting_vendor", "--nocapture", "--test-threads=1"])
            .env_remove("ANYAGENTS_FAKE_MODE")
            .env("ANYAGENTS_MUSE_BIN", &probe)
            .env(ROOT, dir.path())
            .output().unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"));
    }

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
    #[test]
    fn invalid_existing_candidates_never_overwrite_valid_or_invalid_aliases() {
        for bad in [
            "settings.json",
            "muse/settings.json",
            ".config/muse/settings.json",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let selected = dir.path().join("settings.json");
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
        std::fs::create_dir(dir.path().join("settings.json")).unwrap();
        assert!(write(dir.path(), None, Path::new("/opt/bin/anyagents"), &[]).is_err());
    }

    #[test]
    fn absent_config_initializes_and_keeps_valid_custom_data() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), None, Path::new("/opt/bin/anyagents"), &[]).unwrap();
        let path = dir.path().join("settings.json");
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["custom"] = serde_json::json!("keep");
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        write(dir.path(), None, Path::new("/opt/bin/anyagents"), &[]).unwrap();
        let after: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(after["custom"], "keep");
    }

    fn initializer_fixture(session_result: &str) -> std::process::Child {
        let script = format!(
            r#"read -r line
printf '%s\n' '{{"id":1,"result":{{}}}}'
read -r line
case "$line" in *initialized*) ;; *) exit 9 ;; esac
read -r line
printf '%s\n' '{session_result}'
exec sleep 30"#
        );
        std::process::Command::new("/bin/sh")
            .args(["-c", &script])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap()
    }

    #[test]
    fn initializer_observes_handshake_and_persists_matching_launch_identity() {
        let dir = tempfile::tempdir().unwrap();
        let work = dir.path().join("workspace");
        let child =
            initializer_fixture(r#"{"id":2,"result":{"session":{"sessionId":"new-session"}}}"#);
        finish_workspace_session(child, dir.path(), &work, &[]).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("muse-session-id")).unwrap(),
            "new-session\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".anyagents-muse-workspace-session")).unwrap(),
            format!("new-session\t{}\n", work.display())
        );
        assert!(
            !dir.path()
                .join(".anyagents-muse-workspace-session.pending")
                .exists()
        );
    }

    #[test]
    fn initializer_rejects_invalid_session_and_preserves_pointer_on_persistence_failure() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("muse-session-id"), b"previous\n").unwrap();
        for response in [
            r#"{"id":2,"result":{}}"#,
            r#"{"id":2,"error":{"code":-1}}"#,
            "not-json",
        ] {
            let child = initializer_fixture(response);
            assert!(finish_workspace_session(child, dir.path(), dir.path(), &[]).is_err());
            assert_eq!(
                std::fs::read(dir.path().join("muse-session-id")).unwrap(),
                b"previous\n"
            );
        }
        std::fs::remove_file(dir.path().join("muse-session-id")).unwrap();
        std::fs::create_dir(dir.path().join("muse-session-id")).unwrap();
        let child = initializer_fixture(
            r#"{"id":2,"result":{"session":{"sessionId":"recoverable-session"}}}"#,
        );
        assert!(finish_workspace_session(child, dir.path(), dir.path(), &[]).is_err());
        assert!(
            std::fs::read_to_string(dir.path().join(".anyagents-muse-workspace-session.pending"))
                .unwrap()
                .starts_with("recoverable-session\t")
        );
        assert!(
            !dir.path()
                .join(".anyagents-muse-workspace-session")
                .exists()
        );
    }
}
