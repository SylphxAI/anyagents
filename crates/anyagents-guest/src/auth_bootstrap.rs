//! Guest-side harness auth mirroring (`claude-code`, `codex`, `spiron`),
//! Codex startup preflight fail-closed check, and stale kubeconfig cleanup.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyagents_harness::HarnessError;
use anyagents_harness_spiron::{MAX_SOCKET_PATH, SpironHarness, SpironLaunch};
use serde_json::{Value, json};

use crate::guest::GuestConfig;

pub(crate) fn normalize_spiron_model(model: &str) -> &str {
    let m = model.trim();
    let m = m.strip_prefix("sylphx/").unwrap_or(m);
    [
        "kimi-code-plan/",
        "glm-code-plan/",
        "qwen-token-plan/",
        "minimax-code-plan/",
    ]
    .into_iter()
    .find_map(|p| m.strip_prefix(p))
    .unwrap_or(m)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_spiron_launch(
    program: &Path,
    model: Option<&str>,
    session: Option<&str>,
    extra_args: &[String],
    state_dir: &Path,
    work_dir: &Path,
    env: &[(String, String)],
    file_cfg: Option<&GuestConfig>,
) -> SpironLaunch {
    let mut args = vec![
        "--headless".into(),
        "--json".into(),
        "--workdir".into(),
        work_dir.display().to_string(),
        "--state-dir".into(),
        state_dir.display().to_string(),
    ];
    let mcp_config = state_dir.join("spiron-mcp.json");
    if mcp_config.is_file() {
        args.extend(["--mcp-config".into(), mcp_config.display().to_string()]);
    }
    let skills = state_dir.join(".agents/skills");
    if skills.is_dir() {
        args.extend(["--skills-dir".into(), skills.display().to_string()]);
    }
    let sys_prompt = state_dir.join("SYSTEM_PROMPT.md");
    if sys_prompt.is_file() {
        args.extend([
            "--instructions-file".into(),
            sys_prompt.display().to_string(),
        ]);
    }
    if let Some(id) = file_cfg.and_then(|c| c.agent_id.as_deref()) {
        args.extend(["--agent-id".into(), id.to_string()]);
    }
    if let Some(name) = file_cfg.and_then(|c| c.agent_name.as_deref()) {
        args.extend(["--agent-name".into(), name.to_string()]);
    }
    if let Some(m) = model.map(normalize_spiron_model).filter(|s| !s.is_empty()) {
        args.extend(["--model".into(), m.to_string()]);
    }
    args.extend(extra_args.iter().cloned());
    SpironLaunch {
        program: program.to_path_buf(),
        args,
        environment: env.to_vec(),
        work_dir: work_dir.to_path_buf(),
        session: session.map(str::to_string),
        state_dir: state_dir.to_path_buf(),
    }
}

/// Launch `spiron` inside the sandbox using `spiron.headless/v2` (`--listen <socket>`)
/// so native mid-turn steering (`accepted` / `steer_consumed`) works live during turns.
/// Falls back to stdio mode only when explicitly opted into via `ANYAGENTS_GUEST_SPIRON_STDIO=1`
/// or `--guest-spiron-stdio`.
pub(crate) fn launch_guest_spiron(
    mut launch: SpironLaunch,
    state_dir: &Path,
) -> Result<(SpironHarness, Option<PathBuf>), HarnessError> {
    let force_stdio = launch.args.iter().any(|a| a == "--guest-spiron-stdio")
        || std::env::var("ANYAGENTS_GUEST_SPIRON_STDIO")
            .ok()
            .is_some_and(|v| matches!(v.trim(), "1" | "true" | "TRUE"));
    launch.args.retain(|a| a != "--guest-spiron-stdio");
    if force_stdio {
        return Ok((SpironHarness::launch(launch)?, None));
    }

    static SOCK_SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SOCK_SEQ.fetch_add(1, Ordering::Relaxed);
    let candidate = state_dir.join(format!("spiron-{}-{seq}.sock", std::process::id()));
    let socket = if candidate.as_os_str().len() <= MAX_SOCKET_PATH {
        candidate
    } else {
        std::env::temp_dir().join(format!("ag-sp-{}-{seq}.sock", std::process::id()))
    };
    let _ = std::fs::remove_file(&socket);
    let log = state_dir.join("spiron.log");
    let harness =
        SpironHarness::launch_listening(launch, &socket, &log, Duration::from_secs(60), |_pid| {
            Ok(())
        })
        .map_err(|anyagents_harness_spiron::ListenError::Failed(err)| HarnessError::Failed(err))?;
    Ok((harness, Some(socket)))
}

pub(crate) fn prepare_harness_auth(harness: &str, state_dir: &Path, model: Option<&str>) {
    prune_stale_kubeconfig(state_dir);
    match harness {
        "claude-code" => {
            mirror_pair(
                &state_dir.join(".credentials.json"),
                &state_dir.join(".claude/.credentials.json"),
            );
            mirror_pair(
                &state_dir.join(".claude.json"),
                &state_dir.join(".claude/.claude.json"),
            );
            ensure_claude_onboarding(&state_dir.join(".claude.json"));
        }
        "codex" => {
            mirror_pair(
                &state_dir.join("auth.json"),
                &state_dir.join(".codex/auth.json"),
            );
            ensure_codex_config_defaults(&state_dir.join("config.toml"), model);
        }
        "spiron" => {
            mirror_pair(
                &state_dir.join("auth.json"),
                &state_dir.join(".spiron/auth.json"),
            );
            mirror_pair(
                &state_dir.join("auth.json"),
                &state_dir.join(".config/sylphx/auth.json"),
            );
            mirror_pair(
                &state_dir.join(".spiron/auth.json"),
                &state_dir.join(".config/sylphx/auth.json"),
            );
        }
        _ => {}
    }
}

/// Rename `$STATE_DIR/.kube/config` to `.kube/config.stale` if it references a
/// nonexistent absolute `tokenFile:` or `certificate-authority:` path from an
/// older host so `kubectl` fails cleanly or falls back instead of breaking.
pub(crate) fn prune_stale_kubeconfig(state_dir: &Path) {
    let kube_cfg = state_dir.join(".kube/config");
    let Ok(raw) = std::fs::read_to_string(&kube_cfg) else {
        return;
    };
    let has_stale = raw.lines().any(|line| {
        let t = line.trim();
        let rest = t
            .strip_prefix("tokenFile:")
            .or_else(|| t.strip_prefix("certificate-authority:"));
        rest.map(str::trim)
            .map(|p| p.trim_matches(|c| c == '"' || c == '\''))
            .is_some_and(|p| p.starts_with('/') && !Path::new(p).exists())
    });
    if has_stale {
        let _ = std::fs::rename(&kube_cfg, state_dir.join(".kube/config.stale"));
    }
}

/// Fail closed before launching `codex app-server` if zero usable credentials exist,
/// preventing silent 300s+ hangs during startup/first turn.
pub(crate) fn check_codex_auth_preflight(
    program: &Path,
    state_dir: &Path,
    env: &[(String, String)],
) -> Result<(), String> {
    if program.file_name().and_then(|n| n.to_str()) != Some("codex") {
        return Ok(());
    }
    let has_env = |k: &str| {
        env.iter()
            .rev()
            .find(|(ek, _)| ek == k)
            .map(|(_, v)| v.as_str())
            .is_some_and(|v| !v.trim().is_empty())
            || std::env::var(k).is_ok_and(|v| !v.trim().is_empty())
    };
    if has_env("OPENAI_API_KEY") || has_env("CODEX_API_KEY") || has_env("SYLPHX_API_KEY") {
        return Ok(());
    }
    for rel in ["auth.json", ".codex/auth.json"] {
        if let Ok(raw) = std::fs::read_to_string(state_dir.join(rel))
            && serde_json::from_str::<Value>(&raw)
                .is_ok_and(|v| v.as_object().is_some_and(|m| !m.is_empty()))
        {
            return Ok(());
        }
    }
    if std::fs::read_to_string(state_dir.join("config.toml"))
        .is_ok_and(|s| s.contains("experimental_bearer_token"))
    {
        return Ok(());
    }
    Err("codex authentication missing: neither auth.json/.codex/auth.json nor OPENAI_API_KEY/CODEX_API_KEY is configured".into())
}

fn mirror_pair(a: &Path, b: &Path) {
    let read_meta = |p: &Path| {
        let bytes = std::fs::read(p).ok().filter(|v| !v.is_empty())?;
        let mtime = std::fs::metadata(p).ok().and_then(|m| m.modified().ok());
        Some((bytes, mtime))
    };
    let (src, dst) = match (read_meta(a), read_meta(b)) {
        (Some((ab, am)), Some((bb, bm))) => {
            if ab == bb {
                return;
            }
            if bm > am { (bb, a) } else { (ab, b) }
        }
        (Some((ab, _)), None) => (ab, b),
        (None, Some((bb, _))) => (bb, a),
        (None, None) => return,
    };
    if let Some(parent) = dst.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = anyagents_harness::atomic_write_nonempty_file(dst, &src, 0o600);
}

fn ensure_claude_onboarding(path: &Path) {
    let mut root = std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}));
    let obj = root.as_object_mut().unwrap();
    if obj.get("hasCompletedOnboarding") == Some(&Value::Bool(true)) {
        return;
    }
    obj.insert("hasCompletedOnboarding".into(), Value::Bool(true));
    if let Ok(mut bytes) = serde_json::to_vec_pretty(&root) {
        bytes.push(b'\n');
        let _ = anyagents_harness::atomic_write_nonempty_file(path, &bytes, 0o600);
    }
}

fn ensure_codex_config_defaults(path: &Path, model: Option<&str>) {
    let mut text = std::fs::read_to_string(path).unwrap_or_default();
    let mut prefix = String::new();
    if let Some(m) = model.map(str::trim).filter(|s| !s.is_empty())
        && !text
            .lines()
            .any(|l| l.trim_start().starts_with("model =") || l.trim_start().starts_with("model="))
    {
        let norm = m.strip_prefix("sylphx/").unwrap_or(m);
        let norm = norm
            .strip_prefix("openai/")
            .unwrap_or(norm)
            .split('#')
            .next()
            .unwrap_or(norm);
        if let Ok(esc) = serde_json::to_string(norm) {
            prefix.push_str(&format!("model = {esc}\n"));
        }
    }
    if !text.contains("approval_policy") {
        prefix.push_str("approval_policy = \"never\"\n");
    }
    if !text.contains("sandbox_mode") {
        prefix.push_str("sandbox_mode = \"danger-full-access\"\n");
    }
    if !prefix.is_empty() {
        text = format!("{prefix}{text}");
    }
    if !text.contains("[projects.\"/\"]") {
        if !text.is_empty() && !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str("\n[projects.\"/\"]\ntrust_level = \"trusted\"\n");
    }
    let _ = anyagents_harness::atomic_write_nonempty_file(path, text.as_bytes(), 0o600);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mirrors_claude_creds_checks_codex_preflight_and_prunes_stale_kubeconfig() {
        let tmp = tempfile::tempdir().unwrap();
        let state = tmp.path();
        std::fs::create_dir_all(state.join(".claude")).unwrap();
        std::fs::write(
            state.join(".claude/.credentials.json"),
            r#"{"claudeAiOauth":{"accessToken":"t"}}"#,
        )
        .unwrap();
        std::fs::create_dir_all(state.join(".kube")).unwrap();
        std::fs::write(
            state.join(".kube/config"),
            "users:\n- user:\n    tokenFile: /nonexistent/host/token\n",
        )
        .unwrap();

        prepare_harness_auth("claude-code", state, None);
        assert!(state.join(".credentials.json").is_file());
        assert!(state.join(".kube/config.stale").is_file());
        assert!(!state.join(".kube/config").exists());
        let cj: Value =
            serde_json::from_str(&std::fs::read_to_string(state.join(".claude.json")).unwrap())
                .unwrap();
        assert_eq!(cj["hasCompletedOnboarding"], true);

        assert!(check_codex_auth_preflight(Path::new("/usr/local/bin/codex"), state, &[]).is_err());
        std::fs::create_dir_all(state.join(".codex")).unwrap();
        std::fs::write(
            state.join(".codex/auth.json"),
            r#"{"OPENAI_API_KEY":"sk-test"}"#,
        )
        .unwrap();
        prepare_harness_auth("codex", state, Some("gpt-5.4"));
        assert!(state.join("auth.json").is_file());
        assert!(check_codex_auth_preflight(Path::new("/usr/local/bin/codex"), state, &[]).is_ok());
    }
}
