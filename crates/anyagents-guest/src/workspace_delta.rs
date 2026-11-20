//! Compact `<workspace>` git/file-change snapshot & delta builder plus guest
//! session validation and sensitive-file permission hygiene.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyagents_harness::spawn_command;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) fn read_persisted_session(state_dir: &Path, harness: &str) -> Option<String> {
    let raw = std::fs::read_to_string(state_dir.join(format!("{harness}-session-id"))).ok()?;
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

pub(crate) fn persist_session(
    state_dir: &Path,
    harness: &str,
    session_id: &str,
) -> Result<(), String> {
    let trimmed = session_id.trim();
    if trimmed.is_empty() {
        return Ok(());
    }
    let path = state_dir.join(format!("{harness}-session-id"));
    anyagents_harness::atomic_write_nonempty_file(&path, format!("{trimmed}\n").as_bytes(), 0o600)
        .map_err(|e| e.to_string())?;
    update_guest_config_session(state_dir, trimmed);
    Ok(())
}

const WORKSPACE_SNAPSHOT_FILE: &str = "workspace-snapshot.json";
const MAX_REPORTED_FILES: usize = 20;
#[cfg(unix)]
const MAX_PERMISSION_WALK_DEPTH: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WorkspaceSnapshot {
    branch: String,
    head: String,
    dirty_count: usize,
    files: Vec<String>,
}

fn xml_attr_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn resolve_git_bin(git_override: Option<&Path>) -> PathBuf {
    match git_override {
        Some(p) => p.to_path_buf(),
        None => anyagents_toolchain::tool("git", "anyagents-guest.workspace_delta")
            .map(|r| r.path)
            .unwrap_or_else(|_| PathBuf::from("/usr/bin/git")),
    }
}

fn run_git_stdout(git_bin: &Path, work_dir: &Path, args: &[&str]) -> Option<String> {
    let mut cmd = spawn_command(git_bin, &[]).ok()?;
    cmd.current_dir(work_dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .args(args);
    let out = cmd.output().ok()?;
    if !out.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&out.stdout).into_owned())
}

fn capture_git_snapshot(work_dir: &Path, git_override: Option<&Path>) -> Option<WorkspaceSnapshot> {
    if !work_dir.join(".git").exists() {
        return None;
    }
    let git_bin = resolve_git_bin(git_override);
    let branch = run_git_stdout(&git_bin, work_dir, &["rev-parse", "--abbrev-ref", "HEAD"])
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "HEAD".to_string());
    let head = run_git_stdout(&git_bin, work_dir, &["rev-parse", "--short", "HEAD"])
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unborn".to_string());
    let porcelain = run_git_stdout(&git_bin, work_dir, &["status", "--porcelain"])?;
    let mut all_dirty: BTreeSet<String> = BTreeSet::new();
    for line in porcelain.lines() {
        if line.len() < 4 {
            continue;
        }
        let raw_path = line[3..].trim();
        let path = raw_path
            .rsplit_once(" -> ")
            .map(|(_, dst)| dst.trim())
            .unwrap_or(raw_path)
            .trim_matches('"');
        if !path.is_empty() {
            all_dirty.insert(path.to_string());
        }
    }
    let dirty_count = all_dirty.len();
    let files: Vec<String> = all_dirty.into_iter().take(MAX_REPORTED_FILES).collect();
    Some(WorkspaceSnapshot {
        branch,
        head,
        dirty_count,
        files,
    })
}

/// Capture a bounded git workspace snapshot for `work_dir`, diff it against
/// `<state-dir>/workspace-snapshot.json`, persist the new snapshot, and emit a
/// compact `<workspace .../>` XML tag only on the initial turn or when git
/// state/dirty files changed since the previous turn.
pub(crate) fn capture_and_diff(
    work_dir: &Path,
    state_dir: &Path,
    git_override: Option<&Path>,
) -> Option<String> {
    let current = capture_git_snapshot(work_dir, git_override)?;
    let snap_path = state_dir.join(WORKSPACE_SNAPSHOT_FILE);
    let previous: Option<WorkspaceSnapshot> = std::fs::read_to_string(&snap_path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok());

    if let Ok(bytes) = serde_json::to_vec(&current) {
        let _ = anyagents_harness::atomic_write_file(&snap_path, &bytes, 0o600);
    }

    match previous {
        None => {
            let mut tag = format!(
                "<workspace event=\"initial\" branch=\"{}\" head=\"{}\" dirty=\"{}\"",
                xml_attr_escape(&current.branch),
                xml_attr_escape(&current.head),
                current.dirty_count,
            );
            if !current.files.is_empty() {
                tag.push_str(&format!(
                    " files=\"{}\"",
                    xml_attr_escape(&current.files.join(", "))
                ));
            }
            tag.push_str(" />");
            Some(tag)
        }
        Some(prev) if prev == current => None,
        Some(prev) => {
            let prev_set: BTreeSet<&str> = prev.files.iter().map(String::as_str).collect();
            let curr_set: BTreeSet<&str> = current.files.iter().map(String::as_str).collect();
            let mut changed: Vec<&str> = prev_set
                .symmetric_difference(&curr_set)
                .copied()
                .take(MAX_REPORTED_FILES)
                .collect();
            if changed.is_empty() && !current.files.is_empty() {
                changed = current
                    .files
                    .iter()
                    .map(String::as_str)
                    .take(MAX_REPORTED_FILES)
                    .collect();
            }
            let mut tag = format!(
                "<workspace event=\"delta\" branch=\"{}\" head=\"{}\" dirty=\"{}\"",
                xml_attr_escape(&current.branch),
                xml_attr_escape(&current.head),
                current.dirty_count,
            );
            if !changed.is_empty() {
                tag.push_str(&format!(
                    " changed=\"{}\"",
                    xml_attr_escape(&changed.join(", "))
                ));
            }
            tag.push_str(" />");
            Some(tag)
        }
    }
}

fn agy_conversation_exists(state_dir: &Path, sid: &str) -> bool {
    if state_dir
        .join(".gemini")
        .join("antigravity-cli")
        .join("brain")
        .join(sid)
        .is_dir()
    {
        return true;
    }
    if let Ok(home) = std::env::var("HOME") {
        let home_path = PathBuf::from(home.trim());
        if home_path != state_dir
            && home_path
                .join(".gemini")
                .join("antigravity-cli")
                .join("brain")
                .join(sid)
                .is_dir()
        {
            return true;
        }
    }
    false
}

/// Resolve the harness session ID to resume for this turn.
///
/// Priority:
/// 1. Explicit `--session` CLI argument.
/// 2. Persisted `<state-dir>/<harness>-session-id` written by the guest after previous turns.
/// 3. Fallback `guest-config.json` `session_id` hydrated by the coordinator.
///
/// For `agy`, verifies that the candidate conversation directory actually exists
/// under `<state-dir>/.gemini/antigravity-cli/brain/<uuid>` (or `$HOME/...`),
/// returning `None` when stale/missing so `agy` creates a fresh conversation and
/// persists the real UUID cleanly instead of spawning orphaned conversations.
pub(crate) fn resolve_guest_session(
    state_dir: &Path,
    harness_name: &str,
    explicit_arg: Option<&str>,
    file_cfg_session: Option<&str>,
) -> Option<String> {
    let clean = |raw: Option<&str>| -> Option<String> {
        raw.map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    if let Some(explicit) = clean(explicit_arg) {
        return Some(explicit);
    }
    let persisted = read_persisted_session(state_dir, harness_name);
    let from_cfg = clean(file_cfg_session);
    let is_valid = |sid: &str| -> bool {
        if harness_name == "agy" {
            agy_conversation_exists(state_dir, sid)
        } else {
            true
        }
    };
    persisted
        .filter(|sid| is_valid(sid))
        .or_else(|| from_cfg.filter(|sid| is_valid(sid)))
}

/// Keep `<state-dir>/guest-config.json`'s `session_id` synchronized with the
/// live persisted session ID while enforcing `0600` permissions.
pub(crate) fn update_guest_config_session(state_dir: &Path, session_id: &str) {
    let trimmed = session_id.trim();
    if trimmed.is_empty() {
        return;
    }
    let cfg_path = state_dir.join("guest-config.json");
    let Ok(raw) = std::fs::read_to_string(&cfg_path) else {
        return;
    };
    let Ok(mut val) = serde_json::from_str::<Value>(&raw) else {
        return;
    };
    let Some(obj) = val.as_object_mut() else {
        return;
    };
    if obj.get("session_id").and_then(Value::as_str) == Some(trimmed) {
        return;
    }
    obj.insert("session_id".to_string(), Value::String(trimmed.to_string()));
    if let Ok(bytes) = serde_json::to_vec_pretty(&val) {
        let _ = anyagents_harness::atomic_write_nonempty_file(&cfg_path, &bytes, 0o600);
    }
}

/// Ensure `<state-dir>/.config` exists with `0700` permissions and return it as
/// the canonical `XDG_CONFIG_HOME` for both guest harnesses and shell commands.
pub(crate) fn ensure_xdg_config_home(state_dir: &Path) -> PathBuf {
    let config_home = state_dir.join(".config");
    let _ = std::fs::create_dir_all(&config_home);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&config_home, std::fs::Permissions::from_mode(0o700));
    }
    config_home
}

#[cfg(unix)]
fn is_sensitive_filename(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "guest-config.json"
            | "credentials.json"
            | ".credentials.json"
            | "auth.json"
            | "key"
            | ".mcp-token"
    ) || lower.ends_with(".key")
        || lower.ends_with("-session-id")
        || lower.contains("token")
        || lower.contains("secret")
        || lower.contains("credential")
}

#[cfg(unix)]
fn tighten_dir_tree(dir: &Path, depth: usize, tighten_parent_on_hit: bool) {
    use std::os::unix::fs::PermissionsExt as _;

    if depth > MAX_PERMISSION_WALK_DEPTH {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut found_sensitive = false;
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        let ft = meta.file_type();
        if ft.is_symlink() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if ft.is_file() {
            let in_opencode_cfg = dir.file_name().and_then(|s| s.to_str()) == Some("opencode");
            if is_sensitive_filename(name) || in_opencode_cfg {
                let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
                found_sensitive = true;
            }
        } else if ft.is_dir() && depth < MAX_PERMISSION_WALK_DEPTH {
            tighten_dir_tree(&path, depth + 1, true);
        }
    }
    if tighten_parent_on_hit && found_sensitive {
        let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
    }
}

/// Enforce mode `0600` on `guest-config.json` and hydrated credential/key/token
/// files under `state_dir`, and `0700` on `.config` and sensitive config
/// subdirectories containing credentials or keys.
pub(crate) fn tighten_sensitive_permissions(state_dir: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if let Ok(entries) = std::fs::read_dir(state_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Ok(meta) = std::fs::symlink_metadata(&path)
                    && meta.file_type().is_file()
                    && let Some(name) = path.file_name().and_then(|s| s.to_str())
                    && is_sensitive_filename(name)
                {
                    let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
                }
            }
        }
        for top_sensitive in [
            ".codex/auth.json",
            ".claude/.credentials.json",
            ".gemini/antigravity-cli/antigravity-oauth-token",
        ] {
            let p = state_dir.join(top_sensitive);
            if p.is_file() {
                let _ = std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600));
            }
        }
        let config_dir = state_dir.join(".config");
        if config_dir.is_dir() {
            let _ = std::fs::set_permissions(&config_dir, std::fs::Permissions::from_mode(0o700));
            tighten_dir_tree(&config_dir, 0, true);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = state_dir;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_git(dir: &Path, args: &[&str]) {
        let git_bin = resolve_git_bin(None);
        let out = spawn_command(&git_bin, &[])
            .unwrap()
            .current_dir(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    #[test]
    fn workspace_delta_emits_initial_silent_noop_and_external_change_delta() {
        let work = tempfile::tempdir().unwrap();
        let state = tempfile::tempdir().unwrap();

        // Non-git workdir produces no workspace envelope.
        assert!(capture_and_diff(work.path(), state.path(), None).is_none());

        run_git(work.path(), &["init", "-b", "main"]);
        run_git(work.path(), &["config", "user.email", "test@example.com"]);
        run_git(work.path(), &["config", "user.name", "Tester"]);
        std::fs::write(work.path().join("README.md"), "hello\n").unwrap();
        run_git(work.path(), &["add", "README.md"]);
        run_git(work.path(), &["commit", "-m", "init"]);

        // First turn emits compact initial snapshot.
        let initial = capture_and_diff(work.path(), state.path(), None).expect("initial tag");
        assert!(initial.starts_with("<workspace event=\"initial\" branch=\"main\" head=\""));
        assert!(initial.contains("dirty=\"0\""));
        assert!(initial.ends_with(" />"));

        // Second turn with no external changes emits NOTHING (zero token bloat).
        assert!(capture_and_diff(work.path(), state.path(), None).is_none());

        // External file modification between turns triggers a compact delta.
        std::fs::write(work.path().join("README.md"), "hello updated\n").unwrap();
        std::fs::write(work.path().join("notes.txt"), "new file\n").unwrap();
        let delta = capture_and_diff(work.path(), state.path(), None).expect("delta tag");
        assert!(delta.starts_with("<workspace event=\"delta\" branch=\"main\""));
        assert!(delta.contains("dirty=\"2\""));
        assert!(delta.contains("changed=\"README.md, notes.txt\""));

        // Subsequent turn with the same dirty set is silent again.
        assert!(capture_and_diff(work.path(), state.path(), None).is_none());
    }

    #[test]
    fn resolve_guest_session_prefers_valid_persisted_session_over_stale_guest_config() {
        let state = tempfile::tempdir().unwrap();
        let stale_uuid = "00000000-0000-0000-0000-000000000001";
        let live_uuid = "11111111-1111-1111-1111-111111111111";

        // Create brain directory only for `live_uuid`.
        std::fs::create_dir_all(
            state
                .path()
                .join(".gemini/antigravity-cli/brain")
                .join(live_uuid),
        )
        .unwrap();
        persist_session(state.path(), "agy", live_uuid).unwrap();

        // Persisted valid UUID beats stale guest-config UUID.
        assert_eq!(
            resolve_guest_session(state.path(), "agy", None, Some(stale_uuid)).as_deref(),
            Some(live_uuid)
        );

        // When persisted UUID is also missing on disk for `agy`, return None so `agy` starts clean.
        persist_session(state.path(), "agy", "22222222-2222-2222-2222-222222222222").unwrap();
        assert_eq!(
            resolve_guest_session(state.path(), "agy", None, Some(stale_uuid)),
            None
        );

        // Explicit CLI `--session` always wins.
        assert_eq!(
            resolve_guest_session(state.path(), "agy", Some(stale_uuid), None).as_deref(),
            Some(stale_uuid)
        );

        // Non-agy harness still prefers persisted session over file_cfg.session_id.
        persist_session(state.path(), "claude-code", "ses-live").unwrap();
        assert_eq!(
            resolve_guest_session(state.path(), "claude-code", None, Some("ses-stale")).as_deref(),
            Some("ses-live")
        );
    }

    #[cfg(unix)]
    #[test]
    fn tighten_sensitive_permissions_enforces_0600_files_and_0700_config_dirs() {
        use std::os::unix::fs::PermissionsExt as _;

        let state = tempfile::tempdir().unwrap();
        let guest_cfg = state.path().join("guest-config.json");
        std::fs::write(&guest_cfg, r#"{"session_id":"old"}"#).unwrap();
        std::fs::set_permissions(&guest_cfg, std::fs::Permissions::from_mode(0o644)).unwrap();

        let cred_dir = state.path().join(".config/tool-a");
        std::fs::create_dir_all(&cred_dir).unwrap();
        std::fs::set_permissions(&cred_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cred_file = cred_dir.join("credentials.json");
        let key_file = cred_dir.join("key");
        std::fs::write(&cred_file, "{}").unwrap();
        std::fs::write(&key_file, "secret").unwrap();
        std::fs::set_permissions(&cred_file, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::set_permissions(&key_file, std::fs::Permissions::from_mode(0o644)).unwrap();

        tighten_sensitive_permissions(state.path());
        update_guest_config_session(state.path(), "new-sid");

        assert_eq!(
            std::fs::metadata(&guest_cfg).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&cred_file).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&key_file).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&cred_dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(state.path().join(".config"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        let updated: Value =
            serde_json::from_str(&std::fs::read_to_string(&guest_cfg).unwrap()).unwrap();
        assert_eq!(updated["session_id"], "new-sid");
    }
}
