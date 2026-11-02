//! Persistent selection of this session's workspace execution surface.
//!
//! This changes routing only. It never transfers a session, provisions/releases a
//! lease, changes credentials, or edits the model/profile/agent configuration.

use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const SELECTION_FILE: &str = "sylphx-workspace-placement.json";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkspacePlacement {
    Local,
    Cloud,
}

impl WorkspacePlacement {
    pub(crate) fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "local" => Ok(Self::Local),
            "cloud" => Ok(Self::Cloud),
            _ => Err("placement must be `local` or `cloud`".into()),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Cloud => "cloud",
        }
    }
}

fn launch_setting<'a>(env: &'a [(String, String)], key: &str) -> Option<&'a str> {
    env.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

/// The embedding runtime supplies the grant, not the selection file. Omission
/// retains the two surfaces the SDK already supports; an explicit empty grant
/// allows neither. A disabled workspace cannot be re-enabled by selecting it.
fn allowed(config_dir: &Path, env: &[(String, String)], placement: WorkspacePlacement) -> bool {
    // Reuse canonical normalization and launch > process > config precedence.
    // A routing preference or an explicit surface grant cannot re-enable a
    // workspace disabled by its effective sandbox configuration.
    super::resolve_sandbox_mode(config_dir, env) != "none"
        && launch_setting(env, "SYLPHX_WORKSPACE_PLACEMENTS").is_none_or(|grant| {
            grant
                .split(',')
                .map(str::trim)
                .any(|v| v == placement.as_str())
        })
}

/// Invalid saved selections fail closed instead of silently executing elsewhere.
/// Grants are checked on every tool as well as on selection, including revocation.
pub(crate) fn selected(
    config_dir: &Path,
    env: &[(String, String)],
) -> Result<Option<WorkspacePlacement>, String> {
    let raw = match std::fs::read(config_dir.join(SELECTION_FILE)) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("cannot read workspace placement: {e}")),
    };
    let doc: Value = serde_json::from_slice(&raw)
        .map_err(|e| format!("invalid saved workspace placement: {e}"))?;
    let placement =
        WorkspacePlacement::parse(doc.get("placement").and_then(Value::as_str).unwrap_or(""))?;
    if !allowed(config_dir, env, placement) {
        return Err(format!(
            "workspace placement `{}` is not granted",
            placement.as_str()
        ));
    }
    Ok(Some(placement))
}

pub(crate) fn select(
    config_dir: &Path,
    env: &[(String, String)],
    raw: &str,
) -> Result<WorkspacePlacement, String> {
    let placement = WorkspacePlacement::parse(raw)?;
    if !allowed(config_dir, env, placement) {
        return Err(format!("workspace placement `{raw}` is not granted"));
    }
    // Atomic replacement prevents the next tool observing an incomplete setting.
    // A private temporary file also avoids following a pre-existing temp symlink.
    let mut tmp = tempfile::NamedTempFile::new_in(config_dir).map_err(|e| e.to_string())?;
    use std::io::Write as _;
    serde_json::to_writer(&mut tmp, &json!({ "placement": placement }))
        .map_err(|e| e.to_string())?;
    tmp.flush().map_err(|e| e.to_string())?;
    tmp.as_file().sync_all().map_err(|e| e.to_string())?;
    tmp.persist(config_dir.join(SELECTION_FILE))
        .map_err(|e| e.to_string())?;
    Ok(placement)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_is_persistent_and_changes_only_its_own_record() {
        let dir = tempfile::tempdir().unwrap();
        let original = [
            (
                "sylphx-agent.json",
                r#"{"model":"unchanged","profile":"ordinary"}"#,
            ),
            ("sylphx-session-id", "same-session"),
            ("sylphx-transcript.jsonl", "pending/unknown tool operation"),
            ("sylphx-sandbox-lease.json", r#"{"name":"retained-lease"}"#),
            (
                "sylphx-cloud-session.json",
                r#"{"session_id":"retained-cloud-session"}"#,
            ),
        ];
        for (name, bytes) in original {
            std::fs::write(dir.path().join(name), bytes).unwrap();
        }
        assert_eq!(selected(dir.path(), &[]).unwrap(), None);
        assert_eq!(
            select(dir.path(), &[], "local").unwrap(),
            WorkspacePlacement::Local
        );
        assert_eq!(
            selected(dir.path(), &[]).unwrap(),
            Some(WorkspacePlacement::Local)
        );
        assert_eq!(
            select(dir.path(), &[], "cloud").unwrap(),
            WorkspacePlacement::Cloud
        );
        assert_eq!(
            selected(dir.path(), &[]).unwrap(),
            Some(WorkspacePlacement::Cloud)
        );
        for (name, bytes) in original {
            assert_eq!(
                std::fs::read_to_string(dir.path().join(name)).unwrap(),
                bytes
            );
        }
    }

    #[test]
    fn invalid_or_ungranted_selection_preserves_saved_bytes() {
        let dir = tempfile::tempdir().unwrap();
        select(dir.path(), &[], "local").unwrap();
        let before = std::fs::read(dir.path().join(SELECTION_FILE)).unwrap();
        let grant = vec![("SYLPHX_WORKSPACE_PLACEMENTS".into(), "local".into())];
        assert!(
            select(dir.path(), &grant, "cloud")
                .unwrap_err()
                .contains("not granted")
        );
        for invalid in ["", "host", "auto", "../other-agent", "LOCAL"] {
            assert!(select(dir.path(), &[], invalid).is_err());
        }
        assert_eq!(
            std::fs::read(dir.path().join(SELECTION_FILE)).unwrap(),
            before
        );
        let disabled = vec![("SYLPHX_SANDBOX".into(), "none".into())];
        assert!(select(dir.path(), &disabled, "local").is_err());
        assert!(selected(dir.path(), &disabled).is_err());
        assert_eq!(
            std::fs::read(dir.path().join(SELECTION_FILE)).unwrap(),
            before
        );
    }

    #[test]
    fn grant_revocation_or_corruption_never_silently_changes_surface() {
        let dir = tempfile::tempdir().unwrap();
        select(dir.path(), &[], "cloud").unwrap();
        let revoked = vec![("SYLPHX_WORKSPACE_PLACEMENTS".into(), "local".into())];
        assert!(selected(dir.path(), &revoked).is_err());
        std::fs::write(dir.path().join(SELECTION_FILE), "broken").unwrap();
        assert!(selected(dir.path(), &[]).is_err());
    }
}
