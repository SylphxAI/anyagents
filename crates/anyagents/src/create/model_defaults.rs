//! Default harness model resolution and PostgreSQL config persistence for newly spawned agents.

use std::path::Path;

use anyagents_core::{Agent, AgentId, Store};
use anyagents_harness::normalize_harness_name;

/// Fallback default model for `agy` when no explicit model, env override,
/// profile default, or `agy` parent model is configured.
pub(crate) const DEFAULT_AGY_MODEL: &str = "gemini-4-argon-high";

/// Resolve a non-null default model for `harness` when a caller omits `model`.
///
/// Precedence:
/// 1. Explicit env override (`ANYAGENTS_DEFAULT_<HARNESS>_MODEL`, e.g. `ANYAGENTS_DEFAULT_AGY_MODEL`).
/// 2. Parent's configured model when parent uses the same harness.
/// 3. Configured default in `<data>/subagent-defaults.json` when targeting `harness`.
/// 4. Profile-level default (`profile.json` / `.gemini/antigravity-cli/settings.json` / `opencode.json`).
/// 5. Built-in harness default (`spiron`/`spiron-local` -> `DEFAULT_SPIRON_MODEL`, `agy` -> `DEFAULT_AGY_MODEL`).
pub(crate) fn resolve_default_model_for_harness(
    data: &Path,
    harness: &str,
    profile: Option<&str>,
    parent: Option<&Agent>,
) -> Option<String> {
    let norm = normalize_harness_name(harness);

    let env_key = format!(
        "ANYAGENTS_DEFAULT_{}_MODEL",
        norm.to_ascii_uppercase().replace('-', "_")
    );
    if let Ok(val) = std::env::var(&env_key) {
        let trimmed = val.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    if let Some(p) = parent
        && normalize_harness_name(&p.harness) == norm
        && let Some(m) = p.model.as_deref().map(str::trim).filter(|s| !s.is_empty())
    {
        return Some(m.to_string());
    }

    let cfg = super::load_subagent_defaults(data);
    if let Some(spec_harness) = cfg.default.harness.as_deref()
        && normalize_harness_name(spec_harness) == norm
        && let Some(m) = cfg
            .default
            .model
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("inherit"))
    {
        return Some(m.to_string());
    }

    if let Some(m) = profile_configured_default_model(data, norm, profile) {
        return Some(m);
    }

    match norm {
        "spiron" | "spiron-local" => Some(crate::spiron::DEFAULT_SPIRON_MODEL.to_string()),
        "agy" => Some(DEFAULT_AGY_MODEL.to_string()),
        _ => None,
    }
}

fn profile_configured_default_model(
    data: &Path,
    norm_harness: &str,
    profile: Option<&str>,
) -> Option<String> {
    let provider = super::upstream_provider_of(norm_harness, None);
    let profiles_dir = data.join("profiles").join(&provider);
    let mut candidate_dirs = Vec::new();
    if let Some(raw_prof) = profile.map(str::trim).filter(|s| !s.is_empty()) {
        let (prov, name) = match raw_prof.split_once('/') {
            Some((pv, pn)) => (pv.trim(), pn.trim()),
            None => (provider.as_str(), raw_prof),
        };
        if !prov.is_empty() && !name.is_empty() {
            candidate_dirs.push(data.join("profiles").join(prov).join(name));
        }
    }
    if candidate_dirs.is_empty()
        && let Ok(entries) = std::fs::read_dir(&profiles_dir)
    {
        let mut dirs: Vec<_> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect();
        dirs.sort();
        candidate_dirs.extend(dirs);
    }

    for dir in candidate_dirs {
        for rel in [
            "profile.json",
            ".gemini/antigravity-cli/settings.json",
            ".config/opencode/opencode.json",
            "opencode/opencode.json",
        ] {
            let Ok(raw) = std::fs::read_to_string(dir.join(rel)) else {
                continue;
            };
            let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw) else {
                continue;
            };
            for key in ["default_model", "defaultModel", "model"] {
                if let Some(m) = val
                    .get(key)
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    return Some(m.to_string());
                }
            }
        }
    }
    None
}

/// Persist newly spawned agent config, profile markers, and seeded auth credentials
/// into PostgreSQL `agent_files` so any stateless replica immediately sees the
/// spawned child's full configuration and credentials on first dispatch.
pub(crate) fn persist_spawned_agent_config_to_store(
    store: &Store,
    agent_id: &AgentId,
    config_dir: &Path,
) {
    if !store.is_postgres() {
        return;
    }
    let blobs = store.blob_store();
    let config_rels = [
        "SYSTEM_PROMPT.md",
        "AGENTS.md",
        "CLAUDE.md",
        ".claude/CLAUDE.md",
        ".gemini/config/GEMINI.md",
        ".gemini/antigravity-cli/settings.json",
        "opencode/AGENTS.md",
        ".config/opencode/AGENTS.md",
        "opencode/opencode.json",
        ".config/opencode/opencode.json",
        ".anyagents-profile",
        ".anyagents-seeded-profile",
        ".anyagents-managed-worktree.json",
        "repo-spec.json",
    ];
    for rel in config_rels {
        let path = config_dir.join(rel);
        if let Ok(bytes) = std::fs::read(&path)
            && !bytes.is_empty()
        {
            let _ = blobs.put_agent_file(agent_id, "config", rel, &bytes, false);
        }
    }
    let secret_rels = [
        ".gemini/antigravity-cli/antigravity-oauth-token",
        ".claude/.credentials.json",
        ".credentials.json",
        ".codex/auth.json",
    ];
    for rel in secret_rels {
        let path = config_dir.join(rel);
        if let Ok(bytes) = std::fs::read(&path)
            && !bytes.is_empty()
        {
            let _ = blobs.put_agent_file(agent_id, "secrets", rel, &bytes, true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create::{CreateRequest, create, resolve_subagent_defaults};

    #[test]
    fn agy_spawned_without_model_resolves_and_persists_non_null_default_model() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let store = Store::open(data.join("state.db")).unwrap();
        store
            .hold_capability_key("0123456789abcdef0123456789abcdef")
            .unwrap();

        let (harness, model, _profile) = resolve_subagent_defaults(data, None, "agy", None, None);
        assert_eq!(harness, "agy");
        assert!(
            model.as_deref().is_some_and(|m| !m.is_empty()),
            "agy default model must never be null"
        );

        let agent = create(
            &store,
            data,
            CreateRequest {
                name: "agy-child".into(),
                role: None,
                harness: "agy".into(),
                runtime: None,
                budget_cap_micros: None,
                model: None,
                profile: None,
                repo: None,
                branch: None,
                base: None,
                workdir: Some(data.join("work-agy")),
                workdir_default: None,
                description: None,
                instructions: None,
                parent_id: None,
            },
        )
        .unwrap();
        assert_eq!(agent.harness, "agy");
        assert!(
            agent.model.as_deref().is_some_and(|m| !m.is_empty()),
            "persisted agy agent must have a non-null default model"
        );
    }

    #[test]
    fn opencode_profile_default_model_resolves_when_model_omitted() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let prof_dir = data.join("profiles/openai/default/.config/opencode");
        std::fs::create_dir_all(&prof_dir).unwrap();
        std::fs::write(
            prof_dir.join("opencode.json"),
            r#"{"model":"openai/gpt-5.4"}"#,
        )
        .unwrap();

        let resolved =
            resolve_default_model_for_harness(data, "opencode", Some("openai/default"), None);
        assert_eq!(resolved.as_deref(), Some("openai/gpt-5.4"));
    }
}
