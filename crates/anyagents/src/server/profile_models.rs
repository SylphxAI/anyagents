//! Hydrated multi-harness model catalog builder for `GET /v1/profiles/models` (`agents(models)`).
//!
//! Ensures any authenticated agent or operator receives a live, non-empty model
//! catalog across configured profiles and harnesses — even on slim serverless
//! coordinators that do not bundle host CLI binaries.

use std::collections::BTreeMap;

use anyagents_core::Store;
use anyagents_harness::normalize_harness_name;
use serde_json::{Value, json};

/// Built-in default model entries per harness used when host CLI binaries are absent
/// on a slim coordinator and no live upstream OAuth list overrides them.
fn baseline_harness_models(harness: &str) -> Vec<(&'static str, &'static str, bool)> {
    match normalize_harness_name(harness) {
        "agy" => vec![
            ("gemini-4-argon-high", "Gemini 4 Argon (High)", true),
            ("gemini-4-neon-high", "Gemini 4 Neon (High)", false),
            ("gemini-3.8-flash-high", "Gemini 3.8 Flash (High)", false),
            ("claude-opus-5-5-high", "Claude Opus 5.5 (High)", false),
            ("claude-sonnet-5-5-high", "Claude Sonnet 5.5 (High)", false),
        ],
        "claude-code" => vec![
            ("claude-opus-4-6", "Claude Opus 4.6", true),
            ("claude-sonnet-4-6", "Claude Sonnet 4.6", false),
            ("claude-haiku-4-5", "Claude Haiku 4.5", false),
        ],
        "codex" => vec![
            ("gpt-5.4", "GPT-5.4", true),
            ("gpt-5.3-codex", "GPT-5.3 Codex", false),
        ],
        "opencode" => vec![
            ("anthropic/claude-sonnet-4-6", "Claude Sonnet 4.6", true),
            ("openai/gpt-5.4", "GPT-5.4", false),
        ],
        "gemini" => vec![
            ("gemini-2.5-pro", "Gemini 2.5 Pro", true),
            ("gemini-2.5-flash", "Gemini 2.5 Flash", false),
        ],
        "spiron" => vec![(crate::spiron::DEFAULT_SPIRON_MODEL, "Spiron Default", true)],
        "omp" => vec![("claude-sonnet-4-6", "Claude Sonnet 4.6", true)],
        _ => Vec::new(),
    }
}

fn provider_matches_filter(provider: &str, profile: &str, filter: Option<&str>) -> bool {
    let Some(f) = filter else {
        return true;
    };
    let full = format!("{provider}/{profile}");
    f.eq_ignore_ascii_case(provider)
        || f.eq_ignore_ascii_case(profile)
        || f.eq_ignore_ascii_case(&full)
        || (f.eq_ignore_ascii_case("agy") && provider == "google-antigravity")
        || (f.eq_ignore_ascii_case("claude-code") && provider == "anthropic")
        || (f.eq_ignore_ascii_case("codex") && provider == "openai")
        || (f.eq_ignore_ascii_case("gemini") && provider == "google")
}

fn harness_matches_filter(harness: &str, filter: Option<&str>) -> bool {
    let Some(f) = filter else {
        return true;
    };
    let norm = normalize_harness_name(harness);
    norm.eq_ignore_ascii_case(f)
        || normalize_harness_name(f) == norm
        || (norm == "agy" && f.eq_ignore_ascii_case("google-antigravity"))
        || (norm == "claude-code" && f.eq_ignore_ascii_case("anthropic"))
        || (norm == "codex" && f.eq_ignore_ascii_case("openai"))
}

/// Hydrate profiles from PostgreSQL `deployment_blobs(namespace='profiles')` when
/// running in Postgres mode, probe live provider catalogs, and merge configured
/// profiles and harness models so `agents(models)` never returns an empty catalog.
pub(crate) fn catalog_for_store(store: &Store, target: Option<&str>) -> Value {
    let data = store.data_dir();
    crate::create::hydrate_profiles_from_store_if_postgres(store, data);

    let filter = target.map(str::trim).filter(|s| {
        !s.is_empty() && !s.eq_ignore_ascii_case("models") && !s.eq_ignore_ascii_case("all")
    });

    let mut base = crate::create::probe_profile_models_catalog(data, filter);
    let mut agy_cli_models = base
        .get("agy_cli_models")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut profiles = base
        .get("profiles")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut seen_profiles: std::collections::HashSet<String> = profiles
        .iter()
        .filter_map(|p| p.get("profile").and_then(Value::as_str).map(str::to_string))
        .collect();

    // Collect observed models per harness from live agents.
    let mut harness_models: BTreeMap<String, BTreeMap<String, Value>> = BTreeMap::new();
    for h in [
        "claude-code",
        "codex",
        "opencode",
        "agy",
        "gemini",
        "omp",
        "spiron",
    ] {
        if !harness_matches_filter(h, filter) {
            continue;
        }
        let entry = harness_models.entry(h.to_string()).or_default();
        for (id, label, is_default) in baseline_harness_models(h) {
            entry.insert(
                id.to_string(),
                json!({
                    "id": id,
                    "label": label,
                    "default": is_default,
                    "source": "catalog",
                }),
            );
        }
    }

    if let Ok(live) = store.list_live_agents() {
        for agent in live {
            let norm = normalize_harness_name(&agent.harness).to_string();
            if !harness_matches_filter(&norm, filter) {
                continue;
            }
            if let Some(m) = agent
                .model
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                harness_models
                    .entry(norm)
                    .or_default()
                    .entry(m.to_string())
                    .or_insert_with(|| {
                        json!({
                            "id": m,
                            "label": m,
                            "default": false,
                            "source": "configured_agent",
                        })
                    });
            }
        }
    }

    // Merge live `agy_cli_models` into the `agy` harness map, or populate fallback
    // `agy_cli_models` when the coordinator has no host `agy` CLI binary.
    if agy_cli_models.is_empty() && harness_matches_filter("agy", filter) {
        if let Some(agy_map) = harness_models.get("agy") {
            agy_cli_models = agy_map.values().cloned().collect();
        }
    } else if !agy_cli_models.is_empty() && harness_matches_filter("agy", filter) {
        let agy_map = harness_models.entry("agy".to_string()).or_default();
        for item in &agy_cli_models {
            if let Some(id) = item.get("id").and_then(Value::as_str) {
                agy_map.insert(id.to_string(), item.clone());
            }
        }
    }

    // Enumerate all hydrated/local provider profiles (not only `google-antigravity`).
    let profiles_root = data.join("profiles");
    if let Ok(providers) = std::fs::read_dir(&profiles_root) {
        let mut prov_list: Vec<_> = providers.flatten().collect();
        prov_list.sort_by_key(|e| e.file_name());
        for prov_entry in prov_list {
            let prov_path = prov_entry.path();
            if !prov_path.is_dir() {
                continue;
            }
            let provider = prov_entry.file_name().to_string_lossy().into_owned();
            let Ok(prof_dirs) = std::fs::read_dir(&prov_path) else {
                continue;
            };
            let mut prof_list: Vec<_> = prof_dirs.flatten().collect();
            prof_list.sort_by_key(|e| e.file_name());
            for prof_entry in prof_list {
                let prof_dir = prof_entry.path();
                if !prof_dir.is_dir() {
                    continue;
                }
                let profile = prof_entry.file_name().to_string_lossy().into_owned();
                if profile.starts_with('.') {
                    continue;
                }
                if !provider_matches_filter(&provider, &profile, filter) {
                    continue;
                }
                let full_name = format!("{provider}/{profile}");
                if !seen_profiles.insert(full_name.clone()) {
                    continue;
                }
                let compatible = crate::create::compatible_harnesses_for(&provider);
                let mut models = Vec::new();
                for &h in compatible {
                    if let Some(map) = harness_models.get(h) {
                        for m in map.values() {
                            if !models
                                .iter()
                                .any(|existing: &Value| existing["id"] == m["id"])
                            {
                                models.push(m.clone());
                            }
                        }
                    }
                }
                profiles.push(json!({
                    "profile": full_name,
                    "provider": provider,
                    "compatible_harnesses": compatible,
                    "models": models,
                }));
            }
        }
    }

    // Ensure any `google-antigravity` profile entry with empty `cli_models` receives the
    // synthesized `agy_cli_models` list.
    for p in &mut profiles {
        if let Some(obj) = p.as_object_mut()
            && obj.get("provider").and_then(Value::as_str) == Some("google-antigravity")
            && obj
                .get("cli_models")
                .and_then(Value::as_array)
                .is_some_and(Vec::is_empty)
            && !agy_cli_models.is_empty()
        {
            obj.insert(
                "cli_models".to_string(),
                Value::Array(agy_cli_models.clone()),
            );
        }
    }

    let harnesses: Value = Value::Object(
        harness_models
            .into_iter()
            .map(|(h, map)| (h, Value::Array(map.into_values().collect())))
            .collect(),
    );

    if let Some(obj) = base.as_object_mut() {
        obj.insert("agy_cli_models".to_string(), Value::Array(agy_cli_models));
        obj.insert("profiles".to_string(), Value::Array(profiles));
        obj.insert("harnesses".to_string(), harnesses);
        base
    } else {
        json!({
            "agy_cli_models": agy_cli_models,
            "profiles": profiles,
            "harnesses": harnesses,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::NewAgent;

    #[test]
    fn catalog_returns_non_empty_models_across_harnesses_and_profiles_without_host_cli() {
        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path();
        let store = Store::open(data.join("state.db")).unwrap();

        let mut ag = NewAgent::new("director-a", "claude-code");
        ag.model = Some("claude-opus-4-6-custom".into());
        let created = store.create_agent(ag).unwrap();
        store.set_agent_running(&created.id, "/work").unwrap();

        std::fs::create_dir_all(data.join("profiles/anthropic/work")).unwrap();
        std::fs::create_dir_all(data.join("profiles/openai/default")).unwrap();

        let cat = catalog_for_store(&store, None);
        assert!(
            !cat["agy_cli_models"].as_array().unwrap().is_empty(),
            "agy_cli_models must not be empty"
        );
        let profiles = cat["profiles"].as_array().unwrap();
        assert!(
            profiles.iter().any(|p| p["profile"] == "anthropic/work"),
            "expected anthropic/work profile in catalog: {profiles:?}"
        );
        assert!(
            profiles.iter().any(|p| p["profile"] == "openai/default"),
            "expected openai/default profile in catalog: {profiles:?}"
        );
        let claude_models = cat["harnesses"]["claude-code"].as_array().unwrap();
        assert!(
            claude_models
                .iter()
                .any(|m| m["id"] == "claude-opus-4-6-custom"),
            "expected configured agent model in catalog"
        );

        let filtered = catalog_for_store(&store, Some("codex"));
        assert!(filtered["harnesses"].get("codex").is_some());
        assert!(filtered["harnesses"].get("agy").is_none());
    }
}
