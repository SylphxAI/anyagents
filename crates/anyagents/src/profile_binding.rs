//! Harness/model/profile triple validation and configured default profile binding.
//!
//! # Model catalogue
//!
//! A provider profile declares the models its endpoint serves in its own
//! `profile.json` under `models` (an array of model ids). That declaration is
//! the only catalogue authority: a triple whose model is not listed is refused,
//! and a profile whose `profile.json` is unreadable, malformed or carries no
//! `models` array is refused too (fail closed). Harness-written model caches
//! inside a profile directory are never consulted, because profile provisioning
//! copies them between profiles and they therefore say nothing about the
//! endpoint the profile is bound to.
//!
//! Matching is exact. The effort suffix (`<model>#<effort>`) is dropped, and a
//! model id carrying the profile's provider as its first path segment also
//! matches the catalogue entry without that segment.
//!
//! # Default binding
//!
//! When a create, spawn or harness/model switch names no profile, the profile
//! comes from `<data>/profile-bindings.json`:
//!
//! ```json
//! {
//!   "bindings": [
//!     { "harness": "h", "model": "m", "profiles": ["p/a", "p/b"], "selection": "quota" },
//!     { "harness": "h", "profiles": ["p/c"] }
//!   ]
//! }
//! ```
//!
//! A binding with a `model` (effort suffix ignored) wins over a harness-only
//! binding. Candidates must exist, be compatible with the harness, serve the
//! model and hold usable credentials. `selection: "ordered"` (default) takes
//! the first eligible candidate; `selection: "quota"` reads every eligible
//! candidate's live remaining quota, drops exhausted ones, and picks the best
//! remaining-quota-per-assigned-agent score. A quota binding refuses when no
//! candidate's quota can be read. No binding, or no eligible candidate, is a
//! refusal naming the reason; there is no implicit profile name.
//!
//! An installation without a `<data>/profiles` directory has no provider
//! profiles at all; harnesses then use their own native sign-in and none of
//! these checks apply.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, Error, agent_config_dir};
use anyagents_harness::normalize_harness_name;

pub(crate) const BINDINGS_FILE: &str = "profile-bindings.json";

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingsConfig {
    #[serde(default)]
    bindings: Vec<Binding>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    harness: String,
    #[serde(default)]
    model: Option<String>,
    profiles: Vec<String>,
    #[serde(default)]
    selection: Selection,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum Selection {
    #[default]
    Ordered,
    Quota,
}

/// True when the installation manages provider profiles at all.
pub(crate) fn profiles_enabled(data: &Path) -> bool {
    data.join("profiles").is_dir()
}

/// `<model>#<effort>` without the effort suffix.
pub(crate) fn base_model_id(model: &str) -> &str {
    let model = model.trim();
    model.split_once('#').map_or(model, |(base, _)| base).trim()
}

fn profile_dir(data: &Path, provider: &str, profile: &str) -> PathBuf {
    data.join("profiles").join(provider).join(profile)
}

fn split_profile(full: &str) -> Result<(&str, &str), Error> {
    full.trim()
        .split_once('/')
        .map(|(p, n)| (p.trim(), n.trim()))
        .filter(|(p, n)| !p.is_empty() && !n.is_empty())
        .ok_or_else(|| {
            Error::Message(format!(
                "profile `{full}` must be written as `<provider>/<profile>`"
            ))
        })
}

/// Reads the declared model catalogue of a profile. Fails closed.
fn read_catalogue(data: &Path, provider: &str, profile: &str) -> Result<Vec<String>, Error> {
    let path = profile_dir(data, provider, profile).join("profile.json");
    let text = std::fs::read_to_string(&path).map_err(|err| {
        Error::Message(format!(
            "cannot read the model catalogue of profile `{provider}/{profile}` ({err}); refusing"
        ))
    })?;
    let value: serde_json::Value = serde_json::from_str(&text).map_err(|err| {
        Error::Message(format!(
            "model catalogue of profile `{provider}/{profile}` is not valid JSON ({err}); refusing"
        ))
    })?;
    let Some(models) = value.get("models") else {
        return Err(Error::Message(format!(
            "profile `{provider}/{profile}` declares no model catalogue; list the models it serves under `models` in its profile.json"
        )));
    };
    let list = models.as_array().ok_or_else(|| {
        Error::Message(format!(
            "model catalogue of profile `{provider}/{profile}` must be an array of model ids; refusing"
        ))
    })?;
    list.iter()
        .map(|entry| {
            entry
                .as_str()
                .map(base_model_id)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    Error::Message(format!(
                        "model catalogue of profile `{provider}/{profile}` has a non-string or empty entry; refusing"
                    ))
                })
        })
        .collect()
}

/// Ok when `provider/profile` serves `model` according to its declared catalogue.
/// An unspecified model (the harness picks its own default) has nothing to check.
pub(crate) fn ensure_profile_serves_model(
    data: &Path,
    provider: &str,
    profile: &str,
    model: Option<&str>,
) -> Result<(), Error> {
    let Some(model) = model.map(base_model_id).filter(|m| !m.is_empty()) else {
        return Ok(());
    };
    let catalogue = read_catalogue(data, provider, profile)?;
    let unprefixed = model
        .split_once('/')
        .filter(|(prefix, rest)| *prefix == provider && !rest.is_empty())
        .map(|(_, rest)| rest);
    if catalogue
        .iter()
        .any(|entry| entry == model || Some(entry.as_str()) == unprefixed)
    {
        return Ok(());
    }
    Err(Error::Message(format!(
        "profile `{provider}/{profile}` does not serve model `{model}` (its catalogue: {})",
        if catalogue.is_empty() {
            "empty".to_string()
        } else {
            catalogue.join(", ")
        }
    )))
}

/// Full triple check: the profile exists, accepts the harness and serves the model.
pub(crate) fn validate_triple(
    data: &Path,
    harness: &str,
    model: Option<&str>,
    provider: &str,
    profile: &str,
) -> Result<(), Error> {
    if !profiles_enabled(data) {
        return Ok(());
    }
    if !profile_dir(data, provider, profile).is_dir() {
        return Err(Error::Message(format!(
            "provider profile not found: `{provider}/{profile}`"
        )));
    }
    let harness = normalize_harness_name(harness);
    crate::create::ensure_profile_compatible_and_mapped(
        data, provider, profile, harness, model, None,
    )?;
    ensure_profile_serves_model(data, provider, profile, model)
}

/// Positive evidence that a profile holds a credential its harness can use.
/// Missing, empty, malformed or expired-without-refresh material is not evidence.
pub(crate) fn profile_has_usable_credentials(dir: &Path) -> bool {
    let read_json = |rel: &str| -> Option<serde_json::Value> {
        let text = std::fs::read_to_string(dir.join(rel)).ok()?;
        serde_json::from_str(&text).ok()
    };
    let non_empty = |v: Option<&serde_json::Value>| {
        v.and_then(serde_json::Value::as_str)
            .is_some_and(|s| !s.trim().is_empty())
    };
    let now_ms = anyagents_core::now_ms();
    for rel in [".credentials.json", ".claude/.credentials.json"] {
        if let Some(v) = read_json(rel)
            && let Some(oauth) = v.get("claudeAiOauth")
        {
            if non_empty(oauth.get("refreshToken")) {
                return true;
            }
            let expires = oauth
                .get("expiresAt")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0);
            if non_empty(oauth.get("accessToken")) && expires > now_ms {
                return true;
            }
        }
    }
    if let Some(v) = read_json(".gemini/antigravity-cli/antigravity-oauth-token")
        && (non_empty(v.pointer("/token/refresh_token")) || non_empty(v.get("refresh_token")))
    {
        return true;
    }
    for rel in ["auth.json", ".codex/auth.json"] {
        if let Some(v) = read_json(rel)
            && (non_empty(v.pointer("/tokens/refresh_token")) || non_empty(v.get("OPENAI_API_KEY")))
        {
            return true;
        }
    }
    let key_dirs = [dir.join(".config/opencode"), dir.join(".config/sylphx")];
    for key_dir in key_dirs {
        let Ok(entries) = std::fs::read_dir(&key_dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let is_key = name == "key" || path.extension().is_some_and(|ext| ext == "key");
            if is_key
                && path.is_file()
                && std::fs::read_to_string(&path).is_ok_and(|s| !s.trim().is_empty())
            {
                return true;
            }
        }
    }
    false
}

fn load_bindings(data: &Path) -> Result<Option<BindingsConfig>, Error> {
    let path = data.join(BINDINGS_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(Error::Message(format!(
                "cannot read {BINDINGS_FILE} ({err}); refusing to pick a profile"
            )));
        }
    };
    serde_json::from_str(&text).map(Some).map_err(|err| {
        Error::Message(format!(
            "{BINDINGS_FILE} is malformed ({err}); refusing to pick a profile"
        ))
    })
}

fn find_binding<'a>(
    cfg: &'a BindingsConfig,
    harness: &str,
    model: Option<&str>,
) -> Option<&'a Binding> {
    let model = model.map(base_model_id).filter(|m| !m.is_empty());
    let same_harness = |b: &&Binding| normalize_harness_name(b.harness.trim()) == harness;
    let with_model = cfg.bindings.iter().filter(same_harness).find(|b| {
        b.model
            .as_deref()
            .map(base_model_id)
            .filter(|m| !m.is_empty())
            .is_some_and(|m| Some(m) == model)
    });
    with_model.or_else(|| {
        cfg.bindings
            .iter()
            .filter(same_harness)
            .find(|b| b.model.as_deref().map(str::trim).is_none_or(str::is_empty))
    })
}

/// Number of live agents currently bound to each `provider/profile`.
fn assignment_load(data: &Path, agents: &[Agent], full: &str) -> usize {
    agents
        .iter()
        .filter(|a| a.state != "archived")
        .filter(|a| {
            crate::create::bound_profile_name(&agent_config_dir(data, &a.id)).as_deref()
                == Some(full)
        })
        .count()
}

/// Picks the default `provider/profile` for a harness/model with no explicit profile.
///
/// Returns `Ok(None)` only when the installation has no provider profiles.
/// `read_quota(provider, profile)` returns the remaining quota percent or `None`
/// when it cannot be read.
pub(crate) fn resolve_default_profile_with(
    data: &Path,
    agents: &[Agent],
    harness: &str,
    model: Option<&str>,
    read_quota: &dyn Fn(&str, &str) -> Option<i64>,
) -> Result<Option<String>, Error> {
    if !profiles_enabled(data) {
        return Ok(None);
    }
    let harness = normalize_harness_name(harness);
    let model_label = model.map(base_model_id).unwrap_or("(harness default)");
    let no_binding = || {
        Error::Message(format!(
            "no profile given and no profile binding in {BINDINGS_FILE} for harness `{harness}` model `{model_label}`; name a profile or add a binding"
        ))
    };
    let cfg = load_bindings(data)?.ok_or_else(no_binding)?;
    let binding = find_binding(&cfg, harness, model).ok_or_else(no_binding)?;

    let mut rejected: Vec<String> = Vec::new();
    let mut eligible: Vec<(String, String, String)> = Vec::new();
    for full in &binding.profiles {
        let (provider, profile) = match split_profile(full) {
            Ok(pair) => pair,
            Err(err) => {
                rejected.push(err.to_string());
                continue;
            }
        };
        if let Err(err) = validate_triple(data, harness, model, provider, profile) {
            rejected.push(err.to_string());
            continue;
        }
        if !profile_has_usable_credentials(&profile_dir(data, provider, profile)) {
            rejected.push(format!(
                "profile `{provider}/{profile}` holds no usable credentials"
            ));
            continue;
        }
        eligible.push((
            format!("{provider}/{profile}"),
            provider.to_string(),
            profile.to_string(),
        ));
    }
    let refuse = |why: &str, rejected: &[String]| {
        let detail = if rejected.is_empty() {
            String::new()
        } else {
            format!(": {}", rejected.join("; "))
        };
        Error::Message(format!(
            "no eligible profile for harness `{harness}` model `{model_label}` ({why}){detail}"
        ))
    };
    if eligible.is_empty() {
        return Err(refuse("every bound profile was rejected", &rejected));
    }
    match binding.selection {
        Selection::Ordered => Ok(eligible.into_iter().next().map(|(full, _, _)| full)),
        Selection::Quota => {
            // (full, score, remaining, list order)
            let mut best: Option<(String, i64, i64)> = None;
            let mut readable = 0usize;
            for (full, provider, profile) in &eligible {
                let Some(remaining) =
                    read_quota(provider, profile).filter(|p| (0..=100).contains(p))
                else {
                    rejected.push(format!("quota of `{full}` is unreadable"));
                    continue;
                };
                readable += 1;
                if remaining == 0 {
                    rejected.push(format!("`{full}` has no quota left"));
                    continue;
                }
                let load = assignment_load(data, agents, full) as i64;
                let score = remaining * 100 / (load + 1);
                if best
                    .as_ref()
                    .is_none_or(|(_, s, r)| (score, remaining) > (*s, *r))
                {
                    best = Some((full.clone(), score, remaining));
                }
            }
            match best {
                Some((full, _, _)) => Ok(Some(full)),
                None if readable == 0 => Err(refuse(
                    "quota could not be read for any candidate",
                    &rejected,
                )),
                None => Err(refuse("every candidate is out of quota", &rejected)),
            }
        }
    }
}

/// Production entry point: quota is read live from each candidate profile.
pub(crate) fn resolve_default_profile(
    data: &Path,
    agents: &[Agent],
    harness: &str,
    model: Option<&str>,
) -> Result<Option<String>, Error> {
    let model_owned = model.map(str::to_string);
    resolve_default_profile_with(data, agents, harness, model, &|provider, profile| {
        crate::create::probe_profile_remaining_pct(data, provider, profile, model_owned.as_deref())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    fn profile(data: &Path, full: &str, models: Option<&[&str]>, key: bool) {
        let (provider, name) = full.split_once('/').unwrap();
        let dir = data.join("profiles").join(provider).join(name);
        let mut meta = serde_json::json!({
            "provider": provider,
            "kind": "api-key",
            "compatible_harnesses": ["alpha", "spiron"],
        });
        if let Some(models) = models {
            meta["models"] = serde_json::json!(models);
        }
        write(&dir.join("profile.json"), &meta.to_string());
        if key {
            write(&dir.join(".config/opencode/vendor.key"), "k-123\n");
        }
    }

    fn bindings(data: &Path, value: serde_json::Value) {
        write(&data.join(BINDINGS_FILE), &value.to_string());
    }

    fn no_quota(_: &str, _: &str) -> Option<i64> {
        None
    }

    #[test]
    fn catalogue_accepts_listed_model_with_effort_and_provider_prefix() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        profile(data, "vendor/one", Some(&["lab/model-a"]), true);
        ensure_profile_serves_model(data, "vendor", "one", Some("vendor/lab/model-a#xhigh"))
            .unwrap();
        ensure_profile_serves_model(data, "vendor", "one", Some("lab/model-a")).unwrap();
        ensure_profile_serves_model(data, "vendor", "one", None).unwrap();
    }

    #[test]
    fn catalogue_refuses_unlisted_model_and_near_matches() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        profile(data, "vendor/one", Some(&["lab/model-a"]), true);
        for model in [
            "vendor/lab/model-b",
            "lab/model-a-pro",
            "other/lab/model-a",
            "model-a",
        ] {
            let err = ensure_profile_serves_model(data, "vendor", "one", Some(model))
                .unwrap_err()
                .to_string();
            assert!(err.contains("does not serve model"), "{model}: {err}");
        }
    }

    #[test]
    fn catalogue_fails_closed_when_missing_or_unreadable() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        profile(data, "vendor/undeclared", None, true);
        let err = ensure_profile_serves_model(data, "vendor", "undeclared", Some("lab/m"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("declares no model catalogue"), "{err}");

        write(
            &data.join("profiles/vendor/broken/profile.json"),
            "{not json",
        );
        let err = ensure_profile_serves_model(data, "vendor", "broken", Some("lab/m"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("not valid JSON"), "{err}");

        write(
            &data.join("profiles/vendor/shape/profile.json"),
            r#"{"models":"lab/m"}"#,
        );
        let err = ensure_profile_serves_model(data, "vendor", "shape", Some("lab/m"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("must be an array"), "{err}");

        let err = ensure_profile_serves_model(data, "vendor", "absent", Some("lab/m"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot read the model catalogue"), "{err}");
    }

    #[test]
    fn validate_triple_is_inert_without_provider_profiles() {
        let dir = crate::test_tempdir();
        validate_triple(dir.path(), "alpha", Some("lab/m"), "vendor", "none").unwrap();
        assert_eq!(
            resolve_default_profile_with(dir.path(), &[], "alpha", Some("lab/m"), &no_quota)
                .unwrap(),
            None
        );
    }

    #[test]
    fn validate_triple_refuses_incompatible_harness_and_missing_profile() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        profile(data, "vendor/one", Some(&["lab/m"]), true);
        let err = validate_triple(data, "codex", Some("lab/m"), "vendor", "one")
            .unwrap_err()
            .to_string();
        assert!(err.contains("not compatible"), "{err}");
        let err = validate_triple(data, "alpha", Some("lab/m"), "vendor", "gone")
            .unwrap_err()
            .to_string();
        assert!(err.contains("not found"), "{err}");
        validate_triple(data, "alpha", Some("lab/m"), "vendor", "one").unwrap();
    }

    #[test]
    fn default_binding_prefers_model_binding_and_skips_ineligible_candidates() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        profile(data, "vendor/general", Some(&["lab/a", "lab/b"]), true);
        profile(data, "vendor/signed-out", Some(&["lab/b"]), false);
        profile(data, "vendor/wrong-catalogue", Some(&["lab/a"]), true);
        profile(data, "vendor/special", Some(&["lab/b"]), true);
        bindings(
            data,
            serde_json::json!({"bindings": [
                {"harness": "alpha", "profiles": ["vendor/general"]},
                {"harness": "alpha", "model": "lab/b#high",
                 "profiles": ["vendor/signed-out", "vendor/wrong-catalogue", "vendor/special"]}
            ]}),
        );
        let picked =
            resolve_default_profile_with(data, &[], "alpha", Some("lab/b#xhigh"), &no_quota)
                .unwrap();
        assert_eq!(picked.as_deref(), Some("vendor/special"));
        let picked =
            resolve_default_profile_with(data, &[], "alpha", Some("lab/a"), &no_quota).unwrap();
        assert_eq!(picked.as_deref(), Some("vendor/general"));
    }

    #[test]
    fn default_binding_refuses_without_binding_or_eligible_candidate() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        profile(data, "vendor/default", Some(&["lab/a"]), false);
        let err = resolve_default_profile_with(data, &[], "alpha", Some("lab/a"), &no_quota)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no profile binding"), "{err}");

        bindings(
            data,
            serde_json::json!({"bindings": [{"harness": "alpha", "profiles": ["vendor/default"]}]}),
        );
        let err = resolve_default_profile_with(data, &[], "alpha", Some("lab/a"), &no_quota)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no usable credentials"), "{err}");

        write(
            &data.join(BINDINGS_FILE),
            "{\"bindings\": [{\"harnes\": 1}]}",
        );
        let err = resolve_default_profile_with(data, &[], "alpha", Some("lab/a"), &no_quota)
            .unwrap_err()
            .to_string();
        assert!(err.contains("malformed"), "{err}");
    }

    #[test]
    fn quota_selection_picks_best_remaining_share_and_skips_unreadable() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        for name in ["vendor/s1", "vendor/s2", "vendor/s3", "vendor/s4"] {
            profile(data, name, Some(&["lab/a"]), true);
        }
        bindings(
            data,
            serde_json::json!({"bindings": [{"harness": "alpha", "selection": "quota",
                "profiles": ["vendor/s1", "vendor/s2", "vendor/s3", "vendor/s4"]}]}),
        );
        let quota = |_: &str, profile: &str| match profile {
            "s1" => Some(0),
            "s2" => Some(40),
            "s3" => None,
            "s4" => Some(90),
            _ => None,
        };
        let picked =
            resolve_default_profile_with(data, &[], "alpha", Some("lab/a"), &quota).unwrap();
        assert_eq!(picked.as_deref(), Some("vendor/s4"));
    }

    #[test]
    fn quota_selection_weighs_current_assignment_load() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        let store = anyagents_core::Store::open(data.join("state.db")).unwrap();
        profile(data, "vendor/busy", Some(&["lab/a"]), true);
        profile(data, "vendor/idle", Some(&["lab/a"]), true);
        bindings(
            data,
            serde_json::json!({"bindings": [{"harness": "alpha", "selection": "quota",
                "profiles": ["vendor/busy", "vendor/idle"]}]}),
        );
        let mut agents = Vec::new();
        for i in 0..3 {
            let agent = store
                .create_agent(anyagents_core::NewAgent::new(format!("w{i}"), "alpha"))
                .unwrap();
            write(
                &agent_config_dir(data, &agent.id).join(".anyagents-profile"),
                "vendor/busy\n",
            );
            agents.push(agent);
        }
        // busy: 90 / 4 = 22.5 share; idle: 50 / 1 = 50 share.
        let quota = |_: &str, profile: &str| match profile {
            "busy" => Some(90),
            _ => Some(50),
        };
        let picked =
            resolve_default_profile_with(data, &agents, "alpha", Some("lab/a"), &quota).unwrap();
        assert_eq!(picked.as_deref(), Some("vendor/idle"));
    }

    #[test]
    fn quota_selection_fails_closed_when_no_quota_is_readable_or_left() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        profile(data, "vendor/s1", Some(&["lab/a"]), true);
        profile(data, "vendor/s2", Some(&["lab/a"]), true);
        bindings(
            data,
            serde_json::json!({"bindings": [{"harness": "alpha", "selection": "quota",
                "profiles": ["vendor/s1", "vendor/s2"]}]}),
        );
        let err = resolve_default_profile_with(data, &[], "alpha", Some("lab/a"), &no_quota)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("quota could not be read for any candidate"),
            "{err}"
        );
        let empty = |_: &str, _: &str| Some(0);
        let err = resolve_default_profile_with(data, &[], "alpha", Some("lab/a"), &empty)
            .unwrap_err()
            .to_string();
        assert!(err.contains("out of quota"), "{err}");
    }

    #[test]
    fn usable_credentials_require_positive_evidence() {
        let dir = crate::test_tempdir();
        let root = dir.path();
        assert!(!profile_has_usable_credentials(root));
        write(&root.join(".config/opencode/vendor.key"), "  \n");
        assert!(!profile_has_usable_credentials(root));
        write(
            &root.join(".credentials.json"),
            r#"{"claudeAiOauth":{"accessToken":"t","expiresAt":1}}"#,
        );
        assert!(!profile_has_usable_credentials(root));
        write(
            &root.join(".credentials.json"),
            r#"{"claudeAiOauth":{"accessToken":"t","refreshToken":"r","expiresAt":1}}"#,
        );
        assert!(profile_has_usable_credentials(root));
    }
}
