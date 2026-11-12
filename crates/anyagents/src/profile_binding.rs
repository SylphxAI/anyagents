//! Harness/model/profile triple validation and default profile selection.
//!
//! A triple is valid when the profile exists, accepts the harness, and its
//! model catalogue (see [`crate::model_catalogue`]) serves the model. Create,
//! spawn and every harness/model switch validate the final triple and refuse
//! with the reason, so a mismatch never reaches an agent's first turn.
//!
//! # Default profile (no explicit profile)
//!
//! When `<data>/profile-bindings.json` exists it decides:
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
//! binding; no matching binding is a refusal. `selection: "ordered"` (default)
//! takes the first eligible candidate. `selection: "quota"` reads each eligible
//! candidate's live remaining quota, drops exhausted ones, and picks the best
//! remaining-quota-per-assigned-agent score; it refuses when no candidate's
//! quota can be read.
//!
//! Without the file, the provider's own profiles are candidates in the
//! existing order (`default` first, then the rest by name), preferring ones
//! without fresh unhealthy evidence.
//!
//! Either way a candidate is eligible only when the triple is valid and the
//! profile is authenticated: its catalogue was read live with its own
//! credentials, or it holds usable credential material. No eligible candidate
//! is a refusal naming every rejection.
//!
//! An installation without a `<data>/profiles` directory has no provider
//! profiles; harnesses use their own native sign-in and none of this applies.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, Error, agent_config_dir};
use anyagents_harness::normalize_harness_name;

pub(crate) use crate::model_catalogue::base_model_id;
use crate::model_catalogue::{Catalogue, Source};

pub(crate) const BINDINGS_FILE: &str = "profile-bindings.json";

/// Reads a profile's catalogue: `(provider, profile)`.
pub(crate) type CatalogueFn<'a> = &'a dyn Fn(&str, &str) -> Result<Catalogue, Error>;
/// Reads a profile's remaining quota percent: `(provider, profile)`.
pub(crate) type QuotaFn<'a> = &'a dyn Fn(&str, &str) -> Option<i64>;

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

fn live_catalogue(data: &Path) -> impl Fn(&str, &str) -> Result<Catalogue, Error> + '_ {
    move |provider, profile| crate::model_catalogue::resolve(data, provider, profile)
}

fn serves_with(
    provider: &str,
    profile: &str,
    model: Option<&str>,
    catalogue: CatalogueFn<'_>,
) -> Result<Option<Source>, Error> {
    let Some(model) = model.map(base_model_id).filter(|m| !m.is_empty()) else {
        // The harness picks its own default model; there is nothing to check.
        return Ok(None);
    };
    let cat = catalogue(provider, profile)?;
    if crate::model_catalogue::serves(&cat.models, model) {
        return Ok(Some(cat.source));
    }
    let mut listed = cat.models.clone();
    listed.sort();
    Err(Error::Message(format!(
        "profile `{provider}/{profile}` does not serve model `{model}` ({} catalogue: {})",
        cat.source.label(),
        if listed.is_empty() {
            "empty".to_string()
        } else {
            listed.join(", ")
        }
    )))
}

/// Ok when `provider/profile` serves `model`. An unspecified model passes.
pub(crate) fn ensure_profile_serves_model(
    data: &Path,
    provider: &str,
    profile: &str,
    model: Option<&str>,
) -> Result<(), Error> {
    serves_with(provider, profile, model, &live_catalogue(data)).map(|_| ())
}

fn validate_with(
    data: &Path,
    harness: &str,
    model: Option<&str>,
    provider: &str,
    profile: &str,
    catalogue: CatalogueFn<'_>,
) -> Result<Option<Source>, Error> {
    if !profiles_enabled(data) {
        return Ok(None);
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
    serves_with(provider, profile, model, catalogue)
}

/// Full triple check: the profile exists, accepts the harness and serves the model.
pub(crate) fn validate_triple(
    data: &Path,
    harness: &str,
    model: Option<&str>,
    provider: &str,
    profile: &str,
) -> Result<(), Error> {
    validate_with(
        data,
        harness,
        model,
        provider,
        profile,
        &live_catalogue(data),
    )
    .map(|_| ())
}

/// Positive evidence that a profile directory holds a credential a harness can use.
/// Missing, empty, malformed or expired-without-refresh material is not evidence.
pub(crate) fn profile_has_usable_credentials(dir: &Path) -> bool {
    let read_json = |rel: &str| -> Option<serde_json::Value> {
        serde_json::from_str(&std::fs::read_to_string(dir.join(rel)).ok()?).ok()
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
    for rel in [
        "auth.json",
        ".codex/auth.json",
        ".spiron/auth.json",
        ".spiron/credentials.json",
        "credentials.json",
    ] {
        if let Some(v) = read_json(rel)
            && (non_empty(v.pointer("/tokens/refresh_token"))
                || non_empty(v.get("OPENAI_API_KEY"))
                || non_empty(v.get("access_token"))
                || non_empty(v.get("api_key"))
                || v.get("project_keys")
                    .and_then(serde_json::Value::as_object)
                    .is_some_and(|m| m.values().any(|k| non_empty(Some(k)))))
        {
            return true;
        }
    }
    for key_dir in [
        dir.join(".config/opencode"),
        dir.join(".config/sylphx"),
        dir.join(".spiron"),
    ] {
        let Ok(entries) = std::fs::read_dir(&key_dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let is_key =
                entry.file_name() == "key" || path.extension().is_some_and(|ext| ext == "key");
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

/// Number of live agents currently bound to `provider/profile`.
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

/// Candidates without a bindings file: the provider's own profiles, `default` first.
fn provider_candidates(data: &Path, harness: &str, model: Option<&str>) -> Vec<String> {
    let provider = crate::create::upstream_provider_of(harness, model);
    let mut out: Vec<String> = crate::create::list_harness_profiles(data, &provider)
        .into_iter()
        .filter_map(|(name, dir)| {
            let prov = dir.parent()?.file_name()?.to_str()?.to_string();
            // Only real provider profile directories are candidates.
            (dir == profile_dir(data, &prov, &name)).then(|| format!("{prov}/{name}"))
        })
        .collect();
    out.sort_by_key(|full| (!full.ends_with("/default"), full.clone()));
    out.dedup();
    out
}

/// Picks the default `provider/profile` for a harness/model with no explicit profile.
/// Returns `Ok(None)` only when the installation has no provider profiles.
pub(crate) fn resolve_default_profile_with(
    data: &Path,
    agents: &[Agent],
    harness: &str,
    model: Option<&str>,
    catalogue: CatalogueFn<'_>,
    read_quota: QuotaFn<'_>,
) -> Result<Option<String>, Error> {
    if !profiles_enabled(data) {
        return Ok(None);
    }
    let harness = normalize_harness_name(harness);
    let model_label = model.map(base_model_id).unwrap_or("(harness default)");
    let (candidates, selection, from_binding) = match load_bindings(data)? {
        Some(cfg) => {
            let binding = find_binding(&cfg, harness, model).ok_or_else(|| {
                Error::Message(format!(
                    "no profile given and no binding in {BINDINGS_FILE} for harness `{harness}` model `{model_label}`; name a profile or add a binding"
                ))
            })?;
            (binding.profiles.clone(), binding.selection, true)
        }
        None => (
            provider_candidates(data, harness, model),
            Selection::Ordered,
            false,
        ),
    };

    let mut rejected: Vec<String> = Vec::new();
    let mut eligible: Vec<(String, String, String)> = Vec::new();
    for full in &candidates {
        let (provider, profile) = match split_profile(full) {
            Ok(pair) => pair,
            Err(err) => {
                rejected.push(err.to_string());
                continue;
            }
        };
        let source = match validate_with(data, harness, model, provider, profile, catalogue) {
            Ok(source) => source,
            Err(err) => {
                rejected.push(err.to_string());
                continue;
            }
        };
        let authenticated = source.as_ref().is_some_and(Source::authenticated)
            || profile_has_usable_credentials(&profile_dir(data, provider, profile));
        if !authenticated {
            rejected.push(format!(
                "profile `{provider}/{profile}` holds no usable credentials"
            ));
            continue;
        }
        eligible.push((
            full.trim().to_string(),
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
        let why = if candidates.is_empty() {
            "no candidate profiles"
        } else {
            "every candidate was rejected"
        };
        return Err(refuse(why, &rejected));
    }
    match selection {
        Selection::Ordered if from_binding => Ok(eligible.into_iter().next().map(|e| e.0)),
        Selection::Ordered => {
            let healthy = eligible.iter().find(|(_, provider, profile)| {
                !crate::create::profile_has_unhealthy_evidence(&profile_dir(
                    data, provider, profile,
                ))
            });
            Ok(healthy.or(eligible.first()).map(|e| e.0.clone()))
        }
        Selection::Quota => {
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

/// Production entry point: catalogues and quota are read live from each candidate.
pub(crate) fn resolve_default_profile(
    data: &Path,
    agents: &[Agent],
    harness: &str,
    model: Option<&str>,
) -> Result<Option<String>, Error> {
    let model_owned = model.map(str::to_string);
    resolve_default_profile_with(
        data,
        agents,
        harness,
        model,
        &live_catalogue(data),
        &|provider, profile| {
            crate::create::probe_profile_remaining_pct(
                data,
                provider,
                profile,
                model_owned.as_deref(),
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    /// A profile directory; `key` adds credential material.
    fn profile(data: &Path, full: &str, key: bool) {
        let (provider, name) = full.split_once('/').unwrap();
        let dir = data.join("profiles").join(provider).join(name);
        write(
            &dir.join("profile.json"),
            &serde_json::json!({
                "provider": provider,
                "kind": "api-key",
                "compatible_harnesses": ["alpha", "spiron"],
            })
            .to_string(),
        );
        if key {
            write(&dir.join(".config/opencode/vendor.key"), "k-123\n");
        }
    }

    fn bindings(data: &Path, value: serde_json::Value) {
        write(&data.join(BINDINGS_FILE), &value.to_string());
    }

    type Entries = &'static [(&'static str, &'static [&'static str])];

    /// Catalogues by profile name; `live` marks them as authenticated reads.
    fn catalogues(entries: Entries, live: bool) -> impl Fn(&str, &str) -> Result<Catalogue, Error> {
        move |_provider, profile| {
            entries
                .iter()
                .find(|(name, _)| *name == profile)
                .map(|(_, models)| Catalogue {
                    models: models.iter().map(|m| m.to_string()).collect(),
                    source: if live { Source::Live } else { Source::Declared },
                })
                .ok_or_else(|| Error::Message(format!("catalogue of `{profile}` is unknown")))
        }
    }

    fn no_quota(_: &str, _: &str) -> Option<i64> {
        None
    }

    #[test]
    fn validate_refuses_unserved_model_and_unknown_catalogue() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        profile(data, "vendor/one", true);
        let cat = catalogues(&[("one", &["lab/model-a"])], true);
        validate_with(
            data,
            "alpha",
            Some("vendor/lab/model-a#xhigh"),
            "vendor",
            "one",
            &cat,
        )
        .unwrap();
        validate_with(data, "alpha", None, "vendor", "one", &cat).unwrap();
        let err = validate_with(data, "alpha", Some("lab/model-b"), "vendor", "one", &cat)
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not serve model"), "{err}");
        let unknown = catalogues(&[], true);
        let err = validate_with(
            data,
            "alpha",
            Some("lab/model-a"),
            "vendor",
            "one",
            &unknown,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown"), "{err}");
    }

    #[test]
    fn validate_refuses_missing_profile_and_incompatible_harness() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        profile(data, "vendor/one", true);
        let cat = catalogues(&[("one", &["lab/m"])], true);
        let err = validate_with(data, "alpha", Some("lab/m"), "vendor", "gone", &cat)
            .unwrap_err()
            .to_string();
        assert!(err.contains("not found"), "{err}");
        let err = validate_with(data, "codex", Some("lab/m"), "vendor", "one", &cat)
            .unwrap_err()
            .to_string();
        assert!(err.contains("not compatible"), "{err}");
    }

    #[test]
    fn everything_is_inert_without_provider_profiles() {
        let dir = crate::test_tempdir();
        let cat = catalogues(&[], true);
        validate_with(dir.path(), "alpha", Some("lab/m"), "vendor", "none", &cat).unwrap();
        assert_eq!(
            resolve_default_profile_with(dir.path(), &[], "alpha", Some("lab/m"), &cat, &no_quota)
                .unwrap(),
            None
        );
    }

    #[test]
    fn without_bindings_the_provider_default_is_kept_only_when_it_serves_the_model() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        profile(data, "spiron/default", true);
        profile(data, "spiron/other", true);
        let cat = catalogues(
            &[("default", &["lab/a"]), ("other", &["lab/a", "lab/b"])],
            true,
        );
        let pick = |model: &str| {
            resolve_default_profile_with(data, &[], "spiron", Some(model), &cat, &no_quota)
        };
        assert_eq!(pick("lab/a").unwrap().as_deref(), Some("spiron/default"));
        assert_eq!(pick("lab/b").unwrap().as_deref(), Some("spiron/other"));
        let err = pick("lab/c").unwrap_err().to_string();
        assert!(err.contains("every candidate was rejected"), "{err}");
    }

    #[test]
    fn unauthenticated_profiles_are_never_picked() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        profile(data, "spiron/default", false);
        profile(data, "spiron/signed-in", true);
        let declared = catalogues(&[("default", &["lab/a"]), ("signed-in", &["lab/a"])], false);
        let picked =
            resolve_default_profile_with(data, &[], "spiron", Some("lab/a"), &declared, &no_quota)
                .unwrap();
        assert_eq!(picked.as_deref(), Some("spiron/signed-in"));

        std::fs::remove_dir_all(data.join("profiles/spiron/signed-in")).unwrap();
        let err =
            resolve_default_profile_with(data, &[], "spiron", Some("lab/a"), &declared, &no_quota)
                .unwrap_err()
                .to_string();
        assert!(err.contains("no usable credentials"), "{err}");

        // A catalogue read with the profile's own credentials proves authentication.
        let live = catalogues(&[("default", &["lab/a"])], true);
        let picked =
            resolve_default_profile_with(data, &[], "spiron", Some("lab/a"), &live, &no_quota)
                .unwrap();
        assert_eq!(picked.as_deref(), Some("spiron/default"));
    }

    #[test]
    fn binding_prefers_model_binding_and_skips_ineligible_candidates() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        for name in ["vendor/general", "vendor/wrong-catalogue", "vendor/special"] {
            profile(data, name, true);
        }
        let cat = catalogues(
            &[
                ("general", &["lab/a", "lab/b"]),
                ("wrong-catalogue", &["lab/a"]),
                ("special", &["lab/b"]),
            ],
            true,
        );
        bindings(
            data,
            serde_json::json!({"bindings": [
                {"harness": "alpha", "profiles": ["vendor/general"]},
                {"harness": "alpha", "model": "lab/b#high",
                 "profiles": ["vendor/missing", "vendor/wrong-catalogue", "vendor/special"]}
            ]}),
        );
        let pick =
            |model| resolve_default_profile_with(data, &[], "alpha", Some(model), &cat, &no_quota);
        assert_eq!(
            pick("lab/b#xhigh").unwrap().as_deref(),
            Some("vendor/special")
        );
        assert_eq!(pick("lab/a").unwrap().as_deref(), Some("vendor/general"));
    }

    #[test]
    fn binding_refuses_when_absent_for_the_harness_or_malformed() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        profile(data, "vendor/one", true);
        let cat = catalogues(&[("one", &["lab/a"])], true);
        bindings(
            data,
            serde_json::json!({"bindings": [{"harness": "spiron", "profiles": ["vendor/one"]}]}),
        );
        let err = resolve_default_profile_with(data, &[], "alpha", Some("lab/a"), &cat, &no_quota)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no binding"), "{err}");
        write(
            &data.join(BINDINGS_FILE),
            "{\"bindings\": [{\"harnes\": 1}]}",
        );
        let err = resolve_default_profile_with(data, &[], "alpha", Some("lab/a"), &cat, &no_quota)
            .unwrap_err()
            .to_string();
        assert!(err.contains("malformed"), "{err}");
    }

    fn quota_fixture(data: &Path, names: &[&str]) {
        for name in names {
            profile(data, name, true);
        }
        bindings(
            data,
            serde_json::json!({"bindings": [{"harness": "alpha", "selection": "quota",
                "profiles": names}]}),
        );
    }

    const QUOTA_CATALOGUE: Entries = &[
        ("s1", &["lab/a"]),
        ("s2", &["lab/a"]),
        ("s3", &["lab/a"]),
        ("s4", &["lab/a"]),
        ("busy", &["lab/a"]),
        ("idle", &["lab/a"]),
    ];

    #[test]
    fn quota_selection_picks_best_remaining_share_and_skips_unreadable() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        quota_fixture(data, &["vendor/s1", "vendor/s2", "vendor/s3", "vendor/s4"]);
        let cat = catalogues(QUOTA_CATALOGUE, true);
        let quota = |_: &str, profile: &str| match profile {
            "s1" => Some(0),
            "s2" => Some(40),
            "s4" => Some(90),
            _ => None,
        };
        let picked =
            resolve_default_profile_with(data, &[], "alpha", Some("lab/a"), &cat, &quota).unwrap();
        assert_eq!(picked.as_deref(), Some("vendor/s4"));
    }

    #[test]
    fn quota_selection_weighs_current_assignment_load() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        quota_fixture(data, &["vendor/busy", "vendor/idle"]);
        let store = anyagents_core::Store::open(data.join("state.db")).unwrap();
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
        // busy: 90 / (3 + 1) per agent; idle: 50 / 1.
        let quota = |_: &str, profile: &str| {
            if profile == "busy" {
                Some(90)
            } else {
                Some(50)
            }
        };
        let cat = catalogues(QUOTA_CATALOGUE, true);
        let picked =
            resolve_default_profile_with(data, &agents, "alpha", Some("lab/a"), &cat, &quota)
                .unwrap();
        assert_eq!(picked.as_deref(), Some("vendor/idle"));
    }

    #[test]
    fn quota_selection_fails_closed_when_no_quota_is_readable_or_left() {
        let dir = crate::test_tempdir();
        let data = dir.path();
        quota_fixture(data, &["vendor/s1", "vendor/s2"]);
        let cat = catalogues(QUOTA_CATALOGUE, true);
        let err = resolve_default_profile_with(data, &[], "alpha", Some("lab/a"), &cat, &no_quota)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("quota could not be read for any candidate"),
            "{err}"
        );
        let empty = |_: &str, _: &str| Some(0);
        let err = resolve_default_profile_with(data, &[], "alpha", Some("lab/a"), &cat, &empty)
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
