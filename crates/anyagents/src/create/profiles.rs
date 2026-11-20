//! Provider profile registry, harness compatibility mapping, settled-copy caching, listing, and hydration.

use std::path::{Path, PathBuf};

use anyagents_core::{Agent, Error, Store, agent_config_dir};
use anyagents_harness::normalize_harness_name;

use super::profile_rotation::rebalance_provider_profiles;
use super::{
    chmod_seat_file_readable, deduplicate_same_account_profiles, enrich_profiles_usage,
    extract_file_tilde_refs, extract_profile_account_identity,
    propagate_profile_files_to_bound_agents, seeded_profile_name, write_profile_marker,
    write_required_file, write_seat_shared_file,
};

/// One Upstream Provider Profile in the server's centralized profile pool
/// (`<data>/profiles/<provider>/<profile>`).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct ProviderProfileEntry {
    pub provider: String,
    pub profile: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    pub kind: String,
    pub compatible_harnesses: Vec<String>,
    pub assigned_agents: Vec<String>,
    #[serde(default)]
    pub subagent_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<String>,
}

/// Create or update a named provider profile in `<data>/profiles/<provider>/<profile>`.
pub(crate) fn set_provider_profile(
    data: &Path,
    provider_profile: &str,
    key: &str,
) -> Result<(String, String), Error> {
    let (provider, profile) = provider_profile.split_once('/').ok_or_else(|| {
        Error::Message("expected <provider>/<profile> (e.g. provider/example)".into())
    })?;
    let valid = |s: &str| {
        !s.is_empty()
            && s != "."
            && s != ".."
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
    };
    if !valid(provider) || !valid(profile) {
        return Err(Error::Message("invalid provider or profile name".into()));
    }
    let trimmed = key.trim();
    if trimmed.is_empty() || trimmed.contains(char::is_whitespace) {
        return Err(Error::Message(
            "a nonempty single-line key is required".into(),
        ));
    }
    let dir = data.join("profiles").join(provider).join(profile);
    ensure_provider_profile_meta(&dir, provider, None);
    let default_dir = data.join("profiles").join(provider).join("default");
    if default_dir != dir {
        for rel in [".config/opencode/opencode.json", "opencode.json"] {
            let donor = default_dir.join(rel);
            if let Ok(meta) = std::fs::symlink_metadata(&donor)
                && !meta.file_type().is_symlink()
                && meta.file_type().is_file()
                && !dir.join(rel).exists()
                && let Ok(bytes) = std::fs::read(&donor)
                && !bytes.is_empty()
            {
                write_required_file(&dir.join(rel), &bytes, 0o600)?;
            }
        }
    }
    if provider == "google-antigravity" {
        let tok_path = dir
            .join(".gemini")
            .join("antigravity-cli")
            .join("antigravity-oauth-token");
        write_required_file(&tok_path, format!("{trimmed}\n").as_bytes(), 0o600)?;
        let _ = std::fs::remove_file(dir.join(".config/opencode/google.key"));
        propagate_profile_files_to_bound_agents(
            data,
            provider,
            profile,
            &[".gemini/antigravity-cli/antigravity-oauth-token"],
        )?;
        return Ok((provider.to_string(), profile.to_string()));
    }
    let key_filenames: &[&str] = match provider {
        "kimi-code-plan" => &["kimi-code-plan.key", "kimi.key"],
        "qwen-token-plan" => &["qwen-coding-plan.key", "qwen.key"],
        "glm-code-plan" => &["glm-code-plan.key", "glm.key"],
        "minimax-code-plan" => &["minimax-code-plan.key", "minimax.key"],
        "sylphx" => &["sylphx.key"],
        "openrouter" => &["openrouter.key"],
        "deepseek" => &["deepseek.key"],
        "google-gemini" => &["gemini.key"],
        other => {
            let prefix = other.split('-').next().unwrap_or(other);
            let fname = format!("{prefix}.key");
            write_profile_key_file(&dir, &fname, trimmed)?;
            let rel = format!(".config/opencode/{fname}");
            propagate_profile_files_to_bound_agents(data, provider, profile, &[rel.as_str()])?;
            return Ok((provider.to_string(), profile.to_string()));
        }
    };
    let mut rels = Vec::new();
    for fname in key_filenames {
        write_profile_key_file(&dir, fname, trimmed)?;
        rels.push(format!(".config/opencode/{fname}"));
    }
    let rel_refs: Vec<&str> = rels.iter().map(String::as_str).collect();
    propagate_profile_files_to_bound_agents(data, provider, profile, &rel_refs)?;
    Ok((provider.to_string(), profile.to_string()))
}

fn write_profile_key_file(dir: &Path, filename: &str, key: &str) -> Result<(), Error> {
    let key_path = dir.join(".config").join("opencode").join(filename);
    write_required_file(&key_path, format!("{key}\n").as_bytes(), 0o600)
}

/// Resolves the upstream provider an agent draws its credential profile from.
///
/// An agent's `model` prefix (`sylphx/...`, `openrouter/...`, `qwen-token-plan/...`,
/// `kimi-code-plan/...`, `glm-code-plan/...`, `minimax-code-plan/...`) takes
/// precedence because any harness (`opencode`, `omp`, `claude-code`, `codex`)
/// can run against that upstream provider; otherwise defaults to the native
/// subscription provider of the agent's harness.
pub(crate) fn upstream_provider_of(harness: &str, model: Option<&str>) -> String {
    if let Some(provider) = crate::spiron::coding_plan_provider_for_model(model) {
        return provider.to_string();
    }
    if let Some(m) = model.map(str::trim).filter(|s| !s.is_empty())
        && let Some((prefix, _)) = m.split_once('/')
    {
        match prefix {
            "sylphx" | "spiron" | "opencode" | "omp" | "openrouter" | "opengateway"
            | "google-gemini" | "google-antigravity" | "muse" | "qwen-token-plan"
            | "kimi-code-plan" | "glm-code-plan" | "minimax-code-plan" | "anthropic" | "openai"
            | "xai" | "deepseek" | "cursor" => {
                return prefix.to_string();
            }
            _ => {}
        }
    }
    if matches!(
        harness.trim().to_ascii_lowercase().as_str(),
        "sylphx" | "sylphx-agent" | "sylphx_agent" | "sylphx/default"
    ) {
        return "sylphx".to_string();
    }
    match normalize_harness_name(harness) {
        "claude-code" | "claude-cloud" => "anthropic".to_string(),
        "codex" | "openai-cloud" => "openai".to_string(),
        "agy" => "google-antigravity".to_string(),
        "muse" => "muse".to_string(),
        "dsh" => "deepseek".to_string(),
        "grok" => "xai".to_string(),
        "cursor-cloud" => "cursor".to_string(),
        "spiron" | "spiron-local" | "spiron-cloud" => "spiron".to_string(),
        "opencode" => "opencode".to_string(),
        "omp" => "omp".to_string(),
        other => other.to_string(),
    }
}

pub(crate) fn default_kind_and_harnesses(provider: &str) -> (&'static str, Vec<String>) {
    let harnesses = |list: &[&str]| list.iter().map(|s| (*s).to_string()).collect();
    match provider {
        "anthropic" => ("subscription", harnesses(&["claude-code", "claude-cloud"])),
        "openai" => ("subscription", harnesses(&["codex", "openai-cloud"])),
        "google-antigravity" => ("subscription", harnesses(&["agy"])),
        "muse" => ("subscription", harnesses(&["muse"])),
        "deepseek" => ("subscription", harnesses(&["dsh"])),
        "xai" => ("subscription", harnesses(&["grok"])),
        "cursor" => ("subscription", harnesses(&["cursor-cloud"])),
        "spiron" => (
            "api-key",
            harnesses(&["spiron", "spiron-local", "spiron-cloud"]),
        ),
        "opencode" => ("api-key", harnesses(&["opencode", "omp"])),
        "omp" => ("api-key", harnesses(&["omp", "opencode"])),
        "sylphx" => (
            "api-key",
            harnesses(&[
                "spiron",
                "spiron-local",
                "spiron-cloud",
                "opencode",
                "omp",
                "claude-code",
                "claude-cloud",
                "codex",
                "openai-cloud",
                "cursor-cloud",
                "muse",
                "grok",
                "dsh",
            ]),
        ),
        "openrouter" | "opengateway" | "google-gemini" => (
            "api-key",
            harnesses(&[
                "spiron",
                "opencode",
                "omp",
                "claude-code",
                "codex",
                "muse",
                "grok",
                "dsh",
            ]),
        ),
        "qwen-token-plan" | "kimi-code-plan" | "glm-code-plan" | "minimax-code-plan" => (
            "coding-plan",
            harnesses(&[
                "spiron",
                "spiron-cloud",
                "opencode",
                "omp",
                "claude-code",
                "codex",
                "dsh",
            ]),
        ),
        _ => (
            "api-key",
            harnesses(&["spiron", "opencode", "omp", "claude-code", "codex"]),
        ),
    }
}

pub(crate) fn profile_compatible_harnesses(profile_dir: &Path, provider: &str) -> Vec<String> {
    let (_, default_compat) = default_kind_and_harnesses(provider);
    if !profile_dir.join("profile.json").is_file() {
        return default_compat;
    }
    std::fs::read_to_string(profile_dir.join("profile.json"))
        .ok()
        .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
        .and_then(|v| {
            v.get("compatible_harnesses")
                .and_then(serde_json::Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(|x| x.as_str().map(normalize_harness_name).map(str::to_string))
                        .collect::<Vec<_>>()
                })
        })
        .filter(|v| !v.is_empty())
        .unwrap_or(default_compat)
}

fn dir_has_opencode_mapping(dir: &Path, provider: &str) -> bool {
    for rel in [
        "opencode/opencode.json",
        ".config/opencode/opencode.json",
        "opencode.json",
    ] {
        if let Ok(raw) = std::fs::read_to_string(dir.join(rel))
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
            && val
                .get("provider")
                .and_then(serde_json::Value::as_object)
                .is_some_and(|m| !m.is_empty())
        {
            return true;
        }
    }
    for rel in [
        "data-home/opencode/auth.json",
        ".local/share/opencode/auth.json",
    ] {
        if let Ok(raw) = std::fs::read_to_string(dir.join(rel))
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
            && val.as_object().is_some_and(|m| !m.is_empty())
        {
            return true;
        }
    }
    let key_rels: Vec<String> = match provider {
        "sylphx" => vec![
            ".config/sylphx/key".to_string(),
            ".config/opencode/sylphx.key".to_string(),
        ],
        "qwen-token-plan" => vec![
            ".config/opencode/qwen-coding-plan.key".to_string(),
            ".config/opencode/qwen-token-plan.key".to_string(),
        ],
        other => vec![format!(".config/opencode/{other}.key")],
    };
    key_rels.iter().any(|rel| {
        std::fs::read_to_string(dir.join(rel))
            .ok()
            .is_some_and(|s| !s.trim().is_empty())
    })
}

pub(crate) fn ensure_profile_compatible_and_mapped(
    data: &Path,
    provider: &str,
    profile_name: &str,
    target_harness: &str,
    resolved_model: Option<&str>,
    agent_config: Option<&Path>,
) -> Result<(), Error> {
    let valid_token = |s: &str| {
        !s.is_empty()
            && s != "."
            && s != ".."
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
    };
    if !valid_token(provider) || !valid_token(profile_name) {
        return Err(Error::Message(format!(
            "invalid provider profile `{provider}/{profile_name}`"
        )));
    }
    let profile_dir = data.join("profiles").join(provider).join(profile_name);
    let compatible_harnesses = profile_compatible_harnesses(&profile_dir, provider);
    if !compatible_harnesses.iter().any(|h| h == target_harness) {
        return Err(Error::Message(format!(
            "harness `{target_harness}` is not compatible with provider profile `{provider}/{profile_name}` (compatible harnesses: {})",
            compatible_harnesses.join(", ")
        )));
    }
    if target_harness == "opencode" && !matches!(provider, "opencode" | "omp") {
        let explicit_sylphx_model = provider == "sylphx"
            && resolved_model
                .map(str::trim)
                .is_some_and(|m| m.starts_with("sylphx/"));
        let data_donor = std::fs::read_to_string(data.join("opencode-provider.json"))
            .ok()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
            .and_then(|v| {
                v.get("provider")
                    .and_then(serde_json::Value::as_object)
                    .cloned()
            })
            .is_some_and(|m| m.contains_key(provider));
        let mapped = explicit_sylphx_model
            || data_donor
            || dir_has_opencode_mapping(&profile_dir, provider)
            || agent_config.is_some_and(|cfg| dir_has_opencode_mapping(cfg, provider));
        if !mapped {
            return Err(Error::Message(format!(
                "provider profile `{provider}/{profile_name}` has no target mapping for harness `opencode` (fail-closed)"
            )));
        }
    }
    Ok(())
}

/// Whether `harness` can run the provider profile a creation names, checked
/// before anything is created: a creation refused after its row exists is
/// rolled back with a delete of everything keyed by the agent. Only
/// compatibility is checked here; [`ensure_profile_compatible_and_mapped`]
/// still checks the profile's mapping once the agent's config exists.
pub(crate) fn ensure_harness_runs_profile(
    data: &Path,
    harness: &str,
    profile: Option<&str>,
    model: Option<&str>,
) -> Result<(), Error> {
    let Some(explicit) = profile.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(());
    };
    let harness = normalize_harness_name(harness);
    let (provider, profile_name) = match explicit.split_once('/') {
        Some((p, n)) => (p.trim().to_string(), n.trim().to_string()),
        None => (upstream_provider_of(harness, model), explicit.to_string()),
    };
    let plain = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
            && s != "."
            && s != ".."
    };
    if !plain(&provider) || !plain(&profile_name) {
        // The full check refuses it once the config exists.
        return Ok(());
    }
    let profile_dir = data.join("profiles").join(&provider).join(&profile_name);
    let compatible = profile_compatible_harnesses(&profile_dir, &provider);
    if compatible.iter().any(|h| h == harness) {
        return Ok(());
    }
    Err(Error::Message(format!(
        "harness `{harness}` is not compatible with provider profile `{provider}/{profile_name}` \
         (compatible harnesses: {})",
        compatible.join(", ")
    )))
}

fn copy_host_file(src: &Path, dst: &Path) -> bool {
    let Ok(meta) = std::fs::symlink_metadata(src) else {
        return false;
    };
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return false;
    }
    if dst.is_file() {
        return true;
    }
    let Ok(bytes) = std::fs::read(src) else {
        return false;
    };
    if bytes.is_empty() {
        return false;
    }
    write_seat_shared_file(dst, &bytes, 0o600)
}

pub(super) fn json_oauth_expires_at(bytes: &[u8]) -> Option<i64> {
    let val: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let tok = val
        .pointer("/claudeAiOauth/accessToken")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim();
    if tok.is_empty() {
        return None;
    }
    val.pointer("/claudeAiOauth/expiresAt")
        .and_then(serde_json::Value::as_i64)
}

fn json_oauth_has_refresh_token(bytes: &[u8]) -> bool {
    let Ok(val) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return false;
    };
    val.pointer("/claudeAiOauth/refreshToken")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| !s.trim().is_empty())
}

pub(super) fn json_oauth_identity(bytes: &[u8]) -> Option<String> {
    let val: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    for ptr in [
        "/claudeAiOauth/accountUuid",
        "/claudeAiOauth/email",
        "/id_token_email",
        "/email",
        "/account_id",
        "/tokens/account_id",
        "/token/refresh_token",
    ] {
        if let Some(s) = val
            .pointer(ptr)
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return Some(s.to_string());
        }
    }
    None
}

fn merge_preserved_oauth_metadata(src_bytes: &[u8], dst_bytes: &[u8]) -> Option<Vec<u8>> {
    let mut src_val: serde_json::Value = serde_json::from_slice(src_bytes).ok()?;
    let dst_val: serde_json::Value = serde_json::from_slice(dst_bytes).ok()?;
    let mut changed = false;
    if let (Some(dst_oauth), Some(src_oauth)) = (
        dst_val
            .get("claudeAiOauth")
            .and_then(serde_json::Value::as_object),
        src_val
            .get_mut("claudeAiOauth")
            .and_then(serde_json::Value::as_object_mut),
    ) {
        for key in ["accountUuid", "email"] {
            if !src_oauth.contains_key(key)
                && let Some(v) = dst_oauth.get(key)
                && v.as_str().is_some_and(|s| !s.trim().is_empty())
            {
                src_oauth.insert(key.to_string(), v.clone());
                changed = true;
            }
        }
    }
    if let (Some(dst_obj), Some(src_obj)) = (dst_val.as_object(), src_val.as_object_mut()) {
        for key in ["id_token_email", "email", "account_id"] {
            if !src_obj.contains_key(key)
                && let Some(v) = dst_obj.get(key)
                && v.as_str().is_some_and(|s| !s.trim().is_empty())
            {
                src_obj.insert(key.to_string(), v.clone());
                changed = true;
            }
        }
    }
    changed.then(|| serde_json::to_vec(&src_val).ok()).flatten()
}

/// A file's identity and content stamp: any rewrite, rename or chmod changes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileStamp {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}

impl FileStamp {
    fn of(meta: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            dev: meta.dev(),
            ino: meta.ino(),
            len: meta.len(),
            mtime: (meta.mtime(), meta.mtime_nsec()),
            ctime: (meta.ctime(), meta.ctime_nsec()),
        }
    }
}

type CopyStamps = (FileStamp, FileStamp);

/// `(source, destination)` pairs a copy last found nothing to do for, with the
/// stamps both files had then. The decision depends only on the two files, so
/// while neither changes it stands without reading them again.
static SETTLED_COPIES: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<(PathBuf, PathBuf), CopyStamps>>,
> = std::sync::LazyLock::new(Default::default);

fn settled_copies()
-> std::sync::MutexGuard<'static, std::collections::HashMap<(PathBuf, PathBuf), CopyStamps>> {
    SETTLED_COPIES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
thread_local! {
    pub(super) static COPY_EVALUATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) fn copy_seat_file_if_newer(src: &Path, dst: &Path) -> bool {
    let key = (src.to_path_buf(), dst.to_path_buf());
    let stamps = std::fs::symlink_metadata(src)
        .ok()
        .zip(std::fs::metadata(dst).ok())
        .map(|(src_meta, dst_meta)| (FileStamp::of(&src_meta), FileStamp::of(&dst_meta)));
    if stamps.is_some() && settled_copies().get(&key) == stamps.as_ref() {
        return false;
    }
    let copied = copy_seat_file_if_newer_uncached(src, dst);
    // Stamps taken before the read: a change during it is seen next time.
    match stamps.filter(|_| !copied) {
        Some(stamps) => settled_copies().insert(key, stamps),
        None => settled_copies().remove(&key),
    };
    copied
}

fn copy_seat_file_if_newer_uncached(src: &Path, dst: &Path) -> bool {
    #[cfg(test)]
    COPY_EVALUATIONS.with(|count| count.set(count.get() + 1));
    let Ok(src_meta) = std::fs::symlink_metadata(src) else {
        return false;
    };
    if src_meta.file_type().is_symlink() || !src_meta.file_type().is_file() || src_meta.len() == 0 {
        return false;
    }
    let read = std::fs::read(src).or_else(|_| {
        chmod_seat_file_readable(src);
        std::fs::read(src)
    });
    let Ok(mut src_bytes) = read else {
        return false;
    };
    if src_bytes.is_empty() {
        return false;
    }
    if src.file_name().is_some_and(|n| n == ".credentials.json")
        && (json_oauth_expires_at(&src_bytes).is_none()
            || !json_oauth_has_refresh_token(&src_bytes))
    {
        return false;
    }
    if src
        .file_name()
        .is_some_and(|n| n == "antigravity-oauth-token")
    {
        let valid_agy_tok = serde_json::from_slice::<serde_json::Value>(&src_bytes)
            .ok()
            .is_some_and(|v| {
                v.pointer("/token/refresh_token")
                    .or_else(|| v.get("refresh_token"))
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|s| !s.trim().is_empty())
            });
        if !valid_agy_tok {
            return false;
        }
    }
    if let Ok(dst_meta) = std::fs::metadata(dst)
        && dst_meta.is_file()
        && dst_meta.len() > 0
    {
        if let Ok(dst_bytes) = std::fs::read(dst) {
            if src_bytes == dst_bytes {
                return false;
            }
            if let (Some(src_id), Some(dst_id)) = (
                json_oauth_identity(&src_bytes),
                json_oauth_identity(&dst_bytes),
            ) && src_id != dst_id
            {
                return false;
            }
            if let (Some(src_exp), Some(dst_exp)) = (
                json_oauth_expires_at(&src_bytes),
                json_oauth_expires_at(&dst_bytes),
            ) {
                if src_exp <= dst_exp {
                    return false;
                }
            } else if src_meta.modified().ok() <= dst_meta.modified().ok() {
                return false;
            }
            if let Some(merged) = merge_preserved_oauth_metadata(&src_bytes, &dst_bytes) {
                src_bytes = merged;
            }
        } else if src_meta.modified().ok() <= dst_meta.modified().ok() {
            return false;
        }
    }
    write_seat_shared_file(dst, &src_bytes, 0o600)
}

pub(super) fn ensure_provider_profile_meta(
    dir: &Path,
    provider: &str,
    kind_override: Option<&str>,
) {
    let _ = std::fs::create_dir_all(dir);
    let meta_path = dir.join("profile.json");
    if !meta_path.exists() {
        let (default_kind, harnesses) = default_kind_and_harnesses(provider);
        let kind = kind_override.unwrap_or(default_kind);
        let meta = serde_json::json!({
            "provider": provider,
            "kind": kind,
            "compatible_harnesses": harnesses,
        });
        if let Ok(text) = serde_json::to_string_pretty(&meta) {
            write_seat_shared_file(&meta_path, text.as_bytes(), 0o600);
        }
    }
}

/// Discovers and imports existing upstream provider credentials from active seats
/// and host config into `<data>/profiles/<provider>/<profile>`, and returns all
/// upstream provider profiles sorted by provider and profile name.
pub(crate) fn sync_and_list_provider_profiles(
    store: &Store,
    data: &Path,
) -> Vec<ProviderProfileEntry> {
    sync_and_list_provider_profiles_inner(store, data, true, true)
}

pub(crate) fn list_provider_profiles_fast(store: &Store, data: &Path) -> Vec<ProviderProfileEntry> {
    sync_and_list_provider_profiles_inner(store, data, true, true)
}

pub(crate) fn sync_and_list_provider_profiles_with_options(
    store: &Store,
    data: &Path,
    probe_usage: bool,
) -> Vec<ProviderProfileEntry> {
    sync_and_list_provider_profiles_inner(store, data, probe_usage, probe_usage)
}

/// Re-hydrate any newly created or updated profile blobs from PostgreSQL
/// `deployment_blobs(namespace = 'profiles')` into `<data>/profiles` so stateless
/// replicas observe fresh native-refreshed OAuth tokens persisted by any other
/// replica or sandbox turn.
pub(crate) fn hydrate_profiles_from_store_if_postgres(store: &Store, data: &Path) {
    if !store.is_postgres() {
        return;
    }
    let blobs = store.blob_store();
    let Ok(metas) = blobs.list_deployment_blobs("profiles") else {
        return;
    };
    let profiles_root = data.join("profiles");
    for meta in metas {
        if anyagents_core::stateless::validate_blob_path(&meta.path).is_err() {
            continue;
        }
        let dest = profiles_root.join(&meta.path);
        let local_mtime_ms = std::fs::metadata(&dest)
            .ok()
            .and_then(|m| m.modified().ok())
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64);
        let needs_refresh = local_mtime_ms.is_none_or(|local_ms| meta.updated_at_ms > local_ms);
        if !needs_refresh {
            continue;
        }
        let Ok(Some(entry)) = blobs.get_deployment_blob("profiles", &meta.path) else {
            continue;
        };
        let unchanged = std::fs::read(&dest)
            .ok()
            .is_some_and(|existing| existing == entry.content);
        if unchanged {
            continue;
        }
        let mode = if entry.encrypted { 0o600 } else { 0o644 };
        if let Some(parent) = dest.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let wrote = if entry.content.is_empty() {
            std::fs::write(&dest, []).is_ok()
        } else {
            anyagents_harness::atomic_write_nonempty_file(&dest, &entry.content, mode).is_ok()
        };
        if wrote
            && (meta.path.ends_with("/antigravity-oauth-token")
                || meta.path.ends_with("/.credentials.json")
                || meta.path.ends_with("/.codex/auth.json"))
        {
            // Clear local stale error/backoff cache so the next probe immediately uses the fresh token.
            let mut parts = meta.path.splitn(3, '/');
            if let (Some(prov), Some(prof)) = (parts.next(), parts.next()) {
                let prof_dir = profiles_root.join(prov).join(prof);
                let _ = std::fs::remove_file(prof_dir.join(".anyagents-oauth-refresh-attempt-ms"));
                let _ = std::fs::remove_file(prof_dir.join(".anyagents-last-usage.txt"));
            }
        }
    }
}

pub(super) fn sync_and_list_provider_profiles_inner(
    store: &Store,
    data: &Path,
    probe_usage: bool,
    rebalance: bool,
) -> Vec<ProviderProfileEntry> {
    hydrate_profiles_from_store_if_postgres(store, data);
    let agents = store.list_agents().unwrap_or_default();
    let profiles_root = data.join("profiles");
    let _ = std::fs::create_dir_all(&profiles_root);

    let now_ms = anyagents_core::now_ms();
    let seed_stamp = profiles_root.join(".last-agent-cred-sync-ms");
    let skip_heavy_seed = !rebalance
        && !crate::sandbox::is_isolated_test_env()
        && std::fs::read_to_string(&seed_stamp)
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .is_some_and(|last_ms| now_ms.saturating_sub(last_ms) < 30_000);

    if !skip_heavy_seed {
        // Migrate any legacy harness-named empty/default directories if they match a harness name
        // rather than a provider name and have no `profile.json`.
        for legacy_harness in ["claude-code", "codex", "agy", "dsh", "grok", "opengateway"] {
            let legacy_dir = profiles_root.join(legacy_harness);
            if legacy_dir.is_dir() && !legacy_dir.join("default").join("profile.json").exists() {
                let _ = std::fs::remove_dir_all(&legacy_dir);
            }
        }

        // Seed upstream provider profiles from existing seats on this server so pre-existing
        // logins (Subscriptions, Coding Plans, API Keys) are registered in the Master Pool.
        // A seat still being created (or whose creation failed) is not one yet.
        for agent in &agents {
            if matches!(agent.state.as_str(), "archived" | "created") {
                continue;
            }
            let cfg = agent_config_dir(data, &agent.id);
            let provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
            let bound = bound_profile_name(&cfg);
            let seeded = seeded_profile_name(&cfg);
            if seeded.is_some() && bound.is_some() && seeded != bound {
                continue;
            }
            let Some(prof_name) = bound_profile_of_provider(&cfg, &provider) else {
                continue;
            };
            let profile_dir = profiles_root.join(&provider).join(&prof_name);
            match provider.as_str() {
                "anthropic" => {
                    let rels = [".credentials.json", ".claude/.credentials.json"];
                    for src_rel in rels {
                        let src = cfg.join(src_rel);
                        if src.is_file() {
                            ensure_provider_profile_meta(&profile_dir, "anthropic", None);
                            for dst_rel in rels {
                                let _ = copy_seat_file_if_newer(&src, &profile_dir.join(dst_rel));
                            }
                        }
                    }
                }
                "openai" => {
                    let rels = ["auth.json", ".codex/auth.json"];
                    for src_rel in rels {
                        let src = cfg.join(src_rel);
                        if src.is_file() {
                            ensure_provider_profile_meta(&profile_dir, "openai", None);
                            for dst_rel in rels {
                                let _ = copy_seat_file_if_newer(&src, &profile_dir.join(dst_rel));
                            }
                        }
                    }
                }
                "google-antigravity" => {
                    for rel in [
                        ".gemini/antigravity-cli/antigravity-oauth-token",
                        ".gemini/antigravity-cli/antigravity_state.pbtxt",
                        ".gemini/config/config.json",
                    ] {
                        let src = cfg.join(rel);
                        if src.is_file() {
                            ensure_provider_profile_meta(&profile_dir, "google-antigravity", None);
                            let _ = copy_seat_file_if_newer(&src, &profile_dir.join(rel));
                        }
                    }
                }
                "muse" => {
                    for rel in ["muse/auth.json", ".config/muse/auth.json"] {
                        let src = cfg.join(rel);
                        if src.is_file() {
                            ensure_provider_profile_meta(&profile_dir, "muse", None);
                            let _ = copy_seat_file_if_newer(&src, &profile_dir.join(rel));
                        }
                    }
                }
                "sylphx" | "qwen-token-plan" | "kimi-code-plan" | "glm-code-plan"
                | "minimax-code-plan" => {
                    let kind = if provider == "sylphx" {
                        "api-key"
                    } else {
                        "coding-plan"
                    };
                    let opencode_rel = ".config/opencode/opencode.json";
                    let src_json = cfg.join(opencode_rel);
                    if src_json.is_file() {
                        ensure_provider_profile_meta(&profile_dir, &provider, Some(kind));
                        let dst_json = profile_dir.join(opencode_rel);
                        if !dst_json.is_file()
                            && let Ok(text) = std::fs::read_to_string(&src_json)
                            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                            && let Some(prov_block) = val.get("provider")
                        {
                            if let Some(p) = dst_json.parent() {
                                let _ = std::fs::create_dir_all(p);
                            }
                            let clean = serde_json::json!({
                                "$schema": "https://opencode.ai/config.json",
                                "provider": prov_block,
                            });
                            if let Ok(clean_bytes) = serde_json::to_vec_pretty(&clean) {
                                write_seat_shared_file(&dst_json, &clean_bytes, 0o600);
                            }
                        }
                        if let Ok(text) = std::fs::read_to_string(&src_json) {
                            for rel in extract_file_tilde_refs(&text) {
                                let _ = copy_host_file(&cfg.join(&rel), &profile_dir.join(&rel));
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        deduplicate_same_account_profiles(data);
    }

    let mut entries: Vec<ProviderProfileEntry> = Vec::new();
    if let Ok(providers) = std::fs::read_dir(&profiles_root) {
        for prov_entry in providers.flatten() {
            let prov_path = prov_entry.path();
            if !prov_path.is_dir() {
                continue;
            }
            let provider = prov_entry.file_name().to_string_lossy().into_owned();
            if provider.starts_with('.') {
                continue;
            }
            let Ok(prof_dirs) = std::fs::read_dir(&prov_path) else {
                continue;
            };
            for prof_entry in prof_dirs.flatten() {
                let prof_path = prof_entry.path();
                if !prof_path.is_dir() {
                    continue;
                }
                let profile = prof_entry.file_name().to_string_lossy().into_owned();
                if profile.starts_with('.') {
                    continue;
                }
                let (default_kind, default_harnesses) = default_kind_and_harnesses(&provider);
                let mut kind = default_kind.to_string();
                let mut compatible_harnesses = default_harnesses;
                if let Ok(raw) = std::fs::read_to_string(prof_path.join("profile.json"))
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
                {
                    if let Some(k) = val.get("kind").and_then(serde_json::Value::as_str) {
                        kind = k.to_string();
                    }
                    if let Some(arr) = val
                        .get("compatible_harnesses")
                        .and_then(serde_json::Value::as_array)
                    {
                        let parsed: Vec<String> = arr
                            .iter()
                            .filter_map(|v| {
                                v.as_str().map(normalize_harness_name).map(str::to_string)
                            })
                            .collect();
                        if !parsed.is_empty() {
                            compatible_harnesses = parsed;
                        }
                    }
                }
                let explicit_harnesses_in_meta =
                    std::fs::read_to_string(prof_path.join("profile.json"))
                        .ok()
                        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
                        .and_then(|val| {
                            val.get("compatible_harnesses")
                                .and_then(serde_json::Value::as_array)
                                .cloned()
                        })
                        .is_some_and(|arr| !arr.is_empty());
                if !explicit_harnesses_in_meta
                    && matches!(
                        provider.as_str(),
                        "sylphx"
                            | "spiron"
                            | "kimi-code-plan"
                            | "qwen-token-plan"
                            | "glm-code-plan"
                            | "minimax-code-plan"
                            | "openrouter"
                    )
                    && !compatible_harnesses.iter().any(|h| h == "spiron")
                {
                    compatible_harnesses.insert(0, "spiron".to_string());
                }
                let mut seen = std::collections::HashSet::new();
                compatible_harnesses.retain(|h| seen.insert(h.clone()));
                let (acct_email, acct_id) = extract_profile_account_identity(&prof_path, &provider);
                entries.push(ProviderProfileEntry {
                    provider: provider.clone(),
                    profile,
                    account: acct_email.or(acct_id),
                    kind,
                    compatible_harnesses,
                    assigned_agents: Vec::new(),
                    subagent_count: 0,
                    usage: None,
                });
            }
        }
    }

    entries.sort_by(|a, b| (&a.provider, &a.profile).cmp(&(&b.provider, &b.profile)));
    if probe_usage {
        enrich_profiles_usage(data, &mut entries);
    }

    if rebalance
        && !crate::sandbox::is_isolated_test_env()
        && !crate::server::serverless::is_serverless_mode()
        && let Ok(store) = Store::open(data.join("state.db"))
    {
        rebalance_provider_profiles(&store, data, &agents, &entries);
    }

    // Bind active agents to their resolved `<provider>/<profile>`.
    let mut ordered_agents: Vec<&Agent> = agents.iter().filter(|a| a.state != "archived").collect();
    ordered_agents.sort_by_key(|a| a.parent_id.is_some());
    for agent in ordered_agents {
        let is_root = agent.parent_id.is_none();
        let cfg = agent_config_dir(data, &agent.id);
        let expected_provider = upstream_provider_of(&agent.harness, agent.model.as_deref());
        let bound = bound_profile_name(&cfg).or_else(|| {
            if !store.is_postgres() {
                return None;
            }
            store
                .blob_store()
                .get_agent_file(&agent.id, "markers", ".anyagents-profile")
                .ok()
                .flatten()
                .and_then(|e| String::from_utf8(e.content).ok())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        });
        let (bound_prov, bound_prof) = match bound.as_deref() {
            Some(s) if s.contains('/') => {
                let (p, n) = s.split_once('/').unwrap_or((&expected_provider, "default"));
                (p.to_string(), n.to_string())
            }
            Some(s) if !s.is_empty() => (expected_provider.clone(), s.to_string()),
            _ => (expected_provider.clone(), "default".to_string()),
        };
        let normalized = format!("{bound_prov}/{bound_prof}");
        // A listing never writes into an agent whose creation is in flight or
        // failed: its tree may be gone, and a write would bring it back.
        if !crate::server::serverless::is_serverless_mode()
            && agent.state != "created"
            && bound.as_deref() != Some(normalized.as_str())
        {
            write_profile_marker(&cfg, ".anyagents-profile", &format!("{normalized}\n"))
                .unwrap_or_else(|err| eprintln!("runtime persistence failed: {err}"));
        }
        let target_idx = entries
            .iter()
            .position(|e| e.provider == bound_prov && e.profile == bound_prof)
            .or_else(|| entries.iter().position(|e| e.provider == expected_provider));
        if let Some(idx) = target_idx
            && let Some(entry) = entries.get_mut(idx)
        {
            if is_root {
                entry.assigned_agents.push(agent.name.clone());
            } else {
                entry.subagent_count += 1;
            }
        }
    }

    for entry in &mut entries {
        entry.assigned_agents.sort();
    }
    entries
}

/// Harnesses/providers that share compatible credential and profile directories.
pub(crate) fn compatible_harnesses_for(provider_or_harness: &str) -> &'static [&'static str] {
    match provider_or_harness.trim() {
        "spiron" | "spiron-cloud" | "sylphx" | "sylphx-agent" | "sylphx_agent" => {
            &["spiron", "spiron-cloud", "sylphx"]
        }
        "spiron-local" => &["spiron-local", "spiron", "spiron-cloud", "sylphx"],
        "opencode" => &["opencode", "omp"],
        "omp" => &["omp", "opencode"],
        "claude-code" | "claude-cloud" | "anthropic" => {
            &["anthropic", "claude-code", "claude-cloud"]
        }
        "codex" | "openai-cloud" | "openai" => &["openai", "codex", "openai-cloud"],
        "cursor-cloud" | "cursor" => &["cursor", "cursor-cloud"],
        "agy" | "google" | "google-antigravity" => &["google-antigravity", "google", "agy"],
        "grok" | "xai" => &["xai", "grok"],
        _ => &[],
    }
}

/// Lists all available Master Profiles for `provider_or_harness` under `<data>/profiles/<provider>/<profile>`.
pub(crate) fn list_harness_profiles(
    data: &Path,
    provider_or_harness: &str,
) -> Vec<(String, PathBuf)> {
    let provider = match provider_or_harness {
        "claude-code" | "claude-cloud" | "codex" | "openai-cloud" | "cursor-cloud" | "agy"
        | "muse" | "dsh" | "grok" | "opencode" | "omp" | "spiron" | "spiron-local"
        | "spiron-cloud" => upstream_provider_of(provider_or_harness, None),
        other => other.to_string(),
    };
    let mut base_dirs = vec![
        data.join("profiles").join(&provider),
        data.join("profiles").join(provider_or_harness),
        data.join("config").join(&provider),
        data.join("config").join(provider_or_harness),
    ];
    for compat in compatible_harnesses_for(provider_or_harness) {
        base_dirs.push(data.join("profiles").join(compat));
        base_dirs.push(data.join("config").join(compat));
    }
    let mut out: Vec<(String, PathBuf)> = Vec::new();
    for base in base_dirs {
        let Ok(entries) = std::fs::read_dir(&base) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                continue;
            }
            if !out.iter().any(|(n, _)| n == &name) {
                out.push((name, path));
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Returns the profile marker name bound to an agent's config directory, if any.
pub(crate) fn bound_profile_name(config_dir: &Path) -> Option<String> {
    std::fs::read_to_string(config_dir.join(".anyagents-profile"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// The profile of `provider` a seat's binding names: `default` for an unbound
/// seat, and `None` for a binding to another provider's profile. A seat whose
/// model moved from `google-antigravity/...` to `sylphx/...` keeps its old
/// binding until it is rebound; that name is an account of the old provider,
/// never a profile under the new one.
pub(crate) fn bound_profile_of_provider(config_dir: &Path, provider: &str) -> Option<String> {
    let Some(bound) = bound_profile_name(config_dir) else {
        return Some("default".to_string());
    };
    let name = match bound.split_once('/') {
        Some((bound_provider, name)) => {
            if bound_provider != provider
                && !compatible_harnesses_for(provider).contains(&bound_provider)
            {
                return None;
            }
            name
        }
        None => bound.as_str(),
    };
    Some(if name.is_empty() { "default" } else { name }.to_string())
}
