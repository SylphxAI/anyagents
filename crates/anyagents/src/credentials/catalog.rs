//! Provider and harness catalog data for credentials: which record kind a
//! provider uses, which files a login produces, which files a harness reads,
//! and how an account identity is read from those files.
//!
//! This is data about vendor tools, never about any operator's accounts.

use std::path::Path;

use anyagents_core::Error;
use anyagents_harness::normalize_harness_name;

use super::store::{Identity, Kind, RefreshOwner};

/// Resolves the upstream provider an agent draws its credential profile from.
///
/// An agent's `model` prefix (`sylphx/...`, `openrouter/...`, `qwen-token-plan/...`,
/// `kimi-code-plan/...`, `glm-code-plan/...`, `minimax-code-plan/...`) takes
/// precedence because any harness (`opencode`, `omp`, `claude-code`, `codex`)
/// can run against that upstream provider; otherwise defaults to the native
/// subscription provider of the agent's harness.
pub(crate) fn upstream_provider_of(harness: &str, model: Option<&str>) -> String {
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
        "spiron" | "spiron-cloud" => "spiron".to_string(),
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
        "spiron" => ("api-key", harnesses(&["spiron", "spiron-cloud"])),
        "opencode" => ("api-key", harnesses(&["opencode"])),
        "omp" => ("api-key", harnesses(&["omp"])),
        "sylphx" => (
            "api-key",
            harnesses(&[
                "spiron",
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

/// Declared `(primary_credentials, companion_state_files)` for a sign-in provider.
///
/// Only files in this manifest are ever read from a login staging directory or
/// published into a record; vendor runtime artefacts (`cli.log` symlinks, caches,
/// crash dumps, sockets, lockfiles, conversation databases) are discarded with
/// the staging directory.
pub(crate) fn login_manifest(
    provider: &str,
) -> Result<(&'static [&'static str], &'static [&'static str]), Error> {
    match provider {
        "anthropic" => Ok((
            &[".credentials.json", ".claude/.credentials.json"],
            &[".claude.json", ".claude/.claude.json"],
        )),
        "openai" => Ok((&["auth.json", ".codex/auth.json"], &[])),
        "google-antigravity" => Ok((
            &[".gemini/antigravity-cli/antigravity-oauth-token"],
            &[
                ".gemini/antigravity-cli/antigravity_state.pbtxt",
                ".gemini/antigravity-cli/jetski_state.pbtxt",
                ".gemini/config/config.json",
                ".gemini/config/.migrated",
            ],
        )),
        "muse" => Ok((
            &[
                "muse/auth.json",
                ".config/muse/auth.json",
                ".local/share/muse/auth.json",
            ],
            &[
                "settings.json",
                "muse/settings.json",
                "muse/trust.json",
                ".config/muse/settings.json",
                ".config/muse/trust.json",
            ],
        )),
        "xai" => Ok((&[".grok/auth.json"], &[])),
        "spiron" | "sylphx" => Ok((
            &[
                "auth.json",
                "credentials.json",
                ".config/sylphx/auth.json",
                ".spiron/auth.json",
                ".spiron/credentials.json",
                ".config/sylphx/credentials.json",
                ".spiron/key",
                ".config/sylphx/key",
                ".config/opencode/sylphx.key",
            ],
            &[
                "config.toml",
                ".spiron/context.json",
                ".config/sylphx/context.json",
                ".config/opencode/opencode.json",
            ],
        )),
        "opencode" => Ok((
            &[
                "data-home/opencode/auth.json",
                ".local/share/opencode/auth.json",
            ],
            &[
                "opencode/opencode.json",
                ".config/opencode/opencode.json",
                "opencode.json",
            ],
        )),
        "deepseek" => Ok((
            &[".dsh/.credentials.yaml", ".config/opencode/deepseek.key"],
            &[
                ".dsh/settings.yaml",
                ".dsh/.anonymous-user-id",
                ".dsh/profiles/headless/cordis.yml",
            ],
        )),
        "omp" | "cursor" => Ok((
            &[
                "agent.db",
                ".omp/agent/agent.db",
                "cursor-auth.json",
                "auth.json",
                "credentials.json",
            ],
            &["models.yml", ".omp/agent/models.yml"],
        )),
        _ => Err(Error::Message(format!(
            "unsupported sign-in provider: {provider}"
        ))),
    }
}

/// Resolves the target profile for an OAuth sign-in staged at `staging_dir` under
/// strict `1 Email / Account = 1 Profile` and `1 Agent = 1 Assigned Profile` rules:
/// 1. If an existing profile under `provider` already holds the same `account_id` or
///    `email`, updates that single profile in-place (never creating a duplicate profile
///    for the same account).
/// 2. If the signed-in account is new to `provider`:
///    - Uses `explicit_profile` if specified and not already occupied by a different account;
///    - Otherwise uses `default` if `default` does not yet hold a different account;
///    - Otherwise creates a new profile named after the account's email handle.
fn has_credential_material(value: &serde_json::Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object.iter().any(|(key, value)| {
        (matches!(
            key.as_str(),
            "access_token"
                | "accessToken"
                | "refresh_token"
                | "refreshToken"
                | "api_key"
                | "apiKey"
                | "OPENAI_API_KEY"
                | "key"
                | "token"
        ) && value.as_str().is_some_and(|text| !text.trim().is_empty()))
            || has_credential_material(value)
    })
}

pub(crate) fn validate_primary(provider: &str, rel: &str, bytes: &[u8]) -> Result<(), Error> {
    if bytes.is_empty() {
        return Err(Error::Message(format!("empty staged credential: {rel}")));
    }
    if rel.ends_with(".key") || rel.ends_with("/key") {
        let valid_key = std::str::from_utf8(bytes).is_ok_and(|text| {
            let token = text.trim();
            !token.is_empty() && !token.contains(char::is_whitespace)
        });
        if !valid_key {
            return Err(Error::Message(format!("invalid staged API key: {rel}")));
        }
        return Ok(());
    }
    if rel.ends_with(".yaml") || rel.ends_with(".yml") {
        let valid_yaml = std::str::from_utf8(bytes).is_ok_and(|text| !text.trim().is_empty());
        if !valid_yaml {
            return Err(Error::Message(format!(
                "invalid staged YAML credential: {rel}"
            )));
        }
        return Ok(());
    }
    if rel.ends_with(".db") {
        if !bytes.starts_with(b"SQLite format 3\0") {
            return Err(Error::Message(format!(
                "invalid staged SQLite credential store: {rel}"
            )));
        }
        return Ok(());
    }
    // AGY also accepts an operator-provided plaintext refresh token. Other
    // providers use JSON credential stores; metadata alone is not a login.
    let plaintext_agy_token = provider == "google-antigravity"
        && std::str::from_utf8(bytes).is_ok_and(|text| {
            let token = text.trim();
            !token.is_empty()
                && !token.contains(char::is_whitespace)
                && !token.starts_with(['{', '[', '"'])
        });
    if !plaintext_agy_token {
        let value: serde_json::Value = serde_json::from_slice(bytes)
            .map_err(|err| Error::Message(format!("invalid staged credential {rel}: {err}")))?;
        if !has_credential_material(&value) {
            return Err(Error::Message(format!(
                "no usable credential material in staged file: {rel}"
            )));
        }
    }
    Ok(())
}

fn extract_jwt_email(jwt: &str) -> Option<String> {
    let payload_b64 = jwt.split('.').nth(1)?;
    let decoded = decode_b64url(payload_b64)?;
    let val: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
    val.get("email")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            val.pointer("/https:~1~1api.openai.com~1profile/email")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Extracts `(email, account_id)` for a provider profile directory from its
/// vendor credential files or `profile.json`.
pub(crate) fn extract_profile_account_identity(
    prof_dir: &Path,
    provider: &str,
) -> (Option<String>, Option<String>) {
    let mut email: Option<String> = None;
    let mut account_id: Option<String> = None;

    match provider {
        "anthropic" => {
            for rel in [".credentials.json", ".claude/.credentials.json"] {
                if let Ok(text) = std::fs::read_to_string(prof_dir.join(rel))
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                {
                    if email.is_none() {
                        email = val
                            .pointer("/claudeAiOauth/email")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                    if account_id.is_none() {
                        account_id = val
                            .pointer("/claudeAiOauth/accountUuid")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                }
            }
            for rel in [".claude.json", ".claude/.claude.json"] {
                if let Ok(text) = std::fs::read_to_string(prof_dir.join(rel))
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                {
                    if email.is_none() {
                        email = val
                            .pointer("/oauthAccount/emailAddress")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                    if account_id.is_none() {
                        account_id = val
                            .pointer("/oauthAccount/accountUuid")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                }
            }
        }
        "openai" => {
            for rel in ["auth.json", ".codex/auth.json"] {
                if let Ok(text) = std::fs::read_to_string(prof_dir.join(rel))
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                {
                    if email.is_none() {
                        email = val
                            .get("id_token_email")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string)
                            .or_else(|| {
                                val.pointer("/tokens/id_token")
                                    .and_then(serde_json::Value::as_str)
                                    .and_then(extract_jwt_email)
                            });
                    }
                    if account_id.is_none() {
                        account_id = val
                            .pointer("/tokens/account_id")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                }
            }
        }
        "google-antigravity" => {
            let tok_path = prof_dir.join(".gemini/antigravity-cli/antigravity-oauth-token");
            if let Ok(text) = std::fs::read_to_string(tok_path)
                && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
            {
                email = val
                    .get("email")
                    .or_else(|| val.get("id_token_email"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
                account_id = val
                    .get("account_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
            }
        }
        "muse" => {
            for rel in ["muse/auth.json", ".config/muse/auth.json"] {
                if let Ok(text) = std::fs::read_to_string(prof_dir.join(rel))
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                    && email.is_none()
                {
                    email = val
                        .pointer("/providers/meta/user_email")
                        .and_then(serde_json::Value::as_str)
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(str::to_string);
                }
            }
        }
        "xai" => {
            if let Ok(text) = std::fs::read_to_string(prof_dir.join(".grok/auth.json"))
                && let Ok(val) = serde_json::from_str::<serde_json::Value>(&text)
                && let Some(obj) = val.as_object()
            {
                for entry in obj.values() {
                    if email.is_none() {
                        email = entry
                            .get("email")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                    if account_id.is_none() {
                        account_id = entry
                            .get("user_id")
                            .and_then(serde_json::Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);
                    }
                }
            }
        }
        "cursor" => {
            for rel in ["agent.db", ".omp/agent/agent.db"] {
                let db_path = prof_dir.join(rel);
                if db_path.is_file()
                    && let Some(conn) = crate::seat_tree::open_readonly_seat_sqlite(&db_path)
                    && let Ok(id_key) = conn.query_row(
                        "SELECT identity_key FROM auth_credentials WHERE provider = 'cursor' ORDER BY id DESC LIMIT 1",
                        [],
                        |row| row.get::<_, Option<String>>(0),
                    )
                    && let Some(raw_id) = id_key
                {
                    let cleaned = raw_id
                        .strip_prefix("account:")
                        .unwrap_or(&raw_id)
                        .split('|')
                        .next_back()
                        .unwrap_or(&raw_id)
                        .trim()
                        .to_string();
                    if !cleaned.is_empty() {
                        if cleaned.contains('@') {
                            email = Some(cleaned);
                        } else {
                            account_id = Some(cleaned);
                        }
                        break;
                    }
                }
            }
        }
        _ => {}
    }

    if let Ok(raw) = std::fs::read_to_string(prof_dir.join("profile.json"))
        && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
    {
        if email.is_none() {
            email = val
                .get("account")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
        }
        if account_id.is_none() {
            account_id = val
                .get("account_id")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);
        }
    }

    (email, account_id)
}

pub(crate) fn sanitize_account_slug(raw: &str) -> String {
    let base = raw.split('@').next().unwrap_or(raw).trim();
    let mut slug = String::new();
    for ch in base.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            slug.push(ch.to_ascii_lowercase());
        } else if ch == '.' && !slug.ends_with('-') && !slug.is_empty() {
            slug.push('-');
        }
    }
    let slug = slug.trim_matches('-').trim_matches('_');
    if slug.is_empty() || slug == "default" {
        "acct".to_string()
    } else {
        slug.to_string()
    }
}

fn extract_profile_dedup_key(prof_dir: &Path, provider: &str) -> Option<String> {
    let (email, account_id) = extract_profile_account_identity(prof_dir, provider);
    if let Some(em) = email {
        return Some(format!("email:{}", em.to_ascii_lowercase()));
    }
    if let Some(id) = account_id {
        return Some(format!("id:{id}"));
    }
    match provider {
        "deepseek" => read_first_non_empty_file(prof_dir, &[".config/opencode/deepseek.key"])
            .or_else(|| read_dsh_yaml_ref(prof_dir, "DEEPSEEK_API_KEY"))
            .map(|k| format!("key:{k}")),
        "kimi-code-plan" => read_first_non_empty_file(
            prof_dir,
            &[
                ".config/opencode/kimi-code-plan.key",
                ".config/opencode/kimi.key",
            ],
        )
        .map(|k| format!("key:{k}")),
        "glm-code-plan" => read_first_non_empty_file(
            prof_dir,
            &[
                ".config/opencode/glm-code-plan.key",
                ".config/opencode/glm.key",
            ],
        )
        .map(|k| format!("key:{k}")),
        "minimax-code-plan" => read_first_non_empty_file(
            prof_dir,
            &[
                ".config/opencode/minimax-code-plan.key",
                ".config/opencode/minimax.key",
            ],
        )
        .map(|k| format!("key:{k}")),
        "openrouter" => read_first_non_empty_file(prof_dir, &[".config/opencode/openrouter.key"])
            .or_else(|| read_dsh_yaml_ref(prof_dir, "OPENROUTER_API_KEY"))
            .map(|k| format!("key:{k}")),
        "google-gemini" => {
            read_first_non_empty_file(prof_dir, &["gemini-api-key", ".config/opencode/gemini.key"])
                .or_else(|| read_dsh_yaml_ref(prof_dir, "GEMINI_API_KEY"))
                .map(|k| format!("key:{k}"))
        }
        _ => None,
    }
}

pub(crate) fn read_first_non_empty_file(base: &Path, rels: &[&str]) -> Option<String> {
    for rel in rels {
        if let Ok(text) = std::fs::read_to_string(base.join(rel)) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

pub(crate) fn read_dsh_yaml_ref(base: &Path, key_name: &str) -> Option<String> {
    let text = std::fs::read_to_string(base.join(".dsh/.credentials.yaml")).ok()?;
    let prefix = format!("{key_name}:");
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix(&prefix) {
            let val = rest.trim().trim_matches('"').trim_matches('\'');
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

pub(crate) fn decode_b64url(input: &str) -> Option<Vec<u8>> {
    let val_of = |b: u8| -> Option<u8> {
        match b {
            b'A'..=b'Z' => Some(b - b'A'),
            b'a'..=b'z' => Some(b - b'a' + 26),
            b'0'..=b'9' => Some(b - b'0' + 52),
            b'+' | b'-' => Some(62),
            b'/' | b'_' => Some(63),
            _ => None,
        }
    };
    let clean: Vec<u8> = input
        .bytes()
        .filter(|b| *b != b'=' && !b.is_ascii_whitespace())
        .collect();
    let mut out = Vec::with_capacity(clean.len() * 3 / 4);
    for chunk in clean.chunks(4) {
        let b0 = val_of(*chunk.first()?)?;
        let b1 = val_of(*chunk.get(1)?)?;
        out.push((b0 << 2) | (b1 >> 4));
        if let Some(&c2) = chunk.get(2) {
            let b2 = val_of(c2)?;
            out.push((b1 << 4) | (b2 >> 2));
            if let Some(&c3) = chunk.get(3) {
                let b3 = val_of(c3)?;
                out.push((b2 << 6) | b3);
            }
        }
    }
    Some(out)
}

pub(crate) fn json_oauth_expires_at(bytes: &[u8]) -> Option<i64> {
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

pub(crate) fn json_oauth_has_refresh_token(bytes: &[u8]) -> bool {
    let Ok(val) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        return false;
    };
    val.pointer("/claudeAiOauth/refreshToken")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|s| !s.trim().is_empty())
}

pub(crate) fn json_oauth_identity(bytes: &[u8]) -> Option<String> {
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


/// The record kind a provider's login or key produces.
pub(crate) fn provider_kind(provider: &str) -> Kind {
    match default_kind_and_harnesses(provider).0 {
        "subscription" => Kind::OauthSubscription,
        _ => Kind::ApiKey,
    }
}

/// Who refreshes a provider's OAuth credential. Anthropic's refresh token is
/// held by the store and agents receive access-only copies; other vendors'
/// tools refresh (and may rotate) their own tokens, which `absorb` accepts back
/// only for the same identity.
pub(crate) fn refresh_owner(provider: &str) -> RefreshOwner {
    match provider_kind(provider) {
        Kind::OauthSubscription if provider == "anthropic" => RefreshOwner::Store,
        Kind::OauthSubscription => RefreshOwner::Vendor,
        _ => RefreshOwner::None,
    }
}

/// Harnesses that can run on a provider's records.
pub(crate) fn compatible_harnesses(provider: &str) -> Vec<String> {
    default_kind_and_harnesses(provider).1
}

/// The provider whose record a harness/model pair needs (the `model:<provider>` slot).
pub(crate) fn model_provider(harness: &str, model: Option<&str>) -> String {
    upstream_provider_of(harness, model)
}

/// API-key file names a provider's key is published under, relative to a
/// harness config directory.
pub(crate) fn provider_key_files(provider: &str) -> Vec<String> {
    match provider {
        "kimi-code-plan" => vec![".config/opencode/kimi-code-plan.key".into(), ".config/opencode/kimi.key".into()],
        "qwen-token-plan" => vec![".config/opencode/qwen-coding-plan.key".into(), ".config/opencode/qwen.key".into()],
        "glm-code-plan" => vec![".config/opencode/glm-code-plan.key".into(), ".config/opencode/glm.key".into()],
        "minimax-code-plan" => vec![".config/opencode/minimax-code-plan.key".into(), ".config/opencode/minimax.key".into()],
        "sylphx" => vec![".config/opencode/sylphx.key".into(), ".config/sylphx/key".into()],
        "openrouter" => vec![".config/opencode/openrouter.key".into()],
        "deepseek" => vec![".config/opencode/deepseek.key".into()],
        "google-gemini" => vec![".config/opencode/gemini.key".into()],
        other => {
            let prefix = other.split('-').next().unwrap_or(other);
            vec![format!(".config/opencode/{prefix}.key")]
        }
    }
}

/// One file a harness reads from its config directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ManifestEntry {
    pub rel: &'static str,
    /// Carries credential bytes. Every secret path of every harness is removed
    /// from an agent config directory before a launch writes the current set.
    pub secret: bool,
}

const fn secret(rel: &'static str) -> ManifestEntry {
    ManifestEntry { rel, secret: true }
}

const fn companion(rel: &'static str) -> ManifestEntry {
    ManifestEntry { rel, secret: false }
}

const KEY_FILES: [&str; 13] = [
    ".config/sylphx/key",
    ".config/opencode/sylphx.key",
    ".config/opencode/kimi-code-plan.key",
    ".config/opencode/kimi.key",
    ".config/opencode/qwen-token-plan.key",
    ".config/opencode/qwen-coding-plan.key",
    ".config/opencode/qwen.key",
    ".config/opencode/glm-code-plan.key",
    ".config/opencode/glm.key",
    ".config/opencode/minimax-code-plan.key",
    ".config/opencode/minimax.key",
    ".config/opencode/openrouter.key",
    ".config/opencode/deepseek.key",
];

const SPIRON_AUTH: [&str; 7] = [
    "auth.json",
    "credentials.json",
    ".config/sylphx/auth.json",
    ".spiron/auth.json",
    ".spiron/credentials.json",
    ".config/sylphx/credentials.json",
    ".spiron/key",
];

/// The credential manifest of a harness running on a provider. Materialisation
/// writes exactly these files (those present in the bound record) and nothing else.
pub(crate) fn harness_manifest(harness: &str, provider: &str) -> Vec<ManifestEntry> {
    let mut out: Vec<ManifestEntry> = Vec::new();
    let keys = |out: &mut Vec<ManifestEntry>| out.extend(KEY_FILES.iter().map(|r| secret(r)));
    match normalize_harness_name(harness) {
        "claude-code" | "claude-cloud" => {
            if provider == "anthropic" {
                out.extend([secret(".credentials.json"), secret(".claude/.credentials.json")]);
            }
        }
        "codex" | "openai-cloud" => {
            if provider == "openai" {
                out.extend([secret("auth.json"), secret(".codex/auth.json")]);
            } else {
                keys(&mut out);
            }
            out.extend([companion("models_cache.json"), companion("model-catalog-direct.json")]);
        }
        "opencode" => {
            out.extend([
                secret("data-home/opencode/auth.json"),
                secret(".local/share/opencode/auth.json"),
                secret(".config/opencode/gemini.key"),
            ]);
            keys(&mut out);
        }
        "agy" => out.extend([
            secret(".gemini/antigravity-cli/antigravity-oauth-token"),
            secret(".gemini/antigravity-cli/antigravity_state.pbtxt"),
            secret(".gemini/antigravity-cli/jetski_state.pbtxt"),
            companion(".gemini/config/config.json"),
            companion(".gemini/config/.migrated"),
        ]),
        "muse" => out.extend([
            secret("muse/auth.json"),
            secret(".config/muse/auth.json"),
            secret(".local/share/muse/auth.json"),
            companion("muse/trust.json"),
            companion(".config/muse/trust.json"),
        ]),
        "omp" => out.extend([
            secret(".omp/agent/agent.db"),
            secret("agent.db"),
            companion(".omp/agent/models.yml"),
            companion("models.yml"),
        ]),
        "dsh" => out.extend([
            secret(".dsh/.credentials.yaml"),
            companion(".dsh/profiles/headless/cordis.yml"),
        ]),
        "grok" => out.extend([
            secret(".grok/auth.json"),
            companion(".grok/models_cache.json"),
            companion(".grok/settings_cache.json"),
        ]),
        "spiron" | "spiron-cloud" => {
            out.extend(SPIRON_AUTH.iter().map(|r| secret(r)));
            keys(&mut out);
            out.extend([
                companion("config.toml"),
                companion(".spiron/context.json"),
                companion(".config/sylphx/context.json"),
            ]);
        }
        "cursor-cloud" => out.extend([
            secret("cursor-auth.json"),
            secret("auth.json"),
            secret("credentials.json"),
        ]),
        _ => {}
    }
    out
}

/// Every secret path any harness reads. Launch removes all of them before
/// writing the current harness's set, so nothing an earlier harness or an
/// earlier binding left behind can be read.
pub(crate) fn all_secret_paths() -> Vec<&'static str> {
    const HARNESSES: [&str; 11] = [
        "claude-code", "codex", "opencode", "agy", "muse", "omp", "dsh", "grok", "spiron",
        "cursor-cloud", "claude-cloud",
    ];
    let mut out: Vec<&'static str> = Vec::new();
    for h in HARNESSES {
        for provider in ["anthropic", "openai", "example"] {
            for e in harness_manifest(h, provider) {
                if e.secret && !out.contains(&e.rel) {
                    out.push(e.rel);
                }
            }
        }
    }
    out.sort_unstable();
    out
}

/// The account identity a record's material names, read from vendor files.
/// Key-only providers are identified by a digest of the key, never the key.
pub(crate) fn identity_of(dir: &Path, provider: &str) -> Option<Identity> {
    let (email, account_id) = extract_profile_account_identity(dir, provider);
    if let Some(identity) = Identity::from_parts(email, account_id) {
        return Some(identity);
    }
    if let Some(key) = extract_profile_dedup_key(dir, provider).and_then(|k| k.strip_prefix("key:").map(str::to_string)) {
        return Some(Identity::from_key(&key));
    }
    let key_files: Vec<String> = provider_key_files(provider);
    let rels: Vec<&str> = key_files.iter().map(String::as_str).collect();
    if let Some(key) = read_first_non_empty_file(dir, &rels) {
        return Some(Identity::from_key(&key));
    }
    for rel in SPIRON_AUTH {
        if let Ok(raw) = std::fs::read_to_string(dir.join(rel))
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
            && let Some(key) = val
                .get("access_token")
                .or_else(|| val.get("api_key"))
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.trim().is_empty())
        {
            return Some(Identity::from_key(key));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifests_are_relative_and_secret_union_covers_every_harness() {
        let union = all_secret_paths();
        for h in ["claude-code", "codex", "agy", "muse", "spiron", "opencode", "dsh", "grok", "omp"] {
            for p in ["anthropic", "openai", "google-antigravity", "muse", "example"] {
                for e in harness_manifest(h, p) {
                    assert!(!e.rel.starts_with('/') && !e.rel.contains(".."), "{}", e.rel);
                    if e.secret {
                        assert!(union.contains(&e.rel), "{h}/{p}: {}", e.rel);
                    }
                }
            }
        }
        assert!(!union.contains(&"settings.json"));
        assert!(!union.contains(&".claude.json"));
    }

    #[test]
    fn key_only_identities_never_contain_the_key() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".config/opencode")).unwrap();
        std::fs::write(dir.path().join(".config/opencode/openrouter.key"), "sk-example-secret\n").unwrap();
        let id = identity_of(dir.path(), "openrouter").unwrap();
        assert!(id.subject.starts_with("key:"));
        assert!(!id.subject.contains("sk-example"));
    }
}
pub(crate) fn access_only_anthropic_credentials(src_bytes: &[u8], now_ms: i64) -> Vec<u8> {
    if crate::sandbox::is_isolated_test_env()
        && std::env::var_os("ANYAGENTS_ANTHROPIC_OAUTH_TOKEN_URL").is_none()
    {
        return src_bytes.to_vec();
    }
    let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(src_bytes) else {
        return src_bytes.to_vec();
    };
    if let Some(oauth) = val
        .get_mut("claudeAiOauth")
        .and_then(serde_json::Value::as_object_mut)
    {
        let exp = oauth
            .get("expiresAt")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        oauth.insert("refreshToken".to_string(), serde_json::Value::Null);
        if exp <= now_ms {
            oauth.insert(
                "accessToken".to_string(),
                serde_json::Value::String(String::new()),
            );
            oauth.insert("expiresAt".to_string(), serde_json::Value::Number(0.into()));
        }
        if let Ok(out) = serde_json::to_vec(&val) {
            return out;
        }
    }
    src_bytes.to_vec()
}

