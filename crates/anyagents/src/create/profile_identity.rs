//! Account identity extraction, JWT/base64url decoding, and credential metadata readers.

use std::path::Path;

use super::profiles::ensure_provider_profile_meta;
use super::write_seat_shared_file;

pub(crate) fn update_profile_json_identity(
    prof_dir: &Path,
    provider: &str,
    email: Option<&str>,
    account_id: Option<&str>,
) {
    ensure_provider_profile_meta(prof_dir, provider, None);
    let meta_path = prof_dir.join("profile.json");
    let mut val = std::fs::read_to_string(&meta_path)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    let Some(obj) = val.as_object_mut() else {
        return;
    };
    let mut changed = false;
    if let Some(em) = email.map(str::trim).filter(|s| !s.is_empty())
        && obj.get("account").and_then(serde_json::Value::as_str) != Some(em)
    {
        obj.insert(
            "account".to_string(),
            serde_json::Value::String(em.to_string()),
        );
        changed = true;
    }
    if let Some(aid) = account_id.map(str::trim).filter(|s| !s.is_empty())
        && obj.get("account_id").and_then(serde_json::Value::as_str) != Some(aid)
    {
        obj.insert(
            "account_id".to_string(),
            serde_json::Value::String(aid.to_string()),
        );
        changed = true;
    }
    if changed && let Ok(text) = serde_json::to_string_pretty(&val) {
        write_seat_shared_file(&meta_path, text.as_bytes(), 0o600);
    }
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

pub(super) fn sanitize_profile_slug(raw: &str) -> String {
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

pub(super) fn extract_profile_dedup_key(prof_dir: &Path, provider: &str) -> Option<String> {
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

pub(super) fn read_first_non_empty_file(base: &Path, rels: &[&str]) -> Option<String> {
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

pub(super) fn read_dsh_yaml_ref(base: &Path, key_name: &str) -> Option<String> {
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

fn decode_b64url(input: &str) -> Option<Vec<u8>> {
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
