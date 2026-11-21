//! Local subscription status. A stored credential is not a quota measurement.

use std::path::Path;

use base64::Engine;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::credentials::read_anthropic_profile_credentials;
use super::quota::anthropic_tier_label;

pub(super) fn is_passive_subscription(provider: &str) -> bool {
    matches!(
        provider,
        "anthropic"
            | "openai"
            | "google-antigravity"
            | "cursor"
            | "kimi-code-plan"
            | "muse"
            | "xai"
    )
}

#[derive(Default)]
pub(super) struct LocalStatus {
    pub(super) usable: bool,
    pub(super) expired: bool,
    tier: Option<String>,
}

impl LocalStatus {
    pub(super) fn usage(&self) -> String {
        match (self.expired, self.tier.as_deref()) {
            (true, Some(tier)) => format!("oauth expired ({tier})"),
            (true, None) => "oauth expired".into(),
            (false, Some(tier)) => format!("({tier})"),
            (false, None) => String::new(),
        }
    }
}

const AUTH_REJECTED: &str = ".anyagents-auth-rejected";

fn credential_paths(provider: &str) -> &'static [&'static str] {
    match provider {
        "anthropic" => &[".credentials.json", ".claude/.credentials.json"],
        "openai" => &[".codex/auth.json", "auth.json"],
        "google-antigravity" => &[
            ".gemini/antigravity-cli/antigravity-oauth-token",
            ".gemini/oauth_creds.json",
        ],
        "muse" => &[
            "muse/auth.json",
            ".config/muse/auth.json",
            ".local/share/muse/auth.json",
        ],
        "xai" => &[".grok/auth.json"],
        "cursor" => &[
            "cursor-auth.json",
            "auth.json",
            "credentials.json",
            "agent.db",
            ".omp/agent/agent.db",
        ],
        "kimi-code-plan" => &[
            ".config/opencode/kimi-code-plan.key",
            ".config/opencode/kimi.key",
        ],
        _ => &[],
    }
}

fn credential_fingerprint(dir: &Path, provider: &str) -> Option<String> {
    let mut digest = Sha256::new();
    let mut has_auth = false;
    for rel in credential_paths(provider) {
        if let Ok(mut f) = std::fs::File::open(dir.join(rel)) {
            let mut component = Sha256::new();
            if std::io::copy(&mut f, &mut component).is_ok() {
                digest.update(rel.as_bytes());
                digest.update(component.finalize());
                has_auth = true;
            }
        }
    }
    has_auth.then(|| format!("{:x}", digest.finalize()))
}

pub(super) fn record_auth_rejection(dir: &Path, provider: &str) {
    if let Some(fingerprint) = credential_fingerprint(dir, provider) {
        let marker = dir.join(AUTH_REJECTED);
        if !std::fs::read_to_string(&marker).is_ok_and(|s| s == fingerprint) {
            super::write_seat_shared_file(&marker, fingerprint.as_bytes(), 0o600);
        }
    }
}

fn read_auth(dir: &Path, rels: &[&str]) -> Option<Value> {
    rels.iter().find_map(|rel| {
        let bytes = std::fs::read(dir.join(rel)).ok()?;
        serde_json::from_slice(&bytes).ok()
    })
}

// Stored JWT claims are hints for display/rotation only, never authorization.
fn jwt_claims(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn cursor_db_auth(dir: &Path) -> Option<Value> {
    ["agent.db", ".omp/agent/agent.db"]
        .into_iter()
        .find_map(|rel| {
            let db = crate::seat_tree::open_readonly_seat_sqlite(&dir.join(rel))?;
            let data = db.query_row(
            "SELECT data FROM auth_credentials WHERE provider = 'cursor' ORDER BY id DESC LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        ).ok()?;
            serde_json::from_str(&data).ok()
        })
}

fn text<'a>(auth: &'a Value, paths: &[&str]) -> Option<&'a str> {
    paths.iter().find_map(|path| {
        auth.pointer(path)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
    })
}

fn oauth_expiry_ms(auth: &Value) -> Option<i64> {
    ["/token/expiry", "/expiry"]
        .into_iter()
        .find_map(|path| {
            auth.pointer(path)?
                .as_str()?
                .parse::<jiff::Timestamp>()
                .ok()
                .map(|ts| ts.as_millisecond())
        })
        .or_else(|| {
            [
                "/token/expires_at",
                "/expires_at",
                "/token/expiry_date",
                "/expiry_date",
            ]
            .into_iter()
            .find_map(|path| {
                let n = auth.pointer(path)?.as_i64()?;
                Some(if n > 0 && n < 10_000_000_000 {
                    n * 1_000
                } else {
                    n
                })
            })
        })
}

pub(super) fn local_subscription_status(dir: &Path, provider: &str, now_ms: i64) -> LocalStatus {
    let rejected = std::fs::read_to_string(dir.join(AUTH_REJECTED))
        .ok()
        .is_some_and(|prior| credential_fingerprint(dir, provider).as_deref() == Some(&prior));
    let rels = credential_paths(provider);
    let (auth, access, refresh): (_, &[&str], &[&str]) = match provider {
        "anthropic" => (
            read_anthropic_profile_credentials(dir)
                .and_then(|bytes| serde_json::from_slice(&bytes).ok()),
            &["/claudeAiOauth/accessToken"],
            &["/claudeAiOauth/refreshToken"],
        ),
        "openai" => (
            read_auth(dir, rels),
            &["/tokens/access_token", "/OPENAI_API_KEY"],
            &["/tokens/refresh_token"],
        ),
        "google-antigravity" => (
            read_auth(dir, rels),
            &["/token/access_token", "/access_token"],
            &["/token/refresh_token", "/refresh_token"],
        ),
        "muse" => (
            read_auth(dir, rels),
            &["/providers/meta/access_token"],
            &["/providers/meta/refresh_token"],
        ),
        "xai" => (
            read_auth(dir, rels).and_then(|a| {
                if a.get("key").is_some() {
                    Some(a)
                } else {
                    a.as_object()?
                        .values()
                        .find(|v| v.get("key").is_some())
                        .cloned()
                }
            }),
            &["/key"],
            &["/refresh_token"],
        ),
        "cursor" => (
            read_auth(dir, &rels[..3]).or_else(|| cursor_db_auth(dir)),
            &["/access", "/accessToken", "/access_token"],
            &["/refresh", "/refreshToken", "/refresh_token"],
        ),
        "kimi-code-plan" => {
            return LocalStatus {
                usable: !rejected
                    && super::profile_identity::read_first_non_empty_file(dir, rels).is_some(),
                expired: rejected,
                tier: None,
            };
        }
        _ => return LocalStatus::default(),
    };
    let Some(auth) = auth else {
        return LocalStatus {
            expired: rejected,
            ..LocalStatus::default()
        };
    };
    let access_token = text(&auth, access);
    let jwt = access_token.and_then(jwt_claims);
    let openai_identity = (provider == "openai")
        .then(|| text(&auth, &["/tokens/id_token"]).and_then(jwt_claims))
        .flatten();
    let tier = match provider {
        "anthropic" => anthropic_tier_label(
            text(&auth, &["/claudeAiOauth/subscriptionType"]),
            text(&auth, &["/claudeAiOauth/rateLimitTier"]),
        )
        .map(str::to_owned),
        "openai" => openai_identity
            .as_ref()
            .or(jwt.as_ref())
            .and_then(|claims| {
                text(
                    claims,
                    &["/https:~1~1api.openai.com~1auth/chatgpt_plan_type"],
                )
            })
            .filter(|tier| {
                matches!(
                    *tier,
                    "free" | "plus" | "pro" | "team" | "business" | "enterprise" | "edu"
                )
            })
            .map(str::to_owned),
        "google-antigravity" => text(&auth, &["/paidTier/id", "/currentTier/id"])
            .map(str::to_ascii_lowercase)
            .and_then(|tier| {
                ["ultra", "pro", "free"]
                    .into_iter()
                    .find(|name| tier.contains(name))
            })
            .map(str::to_owned),
        _ => None,
    };
    let expiry = if provider == "anthropic" {
        auth.pointer("/claudeAiOauth/expiresAt")
            .and_then(Value::as_i64)
    } else {
        oauth_expiry_ms(&auth)
    };
    let expiry = expiry.or_else(|| jwt.as_ref()?.get("exp")?.as_i64()?.checked_mul(1000));
    let has_access = access_token.is_some();
    let has_refresh = text(&auth, refresh).is_some();
    // Expired access is expected when a native CLI has not run for a while.
    // Only require sign-in when the local credential cannot refresh itself.
    let usable = !rejected && (has_refresh || (has_access && expiry.is_none_or(|ms| ms > now_ms)));
    LocalStatus {
        usable,
        expired: rejected || (has_access && !usable),
        tier,
    }
}
