//! Per-provider live profile usage & quota probing.

use std::path::Path;

use super::credentials::{
    CredentialChange, OAuthRefreshDiskReservation, anthropic_oauth_usage_url,
    post_anthropic_oauth_refresh, propagate_credentials,
};
use super::profile_identity::{
    extract_cursor_oauth_from_db, extract_jwt_sub_tail, read_dsh_yaml_ref,
    read_first_non_empty_file,
};
use super::quota::{
    anthropic_tier_label, format_anthropic_usage_summary, format_reset_eta_epoch_ms,
    format_reset_eta_iso, format_reset_eta_secs, observed_agy_tier, observed_fraction_pct,
    observed_remaining_pct, observed_window_min, overlay_active_quota_reset_on_cached_usage,
    probe_opencode_profile_credentials, profile_has_sylphx_credentials,
    sync_anthropic_profile_metadata,
};
use super::{
    extract_profile_account_identity, parse_remaining_pct_from_usage,
    propagate_profile_files_to_bound_agents, update_profile_json_identity, write_seat_shared_file,
};

pub(crate) fn probe_one_profile_usage(
    data: &Path,
    provider: &str,
    profile: &str,
) -> Option<String> {
    let prof_dir = data.join("profiles").join(provider).join(profile);
    // A probe records its reading inside the profile; a profile that does not
    // exist has no reading, and recording one would create it.
    if !prof_dir.is_dir() {
        return None;
    }
    let last_usage_path = prof_dir.join(".anyagents-last-usage.txt");
    let now_ms = anyagents_core::now_ms();
    let cred_newer_than_usage_cache = |cache_mtime: std::time::SystemTime| -> bool {
        let rel = match provider {
            "google-antigravity" => ".gemini/antigravity-cli/antigravity-oauth-token",
            "openai" => ".codex/auth.json",
            _ => return false,
        };
        std::fs::metadata(prof_dir.join(rel))
            .ok()
            .and_then(|m| m.modified().ok())
            .is_some_and(|tok_mtime| tok_mtime > cache_mtime)
    };
    if provider != "anthropic"
        && let Ok(meta) = std::fs::metadata(&last_usage_path)
        && let Ok(mtime) = meta.modified()
        && !cred_newer_than_usage_cache(mtime)
        && let Ok(dur) = mtime.duration_since(std::time::UNIX_EPOCH)
        && now_ms.saturating_sub(dur.as_millis() as i64) < 120_000
        && let Ok(cached) = std::fs::read_to_string(&last_usage_path)
    {
        let trimmed = cached.trim();
        if trimmed == "__NONE__" {
            return None;
        }
        if !trimmed.is_empty() {
            return Some(overlay_active_quota_reset_on_cached_usage(
                &prof_dir,
                &last_usage_path,
                trimmed,
                now_ms,
            ));
        }
    }

    let http = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(3)))
        .build()
        .new_agent();

    let result = (|| -> Option<String> {
        match provider {
            "anthropic" => {
                let cred_text = std::fs::read_to_string(prof_dir.join(".credentials.json"))
                    .or_else(|_| {
                        std::fs::read_to_string(prof_dir.join(".claude/.credentials.json"))
                    })
                    .ok()?;
                let mut cred: serde_json::Value = serde_json::from_str(&cred_text).ok()?;
                let mut token = cred
                    .pointer("/claudeAiOauth/accessToken")?
                    .as_str()?
                    .trim()
                    .to_string();
                if token.is_empty() {
                    return None;
                }
                let usage_url = anthropic_oauth_usage_url();
                let fetch_usage = |tok: &str| -> Result<serde_json::Value, u16> {
                    match http
                        .get(&usage_url)
                        .header("authorization", &format!("Bearer {tok}"))
                        .header("anthropic-beta", "oauth-2025-04-20")
                        .header("cache-control", "no-cache")
                        .header("user-agent", "claude-code/2.1.310")
                        .call()
                    {
                        Ok(resp) => {
                            let body = resp.into_body().read_to_string().map_err(|_| 0u16)?;
                            serde_json::from_str(&body).map_err(|_| 0u16)
                        }
                        Err(ureq::Error::StatusCode(code)) => Err(code),
                        Err(_) => Err(0),
                    }
                };
                let last_usage_path = prof_dir.join(".anyagents-last-usage.txt");
                let backoff_path = prof_dir.join(".anyagents-usage-backoff-until-ms");
                let now_ms = anyagents_core::now_ms();
                let expires_at = cred
                    .pointer("/claudeAiOauth/expiresAt")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(i64::MAX);
                let fallback_cached_usage =
                    |cred_val: &serde_json::Value, oauth_expired: bool| -> Option<String> {
                        let raw = if let Ok(cached) = std::fs::read_to_string(&last_usage_path)
                            && !cached.trim().is_empty()
                        {
                            overlay_active_quota_reset_on_cached_usage(
                                &prof_dir,
                                &last_usage_path,
                                cached.trim(),
                                now_ms,
                            )
                        } else {
                            let tier = cred_val
                                .pointer("/claudeAiOauth/rateLimitTier")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("");
                            let sub = cred_val
                                .pointer("/claudeAiOauth/subscriptionType")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("");
                            let badge = anthropic_tier_label(Some(sub), Some(tier)).unwrap_or(sub);
                            let base = if badge.is_empty() {
                                String::new()
                            } else {
                                format!("({badge})")
                            };
                            overlay_active_quota_reset_on_cached_usage(
                                &prof_dir,
                                &last_usage_path,
                                &base,
                                now_ms,
                            )
                        };
                        if oauth_expired {
                            let tail = raw.strip_prefix("5h: 100% left · ").unwrap_or(raw.as_str());
                            if tail.is_empty() {
                                return Some("oauth expired".to_string());
                            }
                            return Some(format!("oauth expired · {tail}"));
                        }
                        (!raw.is_empty()).then_some(raw)
                    };
                if expires_at > now_ms
                    && let Ok(meta) = std::fs::metadata(&last_usage_path)
                    && let Ok(mtime) = meta.modified()
                    && let Ok(dur) = mtime.duration_since(std::time::UNIX_EPOCH)
                    && now_ms.saturating_sub(dur.as_millis() as i64) < 60_000
                    && let Some(cached) = fallback_cached_usage(&cred, false)
                {
                    return Some(cached);
                }
                if let Some(backoff_until) = std::fs::read_to_string(&backoff_path)
                    .ok()
                    .and_then(|s| s.trim().parse::<i64>().ok())
                    && backoff_until > now_ms
                {
                    return fallback_cached_usage(&cred, expires_at <= now_ms);
                }
                let expiring_soon = now_ms.saturating_add(45 * 60 * 1000) >= expires_at;
                let initial_usage = if expiring_soon {
                    Err(401)
                } else {
                    fetch_usage(&token)
                };
                let val = match initial_usage {
                    Ok(v) => v,
                    Err(code) if code != 401 => {
                        if code == 429 {
                            write_seat_shared_file(
                                &backoff_path,
                                format!("{}\n", now_ms + 180_000).as_bytes(),
                                0o600,
                            );
                        }
                        return fallback_cached_usage(&cred, expires_at <= now_ms);
                    }
                    Err(_) => {
                        let attempt_path = prof_dir.join(".anyagents-oauth-refresh-attempt-ms");
                        let last_attempt_ms = std::fs::read_to_string(&attempt_path)
                            .ok()
                            .and_then(|s| s.trim().parse::<i64>().ok())
                            .unwrap_or(0);
                        if now_ms.saturating_sub(last_attempt_ms) < 300_000 {
                            if expires_at <= now_ms {
                                return fallback_cached_usage(&cred, true);
                            }
                            match fetch_usage(&token) {
                                Ok(v) => v,
                                Err(code) => {
                                    if code == 429 {
                                        write_seat_shared_file(
                                            &backoff_path,
                                            format!("{}\n", now_ms + 180_000).as_bytes(),
                                            0o600,
                                        );
                                    }
                                    return fallback_cached_usage(&cred, false);
                                }
                            }
                        } else {
                            let rtok = cred
                                .pointer("/claudeAiOauth/refreshToken")
                                .and_then(serde_json::Value::as_str)?
                                .trim()
                                .to_string();
                            if rtok.is_empty() {
                                return fallback_cached_usage(&cred, expires_at <= now_ms);
                            }
                            let reservation = match OAuthRefreshDiskReservation::acquire(
                                &prof_dir,
                                &[".credentials.json", ".claude/.credentials.json"],
                                cred_text.len(),
                                now_ms,
                            ) {
                                Ok(r) => r,
                                Err(err) => {
                                    eprintln!(
                                        "oauth refresh aborted before network call ({}): {err}",
                                        prof_dir.display()
                                    );
                                    return fallback_cached_usage(&cred, expires_at <= now_ms);
                                }
                            };
                            let req_body = serde_json::json!({
                                "grant_type": "refresh_token",
                                "refresh_token": rtok,
                                "client_id": "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
                            })
                            .to_string();
                            let Some(rval) = post_anthropic_oauth_refresh(&http, &req_body) else {
                                return fallback_cached_usage(&cred, expires_at <= now_ms);
                            };
                            let Some(new_acc) = rval
                                .get("access_token")
                                .and_then(|v| v.as_str())
                                .map(|s| s.trim().to_string())
                            else {
                                return fallback_cached_usage(&cred, expires_at <= now_ms);
                            };
                            if let Some(oauth) = cred
                                .get_mut("claudeAiOauth")
                                .and_then(serde_json::Value::as_object_mut)
                            {
                                oauth.insert(
                                    "accessToken".to_string(),
                                    serde_json::Value::String(new_acc.clone()),
                                );
                                if let Some(new_ref) = rval
                                    .get("refresh_token")
                                    .and_then(serde_json::Value::as_str)
                                {
                                    oauth.insert(
                                        "refreshToken".to_string(),
                                        serde_json::Value::String(new_ref.to_string()),
                                    );
                                }
                                if let Some(exp_in) =
                                    rval.get("expires_in").and_then(serde_json::Value::as_i64)
                                {
                                    oauth.insert(
                                        "expiresAt".to_string(),
                                        serde_json::Value::Number((now_ms + exp_in * 1000).into()),
                                    );
                                }
                                if let Ok(updated) = serde_json::to_string(&cred) {
                                    reservation
                                        .persist_credentials(
                                            &[".credentials.json", ".claude/.credentials.json"],
                                            updated.as_bytes(),
                                        )
                                        .map_err(|err| {
                                            eprintln!(
                                                "credential refresh persistence failed: {err}"
                                            )
                                        })
                                        .ok()?;
                                    propagate_profile_files_to_bound_agents(
                                        data,
                                        provider,
                                        profile,
                                        &[".credentials.json", ".claude/.credentials.json"],
                                    )
                                    .map_err(|err| eprintln!("runtime persistence failed: {err}"))
                                    .ok()?;
                                }
                            }
                            token = new_acc;
                            match fetch_usage(&token) {
                                Ok(v) => v,
                                Err(_) => return fallback_cached_usage(&cred, false),
                            }
                        }
                    }
                };
                let tier_badge = sync_anthropic_profile_metadata(
                    &http, data, &prof_dir, profile, &token, &mut cred, false,
                );
                let formatted = format_anthropic_usage_summary(&val, tier_badge);
                if let Some(ref s) = formatted {
                    write_seat_shared_file(&last_usage_path, format!("{s}\n").as_bytes(), 0o600);
                    let _ = std::fs::remove_file(&backoff_path);
                    if parse_remaining_pct_from_usage(s, None).is_some_and(|p| p > 0) {
                        let _ = std::fs::remove_file(prof_dir.join(".anyagents-quota-reset-ms"));
                    }
                }
                formatted
            }
            "openai" => {
                let auth_text = std::fs::read_to_string(prof_dir.join("auth.json"))
                    .or_else(|_| std::fs::read_to_string(prof_dir.join(".codex/auth.json")))
                    .ok()?;
                let mut auth: serde_json::Value = serde_json::from_str(&auth_text).ok()?;
                let token = auth
                    .pointer("/tokens/access_token")?
                    .as_str()?
                    .trim()
                    .to_string();
                if token.is_empty() {
                    return None;
                }
                let acc_id = auth
                    .pointer("/tokens/account_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_string();
                let (oa_em, oa_id) = extract_profile_account_identity(&prof_dir, "openai");
                update_profile_json_identity(
                    &prof_dir,
                    "openai",
                    oa_em.as_deref(),
                    oa_id
                        .as_deref()
                        .or((!acc_id.is_empty()).then_some(acc_id.as_str())),
                );
                let fetch_wham = |tok: &str| -> Option<serde_json::Value> {
                    let mut req = http
                        .get("https://chatgpt.com/backend-api/wham/usage")
                        .header("authorization", &format!("Bearer {tok}"))
                        .header("user-agent", "codex-cli/0.164.0");
                    if !acc_id.is_empty() {
                        req = req.header("chatgpt-account-id", &acc_id);
                    }
                    let resp = req.call().ok()?;
                    let body = resp.into_body().read_to_string().ok()?;
                    serde_json::from_str(&body).ok()
                };
                let val = match fetch_wham(&token) {
                    Some(v) => v,
                    None => {
                        let rtok = auth
                            .pointer("/tokens/refresh_token")
                            .and_then(serde_json::Value::as_str)?
                            .trim()
                            .to_string();
                        if rtok.is_empty() {
                            return None;
                        }
                        let reservation = OAuthRefreshDiskReservation::acquire(
                            &prof_dir,
                            &["auth.json", ".codex/auth.json"],
                            auth_text.len(),
                            anyagents_core::now_ms(),
                        )
                        .map_err(|err| {
                            eprintln!(
                                "oauth refresh aborted before network call ({}): {err}",
                                prof_dir.display()
                            )
                        })
                        .ok()?;
                        let req_body = serde_json::json!({
                            "client_id": "app_EMoamEEZ73f0CkXaXp7hrann",
                            "grant_type": "refresh_token",
                            "refresh_token": rtok,
                        })
                        .to_string();
                        let rresp = http
                            .post("https://auth.openai.com/oauth/token")
                            .header("content-type", "application/json")
                            .send(req_body.as_bytes())
                            .ok()?;
                        let rbody = rresp.into_body().read_to_string().ok()?;
                        let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                        let new_acc = rval.get("access_token")?.as_str()?.trim().to_string();
                        if let Some(tokens_obj) = auth
                            .get_mut("tokens")
                            .and_then(serde_json::Value::as_object_mut)
                        {
                            tokens_obj.insert(
                                "access_token".to_string(),
                                serde_json::Value::String(new_acc.clone()),
                            );
                            if let Some(new_ref) = rval
                                .get("refresh_token")
                                .and_then(serde_json::Value::as_str)
                            {
                                tokens_obj.insert(
                                    "refresh_token".to_string(),
                                    serde_json::Value::String(new_ref.to_string()),
                                );
                            }
                            if let Ok(updated) = serde_json::to_string_pretty(&auth) {
                                reservation
                                    .persist_credentials(
                                        &["auth.json", ".codex/auth.json"],
                                        updated.as_bytes(),
                                    )
                                    .map_err(|err| {
                                        eprintln!("credential refresh persistence failed: {err}")
                                    })
                                    .ok()?;
                                propagate_profile_files_to_bound_agents(
                                    data,
                                    provider,
                                    profile,
                                    &["auth.json", ".codex/auth.json"],
                                )
                                .map_err(|err| eprintln!("runtime persistence failed: {err}"))
                                .ok()?;
                            }
                        }
                        fetch_wham(&new_acc)?
                    }
                };
                let used = val
                    .pointer("/rate_limit/primary_window/used_percent")
                    .and_then(serde_json::Value::as_f64)?;
                let rem = (100.0 - used).clamp(0.0, 100.0).round() as i64;
                let eta = (rem < 100)
                    .then(|| {
                        val.pointer("/rate_limit/primary_window/reset_after_seconds")
                            .and_then(serde_json::Value::as_i64)
                            .and_then(format_reset_eta_secs)
                    })
                    .flatten()
                    .map(|e| format!(" ↻{e}"))
                    .unwrap_or_default();
                match val.get("plan_type").and_then(serde_json::Value::as_str) {
                    Some(plan) if !plan.is_empty() => {
                        Some(format!("7d: {rem}% left{eta} ({plan})"))
                    }
                    _ => Some(format!("7d: {rem}% left{eta}")),
                }
            }
            "google-antigravity" => {
                let tok_rel = ".gemini/antigravity-cli/antigravity-oauth-token";
                let tok_path = prof_dir.join(tok_rel);
                let tok_text = std::fs::read_to_string(&tok_path).ok()?;
                let mut tok_val: serde_json::Value = serde_json::from_str(&tok_text).ok()?;
                let token = tok_val
                    .pointer("/token/access_token")
                    .or_else(|| tok_val.get("access_token"))?
                    .as_str()?
                    .trim()
                    .to_string();
                if token.is_empty() {
                    return None;
                }
                let fetch_summary = |tok: &str| -> Option<serde_json::Value> {
                    let resp = http
                    .post("https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:retrieveUserQuotaSummary")
                    .header("authorization", &format!("Bearer {tok}"))
                    .header("content-type", "application/json")
                    .header("user-agent", "antigravity/3.0.0")
                    .send(b"{}")
                    .ok()?;
                    let body = resp.into_body().read_to_string().ok()?;
                    serde_json::from_str(&body).ok()
                };
                let fetch_models = |tok: &str| -> Option<serde_json::Value> {
                    let resp = http
                    .post("https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:fetchAvailableModels")
                    .header("authorization", &format!("Bearer {tok}"))
                    .header("content-type", "application/json")
                    .header("user-agent", "antigravity")
                    .send(b"{}")
                    .ok()?;
                    let body = resp.into_body().read_to_string().ok()?;
                    serde_json::from_str(&body).ok()
                };
                let mut active_token = token;
                let mut summary_val = fetch_summary(&active_token);
                if summary_val.is_none() {
                    let attempt_path = prof_dir.join(".anyagents-oauth-refresh-attempt-ms");
                    let last_attempt_ms = std::fs::read_to_string(&attempt_path)
                        .ok()
                        .and_then(|s| s.trim().parse::<i64>().ok())
                        .unwrap_or(0);
                    let tok_mtime_ms = std::fs::metadata(&tok_path)
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0);
                    if tok_mtime_ms <= last_attempt_ms
                        && now_ms.saturating_sub(last_attempt_ms) < 300_000
                    {
                        let cached_expired = std::fs::read_to_string(&last_usage_path)
                            .ok()
                            .is_some_and(|s| s.trim().starts_with("oauth expired"));
                        if cached_expired {
                            return Some("oauth expired".to_string());
                        }
                        write_seat_shared_file(&last_usage_path, b"__NONE__\n", 0o600);
                        return None;
                    }
                    let rtok = tok_val
                        .pointer("/token/refresh_token")
                        .or_else(|| tok_val.get("refresh_token"))
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .trim()
                        .to_string();
                    if rtok.is_empty() {
                        write_seat_shared_file(&last_usage_path, b"oauth expired\n", 0o600);
                        return Some("oauth expired".to_string());
                    }
                    let oauth_clients: Vec<(String, String)> =
                        std::env::var("ANYAGENTS_AGY_CLIENT_SECRET")
                            .ok()
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .map(|secret| {
                                vec![(crate::signin::AGY_OAUTH_CLIENT_ID.to_string(), secret)]
                            })
                            .unwrap_or_default();
                    if oauth_clients.is_empty() {
                        // Native harness OAuth refresh inside sandboxes syncs back to
                        // storage; do not scrape host binaries or record a bogus backoff.
                        write_seat_shared_file(&last_usage_path, b"oauth expired\n", 0o600);
                        return Some("oauth expired".to_string());
                    }
                    let reservation = match OAuthRefreshDiskReservation::acquire(
                        &prof_dir,
                        &[tok_rel],
                        tok_text.len(),
                        now_ms,
                    ) {
                        Ok(r) => r,
                        Err(err) => {
                            eprintln!(
                                "oauth refresh aborted before network call ({}): {err}",
                                prof_dir.display()
                            );
                            return None;
                        }
                    };
                    let mut refreshed_tokens = None;
                    for (cid, csec) in oauth_clients {
                        let form_body = format!(
                            "client_id={cid}&client_secret={csec}&refresh_token={rtok}&grant_type=refresh_token"
                        );
                        if let Ok(rresp) = http
                            .post("https://oauth2.googleapis.com/token")
                            .header("content-type", "application/x-www-form-urlencoded")
                            .send(form_body.as_bytes())
                            && let Ok(rbody) = rresp.into_body().read_to_string()
                            && let Ok(rval) = serde_json::from_str::<serde_json::Value>(&rbody)
                            && let Some(acc) = rval
                                .get("access_token")
                                .and_then(serde_json::Value::as_str)
                                .map(|s| s.trim().to_string())
                            && !acc.is_empty()
                        {
                            let next_ref = rval
                                .get("refresh_token")
                                .and_then(serde_json::Value::as_str)
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                                .map(ToOwned::to_owned);
                            refreshed_tokens = Some((acc, next_ref));
                            break;
                        }
                    }
                    let Some((new_acc, new_ref)) = refreshed_tokens else {
                        write_seat_shared_file(&last_usage_path, b"oauth expired\n", 0o600);
                        return Some("oauth expired".to_string());
                    };
                    if let Some(tok_obj) = tok_val
                        .get_mut("token")
                        .and_then(serde_json::Value::as_object_mut)
                    {
                        tok_obj.insert(
                            "access_token".to_string(),
                            serde_json::Value::String(new_acc.clone()),
                        );
                        if let Some(nr) = new_ref {
                            tok_obj
                                .insert("refresh_token".to_string(), serde_json::Value::String(nr));
                        }
                    } else if let Some(root_obj) = tok_val.as_object_mut() {
                        root_obj.insert(
                            "access_token".to_string(),
                            serde_json::Value::String(new_acc.clone()),
                        );
                        if let Some(nr) = new_ref {
                            root_obj
                                .insert("refresh_token".to_string(), serde_json::Value::String(nr));
                        }
                    }
                    if let Ok(updated) = serde_json::to_string_pretty(&tok_val) {
                        reservation
                            .persist_credentials(&[tok_rel], updated.as_bytes())
                            .map_err(|err| {
                                eprintln!("credential refresh persistence failed: {err}")
                            })
                            .ok()?;
                        propagate_credentials(
                            data,
                            provider,
                            profile,
                            &[tok_rel],
                            CredentialChange::Renewed,
                        )
                        .map_err(|err| eprintln!("runtime persistence failed: {err}"))
                        .ok()?;
                    }
                    active_token = new_acc;
                    summary_val = fetch_summary(&active_token);
                }
                if extract_profile_account_identity(&prof_dir, "google-antigravity")
                    .0
                    .is_none()
                    && let Ok(uresp) = http
                        .get("https://www.googleapis.com/oauth2/v2/userinfo")
                        .header("authorization", &format!("Bearer {active_token}"))
                        .call()
                    && let Ok(ubody) = uresp.into_body().read_to_string()
                    && let Ok(uval) = serde_json::from_str::<serde_json::Value>(&ubody)
                {
                    let em = uval.get("email").and_then(serde_json::Value::as_str);
                    let uid = uval.get("id").and_then(serde_json::Value::as_str);
                    update_profile_json_identity(&prof_dir, "google-antigravity", em, uid);
                }
                if let Some(ref sv) = summary_val
                    && let Some(groups) = sv.get("groups").and_then(serde_json::Value::as_array)
                {
                    let mut gem_5h: Option<(i64, Option<String>)> = None;
                    let mut gem_7d: Option<(i64, Option<String>)> = None;
                    let mut opus_5h: Option<i64> = None;
                    let mut opus_7d: Option<i64> = None;
                    for g in groups {
                        let Some(buckets) = g.get("buckets").and_then(serde_json::Value::as_array)
                        else {
                            continue;
                        };
                        for b in buckets {
                            let bid = b
                                .get("bucketId")
                                .and_then(serde_json::Value::as_str)
                                .unwrap_or("");
                            let Some(pct) =
                                b.get("remainingFraction").and_then(observed_fraction_pct)
                            else {
                                continue;
                            };
                            let eta = (pct < 100)
                                .then(|| {
                                    b.get("resetTime")
                                        .and_then(serde_json::Value::as_str)
                                        .and_then(format_reset_eta_iso)
                                })
                                .flatten();
                            match bid {
                                "gemini-5h" => gem_5h = Some((pct, eta)),
                                "gemini-weekly" => gem_7d = Some((pct, eta)),
                                "3p-5h" => opus_5h = Some(pct),
                                "3p-weekly" => opus_7d = Some(pct),
                                _ => {}
                            }
                        }
                    }
                    if let (Some((g5, g5_eta)), Some((mut g7, mut g7_eta))) = (gem_5h, gem_7d) {
                        let now_ms = anyagents_core::now_ms();
                        let tier_cache_path = prof_dir.join(".anyagents-agy-tier.txt");
                        let tier_fresh = std::fs::metadata(&tier_cache_path)
                            .ok()
                            .and_then(|m| m.modified().ok())
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .is_some_and(|d| now_ms.saturating_sub(d.as_millis() as i64) < 600_000)
                            && prof_dir.join(".anyagents-profile-meta-sync-ms").exists();
                        let tier = if tier_fresh
                            && let Ok(cached_tier) = std::fs::read_to_string(&tier_cache_path)
                            && !cached_tier.trim().is_empty()
                            && cached_tier.trim() != "free"
                        {
                            cached_tier.trim().to_string()
                        } else {
                            let resolved_tier = http
                            .post("https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:loadCodeAssist")
                            .header("authorization", &format!("Bearer {active_token}"))
                            .header("content-type", "application/json")
                            .header("user-agent", "antigravity/3.0.0")
                            .send(b"{}")
                            .ok()
                            .and_then(|r| r.into_body().read_to_string().ok())
                            .and_then(|b| serde_json::from_str::<serde_json::Value>(&b).ok())
                            .as_ref().and_then(observed_agy_tier)
                            .filter(|tier| !tier.is_empty())?;
                            write_seat_shared_file(
                                &tier_cache_path,
                                format!("{resolved_tier}\n").as_bytes(),
                                0o600,
                            );
                            write_seat_shared_file(
                                &prof_dir.join(".anyagents-profile-meta-sync-ms"),
                                format!("{now_ms}\n").as_bytes(),
                                0o600,
                            );
                            resolved_tier
                        };
                        let reset_marker = prof_dir.join(".anyagents-quota-reset-ms");
                        if tier == "ultra" && g5.min(g7) > 0 {
                            let _ = std::fs::remove_file(&reset_marker);
                        } else if let Ok(raw_reset) = std::fs::read_to_string(&reset_marker)
                            && let Ok(reset_ms) = raw_reset.trim().parse::<i64>()
                        {
                            if reset_ms <= now_ms {
                                let _ = std::fs::remove_file(&reset_marker);
                            } else {
                                g7 = 0;
                                g7_eta =
                                    format_reset_eta_secs((reset_ms - now_ms) / 1_000).or(g7_eta);
                            }
                        }
                        let e5 = g5_eta.map(|e| format!(" ↻{e}")).unwrap_or_default();
                        let e7 = g7_eta.map(|e| format!(" ↻{e}")).unwrap_or_default();
                        let opus_min = observed_window_min(opus_5h, opus_7d)?;
                        return Some(format!(
                            "5h: {g5}% left{e5} · 7d: {g7}% left{e7} · opus: {opus_min}% left ({tier})"
                        ));
                    }
                }
                let val = fetch_models(&active_token)?;
                let models = val.get("models")?.as_object()?;
                let frac_of = |names: &[&str]| -> Option<i64> {
                    for n in names {
                        if let Some(m) = models.get(*n)
                            && let Some(qi) = m.get("quotaInfo")
                        {
                            let f = qi
                                .get("remainingFraction")
                                .and_then(serde_json::Value::as_f64)?;
                            return Some((f * 100.0).clamp(0.0, 100.0).round() as i64);
                        }
                    }
                    None
                };
                let gem = frac_of(&[
                    "gemini-4-argon-high",
                    "gemini-3.1-pro-high",
                    "gemini-2.5-pro",
                    "gemini-3-flash",
                ])?;
                Some(format!("5h: {gem}% left · 7d: {gem}% left"))
            }
            "kimi-code-plan" => {
                let key = read_first_non_empty_file(
                    &prof_dir,
                    &[
                        ".config/opencode/kimi-code-plan.key",
                        ".config/opencode/kimi.key",
                    ],
                )?;
                let resp = http
                    .get("https://api.kimi.com/coding/v1/usages")
                    .header("authorization", &format!("Bearer {key}"))
                    .header("user-agent", "claude-code/1.0")
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                let parse_num = |ptr: &str| -> Option<f64> {
                    let v = val.pointer(ptr)?;
                    v.as_f64().or_else(|| v.as_str()?.parse::<f64>().ok())
                };
                let pct_5h = if let Some(ratio) = parse_num("/usages/limit_5h/used_ratio") {
                    ((1.0 - ratio) * 100.0).clamp(0.0, 100.0).round() as i64
                } else {
                    let l5 = parse_num("/limits/0/detail/limit")?.max(1.0);
                    let r5 = parse_num("/limits/0/detail/remaining")?;
                    (r5 / l5 * 100.0).clamp(0.0, 100.0).round() as i64
                };
                let pct_mo = if let Some(ratio) = parse_num("/usages/limit_month_total/used_ratio")
                {
                    ((1.0 - ratio) * 100.0).clamp(0.0, 100.0).round() as i64
                } else {
                    let lm = parse_num("/usage/limit")?.max(1.0);
                    let rm = parse_num("/usage/remaining")?;
                    (rm / lm * 100.0).clamp(0.0, 100.0).round() as i64
                };
                let eta_5h = (pct_5h < 100)
                    .then(|| {
                        val.pointer("/usages/limit_5h/reset_time")
                            .or_else(|| val.pointer("/limits/0/detail/resetTime"))
                            .and_then(serde_json::Value::as_str)
                            .and_then(format_reset_eta_iso)
                    })
                    .flatten()
                    .map(|e| format!(" ↻{e}"))
                    .unwrap_or_default();
                let eta_mo = (pct_mo < 100)
                    .then(|| {
                        val.pointer("/usages/limit_month_total/reset_time")
                            .and_then(serde_json::Value::as_str)
                            .and_then(format_reset_eta_iso)
                    })
                    .flatten()
                    .map(|e| format!(" ↻{e}"))
                    .unwrap_or_default();
                Some(format!(
                    "5h: {pct_5h}% left{eta_5h} · mo: {pct_mo}% left{eta_mo}"
                ))
            }
            "minimax-code-plan" => {
                let key = read_first_non_empty_file(
                    &prof_dir,
                    &[
                        ".config/opencode/minimax-code-plan.key",
                        ".config/opencode/minimax.key",
                    ],
                )?;
                let resp = http
                    .get("https://api.minimax.io/v1/api/openplatform/coding_plan/remains")
                    .header("authorization", &format!("Bearer {key}"))
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                let m0 = val.pointer("/model_remains/0")?;
                let pct_5h = if let Some(p) = m0
                    .get("current_interval_remaining_percent")
                    .and_then(serde_json::Value::as_f64)
                {
                    p.clamp(0.0, 100.0).round() as i64
                } else {
                    let tot_5h = m0
                        .get("current_interval_total_count")
                        .and_then(serde_json::Value::as_f64)?
                        .max(1.0);
                    let rem_5h = m0
                        .get("current_interval_usage_count")
                        .and_then(serde_json::Value::as_f64)?;
                    (rem_5h / tot_5h * 100.0).clamp(0.0, 100.0).round() as i64
                };
                let pct_7d = if let Some(p) = m0
                    .get("current_weekly_remaining_percent")
                    .and_then(serde_json::Value::as_f64)
                {
                    p.clamp(0.0, 100.0).round() as i64
                } else {
                    let tot_7d = m0
                        .get("current_weekly_total_count")
                        .and_then(serde_json::Value::as_f64)?
                        .max(1.0);
                    let rem_7d = m0
                        .get("current_weekly_usage_count")
                        .and_then(serde_json::Value::as_f64)?;
                    (rem_7d / tot_7d * 100.0).clamp(0.0, 100.0).round() as i64
                };
                let eta_5h = (pct_5h < 100)
                    .then(|| {
                        m0.get("remains_time")
                            .and_then(serde_json::Value::as_i64)
                            .and_then(|ms| format_reset_eta_secs(ms / 1_000))
                    })
                    .flatten()
                    .map(|e| format!(" ↻{e}"))
                    .unwrap_or_default();
                let eta_7d = (pct_7d < 100)
                    .then(|| {
                        m0.get("weekly_remains_time")
                            .and_then(serde_json::Value::as_i64)
                            .and_then(|ms| format_reset_eta_secs(ms / 1_000))
                    })
                    .flatten()
                    .map(|e| format!(" ↻{e}"))
                    .unwrap_or_default();
                Some(format!(
                    "5h: {pct_5h}% left{eta_5h} · 7d: {pct_7d}% left{eta_7d}"
                ))
            }
            "glm-code-plan" => {
                let key = read_first_non_empty_file(
                    &prof_dir,
                    &[
                        ".config/opencode/glm-code-plan.key",
                        ".config/opencode/glm.key",
                    ],
                )?;
                let resp = http
                    .get("https://api.z.ai/api/monitor/usage/quota/limit")
                    .header("authorization", &key)
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                let limits = val.pointer("/data/limits")?.as_array()?;
                let mut rem_5h = None;
                let mut eta_5h = None;
                let mut rem_7d = None;
                let mut eta_7d = None;
                for item in limits {
                    if item.get("type").and_then(serde_json::Value::as_str) != Some("TOKENS_LIMIT")
                    {
                        continue;
                    }
                    let unit = item.get("unit").and_then(serde_json::Value::as_i64);
                    let rem = item.get("percentage").and_then(observed_remaining_pct)?;
                    let eta = (rem < 100)
                        .then(|| {
                            item.get("nextResetTime")
                                .and_then(serde_json::Value::as_i64)
                                .and_then(format_reset_eta_epoch_ms)
                        })
                        .flatten();
                    if unit == Some(3) {
                        rem_5h = Some(rem);
                        eta_5h = eta;
                    } else if unit == Some(6) {
                        rem_7d = Some(rem);
                        eta_7d = eta;
                    }
                }
                let r5 = rem_5h?;
                let r7 = rem_7d?;
                let e5 = eta_5h.map(|e| format!(" ↻{e}")).unwrap_or_default();
                let e7 = eta_7d.map(|e| format!(" ↻{e}")).unwrap_or_default();
                Some(format!("5h: {r5}% left{e5} · 7d: {r7}% left{e7}"))
            }
            "deepseek" => {
                let key = read_first_non_empty_file(&prof_dir, &[".config/opencode/deepseek.key"])
                    .or_else(|| read_dsh_yaml_ref(&prof_dir, "DEEPSEEK_API_KEY"))?;
                let resp = http
                    .get("https://api.deepseek.com/user/balance")
                    .header("authorization", &format!("Bearer {key}"))
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                let infos = val.get("balance_infos")?.as_array()?;
                let mut best: Option<(f64, String)> = None;
                for info in infos {
                    let bal = info
                        .get("total_balance")
                        .and_then(serde_json::Value::as_str)
                        .and_then(|s| s.parse::<f64>().ok())?;
                    let cur = info
                        .get("currency")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("CNY")
                        .to_string();
                    if best.as_ref().is_none_or(|(b, _)| bal > *b) {
                        best = Some((bal, cur));
                    }
                }
                let (bal, cur) = best?;
                let sym = if cur == "CNY" { "¥" } else { "$" };
                Some(format!("{sym}{bal:.2} {cur}"))
            }
            "openrouter" => {
                let key =
                    read_first_non_empty_file(&prof_dir, &[".config/opencode/openrouter.key"])
                        .or_else(|| read_dsh_yaml_ref(&prof_dir, "OPENROUTER_API_KEY"))?;
                let resp = http
                    .get("https://openrouter.ai/api/v1/credits")
                    .header("authorization", &format!("Bearer {key}"))
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                let total = val
                    .pointer("/data/total_credits")
                    .and_then(serde_json::Value::as_f64)?;
                let used = val
                    .pointer("/data/total_usage")
                    .and_then(serde_json::Value::as_f64)?;
                let rem = (total - used).max(0.0);
                Some(format!("${rem:.2} left (${used:.0} / ${total:.0})"))
            }
            "cursor" => {
                let (mut access, refresh) = extract_cursor_oauth_from_db(&prof_dir)?;
                let fetch_summary = |tok: &str| -> Option<serde_json::Value> {
                    let uid = extract_jwt_sub_tail(tok)?;
                    let resp = http
                        .get("https://cursor.com/api/usage-summary")
                        .header("authorization", &format!("Bearer {tok}"))
                        .header(
                            "cookie",
                            &format!("WorkosCursorSessionToken={uid}%3A%3A{tok}"),
                        )
                        .call()
                        .ok()?;
                    let body = resp.into_body().read_to_string().ok()?;
                    serde_json::from_str(&body).ok()
                };
                let val = match fetch_summary(&access) {
                    Some(v) => v,
                    None if !refresh.is_empty() => {
                        let _reservation = OAuthRefreshDiskReservation::acquire(
                            &prof_dir,
                            &[],
                            refresh.len(),
                            anyagents_core::now_ms(),
                        )
                        .map_err(|err| {
                            eprintln!(
                                "oauth refresh aborted before network call ({}): {err}",
                                prof_dir.display()
                            )
                        })
                        .ok()?;
                        let cursor_cid = concat!("KbZUR41c", "Y7W6zRSd", "pSUJ7I7m", "LYBKOCmB");
                        let req_body = serde_json::json!({
                            "grant_type": "refresh_token",
                            "client_id": cursor_cid,
                            "refresh_token": refresh,
                        })
                        .to_string();
                        let rresp = http
                            .post("https://api2.cursor.sh/oauth/token")
                            .header("content-type", "application/json")
                            .send(req_body.as_bytes())
                            .ok()?;
                        let rbody = rresp.into_body().read_to_string().ok()?;
                        let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                        access = rval.get("access_token")?.as_str()?.trim().to_string();
                        fetch_summary(&access)?
                    }
                    None => return None,
                };
                let plan = val.pointer("/individualUsage/plan")?;
                let limit_cents = plan.get("limit").and_then(serde_json::Value::as_f64)?;
                let rem_cents = plan.get("remaining").and_then(serde_json::Value::as_f64)?;
                let rem_pct = plan
                    .get("totalPercentUsed")
                    .and_then(observed_remaining_pct)?;
                let tier = val
                    .get("membershipType")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                if limit_cents > 0.0 {
                    let rem_usd = (rem_cents / 100.0).round() as i64;
                    let lim_usd = (limit_cents / 100.0).round() as i64;
                    Some(format!(
                        "mo: {rem_pct}% left (${rem_usd}/${lim_usd} {tier})"
                    ))
                } else {
                    Some(format!("mo: {rem_pct}% left ({tier})"))
                }
            }
            "muse" => {
                let auth_text = std::fs::read_to_string(prof_dir.join("muse/auth.json"))
                    .or_else(|_| std::fs::read_to_string(prof_dir.join(".config/muse/auth.json")))
                    .ok()?;
                let auth: serde_json::Value = serde_json::from_str(&auth_text).ok()?;
                let token = auth
                    .pointer("/providers/meta/access_token")?
                    .as_str()?
                    .trim();
                if token.is_empty() {
                    return None;
                }
                let resp = http
                    .post("https://api.meta.ai/muse-code/key")
                    .header("authorization", &format!("Bearer {token}"))
                    .header("x-api-version", "1.0.0")
                    .header("content-type", "application/json")
                    .send(b"{}")
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                if val
                    .get("is_subs_active")
                    .and_then(serde_json::Value::as_bool)
                    != Some(true)
                {
                    return None;
                }
                let tier = val
                    .get("subs_tier_name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("active");
                let short_tier = tier.strip_prefix("Muse Code ").unwrap_or(tier);
                let rem_pct = val
                    .pointer("/subs_usage/window/used_percent")
                    .or_else(|| val.pointer("/subs_usage/used_percent"))
                    .and_then(observed_remaining_pct)?;
                Some(format!("5h: {rem_pct}% left ({short_tier})"))
            }
            "xai" => {
                let auth_rel = ".grok/auth.json";
                let auth_path = prof_dir.join(auth_rel);
                let auth_text = std::fs::read_to_string(&auth_path).ok()?;
                let mut root_auth: serde_json::Value = serde_json::from_str(&auth_text).ok()?;
                let entry_key = if root_auth.get("key").is_some() {
                    None
                } else {
                    root_auth
                        .as_object()?
                        .iter()
                        .find(|(_, v)| v.get("key").is_some())
                        .map(|(k, _)| k.clone())
                };
                let auth_entry = match &entry_key {
                    Some(k) => root_auth.get(k)?,
                    None => &root_auth,
                };
                let key = auth_entry.get("key")?.as_str()?.trim().to_string();
                if key.is_empty() {
                    return None;
                }
                let rtok = auth_entry
                    .get("refresh_token")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_string();
                let issuer = auth_entry
                    .get("oidc_issuer")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("https://auth.x.ai")
                    .trim_end_matches('/')
                    .to_string();
                let cid = auth_entry
                    .get("oidc_client_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_string();
                let fetch_billing = |tok: &str| -> Option<serde_json::Value> {
                    let resp = http
                        .get("https://cli-chat-proxy.grok.com/v1/billing?format=credits")
                        .header("authorization", &format!("Bearer {tok}"))
                        .header("x-xai-token-auth", "xai-grok-cli")
                        .call()
                        .ok()?;
                    let body = resp.into_body().read_to_string().ok()?;
                    serde_json::from_str(&body).ok()
                };
                let val = match fetch_billing(&key) {
                    Some(v) => v,
                    None => {
                        if rtok.is_empty() || cid.is_empty() {
                            return None;
                        }
                        let reservation = OAuthRefreshDiskReservation::acquire(
                            &prof_dir,
                            &[auth_rel],
                            auth_text.len(),
                            anyagents_core::now_ms(),
                        )
                        .map_err(|err| {
                            eprintln!(
                                "oauth refresh aborted before network call ({}): {err}",
                                prof_dir.display()
                            )
                        })
                        .ok()?;
                        let form_body = format!(
                            "grant_type=refresh_token&refresh_token={rtok}&client_id={cid}"
                        );
                        let rresp = http
                            .post(&format!("{issuer}/oauth2/token"))
                            .header("content-type", "application/x-www-form-urlencoded")
                            .send(form_body.as_bytes())
                            .ok()?;
                        let rbody = rresp.into_body().read_to_string().ok()?;
                        let rval: serde_json::Value = serde_json::from_str(&rbody).ok()?;
                        let new_acc = rval.get("access_token")?.as_str()?.trim().to_string();
                        let target_obj = match &entry_key {
                            Some(k) => root_auth
                                .get_mut(k)
                                .and_then(serde_json::Value::as_object_mut),
                            None => root_auth.as_object_mut(),
                        };
                        if let Some(obj) = target_obj {
                            obj.insert(
                                "key".to_string(),
                                serde_json::Value::String(new_acc.clone()),
                            );
                            if let Some(new_ref) = rval
                                .get("refresh_token")
                                .and_then(serde_json::Value::as_str)
                            {
                                obj.insert(
                                    "refresh_token".to_string(),
                                    serde_json::Value::String(new_ref.to_string()),
                                );
                            }
                            if let Ok(updated) = serde_json::to_string_pretty(&root_auth) {
                                reservation
                                    .persist_credentials(&[auth_rel], updated.as_bytes())
                                    .map_err(|err| {
                                        eprintln!("credential refresh persistence failed: {err}")
                                    })
                                    .ok()?;
                                propagate_profile_files_to_bound_agents(
                                    data,
                                    provider,
                                    profile,
                                    &[auth_rel],
                                )
                                .map_err(|err| eprintln!("runtime persistence failed: {err}"))
                                .ok()?;
                            }
                        }
                        fetch_billing(&new_acc)?
                    }
                };
                let used = val
                    .pointer("/config/creditUsagePercent")
                    .or_else(|| val.get("creditUsagePercent"))
                    .and_then(serde_json::Value::as_f64)?;
                let rem = (100.0 - used).clamp(0.0, 100.0).round() as i64;
                let eta = (rem < 100)
                    .then(|| {
                        val.pointer("/config/currentPeriod/end")
                            .or_else(|| val.pointer("/currentPeriod/end"))
                            .and_then(serde_json::Value::as_str)
                            .and_then(format_reset_eta_iso)
                    })
                    .flatten()
                    .map(|e| format!(" ↻{e}"))
                    .unwrap_or_default();
                Some(format!("7d: {rem}% left{eta}"))
            }
            "qwen-token-plan" => {
                let key = read_first_non_empty_file(
                    &prof_dir,
                    &[
                        ".config/opencode/qwen-coding-plan.key",
                        ".config/opencode/qwen.key",
                    ],
                )?;
                let resp = http
                    .get("https://token-plan.maas.qwencloudapi.com/compatible-mode/v1/models")
                    .header("authorization", &format!("Bearer {key}"))
                    .call()
                    .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                let count = val.get("data")?.as_array()?.len();
                Some(format!("active ({count} models)"))
            }
            "sylphx" | "spiron" => {
                let has_creds = profile_has_sylphx_credentials(&prof_dir);
                let health_url = std::env::var("ANYAGENTS_SYLPHX_HEALTH_URL")
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty());
                if let Some(url) = health_url {
                    let resp = http.get(&url).call().ok()?;
                    let body = resp.into_body().read_to_string().ok()?;
                    let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                    if let Some(pct) = val
                        .get("remaining_pct")
                        .or_else(|| val.get("remainingPct"))
                        .and_then(serde_json::Value::as_i64)
                    {
                        return Some(format!("{}% left", pct.clamp(0, 100)));
                    }
                    if val.get("status").and_then(serde_json::Value::as_str) == Some("ok") {
                        return Some("online (gateway-edge)".to_string());
                    }
                    None
                } else if has_creds {
                    Some("online (gateway-edge)".to_string())
                } else if !crate::sandbox::is_isolated_test_env() {
                    let resp = http.get("https://api.sylphx.com/health").call().ok()?;
                    let body = resp.into_body().read_to_string().ok()?;
                    let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                    if val.get("status").and_then(serde_json::Value::as_str) == Some("ok") {
                        Some("online (gateway-edge)".to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            "opencode" | "omp" => probe_opencode_profile_credentials(&prof_dir, provider, now_ms),
            "google-gemini" => {
                let key = read_first_non_empty_file(
                    &prof_dir,
                    &["gemini-api-key", ".config/opencode/gemini.key"],
                )
                .or_else(|| read_dsh_yaml_ref(&prof_dir, "GEMINI_API_KEY"))?;
                let resp = http
                .get(&format!(
                    "https://generativelanguage.googleapis.com/v1beta/models?pageSize=1&key={key}"
                ))
                .call()
                .ok()?;
                let body = resp.into_body().read_to_string().ok()?;
                let val: serde_json::Value = serde_json::from_str(&body).ok()?;
                if val.get("models").is_some() {
                    Some("active (api-key)".to_string())
                } else {
                    None
                }
            }
            _ => None,
        }
    })();
    let result = if provider != "anthropic" {
        let reset_active = std::fs::read_to_string(prof_dir.join(".anyagents-quota-reset-ms"))
            .ok()
            .and_then(|s| s.trim().parse::<i64>().ok())
            .is_some_and(|ms| ms > now_ms);
        match result {
            Some(s) => Some(overlay_active_quota_reset_on_cached_usage(
                &prof_dir,
                &last_usage_path,
                &s,
                now_ms,
            )),
            None if reset_active => Some(overlay_active_quota_reset_on_cached_usage(
                &prof_dir,
                &last_usage_path,
                "",
                now_ms,
            )),
            None => None,
        }
    } else {
        result
    };
    if provider != "anthropic" {
        if let Some(ref s) = result {
            write_seat_shared_file(&last_usage_path, format!("{s}\n").as_bytes(), 0o600);
        } else {
            write_seat_shared_file(&last_usage_path, b"__NONE__\n", 0o600);
        }
    }
    result
}
