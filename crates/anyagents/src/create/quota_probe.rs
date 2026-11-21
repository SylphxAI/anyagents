//! Per-provider live profile usage & quota probing.

use std::path::Path;

use super::profile_identity::{read_dsh_yaml_ref, read_first_non_empty_file};
use super::quota::{
    format_reset_eta_epoch_ms, format_reset_eta_secs, overlay_active_quota_reset_on_cached_usage,
    probe_opencode_profile_credentials, profile_has_sylphx_credentials,
};
use super::subscription_status::{is_passive_subscription, local_subscription_status};
use super::write_seat_shared_file;

pub(crate) fn probe_one_profile_usage(
    data: &Path,
    provider: &str,
    profile: &str,
) -> Option<String> {
    probe_one_profile_usage_with_options(data, provider, profile, false)
}

pub(crate) fn probe_one_profile_usage_with_options(
    data: &Path,
    provider: &str,
    profile: &str,
    bypass_cache: bool,
) -> Option<String> {
    let prof_dir = data.join("profiles").join(provider).join(profile);
    // A probe records its reading inside the profile; a profile that does not
    // exist has no reading, and recording one would create it.
    if !prof_dir.is_dir() {
        return None;
    }
    let last_usage_path = prof_dir.join(".anyagents-last-usage.txt");
    let now_ms = anyagents_core::now_ms();
    if is_passive_subscription(provider) {
        let status = local_subscription_status(&prof_dir, provider, now_ms);
        if status.expired {
            return Some(status.usage());
        }
        // Deliberately ignore the old scraped cache. Only harness reset markers
        // are quota evidence here; having credentials says nothing about quota.
        let usage =
            overlay_active_quota_reset_on_cached_usage(&prof_dir, None, &status.usage(), now_ms);
        return (!usage.is_empty()).then_some(usage);
    }
    if !bypass_cache
        && let Ok(meta) = std::fs::metadata(&last_usage_path)
        && let Ok(mtime) = meta.modified()
        && let Ok(dur) = mtime.duration_since(std::time::UNIX_EPOCH)
        && now_ms.saturating_sub(dur.as_millis() as i64) < 120_000
        && let Ok(cached) = std::fs::read_to_string(&last_usage_path)
    {
        let trimmed = cached.trim();
        if trimmed == "__NONE__" {
            let usage = overlay_active_quota_reset_on_cached_usage(&prof_dir, None, "", now_ms);
            return (!usage.is_empty()).then_some(usage);
        }
        if !trimmed.is_empty() {
            return Some(overlay_active_quota_reset_on_cached_usage(
                &prof_dir,
                Some(&last_usage_path),
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
                    let rem = item.get("percentage").and_then(serde_json::Value::as_f64)?;
                    let rem = (0.0..=100.0)
                        .contains(&rem)
                        .then(|| (100.0 - rem).round() as i64)?;
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
    let usage = overlay_active_quota_reset_on_cached_usage(
        &prof_dir,
        Some(&last_usage_path),
        result.as_deref().unwrap_or_default(),
        now_ms,
    );
    let cached = if usage.is_empty() { "__NONE__" } else { &usage };
    write_seat_shared_file(&last_usage_path, format!("{cached}\n").as_bytes(), 0o600);
    (!usage.is_empty()).then_some(usage)
}
