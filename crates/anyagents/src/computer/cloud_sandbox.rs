//! Cloud computer sandboxes: `tools.computer` requests that name a
//! `session_id` / `sandbox_id` or `provider: "cloud"` are forwarded to the
//! Sylphx sandbox API before anything touches a local desktop.

use super::*;

/// Whether `req` names a cloud sandbox.
pub fn is_cloud_request(req: &Value) -> bool {
    req.get("session_id")
        .or_else(|| req.get("sandbox_id"))
        .and_then(Value::as_str)
        .is_some()
        || req
            .get("provider")
            .and_then(Value::as_str)
            .is_some_and(|p| p.eq_ignore_ascii_case("cloud"))
}

/// Forward every cloud action (including snapshots/clipboard) to the sandbox API.
pub async fn execute(
    data_dir: Option<&Path>,
    config_dir: &Path,
    req: Value,
) -> Result<Value, String> {
    let payload = if let Some(inner) = req.get("action_json").or_else(|| req.get("payload")) {
        inner.clone()
    } else {
        req.clone()
    };
    let display_owned = payload
        .get("display")
        .or_else(|| req.get("display"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let display = display_owned.as_deref();
    let session_id = req
        .get("session_id")
        .or_else(|| req.get("sandbox_id"))
        .and_then(Value::as_str);
    let base_url =
        std::env::var("SYLPHX_SANDBOX_URL").unwrap_or_else(|_| resolve_sylphx_gateway_base_url());
    let api_key = resolve_sylphx_api_key(data_dir, config_dir)
        .ok_or("cloud computer sandbox requires SYLPHX_API_KEY or configured Sylphx gateway key")?;
    let endpoint = format!(
        "{}/v1/sandboxes/{}/computer",
        base_url.trim_end_matches('/'),
        session_id.unwrap_or("default")
    );
    // Preserve the cloud pixel-action protocol; semantic/ref requests stay intact.
    let cloud_payload = if payload
        .get("ref")
        .or_else(|| payload.get("ref_id"))
        .is_none()
    {
        parse_computer_action(&payload)
            .ok()
            .map(|action| json!({"action": action, "display": display}))
    } else {
        None
    }
    .unwrap_or(payload);
    let (status, bytes) = tokio::task::spawn_blocking(move || {
        post_json_bytes(&endpoint, &api_key, &cloud_payload, 30)
    })
    .await
    .map_err(|e| e.to_string())??;
    let body: Value =
        serde_json::from_slice(&bytes).map_err(|e| format!("invalid cloud computer JSON: {e}"))?;
    if (200..300).contains(&status) {
        Ok(body)
    } else {
        Err(format!("cloud computer sandbox returned {status}: {body}"))
    }
}
