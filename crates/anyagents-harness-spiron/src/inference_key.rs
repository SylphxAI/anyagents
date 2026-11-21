//! Write-only setup of the optional customer inference credential.

use std::time::Duration;

use anyagents_harness::RemoteSessionError;
use serde_json::{Value, json};
use ureq::http::Uri;

use crate::cloud::{is_sylphx_gateway_key, validate_uuid};
use crate::cloud_api::{proto, transport};

pub const BYOK_ENV: &str = "ANYAGENTS_SPIRON_CLOUD_BYOK_KEY";
pub const BYOK_ENV_SHORT: &str = "SPIRON_CLOUD_BYOK_KEY";

pub fn is_env_key(key: &str) -> bool {
    matches!(key, BYOK_ENV | BYOK_ENV_SHORT)
}

/// Secret bytes are used only in the write-only setup request, never in durable
/// create/message bodies. Deliberately not serializable.
#[derive(Clone)]
pub struct InferenceKey(String);

impl std::fmt::Debug for InferenceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InferenceKey([redacted])")
    }
}

impl InferenceKey {
    pub fn new(value: String) -> Result<Self, RemoteSessionError> {
        let valid = value.strip_prefix("sylphx_sk_").is_some_and(|suffix| {
            !suffix.is_empty()
                && suffix.len() <= 256
                && suffix
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        });
        if !valid {
            return Err(proto(
                "invalid Spiron inference key: expected a secret Access key without whitespace",
            ));
        }
        anyagents_harness::masking::register_secret_values([&value]);
        Ok(Self(value))
    }

    /// Unset enables the legacy path; a present but invalid setting fails closed.
    pub fn from_process_env() -> Result<Option<Self>, RemoteSessionError> {
        for name in [BYOK_ENV, BYOK_ENV_SHORT] {
            match std::env::var(name) {
                Ok(value) => return Self::new(value).map(Some),
                Err(std::env::VarError::NotPresent) => (),
                Err(std::env::VarError::NotUnicode(_)) => {
                    return Err(proto("Spiron inference key setting must be UTF-8"));
                }
            }
        }
        Ok(None)
    }

    pub fn validate_target(base_url: &str, bearer: Option<&str>) -> Result<(), RemoteSessionError> {
        if !bearer.is_some_and(|t| !t.trim().is_empty() && !is_sylphx_gateway_key(t)) {
            return Err(RemoteSessionError::Auth(
                "Spiron inference key setup requires a separate Spiron API key or JWT".into(),
            ));
        }
        let uri: Uri = base_url
            .parse()
            .map_err(|_| proto("invalid Spiron inference key origin"))?;
        let Some(authority) = uri.authority() else {
            return Err(proto("invalid Spiron inference key origin"));
        };
        let loopback = matches!(authority.host(), "127.0.0.1" | "[::1]");
        if base_url.contains('#')
            || authority.as_str().contains('@')
            || !matches!(uri.path(), "" | "/")
            || uri.query().is_some()
            || !(uri.scheme_str() == Some("https")
                || (loopback && uri.scheme_str() == Some("http")))
        {
            return Err(proto(
                "Spiron inference key setup requires an HTTPS origin without credentials or query",
            ));
        }
        Ok(())
    }

    /// PUT is idempotent. Require exact acknowledgement; never surface provider
    /// response bytes (even on malformed replies) or follow redirects with the key.
    pub fn install(
        &self,
        base_url: &str,
        bearer: Option<&str>,
        agent_id: &str,
        timeout: Duration,
    ) -> Result<(), RemoteSessionError> {
        Self::validate_target(base_url, bearer)?;
        let id = validate_uuid(agent_id, "agent_id")?;
        let client: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            .max_redirects(0)
            .max_redirects_will_error(false)
            .http_status_as_error(false)
            .proxy(None)
            .build()
            .into();
        let mut response = client
            .put(format!(
                "{}/v1/agents/{id}/inference-key",
                base_url.trim_end_matches('/')
            ))
            .header("accept", "application/json")
            .header(
                "authorization",
                format!("Bearer {}", bearer.unwrap_or_default()),
            )
            .send_json(json!({ "sylphx_api_key": self.0 }))
            .map_err(|_| {
                transport("Spiron inference key setup request failed; message not sent")
            })?;
        let status = response.status().as_u16();
        if status != 200 {
            return Err(proto(format!(
                "Spiron inference key setup returned HTTP {status}; message not sent"
            )));
        }
        let configured = response
            .body_mut()
            .with_config()
            .limit(4096)
            .read_json::<Value>()
            .ok()
            .and_then(|json| json.get("byok_configured").and_then(Value::as_bool));
        if configured != Some(true) {
            return Err(proto(
                "Spiron did not confirm inference key setup; message not sent",
            ));
        }
        Ok(())
    }
}
