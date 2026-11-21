use super::SylphxSandboxClient;
use std::time::Duration;

impl SylphxSandboxClient {
    pub(super) fn redact(&self, text: String) -> String {
        if self.scoped
            && let Some(token) = &self.token
        {
            return text.replace(token, "[redacted]");
        }
        text
    }

    pub(super) fn redact_event(
        &self,
        mut event: super::EnvdProcessEvent,
    ) -> super::EnvdProcessEvent {
        use super::EnvdProcessEvent;
        if self.scoped
            && let Some(token) = &self.token
        {
            match &mut event {
                EnvdProcessEvent::Data {
                    stdout,
                    stderr,
                    pty,
                } => {
                    for bytes in [stdout, stderr, pty] {
                        if bytes.windows(token.len()).any(|w| w == token.as_bytes()) {
                            *bytes = String::from_utf8_lossy(bytes)
                                .replace(token, "[redacted]")
                                .into_bytes();
                        }
                    }
                }
                EnvdProcessEvent::End { error, status, .. } => {
                    for text in [error, status].into_iter().flatten() {
                        *text = text.replace(token, "[redacted]");
                    }
                }
                _ => {}
            }
        }
        event
    }

    pub(super) fn rest_agent(&self, timeout_ms: u64) -> ureq::Agent {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_millis(timeout_ms.max(1_000))))
            .http_status_as_error(false)
            .max_redirects(if self.scoped { 0 } else { 10 })
            .build()
            .new_agent()
    }

    pub(super) fn stream_agent(&self) -> ureq::Agent {
        // Long-lived turns via envd streaming must never be aborted at 120s/300s.
        ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(15)))
            .timeout_global(None)
            .timeout_recv_body(None)
            .timeout_send_body(None)
            .http_status_as_error(false)
            .max_redirects(if self.scoped { 0 } else { 10 })
            .build()
            .new_agent()
    }

    pub(super) fn envd_auth<'a>(
        &'a self,
        url: &str,
        lease_token: Option<&'a str>,
    ) -> Result<Option<&'a str>, String> {
        if !self.scoped {
            return Ok(lease_token.or(self.token.as_deref()));
        }
        if self.token.as_deref().is_some_and(|key| url.contains(key)) {
            return Err("invalid sandbox envd endpoint".into());
        }
        crate::sandbox_exec::validate_scoped_endpoint(url)?;
        let same_origin = url::Url::parse(url)
            .ok()
            .zip(url::Url::parse(&self.endpoint).ok())
            .is_some_and(|(envd, provider)| envd.origin() == provider.origin());
        match (lease_token, same_origin) {
            (Some(token), false) if Some(token) == self.token.as_deref() => {
                Err("sandbox billing credential cannot be used at a separate origin".into())
            }
            (Some(token), _) => Ok(Some(token)),
            (None, true) => Ok(self.token.as_deref()),
            (None, false) => {
                Err("sandbox envd has no lease credential for its separate origin".into())
            }
        }
    }
}
