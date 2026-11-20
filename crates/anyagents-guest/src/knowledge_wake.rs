//! Transport the current wake context in memory, not via the staged prompt.
use std::time::Duration;

use anyagents_harness::OutboundMessage;
use serde::Deserialize;

#[derive(Deserialize)]
struct Wake {
    context: Option<String>,
}

pub(super) fn enrich(
    message: &mut OutboundMessage,
    callback_url: &str,
    token: &str,
    turn_id: &str,
) {
    message.context.org_instructions = fetch(callback_url, token, turn_id);
}

fn fetch(callback_url: &str, token: &str, turn_id: &str) -> Option<String> {
    let base = callback_url.strip_suffix("/v1/internal/guest/events")?;
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(2)))
        .max_redirects(0)
        .build()
        .into();
    let mut response = agent
        .post(format!("{base}/v1/internal/guest/knowledge-wake"))
        .header("authorization", format!("Bearer {token}"))
        .send_json(serde_json::json!({"turn_id": turn_id}))
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    response
        .body_mut()
        .with_config()
        .limit(32 * 1024)
        .read_json::<Wake>()
        .ok()?
        .context
        .filter(|section| {
            section.len() <= 4096
                && section.starts_with("<org_instructions>\n")
                && section.ends_with("</org_instructions>")
        })
}
