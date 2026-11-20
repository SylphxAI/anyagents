//! Push delivery for generic webhook watches.

use anyagents_core::{AgentId, AgentSchedule, Error, MessageId, Store};
use sha2::{Digest, Sha256};

use super::{
    Owner, WatchTarget, cancel_for_gone_owner, claim_and_deliver_scheduled_wake, parse_watch_spec,
    schedule_owner, schedules_changed,
};

#[derive(Debug, Default)]
pub struct WebhookDispatch {
    pub matched: usize,
    pub fired_agents: Vec<AgentId>,
    /// A retry must be requested if one or more subscriptions could not commit.
    pub retry_required: bool,
}

const MAX_WEBHOOK_PAYLOAD_BYTES: usize = 8 * 1024;

/// Missing producer IDs identify fresh deliveries, not retries. A clock and
/// body length are not unique when several requests arrive together.
pub(crate) fn fresh_delivery_id() -> String {
    MessageId::generate().to_string()
}

/// Match the exact topic or the `*` watch. Each persistent watch queues one
/// wake per delivery ID, even while previous wakes are undelivered.
pub fn fire_webhook_event(
    store: &Store,
    topic: &str,
    delivery_id: &str,
    payload: &serde_json::Value,
    now: i64,
) -> Result<WebhookDispatch, Error> {
    let topic = topic.trim();
    let mut dispatch = WebhookDispatch::default();
    if topic.is_empty() {
        return Ok(dispatch);
    }
    let payload_snippet = format_webhook_payload(payload);
    let delivery_key = Sha256::digest(delivery_id.as_bytes());
    for sched in store.list_schedules(None, true)? {
        if sched.kind != "watch" {
            continue;
        }
        let Ok(WatchTarget::Webhook { topic: watched }) = parse_watch_spec(&sched.spec) else {
            continue;
        };
        if watched != topic && watched != "*" {
            continue;
        }
        let agent = match schedule_owner(store, &sched) {
            Owner::Live(agent) => agent,
            Owner::Gone => {
                cancel_for_gone_owner(store, &sched);
                continue;
            }
            Owner::Unknown => {
                dispatch.retry_required |= sched.persistent;
                continue;
            }
        };
        dispatch.matched += 1;
        let prompt = format!(
            "[webhook: {topic} delivery={delivery_id}]\n{}\n\nPayload:\n{payload_snippet}",
            sched.prompt
        );
        let observed = format!("event:{topic}:{delivery_id}");
        let fired = if sched.persistent {
            let source_key = format!("schedule:{}:webhook:v1:{delivery_key:x}", sched.id);
            match deliver_subscription(store, &sched, &source_key, prompt, &observed, now) {
                Ok(committed) => committed,
                Err(err) => {
                    eprintln!("Webhook subscription {} not delivered: {err}", sched.id);
                    dispatch.retry_required = true;
                    false
                }
            }
        } else {
            claim_and_deliver_scheduled_wake(
                store,
                &agent,
                &sched,
                None,
                "fired",
                None,
                sched.watch_baseline.as_deref(),
                Some(&observed),
                &prompt,
                &format!("schedule:{}:webhook:{topic}:{delivery_id}", sched.id),
                now,
            )
        };
        if fired && !dispatch.fired_agents.contains(&agent.id) {
            dispatch.fired_agents.push(agent.id);
        }
    }
    if dispatch.matched > 0 {
        schedules_changed(store.data_dir());
    }
    Ok(dispatch)
}

fn deliver_subscription(
    store: &Store,
    sched: &AgentSchedule,
    source_key: &str,
    prompt: String,
    observed: &str,
    now: i64,
) -> Result<bool, Error> {
    let _lock = store.try_lock_agent_lifecycle(&sched.agent_id)?;
    let agent = match store.agent(&sched.agent_id) {
        Ok(agent) => agent,
        Err(Error::AgentNotFound(_)) => return Ok(false),
        Err(err) => return Err(err),
    };
    if crate::server::is_explicitly_archived(store.data_dir(), &agent) {
        return Ok(false);
    }
    let fired = store
        .deliver_persistent_webhook_wake(&sched.id, &agent.id, source_key, prompt, observed, now)?;
    if fired {
        crate::create::poke_agent_wake_socket(store.data_dir(), &agent.id);
        crate::create::poke_agent_wake_socket_mail(store.data_dir(), &agent.id);
    }
    Ok(fired)
}

fn format_webhook_payload(payload: &serde_json::Value) -> String {
    let raw = serde_json::to_string_pretty(payload).unwrap_or_else(|_| payload.to_string());
    if raw.len() <= MAX_WEBHOOK_PAYLOAD_BYTES {
        return raw;
    }
    let mut end = MAX_WEBHOOK_PAYLOAD_BYTES;
    while !raw.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...(truncated)", &raw[..end])
}
