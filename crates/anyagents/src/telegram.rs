//! Long-poll Telegram bots recorded as channels.
//!
//! The token is read from the credential store when a poll connects, and it
//! stays in memory. `getUpdates` and `sendMessage` are built without it; the
//! token is attached only on the request path. Failures name no URL, so a
//! token on that path cannot reach a log.

use std::sync::Arc;
use std::time::Duration;

use anyagents_channel_telegram::{BotCall, Poll, deliver_to_mailbox};
use anyagents_core::{Channel, CredentialStore, FileCredentialStore, Store};

const API: &str = "https://api.telegram.org";

/// One poll cycle for every telegram channel. A channel with no secret is skipped.
pub fn poll_once(store: &Store, data_dir: &std::path::Path) -> Result<(), anyagents_core::Error> {
    let secrets = FileCredentialStore::new(data_dir);
    let mut failure = None;
    for channel in store.list_channels()? {
        if let Err(err) = poll_channel(store, &secrets, &channel)
            && failure.is_none()
        {
            failure = Some(err);
        }
    }
    match failure {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

fn poll_channel(
    store: &Store,
    secrets: &FileCredentialStore,
    channel: &Channel,
) -> Result<(), anyagents_core::Error> {
    if channel.kind != "telegram" {
        return Ok(());
    }
    let Some(credential_ref) = channel.credential_ref.as_deref() else {
        return Ok(());
    };
    let Some(agent_id) = channel.agents.first() else {
        return Ok(());
    };
    let secret = secrets.read(credential_ref)?;
    let token = secret
        .as_str()
        .map_err(|_| anyagents_core::Error::Message("telegram token is not text".into()))?
        .to_string();
    let mut poll = Poll::default();
    let call = poll.get_updates(0);
    let response = call_api(&token, &call)?;
    let body = updates_payload(&response)?;
    let routed = poll
        .route("bot", agent_id.as_str(), &body)
        .map_err(|err| anyagents_core::Error::Message(err.to_string()))?;
    for one in &routed {
        deliver_to_mailbox(store, one, &[])?;
    }
    Ok(())
}

/// Run `poll_once` until `stop` is set. A failed poll waits and tries again.
pub async fn run(
    store: Arc<Store>,
    data_dir: std::path::PathBuf,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *stop.borrow() {
            return;
        }
        let store = Arc::clone(&store);
        let dir = data_dir.clone();
        match tokio::task::spawn_blocking(move || poll_once(&store, &dir)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                let _ = err;
            }
            Err(err) => {
                let _ = err;
            }
        }
        tokio::select! {
            _ = stop.changed() => {
                if *stop.borrow() {
                    return;
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
    }
}

/// The update list from a `getUpdates` response. Anything else is refused.
pub(crate) fn updates_payload(response: &str) -> Result<String, anyagents_core::Error> {
    let value: serde_json::Value = serde_json::from_str(response)
        .map_err(|_| anyagents_core::Error::Message("telegram response is not json".into()))?;
    match value.get("result") {
        Some(serde_json::Value::Array(items)) => {
            Ok(serde_json::Value::Array(items.clone()).to_string())
        }
        _ => Err(anyagents_core::Error::Message(
            "telegram response has no update list".into(),
        )),
    }
}

fn call_api(token: &str, call: &BotCall) -> Result<String, anyagents_core::Error> {
    let url = format!("{API}/bot{token}/{}", call.method);
    let response = ureq::post(&url)
        .send_json(&call.body)
        .map_err(|_| anyagents_core::Error::Message("telegram request failed".into()))?;
    response
        .into_body()
        .read_to_string()
        .map_err(|_| anyagents_core::Error::Message("telegram response could not be read".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn updates_payload_keeps_only_the_result_array() {
        let payload = updates_payload(r#"{"ok":true,"result":[{"update_id":1}]}"#).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(parsed[0]["update_id"], 1);
        assert!(updates_payload(r#"{"ok":true,"result":{"message_id":9}}"#).is_err());
    }

    #[test]
    fn a_channel_without_a_secret_is_not_polled() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: None,
            })
            .unwrap();
        poll_once(&store, dir.path()).unwrap();
    }
}
