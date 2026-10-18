//! Long-poll Telegram bots recorded as channels, and post one agent's text back.
//!
//! The token is read from the credential store when a poll or a send connects,
//! and it stays in memory. `getUpdates` and `sendMessage` are built without it;
//! the token is attached only on the request path. Failures name no URL, so a
//! token on that path cannot reach a log.

use std::sync::Arc;
use std::time::Duration;

use anyagents_channel_telegram::{
    BotCall, Poll, deliver_to_mailbox, send_message, sent_message_id,
};
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
    let token = token_of(secrets, credential_ref)?;
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

/// A `sendMessage` for one chat, plus the secret it must be sent with.
#[derive(Debug)]
pub(crate) struct Outbound {
    pub credential_ref: String,
    pub call: BotCall,
}

/// Build the outbound call. The token is not read here.
pub(crate) fn outbound_call(
    store: &Store,
    from: Option<&str>,
    chat_id: i64,
    text: &str,
    reply_to: Option<i64>,
) -> Result<Outbound, anyagents_core::Error> {
    let channel = channel_for(store, from)?;
    let Some(credential_ref) = channel.credential_ref else {
        return Err(anyagents_core::Error::Message(
            "telegram channel has no credential".into(),
        ));
    };
    Ok(Outbound {
        credential_ref,
        call: send_message(chat_id, text, reply_to),
    })
}

/// Post one text through the agent's bot. The token is read for this call only.
pub fn post_text(
    store: &Store,
    data_dir: &std::path::Path,
    from: Option<&str>,
    chat_id: i64,
    text: &str,
    reply_to: Option<i64>,
) -> Result<i64, anyagents_core::Error> {
    let outbound = outbound_call(store, from, chat_id, text, reply_to)?;
    let token = token_of(
        &FileCredentialStore::new(data_dir),
        &outbound.credential_ref,
    )?;
    let response = call_api(&token, &outbound.call)?;
    sent_message_id(&response).map_err(|err| anyagents_core::Error::Message(err.to_string()))
}

fn channel_for(store: &Store, from: Option<&str>) -> Result<Channel, anyagents_core::Error> {
    let mut channels = Vec::new();
    for channel in store.list_channels()? {
        if channel.kind == "telegram" && channel.credential_ref.is_some() {
            channels.push(channel);
        }
    }
    if let Some(name) = from {
        let agent = store.agent_by_name(name)?;
        return channels
            .into_iter()
            .find(|channel| channel.agents.iter().any(|id| id == &agent.id))
            .ok_or_else(|| {
                anyagents_core::Error::Message(format!("no telegram channel for {name}"))
            });
    }
    let names = agent_names(store, &channels)?;
    match channels.len() {
        1 => Ok(channels.remove(0)),
        0 => Err(anyagents_core::Error::Message("no telegram channel".into())),
        _ => Err(anyagents_core::Error::Message(format!(
            "telegram sender is ambiguous: {names}"
        ))),
    }
}

/// `None` means the caller named nobody. MCP always passes `agent:<name>`.
pub(crate) fn bound_sender(sender: Option<&str>) -> Result<Option<String>, anyagents_core::Error> {
    match sender {
        None => Ok(None),
        Some(raw) => match anyagents_core::Address::parse(raw)? {
            anyagents_core::Address::Agent(name) => Ok(Some(name)),
            _ => Err(anyagents_core::Error::Message(
                "telegram sender must be an agent".into(),
            )),
        },
    }
}

fn agent_names(store: &Store, channels: &[Channel]) -> Result<String, anyagents_core::Error> {
    let mut names = Vec::new();
    for channel in channels {
        for id in &channel.agents {
            names.push(store.agent(id)?.name);
        }
    }
    names.sort();
    names.dedup();
    Ok(names.join(", "))
}

fn token_of(
    secrets: &FileCredentialStore,
    credential_ref: &str,
) -> Result<String, anyagents_core::Error> {
    let secret = secrets.read(credential_ref)?;
    secret
        .as_str()
        .map(ToString::to_string)
        .map_err(|_| anyagents_core::Error::Message("telegram token is not text".into()))
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

    #[test]
    fn outbound_uses_the_named_agents_bot_and_keeps_the_token_out() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let ada = store
            .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
            .unwrap();
        let bea = store
            .create_agent(anyagents_core::NewAgent::new("bea", "codex"))
            .unwrap();
        let ada_channel = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: Some("channels/ada".into()),
            })
            .unwrap();
        let bea_channel = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: Some("channels/bea".into()),
            })
            .unwrap();
        store.attach_channel(&ada_channel.id, &ada.id).unwrap();
        store.attach_channel(&bea_channel.id, &bea.id).unwrap();

        let sent = outbound_call(&store, Some("bea"), 100, "hello", Some(5)).unwrap();
        assert_eq!(sent.credential_ref, "channels/bea");
        assert_eq!(sent.call.method, "sendMessage");
        assert_eq!(sent.call.body["chat_id"], 100);
        assert_eq!(sent.call.body["text"], "hello");
        assert_eq!(sent.call.body["reply_to_message_id"], 5);
        assert!(!sent.call.body.to_string().contains("token"));

        let ambiguous = outbound_call(&store, None, 100, "hello", None).unwrap_err();
        let message = ambiguous.to_string();
        assert!(message.contains("ada"), "{message}");
        assert!(message.contains("bea"), "{message}");
        assert!(outbound_call(&store, Some("cy"), 100, "hello", None).is_err());
    }

    #[test]
    fn one_telegram_channel_sends_without_a_named_agent() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let ada = store
            .create_agent(anyagents_core::NewAgent::new("ada", "codex"))
            .unwrap();
        let channel = store
            .create_channel(anyagents_core::NewChannel {
                kind: "telegram".into(),
                identity: None,
                config: "{}".into(),
                credential_ref: Some("channels/ada".into()),
            })
            .unwrap();
        store.attach_channel(&channel.id, &ada.id).unwrap();
        let sent = outbound_call(&store, None, 100, "hello", None).unwrap();
        assert_eq!(sent.credential_ref, "channels/ada");
        assert_eq!(sent.call.method, "sendMessage");
    }

    #[test]
    fn no_telegram_channel_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let err = outbound_call(&store, None, 100, "hello", None).unwrap_err();
        assert!(err.to_string().contains("no telegram channel"), "{err}");
    }

    #[test]
    fn a_bound_sender_is_that_agent_and_anything_else_is_refused() {
        assert_eq!(
            bound_sender(Some("agent:ada")).unwrap().as_deref(),
            Some("ada")
        );
        assert_eq!(bound_sender(None).unwrap(), None);
        assert!(bound_sender(Some("group:crew")).is_err());
    }
}
