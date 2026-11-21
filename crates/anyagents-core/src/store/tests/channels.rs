use super::super::*;
use super::helpers::{agent, setup};

#[test]
fn a_chat_room_reaches_only_the_agents_whose_bot_is_on_that_chat() {
    let (_dir, store) = setup();
    let alice = agent(&store, "alice");
    let bob = agent(&store, "bob");
    let carol = agent(&store, "carol");
    // Two bots on chat A, one bot on chat B.
    for (name, chat) in [(&alice, -1001_i64), (&bob, -1001), (&carol, -2002)] {
        let channel = store
            .create_channel(NewChannel {
                kind: "telegram".to_string(),
                identity: None,
                config: "{}".to_string(),
                credential_ref: None,
            })
            .unwrap();
        store.attach_channel(&channel.id, &name.id).unwrap();
        store
            .set_channel_state(
                &channel.id,
                &format!(
                    "{{\"chats\":[{{\"agent\":\"{}\",\"chat_id\":{chat}}}]}}",
                    name.name
                ),
            )
            .unwrap();
    }

    let one = |text: &str, from: &str| Incoming {
        text: text.to_string(),
        sender: Some(from.to_string()),
        source_time_ms: None,
        source_key: None,
        reply_to: None,
        quote: None,
        origin: None,
        received_at_ms: None,
    };

    // A post in chat A reaches the other bot on chat A, and no one on B.
    let delivered = store
        .receive_room(&(-1001_i64).to_string(), one("in A", "agent:alice"))
        .unwrap();
    let names: Vec<&str> = delivered
        .iter()
        .map(|(agent, _)| agent.name.as_str())
        .collect();
    assert_eq!(names, vec!["bob"], "{names:?}");
    assert!(
        !store
            .inbox(&carol.id, None)
            .unwrap()
            .iter()
            .any(|m| m.text == "in A")
    );

    // A post in chat B reaches only its own bot.
    let delivered = store
        .receive_room("telegram:chat:-2002", one("in B", "agent:carol"))
        .unwrap();
    assert!(delivered.is_empty(), "{delivered:?}");

    // A room that is neither a chat nor a group reaches nobody. A name the
    // server does not know must never fan out to every agent.
    let delivered = store
        .receive_room("crew", one("everyone", "agent:alice"))
        .unwrap();
    assert!(delivered.is_empty(), "{delivered:?}");
}

#[test]
fn channel_state_compare_set_writes_only_over_its_own_snapshot() {
    let (_dir, store) = setup();
    let owner = agent(&store, "alpha");
    let channel = store
        .create_channel(NewChannel {
            kind: "telegram".into(),
            identity: Some("bot".into()),
            config: "{}".into(),
            credential_ref: Some("channels/one".into()),
        })
        .unwrap();
    store.attach_channel(&channel.id, &owner.id).unwrap();
    store
        .set_channel_state(&channel.id, r#"{"offset":1}"#)
        .unwrap();
    let snapshot = store.channel(&channel.id).unwrap().unwrap();
    let state_of = |store: &Store| store.channel(&channel.id).unwrap().unwrap().state;

    assert!(
        store
            .compare_set_channel_state(&snapshot, &owner, r#"{"offset":2}"#)
            .unwrap()
    );
    assert_eq!(state_of(&store), r#"{"offset":2}"#);

    // The state moved on since the snapshot: a stale write is refused.
    assert!(
        !store
            .compare_set_channel_state(&snapshot, &owner, r#"{"offset":9}"#)
            .unwrap()
    );
    assert_eq!(state_of(&store), r#"{"offset":2}"#);

    // The owner's lifecycle changed since it was read: refused.
    let snapshot = store.channel(&channel.id).unwrap().unwrap();
    store.set_agent_state(&owner.id, "paused").unwrap();
    assert!(
        !store
            .compare_set_channel_state(&snapshot, &owner, r#"{"offset":9}"#)
            .unwrap()
    );
    let owner = store.agent(&owner.id).unwrap();

    // The owner no longer holds the channel: refused.
    store.detach_channel_agents(&channel.id).unwrap();
    assert!(
        !store
            .compare_set_channel_state(&snapshot, &owner, r#"{"offset":9}"#)
            .unwrap()
    );
    assert_eq!(state_of(&store), r#"{"offset":2}"#);
    assert!(
        store
            .compare_set_channel_state(&snapshot, &owner, "not json")
            .is_err()
    );
}

#[test]
fn channel_attaches_to_an_agent() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    let channel = store
        .create_channel(NewChannel {
            kind: "telegram".into(),
            identity: Some("bot".into()),
            config: "{}".into(),
            credential_ref: Some("channels/one".into()),
        })
        .unwrap();
    store.attach_channel(&channel.id, &agent.id).unwrap();
    let listed = store.list_channels().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].agents, vec![agent.id.clone()]);
    assert_eq!(listed[0].credential_ref.as_deref(), Some("channels/one"));
    assert!(!format!("{listed:?}").contains("token"));
    let missing = ChannelId::generate();
    assert!(matches!(
        store.attach_channel(&missing, &agent.id).unwrap_err(),
        Error::ChannelNotFound(_)
    ));
}
