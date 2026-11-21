use super::super::*;
use super::helpers::{agent, setup};

/// A named group has its own membership: a line reaches its members, a
/// non-member receives nothing, and an unknown name reaches nobody.
#[test]
fn a_named_group_reaches_only_its_own_members() {
    let (_dir, store) = setup();
    let alice = agent(&store, "alice");
    let bob = agent(&store, "bob");
    let carol = agent(&store, "carol");
    store.join_group("crew", &alice.id).unwrap();
    store.join_group("crew", &bob.id).unwrap();

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

    // A human line reaches every member.
    let delivered = store
        .receive_room("group:crew", one("hi all", "telegram:user:1"))
        .unwrap();
    let names: Vec<&str> = delivered
        .iter()
        .map(|(agent, _)| agent.name.as_str())
        .collect();
    assert_eq!(names, vec!["alice", "bob"], "{names:?}");
    assert!(
        !store
            .inbox(&carol.id, None)
            .unwrap()
            .iter()
            .any(|m| m.text == "hi all")
    );

    // An agent line reaches the other member, never the author.
    let delivered = store
        .receive_room("crew", one("from alice", "agent:alice"))
        .unwrap();
    let names: Vec<&str> = delivered
        .iter()
        .map(|(agent, _)| agent.name.as_str())
        .collect();
    assert_eq!(names, vec!["bob"], "{names:?}");

    // An unknown group reaches nobody rather than everybody.
    let delivered = store
        .receive_room("group:nobody", one("lost", "agent:alice"))
        .unwrap();
    assert!(delivered.is_empty(), "{delivered:?}");
}

/// A named group survives a restart: membership is stored, not held in
/// memory, and a message already stored is still readable afterwards.
#[test]
fn a_group_message_survives_a_restart() {
    let (dir, store) = setup();
    let alice = agent(&store, "alice");
    let bob = agent(&store, "bob");
    store.join_group("crew", &alice.id).unwrap();
    store.join_group("crew", &bob.id).unwrap();
    store
        .receive_room(
            "group:crew",
            Incoming {
                text: "kept".to_string(),
                sender: Some("agent:alice".to_string()),
                source_time_ms: None,
                source_key: None,
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: None,
            },
        )
        .unwrap();
    drop(store);

    let reopened = Store::open(dir.path().join("state.db")).unwrap();
    assert_eq!(
        reopened.group_members("crew").unwrap(),
        vec!["alice", "bob"]
    );
    let bob_id = reopened.agent_by_name("bob").unwrap().id;
    assert!(
        reopened
            .inbox(&bob_id, None)
            .unwrap()
            .iter()
            .any(|message| message.text == "kept")
    );
}
