use super::super::*;
use super::helpers::{agent, setup};

#[test]
fn relationship_discovery_includes_children_and_siblings_without_roles() {
    let (dir, store) = setup();
    let parent = agent(&store, "parent");
    let mut new = NewAgent::new("first", "codex");
    new.parent_id = Some(parent.id.clone());
    let first = store.create_agent(new).unwrap();
    let mut new = NewAgent::new("second", "codex");
    new.parent_id = Some(parent.id.clone());
    let second = store.create_agent(new).unwrap();
    store.set_agent_scope(&first.id, "siblings").unwrap();
    store.set_agent_scope(&second.id, "siblings").unwrap();
    let roster = read_children_roster(dir.path(), parent.id.as_str());
    assert_eq!(
        roster.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
        ["first", "second"]
    );
    let ReceiveOutcome::New(parent_mail) =
        store.receive(&parent.id, Incoming::new("check")).unwrap()
    else {
        panic!("new mail expected")
    };
    let topology = parent_mail.context.topology.as_deref().unwrap();
    assert!(topology.contains("agent:first"));
    assert!(topology.contains("agent:second"));
    let ReceiveOutcome::New(child_mail) = store.receive(&first.id, Incoming::new("check")).unwrap()
    else {
        panic!("new mail expected")
    };
    assert!(
        child_mail
            .context
            .topology
            .as_deref()
            .unwrap()
            .contains("agent:second")
    );
    // A private sibling remains hidden even though it has a label.
    store
        .set_agent_role(&second.id, Some("Researcher"))
        .unwrap();
    store.set_agent_scope(&second.id, "private").unwrap();
    let ReceiveOutcome::New(changed) = store
        .receive(&first.id, Incoming::new("check again"))
        .unwrap()
    else {
        panic!("new mail expected")
    };
    assert!(
        changed
            .context
            .topology
            .as_deref()
            .unwrap()
            .contains("-sibling: agent:second")
    );
}

#[test]
fn event_driven_topology_initial_delta_and_isolated_scope() {
    let (_dir, store) = setup();
    let root_worker = store
        .create_agent(NewAgent {
            name: "root_worker".to_string(),
            role: Some("公司業務總管".to_string()),
            description: None,
            harness: "agy".to_string(),
            runtime: None,
            remote_budget_micros: None,
            model: None,
            workdir: None,
            instructions: None,
            parent_id: None,
        })
        .unwrap();
    let helper = store
        .create_agent(NewAgent {
            name: "helper".to_string(),
            role: Some("產品與功能".to_string()),
            description: None,
            harness: "agy".to_string(),
            runtime: None,
            remote_budget_micros: None,
            model: None,
            workdir: None,
            instructions: None,
            parent_id: Some(root_worker.id.clone()),
        })
        .unwrap();
    let renamed_worker = store
        .create_agent(NewAgent {
            name: "renamed_worker".to_string(),
            role: Some("商業化與營收".to_string()),
            description: None,
            harness: "agy".to_string(),
            runtime: None,
            remote_budget_micros: None,
            model: None,
            workdir: None,
            instructions: None,
            parent_id: Some(root_worker.id.clone()),
        })
        .unwrap();
    assert_eq!(root_worker.scope(), "public");
    store.set_agent_scope(&helper.id, "siblings").unwrap();
    store
        .set_agent_scope(&renamed_worker.id, "siblings")
        .unwrap();

    // 1. First message to helper from root_worker -> emits `<topology event="initial" ... />`
    let mut m1 = Incoming::new("Start sprint");
    m1.sender = Some("agent:root_worker".to_string());
    let ReceiveOutcome::New(msg1) = store.receive(&helper.id, m1).unwrap() else {
        panic!("expected new message");
    };
    assert_eq!(msg1.context.sender_rel.as_deref(), Some("parent"));
    assert_eq!(msg1.context.sender_role.as_deref(), Some("公司業務總管"));
    let topo1 = msg1.context.topology.as_deref().expect("initial topology");
    assert!(topo1.contains("event=\"initial\""), "topo1={topo1}");
    assert!(topo1.contains("scope=\"siblings\""), "topo1={topo1}");
    assert!(
        topo1.contains("parent=\"agent:root_worker [公司業務總管]\""),
        "topo1={topo1}"
    );
    assert!(
        topo1.contains("agent:renamed_worker [商業化與營收]"),
        "topo1={topo1}"
    );

    // 2. Second message with no topology change -> `topology` is None
    let mut m2 = Incoming::new("Status check");
    m2.sender = Some("agent:renamed_worker".to_string());
    let ReceiveOutcome::New(msg2) = store.receive(&helper.id, m2).unwrap() else {
        panic!("expected new message");
    };
    assert_eq!(msg2.context.sender_rel.as_deref(), Some("sibling"));
    assert!(
        msg2.context.topology.is_none(),
        "no delta expected when unchanged"
    );

    // 3. New standing sibling `reader_worker` added -> next message to helper emits `<topology event="delta" ... />`
    let reader_worker = store
        .create_agent(NewAgent {
            name: "reader_worker".to_string(),
            role: Some("用戶體驗與設計".to_string()),
            description: None,
            harness: "agy".to_string(),
            runtime: None,
            remote_budget_micros: None,
            model: None,
            workdir: None,
            instructions: None,
            parent_id: Some(root_worker.id.clone()),
        })
        .unwrap();
    store
        .set_agent_scope(&reader_worker.id, "siblings")
        .unwrap();
    let mut m3 = Incoming::new("Design sync");
    m3.sender = Some("agent:root_worker".to_string());
    let ReceiveOutcome::New(msg3) = store.receive(&helper.id, m3).unwrap() else {
        panic!("expected new message");
    };
    let topo3 = msg3
        .context
        .topology
        .as_deref()
        .expect("delta topology on add");
    assert!(topo3.contains("event=\"delta\""), "topo3={topo3}");
    assert!(
        topo3.contains("+sibling: agent:reader_worker [用戶體驗與設計]"),
        "topo3={topo3}"
    );

    // 4. Archive `reader_worker` -> next message to helper emits `-sibling: reader_worker`
    store
        .set_agent_state(&reader_worker.id, "archived")
        .unwrap();
    let mut m4 = Incoming::new("After archive");
    m4.sender = Some("agent:root_worker".to_string());
    let ReceiveOutcome::New(msg4) = store.receive(&helper.id, m4).unwrap() else {
        panic!("expected new message");
    };
    let topo4 = msg4
        .context
        .topology
        .as_deref()
        .expect("delta topology on archive");
    assert!(
        topo4.contains("-sibling: agent:reader_worker [用戶體驗與設計]"),
        "topo4={topo4}"
    );

    // 5. Explicitly isolated agents see no siblings regardless of name or role
    let stanley = store
        .create_agent(NewAgent {
            name: "stanley-assistant".to_string(),
            role: Some("Stanley 專屬助手".to_string()),
            description: None,
            harness: "agy".to_string(),
            runtime: None,
            remote_budget_micros: None,
            model: None,
            workdir: None,
            instructions: None,
            parent_id: Some(root_worker.id.clone()),
        })
        .unwrap();
    store.set_agent_scope(&stanley.id, "isolated").unwrap();
    let mut m5 = Incoming::new("Hello Stanley");
    m5.sender = Some("agent:root_worker".to_string());
    let ReceiveOutcome::New(msg5) = store.receive(&stanley.id, m5).unwrap() else {
        panic!("expected new message");
    };
    let topo5 = msg5
        .context
        .topology
        .as_deref()
        .expect("initial topology for isolated");
    assert!(topo5.contains("scope=\"isolated\""), "topo5={topo5}");
    assert!(!topo5.contains("siblings="), "topo5={topo5}");
}
