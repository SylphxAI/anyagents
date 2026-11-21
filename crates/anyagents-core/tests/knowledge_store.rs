use anyagents_core::knowledge::{Actor, Edit, Mode, Scope};
use anyagents_core::{Error, GroupId, NewAgent, Store};

const INSTRUCTIONS: &str = "org:instructions";
const DOMAIN: &str = "domain:example";

#[test]
fn knowledge_sqlite_contract() {
    let dir = tempfile::tempdir().unwrap();
    check_contract(&Store::open(dir.path().join("state.db")).unwrap());
}

#[test]
fn knowledge_postgres_contract() {
    let Ok(url) = std::env::var("ANYAGENTS_TEST_POSTGRES_URL") else {
        eprintln!("skipping knowledge PostgreSQL contract: ANYAGENTS_TEST_POSTGRES_URL unset");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    check_contract(&Store::open_postgres(&url, dir.path()).unwrap());
}

fn check_contract(store: &Store) {
    let tag = uuid::Uuid::new_v4().simple().to_string();
    let mut label = NewAgent::new(format!("k-label-{tag}"), "test");
    label.role = Some("rd-director".into());
    let reader_id = store.create_agent(label).unwrap().id;
    let editor_id = store
        .create_agent(NewAgent::new(format!("k-editor-{tag}"), "test"))
        .unwrap()
        .id;
    let owner = Actor::Owner("owner".into());
    let reader = Actor::Agent(reader_id.clone());
    let editor = Actor::Agent(editor_id.clone());
    let personal = format!("agent:{editor_id}");
    let phrase = "部署守則";
    let searchable = format!("{phrase} lex{tag}");
    let original = Edit {
        body: Some(format!("{searchable} <marker>& {}", "中".repeat(3500))),
        reason: "initial policy".into(),
        sim_problem: Some("stale policy".into()),
        ..Edit::default()
    };
    for scope in [INSTRUCTIONS, "org:memory", DOMAIN] {
        store
            .knowledge_write(&owner, scope, &tag, Some(0), &original, false)
            .unwrap();
        assert!(matches!(
            store.knowledge_read(&reader, scope, &tag),
            Err(Error::PermissionDenied(_))
        ));
    }
    assert!(matches!(
        store.knowledge_grant(&reader, &reader_id, "*", Mode::Read),
        Err(Error::PermissionDenied(_))
    ));
    assert!(store.knowledge_wake_context(&reader_id).unwrap().is_none());
    store
        .knowledge_write(&editor, &personal, &tag, Some(0), &original, false)
        .unwrap();
    assert!(store.knowledge_read(&editor, &personal, &tag).is_ok());
    assert!(
        store
            .knowledge_search(&reader, &Scope::All, &searchable, 20)
            .unwrap()
            .is_empty()
    );
    let group = GroupId::parse(&format!("readers-{tag}")).unwrap();
    store.join_group(group.as_str(), &reader_id).unwrap();
    let broad = store
        .knowledge_grant_group(&owner, &group, "org:*", Mode::Read)
        .unwrap();
    assert_eq!(broad.group_id.as_deref(), Some(group.as_str()));
    assert!(broad.agent_id.is_empty());
    let denied = store.knowledge_propose(&editor, DOMAIN, &tag, 1, &original);
    assert!(matches!(denied, Err(Error::PermissionDenied(_))));
    let domain = store
        .knowledge_grant_group(&owner, &group, DOMAIN, Mode::Read)
        .unwrap();
    assert!(
        store
            .knowledge_propose(&reader, DOMAIN, &tag, 1, &original)
            .is_ok()
    );
    assert!(matches!(
        store.knowledge_write(&reader, DOMAIN, &tag, Some(1), &original, false),
        Err(Error::PermissionDenied(_))
    ));
    store.knowledge_revoke(&owner, &domain.id).unwrap();
    let instructions = store.knowledge_wake_context(&reader_id).unwrap().unwrap();
    assert!(instructions.len() <= 4096 && instructions.ends_with("</org_instructions>"));
    assert!(instructions.contains("&lt;marker&gt;&amp;"));
    assert!(store.knowledge_wake_context(&editor_id).unwrap().is_none());
    assert!(matches!(
        store.knowledge_search(&reader, &Scope::parse(DOMAIN).unwrap(), phrase, 20),
        Err(Error::PermissionDenied(_))
    ));
    let broken = Edit {
        body: Some(format!("部署 署守 守則 lex{tag}")),
        ..original.clone()
    };
    store
        .knowledge_write(
            &owner,
            "org:memory",
            &format!("noise-{tag}"),
            None,
            &broken,
            false,
        )
        .unwrap();
    let hits = store
        .knowledge_search(&reader, &Scope::All, &searchable, 20)
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert!(
        hits.iter()
            .all(|h| h.topic == tag && h.version == 1 && h.namespace.starts_with("org:"))
    );
    store.leave_group(group.as_str(), &reader_id).unwrap();
    assert!(matches!(
        store.knowledge_read(&reader, INSTRUCTIONS, &tag),
        Err(Error::PermissionDenied(_))
    ));
    assert!(
        store
            .knowledge_search(&reader, &Scope::All, &searchable, 20)
            .unwrap()
            .is_empty()
    );
    store.join_group(group.as_str(), &reader_id).unwrap();
    let wildcard = store
        .knowledge_grant(&owner, &reader_id, "*", Mode::Read)
        .unwrap();
    assert!(matches!(
        store.knowledge_read(&reader, &personal, &tag),
        Err(Error::PermissionDenied(_))
    ));
    assert!(
        store
            .knowledge_search(&reader, &Scope::All, &searchable, 20)
            .unwrap()
            .iter()
            .all(|h| h.namespace != personal)
    );
    store.knowledge_revoke(&owner, &wildcard.id).unwrap();

    let update = Edit {
        body: Some("updated instructions".into()),
        reason: "feedback".into(),
        sim_observed: Some("observed new behavior".into()),
        ..Edit::default()
    };
    assert!(matches!(
        store.knowledge_write(&reader, INSTRUCTIONS, &tag, Some(1), &update, false),
        Err(Error::PermissionDenied(_))
    ));
    store
        .knowledge_grant(&owner, &reader_id, "org:instructions", Mode::Propose)
        .unwrap();
    store
        .knowledge_grant(&owner, &editor_id, "org:instructions", Mode::Write)
        .unwrap();
    let proposal = store
        .knowledge_propose(&reader, INSTRUCTIONS, &tag, 1, &update)
        .unwrap();
    assert!(matches!(
        store.knowledge_review(&reader, INSTRUCTIONS, &proposal.id, Some(1), true),
        Err(Error::PermissionDenied(_))
    ));
    let (reviewed, new_doc) = store
        .knowledge_review(&editor, INSTRUCTIONS, &proposal.id, Some(1), true)
        .unwrap();
    assert_eq!(reviewed.status, "promoted");
    assert_eq!(new_doc.unwrap().version, 2);
    assert!(matches!(
        store.knowledge_write(&owner, INSTRUCTIONS, &tag, Some(1), &original, false),
        Err(Error::KnowledgeConflict { current: 2 })
    ));
    assert!(
        store
            .knowledge_wake_context(&reader_id)
            .unwrap()
            .unwrap()
            .contains("updated instructions")
    );
    let history = store
        .knowledge_revisions(&reader, INSTRUCTIONS, &tag, None, 8)
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].sim_observed, update.sim_observed);
    assert_eq!(history[0].prior_body, original.body);
    assert_eq!(history[1].sim_problem, original.sim_problem);
    let restored = store
        .knowledge_rollback(&owner, INSTRUCTIONS, &tag, 1, None, "restore baseline")
        .unwrap();
    assert_eq!(restored.version, 3);
    assert_eq!(Some(restored.body), original.body);
    let exact = Scope::Namespace(INSTRUCTIONS.into());
    assert_eq!(
        store
            .knowledge_search(&reader, &exact, &searchable, 20)
            .unwrap()[0]
            .version,
        3
    );
    assert!(store.knowledge_revoke(&owner, &broad.id).unwrap());
    assert!(store.knowledge_wake_context(&reader_id).unwrap().is_none());
    assert!(matches!(
        store.knowledge_search(&reader, &exact, &searchable, 20),
        Err(Error::PermissionDenied(_))
    ));
    assert!(
        store
            .knowledge_list(&reader, &Scope::Org, 20, 0, "")
            .unwrap()
            .is_empty()
    );
    let deletion = Edit {
        reason: "retire".into(),
        ..Edit::default()
    };
    assert_eq!(
        store
            .knowledge_write(&owner, INSTRUCTIONS, &tag, Some(3), &deletion, true)
            .unwrap()
            .version,
        4
    );
    assert!(
        store
            .knowledge_search(&owner, &exact, &searchable, 20)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn sqlite_knowledge_replays_committed_revisions_after_index_contention_and_loss() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state.db");
    let store = Store::open(&state).unwrap();
    let owner = Actor::Owner("fixture-owner".into());
    let scope = Scope::Namespace(INSTRUCTIONS.into());
    let index = store.memory_index_path().to_path_buf();
    let write = |store: &Store, topic: &str, version, body: &str, delete| {
        store
            .knowledge_write(
                &owner,
                INSTRUCTIONS,
                topic,
                Some(version),
                &Edit {
                    body: Some(body.into()),
                    reason: "fixture".into(),
                    ..Edit::default()
                },
                delete,
            )
            .unwrap()
    };
    let search =
        |store: &Store, phrase| store.knowledge_search(&owner, &scope, phrase, 10).unwrap();
    write(&store, "replace", 0, "oldsentinel", false);
    write(&store, "retire", 0, "deletedsentinel", false);
    assert_eq!(search(&store, "oldsentinel")[0].version, 1);
    assert_eq!(search(&store, "deletedsentinel")[0].version, 1);

    let blocker = rusqlite::Connection::open(&index).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    // Use a deadline and always release the external lock before joining, so
    // the test reports the regression rather than deadlocking when it fails.
    std::thread::scope(|thread| {
        let (done, result) = std::sync::mpsc::channel();
        let store = &store;
        let writer = thread.spawn(move || {
            write(store, "replace", 1, "newsentinel", false);
            write(store, "retire", 1, "deletedsentinel", true);
            done.send(()).unwrap();
        });
        let while_locked = result.recv_timeout(std::time::Duration::from_secs(3));
        blocker.execute_batch("ROLLBACK").unwrap();
        writer.join().unwrap();
        while_locked.expect("knowledge mutations must not wait for the index lock");
    });
    drop(blocker);
    assert!(search(&store, "oldsentinel").is_empty());
    assert!(search(&store, "deletedsentinel").is_empty());
    assert_eq!(search(&store, "newsentinel")[0].version, 2);

    // Rebuild across more than one batch, including repeated changes to one
    // topic and a tombstone that must not become a searchable document.
    for version in 2..132 {
        write(&store, "replace", version, "newsentinel", false);
    }
    assert_eq!(search(&store, "newsentinel")[0].version, 132);
    drop(store);
    std::fs::remove_file(&index).unwrap();
    let reopened = Store::open(&state).unwrap();
    assert_eq!(search(&reopened, "newsentinel")[0].version, 132);
    assert!(search(&reopened, "oldsentinel").is_empty());
    assert!(search(&reopened, "deletedsentinel").is_empty());
}
