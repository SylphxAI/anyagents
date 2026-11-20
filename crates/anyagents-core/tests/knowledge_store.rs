use anyagents_core::knowledge::{Actor, Edit, Mode, Scope};
use anyagents_core::{Error, NewAgent, Store};

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
    let broad = store
        .knowledge_grant(&owner, &reader_id, "org:*", Mode::Read)
        .unwrap();
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
