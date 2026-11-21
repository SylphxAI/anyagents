use agent_ingress::{
    BlockMatch, BlockScope, ChatType, IngressBlockRule, IngressBlockStore, IngressDecision,
    SenderIdentity, SenderRelationship, StoreError, ValidatedBlockRule,
};
use anyagents_core::db::TEST_POSTGRES_URL_ENV;
use anyagents_core::{AgentId, IngressBlocks, NewAgent, Store};

const CHANNEL: &str = "telegram:account-a";
const OTHER_ACCOUNT: &str = "telegram:account-b";
const TARGET: &str = "42";
const NOW: i64 = 1_700_000_000_000;

#[test]
fn ingress_blocks_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    runtime().block_on(check_store(|| Store::open(&path).unwrap()));
}

#[test]
fn ingress_blocks_postgres() {
    let Some(url) = std::env::var(TEST_POSTGRES_URL_ENV)
        .or_else(|_| std::env::var("TEST_DATABASE_URL"))
        .ok()
        .filter(|url| !url.trim().is_empty())
    else {
        eprintln!("skipping ingress PostgreSQL contract: {TEST_POSTGRES_URL_ENV} not set");
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    runtime().block_on(check_store(|| {
        Store::open_postgres(url.trim(), dir.path()).unwrap()
    }));
}

#[test]
fn ingress_blocks_propagates_read_failure_without_querying_for_protected_senders() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.db");
    let store = Store::open(&path).unwrap();
    let agent = create_agent(&store);
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("DROP TABLE ingress_blocks")
        .unwrap();

    runtime().block_on(async {
        let rules = IngressBlocks::new(&store, &agent);
        let mut sender = sender();
        assert!(matches!(
            rules.is_blocked(&sender, NOW).await,
            Err(StoreError::Backend(_))
        ));
        for relationship in [
            SenderRelationship::Owner,
            SenderRelationship::Admin,
            SenderRelationship::Parent,
            SenderRelationship::Peer,
        ] {
            sender.relationship = relationship;
            assert_eq!(
                rules.is_blocked(&sender, NOW).await.unwrap(),
                IngressDecision::Allow
            );
            let would_block = rule("refused", CHANNEL, sender_scope(TARGET));
            let target = would_block.target.clone();
            assert!(ValidatedBlockRule::new(would_block, &target, relationship).is_err());
        }
    });
}

fn runtime() -> tokio::runtime::Runtime {
    // The synchronous Store uses its own runtime for PostgreSQL I/O.
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

async fn check_store(open: impl Fn() -> Store) {
    let store = open();
    let agent = create_agent(&store);
    let other_agent = create_agent(&store);
    let sender = sender();
    let old = rule("old", CHANNEL, sender_scope(TARGET));
    let chat = rule(
        "chat",
        CHANNEL,
        BlockScope::Chat {
            chat_id: TARGET.into(),
        },
    );
    let other_channel = rule("other-account", OTHER_ACCOUNT, sender_scope(TARGET));
    let unrelated = rule("unrelated", CHANNEL, sender_scope("unrelated"));
    let isolated = rule("second-agent", CHANNEL, sender_scope(TARGET));
    let replacement = IngressBlockRule {
        rule_id: "replacement".into(),
        reason: Some("manual review".into()),
        blocked_by: "verified operator".into(),
        created_at_ms: NOW + 1,
        expires_at_ms: Some(NOW + 10),
        ..old.clone()
    };
    {
        let rules = IngressBlocks::new(&store, &agent);
        assert_eq!(
            rules.is_blocked(&sender, NOW).await.unwrap(),
            IngressDecision::Allow
        );
        for rule in [&old, &chat, &other_channel, &unrelated, &replacement] {
            rules.block(validated(rule)).await.unwrap();
        }
        let other_rules = IngressBlocks::new(&store, &other_agent);
        other_rules.block(validated(&isolated)).await.unwrap();
    }
    drop(store);

    let reopened = open();
    let rules = IngressBlocks::new(&reopened, &agent);
    let other_rules = IngressBlocks::new(&reopened, &other_agent);
    assert_eq!(
        other_rules.list_blocks().await.unwrap(),
        std::slice::from_ref(&isolated)
    );
    let mut listed = rules.list_blocks().await.unwrap();
    listed.sort_by(|left, right| left.rule_id.cmp(&right.rule_id));
    assert_eq!(
        listed,
        [
            chat.clone(),
            other_channel.clone(),
            replacement.clone(),
            unrelated
        ]
    );
    assert_eq!(
        other_rules.matching_rules(&sender).await.unwrap(),
        [isolated]
    );

    let mut candidates = rules.matching_rules(&sender).await.unwrap();
    candidates.sort_by(|left, right| left.rule_id.cmp(&right.rule_id));
    assert_eq!(candidates, [chat.clone(), replacement.clone()]);
    let no_chat = SenderIdentity {
        chat_id: None,
        ..sender.clone()
    };
    assert_eq!(
        rules.matching_rules(&no_chat).await.unwrap(),
        std::slice::from_ref(&replacement)
    );
    let other_account = SenderIdentity {
        channel: OTHER_ACCOUNT.into(),
        ..sender.clone()
    };
    assert_eq!(
        rules.matching_rules(&other_account).await.unwrap(),
        [other_channel]
    );

    assert_eq!(
        rules.is_blocked(&sender, NOW + 9).await.unwrap(),
        drop_for(&replacement)
    );
    assert_eq!(
        rules.is_blocked(&sender, NOW + 10).await.unwrap(),
        drop_for(&chat)
    );
    assert_eq!(
        rules.is_blocked(&no_chat, NOW + 10).await.unwrap(),
        IngressDecision::Allow
    );
    assert_eq!(
        rules.matching_rules(&no_chat).await.unwrap(),
        std::slice::from_ref(&replacement)
    );
    let protected = SenderIdentity {
        relationship: SenderRelationship::Owner,
        ..sender.clone()
    };
    assert_eq!(
        rules.is_blocked(&protected, NOW + 9).await.unwrap(),
        IngressDecision::Allow
    );

    assert!(rules.unblock(&replacement.target).await.unwrap());
    assert!(!rules.unblock(&replacement.target).await.unwrap());
    assert_eq!(rules.matching_rules(&sender).await.unwrap(), [chat]);
    assert_eq!(other_rules.matching_rules(&sender).await.unwrap().len(), 1);

    reopened.delete_agent(&agent).unwrap();
    assert!(rules.list_blocks().await.unwrap().is_empty());
    assert_eq!(other_rules.list_blocks().await.unwrap().len(), 1);
    assert!(matches!(
        rules.block(validated(&replacement)).await,
        Err(StoreError::Backend(_))
    ));
    reopened.delete_agent(&other_agent).unwrap();
}

fn create_agent(store: &Store) -> AgentId {
    let name = format!("ingress-{}", uuid::Uuid::new_v4().simple());
    store.create_agent(NewAgent::new(name, "test")).unwrap().id
}

fn sender() -> SenderIdentity {
    SenderIdentity {
        channel: CHANNEL.into(),
        sender_id: TARGET.into(),
        chat_id: Some(TARGET.into()),
        chat_type: ChatType::Group,
        username: None,
        display_name: None,
        relationship: SenderRelationship::Stranger,
    }
}

fn sender_scope(id: &str) -> BlockScope {
    BlockScope::Sender {
        sender_id: id.into(),
    }
}

fn rule(id: &str, channel: &str, scope: BlockScope) -> IngressBlockRule {
    IngressBlockRule {
        rule_id: id.into(),
        target: BlockMatch {
            channel: channel.into(),
            scope,
        },
        reason: None,
        blocked_by: "fixture actor".into(),
        created_at_ms: NOW,
        expires_at_ms: None,
    }
}

fn validated(rule: &IngressBlockRule) -> ValidatedBlockRule {
    ValidatedBlockRule::new(rule.clone(), &rule.target, SenderRelationship::Stranger).unwrap()
}

fn drop_for(rule: &IngressBlockRule) -> IngressDecision {
    IngressDecision::Drop {
        rule_id: rule.rule_id.clone(),
        reason: rule.reason.clone(),
    }
}
