use super::*;
use crate::{Agent, Incoming, NewAgent, ReceiveOutcome};
use serde_json::json;

struct Fixture {
    dir: tempfile::TempDir,
    store: Store,
    root: Agent,
    target: Agent,
    peer: Agent,
}
impl Fixture {
    fn new() -> Self {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let root = store.create_agent(NewAgent::new("root", "sylphx")).unwrap();
        let mut child = NewAgent::new("child", "sylphx");
        child.parent_id = Some(root.id.clone());
        let target = store.create_agent(child).unwrap();
        let peer = store.create_agent(NewAgent::new("peer", "sylphx")).unwrap();
        Self {
            dir,
            store,
            root,
            target,
            peer,
        }
    }
    fn message(&self, text: &str) -> crate::Message {
        let mut incoming = Incoming::new(text);
        incoming.sender = Some("external:user".into());
        incoming.reply_to = Some("group:fixture".into());
        incoming.source_time_ms = Some(123);
        incoming.received_at_ms = Some(456);
        match self.store.receive(&self.target.id, incoming).unwrap() {
            ReceiveOutcome::New(m) => m,
            _ => unreachable!(),
        }
    }
    fn redact(&self, request: &Request) -> Result<Receipt, Error> {
        self.store
            .redact_conversation(Actor::Agent(self.root.id.clone()), &self.target.id, request)
    }
    fn path(&self, rel: &str) -> PathBuf {
        let path = agent_config_dir(self.dir.path(), &self.target.id).join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        path
    }
}
fn request(id: &str, span: Option<Span>) -> Request {
    Request {
        message_id: Some(id.into()),
        seq: None,
        span,
        reason: "accidental synthetic capture".into(),
    }
}
fn chars(start: usize, end: usize) -> Span {
    Span {
        unit: SpanUnit::Chars,
        start,
        end,
    }
}

#[test]
fn redaction_preserves_mailbox_order_metadata_other_rows_and_removes_search_term() {
    let f = Fixture::new();
    let before = f.message("untouched before");
    let target = f.message("prefix fixturecanary後 suffix");
    let after = f.message("untouched after");
    f.store.advance_delivered(&f.target.id, 1).unwrap();
    let cursor = f.store.delivered_cursor(&f.target.id).unwrap();
    assert_eq!(
        f.store
            .search_conversation(Some("child"), None, None, None, "fixturecanary")
            .unwrap()
            .len(),
        1
    );
    let req = request(target.id.as_str(), Some(chars(7, 21)));
    let receipt = f.redact(&req).unwrap();
    assert!(receipt.full_scrub, "{receipt:?}");
    assert_eq!(receipt.permission, "conversation.redact");
    assert_eq!(receipt.removed_bytes, 16);
    assert_eq!(receipt.removed_chars, 14);
    let messages = f.store.inbox(&f.target.id, None).unwrap();
    assert_eq!(messages[0].text, before.text);
    assert_eq!(messages[2], after);
    let mut expected = target.clone();
    expected.text = "prefix [redacted] suffix".into();
    assert_eq!(messages[1], expected);
    assert_eq!(
        messages.iter().map(|m| m.seq).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(f.store.delivered_cursor(&f.target.id).unwrap(), cursor);
    assert!(
        f.store
            .search_conversation(Some("child"), None, None, None, "fixturecanary")
            .unwrap()
            .is_empty()
    );
    let conn = f.store.conn();
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM rag_chunks_fts WHERE rag_chunks_fts MATCH 'fixturecanary'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    let stored: (String, String) = conn
        .query_row(
            "SELECT text, content_hash FROM rag_chunks WHERE chunk_id = ?1",
            [target.id.as_str()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        stored,
        (expected.text.clone(), rag_content_hash(&expected.text))
    );
    drop(conn);
    let audit = serde_json::to_string(&receipt).unwrap();
    assert!(!audit.contains("fixturecanary"));
    assert!(!audit.contains(&rag_content_hash(&target.text)));
    // Check physical SQLite pages/WAL too, not just logical SELECT or MATCH.
    for suffix in ["state.db", "state.db-wal"] {
        let bytes = std::fs::read(f.dir.path().join(suffix)).unwrap_or_default();
        assert!(!bytes.windows(13).any(|w| w == b"fixturecanary"));
        assert!(
            !bytes
                .windows(32)
                .any(|w| w == rag_content_hash(&target.text).as_bytes())
        );
    }
}

#[test]
fn exact_provider_retry_and_redaction_replay_cannot_resurrect_plaintext() {
    let f = Fixture::new();
    let original = "a long synthetic exactredeliverycanary captured in a provider message";
    let mut incoming = Incoming::new(original);
    incoming.source_key = Some("first-source".into());
    incoming.origin = Some(crate::Origin {
        chat_id: Some(123),
        message_id: Some(2),
        thread_id: None,
    });
    let ReceiveOutcome::New(mut expected) =
        f.store.receive(&f.target.id, incoming.clone()).unwrap()
    else {
        panic!("initial message must be new");
    };
    assert_eq!(
        f.store
            .search_conversation(Some("child"), None, None, None, "exactredeliverycanary")
            .unwrap()
            .len(),
        1
    );
    let req = request(expected.id.as_str(), None);
    let receipt = f.redact(&req).unwrap();
    assert!(receipt.full_scrub);
    expected.text = TOMBSTONE.into();
    incoming.source_key = Some("second-source".into());
    let ReceiveOutcome::Duplicate(duplicate) = f.store.receive(&f.target.id, incoming).unwrap()
    else {
        panic!("same-origin retry must be duplicate");
    };
    assert_eq!(duplicate, expected);
    assert_eq!(f.redact(&req).unwrap(), receipt);
    assert_eq!(f.store.inbox(&f.target.id, None).unwrap(), vec![expected]);
    assert!(
        f.store
            .search_conversation(Some("child"), None, None, None, "exactredeliverycanary")
            .unwrap()
            .is_empty()
    );
    let (count, audit): (i64, String) = f
        .store
        .conn()
        .query_row(
            "SELECT count(*), receipt FROM conversation_redactions",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(serde_json::from_str::<Receipt>(&audit).unwrap(), receipt);
}

// Public-API alias.rs counterexample: provider promotion must not
// discard the relay's already-coalesced origin or source-key identity.
#[test]
fn promoted_basic_group_alias_retry_cannot_resurrect_redacted_message() {
    let f = Fixture::new();
    let canary = "synthetic-basic-group-alias-private-capture-longer-than-tombstone";
    let make = |key: &str, id: i64| {
        let mut input = Incoming::new(canary);
        input.sender = Some("telegram:user:1".into());
        input.source_key = Some(key.into());
        input.source_time_ms = Some(123);
        input.origin = Some(crate::Origin {
            chat_id: Some(-1234),
            message_id: Some(id),
            thread_id: None,
        });
        input
    };
    let ReceiveOutcome::New(message) = f
        .store
        .receive(&f.target.id, make("tg-in:initial", 42))
        .unwrap()
    else {
        panic!("initial relay must be new");
    };
    let ReceiveOutcome::Duplicate(mut promoted) = f
        .store
        .receive(&f.target.id, make("webhook:direct", 43))
        .unwrap()
    else {
        panic!("direct webhook must coalesce with the relay");
    };
    assert_eq!(
        (promoted.id.clone(), promoted.seq),
        (message.id, message.seq)
    );
    assert_eq!(promoted.origin.unwrap().message_id, Some(43));
    assert_eq!(promoted.source_key, None);
    let req = request(promoted.id.as_str(), None);
    let receipt = f.redact(&req).unwrap();
    assert!(receipt.full_scrub);
    promoted.text = TOMBSTONE.into();
    assert_eq!(
        f.store.inbox(&f.target.id, None).unwrap(),
        vec![promoted.clone()]
    );
    assert!(
        f.store
            .search_conversation(Some("child"), None, None, None, canary)
            .unwrap()
            .is_empty()
    );
    let retry = f
        .store
        .receive(&f.target.id, make("tg-in:initial", 42))
        .unwrap();
    assert_eq!(retry, ReceiveOutcome::Duplicate(promoted.clone()));
    assert_eq!(f.store.inbox(&f.target.id, None).unwrap(), vec![promoted]);
    assert!(
        f.store
            .search_conversation(Some("child"), None, None, None, canary)
            .unwrap()
            .is_empty()
    );
    assert_eq!(f.redact(&req).unwrap(), receipt);
    let (count, audit): (i64, String) = f
        .store
        .conn()
        .query_row(
            "SELECT count(*), receipt FROM conversation_redactions",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(serde_json::from_str::<Receipt>(&audit).unwrap(), receipt);
    // The relay baseline is a second body copy under the OLD origin, not the
    // promoted origin. It must be scrubbed too, not merely fenced at ingress.
    let baseline: String = f.store.conn().query_row(
        "SELECT body FROM message_versions WHERE agent_id = ?1 AND chat_id = -1234 AND message_id = 42",
        [f.target.id.as_str()], |r| r.get(0)).unwrap();
    assert_eq!(baseline, TOMBSTONE);
    for suffix in ["state.db", "state.db-wal"] {
        let bytes = std::fs::read(f.dir.path().join(suffix)).unwrap_or_default();
        assert!(!bytes.windows(canary.len()).any(|w| w == canary.as_bytes()));
    }
}

// Vesta's local_id_collision.rs: bot-local numbers are not global identities.
#[test]
fn local_id_collision_preserves_independent_bot_message_after_redaction() {
    for redact in [false, true] {
        let f = Fixture::new();
        let mut first = Incoming::new("selected private message from bot A's namespace");
        first.sender = Some("telegram:user:1".into());
        first.source_key = Some("webhook:bot-a:initial".into());
        first.source_time_ms = Some(123);
        first.origin = Some(crate::Origin {
            chat_id: Some(-1234),
            message_id: Some(43),
            thread_id: None,
        });
        let ReceiveOutcome::New(original) = f.store.receive(&f.target.id, first).unwrap() else {
            panic!("initial message must be new")
        };
        if redact {
            assert!(
                f.redact(&request(original.id.as_str(), None))
                    .unwrap()
                    .full_scrub
            );
        }
        for (key, sender, time, body) in [
            (
                "tg-in:-1234:bot-b:43",
                "telegram:user:2",
                60_123,
                "independent relay message",
            ),
            (
                "webhook:bot-c:later",
                "telegram:user:3",
                120_123,
                "independent direct message",
            ),
        ] {
            let mut other = Incoming::new(body);
            other.sender = Some(sender.into());
            other.source_key = Some(key.into());
            other.source_time_ms = Some(time);
            other.origin = Some(crate::Origin {
                chat_id: Some(-1234),
                message_id: Some(43),
                thread_id: None,
            });
            let ReceiveOutcome::New(stored) = f.store.receive(&f.target.id, other).unwrap() else {
                panic!("independent bot-local number must not coalesce")
            };
            assert_eq!(stored.text, body);
            assert_eq!(
                f.store
                    .receive(&f.target.id, {
                        let mut retry = Incoming::new(body);
                        retry.source_key = Some(key.into());
                        retry
                    })
                    .unwrap(),
                ReceiveOutcome::Duplicate(stored.clone())
            );
            let owner: String = f.store.conn().query_row("SELECT identity_id FROM message_source_aliases WHERE agent_id=?1 AND source_key=?2", params![f.target.id.as_str(), key], |r| r.get(0)).unwrap();
            assert_eq!(owner, stored.id.as_str());
        }
        let mailbox = f.store.inbox(&f.target.id, None).unwrap();
        assert_eq!(mailbox.len(), 3);
        assert_eq!(
            mailbox[0].text,
            if redact {
                TOMBSTONE
            } else {
                original.text.as_str()
            }
        );
    }
}

// Vesta's public baseline_alias.rs: the canonical body gains a quote after
// promotion, so its whole text is no longer contained in the old raw baseline.
#[test]
fn baseline_alias_full_erasure_scrubs_quote_enriched_old_origin_and_db_wal() {
    for explicit_span in [false, true] {
        let f = Fixture::new();
        let canary = "synthetic-basic-group-old-alias-baseline-private-capture";
        let make = |key: &str, id: i64| {
            let mut input = Incoming::new(canary);
            input.sender = Some("telegram:user:1".into());
            input.source_key = Some(key.into());
            input.source_time_ms = Some(123);
            input.origin = Some(crate::Origin {
                chat_id: Some(-1234),
                message_id: Some(id),
                thread_id: None,
            });
            input
        };
        let ReceiveOutcome::New(message) = f
            .store
            .receive(&f.target.id, make("tg-in:initial", 42))
            .unwrap()
        else {
            panic!("initial relay must be new");
        };
        let mut direct = make("webhook:direct", 43);
        direct.quote = Some(crate::Quote {
            author: "external:quote-author".into(),
            author_is_bot: false,
            sender_id: None,
            text: "benign quoted context".into(),
            reference: None,
            mentions: Vec::new(),
        });
        let ReceiveOutcome::Duplicate(promoted) = f.store.receive(&f.target.id, direct).unwrap()
        else {
            panic!("direct webhook must coalesce before erasure");
        };
        assert_eq!(
            (promoted.id.clone(), promoted.seq),
            (message.id, message.seq)
        );
        assert!(promoted.text.contains(canary));
        assert!(promoted.text.contains("benign quoted context"));
        let baseline = || -> String {
            f.store.conn().query_row(
                "SELECT body FROM message_versions WHERE agent_id=?1 AND chat_id=-1234 AND message_id=42",
                [f.target.id.as_str()], |r| r.get(0)).unwrap()
        };
        assert_eq!(baseline(), canary);
        let req = request(
            promoted.id.as_str(),
            explicit_span.then(|| chars(0, promoted.text.chars().count())),
        );
        let receipt = f.redact(&req).unwrap();
        assert!(receipt.full_scrub, "{receipt:?}");
        assert!(receipt.unscrubbed.is_empty());
        assert_eq!(baseline(), TOMBSTONE);
        let aliases: i64 = f.store.conn().query_row(
            "SELECT count(*) FROM message_origin_aliases WHERE agent_id=?1 AND message_id IN (42,43)",
            [f.target.id.as_str()], |r| r.get(0)).unwrap();
        assert_eq!(aliases, 2);
        assert_eq!(
            f.store.inbox(&f.target.id, None).unwrap()[0].text,
            TOMBSTONE
        );
        assert!(
            f.store
                .search_conversation(Some("child"), None, None, None, canary)
                .unwrap()
                .is_empty()
        );
        for suffix in ["state.db", "state.db-wal"] {
            let path = f.dir.path().join(suffix);
            let bytes = if suffix == "state.db" {
                std::fs::read(&path).unwrap()
            } else {
                match std::fs::read(&path) {
                    Ok(bytes) => bytes,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                    Err(e) => panic!("cannot inspect {}: {e}", path.display()),
                }
            };
            assert!(
                !bytes.windows(canary.len()).any(|w| w == canary.as_bytes()),
                "residual canary in {suffix}"
            );
        }
        assert_eq!(f.redact(&req).unwrap(), receipt);
    }
}

#[test]
fn bound_baseline_verification_reports_ignored_writes_and_refuses_stale_replay() {
    for ignore_write in [false, true] {
        let f = Fixture::new();
        let mut incoming = Incoming::new("synthetic baseline verification canary");
        incoming.source_key = Some("bot-a:100".into());
        incoming.origin = Some(crate::Origin {
            chat_id: Some(-1234),
            message_id: Some(42),
            thread_id: None,
        });
        let ReceiveOutcome::New(m) = f.store.receive(&f.target.id, incoming).unwrap() else {
            panic!("new")
        };
        if ignore_write {
            f.store.conn().execute_batch("CREATE TRIGGER ignore_baseline BEFORE UPDATE ON message_versions BEGIN SELECT RAISE(IGNORE); END;").unwrap();
        }
        let req = request(m.id.as_str(), None);
        let receipt = f.redact(&req).unwrap();
        if ignore_write {
            assert!(!receipt.full_scrub);
            assert!(receipt.unscrubbed.iter().any(
                |f| f.path.contains("#message_versions/") && f.error.contains("not tombstoned")
            ));
            let saved: String = f
                .store
                .conn()
                .query_row(
                    "SELECT receipt FROM conversation_redactions WHERE id=?1",
                    [&receipt.audit_id],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(serde_json::from_str::<Receipt>(&saved).unwrap(), receipt);
        } else {
            assert!(receipt.full_scrub);
            assert_eq!(f.redact(&req).unwrap(), receipt);
            f.store
                .conn()
                .execute(
                    "UPDATE message_versions SET body='synthetic out-of-band baseline drift'",
                    [],
                )
                .unwrap();
        }
        let err = f.redact(&req).unwrap_err();
        assert!(
            err.to_string()
                .contains("existing receipt does not verify bound versions"),
            "{err}"
        );
    }
}

#[test]
fn local_id_collision_cannot_erase_an_independent_accounts_baseline() {
    let f = Fixture::new();
    let make = |key: &str, text: &str, sender: &str, time: i64| {
        let mut input = Incoming::new(text);
        input.sender = Some(sender.into());
        input.source_key = Some(key.into());
        input.source_time_ms = Some(time);
        input.origin = Some(crate::Origin {
            chat_id: Some(-1234),
            message_id: Some(43),
            thread_id: None,
        });
        input
    };
    let independent = "independent bot B baseline must survive bot A erasure";
    assert!(matches!(
        f.store
            .receive(
                &f.target.id,
                make("bot-b:1", independent, "telegram:user:2", 123)
            )
            .unwrap(),
        ReceiveOutcome::New(_)
    ));
    let ReceiveOutcome::New(selected) = f
        .store
        .receive(
            &f.target.id,
            make(
                "bot-a:2",
                "selected private bot A body",
                "telegram:user:1",
                60_123,
            ),
        )
        .unwrap()
    else {
        panic!("independent accounts")
    };
    assert!(
        f.redact(&request(selected.id.as_str(), None))
            .unwrap()
            .full_scrub
    );
    let baseline: (String, String) = f.store.conn().query_row("SELECT body, origin_namespace FROM message_versions WHERE agent_id=?1 AND chat_id=-1234 AND message_id=43", [f.target.id.as_str()], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
    assert_eq!(baseline, (independent.into(), "direct:bot-b".into()));
    // A changed update key within bot A stays erased, but cannot poison bot B.
    assert!(matches!(
        f.store
            .receive(
                &f.target.id,
                make(
                    "bot-a:3",
                    "selected private bot A body",
                    "telegram:user:1",
                    60_123
                )
            )
            .unwrap(),
        ReceiveOutcome::Duplicate(_)
    ));
    assert_eq!(
        f.store.inbox(&f.target.id, None).unwrap()[0].text,
        independent
    );
}

#[test]
fn busy_wal_reader_reports_residual_window_instead_of_full_scrub() {
    let f = Fixture::new();
    let canary = "synthetic-busy-reader-physical-residual-canary";
    let m = f.message(canary);
    f.store
        .conn()
        .busy_timeout(std::time::Duration::from_millis(20))
        .unwrap();
    let reader = rusqlite::Connection::open(f.dir.path().join("state.db")).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    assert_eq!(
        reader
            .query_row(
                "SELECT body FROM messages WHERE id=?1",
                [m.id.as_str()],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        canary
    );
    let receipt = f.redact(&request(m.id.as_str(), None)).unwrap();
    assert!(!receipt.full_scrub);
    assert!(receipt.unscrubbed.iter().any(|f| f.path.ends_with("state.db-wal") && f.error.contains("until the reader exits")));
    let stored: String = f
        .store
        .conn()
        .query_row(
            "SELECT receipt FROM conversation_redactions WHERE id=?1",
            [&receipt.audit_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(serde_json::from_str::<Receipt>(&stored).unwrap(), receipt);
    assert_eq!(
        f.store.inbox(&f.target.id, None).unwrap()[0].text,
        TOMBSTONE
    );
    reader.execute_batch("ROLLBACK").unwrap();
    let checkpoint: (i64, i64, i64) = f
        .store
        .conn()
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert_eq!(checkpoint, (0, 0, 0));
    for suffix in ["state.db", "state.db-wal"] {
        let bytes = std::fs::read(f.dir.path().join(suffix)).unwrap();
        assert!(!bytes.windows(canary.len()).any(|w| w == canary.as_bytes()));
    }
}

#[test]
fn legacy_baseline_without_account_is_preserved_and_reported() {
    let f = Fixture::new();
    let mut input = Incoming::new("synthetic old baseline capture");
    input.source_key = Some("bot-a:1".into());
    input.origin = Some(crate::Origin {
        chat_id: Some(-1234),
        message_id: Some(42),
        thread_id: None,
    });
    let ReceiveOutcome::New(m) = f.store.receive(&f.target.id, input).unwrap() else {
        panic!("new")
    };
    f.store
        .conn()
        .execute("UPDATE message_versions SET origin_namespace=NULL", [])
        .unwrap();
    let receipt = f.redact(&request(m.id.as_str(), None)).unwrap();
    assert!(!receipt.full_scrub);
    assert!(
        receipt
            .unscrubbed
            .iter()
            .any(|f| f.error.contains("account is unknown"))
    );
}

#[test]
fn all_coalescence_paths_retain_aliases_across_redaction_reopen_and_aged_history() {
    let f = Fixture::new();
    let canary = "retainedaliascanary original provider body";
    let make = |key: &str, id: i64| {
        let mut input = Incoming::new(canary);
        input.sender = Some("telegram:user:1".into());
        input.source_time_ms = Some(123);
        input.source_key = Some(key.into());
        input.origin = Some(crate::Origin {
            chat_id: Some(-1234),
            message_id: Some(id),
            thread_id: None,
        });
        input
    };
    let ReceiveOutcome::New(message) = f
        .store
        .receive(&f.target.id, make("tg-in:initial", 42))
        .unwrap()
    else {
        panic!("initial relay must be new");
    };
    // source-key duplicate, relay -> direct promotion, origin duplicate and
    // body-equivalent relay coalescence each bring different aliases.
    for input in [
        make("tg-in:initial", 41),
        make("webhook:direct", 43),
        make("webhook:other-source", 43),
        make("room:relay", 44),
    ] {
        let ReceiveOutcome::Duplicate(duplicate) = f.store.receive(&f.target.id, input).unwrap()
        else {
            panic!("known aliases must coalesce");
        };
        assert_eq!(
            (duplicate.id, duplicate.seq),
            (message.id.clone(), message.seq)
        );
    }
    // The identical edit returns through a different duplicate path after the
    // edit-baseline lookup. Its source identity must also survive erasure.
    let mut edit = make("webhook:edit-source", 43);
    edit.text = format!("[Edited message #43]: {canary}");
    assert!(matches!(
        f.store.receive(&f.target.id, edit).unwrap(),
        ReceiveOutcome::Duplicate(_)
    ));
    for i in 0..80 {
        let mut other = Incoming::new(format!("unrelated row {i}"));
        other.origin = Some(crate::Origin {
            chat_id: Some(123),
            message_id: Some(i),
            thread_id: None,
        });
        f.store.receive(&f.target.id, other).unwrap();
    }
    let req = request(message.id.as_str(), None);
    let receipt = f.redact(&req).unwrap();
    assert!(receipt.full_scrub);
    let expected = f
        .store
        .message(&f.target.id, message.id.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(expected.text, TOMBSTONE);
    let store = Store::open(f.dir.path().join("state.db")).unwrap();
    for key in [
        "tg-in:initial",
        "webhook:direct",
        "webhook:other-source",
        "room:relay",
        "webhook:edit-source",
    ] {
        let mut retry = Incoming::new(canary);
        retry.source_key = Some(key.into());
        assert_eq!(
            store.receive_direct_send(&f.target.id, retry).unwrap(),
            ReceiveOutcome::Duplicate(expected.clone())
        );
    }
    // Unknown relay formats only prove their exact source key. A new source
    // may be learned from a known ACCOUNT+origin, never a bare local number.
    for (id, key) in [
        (41, "tg-in:initial"),
        (42, "tg-in:initial"),
        (43, "webhook:learned-source"),
        (44, "room:relay"),
    ] {
        let retry = make(key, id);
        assert_eq!(
            store.receive(&f.target.id, retry).unwrap(),
            ReceiveOutcome::Duplicate(expected.clone())
        );
        // Chained identity learned AFTER redaction is retained as well.
        let mut source_only = Incoming::new(canary);
        source_only.source_key = Some(key.into());
        assert_eq!(
            store.receive(&f.target.id, source_only).unwrap(),
            ReceiveOutcome::Duplicate(expected.clone())
        );
        let mut edit_retry = make(key, id);
        edit_retry.origin.as_mut().unwrap().message_id = None;
        edit_retry.text = format!("[Edited message #{id}]: {canary}");
        assert_eq!(
            store.receive(&f.target.id, edit_retry).unwrap(),
            ReceiveOutcome::Duplicate(expected.clone())
        );
    }
    assert_eq!(store.inbox(&f.target.id, None).unwrap().len(), 81);
    assert!(
        store
            .search_conversation(Some("child"), None, None, None, "retainedaliascanary")
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .redact_conversation(Actor::Agent(f.root.id.clone()), &f.target.id, &req)
            .unwrap(),
        receipt
    );
    assert_eq!(
        store
            .conn()
            .query_row("SELECT count(*) FROM conversation_redactions", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    let raw_baselines: i64 = store.conn().query_row(
        "SELECT count(*) FROM message_versions WHERE agent_id = ?1 AND body LIKE '%retainedaliascanary%'",
        [f.target.id.as_str()], |r| r.get(0)).unwrap();
    assert_eq!(raw_baselines, 0);
    // Recipient/chat isolation: these are separate messages, not aliases.
    assert!(matches!(
        store
            .receive(&f.peer.id, make("tg-in:initial", 42))
            .unwrap(),
        ReceiveOutcome::New(_)
    ));
    let mut other_chat = make("independent-chat", 42);
    other_chat.origin.as_mut().unwrap().chat_id = Some(-5678);
    assert!(matches!(
        store.receive(&f.target.id, other_chat).unwrap(),
        ReceiveOutcome::New(_)
    ));
}

#[test]
fn late_quote_redelivery_inherits_the_complete_alias_set() {
    let f = Fixture::new();
    let canary = "latequotecanary provider reply";
    let make = |key: &str, id: i64| {
        let mut input = Incoming::new(canary);
        input.sender = Some("telegram:user:1".into());
        input.source_time_ms = Some(123);
        input.source_key = Some(key.into());
        input.origin = Some(crate::Origin {
            chat_id: Some(-1234),
            message_id: Some(id),
            thread_id: None,
        });
        input
    };
    let ReceiveOutcome::New(first) = f
        .store
        .receive(&f.target.id, make("tg-in:initial", 42))
        .unwrap()
    else {
        panic!("first relay must be new");
    };
    f.store.note_delivered(&f.target.id, first.seq).unwrap();
    let mut quoted = make("webhook:quoted", 43);
    quoted.quote = Some(crate::Quote {
        author: "author".into(),
        author_is_bot: false,
        sender_id: None,
        text: "quoted context".into(),
        reference: None,
        mentions: Vec::new(),
    });
    let ReceiveOutcome::New(second) = f.store.receive(&f.target.id, quoted).unwrap() else {
        panic!("a late quote after delivery must get its intentional second seq");
    };
    assert_ne!(first.id, second.id);
    assert_eq!(second.seq, first.seq + 1);
    assert!(second.text.contains("quoted context"));
    // The selected second row must fence the first row's old aliases too.
    // This operation intentionally does not erase a different mailbox entry.
    let first_before = f
        .store
        .message(&f.target.id, first.id.as_str())
        .unwrap()
        .unwrap();
    let req = request(second.id.as_str(), None);
    let receipt = f.redact(&req).unwrap();
    assert!(receipt.full_scrub);
    let expected = f
        .store
        .message(&f.target.id, second.id.as_str())
        .unwrap()
        .unwrap();
    let store = Store::open(f.dir.path().join("state.db")).unwrap();
    for key in ["tg-in:initial", "webhook:quoted"] {
        let mut retry = Incoming::new(canary);
        retry.source_key = Some(key.into());
        assert_eq!(
            store.receive(&f.target.id, retry).unwrap(),
            ReceiveOutcome::Duplicate(expected.clone())
        );
    }
    for (id, key) in [(42, "tg-in:initial"), (43, "webhook:new-source")] {
        assert_eq!(
            store.receive(&f.target.id, make(key, id)).unwrap(),
            ReceiveOutcome::Duplicate(expected.clone())
        );
    }
    assert_eq!(
        store.inbox(&f.target.id, None).unwrap(),
        vec![first_before, expected]
    );
    assert_eq!(
        store
            .redact_conversation(Actor::Agent(f.root.id.clone()), &f.target.id, &req)
            .unwrap(),
        receipt
    );
}

#[test]
fn alias_insert_failure_rolls_back_promotion_and_keeps_the_original_identity() {
    let f = Fixture::new();
    let mut relay = Incoming::new("ordinary relay control");
    relay.sender = Some("telegram:user:1".into());
    relay.source_time_ms = Some(123);
    relay.source_key = Some("tg-in:initial".into());
    relay.origin = Some(crate::Origin {
        chat_id: Some(-1234),
        message_id: Some(42),
        thread_id: None,
    });
    let ReceiveOutcome::New(message) = f.store.receive(&f.target.id, relay.clone()).unwrap() else {
        panic!("new relay");
    };
    f.store.conn().execute_batch(
        "CREATE TRIGGER refuse_alias BEFORE INSERT ON message_source_aliases WHEN NEW.source_key = 'webhook:direct' BEGIN SELECT RAISE(ABORT, 'synthetic alias refusal'); END;"
    ).unwrap();
    relay.source_key = Some("webhook:direct".into());
    relay.origin.as_mut().unwrap().message_id = Some(43);
    let err = f.store.receive(&f.target.id, relay).unwrap_err();
    assert!(err.to_string().contains("synthetic alias refusal"), "{err}");
    assert_eq!(f.store.inbox(&f.target.id, None).unwrap(), vec![message]);
    assert_eq!(
        f.store
            .conn()
            .query_row(
                "SELECT count(*) FROM message_origin_aliases WHERE message_id = 43",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
}

#[test]
fn replay_refuses_a_stale_receipt_when_the_canonical_tombstone_has_drifted() {
    for span in [None, Some(chars(7, 19))] {
        let f = Fixture::new();
        let m = f.message("prefix replaycanary suffix");
        let req = request(m.id.as_str(), span);
        let receipt = f.redact(&req).unwrap();
        assert_eq!(f.redact(&req).unwrap(), receipt);
        // Synthetic storage corruption, not an application writer: replay must
        // independently verify today's canonical body rather than trust audit.
        f.store
            .conn()
            .execute(
                "UPDATE messages SET body = ?1 WHERE id = ?2",
                params![m.text, m.id.as_str()],
            )
            .unwrap();
        let err = f.redact(&req).unwrap_err();
        assert!(
            err.to_string()
                .contains("existing receipt does not verify the current body"),
            "{err}"
        );
        let audit: String = f
            .store
            .conn()
            .query_row(
                "SELECT receipt FROM conversation_redactions WHERE id = ?1",
                [&receipt.audit_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(serde_json::from_str::<Receipt>(&audit).unwrap(), receipt);
    }
}

// A provider retry/edit is not permission to undo an audited erasure. Exercise
// both duplicate-enrichment branches, basic groups, and an origin older than
// the ordinary deduplicator's bounded history window.
#[test]
fn redacted_provider_redelivery_and_edits_preserve_body_index_and_audit() {
    for chat_id in [-1_001_234_567_890, -1234] {
        for source in ["webhook:initial", "tg-in:initial"] {
            for partial in [false, true] {
                let f = Fixture::new();
                let original =
                    "prefix providerredeliverycanary and additional private fixture text suffix";
                let mut incoming = Incoming::new(original);
                incoming.sender = Some("telegram:user:123".into());
                incoming.source_key = Some(source.into());
                incoming.source_time_ms = Some(123);
                incoming.origin = Some(crate::Origin {
                    chat_id: Some(chat_id),
                    message_id: Some(42),
                    thread_id: Some(7),
                });
                let ReceiveOutcome::New(m) =
                    f.store.receive(&f.target.id, incoming.clone()).unwrap()
                else {
                    panic!("initial message must be new");
                };
                for alias in ["webhook:retry", "tg-in:retry", "room:retry"] {
                    let mut equivalent = incoming.clone();
                    equivalent.source_key = Some(alias.into());
                    assert!(matches!(
                        f.store.receive(&f.target.id, equivalent).unwrap(),
                        ReceiveOutcome::Duplicate(_)
                    ));
                }
                let m = f
                    .store
                    .message(&f.target.id, m.id.as_str())
                    .unwrap()
                    .unwrap();
                let span = partial.then(|| chars(7, original.chars().count() - 7));
                let req = request(m.id.as_str(), span);
                let receipt = f.redact(&req).unwrap();
                assert!(receipt.full_scrub, "{receipt:?}");
                let mut expected = m.clone();
                expected.text = if partial {
                    "prefix [redacted] suffix".into()
                } else {
                    TOMBSTONE.into()
                };
                let audit_before: String = f
                    .store
                    .conn()
                    .query_row(
                        "SELECT receipt FROM conversation_redactions WHERE id = ?1",
                        [&receipt.audit_id],
                        |r| r.get(0),
                    )
                    .unwrap();
                let binding: (i64, i64) = f.store.conn().query_row(
                    "SELECT origin_chat_id, origin_message_id FROM conversation_redactions WHERE id = ?1",
                    [&receipt.audit_id], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
                assert_eq!(binding, (chat_id, 42));
                let baseline_before: String = f.store.conn().query_row(
                    "SELECT body FROM message_versions WHERE agent_id = ?1 AND chat_id = ?2 AND message_id = 42",
                    params![f.target.id.as_str(), chat_id], |r| r.get(0)).unwrap();
                // Use a second connection so protection must survive persistence.
                let reopened = Store::open(f.dir.path().join("state.db")).unwrap();
                for aged in [false, true] {
                    if aged {
                        for i in 0..65 {
                            let mut filler = Incoming::new("unrelated provider history");
                            filler.origin = Some(crate::Origin {
                                chat_id: Some(chat_id),
                                message_id: Some(1000 + i),
                                thread_id: None,
                            });
                            reopened.receive(&f.target.id, filler).unwrap();
                        }
                    }
                    for (key, text) in [
                        (source, original.to_string()),
                        ("webhook:retry", original.to_string()),
                        ("tg-in:retry", original.to_string()),
                        ("room:retry", original.to_string()),
                        (
                            "webhook:edit",
                            format!("[Edited message #42]: {original} extended edit"),
                        ),
                    ] {
                        let mut retry = incoming.clone();
                        retry.source_key = Some(key.into());
                        retry.text = text;
                        let outcome = reopened.receive(&f.target.id, retry).unwrap();
                        let ReceiveOutcome::Duplicate(duplicate) = outcome else {
                            panic!("redacted origin must not append a new row: {outcome:?}");
                        };
                        assert_eq!(
                            duplicate, expected,
                            "{chat_id} {source} {partial} {aged} {key}"
                        );
                        assert_eq!(reopened.inbox(&f.target.id, None).unwrap()[0], expected);
                        assert!(
                            reopened
                                .search_conversation(
                                    Some("child"),
                                    None,
                                    None,
                                    None,
                                    "providerredeliverycanary"
                                )
                                .unwrap()
                                .is_empty()
                        );
                        let conn = reopened.conn();
                        let (audit, count): (String, i64) = conn.query_row(
                            "SELECT receipt, (SELECT count(*) FROM conversation_redactions) FROM conversation_redactions WHERE id = ?1",
                            [&receipt.audit_id], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
                        assert_eq!(audit, audit_before);
                        assert_eq!(count, 1);
                        let baseline: String = conn.query_row(
                            "SELECT body FROM message_versions WHERE agent_id = ?1 AND chat_id = ?2 AND message_id = 42",
                            params![f.target.id.as_str(), chat_id], |r| r.get(0)).unwrap();
                        assert_eq!(baseline, baseline_before);
                        let indexed: String = conn
                            .query_row(
                                "SELECT text FROM rag_chunks WHERE chunk_id = ?1",
                                [m.id.as_str()],
                                |r| r.get(0),
                            )
                            .unwrap();
                        assert_eq!(indexed, expected.text);
                    }
                }
                assert_eq!(f.redact(&req).unwrap(), receipt);
                assert_eq!(reopened.inbox(&f.target.id, None).unwrap().len(), 66);
                // Binding is recipient + chat + provider message, never message id alone.
                let mut other_chat = incoming.clone();
                other_chat.source_key = Some("webhook:other-chat".into());
                other_chat.origin.as_mut().unwrap().chat_id = Some(chat_id - 1);
                assert!(matches!(
                    reopened.receive(&f.target.id, other_chat).unwrap(),
                    ReceiveOutcome::New(_)
                ));
                assert!(matches!(
                    reopened.receive(&f.peer.id, incoming).unwrap(),
                    ReceiveOutcome::New(_)
                ));
            }
        }
    }
}

#[test]
fn redacted_relay_alias_cannot_enrich_either_body_update_path() {
    // Different bot-local message IDs bypass the exact-origin binding. The
    // existing relay matcher still finds this canonical row by safe body/user/
    // time; its identity guard must refuse a quote upgrade containing old text.
    for (stored_key, retry_key) in [
        ("tg-in:initial", "webhook:alias"),
        ("webhook:initial", "tg-in:alias"),
    ] {
        let f = Fixture::new();
        let mut incoming = Incoming::new("prefix relayaliascanary suffix");
        incoming.sender = Some("telegram:user:123".into());
        incoming.source_time_ms = Some(123);
        incoming.source_key = Some(stored_key.into());
        incoming.origin = Some(crate::Origin {
            chat_id: Some(-1234),
            message_id: Some(42),
            thread_id: None,
        });
        let ReceiveOutcome::New(mut expected) =
            f.store.receive(&f.target.id, incoming.clone()).unwrap()
        else {
            panic!("initial message must be new");
        };
        let req = request(expected.id.as_str(), Some(chars(7, 23)));
        let receipt = f.redact(&req).unwrap();
        expected.text = "prefix [redacted] suffix".into();
        incoming.text = expected.text.clone();
        incoming.source_key = Some(retry_key.into());
        incoming.origin.as_mut().unwrap().message_id = Some(43);
        incoming.quote = Some(crate::Quote {
            author: "external:user".into(),
            author_is_bot: false,
            sender_id: None,
            text: "relayaliascanary in a late quote upgrade".into(),
            reference: None,
            mentions: Vec::new(),
        });
        let outcome = f.store.receive(&f.target.id, incoming).unwrap();
        let ReceiveOutcome::Duplicate(duplicate) = outcome else {
            panic!("relay alias must resolve the existing row: {outcome:?}");
        };
        assert_eq!(duplicate, expected);
        assert_eq!(f.store.inbox(&f.target.id, None).unwrap(), vec![expected]);
        assert_eq!(f.redact(&req).unwrap(), receipt);
    }
}

#[test]
fn unredacted_provider_duplicates_still_enrich_both_body_update_paths() {
    for source in ["webhook:initial", "tg-in:initial"] {
        let f = Fixture::new();
        let mut incoming = Incoming::new("short body");
        incoming.sender = Some("telegram:user:123".into());
        incoming.source_key = Some(source.into());
        incoming.origin = Some(crate::Origin {
            chat_id: Some(-1_001_234_567_890),
            message_id: Some(42),
            thread_id: None,
        });
        let ReceiveOutcome::New(m) = f.store.receive(&f.target.id, incoming.clone()).unwrap()
        else {
            panic!("initial message must be new");
        };
        incoming.source_key = Some("webhook:retry".into());
        incoming.text = "short body with legitimate extra context".into();
        let ReceiveOutcome::Duplicate(enriched) =
            f.store.receive(&f.target.id, incoming.clone()).unwrap()
        else {
            panic!("unredacted retry must enrich the existing row");
        };
        assert_eq!(enriched.id, m.id);
        assert_eq!(enriched.text, incoming.text);
        assert_eq!(
            f.store.inbox(&f.target.id, None).unwrap()[0].text,
            incoming.text
        );
    }
}

#[test]
fn replay_by_original_span_or_seq_returns_existing_audit_after_reopen() {
    let f = Fixture::new();
    let m = f.message("first syntheticcanary last");
    let req = request(m.id.as_str(), Some(chars(6, 21)));
    let one = f.redact(&req).unwrap();
    let reopened = Store::open(f.dir.path().join("state.db")).unwrap();
    let mut by_seq = req.clone();
    by_seq.message_id = None;
    by_seq.seq = Some(m.seq);
    let two = reopened
        .redact_conversation(Actor::Agent(f.root.id.clone()), &f.target.id, &by_seq)
        .unwrap();
    assert_eq!(one, two);
    assert_eq!(
        f.store.inbox(&f.target.id, None).unwrap()[0].text,
        "first [redacted] last"
    );
    assert_eq!(
        f.store
            .conn()
            .query_row("SELECT count(*) FROM conversation_redactions", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn peers_self_and_descendants_cannot_redact_even_with_shared_group() {
    let f = Fixture::new();
    let m = f.message("syntheticcanary");
    f.store.join_group("shared", &f.target.id).unwrap();
    f.store.join_group("shared", &f.peer.id).unwrap();
    for id in [&f.peer.id, &f.target.id] {
        let err = f
            .store
            .redact_conversation(
                Actor::Agent(id.clone()),
                &f.target.id,
                &request(m.id.as_str(), None),
            )
            .unwrap_err();
        assert!(matches!(err, Error::PermissionDenied(_)), "{err}");
    }
    let err = f
        .store
        .redact_conversation(
            Actor::Agent(f.target.id.clone()),
            &f.root.id,
            &request(m.id.as_str(), None),
        )
        .unwrap_err();
    assert!(matches!(err, Error::PermissionDenied(_)), "{err}");
    assert_eq!(f.store.inbox(&f.target.id, None).unwrap()[0], m);
    assert_eq!(
        f.store
            .conn()
            .query_row("SELECT count(*) FROM conversation_redactions", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn admin_operator_and_deep_ancestor_are_authorized() {
    let f = Fixture::new();
    let mut ancestor = f.root.clone();
    for i in 0..20 {
        let mut input = NewAgent::new(format!("depth-{i}"), "sylphx");
        input.parent_id = Some(ancestor.id.clone());
        ancestor = f.store.create_agent(input).unwrap();
    }
    f.store
        .set_agent_parent(&f.target.id, Some(&ancestor.id))
        .unwrap();
    let m = f.message("deep canary");
    f.redact(&request(m.id.as_str(), None)).unwrap();
    f.store.set_agent_admin(&f.peer.id, true).unwrap();
    let out = f
        .store
        .record_outbound(&f.target.id, "group:fixture", None, "outbound canary")
        .unwrap();
    f.store
        .redact_conversation(
            Actor::Agent(f.peer.id.clone()),
            &f.target.id,
            &request(&out, None),
        )
        .unwrap();
    let tool = f
        .store
        .record_tool_event(&f.target.id, "bash", "tool canary")
        .unwrap();
    f.store
        .redact_conversation(Actor::Operator, &f.target.id, &request(&tool, None))
        .unwrap();
    let conn = f.store.conn();
    let all: Vec<String> = conn
        .prepare("SELECT text FROM rag_chunks WHERE agent_id = ?1")
        .unwrap()
        .query_map([f.target.id.as_str()], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(all, vec![TOMBSTONE, TOMBSTONE, TOMBSTONE]);
}

#[test]
fn invalid_spans_are_refused_without_audit_or_any_change() {
    let f = Fixture::new();
    let m = f.message("a後b");
    for span in [
        chars(0, 5),
        chars(2, 1),
        chars(0, 0),
        Span {
            unit: SpanUnit::Bytes,
            start: 1,
            end: 2,
        },
        Span {
            unit: SpanUnit::Bytes,
            start: 0,
            end: usize::MAX,
        },
    ] {
        let err = f.redact(&request(m.id.as_str(), Some(span))).unwrap_err();
        assert!(err.to_string().contains("span"), "{err}");
    }
    assert_eq!(f.store.inbox(&f.target.id, None).unwrap()[0], m);
    assert_eq!(
        f.store
            .conn()
            .query_row("SELECT count(*) FROM conversation_redactions", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn all_exact_session_copies_scrub_decoded_span_preserving_unrelated_lines() {
    let f = Fixture::new();
    let m = f.message("before \"synthetic\\後\" after");
    let primary = f.path("sylphx-transcript.jsonl");
    let claude = f.path("projects/project/session.jsonl");
    let unrelated = "{ \"role\" : \"user\", \"content\" : \"untouched\" }\n";
    let source = format!(
        "{}\n{unrelated}",
        json!({"type":"function_call_output", "call_id":"call_1", "output":m.text})
    );
    std::fs::write(&primary, &source).unwrap();
    std::fs::write(&claude, format!("{}\n{unrelated}", json!({"uuid":"stable", "message":{"content":[{"text":format!("envelope {} end",m.text)}]}}))).unwrap();
    let receipt = f
        .redact(&request(m.id.as_str(), Some(chars(8, 19))))
        .unwrap();
    assert!(receipt.full_scrub, "{receipt:?}");
    assert_eq!(receipt.scrubbed_paths.len(), 2);
    for path in [&primary, &claude] {
        let raw = std::fs::read_to_string(path).unwrap();
        assert!(raw.ends_with(unrelated));
        assert!(!raw.contains("synthetic"));
        for line in raw.lines() {
            let _: serde_json::Value = serde_json::from_str(line).unwrap();
        }
    }
    let line: serde_json::Value = serde_json::from_str(
        std::fs::read_to_string(primary)
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(line["call_id"], "call_1");
    assert_eq!(line["output"], "before \"[redacted]\" after");
}

#[test]
fn transcript_selector_erases_private_tool_output_and_is_idempotent() {
    let f = Fixture::new();
    let path = f.path("sylphx-transcript.jsonl");
    let keep = "{ \"role\": \"user\", \"content\": \"unchanged\" }\n";
    std::fs::write(&path, format!("{keep}{}\n{keep}", json!({"type":"function_call_output","call_id":"call_1","output":"prefix toolcanary suffix"}))).unwrap();
    let req = request("transcript:1", Some(chars(7, 17)));
    let receipt = f.redact(&req).unwrap();
    assert!(receipt.full_scrub, "{receipt:?}");
    let raw = std::fs::read_to_string(&path).unwrap();
    assert!(raw.starts_with(keep));
    assert!(raw.ends_with(keep));
    assert_eq!(raw.lines().count(), 3);
    let val: serde_json::Value = serde_json::from_str(raw.lines().nth(1).unwrap()).unwrap();
    assert_eq!(val["output"], "prefix [redacted] suffix");
    assert_eq!(val["call_id"], "call_1");
    assert_eq!(f.redact(&req).unwrap(), receipt);
    assert!(
        !std::fs::read_to_string(&path)
            .unwrap()
            .contains("toolcanary")
    );
}

#[test]
fn unreadable_invalid_or_unsupported_copies_are_explicit_not_full_scrub() {
    let f = Fixture::new();
    let m = f.message("syntheticcanary");
    let bad = f.path("projects/broken.jsonl");
    std::fs::write(&bad, "not json").unwrap();
    let binary = f.path("data-home/opencode/session.db");
    std::fs::write(&binary, "binary fixture").unwrap();
    let receipt = f.redact(&request(m.id.as_str(), None)).unwrap();
    assert!(!receipt.full_scrub);
    assert_eq!(receipt.unscrubbed.len(), 2);
    assert!(
        receipt
            .unscrubbed
            .iter()
            .any(|f| f.path == bad.display().to_string())
    );
    assert!(
        receipt
            .unscrubbed
            .iter()
            .any(|f| f.path == binary.display().to_string())
    );
    assert_eq!(std::fs::read_to_string(bad).unwrap(), "not json");
}

#[test]
fn symlink_and_hard_link_cannot_modify_other_agent_or_outside_file() {
    let f = Fixture::new();
    let m = f.message("syntheticcanary");
    let outside = f.dir.path().join("outside.jsonl");
    let raw = format!("{}\n", json!({"content":m.text}));
    std::fs::write(&outside, &raw).unwrap();
    let link = f.path("projects/linked.jsonl");
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    let hard = f.path("projects/hard.jsonl");
    std::fs::hard_link(&outside, &hard).unwrap();
    let receipt = f.redact(&request(m.id.as_str(), None)).unwrap();
    assert!(!receipt.full_scrub);
    assert_eq!(receipt.unscrubbed.len(), 2);
    assert_eq!(std::fs::read_to_string(outside).unwrap(), raw);
}

#[test]
fn running_or_locked_harness_refuses_instead_of_claiming_live_memory_erasure() {
    let f = Fixture::new();
    let m = f.message("syntheticcanary");
    let req = request(m.id.as_str(), None);
    f.store.set_agent_state(&f.target.id, "running").unwrap();
    assert!(
        f.redact(&req)
            .unwrap_err()
            .to_string()
            .contains("quiescent")
    );
    f.store.set_agent_state(&f.target.id, "paused").unwrap();
    let _lock = f.store.try_lock_agent(&f.target.id).unwrap();
    assert!(matches!(f.redact(&req).unwrap_err(), Error::AgentBusy));
    assert_eq!(f.store.inbox(&f.target.id, None).unwrap()[0], m);
}

#[test]
fn telegram_edit_baseline_is_scrubbed_without_changing_other_provider_rows() {
    let f = Fixture::new();
    let mut input = Incoming::new("prefix providercanary suffix");
    input.origin = Some(crate::Origin {
        chat_id: Some(1),
        message_id: Some(2),
        thread_id: None,
    });
    let id = match f.store.receive(&f.target.id, input).unwrap() {
        ReceiveOutcome::New(m) => m.id,
        _ => unreachable!(),
    };
    f.redact(&request(id.as_str(), Some(chars(7, 21)))).unwrap();
    let baseline: String = f.store.conn().query_row("SELECT body FROM message_versions WHERE agent_id = ?1 AND chat_id = 1 AND message_id = 2", [f.target.id.as_str()], |r| r.get(0)).unwrap();
    assert_eq!(baseline, "prefix [redacted] suffix");
}

#[test]
fn audit_reason_cannot_repeat_removed_text_and_tombstone_span_reuses_receipt() {
    let f = Fixture::new();
    let m = f.message("syntheticcanary");
    let mut req = request(m.id.as_str(), None);
    req.reason = "remove syntheticcanary".into();
    assert!(
        f.redact(&req)
            .unwrap_err()
            .to_string()
            .contains("audit reason")
    );
    req.reason = "accidental synthetic capture".into();
    let receipt = f.redact(&req).unwrap();
    req.span = Some(chars(0, 10));
    assert_eq!(f.redact(&req).unwrap(), receipt);
}

#[test]
fn nonmatching_occurrence_is_reported_without_erasing_an_unrelated_message() {
    let f = Fixture::new();
    let m = f.message("prefix syntheticcanary suffix");
    let path = f.path("sylphx-transcript.jsonl");
    let raw = format!(
        "{}\n",
        json!({"role":"user","content":"different syntheticcanary body"})
    );
    std::fs::write(&path, &raw).unwrap();
    let receipt = f
        .redact(&request(m.id.as_str(), Some(chars(7, 22))))
        .unwrap();
    assert!(!receipt.full_scrub);
    assert_eq!(receipt.unscrubbed.len(), 1);
    assert_eq!(std::fs::read_to_string(path).unwrap(), raw);
}

#[test]
fn transcript_content_equal_to_protocol_identifier_never_changes_call_id() {
    let f = Fixture::new();
    let path = f.path("sylphx-transcript.jsonl");
    std::fs::write(
        &path,
        "{\"type\":\"function_call_output\",\"call_id\":\"call_1\",\"output\":\"call_1\"}\n",
    )
    .unwrap();
    let receipt = f.redact(&request("transcript:0", None)).unwrap();
    let value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(value["call_id"], "call_1");
    assert_eq!(value["output"], TOMBSTONE);
    assert!(!receipt.full_scrub); // Deliberately preserved protocol metadata is reported.
}

#[test]
fn linked_agent_directory_cannot_redirect_scrubbing_outside_the_managed_tree() {
    let f = Fixture::new();
    let m = f.message("syntheticcanary");
    let outside = f.dir.path().join("outside-agent");
    std::fs::create_dir_all(outside.join("config")).unwrap();
    let external_session = outside.join("config/sylphx-transcript.jsonl");
    let raw = format!("{}\n", json!({"output":m.text}));
    std::fs::write(&external_session, &raw).unwrap();
    std::fs::create_dir_all(f.dir.path().join("agents")).unwrap();
    std::os::unix::fs::symlink(
        &outside,
        f.dir.path().join("agents").join(f.target.id.as_str()),
    )
    .unwrap();
    let receipt = f.redact(&request(m.id.as_str(), None)).unwrap();
    assert!(!receipt.full_scrub);
    assert_eq!(receipt.unscrubbed.len(), 1);
    assert_eq!(std::fs::read_to_string(external_session).unwrap(), raw);
}

#[test]
fn nested_serialized_json_and_unicode_escapes_scrub_logical_text() {
    let f = Fixture::new();
    let m = f.message("prefix 後syntheticcanary suffix");
    let path = f.path("sylphx-transcript.jsonl");
    let nested = json!({"rows":[{"value":m.text}]}).to_string();
    let raw = format!(
        "{}\n",
        json!({"type":"function_call_output", "call_id":"stable", "output":nested})
    )
    .replace('後', "\\u5f8c");
    std::fs::write(&path, raw).unwrap();
    let receipt = f
        .redact(&request(m.id.as_str(), Some(chars(7, 23))))
        .unwrap();
    assert!(receipt.full_scrub, "{receipt:?}");
    let outer: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let inner: serde_json::Value = serde_json::from_str(outer["output"].as_str().unwrap()).unwrap();
    assert_eq!(inner["rows"][0]["value"], "prefix [redacted] suffix");
    assert_eq!(outer["call_id"], "stable");
}
