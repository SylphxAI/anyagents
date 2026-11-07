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
        let dir = tempfile::tempdir().unwrap();
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
