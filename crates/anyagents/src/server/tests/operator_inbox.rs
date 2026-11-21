use super::super::messaging::{SendPlan, send, send_sync};
use super::super::messaging_conversation::{Since, inbox, operator_inbox, operator_reports};
use super::super::*;
use super::helpers::{archetype_validation_state, http};
use crate::api::SendRequest;
use anyagents_core::Agent;
use axum::Json;
use axum::http::StatusCode;

#[tokio::test]
async fn agents_report_to_the_operator_and_only_the_operator_reads_it() {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let root = state
        .store
        .create_agent(anyagents_core::NewAgent::new("report-root", "opencode"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("report-child", "opencode");
    new.parent_id = Some(root.id.clone());
    let child = state.store.create_agent(new).unwrap();
    let as_agent = |agent: &Agent| Principal::Agent {
        agent_id: agent.id.clone(),
        name: agent.name.clone(),
    };
    let report = |principal: Principal, to: &[&str], text: &str| {
        send(
            principal,
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(SendRequest {
                to: to.iter().map(|to| to.to_string()).collect(),
                text: text.to_string(),
                ..SendRequest::default()
            }),
        )
    };
    let read = |principal: Principal, since: Option<i64>| {
        operator_reports(
            principal,
            State(state.clone()),
            Query(Since {
                since,
                limit: None,
                unread: None,
                sender: None,
            }),
        )
    };

    // Delivery: the report lands in the operator's inbox, keyed to the
    // sender's immutable id, and in the sender's own history.
    let text = "worktree add failed: EACCES on .git/worktrees; tried a fresh clone";
    let Json(sent) = report(as_agent(&child), &["operator"], text).await.unwrap();
    assert_eq!(sent.deliveries.len(), 1);
    let delivery = &sent.deliveries[0];
    assert_eq!(delivery.to, "operator");
    assert!(!delivery.duplicate);
    let Json(inbox_view) = read(Principal::Operator, None).await.unwrap();
    assert_eq!(inbox_view.messages.len(), 1);
    let stored = &inbox_view.messages[0];
    assert_eq!(stored.id, delivery.id);
    assert_eq!(stored.text, text);
    assert_eq!(stored.sender, format!("agent:{}", child.id));
    assert_eq!(stored.sender_name.as_deref(), Some("report-child"));
    let history = state
        .store
        .conversation(&child.id, Some("operator"), None, None)
        .unwrap();
    assert!(
        history
            .iter()
            .any(|turn| turn.direction == "out" && turn.text == text),
        "{history:?}"
    );
    assert!(
        read(Principal::Operator, Some(stored.seq))
            .await
            .unwrap()
            .0
            .messages
            .is_empty()
    );

    // An agent may only write: neither the operator route nor an agent
    // mailbox route reaches it, for a root admin too.
    for principal in [as_agent(&child), as_agent(&root)] {
        let err = read(principal.clone(), None).await.unwrap_err();
        assert_eq!(err.status, StatusCode::FORBIDDEN, "{}", err.message);
        let err = inbox(
            principal.clone(),
            State(state.clone()),
            AxumPath("operator".into()),
            Query(Since {
                since: None,
                limit: None,
                unread: None,
                sender: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::FORBIDDEN, "{}", err.message);
        let err = operator_inbox(
            principal,
            State(state.clone()),
            AxumPath("operator".into()),
            Query(Since {
                since: None,
                limit: None,
                unread: None,
                sender: None,
            }),
        )
        .await
        .unwrap_err();
        assert_ne!(err.status, StatusCode::OK, "{}", err.message);
    }

    // Deduplication: the same report again is the stored one.
    let Json(again) = report(as_agent(&child), &["operator"], text).await.unwrap();
    assert!(again.deliveries[0].duplicate);
    assert_eq!(again.deliveries[0].id, delivery.id);
    let Json(counted) = read(Principal::Operator, None).await.unwrap();
    assert_eq!(counted.messages[0].repeats, 1);

    // Rate limit: after the burst the sender is refused with 429, before
    // any other address of the same send is written.
    for i in 1..anyagents_core::OPERATOR_BURST {
        let Json(sent) = report(as_agent(&child), &["operator"], &format!("defect {i}"))
            .await
            .unwrap();
        assert!(!sent.deliveries[0].duplicate);
    }
    let parent_mail = state.store.inbox(&root.id, None).unwrap().len();
    let err = report(as_agent(&child), &["parent", "operator"], "one more")
        .await
        .unwrap_err();
    assert_eq!(err.status, StatusCode::TOO_MANY_REQUESTS, "{}", err.message);
    assert!(err.message.contains("Retry in"), "{}", err.message);
    assert_eq!(
        state.store.inbox(&root.id, None).unwrap().len(),
        parent_mail
    );
    // Another sender has its own limit.
    let Json(other) = report(as_agent(&root), &["operator"], "one more")
        .await
        .unwrap();
    assert!(!other.deliveries[0].duplicate);
    let Json(all) = read(Principal::Operator, None).await.unwrap();
    assert_eq!(
        all.messages.len(),
        usize::try_from(anyagents_core::OPERATOR_BURST).unwrap() + 1
    );
}

#[test]
fn concurrent_reports_never_leave_a_mixed_send_half_delivered() {
    let dir = crate::test_tempdir();
    let store = Arc::new(Store::open(dir.path().join("state.db")).unwrap());
    let parent = store
        .create_agent(anyagents_core::NewAgent::new("race-parent", "opencode"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("race-child", "opencode");
    new.parent_id = Some(parent.id.clone());
    store.create_agent(new).unwrap();
    let senders = 8;
    let barrier = Arc::new(std::sync::Barrier::new(senders));
    let workers: Vec<_> = (0..senders)
        .map(|i| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let actor = Actor::Agent("race-child".into());
                let body = SendRequest {
                    sender: Some(actor.as_address()),
                    to: vec!["parent".into(), "operator".into()],
                    text: format!("defect {i}"),
                    ..SendRequest::default()
                };
                let plan = SendPlan::parse(&store, &actor, &body).unwrap();
                barrier.wait();
                send_sync(&store, &actor, body, plan, None)
                    .map(|_| ())
                    .map_err(|err| err.status)
            })
        })
        .collect();
    let results: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert!(
        results
            .iter()
            .all(|r| matches!(r, Ok(()) | Err(StatusCode::TOO_MANY_REQUESTS))),
        "{results:?}"
    );
    let sent = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(
        sent,
        usize::try_from(anyagents_core::OPERATOR_BURST).unwrap()
    );
    // Every refused send wrote nothing: the parent got exactly the sends
    // the operator inbox took.
    assert_eq!(store.operator_inbox(None, None).unwrap().len(), sent);
    assert_eq!(store.inbox(&parent.id, None).unwrap().len(), sent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn over_http_a_report_is_capped_at_16_kib_and_only_the_operator_token_reads_it() {
    let dir = crate::test_tempdir();
    let mut state = archetype_validation_state(dir.path());
    let operator_token = "operator-token-0123456789abcdef0123456789abcdef";
    state.auth = auth::Mode::Confined {
        operator_digest: Some(auth::operator_digest(operator_token)),
    };
    let parent = state
        .store
        .create_agent(anyagents_core::NewAgent::new("http-parent", "opencode"))
        .unwrap();
    let mut new = anyagents_core::NewAgent::new("http-child", "opencode");
    new.parent_id = Some(parent.id.clone());
    let child = state.store.create_agent(new).unwrap();
    let capability = state.store.issue_capability(&child.id).unwrap();
    let socket = dir.path().join("operator-inbox.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let app = build_router(state.clone());
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    let report = |text: String| serde_json::json!({ "to": ["parent", "operator"], "text": text });

    // One byte over the cap is refused before any target is written.
    let max = 16 * 1024;
    let (status, refused) = http(
        &socket,
        "POST",
        "/v1/send",
        Some(report("x".repeat(max + 1))),
        Some(&capability),
        None,
    )
    .await;
    assert_eq!(status, hyper::StatusCode::BAD_REQUEST, "{refused}");
    assert!(
        refused["error"].as_str().unwrap().contains("16 KiB"),
        "{refused}"
    );
    assert!(state.store.inbox(&parent.id, None).unwrap().is_empty());
    assert!(state.store.operator_inbox(None, None).unwrap().is_empty());
    // Exactly the cap is delivered to both.
    let (status, sent) = http(
        &socket,
        "POST",
        "/v1/send",
        Some(report("y".repeat(max))),
        Some(&capability),
        None,
    )
    .await;
    assert_eq!(status, hyper::StatusCode::OK, "{sent}");
    assert_eq!(sent["deliveries"].as_array().unwrap().len(), 2, "{sent}");

    // The agent's capability, no credential and a wrong token cannot read.
    for (capability, operator, expected) in [
        (
            Some(capability.as_str()),
            None,
            hyper::StatusCode::FORBIDDEN,
        ),
        (None, None, hyper::StatusCode::UNAUTHORIZED),
        (None, Some("not-the-token"), hyper::StatusCode::UNAUTHORIZED),
    ] {
        let (status, body) = http(
            &socket,
            "GET",
            "/v1/operator/inbox",
            None,
            capability,
            operator,
        )
        .await;
        assert_eq!(status, expected, "{body}");
    }
    let (status, read) = http(
        &socket,
        "GET",
        "/v1/operator/inbox?limit=5",
        None,
        None,
        Some(operator_token),
    )
    .await;
    assert_eq!(status, hyper::StatusCode::OK, "{read}");
    let messages = read["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 1, "{read}");
    assert_eq!(messages[0]["text"].as_str().unwrap().len(), max);
    assert_eq!(messages[0]["sender_name"], "http-child");
    server.abort();
}
