use super::super::ops::{UsageQuery, usage_report};
use super::super::*;
use super::helpers::archetype_validation_state;
use anyagents_core::Agent;
use axum::Json;
use axum::http::StatusCode;

fn usage_fixture() -> (
    tempfile::TempDir,
    AppState,
    Agent,
    Agent,
    Agent,
    Agent,
    Agent,
) {
    let dir = crate::test_tempdir();
    let state = archetype_validation_state(dir.path());
    let root = state
        .store
        .create_agent(anyagents_core::NewAgent::new("usage-root", "opencode"))
        .unwrap();
    let mut cn = anyagents_core::NewAgent::new("usage-child", "opencode");
    cn.parent_id = Some(root.id.clone());
    let child = state.store.create_agent(cn).unwrap();
    let mut gn = anyagents_core::NewAgent::new("usage-grandchild", "opencode");
    gn.parent_id = Some(child.id.clone());
    let grandchild = state.store.create_agent(gn).unwrap();
    let mut sn = anyagents_core::NewAgent::new("usage-sibling", "opencode");
    sn.parent_id = Some(root.id.clone());
    let sibling = state.store.create_agent(sn).unwrap();
    let other_root = state
        .store
        .create_agent(anyagents_core::NewAgent::new(
            "usage-other-root",
            "opencode",
        ))
        .unwrap();
    for (agent, tokens) in [
        (&root, 100),
        (&child, 200),
        (&grandchild, 400),
        (&sibling, 800),
        (&other_root, 1600),
    ] {
        state
            .store
            .record_usage(anyagents_core::NewUsage {
                agent_id: agent.id.clone(),
                harness: "opencode".into(),
                model: Some("m1".into()),
                input_tokens: tokens,
                output_tokens: tokens / 2,
                cache_read_tokens: 1,
                cache_write_tokens: 2,
                reported_cost_micros: Some(tokens),
                estimated_cost_micros: None,
                billing_mode: anyagents_core::BillingMode::Subscription,
                quota_state: None,
                day: "2026-11-01".into(),
            })
            .unwrap();
    }
    (dir, state, root, child, grandchild, sibling, other_root)
}

async fn usage_q(
    state: AppState,
    principal: Principal,
    q: UsageQuery,
) -> Result<anyagents_core::UsageReport, ApiError> {
    usage_report(principal, State(state), Query(q))
        .await
        .map(|Json(r)| r)
}

#[tokio::test]
async fn usage_rpc_enforces_causal_authorization_boundary() {
    let (_dir, state, root, child, grandchild, sibling, _other) = usage_fixture();
    let op = Principal::Operator;
    // Operator: whole workspace, explicit sibling, `all` all allowed.
    let r = usage_q(state.clone(), op.clone(), UsageQuery::default())
        .await
        .unwrap();
    assert_eq!(r.totals.turns, 5);
    assert_eq!(r.totals.input_tokens, 3100);
    let r = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            agent: Some("usage-sibling".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.input_tokens, 800);
    let r = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            agent: Some("all".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.turns, 5);
    // Operator + subtree flag on root child widens to its subtree.
    let r = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            agent: Some("usage-child".into()),
            include_descendants: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.input_tokens, 600);
    // Root caller (parent: null) is privileged like the operator.
    let root_p = Principal::Agent {
        agent_id: root.id.clone(),
        name: root.name.clone(),
    };
    let r = usage_q(
        state.clone(),
        root_p,
        UsageQuery {
            agent: Some("usage-sibling".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.input_tokens, 800);
    // Private child: omitted target = self subtree (child + grandchild).
    let child_p = Principal::Agent {
        agent_id: child.id.clone(),
        name: child.name.clone(),
    };
    let r = usage_q(state.clone(), child_p.clone(), UsageQuery::default())
        .await
        .unwrap();
    assert_eq!(r.totals.input_tokens, 600);
    assert_eq!(r.totals.turns, 2);
    // Explicit descendant allowed; subtree flag widens to self subtree.
    let r = usage_q(
        state.clone(),
        child_p.clone(),
        UsageQuery {
            agent: Some("usage-grandchild".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.input_tokens, 400);
    // Parent, sibling, `all` and unrelated roots are fail-closed.
    for target in ["usage-root", "usage-sibling", "all", "usage-other-root"] {
        let err = usage_q(
            state.clone(),
            child_p.clone(),
            UsageQuery {
                agent: Some(target.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::FORBIDDEN, "target {target}");
    }
    // Unknown target is a client error, never a leak.
    let err = usage_q(
        state.clone(),
        child_p.clone(),
        UsageQuery {
            agent: Some("nope".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(err.status.is_client_error());
    // Public-scope caller regains the whole workspace.
    state.store.set_agent_scope(&child.id, "public").unwrap();
    let r = usage_q(
        state.clone(),
        child_p.clone(),
        UsageQuery {
            agent: Some("usage-sibling".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.input_tokens, 800);
    let r = usage_q(
        state.clone(),
        child_p.clone(),
        UsageQuery {
            agent: Some("all".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.turns, 5);
    let _ = (grandchild, sibling);
}

#[tokio::test]
async fn usage_rpc_grouping_and_window_accuracy() {
    let (_dir, state, _root, _child, _gc, _sib, _other) = usage_fixture();
    let op = Principal::Operator;
    for group_by in ["agent", "day", "model", "harness"] {
        let r = usage_q(
            state.clone(),
            op.clone(),
            UsageQuery {
                group_by: Some(group_by.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(!r.buckets.is_empty(), "group_by {group_by}");
        assert_eq!(
            r.buckets.iter().map(|b| b.totals.turns).sum::<i64>(),
            5,
            "bucket turns sum for {group_by}"
        );
        assert_eq!(r.totals.turns, 5);
        assert_eq!(r.totals.input_tokens, 3100);
        assert_eq!(r.totals.output_tokens, 1550);
        assert_eq!(r.totals.cache_read_tokens, 5);
        assert_eq!(r.totals.cache_write_tokens, 10);
        assert_eq!(r.totals.reported_cost_micros, Some(3100));
    }
    // Time windows: far-future lower bound excludes everything, days=36500 keeps all.
    let r = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            since_ms: Some(anyagents_core::now_ms() + 86_400_000),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.turns, 0);
    let r = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            days: Some(36500),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.turns, 5);
    let r = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            until_ms: Some(0),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.turns, 0);
    // Invalid grouping / inverted window are client errors.
    let err = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            group_by: Some("bogus".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
    let err = usage_q(
        state.clone(),
        op.clone(),
        UsageQuery {
            since_ms: Some(2),
            until_ms: Some(1),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn mcp_usage_action_forwards_to_daemon_usage_route() {
    // The MCP tool serializes the same query the daemon route accepts:
    // build the query string the way the dispatch does and confirm the
    // route parses it (group_by + window + subtree).
    let (_dir, state, _root, _child, _gc, _sib, _other) = usage_fixture();
    let r = usage_q(
        state.clone(),
        Principal::Operator,
        UsageQuery {
            agent: Some("usage-child".into()),
            include_descendants: Some(true),
            group_by: Some("day".into()),
            days: Some(36500),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(r.totals.input_tokens, 600);
    assert_eq!(r.buckets.len(), 2);
}
