use super::super::*;
use super::helpers::{agent, setup};
use crate::id::UsageId;

#[test]
fn usage_round_trips() {
    let (_dir, store) = setup();
    let agent = agent(&store, "alpha");
    let record = store
        .record_usage(NewUsage {
            agent_id: agent.id.clone(),
            harness: "claude-code".into(),
            model: Some("model".into()),
            input_tokens: 3,
            output_tokens: 4,
            cache_read_tokens: 1,
            cache_write_tokens: 0,
            reported_cost_micros: None,
            estimated_cost_micros: Some(12),
            billing_mode: BillingMode::Subscription,
            quota_state: None,
            day: "2026-10-18".into(),
        })
        .unwrap();
    assert_eq!(record.input_tokens, 3);
    assert_eq!(store.list_usage(Some(&agent.id)).unwrap(), vec![record]);
}

fn usage_record(agent_id: &AgentId, at_ms: i64, day: &str, model: &str) -> UsageRecord {
    UsageRecord {
        id: UsageId::generate(),
        agent_id: agent_id.clone(),
        harness: "harness-a".into(),
        model: Some(model.into()),
        input_tokens: 10,
        output_tokens: 20,
        cache_read_tokens: 3,
        cache_write_tokens: 4,
        reported_cost_micros: Some(5),
        estimated_cost_micros: None,
        billing_mode: BillingMode::Subscription,
        quota_state: None,
        day: day.into(),
        recorded_at_ms: at_ms,
    }
}

#[test]
fn aggregate_usage_groups_and_windows() {
    let (_dir, store) = setup();
    let alpha = agent(&store, "alpha");
    let beta = agent(&store, "beta");
    let now = 1_000_000_000_000i64;
    let day_ms = 86_400_000i64;
    let records = vec![
        usage_record(&alpha.id, now - day_ms, "2026-10-17", "m1"),
        usage_record(&alpha.id, now, "2026-10-18", "m1"),
        usage_record(&alpha.id, now, "2026-10-18", "m2"),
        usage_record(&beta.id, now, "2026-10-18", "m1"),
    ];
    let names = std::collections::HashMap::from([
        (alpha.id.clone(), alpha.name.clone()),
        (beta.id.clone(), beta.name.clone()),
    ]);
    let ids = vec![alpha.id.clone(), beta.id.clone()];
    // Inclusive window: only the two records at `now`.
    let report = aggregate_usage(
        &records,
        &ids,
        &names,
        UsageFilter {
            since_ms: Some(now),
            until_ms: Some(now),
            ..UsageFilter::default()
        },
        now,
    )
    .unwrap();
    assert_eq!(report.totals.turns, 3);
    assert_eq!(report.totals.input_tokens, 30);
    assert_eq!(report.totals.reported_cost_micros, Some(15));
    // group_by model -> alpha splits into m1/m2, beta is m1.
    let by_model = aggregate_usage(
        &records,
        &ids,
        &names,
        UsageFilter {
            group_by: UsageGroupBy::Model,
            ..UsageFilter::default()
        },
        now,
    )
    .unwrap();
    assert_eq!(by_model.buckets.len(), 3);
    assert_eq!(by_model.buckets[0].model.as_deref(), Some("m1"));
    // group_by day -> alpha has two days.
    let by_day = aggregate_usage(
        &records,
        &ids,
        &names,
        UsageFilter {
            group_by: UsageGroupBy::Day,
            ..UsageFilter::default()
        },
        now,
    )
    .unwrap();
    assert_eq!(by_day.buckets.len(), 3);
    // days=1 keeps everything recorded within the last day (all four).
    let by_days = aggregate_usage(
        &records,
        &ids,
        &names,
        UsageFilter {
            days: Some(1),
            ..UsageFilter::default()
        },
        now,
    )
    .unwrap();
    assert_eq!(by_days.totals.turns, 4);
    // group_by harness collapses to one bucket per agent.
    let by_harness = aggregate_usage(
        &records,
        &ids,
        &names,
        UsageFilter {
            group_by: UsageGroupBy::Harness,
            ..UsageFilter::default()
        },
        now,
    )
    .unwrap();
    assert_eq!(by_harness.buckets.len(), 2);
    assert!(
        by_harness
            .buckets
            .iter()
            .all(|b| b.harness.as_deref() == Some("harness-a"))
    );
    // Bad inputs fail rather than guessing.
    assert!(
        UsageFilter {
            days: Some(0),
            ..UsageFilter::default()
        }
        .window(now)
        .is_err()
    );
    assert!(UsageGroupBy::parse("week").is_err());
}

#[test]
fn resolve_usage_agents_enforces_causal_boundary() {
    let (_dir, store) = setup();
    let root = agent(&store, "root");
    let child_new = {
        let mut n = NewAgent::new("child", "claude-code");
        n.parent_id = Some(root.id.clone());
        n
    };
    let child = store.create_agent(child_new).unwrap();
    let grand_new = {
        let mut n = NewAgent::new("grandchild", "claude-code");
        n.parent_id = Some(child.id.clone());
        n
    };
    let grandchild = store.create_agent(grand_new).unwrap();
    let sib_new = {
        let mut n = NewAgent::new("sibling", "claude-code");
        n.parent_id = Some(root.id.clone());
        n
    };
    let sibling = store.create_agent(sib_new).unwrap();
    let agents = store.list_agents().unwrap();
    let caller = agents.iter().find(|a| a.id == child.id).unwrap();

    // Omitted target: caller's own subtree only.
    let ids = resolve_usage_agents(Some(caller), &agents, None, None).unwrap();
    assert!(ids.contains(&child.id));
    assert!(ids.contains(&grandchild.id));
    assert!(!ids.contains(&root.id));
    assert!(!ids.contains(&sibling.id));
    // Self, child and grandchild are visible; parent and sibling are not.
    for name in ["self", "child", "grandchild"] {
        assert!(
            resolve_usage_agents(Some(caller), &agents, Some(name), None).is_ok(),
            "{name} should be visible"
        );
    }
    for name in ["root", "sibling", "all", "*"] {
        assert!(
            matches!(
                resolve_usage_agents(Some(caller), &agents, Some(name), None),
                Err(UsageAccessError::Forbidden(_))
            ),
            "{name} should be forbidden"
        );
    }
    assert!(matches!(
        resolve_usage_agents(Some(caller), &agents, Some("ghost"), None),
        Err(UsageAccessError::Invalid(_))
    ));
    // Explicit target without the subtree switch is exact-agent only.
    let exact = resolve_usage_agents(Some(caller), &agents, Some("child"), None).unwrap();
    assert_eq!(exact, vec![child.id.clone()]);
    let subtree = resolve_usage_agents(Some(caller), &agents, Some("child"), Some(true)).unwrap();
    assert_eq!(subtree.len(), 2);
    // A public-scope caller and a root caller are privileged.
    store.set_agent_scope(&child.id, "public").unwrap();
    let agents = store.list_agents().unwrap();
    let public_caller = agents.iter().find(|a| a.id == child.id).unwrap();
    let all = resolve_usage_agents(Some(public_caller), &agents, Some("all"), None).unwrap();
    assert_eq!(all.len(), 4);
    assert!(resolve_usage_agents(Some(public_caller), &agents, Some("root"), None).is_ok());
    let root_caller = agents.iter().find(|a| a.id == root.id).unwrap();
    assert_eq!(
        resolve_usage_agents(Some(root_caller), &agents, None, None)
            .unwrap()
            .len(),
        4
    );
    // The local operator (`None`) is privileged too.
    assert_eq!(
        resolve_usage_agents(None, &agents, Some("sibling"), Some(true)).unwrap(),
        vec![sibling.id.clone()]
    );
}

#[test]
fn utc_day_and_usage_sums() {
    assert_eq!(utc_day(0), "1970-01-01");
    assert_eq!(utc_day(1_700_000_000_000), "2023-11-14");
    let alpha = AgentId::generate();
    let beta = AgentId::generate();
    let records = [
        sample_usage(alpha.clone(), "2023-11-14", 1, Some(5)),
        sample_usage(alpha.clone(), "2023-11-15", 4, None),
        sample_usage(beta.clone(), "2023-11-14", 2, Some(7)),
    ];
    let by_agent = sum_usage(&records, false);
    assert_eq!(by_agent.len(), 2);
    let alpha_total = by_agent.iter().find(|row| row.agent_id == alpha).unwrap();
    assert_eq!(alpha_total.day, None);
    assert_eq!(alpha_total.input_tokens, 5);
    assert_eq!(alpha_total.reported_cost_micros, Some(5));
    let by_day = sum_usage(&records, true);
    assert_eq!(by_day.len(), 3);
    assert!(
        by_day
            .iter()
            .any(|row| row.day.as_deref() == Some("2023-11-15")
                && row.reported_cost_micros.is_none())
    );
}

fn sample_usage(agent_id: AgentId, day: &str, input: i64, cost: Option<i64>) -> UsageRecord {
    UsageRecord {
        id: crate::UsageId::generate(),
        agent_id,
        harness: "claude-code".into(),
        model: None,
        input_tokens: input,
        output_tokens: 0,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
        reported_cost_micros: cost,
        estimated_cost_micros: None,
        billing_mode: BillingMode::Subscription,
        quota_state: None,
        day: day.into(),
        recorded_at_ms: 0,
    }
}
