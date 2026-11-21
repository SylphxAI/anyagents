//! Run against a dedicated PostgreSQL 16+ database with pg_stat_statements
//! preloaded and CREATE EXTENSION pg_stat_statements applied there.
use std::process::Command;
use std::sync::{Arc, Barrier};

use anyagents_core::{AgentId, NewAgent, Store};
use serde_json::json;

use super::super::build_router;
use super::helpers::{env_lock, raw_http, state_for_store};

const POOL_MAX: &str = "ANYAGENTS_PG_MAX_CONNECTIONS";
const POOL_TIMEOUT: &str = "ANYAGENTS_PG_ACQUIRE_TIMEOUT_SECS";

fn statements(url: &str) -> i64 {
    let output = Command::new("psql")
        .args([
            "-XAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "SELECT COALESCE(sum(calls), 0)::bigint FROM pg_stat_statements
             WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
               AND query NOT LIKE '%FROM pg_stat_statements%'",
            url,
        ])
        .output()
        .expect("psql is required for the PostgreSQL budget check");
    assert!(
        output.status.success(),
        "cannot count statements: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

#[test]
fn postgres_coordinator_hot_paths_stay_within_sql_budgets_and_handle_burst() {
    let Ok(url) = std::env::var("ANYAGENTS_HOT_PATH_TEST_POSTGRES_URL") else {
        eprintln!("skipping: ANYAGENTS_HOT_PATH_TEST_POSTGRES_URL is not set");
        return;
    };
    // Run in a fresh process so pool env settings and the runtime's timers do
    // not interfere with other fixtures or count their statements.
    let _env = env_lock();
    if super::helpers::single_test_worker(
        module_path!(),
        "postgres_coordinator_hot_paths_stay_within_sql_budgets_and_handle_burst",
    ) {
        return;
    }
    // SAFETY: the isolated worker has not yet constructed a Store or runtime.
    unsafe {
        std::env::set_var(POOL_MAX, "4");
        std::env::set_var(POOL_TIMEOUT, "3");
        std::env::remove_var("ANYAGENTS_SERVERLESS");
    }
    let dir = crate::test_tempdir();
    let mut state = state_for_store(Store::open_postgres(&url, dir.path()).unwrap());
    state.auth = crate::auth::Mode::Confined {
        operator_digest: None,
    };
    let prefix = format!("burst-{}", AgentId::generate().as_str());
    let parent = state
        .store
        .create_agent(NewAgent::new(format!("{prefix}-parent"), "test"))
        .unwrap();
    state.store.set_agent_scope(&parent.id, "isolated").unwrap();
    let capability = state.store.issue_capability(&parent.id).unwrap();
    let headers = [(crate::operator::AGENT_HEADER, capability.as_str())];
    let mut recipients = Vec::new();
    for i in 0..32 {
        let mut new = NewAgent::new(format!("{prefix}-child-{i}"), "test");
        new.parent_id = Some(parent.id.clone());
        let child = state.store.create_agent(new).unwrap();
        state.store.set_agent_state(&child.id, "running").unwrap();
        recipients.push(child);
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let listener = rt
        .block_on(async { tokio::net::TcpListener::bind("127.0.0.1:0").await })
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let router = build_router(state.clone());
    let server = rt.spawn(async move { axum::serve(listener, router).await.unwrap() });
    let roster = || raw_http(addr, "GET", "/v1/agents", &headers, "");
    let send = |i: usize, suffix: &str| {
        raw_http(addr, "POST", "/v1/send", &headers,
        &json!({"to": [format!("agent:{}", recipients[i].name)], "text": format!("message {i} {suffix}")}).to_string())
    };
    // Warm the common statement shapes and verify the actual HTTP DTOs before
    // counting. Count successful executions (including transaction control if
    // tracked by PostgreSQL), not parse/describe or protocol ping packets.
    let (status, body) = roster();
    assert_eq!(status, 200, "warm roster: {body}");
    let (status, body) = send(0, "warm");
    assert_eq!(status, 200, "warm send: {body}");
    let before = statements(&url);
    let (status, body) = roster();
    let after_roster = statements(&url);
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["agents"].as_array().unwrap().len(),
        recipients.len() + 1
    );
    let (status, body) = send(0, "budget");
    let after_send = statements(&url);
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["deliveries"].as_array().unwrap().len(), 1);
    let roster_sql = after_roster - before;
    let send_sql = after_send - after_roster;
    eprintln!("SQL executions: GET /v1/agents = {roster_sql}, POST /v1/send = {send_sql}");
    assert!(
        (1..=8).contains(&roster_sql),
        "roster SQL budget: {roster_sql}"
    );
    assert!((1..=33).contains(&send_sql), "send SQL budget: {send_sql}");
    let gate = Arc::new(Barrier::new(64));
    std::thread::scope(|scope| {
        let handles = (0..64)
            .map(|i| {
                let gate = Arc::clone(&gate);
                let (roster, send) = (&roster, &send);
                scope.spawn(move || {
                    gate.wait();
                    let (status, body) = if i < 32 {
                        roster()
                    } else {
                        send(i - 32, "burst")
                    };
                    assert_eq!(status, 200, "request {i}: {body}");
                    let key = if i < 32 { "agents" } else { "deliveries" };
                    assert!(!body[key].as_array().unwrap().is_empty());
                })
            })
            .collect::<Vec<_>>();
        for h in handles {
            h.join().unwrap();
        }
    });
    for (i, agent) in recipients.iter().enumerate() {
        let inbox = state.store.inbox(&agent.id, None).unwrap();
        assert_eq!(inbox.len(), if i == 0 { 3 } else { 1 });
        assert!(inbox.iter().any(|m| m.text == format!("message {i} burst")));
    }
    server.abort();
    for agent in recipients.iter().chain(std::iter::once(&parent)) {
        state.store.delete_agent(&agent.id).unwrap();
    }
}
