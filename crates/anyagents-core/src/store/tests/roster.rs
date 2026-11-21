use super::super::*;
use super::helpers::{agent, setup};

#[test]
fn batch_metadata_matches_single_agent_queries() {
    let (_dir, store) = setup();
    let own = agent(&store, "before-rename");
    let own = store.rename_agent(&own.id, "after-rename").unwrap();
    for i in 0..3 {
        store
            .receive(&own.id, Incoming::new(format!("batch {i}")))
            .unwrap();
    }
    let metadata = store.agent_query_metadata().unwrap();
    assert_eq!(
        metadata.aliases.get(own.id.as_str()).unwrap(),
        &store.agent_aliases(&own.id).unwrap()
    );
    assert_eq!(
        metadata
            .oldest_undelivered
            .get(own.id.as_str())
            .map(|t| 123456789_i64.saturating_sub(*t).max(0)),
        store
            .lag(&own.id, 123456789)
            .unwrap()
            .oldest_undelivered_age_ms
    );
    assert_eq!(
        metadata.open_turns.contains_key(own.id.as_str()),
        store.has_open_turn(&own.id).unwrap()
    );
    // Batched lag must skip passive reactions just like the single-agent path.
    store
        .conn()
        .execute(
            "UPDATE messages SET context = ?1 WHERE agent_id = ?2 AND seq = 1",
            params![r#"{"event":"reaction_passive"}"#, own.id.as_str()],
        )
        .unwrap();
    let now = crate::now_ms();
    let metadata = store.agent_query_metadata().unwrap();
    assert_eq!(
        metadata
            .oldest_undelivered
            .get(own.id.as_str())
            .map(|t| now.saturating_sub(*t).max(0)),
        store.lag(&own.id, now).unwrap().oldest_undelivered_age_ms
    );
    for seq in 1..=3 {
        store.note_delivered(&own.id, seq).unwrap();
    }
    let metadata = store.agent_query_metadata().unwrap();
    assert!(!metadata.oldest_undelivered.contains_key(own.id.as_str()));
    assert!(metadata.open_turns.contains_key(own.id.as_str()));
    store.note_turn_completed(&own.id).unwrap();
    assert!(
        !store
            .agent_query_metadata()
            .unwrap()
            .open_turns
            .contains_key(own.id.as_str())
    );
    store.set_agent_state(&own.id, "archived").unwrap();
    assert!(
        !store
            .agent_query_metadata()
            .unwrap()
            .oldest_undelivered
            .contains_key(own.id.as_str())
    );
}

fn child(store: &Store, name: &str, parent: &Agent) -> Agent {
    let mut new = NewAgent::new(name, "claude-code");
    new.parent_id = Some(parent.id.clone());
    store.create_agent(new).unwrap()
}

fn names(agents: &[Agent]) -> Vec<&str> {
    agents.iter().map(|a| a.name.as_str()).collect()
}

#[test]
fn roster_agents_reads_live_agents_and_their_ancestors_but_not_the_archive() {
    let (_dir, store) = setup();
    let root = agent(&store, "root");
    let old = child(&store, "old", &root);
    let live = child(&store, "live", &old);
    let gone = child(&store, "gone", &root);
    let paused = agent(&store, "paused");
    store.set_agent_state(&old.id, "archived").unwrap();
    store.set_agent_state(&gone.id, "archived").unwrap();
    store.set_agent_state(&paused.id, "paused").unwrap();
    let _ = live;

    // Default: every non-archived agent plus the archived ancestor on a
    // live agent's path; the unrelated archived leaf is never read.
    let (agents, archived) = store.roster_agents(None).unwrap();
    assert_eq!(names(&agents), ["live", "old", "paused", "root"]);
    assert_eq!(archived, 2);

    let (agents, _) = store.roster_agents(Some("paused")).unwrap();
    assert_eq!(names(&agents), ["paused"]);
    let (agents, _) = store.roster_agents(Some("archived")).unwrap();
    assert_eq!(names(&agents), ["gone", "old"]);
    let (agents, archived) = store.roster_agents(Some("all")).unwrap();
    assert_eq!(agents, store.list_agents().unwrap());
    assert_eq!(archived, 2);
    let (agents, _) = store.roster_agents(Some("bogus")).unwrap();
    assert!(agents.is_empty());
}

#[test]
fn roster_agents_seeds_from_indexes_instead_of_scanning_agent_rows() {
    let (_dir, store) = setup();
    let conn = store.conn();
    let plan = |sql: &str| -> String {
        conn.db_query_vec(&format!("EXPLAIN QUERY PLAN {sql}"), &[], |r| {
            r.get::<String>(3)
        })
        .unwrap()
        .join("\n")
    };
    let count = plan("SELECT count(*) FROM agents WHERE state = 'archived'");
    assert!(count.contains("COVERING INDEX idx_agents_state"), "{count}");
    let live =
        plan("SELECT id FROM agents WHERE state != 'archived' AND (NULL IS NULL OR state = NULL)");
    assert!(live.contains("idx_agents_live_name"), "{live}");
}

#[test]
fn hot_path_queries_use_their_indexes() {
    let (_dir, store) = setup();
    let conn = store.read_conn();
    let plan = |sql: &str| -> String {
        conn.db_query_vec(&format!("EXPLAIN QUERY PLAN {sql}"), &[], |r| {
            r.get::<String>(3)
        })
        .unwrap()
        .join("\n")
    };
    let active = plan(concat!(
        "SELECT ",
        schedule_cols!(),
        " FROM agent_schedules WHERE state = 'active' ORDER BY created_at_ms ASC, id ASC"
    ));
    assert!(active.contains("idx_agent_schedules_active"), "{active}");
    assert!(!active.contains("TEMP B-TREE"), "{active}");
    let skipped = plan(
        "SELECT id FROM agent_schedules WHERE kind = 'at' AND state = 'fired'
           AND last_fired_at_ms IS NULL AND last_observed LIKE 'skipped:%'
           AND next_fire_at_ms >= 1",
    );
    assert!(
        skipped.contains("idx_agent_schedules_kind_state"),
        "{skipped}"
    );
    let children = plan(concat!(
        "SELECT ",
        agent_cols!(),
        " FROM agents WHERE parent_id = 'x' AND state != 'archived'
         ORDER BY created_at_ms, name"
    ));
    assert!(children.contains("idx_agents_live_parent"), "{children}");
    assert!(!children.contains("TEMP B-TREE"), "{children}");
}

#[test]
fn narrow_agent_reads_match_the_full_rows() {
    let (_dir, store) = setup();
    let root = agent(&store, "root");
    let live = child(&store, "live", &root);
    let gone = child(&store, "gone", &root);
    let paused = child(&store, "paused", &root);
    store.set_agent_state(&gone.id, "archived").unwrap();
    store.set_agent_state(&paused.id, "paused").unwrap();
    store.set_agent_running(&live.id, "/work").unwrap();

    assert_eq!(
        names(&store.live_children_of(&root.id).unwrap()),
        ["live", "paused"]
    );
    let children = store.children_of(&root.id).unwrap();
    let mut all_children = names(&children);
    all_children.sort_unstable();
    assert_eq!(all_children, ["gone", "live", "paused"]);
    assert!(store.live_children_of(&live.id).unwrap().is_empty());

    assert_eq!(
        store.agent_ids_in_state("running").unwrap(),
        std::slice::from_ref(&live.id)
    );
    assert_eq!(
        store.agent_ids_in_state("archived").unwrap(),
        std::slice::from_ref(&gone.id)
    );

    let full = store.list_agents().unwrap();
    let all = store.agent_summaries(true).unwrap();
    assert_eq!(all.len(), full.len());
    for (summary, agent) in all.iter().zip(&full) {
        assert_eq!(
            summary,
            &AgentSummary {
                id: agent.id.clone(),
                name: agent.name.clone(),
                harness: agent.harness.clone(),
                runtime: agent.runtime.clone(),
                remote_budget_micros: agent.remote_budget_micros,
                model: agent.model.clone(),
                state: agent.state.clone(),
                parent_id: agent.parent_id.clone(),
                role: agent.role.clone(),
                created_at_ms: agent.created_at_ms,
            }
        );
    }
    let live_names: Vec<_> = store
        .agent_summaries(false)
        .unwrap()
        .into_iter()
        .map(|a| a.name)
        .collect();
    assert_eq!(live_names, ["live", "paused", "root"]);
}

#[test]
fn roster_agents_survives_a_parent_cycle() {
    let (_dir, store) = setup();
    let a = agent(&store, "a");
    let b = child(&store, "b", &a);
    store
        .conn()
        .execute(
            "UPDATE agents SET parent_id = ?1 WHERE id = ?2",
            params![b.id.as_str(), a.id.as_str()],
        )
        .unwrap();
    let (agents, _) = store.roster_agents(None).unwrap();
    assert_eq!(names(&agents), ["a", "b"]);
}
