//! Serverless watch & schedule tick evaluation: remote-turn-aware `agent:idle` watches,
//! past-due one-shot `at` catch-up, archived-target watch sweeping, and stale `coalesced`
//! recurring cron recovery.

use std::collections::{HashMap, HashSet};

use anyagents_core::stateless::ACTIVE_REMOTE_TURN_FRESHNESS_MS;
use anyagents_core::{Agent, AgentId, AgentSchedule, Incoming, Store};
use anyagents_harness::TurnState;
use anyagents_supervisor::{HealthBoard, LiveState};

use crate::schedule::{
    WatchTarget, parse_at_ms, parse_watch_spec, schedules_changed, tick_serverless_schedules,
};

/// Build a [`HealthBoard`] overlay projecting fresh active `pending_remote_turns`
/// rows as live `TurnState::InTurn` processes so `agent:idle` watches and the
/// turn-ownership guard see active remote turns accurately in serverless mode.
pub(crate) fn effective_health_with_remote_turns(
    store: &Store,
    base: &HealthBoard,
    now: i64,
) -> (HealthBoard, HashMap<String, i64>) {
    let fresh_since = now.saturating_sub(ACTIVE_REMOTE_TURN_FRESHNESS_MS);
    let active_remote = store
        .remote_turn_store()
        .list_active_fresh_by_agent(fresh_since)
        .unwrap_or_default();
    if active_remote.is_empty() {
        return (base.clone(), active_remote);
    }

    let overlay = HealthBoard::default();
    if let Ok(live_agents) = store.list_live_agents() {
        for agent in &live_agents {
            if let Some(live) = base.get(&agent.id) {
                overlay.publish(&agent.id, live);
            }
        }
    }
    for (aid_str, started_ms) in &active_remote {
        let Ok(aid) = AgentId::parse(aid_str) else {
            continue;
        };
        let already_in_turn = base
            .get(&aid)
            .is_some_and(|l| l.process_alive && l.turn_state == TurnState::InTurn);
        if !already_in_turn {
            overlay.publish(
                &aid,
                LiveState {
                    process_alive: true,
                    turn_state: TurnState::InTurn,
                    delivery: None,
                    quota_reset_at_ms: None,
                    restart_at_ms: None,
                    pid: None,
                    live_harness: None,
                    live_model: None,
                    process_started_at_ms: Some(*started_ms),
                    last_error: None,
                    retry_at_ms: None,
                },
            );
        }
    }
    (overlay, active_remote)
}

/// Cancel every active `agent:idle:<target>` watch targeting `archived` (matched
/// by immutable ID, name, or alias). Called immediately on agent archive.
pub(crate) fn cancel_watches_targeting_agent(store: &Store, archived: &Agent) -> usize {
    let Ok(active_schedules) = store.list_schedules(None, true) else {
        return 0;
    };
    let aliases = store.agent_aliases(&archived.id).unwrap_or_default();
    let mut cancelled = 0usize;
    for sched in active_schedules {
        if sched.kind != "watch" {
            continue;
        }
        let Ok(WatchTarget::AgentIdle { target }) = parse_watch_spec(&sched.spec) else {
            continue;
        };
        let matches_archived = target == archived.id.as_str()
            || target.eq_ignore_ascii_case(&archived.name)
            || aliases.iter().any(|al| target.eq_ignore_ascii_case(al));
        if matches_archived && store.cancel_schedule(None, &sched.id).unwrap_or(0) > 0 {
            cancelled += 1;
        }
    }
    if cancelled > 0 {
        schedules_changed(store.data_dir());
    }
    cancelled
}

/// Sweep all active `agent:idle:<target>` watches whose target agent is already
/// archived or deleted, cancelling them cleanly so dead watches never linger.
pub(crate) fn sweep_watches_targeting_archived_agents(store: &Store) -> usize {
    let Ok(active_schedules) = store.list_schedules(None, true) else {
        return 0;
    };
    let mut target_archived_cache: HashMap<String, bool> = HashMap::new();
    let mut cancelled = 0usize;
    for sched in active_schedules {
        if sched.kind != "watch" {
            continue;
        }
        let Ok(WatchTarget::AgentIdle { target }) = parse_watch_spec(&sched.spec) else {
            continue;
        };
        let cache_key = target.to_ascii_lowercase();
        let is_gone_or_archived = *target_archived_cache.entry(cache_key).or_insert_with(|| {
            match store.agent_by_name(&target) {
                Ok(agent) => {
                    agent.state == "archived"
                        || super::is_explicitly_archived(store.data_dir(), &agent)
                }
                Err(anyagents_core::Error::AgentNotFound(_)) => true,
                Err(_) => false,
            }
        });
        if is_gone_or_archived && store.cancel_schedule(None, &sched.id).unwrap_or(0) > 0 {
            cancelled += 1;
        }
    }
    if cancelled > 0 {
        schedules_changed(store.data_dir());
    }
    cancelled
}

/// Catch up orphaned one-shot `at` schedules stuck `active` with `next_fire_at_ms = NULL`
/// (e.g. from legacy migrations or partial writes) whose spec timestamp has already passed.
pub(crate) fn catch_up_orphaned_at_schedules(store: &Store, now: i64) -> (usize, Vec<AgentId>) {
    let Ok(active_schedules) = store.list_schedules(None, true) else {
        return (0, Vec::new());
    };
    let mut fired = 0usize;
    let mut woken = Vec::new();
    for sched in active_schedules {
        if sched.kind != "at" || sched.next_fire_at_ms.is_some() {
            continue;
        }
        let due_ms = parse_at_ms(&sched.spec, sched.created_at_ms).unwrap_or(sched.created_at_ms);
        if due_ms > now {
            continue;
        }
        let Ok(owner) = store.agent(&sched.agent_id) else {
            let _ = store.cancel_schedule(None, &sched.id);
            continue;
        };
        if owner.state == "archived" || super::is_explicitly_archived(store.data_dir(), &owner) {
            let _ = store.cancel_schedule(None, &sched.id);
            continue;
        }
        let Ok(_lifecycle) = store.try_lock_agent_lifecycle(&owner.id) else {
            continue;
        };
        let source_key = format!("schedule:{}:{due_ms}", sched.id);
        let delivered = store
            .claim_schedule_tick_with_wake(
                &sched.id,
                None,
                "fired",
                None,
                Some(now),
                None,
                Some("fired"),
                &owner.id,
                Incoming {
                    text: sched.prompt.clone(),
                    sender: Some(format!("schedule:{}", sched.id)),
                    source_time_ms: Some(now),
                    source_key: Some(source_key),
                    reply_to: None,
                    quote: None,
                    origin: None,
                    received_at_ms: Some(now),
                },
            )
            .unwrap_or(false);
        if delivered {
            crate::create::poke_agent_wake_socket(store.data_dir(), &owner.id);
            crate::create::poke_agent_wake_socket_mail(store.data_dir(), &owner.id);
            fired += 1;
            if !woken.contains(&owner.id) {
                woken.push(owner.id);
            }
        }
    }
    if fired > 0 {
        schedules_changed(store.data_dir());
    }
    (fired, woken)
}

/// Unstick active recurring `every` schedules whose `last_observed` is `"coalesced"`
/// once their owner agent is idle (no active remote turn, not paused/archived):
/// - If the previous cron wake is still undelivered in the idle owner's mailbox,
///   surface the owner in `woken` so the tick immediately dispatches it.
/// - If the previous wake was already consumed (`!pending_prev`) yet the schedule
///   remained marked `coalesced` past its interval, fire a catch-up tick immediately
///   and advance `next_fire_at_ms` so recurring crons never stall for hours.
pub(crate) fn unstick_stale_coalesced_crons(
    store: &Store,
    active_remote: &HashMap<String, i64>,
    now: i64,
) -> (usize, Vec<AgentId>) {
    let Ok(active_schedules) = store.list_schedules(None, true) else {
        return (0, Vec::new());
    };
    let mut fired = 0usize;
    let mut woken = Vec::new();
    for sched in active_schedules {
        if sched.kind != "every" || sched.last_observed.as_deref() != Some("coalesced") {
            continue;
        }
        if active_remote.contains_key(sched.agent_id.as_str()) {
            continue;
        }
        let Ok(owner) = store.agent(&sched.agent_id) else {
            continue;
        };
        if matches!(owner.state.as_str(), "paused" | "archived")
            || super::is_explicitly_archived(store.data_dir(), &owner)
        {
            continue;
        }
        let sender = format!("schedule:{}", sched.id);
        let prev_pending = store
            .has_undelivered_message_from(&owner.id, &sender)
            .unwrap_or(false);
        if prev_pending {
            if !woken.contains(&owner.id) {
                woken.push(owner.id);
            }
            continue;
        }
        let interval = sched.interval_ms.unwrap_or(60_000).max(1_000);
        let reference_ts = sched.last_fired_at_ms.unwrap_or(sched.created_at_ms);
        if now.saturating_sub(reference_ts) < interval {
            continue;
        }
        if fire_coalesced_catchup_tick(store, &owner, &sched, interval, now) {
            fired += 1;
            if !woken.contains(&owner.id) {
                woken.push(owner.id);
            }
        }
    }
    if fired > 0 {
        schedules_changed(store.data_dir());
    }
    (fired, woken)
}

fn fire_coalesced_catchup_tick(
    store: &Store,
    owner: &Agent,
    sched: &AgentSchedule,
    interval: i64,
    now: i64,
) -> bool {
    let Ok(_lifecycle) = store.try_lock_agent_lifecycle(&owner.id) else {
        return false;
    };
    let next_due = now.checked_add(interval);
    let source_key = format!("schedule:{}:catchup:{now}", sched.id);
    let delivered = store
        .claim_schedule_tick_with_wake(
            &sched.id,
            sched.next_fire_at_ms,
            "active",
            next_due,
            Some(now),
            None,
            Some("fired"),
            &owner.id,
            Incoming {
                text: sched.prompt.clone(),
                sender: Some(format!("schedule:{}", sched.id)),
                source_time_ms: Some(now),
                source_key: Some(source_key),
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: Some(now),
            },
        )
        .unwrap_or(false);
    if delivered {
        crate::create::poke_agent_wake_socket(store.data_dir(), &owner.id);
        crate::create::poke_agent_wake_socket_mail(store.data_dir(), &owner.id);
    }
    delivered
}

/// Bounded serverless tick pass evaluating watches (with live remote-turn awareness),
/// archived-target watch cleanup, orphaned `at` catch-up, due timers/crons, and
/// stale `coalesced` cron recovery.
pub(crate) fn tick_serverless_watches_and_schedules(
    store: &Store,
    health: &HealthBoard,
    now: i64,
) -> (usize, Vec<AgentId>) {
    let _ = sweep_watches_targeting_archived_agents(store);
    let (at_fired, at_woken) = catch_up_orphaned_at_schedules(store, now);
    let (effective_health, active_remote) = effective_health_with_remote_turns(store, health, now);
    let (sched_fired, sched_woken) = tick_serverless_schedules(store, &effective_health, now);
    let (cron_fired, cron_woken) = unstick_stale_coalesced_crons(store, &active_remote, now);

    let mut seen = HashSet::new();
    let mut woken = Vec::new();
    for aid in at_woken.into_iter().chain(sched_woken).chain(cron_woken) {
        if seen.insert(aid.clone()) {
            woken.push(aid);
        }
    }
    (at_fired + sched_fired + cron_fired, woken)
}

/// Advance the durable mailbox delivery cursor and turn ledger on remote turn
/// settlement, accounting for any newer messages read via mid-turn `inbox()`.
/// Never advances `delivered_seq` on failed/cancelled turns so unread mail is
/// preserved across crashes.
pub(crate) fn settle_turn_mailbox_cursor(
    store: &Store,
    turn: &anyagents_core::stateless::PendingRemoteTurn,
) {
    let msg_seq = turn
        .metadata_json
        .as_ref()
        .and_then(|m| m.get("message_seq").and_then(serde_json::Value::as_i64));
    let inbox_seq = turn.metadata_json.as_ref().and_then(|m| {
        m.get(anyagents_core::stateless::MAX_INBOX_SEQ_META_KEY)
            .and_then(serde_json::Value::as_i64)
    });
    if turn.status == "completed" {
        if let Some(seq) = msg_seq.into_iter().chain(inbox_seq).max() {
            let _ = store.mark_delivered_up_to(&turn.agent_id, seq);
            let opener = turn
                .metadata_json
                .as_ref()
                .and_then(|m| m.get("opener_id").and_then(serde_json::Value::as_str));
            let _ = store.note_turn_completed_with_output_for(
                &turn.agent_id,
                seq,
                opener.zip(turn.result_text.as_deref()),
            );
        }
    } else if let Some(seq) = msg_seq {
        let err = turn
            .error_text
            .as_deref()
            .unwrap_or("serverless turn failed");
        let _ = store.note_turn_errored_for(&turn.agent_id, seq, err, false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyagents_core::stateless::{NewRemoteTurn, SettleRemoteTurn, hash_callback_token};
    use anyagents_core::{NewAgent, NewSchedule};
    use serde_json::json;

    #[test]
    fn serverless_agent_idle_watch_holds_during_active_remote_turn_and_fires_on_completion() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let empty_health = HealthBoard::default();
        let parent = store
            .create_agent(NewAgent::new("watch-parent", "claude-code"))
            .unwrap();
        let child = store
            .create_agent(NewAgent::new("watch-child", "agy"))
            .unwrap();
        store.set_agent_running(&parent.id, "/work").unwrap();
        store.set_agent_running(&child.id, "/work").unwrap();

        // Child receives its task and runs an active remote turn (no local HealthBoard entry).
        let anyagents_core::ReceiveOutcome::New(task_msg) = store
            .receive(&child.id, Incoming::new("build slice"))
            .unwrap()
        else {
            panic!("expected new task message");
        };
        let turn = store
            .remote_turn_store()
            .create_turn(&NewRemoteTurn {
                turn_id: "turn_watch_1".into(),
                agent_id: child.id.clone(),
                harness_kind: "agy".into(),
                provider_kind: Some("sandbox".into()),
                remote_run_id: Some("run-1".into()),
                callback_token_hash: hash_callback_token("tok-1"),
                cursor: None,
                status: "running".into(),
                metadata_json: Some(json!({ "message_seq": task_msg.seq })),
                next_poll_at_ms: None,
            })
            .unwrap();
        let tick_now = turn.created_at_ms + 8_000;

        // Create `agent:idle:watch-child` while remote turn is active.
        let (eff_health, _) = effective_health_with_remote_turns(&store, &empty_health, tick_now);
        let created = crate::schedule::create_agent_schedule(
            &store,
            &eff_health,
            &parent,
            None,
            None,
            Some("agent:idle:watch-child"),
            "child finished",
        )
        .unwrap();
        assert!(
            created
                .watch_baseline
                .as_deref()
                .unwrap_or("")
                .starts_with("causal:1:"),
            "watch created during active remote turn must latch await_turn=1, got {:?}",
            created.watch_baseline
        );

        // Tick 8s later while remote turn is still active: must NOT false-fire!
        let (fired, woken) = tick_serverless_watches_and_schedules(&store, &empty_health, tick_now);
        assert_eq!(fired, 0);
        assert!(woken.is_empty());
        assert!(store.inbox(&parent.id, None).unwrap().is_empty());

        // Settle remote turn via the real serverless cursor settler -> next tick fires cleanly!
        let settled = store
            .remote_turn_store()
            .settle_turn(
                "turn_watch_1",
                &SettleRemoteTurn {
                    status: "completed".into(),
                    result_text: Some("done".into()),
                    error_text: None,
                    usage_json: None,
                    cursor: None,
                    settled_at_ms: tick_now + 5_000,
                },
            )
            .unwrap()
            .unwrap();
        settle_turn_mailbox_cursor(&store, &settled.turn);

        let (fired2, woken2) =
            tick_serverless_watches_and_schedules(&store, &empty_health, tick_now + 6_000);
        assert_eq!(fired2, 1);
        assert_eq!(woken2, vec![parent.id.clone()]);
        let inbox = store.inbox(&parent.id, None).unwrap();
        assert_eq!(inbox.len(), 1);
        assert!(inbox[0].text.contains("transition=idle"));
        assert!(inbox[0].text.contains("child finished"));
    }

    #[test]
    fn past_due_orphaned_at_schedule_with_null_next_fire_catches_up_and_fires() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("at-owner", "claude-code"))
            .unwrap();
        store.set_agent_running(&owner.id, "/work").unwrap();

        let sched = store
            .create_schedule(NewSchedule {
                agent_id: owner.id.clone(),
                kind: "at".into(),
                spec: "2020-01-01T00:00:00Z".into(),
                prompt: "overdue reminder".into(),
                interval_ms: None,
                next_fire_at_ms: None,
                watch_baseline: None,
            })
            .unwrap();
        assert_eq!(sched.state, "active");
        assert!(sched.next_fire_at_ms.is_none());

        let (fired, woken) =
            tick_serverless_watches_and_schedules(&store, &health, anyagents_core::now_ms());
        assert_eq!(fired, 1);
        assert_eq!(woken, vec![owner.id.clone()]);
        let after = store.get_schedule(&sched.id).unwrap().unwrap();
        assert_eq!(after.state, "fired");
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);
    }

    #[test]
    fn archiving_target_agent_cancels_matching_agent_idle_watches() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let parent = store
            .create_agent(NewAgent::new("parent-arch", "claude-code"))
            .unwrap();
        let child = store
            .create_agent(NewAgent::new("child-arch", "agy"))
            .unwrap();
        store.set_agent_running(&parent.id, "/work").unwrap();
        store.set_agent_running(&child.id, "/work").unwrap();
        let watch = crate::schedule::create_agent_schedule(
            &store,
            &health,
            &parent,
            None,
            None,
            Some("agent:idle:child-arch"),
            "watch child",
        )
        .unwrap();

        let archived_child = store.set_agent_state(&child.id, "archived").unwrap();
        let cancelled = cancel_watches_targeting_agent(&store, &archived_child);
        assert_eq!(cancelled, 1);
        assert_eq!(
            store.get_schedule(&watch.id).unwrap().unwrap().state,
            "cancelled"
        );
    }

    #[test]
    fn stale_coalesced_recurring_cron_unstucks_once_agent_is_idle() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let health = HealthBoard::default();
        let owner = store
            .create_agent(NewAgent::new("cron-owner", "claude-code"))
            .unwrap();
        store.set_agent_running(&owner.id, "/work").unwrap();

        let now = anyagents_core::now_ms();
        let interval = 3_600_000i64;
        let sched = store
            .create_schedule(NewSchedule {
                agent_id: owner.id.clone(),
                kind: "every".into(),
                spec: "1h".into(),
                prompt: "hourly pulse".into(),
                interval_ms: Some(interval),
                next_fire_at_ms: Some(now + 1_800_000),
                watch_baseline: None,
            })
            .unwrap();
        // Simulate a cron that was marked `coalesced` over 2 hours ago while its
        // previous wake has since been delivered.
        store
            .claim_schedule_tick(
                &sched.id,
                Some(now + 1_800_000),
                "active",
                Some(now + 1_800_000),
                Some(now - 2 * interval),
                None,
                Some("coalesced"),
            )
            .unwrap();

        let (fired, woken) = tick_serverless_watches_and_schedules(&store, &health, now);
        assert_eq!(fired, 1);
        assert_eq!(woken, vec![owner.id.clone()]);
        let after = store.get_schedule(&sched.id).unwrap().unwrap();
        assert_eq!(after.last_observed.as_deref(), Some("fired"));
        assert_eq!(store.inbox(&owner.id, None).unwrap().len(), 1);
    }
}
