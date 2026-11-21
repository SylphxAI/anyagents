//! Evaluate watch probes and retain their mailbox wakes.

use super::*;

/// Evaluate a single callback while its caller holds the owner's lifecycle lock.
pub(crate) fn evaluate_locked_watch_schedule(
    store: &Store,
    health: &HealthBoard,
    owner: &Agent,
    sched: &AgentSchedule,
    now: i64,
    slot_now: i64,
) -> (bool, Option<i64>) {
    evaluate_probe(store, health, sched, now, Some((owner, slot_now)))
}

#[cfg(test)]
pub(super) fn evaluate_single_watch_schedule(
    store: &Store,
    health: &HealthBoard,
    sched: &AgentSchedule,
    now: i64,
) -> (bool, Option<i64>) {
    evaluate_probe(store, health, sched, now, None)
}

fn evaluate_probe(
    store: &Store,
    health: &HealthBoard,
    sched: &AgentSchedule,
    now: i64,
    owner: Option<(&Agent, i64)>,
) -> (bool, Option<i64>) {
    let mut entry = Entry {
        sched: sched.clone(),
        watch: parse_watch_spec(&sched.spec).ok(),
        check_at: i64::MAX,
        idle_target: None,
        local_file: false,
    };
    let out = evaluate_watch_entry(
        store,
        health,
        &mut entry,
        false,
        &HashSet::new(),
        now,
        owner,
    );
    (
        out.fired,
        (entry.check_at < i64::MAX).then_some(entry.check_at),
    )
}

/// When a watch is probed next after a look at `now`.
pub(super) fn watch_probe_at(
    watch: Option<&WatchTarget>,
    event_driven: bool,
    local_fallback: &HashSet<WatchTarget>,
    now: i64,
) -> i64 {
    match watch {
        // A spec never changes, and webhook watches are push-triggered only.
        None | Some(WatchTarget::Webhook { .. }) => i64::MAX,
        Some(target @ (WatchTarget::GitHubPr { .. } | WatchTarget::GitHubCheck { .. })) => {
            github::next_probe_at(target, now).max(now.saturating_add(RETRY_MS))
        }
        Some(WatchTarget::AgentIdle { .. }) => {
            if event_driven {
                i64::MAX
            } else {
                now.saturating_add(LOCAL_WATCH_PROBE_MS)
            }
        }
        Some(target @ (WatchTarget::PathChanged { .. } | WatchTarget::ProcessExit { .. })) => {
            if event_driven && !local_fallback.contains(target) {
                i64::MAX
            } else {
                now.saturating_add(LOCAL_WATCH_PROBE_MS)
            }
        }
    }
}

pub(super) fn evaluate_watch_entry(
    store: &Store,
    health: &HealthBoard,
    entry: &mut Entry,
    event_driven: bool,
    local_fallback: &HashSet<WatchTarget>,
    now: i64,
    locked_owner: Option<(&Agent, i64)>,
) -> EntryOutcome {
    let (eval, next_probe) = if let Some(WatchTarget::PathChanged { path }) = &entry.watch {
        let (eval, local) = file::evaluate(
            store,
            &entry.sched,
            path,
            now,
            locked_owner.map(|(agent, _)| agent),
        );
        let next = if local {
            watch_probe_at(entry.watch.as_ref(), event_driven, local_fallback, now)
        } else {
            now.saturating_add(file::PROBE_INTERVAL_MS)
        };
        (eval, next)
    } else {
        (
            evaluate_watch(
                store,
                health,
                &entry.sched,
                entry.watch.as_ref(),
                &mut entry.idle_target,
                now,
            ),
            watch_probe_at(entry.watch.as_ref(), event_driven, local_fallback, now),
        )
    };
    entry.check_at = match locked_owner {
        Some((_, slot_now)) if next_probe < i64::MAX => next_probe.max(slot_now.saturating_add(1)),
        _ => next_probe,
    };
    let recheck_at = now.saturating_add(LOCAL_WATCH_PROBE_MS);
    let sched = &entry.sched;
    let changed = eval
        .baseline
        .as_ref()
        .is_some_and(|b| Some(b) != sched.watch_baseline.as_ref())
        || eval
            .observed
            .as_ref()
            .is_some_and(|o| Some(o) != sched.last_observed.as_ref());
    if !eval.triggered && !changed {
        return EntryOutcome::default();
    }
    let loaded;
    let agent = if let Some((owner, _)) = locked_owner {
        owner
    } else {
        loaded = match schedule_owner(store, sched) {
            Owner::Live(agent) => agent,
            Owner::Gone => return cancel_for_gone_owner(store, sched),
            Owner::Unknown => {
                entry.check_at = entry.check_at.min(recheck_at);
                return EntryOutcome::default();
            }
        };
        &loaded
    };
    if !eval.triggered {
        if let Err(err) = store.update_schedule_watch_observation(
            &sched.id,
            eval.baseline.as_deref(),
            eval.observed.as_deref(),
        ) {
            eprintln!("Schedule {} observation write failed: {err}", sched.id);
        }
        return EntryOutcome::WROTE;
    }
    let Some(previous_pending) = previous_wake_pending(store, agent, sched) else {
        entry.check_at = entry.check_at.min(recheck_at);
        return EntryOutcome::default();
    };
    if previous_pending {
        // Record the latest observation without consuming the transition or
        // moving the baseline; it is written once the pending wake is taken.
        entry.check_at = entry.check_at.min(recheck_at);
        if eval.observed.is_some() && eval.observed != sched.last_observed {
            if let Err(err) = store.update_schedule_watch_observation(
                &sched.id,
                sched.watch_baseline.as_deref(),
                eval.observed.as_deref(),
            ) {
                eprintln!("Schedule {} observation write failed: {err}", sched.id);
            }
            return EntryOutcome::WROTE;
        }
        return EntryOutcome::default();
    }
    let (wake_prompt, source_key) = if let Some(ref tr) = eval.transition {
        (
            format!("[watch: {} transition={tr}]\n{}", sched.spec, sched.prompt),
            format!("schedule:{}:watch:{tr}:{now}", sched.id),
        )
    } else {
        (sched.prompt.clone(), format!("schedule:{}:watch", sched.id))
    };
    let fired = claim_and_deliver_scheduled_wake(
        store,
        agent,
        sched,
        sched.next_fire_at_ms,
        eval.next_state,
        if eval.next_state != "active" {
            None
        } else if locked_owner.is_some() && entry.check_at < i64::MAX {
            // A non-terminal transition and its next probe share one claim.
            Some(entry.check_at)
        } else {
            sched.next_fire_at_ms
        },
        eval.baseline.as_deref(),
        eval.observed.as_deref(),
        &wake_prompt,
        &source_key,
        now,
        locked_owner.is_some(),
    );
    if !fired {
        entry.check_at = entry.check_at.min(now.saturating_add(RETRY_MS));
    }
    EntryOutcome { fired, wrote: true }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn claim_and_deliver_scheduled_wake(
    store: &Store,
    agent: &Agent,
    sched: &AgentSchedule,
    expected_due: Option<i64>,
    next_state: &str,
    next_due: Option<i64>,
    baseline: Option<&str>,
    observed: Option<&str>,
    prompt: &str,
    source_key: &str,
    now: i64,
    lifecycle_held: bool,
) -> bool {
    let result = (|| -> Result<bool, anyagents_core::Error> {
        // Fresh lifecycle and external retirement are serialized with archive.
        let (_lifecycle, fresh_agent) = if lifecycle_held {
            (None, None)
        } else {
            let Ok(lifecycle) = store.try_lock_agent_lifecycle(&agent.id) else {
                return Ok(false);
            };
            (Some(lifecycle), Some(store.agent(&agent.id)?))
        };
        let agent = fresh_agent.as_ref().unwrap_or(agent);
        if agent.state == "archived"
            || crate::server::is_explicitly_archived(store.data_dir(), agent)
        {
            return Ok(false);
        }
        let delivered = store.claim_schedule_tick_with_wake(
            &sched.id,
            expected_due,
            next_state,
            next_due,
            Some(now),
            baseline,
            observed,
            &agent.id,
            Incoming {
                text: prompt.into(),
                sender: Some(schedule_sender(&sched.id)),
                source_time_ms: Some(now),
                source_key: Some(source_key.into()),
                reply_to: None,
                quote: None,
                origin: None,
                received_at_ms: Some(now),
            },
        )?;
        if delivered {
            crate::create::poke_agent_wake_socket(store.data_dir(), &agent.id);
            crate::create::poke_agent_wake_socket_mail(store.data_dir(), &agent.id);
        }
        Ok(delivered)
    })();
    match result {
        Ok(delivered) => delivered,
        Err(err) => {
            eprintln!("Schedule {} not consumed: {err}", sched.id);
            false
        }
    }
}
