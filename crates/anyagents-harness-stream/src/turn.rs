use anyagents_harness::TurnUsage;

use super::State;

/// The turn ended: its inputs are finished, the first carrying its usage. An
/// Antigravity turn that closed took the line that opened it; a message its
/// cascade's mailbox still holds was not taken by it and finishes nothing.
pub(super) fn finish_open(state: &mut State, usage: impl FnOnce(&mut State) -> TurnUsage) {
    state.turn_user_inputs = 0;
    state.last_turn_step_type = None;
    state.idle_since = None;
    state.agy_turn_has_work = false;
    if state.open.is_empty() {
        return;
    }
    let mut usage = Some(usage);
    for id in std::mem::take(&mut state.open) {
        if state.agy_queued.iter().any(|queued| queued.id == id)
            || (state
                .agy_steer
                .as_ref()
                .is_some_and(|steer| steer.id == id && steer.outcome.is_none())
                && !state.agy_taken.contains(&id))
        {
            continue;
        }
        if state.seen.contains(&id) {
            state.agy_taken.insert(id.clone());
        }
        state.finished.insert(id.clone());
        let item_usage = usage.take().map(|usage| usage(state)).unwrap_or_default();
        state.usages.push((id, item_usage));
    }
    // Follow-up lines each open a separate turn. Do not treat them
    // as steers into the next turn and finish them all on one result.
    if let Some(next) = state.followups.pop_front() {
        state.turns_started += 1;
        state.open.push(next);
    }
}
