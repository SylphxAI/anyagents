//! Add the recipient's current organization context only to fresh wake input.
use anyagents_core::{AgentId, Store};
use anyagents_harness::{Harness, OutboundMessage, TurnState};

pub(super) fn enrich(
    store: &Store,
    agent: &AgentId,
    harness: &impl Harness,
    message: &mut OutboundMessage,
) {
    let health = harness.health();
    if !health.process_alive
        || health.turn_state != TurnState::Idle
        || !harness.input_ready()
        || harness.pending_input_owner().is_some()
        || harness.proven(&message.id).is_some()
    {
        return;
    }
    message.context.org_instructions = store.knowledge_wake_context(agent).ok().flatten();
}
