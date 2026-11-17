//! Offer order is independent of the contiguous mailbox commit order.
use anyagents_core::{AgentId, Message, Store};
use anyagents_harness::{Harness, Proof};

// Bound each snapshot, including its head. Appended mail is considered by a
// later snapshot; no unbounded read or speculative cursor is needed.
pub(crate) const LOOKAHEAD: usize = 256;

pub(crate) fn external(message: &Message) -> bool {
    message.context.sender_rel.as_deref() == Some("external_user")
}

pub(crate) fn background(message: &Message) -> bool {
    message.context.sender_rel.as_deref() == Some("child")
        // This is the system schedule publisher's reserved sender namespace,
        // not text, an agent display name, or a claimed relationship in a body.
        || message.sender.as_deref().and_then(|s| s.strip_prefix("schedule:"))
            .is_some_and(|id| !id.is_empty())
}

/// Cross only known background/external lines. Unknown, parent, operator and
/// peer mail are causal barriers. External lines retain their mailbox order.
pub(crate) fn ahead(
    store: &Store,
    agent: &AgentId,
    head: &Message,
) -> Result<(i64, Vec<Message>), anyagents_core::Error> {
    let mut through = head.seq;
    let mut external_mail = Vec::new();
    if background(head) {
        for _ in 1..LOOKAHEAD {
            let Some(next) = store.undelivered_after(agent, through)? else {
                break;
            };
            if !background(&next) && !external(&next) {
                break;
            }
            through = next.seq;
            if external(&next) {
                external_mail.push(next);
            }
        }
    }
    Ok((through, external_mail))
}

/// A reordered external completion may predate background that now precedes
/// it in the cursor. Never close that background's newer, unfinished turn.
/// Derive the risk from durable mail, so replay/replacement cannot erase it.
/// Missing/duplicate historical proof is not completion evidence.
pub(crate) fn may_complete<H: Harness>(
    store: &Store,
    agent: &AgentId,
    harness: &H,
    message: &Message,
) -> Result<bool, anyagents_core::Error> {
    if !external(message) {
        return Ok(true);
    }
    let Some(open) = store.open_turn(agent)? else {
        return Ok(true);
    };
    let risk = open
        .messages
        .iter()
        .any(|m| m.seq < message.seq && background(m));
    Ok(!risk
        || open
            .messages
            .iter()
            .all(|m| m.id == message.id || finished(harness, m.id.as_str())))
}

fn finished<H: Harness>(harness: &H, id: &str) -> bool {
    matches!(
        harness.proven(id),
        Some(Proof::Taken {
            turn_finished: true
        })
    )
}

/// A recovery result cannot close unfinished mail accepted while it was in
/// flight. In a potentially reordered span, require historical finished proofs
/// too: a missing proof is not evidence that the newer background turn ended.
pub(crate) fn recovery_covers<H: Harness>(
    store: &Store,
    agent: &AgentId,
    harness: &H,
    through: i64,
) -> Result<bool, anyagents_core::Error> {
    let Some(open) = store.open_turn(agent)? else {
        return Ok(true);
    };
    let mut has_background = false;
    let reordered = open.messages.iter().any(|m| {
        has_background |= background(m);
        has_background && external(m)
    });
    Ok(open
        .messages
        .iter()
        .filter(|m| reordered || m.seq > through)
        .all(|m| finished(harness, m.id.as_str())))
}
