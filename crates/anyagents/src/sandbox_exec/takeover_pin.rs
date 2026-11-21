//! Durable browser takeover custody, scoped to one agent and one compute lease.
//!
//! The lifecycle lock serializes pin creation/revocation with destructive idle
//! paths. Registration never takes the process lock: a running harness may
//! itself be waiting for the browser tool to register.

use agent_tools::sha256_hex;
use anyagents_core::stateless::SandboxBinding;
use anyagents_core::{AgentId, Error, Store, now_ms};
use serde::{Deserialize, Serialize};

const NS: &str = "sandbox-takeover";
const FILE: &str = "pin.json";
const TTL_MS: i64 = 30 * 60 * 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Rejection {
    Expired,
    Ended,
    Unauthorized,
    Occupied,
    Store,
}

#[derive(Debug, Deserialize, Serialize)]
struct Pin {
    lease: String,
    session: String,
    token_hash: String,
    expires_ms: i64,
    ended: bool,
}

fn lease_key(binding: &SandboxBinding) -> Option<String> {
    let key = binding
        .lease_resource
        .as_deref()
        .or(binding.lease_id.as_deref())?;
    (!key.is_empty() && binding.provider != "local")
        .then(|| sha256_hex(format!("{}\0{key}", binding.provider).as_bytes()))
}

fn read(store: &Store, agent: &AgentId) -> Result<Option<Pin>, Error> {
    store
        .blob_store()
        .get_agent_file(agent, NS, FILE)?
        .map(|blob| {
            serde_json::from_slice(&blob.content).map_err(|e| Error::Message(e.to_string()))
        })
        .transpose()
}

fn write(store: &Store, agent: &AgentId, pin: &Pin) -> Result<(), Error> {
    let bytes = serde_json::to_vec(pin).map_err(|e| Error::Message(e.to_string()))?;
    store
        .blob_store()
        .put_agent_file(agent, NS, FILE, &bytes, false)?;
    Ok(())
}

fn check_agent(store: &Store, id: &AgentId) -> Result<(), Rejection> {
    match store.agent(id) {
        Ok(agent) if !matches!(agent.state.as_str(), "archived" | "paused") => Ok(()),
        Ok(_) | Err(Error::AgentNotFound(_)) => Err(Rejection::Ended),
        Err(_) => Err(Rejection::Store),
    }
}

fn check_pin(pin: &Pin, binding: Option<&SandboxBinding>, now: i64) -> Result<i64, Rejection> {
    if pin.ended {
        return Err(Rejection::Ended);
    }
    if now >= pin.expires_ms {
        return Err(Rejection::Expired);
    }
    let binding = binding.ok_or(Rejection::Ended)?;
    if binding.lease_state != "ready" || lease_key(binding).as_ref() != Some(&pin.lease) {
        return Err(Rejection::Ended);
    }
    if binding
        .lease_expires_at_ms
        .is_some_and(|expiry| now >= expiry)
    {
        return Err(Rejection::Expired);
    }
    Ok(pin.expires_ms)
}

/// Returns the fixed deadline, or None for standalone agents without a remote binding.
pub(crate) fn register(
    store: &Store,
    agent: &AgentId,
    session: &str,
    token: &str,
    reconnect: bool,
) -> Result<Option<i64>, Rejection> {
    let _guard = store
        .lock_agent_lifecycle(agent)
        .map_err(|_| Rejection::Store)?;
    register_locked(store, agent, session, token, reconnect, now_ms())
}

fn register_locked(
    store: &Store,
    agent: &AgentId,
    session: &str,
    token: &str,
    reconnect: bool,
    now: i64,
) -> Result<Option<i64>, Rejection> {
    check_agent(store, agent)?;
    let binding = store.sandbox_binding(agent).map_err(|_| Rejection::Store)?;
    let token_hash = sha256_hex(token.as_bytes());
    if let Some(pin) = read(store, agent).map_err(|_| Rejection::Store)? {
        if pin.session == session {
            if !anyagents_core::constant_time_eq(pin.token_hash.as_bytes(), token_hash.as_bytes()) {
                return Err(Rejection::Unauthorized);
            }
            return check_pin(&pin, binding.as_ref(), now).map(Some);
        }
        if check_pin(&pin, binding.as_ref(), now).is_ok() {
            return Err(Rejection::Occupied);
        }
    }
    if reconnect {
        return Err(Rejection::Ended);
    }
    let Some(binding) = binding.filter(|b| b.provider != "local") else {
        return Ok(None);
    };
    let lease = lease_key(&binding).ok_or(Rejection::Ended)?;
    if binding.lease_state != "ready" {
        return Err(Rejection::Ended);
    }
    let expires_ms = binding
        .lease_expires_at_ms
        .unwrap_or(i64::MAX)
        .min(now.saturating_add(TTL_MS));
    if expires_ms <= now {
        return Err(Rejection::Expired);
    }
    let pin = Pin {
        lease,
        session: session.to_owned(),
        token_hash,
        expires_ms,
        ended: false,
    };
    write(store, agent, &pin).map_err(|_| Rejection::Store)?;
    Ok(Some(expires_ms))
}

/// Check a live tunnel against durable state, including releases by other replicas.
pub(crate) fn check(store: &Store, agent: &AgentId, session: &str) -> Result<(), Rejection> {
    check_agent(store, agent)?;
    let pin = read(store, agent)
        .map_err(|_| Rejection::Store)?
        .filter(|p| p.session == session)
        .ok_or(Rejection::Ended)?;
    let binding = store.sandbox_binding(agent).map_err(|_| Rejection::Store)?;
    check_pin(&pin, binding.as_ref(), now_ms()).map(|_| ())
}

/// Idle paths hold the lifecycle lock. No pin write or idle timestamp reset is
/// needed at expiration: the absolute deadline itself makes a pin inactive.
pub(crate) fn holds(store: &Store, binding: &SandboxBinding, now: i64) -> Result<bool, Error> {
    Ok(read(store, &binding.agent_id)?
        .is_some_and(|pin| check_pin(&pin, Some(binding), now).is_ok()))
}

pub(crate) fn unregister(store: &Store, agent: &AgentId, session: &str) -> Result<(), Error> {
    let _guard = store.lock_agent_lifecycle(agent)?;
    end_locked(store, agent, Some(session))
}

/// Caller owns the lifecycle lock. Retain one small tombstone so a retry of
/// the last registration cannot turn a stopped session back into a fresh pin.
pub(crate) fn end_locked(
    store: &Store,
    agent: &AgentId,
    session: Option<&str>,
) -> Result<(), Error> {
    if let Some(mut pin) = read(store, agent)?
        && !pin.ended
        && session.is_none_or(|id| pin.session == id)
    {
        pin.ended = true;
        write(store, agent, &pin)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox_exec::idle::{
        handle_agent_idle_release, sweep_idle_sandboxes_with_resolver,
    };
    use anyagents_core::NewAgent;

    fn fixture(provider: &str) -> (tempfile::TempDir, Store, SandboxBinding) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let agent = store
            .create_agent(NewAgent::new("takeover-test", "claude-code"))
            .unwrap();
        let mut binding = SandboxBinding::new(agent.id, provider);
        binding.lease_id = Some("compute-1".into());
        binding.lease_state = "ready".into();
        binding.idle_ttl_ms = 0;
        store.sandbox_binding_store().upsert(&binding).unwrap();
        (dir, store, binding)
    }

    #[test]
    fn takeover_pin_survives_store_reopen_without_extending_and_ends_by_session() {
        let (dir, store, mut binding) = fixture("sylphx");
        let agent = &binding.agent_id;
        let now = 10_000;
        binding.lease_expires_at_ms = Some(now + 1_000);
        store.sandbox_binding_store().upsert(&binding).unwrap();
        let start = |store: &Store, sid, token, reconnect, at| {
            register_locked(store, agent, sid, token, reconnect, at)
        };
        assert_eq!(
            start(&store, "initial", "token-one", false, now),
            Ok(Some(now + 1_000))
        );
        assert_eq!(
            start(&store, "other", "token-two", false, now),
            Err(Rejection::Occupied)
        );
        drop(store);
        let store = Store::open(dir.path().join("state.db")).unwrap();
        // Even a provider renewal cannot move the original deadline.
        binding.lease_expires_at_ms = Some(now + 10_000);
        store.sandbox_binding_store().upsert(&binding).unwrap();
        assert_eq!(
            start(&store, "initial", "token-one", true, now + 100),
            Ok(Some(now + 1_000))
        );
        assert_eq!(
            start(&store, "initial", "wrong", true, now + 100),
            Err(Rejection::Unauthorized)
        );
        unregister(&store, agent, "stale").unwrap();
        assert!(holds(&store, &binding, now + 100).unwrap());
        assert_eq!(
            start(&store, "initial", "token-one", true, now + 1_000),
            Err(Rejection::Expired)
        );
        unregister(&store, agent, "initial").unwrap();
        assert!(!holds(&store, &binding, now + 100).unwrap());
        assert_eq!(
            start(&store, "initial", "token-one", true, now + 100),
            Err(Rejection::Ended)
        );
        assert_eq!(
            start(&store, "initial", "token-one", false, now + 100),
            Err(Rejection::Ended)
        );
        assert_eq!(
            start(&store, "unknown", "token-one", true, now + 100),
            Err(Rejection::Ended)
        );
    }

    #[test]
    fn takeover_pin_defers_batch_idle_and_sweep_without_resetting_idle_age() {
        let (dir, store, binding) = fixture("legacy_http");
        let agent = &binding.agent_id;
        let expiry = register(&store, agent, "test-session", "test-token", false)
            .unwrap()
            .unwrap();
        assert!(expiry <= now_ms() + TTL_MS);
        handle_agent_idle_release(&store, agent, dir.path());
        let idle = store
            .sandbox_binding(agent)
            .unwrap()
            .unwrap()
            .idle_since_ms
            .unwrap();
        let sweep =
            |at| sweep_idle_sandboxes_with_resolver(&store, dir.path(), at, |_| Ok(None)).unwrap();
        let report = sweep(expiry - 1);
        assert_eq!(report.skipped_busy, vec![agent.clone()]);
        assert_eq!(
            store.sandbox_binding(agent).unwrap().unwrap().idle_since_ms,
            Some(idle)
        );
        assert_eq!(sweep(expiry).released, vec![agent.clone()]);
        assert_eq!(
            store.sandbox_binding(agent).unwrap().unwrap().lease_state,
            "detached"
        );
    }

    #[test]
    fn takeover_pin_does_not_follow_lease_replacement_or_agent_archive() {
        let (_dir, store, mut binding) = fixture("sylphx");
        let agent = &binding.agent_id;
        register(&store, agent, "test-session", "test-token", false).unwrap();
        binding.lease_id = Some("compute-2".into());
        store.sandbox_binding_store().upsert(&binding).unwrap();
        assert!(!holds(&store, &binding, now_ms()).unwrap());
        assert_eq!(check(&store, agent, "test-session"), Err(Rejection::Ended));
        register(&store, agent, "new-session", "test-token", false).unwrap();
        let _guard = store.lock_agent_lifecycle(agent).unwrap();
        store.set_agent_state(agent, "archived").unwrap();
        end_locked(&store, agent, None).unwrap();
        assert_eq!(check(&store, agent, "new-session"), Err(Rejection::Ended));
        store.set_agent_state(agent, "active").unwrap();
        assert_eq!(check(&store, agent, "new-session"), Err(Rejection::Ended));
    }
}
