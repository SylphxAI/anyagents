//! Bridge the in-memory tunnel registry to durable lease custody.

use super::*;
use crate::sandbox_exec::takeover_pin::{self, Rejection};

pub(super) type RegisterError = (&'static str, &'static str, u16);
pub(super) const EXPIRED: RegisterError = ("EXPIRED", "takeover lease expired", 4410);
pub(super) const ENDED: RegisterError = ("ENDED", "takeover session ended", 4411);
const UNAVAILABLE: RegisterError = ("UNAVAILABLE", "cannot verify takeover lease", 4503);

fn register_error(err: Rejection) -> RegisterError {
    match err {
        Rejection::Expired => EXPIRED,
        Rejection::Ended => ENDED,
        Rejection::Unauthorized => ("UNAUTHORIZED", "takeover token mismatch on reconnect", 4401),
        Rejection::Occupied => ("CONFLICT", "another takeover pins this lease", 4409),
        Rejection::Store => UNAVAILABLE,
    }
}

#[derive(Clone)]
pub(super) struct LeasePin {
    store: Arc<Store>,
    agent: anyagents_core::AgentId,
    session: String,
    deadline: Instant,
    expires_ms: i64,
}

impl LeasePin {
    pub(super) async fn register(
        store: Arc<Store>,
        owner: &TunnelOwner,
        session: &str,
        token: &str,
        reconnect: bool,
    ) -> Result<Option<Self>, RegisterError> {
        let TunnelOwner::Agent(agent) = owner else {
            return if reconnect { Err(ENDED) } else { Ok(None) };
        };
        let (agent, session, token) = (agent.clone(), session.to_owned(), token.to_owned());
        tokio::task::spawn_blocking(move || {
            let deadline = takeover_pin::register(&store, &agent, &session, &token, reconnect)
                .map_err(register_error)?;
            let Some(deadline) = deadline else {
                return Ok(None);
            };
            let remaining = deadline.saturating_sub(anyagents_core::now_ms());
            if remaining <= 0 {
                return Err(EXPIRED);
            }
            Ok(Some(Self {
                store,
                agent,
                session,
                expires_ms: deadline,
                deadline: Instant::now() + Duration::from_millis(remaining as u64),
            }))
        })
        .await
        .map_err(|_| UNAVAILABLE)?
    }

    pub(super) fn deadline(&self) -> Instant {
        self.deadline
    }

    pub(super) fn expires_ms(&self) -> i64 {
        self.expires_ms
    }

    pub(super) fn remaining_ms(&self) -> u64 {
        self.deadline
            .saturating_duration_since(Instant::now())
            .as_millis() as u64
    }

    pub(super) async fn check(&self) -> Result<(), RegisterError> {
        if Instant::now() >= self.deadline {
            return Err(EXPIRED);
        }
        let pin = self.clone();
        tokio::task::spawn_blocking(move || {
            takeover_pin::check(&pin.store, &pin.agent, &pin.session)
        })
        .await
        .map_err(|_| UNAVAILABLE)?
        .map_err(register_error)
    }

    pub(super) async fn end(&self) {
        let pin = self.clone();
        if let Err(err) = tokio::task::spawn_blocking(move || {
            takeover_pin::unregister(&pin.store, &pin.agent, &pin.session)
        })
        .await
        .map_err(|e| e.to_string())
        .and_then(|res| res.map_err(|e| e.to_string()))
        {
            eprintln!("anyagents-sandbox: cannot unregister takeover pin: {err}");
        }
    }
}
