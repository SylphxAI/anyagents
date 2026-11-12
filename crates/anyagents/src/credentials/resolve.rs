//! Typed resolution: binding, then operator default, then `Unbound`.
//!
//! There is no implicit `default` record, no "first healthy" choice and no
//! name-based compatibility. Every failure is a typed [`CredentialError`].

use std::fmt;
use std::path::Path;

use anyagents_core::{Agent, Store};

use super::binding::{self, Slot, Target};
use super::catalog;
use super::config::{OperatorConfig, PoolPolicy};
use super::store::{CredentialStore, Identity, RecordMeta, RecordRef};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CredentialError {
    /// No binding and no operator default for the slot.
    Unbound { slot: Slot },
    /// The target names a record that does not exist.
    Missing { slot: Slot, target: String },
    /// The record cannot serve the slot (wrong kind or provider), or the
    /// harness cannot run on the record's provider.
    WrongKind { slot: Slot, target: String, want: String },
    /// The record carries a known-bad marker (failed probe or import).
    Invalid { slot: Slot, target: String, reason: String },
    /// The record's identity is not the one the slot requires.
    IdentityMismatch { slot: Slot, target: String, want: String, got: String },
    /// Every pool member is exhausted or invalid.
    PoolExhausted { slot: Slot, pool: String },
    /// Required state (store, bindings, operator config) could not be read.
    StoreUnreadable { reason: String },
}

impl CredentialError {
    /// Stable machine-readable reason, used for health and notices.
    pub fn code(&self) -> &'static str {
        match self {
            CredentialError::Unbound { .. } => "unbound",
            CredentialError::Missing { .. } => "missing",
            CredentialError::WrongKind { .. } => "wrong_kind",
            CredentialError::Invalid { .. } => "invalid",
            CredentialError::IdentityMismatch { .. } => "identity_mismatch",
            CredentialError::PoolExhausted { .. } => "pool_exhausted",
            CredentialError::StoreUnreadable { .. } => "store_unreadable",
        }
    }

    pub fn slot(&self) -> Option<&Slot> {
        match self {
            CredentialError::Unbound { slot }
            | CredentialError::Missing { slot, .. }
            | CredentialError::WrongKind { slot, .. }
            | CredentialError::Invalid { slot, .. }
            | CredentialError::IdentityMismatch { slot, .. }
            | CredentialError::PoolExhausted { slot, .. } => Some(slot),
            CredentialError::StoreUnreadable { .. } => None,
        }
    }

    fn unreadable(err: impl fmt::Display) -> Self {
        CredentialError::StoreUnreadable {
            reason: err.to_string(),
        }
    }
}

impl fmt::Display for CredentialError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "credential_blocked: {}", self.code())?;
        match self {
            CredentialError::Unbound { slot } => {
                write!(f, " (slot {slot}: no binding and no operator default)")
            }
            CredentialError::Missing { slot, target } => {
                write!(f, " (slot {slot}: {target} does not exist)")
            }
            CredentialError::WrongKind { slot, target, want } => {
                write!(f, " (slot {slot}: {target} cannot serve {want})")
            }
            CredentialError::Invalid { slot, target, reason } => {
                write!(f, " (slot {slot}: {target}: {reason})")
            }
            CredentialError::IdentityMismatch { slot, target, want, got } => {
                write!(f, " (slot {slot}: {target} is {got}, expected {want})")
            }
            CredentialError::PoolExhausted { slot, pool } => {
                write!(f, " (slot {slot}: every member of pool:{pool} is exhausted)")
            }
            CredentialError::StoreUnreadable { reason } => write!(f, " ({reason})"),
        }
    }
}

impl From<CredentialError> for anyagents_core::Error {
    fn from(value: CredentialError) -> Self {
        anyagents_core::Error::Message(value.to_string())
    }
}

/// Where a resolved target came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    Binding,
    OperatorDefault,
}

/// A slot resolved to one published record generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub slot: Slot,
    pub target: Target,
    pub origin: Origin,
    pub record: RecordRef,
    pub meta: RecordMeta,
    /// Set when `target` is a pool: why this member was picked.
    pub pool_reason: Option<String>,
}

/// The slots a launch of `agent` on `harness` needs. Tool slots are needed
/// only when the agent or the operator configured them; an unconfigured tool
/// stays unauthenticated rather than inheriting an ambient identity.
pub fn need(store: &Store, config: &OperatorConfig, agent: &Agent, harness: &str) -> Result<Vec<Slot>, CredentialError> {
    let provider = catalog::model_provider(harness, agent.model.as_deref());
    let mut slots = Vec::new();
    if !catalog::harness_manifest(harness, &provider).is_empty() {
        slots.push(Slot::Model(provider));
    }
    let bound = binding::bindings_of(store, &agent.id).map_err(CredentialError::unreadable)?;
    for slot in [Slot::Github, Slot::GitAuthor, Slot::Op] {
        if bound.iter().any(|b| b.slot == slot) || config.defaults.contains_key(&slot) {
            slots.push(slot);
        }
    }
    for b in bound {
        if matches!(b.slot, Slot::Tool(_)) && !slots.contains(&b.slot) {
            slots.push(b.slot);
        }
    }
    Ok(slots)
}

/// Resolves one slot of one agent.
///
/// `remaining_pct` answers a pool member's remaining quota (`None` = unknown,
/// which counts as available but ranks below any known positive reading).
pub fn resolve(
    store: &Store,
    creds: &CredentialStore,
    config: &OperatorConfig,
    agent: &Agent,
    harness: &str,
    slot: &Slot,
    remaining_pct: &dyn Fn(&RecordRef) -> Option<i64>,
) -> Result<Resolved, CredentialError> {
    let bound = binding::binding(store, &agent.id, slot).map_err(CredentialError::unreadable)?;
    let (target, origin) = match bound {
        Some(b) => (b.target, Origin::Binding),
        None => match config.defaults.get(slot) {
            Some(t) => (t.clone(), Origin::OperatorDefault),
            None => return Err(CredentialError::Unbound { slot: slot.clone() }),
        },
    };
    let (record, pool_reason) = match &target {
        Target::Record(r) => (r.clone(), None),
        Target::Pool(name) => pick_pool_member(creds, config, slot, name, remaining_pct)?,
    };
    let meta = creds
        .get(&record)
        .map_err(CredentialError::unreadable)?
        .ok_or_else(|| CredentialError::Missing {
            slot: slot.clone(),
            target: target.to_string(),
        })?;
    check_serves(slot, &target, &record, harness)?;
    if let Some(reason) = &meta.invalid_reason {
        return Err(CredentialError::Invalid {
            slot: slot.clone(),
            target: target.to_string(),
            reason: reason.clone(),
        });
    }
    Ok(Resolved {
        slot: slot.clone(),
        target,
        origin,
        record,
        meta,
        pool_reason,
    })
}

/// Whether a record can serve a slot for a harness: a model slot needs a
/// record of that provider whose provider runs on the harness; tool slots need
/// their kind.
pub fn check_serves(slot: &Slot, target: &Target, record: &RecordRef, harness: &str) -> Result<(), CredentialError> {
    use super::store::Kind;
    let wrong = |want: String| CredentialError::WrongKind {
        slot: slot.clone(),
        target: target.to_string(),
        want,
    };
    match slot {
        Slot::Model(provider) => {
            if &record.provider != provider
                || !matches!(record.kind, Kind::OauthSubscription | Kind::ApiKey)
            {
                return Err(wrong(format!("model provider {provider}")));
            }
            let harness = anyagents_harness::normalize_harness_name(harness);
            if !catalog::compatible_harnesses(provider).iter().any(|h| h == harness) {
                return Err(wrong(format!("harness {harness}")));
            }
        }
        Slot::Github if !matches!(record.kind, Kind::GithubUser | Kind::GithubApp) => {
            return Err(wrong("a GitHub identity".into()));
        }
        Slot::GitAuthor if record.kind != Kind::GithubUser => {
            return Err(wrong("a GitHub user identity".into()));
        }
        Slot::Op if record.kind != Kind::OpServiceAccount => {
            return Err(wrong("an op service account".into()));
        }
        Slot::Channel(_) if record.kind != Kind::ChannelBot => {
            return Err(wrong("a channel bot".into()));
        }
        _ => {}
    }
    Ok(())
}

fn pick_pool_member(
    creds: &CredentialStore,
    config: &OperatorConfig,
    slot: &Slot,
    pool: &str,
    remaining_pct: &dyn Fn(&RecordRef) -> Option<i64>,
) -> Result<(RecordRef, Option<String>), CredentialError> {
    let (members, policy) = config.pools.get(pool).ok_or_else(|| CredentialError::Missing {
        slot: slot.clone(),
        target: format!("pool:{pool}"),
    })?;
    let mut candidates: Vec<(usize, &RecordRef, Option<i64>)> = Vec::new();
    for (i, member) in members.iter().enumerate() {
        match creds.get(member).map_err(CredentialError::unreadable)? {
            Some(meta) if meta.invalid_reason.is_none() => {
                let pct = remaining_pct(member);
                if pct != Some(0) {
                    candidates.push((i, member, pct));
                }
            }
            _ => {}
        }
    }
    let chosen = match policy {
        PoolPolicy::Ordered => candidates.first(),
        PoolPolicy::MostRemaining => candidates.iter().max_by(|a, b| {
            // Known positive readings outrank unknown; earlier members win ties.
            let key = |c: &(usize, &RecordRef, Option<i64>)| (c.2.map_or(-1, |p| p), -(c.0 as i64));
            key(a).cmp(&key(b))
        }),
    };
    let (_, member, pct) = chosen.ok_or_else(|| CredentialError::PoolExhausted {
        slot: slot.clone(),
        pool: pool.to_string(),
    })?;
    let reason = match pct {
        Some(p) => format!("pool:{pool} {policy:?} picked {member} ({p}% left)"),
        None => format!("pool:{pool} {policy:?} picked {member} (quota unknown)"),
    };
    Ok(((*member).clone(), Some(reason)))
}

/// Resolves every needed slot; the first failure stops the launch.
pub fn resolve_all(
    store: &Store,
    data: &Path,
    agent: &Agent,
    harness: &str,
    remaining_pct: &dyn Fn(&RecordRef) -> Option<i64>,
) -> Result<Vec<Resolved>, CredentialError> {
    let creds = CredentialStore::open(data).map_err(CredentialError::unreadable)?;
    let config = OperatorConfig::load(data).map_err(CredentialError::unreadable)?;
    let mut out = Vec::new();
    for slot in need(store, &config, agent, harness)? {
        let resolved = resolve(store, &creds, &config, agent, harness, &slot, remaining_pct)?;
        if matches!(resolved.target, Target::Pool(_)) {
            store
                .record_credential_observation(
                    &agent.id,
                    &slot.to_string(),
                    &resolved.record.to_string(),
                    resolved.meta.generation as i64,
                    resolved.pool_reason.as_deref().unwrap_or(""),
                )
                .map_err(CredentialError::unreadable)?;
        }
        out.push(resolved);
    }
    Ok(out)
}

/// The identity an expected-identity check compares against, when the
/// operator pinned one for the slot's record.
pub fn expect_identity(meta: &RecordMeta, want: Option<&Identity>, slot: &Slot) -> Result<(), CredentialError> {
    if let (Some(want), Some(got)) = (want, meta.identity.as_ref())
        && !want.same_account(got)
    {
        return Err(CredentialError::IdentityMismatch {
            slot: slot.clone(),
            target: meta.record.to_string(),
            want: want.subject.clone(),
            got: got.subject.clone(),
        });
    }
    Ok(())
}
