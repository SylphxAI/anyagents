//! Typed binding slots and targets (ADR 0004 decision 2).
//!
//! The rows themselves live in the store's `credential_bindings` table; this
//! module is the typed edge the rest of the crate uses.

use std::fmt;
use std::str::FromStr;

use anyagents_core::{AgentId, Error, Store};

use super::store::{RecordRef, valid_segment};

/// What a binding is for.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Slot {
    /// The model provider credential a harness authenticates with.
    Model(String),
    Github,
    GitAuthor,
    Op,
    /// Further tool slots (`kube`, `build_key`, ...).
    Tool(String),
    Channel(String),
}

impl fmt::Display for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Slot::Model(p) => write!(f, "model:{p}"),
            Slot::Github => f.write_str("tool:github"),
            Slot::GitAuthor => f.write_str("tool:git_author"),
            Slot::Op => f.write_str("tool:op"),
            Slot::Tool(t) => write!(f, "tool:{t}"),
            Slot::Channel(c) => write!(f, "channel:{c}"),
        }
    }
}

impl FromStr for Slot {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bad = || Error::Message(format!("invalid credential slot `{s}`"));
        let (head, tail) = s.split_once(':').ok_or_else(bad)?;
        if !valid_segment(tail) && head != "channel" {
            return Err(bad());
        }
        match head {
            "model" => Ok(Slot::Model(tail.to_string())),
            "tool" => Ok(match tail {
                "github" => Slot::Github,
                "git_author" => Slot::GitAuthor,
                "op" => Slot::Op,
                other => Slot::Tool(other.to_string()),
            }),
            "channel" if !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') => {
                Ok(Slot::Channel(tail.to_string()))
            }
            _ => Err(bad()),
        }
    }
}

/// What a binding points at.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Target {
    Record(RecordRef),
    Pool(String),
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Target::Record(r) => write!(f, "record:{r}"),
            Target::Pool(p) => write!(f, "pool:{p}"),
        }
    }
}

impl FromStr for Target {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(rest) = s.strip_prefix("record:") {
            return Ok(Target::Record(rest.parse()?));
        }
        if let Some(rest) = s.strip_prefix("pool:")
            && valid_segment(rest)
        {
            return Ok(Target::Pool(rest.to_string()));
        }
        Err(Error::Message(format!(
            "invalid credential target `{s}`; expected record:<kind>/<provider>/<name> or pool:<name>"
        )))
    }
}

/// One binding as read from the store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
    pub slot: Slot,
    pub target: Target,
    pub set_by: String,
    pub set_at_ms: i64,
    pub reason: String,
}

/// The only writer of bindings: an explicit, audited configure call (or the
/// one-shot migration, whose actor is `migration`). The caller has already
/// checked the actor's authority over `agent`.
pub fn bind(
    store: &Store,
    agent: &AgentId,
    slot: &Slot,
    target: &Target,
    actor: &str,
    reason: &str,
) -> Result<Binding, Error> {
    let row = store.bind_credential(agent, &slot.to_string(), &target.to_string(), actor, reason)?;
    from_row(row)
}

pub fn unbind(store: &Store, agent: &AgentId, slot: &Slot, actor: &str, reason: &str) -> Result<Option<Binding>, Error> {
    store
        .unbind_credential(agent, &slot.to_string(), actor, reason)?
        .map(from_row)
        .transpose()
}

pub fn binding(store: &Store, agent: &AgentId, slot: &Slot) -> Result<Option<Binding>, Error> {
    store
        .credential_binding(agent, &slot.to_string())?
        .map(from_row)
        .transpose()
}

pub fn bindings_of(store: &Store, agent: &AgentId) -> Result<Vec<Binding>, Error> {
    store
        .credential_bindings_of(agent)?
        .into_iter()
        .map(from_row)
        .collect()
}

/// A stored row that no longer parses is an error, never a silently skipped
/// binding: resolution fails closed on it.
fn from_row(row: anyagents_core::CredentialBinding) -> Result<Binding, Error> {
    Ok(Binding {
        slot: row.slot.parse()?,
        target: row.target.parse()?,
        set_by: row.set_by,
        set_at_ms: row.set_at_ms,
        reason: row.reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_and_targets_round_trip() {
        for s in ["model:anthropic", "tool:github", "tool:git_author", "tool:op", "tool:kube", "channel:chn_01abc"] {
            assert_eq!(s.parse::<Slot>().unwrap().to_string(), s);
        }
        for t in ["record:api_key/openrouter/main", "pool:example"] {
            assert_eq!(t.parse::<Target>().unwrap().to_string(), t);
        }
        for bad in ["model:", "model:Up", "nope:x", "tool:../x"] {
            assert!(bad.parse::<Slot>().is_err(), "{bad}");
        }
        for bad in ["record:x", "pool:", "default", "anthropic/default"] {
            assert!(bad.parse::<Target>().is_err(), "{bad}");
        }
    }
}
