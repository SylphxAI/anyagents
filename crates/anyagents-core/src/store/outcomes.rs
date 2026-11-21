//! Supervision verdicts of agent work persisted to `outcomes`.

use serde::{Deserialize, Serialize};

use super::helpers::agent_by_id;
use super::ingress::IngressHistory;
use super::schedules::cancel_archived_schedules;
use super::types::{Agent, Incoming, Wake};
use super::{Store, now_ms, turn_errors};
use crate::{db::DbExecExt as _, db_params, error::Error, id::AgentId};

const MAX_NOTE_CHARS: usize = 2_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeKind {
    Accepted,
    Rework,
    Rejected,
    Abandoned,
}

impl OutcomeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rework => "rework",
            Self::Rejected => "rejected",
            Self::Abandoned => "abandoned",
        }
    }

    pub fn parse_verdict(text: &str) -> Option<Self> {
        match text.trim().to_ascii_lowercase().as_str() {
            "accepted" => Some(Self::Accepted),
            "rework" => Some(Self::Rework),
            "rejected" => Some(Self::Rejected),
            "abandoned" => Some(Self::Abandoned),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    pub id: String,
    pub agent_id: AgentId,
    pub kind: OutcomeKind,
    pub subject: String,
    pub dedupe_key: String,
    pub actor: String,
    pub harness: String,
    pub model: Option<String>,
    pub role: Option<String>,
    pub profile: Option<String>,
    pub note: Option<String>,
    pub claimed_at_ms: i64,
    pub observed_at_ms: i64,
}

impl Store {
    /// A terminal verdict for the current archive, not a historical task run.
    pub fn has_terminal_archive_verdict(&self, agent_id: &AgentId) -> Result<bool, Error> {
        self.read_conn().db_query_one(
            "SELECT EXISTS(SELECT 1 FROM agents WHERE id = ?1 AND state = 'archived'
                           AND terminal_archive_verdict IS NOT NULL)",
            &db_params![agent_id],
            |row| row.get(0),
        )
    }

    /// Record an operator/ancestor verdict and apply its task lifecycle action atomically.
    pub fn apply_verdict(
        &self,
        actor_id: Option<&AgentId>,
        target_id: &AgentId,
        kind: OutcomeKind,
        reason: Option<String>,
        profile: Option<String>,
    ) -> Result<(Outcome, Option<Agent>), Error> {
        let note = reason
            .as_deref()
            .map(str::trim)
            .filter(|n| !n.is_empty())
            .map(|n| {
                crate::masking::mask_known_secrets(n)
                    .chars()
                    .take(MAX_NOTE_CHARS)
                    .collect()
            });
        let profile = profile
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(str::to_string);
        let (outcome, changed, mail) = self.write_tx(|tx| {
            let target = agent_by_id(tx, target_id)?;
            let now = now_ms();
            let (actor, sender) = if let Some(id) = actor_id {
                if id == target_id {
                    return Err(Error::PermissionDenied("an agent cannot record a verdict on its own work".into()));
                }
                let caller = agent_by_id(tx, id)?;
                let mut cur = target.parent_id.clone();
                let mut ok = false;
                for _ in 0..17 {
                    let Some(pid) = cur else { break };
                    if pid == *id { ok = true; break; }
                    cur = agent_by_id(tx, &pid)?.parent_id;
                }
                let actor = format!("agent:{}", caller.name);
                if !ok {
                    return Err(Error::PermissionDenied(
                        "administrative authority required".into(),
                    ));
                }
                (actor, format!("agent:{id}"))
            } else {
                ("operator".into(), "operator".into())
            };
            let outcome = Outcome {
                id: format!("out_{}", uuid::Uuid::now_v7().simple()),
                agent_id: target.id.clone(),
                kind,
                subject: format!("agent:{}", target.id),
                dedupe_key: format!("verdict:{}", uuid::Uuid::now_v7()),
                actor,
                harness: target.harness.clone(),
                model: target.model.clone(),
                role: target.role.clone(),
                profile: profile.clone(),
                note: note.clone(),
                claimed_at_ms: now,
                observed_at_ms: now,
            };
            let insert_sql = "INSERT INTO outcomes (id, agent_id, kind, source, subject, dedupe_key, \
                actor, harness, model, role, profile, note, claimed_at_ms, observed_at_ms) \
                VALUES (?1, ?2, ?3, 'verdict', ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)";
            tx.db_execute(
                insert_sql,
                &db_params![
                    outcome.id, outcome.agent_id, kind.as_str(), outcome.subject,
                    outcome.dedupe_key, outcome.actor, outcome.harness, outcome.model,
                    outcome.role, outcome.profile, outcome.note, now, now
                ],
            )?;
            if !target.is_task_subagent() {
                return Ok((outcome, None, None));
            }
            let (state, mail) = if kind == OutcomeKind::Rework {
                let text = outcome.note.as_deref().unwrap_or("Please rework the task.");
                let mut inc = Incoming::new(text);
                inc.sender = Some(sender);
                let sql = "UPDATE agents SET state = 'running' WHERE id = ?1";
                tx.db_execute(sql, &db_params![target_id])?;
                turn_errors::clear_parking(tx, target_id)?;
                let key = Some(format!("anyagents:verdict:{}", outcome.id));
                let mail = Self::receive_on(
                    tx,
                    target_id,
                    inc,
                    key,
                    now,
                    IngressHistory::default(),
                    false,
                    Some("task_rework"),
                    &mut Vec::new(),
                )?;
                ("running", Some(mail))
            } else {
                let sql = "UPDATE agents SET state = 'archived', terminal_archive_verdict = ?2
                           WHERE id = ?1";
                tx.db_execute(sql, &db_params![target_id, outcome.id])?;
                cancel_archived_schedules(tx, target_id)?;
                ("archived", None)
            };
            let mut changed = target;
            changed.state = state.into();
            Ok((outcome, Some(changed), mail))
        })?;
        if let Some(mail) = mail {
            self.finish_receive(mail, &[]);
        }
        if changed.is_some() {
            self.notify(Wake::Agent {
                agent_id: target_id.clone(),
            });
        }
        Ok((outcome, changed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NewAgent;

    use OutcomeKind::*;

    #[test]
    fn apply_verdict_enforces_downward_supervision_and_task_lifecycle() {
        let dir = crate::test_tempdir();
        let s = Store::open(dir.path().join("state.db")).unwrap();
        let mk = |n: &str, p: Option<&AgentId>| {
            let mut a = NewAgent::new(n, "codex");
            a.parent_id = p.cloned();
            s.create_agent(a).unwrap()
        };
        let v = |a, t, k, r| s.apply_verdict(a, t, k, r, None);
        let r = mk("root", None);
        let c = mk("c1", Some(&r.id));
        let gc = mk("gc1", Some(&c.id));
        let sib = mk("sib", Some(&r.id));
        assert!(v(Some(&c.id), &c.id, Accepted, None).is_err());
        assert!(v(Some(&sib.id), &c.id, Accepted, None).is_err());
        let (o, ch) = v(Some(&r.id), &gc.id, Rework, Some("fix".into())).unwrap();
        assert_eq!((o.kind, o.actor.as_str()), (Rework, "agent:root"));
        assert_eq!(ch.unwrap().state, "running");
        assert_eq!(s.inbox(&gc.id, None).unwrap()[0].text, "fix");
        let (o, ch) = v(Some(&c.id), &gc.id, Accepted, None).unwrap();
        assert_eq!((o.kind, ch.unwrap().state.as_str()), (Accepted, "archived"));
        let (o, _) = v(None, &sib.id, Rejected, None).unwrap();
        assert_eq!((o.actor.as_str(), o.kind), ("operator", Rejected));
    }
}
