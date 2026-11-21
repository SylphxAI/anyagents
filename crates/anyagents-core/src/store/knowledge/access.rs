//! Grant predicates shared by direct access and scoped retrieval.

use serde::{Deserialize, Serialize};

use super::{
    AgentId, DbExec, DbExecExt, DbParam, Error, Store, db_params, invalid, new_id, now_ms,
};
use crate::GroupId;
use crate::db::DbBackend;
use crate::store::helpers::agent_exists;

/// Construct only from a verified transport principal (or a trusted delivery target).
#[derive(Clone, Debug)]
pub enum Actor {
    Owner(String),
    Agent(AgentId),
    Scoped {
        id: String,
        read: bool,
        write: bool,
        grant: bool,
    },
}

impl Actor {
    pub(super) fn id(&self) -> String {
        match self {
            Self::Owner(id) | Self::Scoped { id, .. } => id.clone(),
            Self::Agent(id) => id.to_string(),
        }
    }

    fn grantor(&self) -> Result<&str, Error> {
        match self {
            Self::Owner(id)
            | Self::Scoped {
                id, grant: true, ..
            } => Ok(id),
            _ => Err(denied()),
        }
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Read,
    Write,
    Propose,
}
impl Mode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Propose => "propose",
        }
    }
}

#[derive(Clone, Debug)]
pub enum Scope {
    All,
    Org,
    Domain(String),
    Agent,
    Namespace(String),
}
impl Scope {
    pub fn parse(text: &str) -> Result<Self, Error> {
        Ok(match text {
            "all" => Self::All,
            "org" => Self::Org,
            "agent" => Self::Agent,
            _ if text.starts_with("domain:") => {
                validate_namespace(text)?;
                Self::Domain(text.into())
            }
            _ => return Err(invalid("scope must be all, org, agent or domain:<slug>")),
        })
    }
}

pub(super) fn slug(text: &str, max: usize) -> bool {
    !text.is_empty()
        && text.len() <= max
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b':' | b'.'))
}

pub(super) fn validate_namespace(namespace: &str) -> Result<(), Error> {
    let Some((root, tail)) = namespace.split_once(':') else {
        return Err(invalid("namespace must be qualified"));
    };
    if namespace.len() > 120
        || !matches!(root, "org" | "domain" | "agent")
        || !tail.split(':').all(|part| slug(part, 100))
        || (root == "agent" && AgentId::parse(tail).is_err())
    {
        return Err(invalid("invalid knowledge namespace"));
    }
    Ok(())
}

fn validate_pattern(pattern: &str) -> Result<(), Error> {
    if pattern == "*" {
        return Ok(());
    }
    if let Some(prefix) = pattern.strip_suffix(":*") {
        if matches!(prefix, "org" | "domain" | "agent") {
            return Ok(());
        }
        validate_namespace(prefix)?;
        if !prefix.starts_with("agent:") {
            return Ok(());
        }
        return Err(invalid("personal namespaces do not have descendants"));
    }
    validate_namespace(pattern)
}

// Patterns are exact, global or end in ':*'. No SQL wildcard semantics.
const MATCH: &str = "(g.namespace_pattern = {ns} OR g.namespace_pattern = '*' OR
    (substr(g.namespace_pattern, length(g.namespace_pattern), 1) = '*' AND
     substr({ns}, 1, length(g.namespace_pattern) - 1) = substr(g.namespace_pattern, 1, length(g.namespace_pattern) - 1)))";

// Keep the same membership and namespace checks for point access, listing, and
// search. Never expand a group into durable agent grants.
fn grant_queries(who: &str, modes: &str, namespace: &str) -> [String; 2] {
    let pattern = MATCH.replace("{ns}", namespace);
    [
        format!(
            "SELECT g.id FROM knowledge_grants g
             WHERE g.agent_id = {who} AND ({modes}) AND {pattern}"
        ),
        format!(
            "SELECT g.id FROM knowledge_group_grants g
             JOIN group_agents m ON m.group_name = g.group_name
             WHERE m.agent_id = {who} AND ({modes}) AND {pattern}"
        ),
    ]
}

pub(super) fn can(
    conn: &impl DbExec,
    actor: &Actor,
    namespace: &str,
    mode: Mode,
    lock: bool,
) -> Result<bool, Error> {
    let id = match actor {
        Actor::Owner(_) => return Ok(true),
        Actor::Scoped { read, write, .. } => {
            return Ok(match mode {
                Mode::Read => *read,
                Mode::Write | Mode::Propose => *write,
            });
        }
        Actor::Agent(id) => id,
    };
    if let Some(personal) = namespace.strip_prefix("agent:") {
        // No grant, even '*', crosses another agent's personal boundary.
        return Ok(personal == id.as_str() && matches!(mode, Mode::Read | Mode::Write));
    }
    let lock = if lock && conn.backend() == DbBackend::Postgres {
        " FOR SHARE"
    } else {
        ""
    };
    if matches!(mode, Mode::Propose) && namespace.starts_with("org:") {
        // Live here means a non-archived roster identity, not a running
        // process. A default proposal never reads or modifies the document.
        let live = conn.db_query_opt(
            &format!("SELECT id FROM agents WHERE id = ?1 AND state != 'archived'{lock}"),
            &db_params![id.as_str()],
            |_| Ok(()),
        )?;
        if live.is_some() {
            return Ok(true);
        }
    }
    let modes = if matches!(mode, Mode::Propose) && namespace.starts_with("domain:") {
        // Domain read/edit authority includes drafting. An explicit proposal
        // grant still works without conferring either read or edit authority.
        "g.mode = ?2 OR g.mode IN ('read', 'write')"
    } else {
        "g.mode = ?2"
    };
    let params = db_params![id.as_str(), mode.as_str(), namespace];
    for sql in grant_queries("?1", modes, "?3") {
        // A write authorized via a group locks both the grant and the
        // membership row on PostgreSQL, so removal serializes with the write.
        if conn
            .db_query_opt(
                &format!("{sql} ORDER BY g.id LIMIT 1{lock}"),
                &params,
                |_| Ok(()),
            )?
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub(super) fn require(
    conn: &impl DbExec,
    actor: &Actor,
    namespace: &str,
    mode: Mode,
    lock: bool,
) -> Result<(), Error> {
    if can(conn, actor, namespace, mode, lock)? {
        Ok(())
    } else {
        Err(denied())
    }
}

pub(super) fn require_scope(conn: &impl DbExec, actor: &Actor, scope: &Scope) -> Result<(), Error> {
    if matches!(actor, Actor::Scoped { read: false, .. }) {
        return Err(denied());
    }
    if let Scope::Namespace(namespace) | Scope::Domain(namespace) = scope {
        validate_namespace(namespace)?;
        return require(conn, actor, namespace, Mode::Read, false);
    }
    Ok(())
}

pub(super) fn visibility(actor: &Actor, scope: &Scope, params: &mut Vec<DbParam>) -> String {
    let selector = match scope {
        Scope::All => "1 = 1".into(),
        Scope::Org => "substr(d.namespace, 1, 4) = 'org:'".into(),
        Scope::Agent => "substr(d.namespace, 1, 6) = 'agent:'".into(),
        Scope::Domain(ns) | Scope::Namespace(ns) => {
            params.extend(db_params![ns]);
            format!("d.namespace = ?{}", params.len())
        }
    };
    let Actor::Agent(id) = actor else {
        return if matches!(actor, Actor::Scoped { read: false, .. }) {
            "1 = 0".into()
        } else {
            selector
        };
    };
    params.extend(db_params![format!("agent:{id}"), id.as_str()]);
    let own = params.len() - 1;
    let who = params.len();
    let [direct, group] = grant_queries(&format!("?{who}"), "g.mode = 'read'", "d.namespace");
    format!(
        "({selector}) AND (d.namespace = ?{own} OR (substr(d.namespace, 1, 6) <> 'agent:'
        AND EXISTS ({direct} UNION ALL {group})))"
    )
}

fn denied() -> Error {
    Error::PermissionDenied("knowledge capability required".into())
}

#[derive(Debug, Serialize)]
pub struct Grant {
    pub id: String,
    /// Nonempty for direct grants. The field is absent from group-grant JSON.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub agent_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    pub namespace_pattern: String,
    pub mode: String,
    pub granted_by: String,
    pub granted_at_ms: i64,
}

impl Store {
    pub fn knowledge_grant(
        &self,
        actor: &Actor,
        agent: &AgentId,
        pattern: &str,
        mode: Mode,
    ) -> Result<Grant, Error> {
        let owner = actor.grantor()?;
        validate_pattern(pattern)?;
        self.write_tx(|tx| {
            // The target is an identity, independent of its process state.
            agent_exists(tx, agent)?;
            tx.db_execute("INSERT INTO knowledge_grants (id, agent_id, namespace_pattern, mode, granted_by, granted_at_ms)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT (agent_id, mode, namespace_pattern) DO NOTHING",
                &db_params![new_id("kgrant"), agent.as_str(), pattern, mode.as_str(), owner, now_ms()])?;
            tx.db_query_one("SELECT id, agent_id, namespace_pattern, mode, granted_by, granted_at_ms, CAST(NULL AS TEXT) AS group_id FROM knowledge_grants
                WHERE agent_id = ?1 AND namespace_pattern = ?2 AND mode = ?3",
                &db_params![agent.as_str(), pattern, mode.as_str()], grant)
        })
    }

    pub fn knowledge_grant_group(
        &self,
        actor: &Actor,
        group: &GroupId,
        pattern: &str,
        mode: Mode,
    ) -> Result<Grant, Error> {
        let owner = actor.grantor()?;
        validate_pattern(pattern)?;
        self.write_tx(|tx| {
            let lock = if tx.backend() == DbBackend::Postgres { " FOR KEY SHARE" } else { "" };
            tx.db_query_opt(
                &format!("SELECT name FROM groups WHERE name = ?1{lock}"),
                &db_params![group.as_str()],
                |_| Ok(()),
            )?.ok_or(Error::KnowledgeMissing)?;
            tx.db_execute("INSERT INTO knowledge_group_grants (id, group_name, namespace_pattern, mode, granted_by, granted_at_ms)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT (group_name, mode, namespace_pattern) DO NOTHING",
                &db_params![new_id("kgrant"), group.as_str(), pattern, mode.as_str(), owner, now_ms()])?;
            tx.db_query_one("SELECT id, '' AS agent_id, namespace_pattern, mode, granted_by, granted_at_ms, group_name AS group_id
                FROM knowledge_group_grants WHERE group_name = ?1 AND namespace_pattern = ?2 AND mode = ?3",
                &db_params![group.as_str(), pattern, mode.as_str()], grant)
        })
    }

    /// An agent filter selects direct grants only; the unfiltered cursor spans both tables.
    pub fn knowledge_grants(
        &self,
        actor: &Actor,
        agent: Option<&AgentId>,
        after: &str,
        limit: usize,
    ) -> Result<Vec<Grant>, Error> {
        actor.grantor()?;
        self.read_conn().db_query_vec("SELECT id, agent_id, namespace_pattern, mode, granted_by, granted_at_ms, CAST(NULL AS TEXT) AS group_id
            FROM knowledge_grants WHERE (CAST(?1 AS TEXT) IS NULL OR agent_id = ?1) AND id > ?2
            UNION ALL
            SELECT id, '' AS agent_id, namespace_pattern, mode, granted_by, granted_at_ms, group_name AS group_id
            FROM knowledge_group_grants WHERE CAST(?1 AS TEXT) IS NULL AND id > ?2
            ORDER BY id LIMIT ?3",
            &db_params![agent.map(AgentId::as_str), after, limit.clamp(1, 101) as i64], grant)
    }

    pub fn knowledge_revoke(&self, actor: &Actor, id: &str) -> Result<bool, Error> {
        actor.grantor()?;
        self.write_tx(|tx| {
            let params = db_params![id];
            let direct = tx.db_execute("DELETE FROM knowledge_grants WHERE id = ?1", &params)?;
            let group =
                tx.db_execute("DELETE FROM knowledge_group_grants WHERE id = ?1", &params)?;
            Ok(direct != 0 || group != 0)
        })
    }
}

fn grant(r: &crate::db::DbRow<'_>) -> Result<Grant, Error> {
    Ok(Grant {
        id: r.get(0)?,
        agent_id: r.get(1)?,
        group_id: r.get(6)?,
        namespace_pattern: r.get(2)?,
        mode: r.get(3)?,
        granted_by: r.get(4)?,
        granted_at_ms: r.get(5)?,
    })
}
