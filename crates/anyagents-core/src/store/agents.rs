//! Agent CRUD, state transitions, scope/lifecycle policy, aliases, and rosters.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyagents_harness::{ExecutionRuntime, resolve_harness_and_runtime};
use rusqlite::Connection;

use super::helpers::{
    agent_by_id, agent_exists, agent_from_row_db, agent_summary_from_row_db, encode_env,
    refuse_reserved_agent_name, validate_token,
};
use super::schedules::cancel_archived_schedules;
use super::types::{Agent, AgentQueryMetadata, AgentSummary, Incoming, NewAgent, Wake};
use super::{ReadConn, Store, credentials, lifecycle, now_ms, registry, runtime, turn_errors};
use crate::db::{DbExec, DbExecExt};
use crate::db_params;
use crate::error::Error;
use crate::id::AgentId;

const AGENT_COLS: &str = agent_cols!();
const AGENT_LIST_COLS: &str = "id, name, harness, model, workdir, \
     CASE WHEN state = 'archived' THEN NULL ELSE instructions END AS instructions, \
     state, created_at_ms, launch_env, parent_id, is_admin, role, description, \
     runtime, remote_budget_micros";

fn budget_micros_to_db(cap_micros: Option<u64>) -> Result<Option<i64>, Error> {
    match cap_micros.filter(|&cap| cap > 0) {
        None => Ok(None),
        Some(cap) => i64::try_from(cap).map(Some).map_err(|_| {
            Error::Message(format!(
                "remote_budget_micros {cap} exceeds maximum supported i64"
            ))
        }),
    }
}

/// Default page size for unbounded `"all"` / `"archived"` roster queries.
pub const DEFAULT_ARCHIVE_ROSTER_LIMIT: usize = 200;

/// Options for SQL-paged roster queries ([`Store::roster_agents_paged`]).
#[derive(Clone, Copy, Debug, Default)]
pub struct RosterPageQuery<'a> {
    pub state: Option<&'a str>,
    pub parent_id: Option<&'a AgentId>,
    pub root_only: bool,
    pub include_descendants: bool,
    pub limit: Option<usize>,
    pub offset: usize,
}

fn is_scope_env_key(key: &str) -> bool {
    matches!(key, "ANYAGENTS_SCOPE" | "ANYAGENTS_PUBLIC")
}

fn is_policy_env_key(key: &str) -> bool {
    is_scope_env_key(key) || key == "ANYAGENTS_LIFECYCLE"
}

/// Validate and normalize an agent lifecycle (`"task"` or `"standing"`) without persisting state.
pub fn parse_agent_lifecycle(lifecycle: &str) -> Result<&'static str, Error> {
    match lifecycle.trim().to_ascii_lowercase().as_str() {
        "standing" | "permanent" | "daemon" | "resident" => Ok("standing"),
        "task" | "ephemeral" | "freelance" | "oneshot" => Ok("task"),
        other => Err(Error::Message(format!(
            "invalid lifecycle `{other}`; expected `task` or `standing`"
        ))),
    }
}

fn lifecycle_env(agent: &Agent, lifecycle: &str) -> Vec<(String, String)> {
    let mut env = agent.launch_env.clone();
    env.retain(|(key, _)| key != "ANYAGENTS_LIFECYCLE");
    env.push(("ANYAGENTS_LIFECYCLE".to_string(), lifecycle.to_string()));
    env
}

/// Validate and normalize an agent scope without persisting any state.
pub fn parse_agent_scope(scope: &str) -> Result<&'static str, Error> {
    match scope.trim().to_ascii_lowercase().as_str() {
        "isolated" | "none" => Ok("isolated"),
        "private" | "false" | "0" => Ok("private"),
        "siblings" | "sibling" | "team" => Ok("siblings"),
        "public" | "org" | "all" | "true" | "1" => Ok("public"),
        other => Err(Error::Message(format!(
            "invalid scope `{other}`; expected `isolated`, `private`, `siblings`, or `public`"
        ))),
    }
}

pub(super) fn scope_env(agent: &Agent, scope: &str) -> Vec<(String, String)> {
    let mut env = agent.launch_env.clone();
    env.retain(|(key, _)| !is_scope_env_key(key));
    env.push(("ANYAGENTS_SCOPE".to_string(), scope.to_string()));
    if scope == "public" {
        env.push(("ANYAGENTS_PUBLIC".to_string(), "1".to_string()));
    }
    env
}

impl Store {
    pub fn create_agent(&self, new: NewAgent) -> Result<Agent, Error> {
        validate_token(&new.name, "agent name")?;
        refuse_reserved_agent_name(&new.name)?;
        if new.harness.trim().is_empty() {
            return Err(Error::Message("harness is empty".into()));
        }
        let explicit_runtime = new
            .runtime
            .as_ref()
            .map(ExecutionRuntime::as_canonical_string);
        let resolved = resolve_harness_and_runtime(
            &new.harness,
            explicit_runtime.as_deref(),
            ExecutionRuntime::Local,
        )
        .map_err(|err| Error::Message(err.to_string()))?;
        if resolved.harness.is_empty() {
            return Err(Error::Message("harness is empty".into()));
        }
        let budget_db = budget_micros_to_db(new.remote_budget_micros)?;
        let budget_cap = budget_db.map(|v| v as u64);
        let clean_role = new
            .role
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let clean_description = new
            .description
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                new.instructions
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .and_then(|raw| raw.lines().next())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|line| line.chars().take(300).collect())
            });
        let runtime_str = resolved.runtime.as_canonical_string();
        let mut agent = Agent {
            is_admin: false,
            id: AgentId::generate(),
            name: new.name,
            role: clean_role,
            description: clean_description,
            harness: resolved.harness,
            runtime: resolved.runtime,
            remote_budget_micros: budget_cap,
            model: new.model,
            workdir: new.workdir,
            instructions: new.instructions,
            state: "created".into(),
            created_at_ms: now_ms(),
            launch_env: Vec::new(),
            parent_id: new.parent_id,
        };
        agent.launch_env = scope_env(&agent, agent.default_scope());
        self.write_tx(|tx| {
            let taken: i64 = tx.db_query_one(
                "SELECT COUNT(*) FROM agents WHERE name = ?1",
                &db_params![agent.name],
                |row| row.get(0),
            )?;
            if taken != 0 {
                return Err(Error::AgentExists(agent.name.clone()));
            }
            if lifecycle::spawn_reservation(tx, &agent.name)?.is_some() {
                return Err(Error::NameReserved(agent.name.clone()));
            }
            tx.db_execute(
                "INSERT INTO agents (
                    id, name, harness, model, workdir, instructions, state, created_at_ms, parent_id, role, description, launch_env, runtime, remote_budget_micros
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'created', ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                &db_params![
                    agent.id.as_str(),
                    agent.name,
                    agent.harness,
                    agent.model,
                    agent.workdir,
                    agent.instructions,
                    agent.created_at_ms,
                    agent.parent_id.as_ref().map(|id| id.as_str()),
                    agent.role,
                    agent.description,
                    encode_env(&agent.launch_env),
                    runtime_str,
                    budget_db,
                ],
            )?;
            tx.db_execute(
                "INSERT INTO mailbox_cursors (agent_id, delivered_seq) VALUES (?1, 0)",
                &db_params![agent.id.as_str()],
            )?;
            runtime::seed_runtime(tx, &agent.id)?;
            Ok(())
        })?;
        Ok(agent)
    }

    pub fn agent(&self, id: &AgentId) -> Result<Agent, Error> {
        agent_by_id(&self.read_conn(), id)
    }

    pub fn set_agent_admin(&self, agent_id: &AgentId, is_admin: bool) -> Result<(), Error> {
        self.write_tx(|tx| {
            let agent = agent_by_id(tx, agent_id)?;
            if is_admin && agent.parent_id.is_some() {
                return Err(Error::Message(
                    "a subagent cannot hold administrative authority".into(),
                ));
            }
            tx.db_execute(
                "UPDATE agents SET is_admin = ?2 WHERE id = ?1",
                &db_params![agent_id.as_str(), is_admin],
            )?;
            Ok(())
        })
    }

    pub fn set_agent_model(&self, agent_id: &AgentId, model: &str) -> Result<(), Error> {
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            tx.db_execute(
                "UPDATE agents SET model = ?2 WHERE id = ?1",
                &db_params![agent_id.as_str(), model],
            )?;
            Ok(())
        })?;
        self.notify(Wake::Agent {
            agent_id: agent_id.clone(),
        });
        Ok(())
    }

    pub fn set_agent_harness(&self, agent_id: &AgentId, harness: &str) -> Result<Agent, Error> {
        if harness.trim().is_empty() {
            return Err(Error::Message("harness is empty".into()));
        }
        let updated = self.write_tx(|tx| {
            let current = agent_by_id(tx, agent_id)?;
            let resolved = resolve_harness_and_runtime(harness, None, current.runtime)
                .map_err(|err| Error::Message(err.to_string()))?;
            if resolved.harness.is_empty() {
                return Err(Error::Message("harness is empty".into()));
            }
            let runtime_str = resolved.runtime.as_canonical_string();
            tx.db_execute(
                "UPDATE agents SET harness = ?2, runtime = ?3 WHERE id = ?1",
                &db_params![agent_id.as_str(), resolved.harness, runtime_str],
            )?;
            agent_by_id(tx, agent_id)
        })?;
        self.notify(Wake::Agent {
            agent_id: agent_id.clone(),
        });
        Ok(updated)
    }

    pub fn set_agent_runtime(
        &self,
        agent_id: &AgentId,
        runtime: &ExecutionRuntime,
    ) -> Result<(), Error> {
        let explicit = runtime.as_canonical_string();
        self.write_tx(|tx| {
            let current = agent_by_id(tx, agent_id)?;
            let resolved = resolve_harness_and_runtime(
                &current.canonical_harness(),
                Some(&explicit),
                ExecutionRuntime::Local,
            )
            .map_err(|err| Error::Message(err.to_string()))?;
            let runtime_str = resolved.runtime.as_canonical_string();
            tx.db_execute(
                "UPDATE agents SET harness = ?2, runtime = ?3 WHERE id = ?1",
                &db_params![agent_id.as_str(), resolved.harness, runtime_str],
            )?;
            Ok(())
        })?;
        self.notify(Wake::Agent {
            agent_id: agent_id.clone(),
        });
        Ok(())
    }

    pub fn set_agent_harness_and_runtime(
        &self,
        agent_id: &AgentId,
        harness: &str,
        runtime: &ExecutionRuntime,
        model: Option<&str>,
    ) -> Result<(), Error> {
        if harness.trim().is_empty() {
            return Err(Error::Message("harness is empty".into()));
        }
        let explicit = runtime.as_canonical_string();
        let resolved =
            resolve_harness_and_runtime(harness, Some(&explicit), ExecutionRuntime::Local)
                .map_err(|err| Error::Message(err.to_string()))?;
        if resolved.harness.is_empty() {
            return Err(Error::Message("harness is empty".into()));
        }
        let runtime_str = resolved.runtime.as_canonical_string();
        let clean_model = model.map(str::trim).filter(|s| !s.is_empty());
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            if let Some(m) = clean_model {
                tx.db_execute(
                    "UPDATE agents SET harness = ?2, runtime = ?3, model = ?4 WHERE id = ?1",
                    &db_params![agent_id.as_str(), resolved.harness, runtime_str, m],
                )?;
            } else {
                tx.db_execute(
                    "UPDATE agents SET harness = ?2, runtime = ?3 WHERE id = ?1",
                    &db_params![agent_id.as_str(), resolved.harness, runtime_str],
                )?;
            }
            Ok(())
        })?;
        self.notify(Wake::Agent {
            agent_id: agent_id.clone(),
        });
        Ok(())
    }

    pub fn set_agent_budget_micros(
        &self,
        agent_id: &AgentId,
        cap_micros: Option<u64>,
    ) -> Result<(), Error> {
        let db_cap = budget_micros_to_db(cap_micros)?;
        self.write_tx(|tx| {
            agent_exists(tx, agent_id)?;
            tx.db_execute(
                "UPDATE agents SET remote_budget_micros = ?2 WHERE id = ?1",
                &db_params![agent_id.as_str(), db_cap],
            )?;
            Ok(())
        })?;
        self.notify(Wake::Agent {
            agent_id: agent_id.clone(),
        });
        Ok(())
    }

    /// Rename an agent in place while preserving its immutable `AgentId`,
    /// mailbox, sessions, grants, and recording the old name in `agent_aliases`.
    pub fn rename_agent(&self, agent_id: &AgentId, new_name: &str) -> Result<Agent, Error> {
        let clean = new_name
            .trim()
            .strip_prefix("agent:")
            .unwrap_or(new_name.trim());
        validate_token(clean, "agent name")?;
        refuse_reserved_agent_name(clean)?;
        // The agent's search rows are refiled from the committed name.
        let (updated, changed) = self.reindex_around(&[], &[agent_id.as_str()], || {
            self.write_tx(|tx| {
                let current = agent_by_id(tx, agent_id)?;
                if current.name == clean {
                    return Ok((current, false));
                }
                let taken: i64 = tx.db_query_one(
                    "SELECT COUNT(*) FROM agents WHERE name = ?1 AND id != ?2",
                    &db_params![clean, agent_id.as_str()],
                    |row| row.get(0),
                )?;
                if taken != 0 {
                    return Err(Error::AgentExists(clean.to_string()));
                }
                tx.db_execute(
                    "UPDATE agents SET name = ?2 WHERE id = ?1",
                    &db_params![agent_id.as_str(), clean],
                )?;
                tx.db_execute(
                    "INSERT INTO agent_aliases (alias, agent_id, created_at_ms) VALUES (?1, ?2, ?3)
                     ON CONFLICT (alias) DO UPDATE SET agent_id = excluded.agent_id, created_at_ms = excluded.created_at_ms",
                    &db_params![current.name, agent_id.as_str(), now_ms()],
                )?;
                tx.db_execute("DELETE FROM agent_aliases WHERE alias = ?1", &db_params![clean])?;
                Ok((agent_by_id(tx, agent_id)?, true))
            })
        })?;
        if changed {
            self.notify(Wake::Agent {
                agent_id: agent_id.clone(),
            });
        }
        Ok(updated)
    }

    /// Set or clear an agent's short role label (e.g. `"Research"`, `"Design"`, `"Support"`).
    pub fn set_agent_role(&self, id: &AgentId, role: Option<&str>) -> Result<Agent, Error> {
        let clean = role.map(str::trim).filter(|s| !s.is_empty());
        self.write_tx(|tx| {
            agent_exists(tx, id)?;
            tx.db_execute(
                "UPDATE agents SET role = ?1 WHERE id = ?2",
                &db_params![clean, id.as_str()],
            )?;
            agent_by_id(tx, id)
        })
    }

    /// Transfer custody (reparent) of an agent to a new parent agent (or `None` to promote to root).
    /// Rejects self-parenting and cycles in the hierarchy tree.
    pub fn set_agent_parent(
        &self,
        agent_id: &AgentId,
        new_parent_id: Option<&AgentId>,
    ) -> Result<Agent, Error> {
        let updated = self.write_tx(|tx| {
            let current = agent_by_id(tx, agent_id)?;
            if let Some(np) = new_parent_id {
                if np == agent_id {
                    return Err(Error::Message(
                        "cannot transfer an agent to itself as parent".into(),
                    ));
                }
                let mut cursor = Some(np.clone());
                let mut hops = 0usize;
                while let Some(cid) = cursor {
                    if &cid == agent_id {
                        return Err(Error::Message(
                            "cannot transfer an agent under its own descendant (cycle detected)"
                                .into(),
                        ));
                    }
                    hops += 1;
                    if hops > 64 {
                        break;
                    }
                    let anc = agent_by_id(tx, &cid)?;
                    cursor = anc.parent_id;
                }
            }
            let parent_str = new_parent_id.map(AgentId::as_str);
            tx.db_execute(
                "UPDATE agents SET parent_id = ?1 WHERE id = ?2",
                &db_params![parent_str, current.id.as_str()],
            )?;
            agent_by_id(tx, agent_id)
        })?;
        self.notify(Wake::Agent {
            agent_id: agent_id.clone(),
        });
        Ok(updated)
    }

    /// Former names recorded for this agent via `rename_agent`.
    pub fn agent_aliases(&self, id: &AgentId) -> Result<Vec<String>, Error> {
        self.read_conn().db_query_vec(
            "SELECT alias FROM agent_aliases WHERE agent_id = ?1 ORDER BY created_at_ms DESC",
            &db_params![id.as_str()],
            |row| row.get(0),
        )
    }

    /// Administrative metadata only; callers must never pass credentials here.
    pub fn record_admin_audit(&self, actor: &str, action: &str, target: &str) -> Result<(), Error> {
        self.write_tx(|tx| {
            tx.db_execute(
                "INSERT INTO admin_audit (created_at, actor, action, target) VALUES (?1, ?2, ?3, ?4)",
                &db_params![now_ms(), actor, action, target],
            )?;
            Ok(())
        })
    }

    /// Set or clear an agent's public capability/charter description (`description` column, Layer 1).
    pub fn set_agent_description(
        &self,
        id: &AgentId,
        description: Option<&str>,
    ) -> Result<Agent, Error> {
        let clean = description.map(str::trim).filter(|s| !s.is_empty());
        self.write_tx(|tx| {
            agent_exists(tx, id)?;
            tx.db_execute(
                "UPDATE agents SET description = ?1 WHERE id = ?2",
                &db_params![clean, id.as_str()],
            )?;
            agent_by_id(tx, id)
        })
    }

    pub fn set_agent_instructions(
        &self,
        id: &AgentId,
        instructions: Option<&str>,
    ) -> Result<Agent, Error> {
        let clean = instructions.map(str::trim).filter(|s| !s.is_empty());
        self.write_tx(|tx| {
            let current = agent_by_id(tx, id)?;
            let derived_desc = if current.description.is_none() {
                clean
                    .and_then(|raw| raw.lines().next())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|line| line.chars().take(300).collect::<String>())
            } else {
                current.description
            };
            tx.db_execute(
                "UPDATE agents SET instructions = ?1, description = ?2 WHERE id = ?3",
                &db_params![clean, derived_desc, id.as_str()],
            )?;
            agent_by_id(tx, id)
        })
    }

    pub fn agent_by_name(&self, name: &str) -> Result<Agent, Error> {
        let lookup = name.strip_prefix("agent:").unwrap_or(name);
        let conn = self.read_conn();
        if let Some(agent) = conn.db_query_opt(
            concat!(
                "SELECT ",
                agent_cols!(),
                " FROM agents WHERE name = ?1 OR id = ?1"
            ),
            &db_params![lookup],
            agent_from_row_db,
        )? {
            return Ok(agent);
        }
        let aliased = conn.db_query_opt(
            "SELECT a.id, a.name, a.harness, a.model, a.workdir, a.instructions, a.state, \
                    a.created_at_ms, a.launch_env, a.parent_id, a.is_admin, a.role, \
                    a.description, a.runtime, a.remote_budget_micros \
             FROM agent_aliases al \
             JOIN agents a ON a.id = al.agent_id \
             WHERE al.alias = ?1",
            &db_params![lookup],
            agent_from_row_db,
        )?;
        aliased.ok_or_else(|| Error::AgentNotFound(name.to_string()))
    }

    pub fn list_agents(&self) -> Result<Vec<Agent>, Error> {
        self.read_conn().db_query_vec(
            concat!("SELECT ", agent_cols!(), " FROM agents ORDER BY name"),
            &[],
            agent_from_row_db,
        )
    }

    /// Every agent that is not archived, ordered by name. Hot paths use this
    /// instead of [`Store::list_agents`] so their cost follows the live fleet,
    /// not the ever-growing archive (served by `idx_agents_state`).
    pub fn list_live_agents(&self) -> Result<Vec<Agent>, Error> {
        self.read_conn().db_query_vec(
            concat!(
                "SELECT ",
                agent_cols!(),
                " FROM agents WHERE state IN ('created', 'running', 'idle', 'paused') ORDER BY name"
            ),
            &[],
            agent_from_row_db,
        )
    }

    /// Agents a roster needs, without reading the archive: the agents whose
    /// state matches (`None` is every non-archived agent, `"all"` every agent),
    /// plus every ancestor of a match so a tree keeps its path to the root.
    /// The second value is the number of archived agents. Ordered by name.
    pub fn roster_agents(&self, state: Option<&str>) -> Result<(Vec<Agent>, usize), Error> {
        let conn = self.read_conn();
        let archived: i64 = conn.db_query_one(
            "SELECT count(*) FROM agents WHERE state = 'archived'",
            &[],
            |r| r.get(0),
        )?;
        let agents = match state {
            Some("all") => conn.db_query_vec(
                &format!("SELECT {AGENT_COLS} FROM agents ORDER BY name"),
                &[],
                agent_from_row_db,
            )?,
            Some("archived") => conn.db_query_vec(
                &format!("SELECT {AGENT_COLS} FROM agents WHERE state = 'archived' ORDER BY name"),
                &[],
                agent_from_row_db,
            )?,
            _ => {
                // UNION (not UNION ALL) deduplicates, so a parent cycle ends.
                conn.db_query_vec(
                    &format!(
                        "WITH RECURSIVE wanted(id) AS (
                             SELECT id FROM agents
                              WHERE state != 'archived' AND (?1 IS NULL OR state = ?1)
                             UNION
                             SELECT a.parent_id FROM agents a JOIN wanted w ON a.id = w.id
                              WHERE a.parent_id IS NOT NULL
                         )
                         SELECT {AGENT_COLS} FROM agents WHERE id IN (SELECT id FROM wanted)
                         ORDER BY name"
                    ),
                    &db_params![state],
                    agent_from_row_db,
                )?
            }
        };
        Ok((agents, archived as usize))
    }

    /// SQL-paged roster query for `"all"` and `"archived"` states (pushing
    /// `LIMIT`/`OFFSET`, non-archived-first `created_at_ms DESC, name ASC`
    /// ordering, `NULL` archived `instructions`, and optional `WITH RECURSIVE`
    /// descendant traversal directly into SQLite/PostgreSQL).
    ///
    /// Always reports the exact total `archived_count`. For live states
    /// (`None`, `"running"`, `"idle"`, `"paused"`, `"created"`), delegates to
    /// [`Self::roster_agents`].
    pub fn roster_agents_paged(
        &self,
        query: RosterPageQuery<'_>,
    ) -> Result<(Vec<Agent>, usize), Error> {
        if !matches!(query.state, Some("all" | "archived")) {
            return self.roster_agents(query.state);
        }
        let conn = self.read_conn();
        let archived: i64 = conn.db_query_one(
            "SELECT count(*) FROM agents WHERE state = 'archived'",
            &[],
            |r| r.get(0),
        )?;
        let limit = i64::try_from(query.limit.unwrap_or(DEFAULT_ARCHIVE_ROSTER_LIMIT).max(1))
            .unwrap_or(i64::MAX);
        let offset = i64::try_from(query.offset).unwrap_or(i64::MAX);
        let state_clause = if query.state == Some("archived") {
            "state = 'archived'"
        } else {
            "1 = 1"
        };
        let order_clause = "ORDER BY (CASE WHEN state = 'archived' THEN 1 ELSE 0 END) ASC, \
                            created_at_ms DESC, name ASC";

        let agents = if let Some(pid) = query.parent_id {
            if query.include_descendants {
                let sql = format!(
                    "WITH RECURSIVE descendants(id) AS (
                         SELECT id FROM agents WHERE parent_id = ?1
                         UNION
                         SELECT a.id FROM agents a JOIN descendants d ON a.parent_id = d.id
                     )
                     SELECT {AGENT_LIST_COLS} FROM agents
                     WHERE id IN (SELECT id FROM descendants) AND {state_clause}
                     {order_clause}
                     LIMIT ?2 OFFSET ?3"
                );
                conn.db_query_vec(
                    &sql,
                    &db_params![pid.as_str(), limit, offset],
                    agent_from_row_db,
                )?
            } else {
                let sql = format!(
                    "SELECT {AGENT_LIST_COLS} FROM agents
                     WHERE parent_id = ?1 AND {state_clause}
                     {order_clause}
                     LIMIT ?2 OFFSET ?3"
                );
                conn.db_query_vec(
                    &sql,
                    &db_params![pid.as_str(), limit, offset],
                    agent_from_row_db,
                )?
            }
        } else if query.root_only {
            let sql = format!(
                "SELECT {AGENT_LIST_COLS} FROM agents
                 WHERE parent_id IS NULL AND {state_clause}
                 {order_clause}
                 LIMIT ?1 OFFSET ?2"
            );
            conn.db_query_vec(&sql, &db_params![limit, offset], agent_from_row_db)?
        } else {
            let sql = format!(
                "SELECT {AGENT_LIST_COLS} FROM agents
                 WHERE {state_clause}
                 {order_clause}
                 LIMIT ?1 OFFSET ?2"
            );
            conn.db_query_vec(&sql, &db_params![limit, offset], agent_from_row_db)?
        };
        Ok((agents, archived as usize))
    }

    /// Load all agent aliases in a single query (`agent_id -> sorted aliases`).
    pub fn agent_aliases_map(
        &self,
    ) -> Result<std::collections::HashMap<String, Vec<String>>, Error> {
        let conn = self.read_conn();
        let mut aliases: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for (id, alias) in conn.db_query_vec(
            "SELECT agent_id, alias FROM agent_aliases ORDER BY alias",
            &[],
            |r| Ok((r.get::<String>(0)?, r.get::<String>(1)?)),
        )? {
            aliases.entry(id).or_default().push(alias);
        }
        Ok(aliases)
    }

    /// Load roster metadata with a fixed number of SQL statements. Indexed range
    /// seeks avoid scanning the message history of archived agents.
    pub fn agent_query_metadata(&self) -> Result<AgentQueryMetadata, Error> {
        let mut metadata = AgentQueryMetadata {
            aliases: self.agent_aliases_map()?,
            ..AgentQueryMetadata::default()
        };
        {
            let conn = self.read_conn();
            let not_passive = conn.backend().sql_not_passive_reaction("m.context");
            let oldest_sql = format!(
                "SELECT a.id, (SELECT m.received_at_ms FROM messages m
                   WHERE m.agent_id = a.id AND m.seq > COALESCE(d.delivered_seq, 0)
                     AND {not_passive}
                   ORDER BY m.seq LIMIT 1)
                 FROM agents a LEFT JOIN mailbox_cursors d ON d.agent_id = a.id
                 WHERE a.state != 'archived'"
            );
            for (id, timestamp) in conn.db_query_vec(&oldest_sql, &[], |r| {
                Ok((r.get::<String>(0)?, r.get::<Option<i64>>(1)?))
            })? {
                if let Some(timestamp) = timestamp {
                    metadata.oldest_undelivered.insert(id, timestamp);
                }
            }
            for (id, timestamp) in conn.db_query_vec(
                "SELECT t.agent_id, COALESCE((SELECT m.received_at_ms FROM messages m
                    WHERE m.agent_id = t.agent_id AND m.seq > t.completed_seq AND m.seq <= t.opened_seq
                    ORDER BY m.seq LIMIT 1), t.opened_at_ms)
                 FROM turn_recovery t JOIN agents a ON a.id = t.agent_id
                 WHERE t.opened_seq > t.completed_seq AND a.state != 'archived'",
                &[],
                |r| Ok((r.get::<String>(0)?, r.get::<i64>(1)?)),
            )? {
                metadata.open_turns.insert(id, timestamp);
            }
        }
        for schedule in self.list_schedules(None, true)? {
            metadata
                .schedules
                .entry(schedule.agent_id.to_string())
                .or_default()
                .push(schedule);
        }
        metadata.turn_errors = self
            .current_turn_errors()?
            .into_iter()
            .map(|(id, errors)| (id.to_string(), errors))
            .collect();
        let fresh_since =
            now_ms().saturating_sub(crate::stateless::ACTIVE_REMOTE_TURN_FRESHNESS_MS);
        metadata.active_remote_turns = self
            .remote_turn_store()
            .list_active_fresh_by_agent(fresh_since)
            .unwrap_or_default();
        metadata.active_proc_locks = self.active_agent_proc_locks();
        Ok(metadata)
    }

    pub fn children_of(&self, parent_id: &AgentId) -> Result<Vec<Agent>, Error> {
        self.read_conn().db_query_vec(
            concat!(
                "SELECT ",
                agent_cols!(),
                " FROM agents WHERE parent_id = ?1 ORDER BY created_at_ms, name"
            ),
            &db_params![parent_id.as_str()],
            agent_from_row_db,
        )
    }

    /// A parent's children that are not archived, oldest first. Served by a
    /// partial index, so a parent's archived history is never read.
    pub fn live_children_of(&self, parent_id: &AgentId) -> Result<Vec<Agent>, Error> {
        self.read_conn().db_query_vec(
            concat!(
                "SELECT ",
                agent_cols!(),
                " FROM agents WHERE parent_id = ?1 AND state != 'archived'
                 ORDER BY created_at_ms, name"
            ),
            &db_params![parent_id.as_str()],
            agent_from_row_db,
        )
    }

    /// Ids of the agents in one lifecycle state, by name, from the state index.
    pub fn agent_ids_in_state(&self, state: &str) -> Result<Vec<AgentId>, Error> {
        let ids = self.read_conn().db_query_vec(
            "SELECT id FROM agents WHERE state = ?1 ORDER BY name",
            &db_params![state],
            |row| row.get::<String>(0),
        )?;
        ids.into_iter()
            .map(|id| AgentId::parse(&id))
            .collect::<Result<_, Error>>()
    }

    /// Every agent (or every non-archived one) by name, without reading
    /// instructions or decoding launch environments.
    pub fn agent_summaries(&self, include_archived: bool) -> Result<Vec<AgentSummary>, Error> {
        let sql = if include_archived {
            concat!(
                "SELECT ",
                agent_summary_cols!(),
                " FROM agents ORDER BY name"
            )
        } else {
            concat!(
                "SELECT ",
                agent_summary_cols!(),
                " FROM agents WHERE state != 'archived' ORDER BY name"
            )
        };
        self.read_conn()
            .db_query_vec(sql, &[], agent_summary_from_row_db)
    }

    /// Replace the harness launch environment, preserving communication policy.
    /// Only `set_agent_scope` owns `ANYAGENTS_SCOPE` / `ANYAGENTS_PUBLIC`;
    /// gateway refreshes and stale environment snapshots must not overwrite them.
    /// Never called with a provider key.
    pub fn set_agent_launch_env(
        &self,
        id: &AgentId,
        env: &[(String, String)],
    ) -> Result<Agent, Error> {
        self.update_agent_launch_env(id, None, |current| {
            let mut refreshed: Vec<_> = env
                .iter()
                .filter(|(key, _)| !is_policy_env_key(key))
                .cloned()
                .collect();
            refreshed.extend(
                current
                    .launch_env
                    .iter()
                    .filter(|(key, _)| is_policy_env_key(key))
                    .cloned(),
            );
            refreshed
        })
    }

    fn update_agent_launch_env(
        &self,
        id: &AgentId,
        audit: Option<(&str, &str)>,
        update: impl FnOnce(&Agent) -> Vec<(String, String)>,
    ) -> Result<Agent, Error> {
        let changed = self.write_tx(|tx| {
            // Read and merge under the same write transaction, including when
            // callers use separate Store connections.
            let current = agent_by_id(tx, id)?;
            let json = encode_env(&update(&current));
            // A database written before this column existed still reads: the
            // harness then keeps whatever sign-in it already has.
            let updated = tx
                .db_execute(
                    "UPDATE agents SET launch_env = ?1 WHERE id = ?2",
                    &db_params![json, id.as_str()],
                )
                .map_err(|err| {
                    if err.is_missing_schema_object() {
                        Error::Message(
                            "this database has no launch environment column yet".to_string(),
                        )
                    } else {
                        err
                    }
                })?;
            if updated == 0 {
                return Err(Error::AgentNotFound(id.to_string()));
            }
            let changed = agent_by_id(tx, id)?;
            if let Some((actor, source)) = audit {
                tx.db_execute(
                    "INSERT INTO admin_audit (created_at, actor, action, target, before_scope, after_scope, source)
                     VALUES (?1, ?2, 'agents.scope', ?3, ?4, ?5, ?6)",
                    &db_params![now_ms(), actor, changed.name, current.scope(), changed.scope(), source],
                )?;
            }
            Ok(changed)
        })?;
        self.notify(Wake::Agent {
            agent_id: id.clone(),
        });
        Ok(changed)
    }

    /// Persist an allocated workdir without starting or waking the agent.
    /// Recovery may fill a missing value, but must not replace a newer one.
    pub fn set_agent_workdir_if_missing(
        &self,
        id: &AgentId,
        workdir: &str,
    ) -> Result<Agent, Error> {
        self.write_tx(|tx| {
            tx.db_execute(
                "UPDATE agents SET workdir = ?1 WHERE id = ?2 AND workdir IS NULL",
                &db_params![workdir, id.as_str()],
            )?;
            agent_by_id(tx, id)
        })
    }

    /// Store the work directory and mark the agent running.
    pub fn set_agent_running(&self, id: &AgentId, workdir: &str) -> Result<Agent, Error> {
        let changed = self.write_tx(|tx| {
            let updated = tx.db_execute(
                "UPDATE agents SET workdir = ?1, state = 'running' WHERE id = ?2",
                &db_params![workdir, id.as_str()],
            )?;
            if updated == 0 {
                return Err(Error::AgentNotFound(id.to_string()));
            }
            agent_by_id(tx, id)
        })?;
        self.notify(Wake::Agent {
            agent_id: id.clone(),
        });
        Ok(changed)
    }

    /// Update an agent's lifecycle state (`running`, `idle`, `paused`, `archived`, `stopped`) and
    /// notify the supervisor so it starts or stops the worker slot.
    pub fn set_agent_state(&self, id: &AgentId, state: &str) -> Result<Agent, Error> {
        let changed = self.write_tx(|tx| {
            let updated = tx.db_execute(
                "UPDATE agents SET state = ?1 WHERE id = ?2",
                &db_params![state, id.as_str()],
            )?;
            if updated == 0 {
                return Err(Error::AgentNotFound(id.to_string()));
            }
            if state == "running" {
                turn_errors::clear_parking(tx, id)?;
            } else if state == "archived" {
                cancel_archived_schedules(tx, id)?;
            }
            agent_by_id(tx, id)
        })?;
        self.notify(Wake::Agent {
            agent_id: id.clone(),
        });
        Ok(changed)
    }

    /// Atomically transition any running agent to
    /// `idle` (process released / scaled to zero) when it has delivered every
    /// message in its mailbox, completed its latest turn
    /// (`completed_seq >= opened_seq >= max_seq > 0`), and has no active
    /// (`running`) child sub-agents of its own.
    ///
    /// Returns `Ok(true)` when the agent transitioned to `idle` (and
    /// notifies the supervisor to stop its worker slot), or `Ok(false)` if new
    /// mail arrived, a turn is still open, or a child sub-agent is still running.
    pub fn complete_idle_agent(&self, id: &AgentId) -> Result<bool, Error> {
        let err_file =
            crate::agent_config_dir(self.data_dir(), id).join(".anyagents-last-error.json");
        if let Ok(raw) = std::fs::read_to_string(&err_file)
            && let Ok(val) = serde_json::from_str::<serde_json::Value>(&raw)
            && val
                .get("error")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|s| !s.trim().is_empty())
        {
            return Ok(false);
        }
        // Read-only precheck: an agent that cannot scale to zero (a running
        // child, mail past its cursor, no completed turn) is reviewed on every
        // idle pass and must not take the writer lock each time. The UPDATE
        // below re-checks the same predicate under the lock.
        let eligible: bool = self.read_conn().db_query_one(
            concat!(
                "SELECT EXISTS(SELECT 1 FROM agents WHERE ",
                idle_release_predicate!(),
                ")"
            ),
            &db_params![id.as_str()],
            |row| row.get(0),
        )?;
        if !eligible {
            return Ok(false);
        }
        let (transitioned, parent_to_wake, silent_completion) = self.write_tx(|tx| {
            let parent_id_opt: Option<AgentId> = tx.db_query_opt(
                "SELECT parent_id FROM agents WHERE id = ?1 AND parent_id IS NOT NULL",
                &db_params![id.as_str()],
                |r| r.parse_str(0),
            )?;
            let silent_info: Option<(AgentId, String, i64)> = tx.db_query_opt(
                "SELECT a.parent_id, a.name, tr.completed_seq
                 FROM agents a
                 JOIN agents p ON p.id = a.parent_id
                 JOIN turn_recovery tr ON tr.agent_id = a.id
                 WHERE a.id = ?1
                   AND a.parent_id IS NOT NULL
                   AND a.state = 'running'
                   AND NOT EXISTS (
                       SELECT 1 FROM task_turn_contexts t
                       WHERE t.child_id = a.id AND t.closed_delivery_seq = tr.completed_seq
                   )
                   AND EXISTS (
                       SELECT 1 FROM messages m
                       WHERE m.agent_id = ?1
                         AND m.seq >= tr.opened_seq
                         AND m.sender = 'agent:' || p.name
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM outbound_messages om
                       WHERE om.agent_id = ?1
                         AND om.sent_at_ms >= tr.opened_at_ms
                   )",
                &db_params![id.as_str()],
                |r| Ok((r.parse_str(0)?, r.get(1)?, r.get(2)?)),
            )?;
            let updated = tx.db_execute(
                concat!(
                    "UPDATE agents SET state = 'idle' WHERE ",
                    idle_release_predicate!()
                ),
                &db_params![id.as_str()],
            )?;
            Ok((
                updated > 0,
                if updated > 0 { parent_id_opt } else { None },
                if updated > 0 { silent_info } else { None },
            ))
        })?;
        if transitioned {
            self.notify(Wake::Agent {
                agent_id: id.clone(),
            });
            if let Some(pid) = parent_to_wake {
                self.notify(Wake::Agent { agent_id: pid });
            }
            if let Some((parent_id, sub_name, completed_seq)) = silent_completion {
                let notice = format!(
                    "[Sub-Agent Completed] Sub-agent `agent:{sub_name}` (`{id}`) finished its turn and scaled to zero (`state = idle`) without sending a direct reply. Call `subagents({{ \"action\": \"inspect\", \"agent\": \"{sub_name}\" }})` to read its output or `subagents({{ \"action\": \"steer\", \"agent\": \"{sub_name}\", \"prompt\": \"...\" }})` to continue."
                );
                let _ = self.receive(
                    &parent_id,
                    Incoming {
                        text: notice,
                        sender: Some(format!("agent:{sub_name}")),
                        source_time_ms: Some(now_ms()),
                        source_key: Some(format!("subagent_completed:{id}:{completed_seq}")),
                        reply_to: Some(format!("agent:{sub_name}")),
                        quote: None,
                        origin: None,
                        received_at_ms: None,
                    },
                );
            }
        }
        Ok(transitioned)
    }

    /// Compatibility alias; idle release now applies to roots and children alike.
    pub fn complete_idle_subagent(&self, id: &AgentId) -> Result<bool, Error> {
        self.complete_idle_agent(id)
    }

    /// A creation that failed before admission must not keep its name reserved.
    /// A created row may already have an allocated workdir; allocation alone
    /// does not admit it. Archive the row, clear its workdir, and rename it to
    /// its own id, which can never collide with a chosen name. The id, history
    /// and journal are untouched. Admitted agents are refused.
    pub fn release_unadmitted_agent(&self, id: &AgentId) -> Result<Agent, Error> {
        let released = self.write_tx(|tx| {
            let changed = tx.db_execute(
                "UPDATE agents SET name = id, state = 'archived', workdir = NULL
                 WHERE id = ?1 AND (workdir IS NULL OR state = 'created') AND is_admin = 0",
                &db_params![id.as_str()],
            )?;
            if changed == 0 {
                agent_exists(tx, id)?;
                return Err(Error::Message(format!(
                    "agent {id} was admitted; its name is not a failed reservation"
                )));
            }
            cancel_archived_schedules(tx, id)?;
            agent_by_id(tx, id)
        })?;
        self.notify(Wake::Agent {
            agent_id: id.clone(),
        });
        Ok(released)
    }

    /// Startup repair, run while the server lock excludes concurrent creation:
    /// every row that never reached admission is a failed or interrupted
    /// creation. Release each one's name. Returns the released `(id, name)`s.
    pub fn release_unadmitted_names(&self) -> Result<Vec<(AgentId, String)>, Error> {
        self.release_unadmitted_names_older_than(0)
    }

    /// Release unadmitted agent name reservations created at least `min_age_ms`
    /// ago (and, on PostgreSQL, without a fresh runtime heartbeat), so peer
    /// coordinator replicas booting in multi-replica mode never clobber an
    /// in-flight `create_agent` on another pod.
    pub fn release_unadmitted_names_older_than(
        &self,
        min_age_ms: i64,
    ) -> Result<Vec<(AgentId, String)>, Error> {
        let cutoff = now_ms().saturating_sub(min_age_ms.max(0));
        let rows: Vec<(String, String)> = if min_age_ms > 0 && self.is_postgres() {
            self.read_conn().db_query_vec(
                "SELECT a.id, a.name
                 FROM agents a
                 LEFT JOIN agent_runtime r ON r.agent_id = a.id
                 WHERE (a.workdir IS NULL OR a.state = 'created')
                   AND a.is_admin = 0
                   AND a.name != a.id
                   AND a.state IN ('created', 'archived')
                   AND a.created_at_ms <= ?1
                   AND COALESCE(r.heartbeat_at_ms, 0) <= ?1",
                &db_params![cutoff],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?
        } else if min_age_ms > 0 {
            self.read_conn().db_query_vec(
                "SELECT id, name FROM agents
                 WHERE (workdir IS NULL OR state = 'created') AND is_admin = 0 AND name != id
                   AND state IN ('created', 'archived')
                   AND created_at_ms <= ?1",
                &db_params![cutoff],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?
        } else {
            self.read_conn().db_query_vec(
                "SELECT id, name FROM agents
                 WHERE (workdir IS NULL OR state = 'created') AND is_admin = 0 AND name != id
                   AND state IN ('created', 'archived')",
                &[],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?
        };
        let mut released = Vec::new();
        for (id, name) in rows {
            let id = AgentId::parse(&id)?;
            self.release_unadmitted_agent(&id)?;
            released.push((id, name));
        }
        Ok(released)
    }

    pub fn delete_agent(&self, id: &AgentId) -> Result<Agent, Error> {
        self.delete_agent_by(id, "unattributed")
    }

    /// Delete an agent and everything keyed by it in one commit. Its credential
    /// bindings are removed with one audit row each, naming `actor`. The
    /// lifecycle journal and binding audit are kept.
    pub fn delete_agent_by(&self, id: &AgentId, actor: &str) -> Result<Agent, Error> {
        // The agent's search rows leave the index after the commit, in the
        // index's own transaction; the pending entry recorded first covers an
        // interruption in between.
        self.invalidate_index(&[], &[id.as_str()])?;
        let (deleted, max_rowids) = self.write_tx(|tx| {
            let agent = agent_by_id(tx, id)?;
            credentials::delete_bindings_for_deleted_agent(tx, id, actor)?;
            for table in [
                "agent_runtime",
                "turns",
                // Never written; kept, and kept cleared, for one release so a
                // rollback to a binary that still deletes from it can delete
                // agents. Drop the table and this entry together after that.
                "steer_holds",
                "rejected_messages",
                "session_leases",
                "quota_blocks",
                "harness_leases",
                "agent_aliases",
                "agent_capabilities",
                "remote_session_tokens",
                "usage_records",
                "outcomes",
                "skill_reads",
                "mailbox_cursors",
                "turn_recovery",
                "turn_errors",
                "messages",
                "message_versions",
                "outbound_messages",
                "group_agents",
                "channel_agents",
            ] {
                tx.db_execute(
                    &format!("DELETE FROM {table} WHERE agent_id = ?1"),
                    &db_params![id.as_str()],
                )?;
            }
            tx.db_execute("DELETE FROM agents WHERE id = ?1", &db_params![id.as_str()])?;
            // Every row inserted after this commit lies above these, even
            // one that reuses a rowid the deleted rows held.
            let max_rowids = tx.db_query_one(
                "SELECT (SELECT COALESCE(MAX(rowid), 0) FROM messages),
                        (SELECT COALESCE(MAX(rowid), 0) FROM outbound_messages)",
                &[],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            Ok((agent, max_rowids))
        })?;
        self.memory_index().agent_deleted(id.as_str(), max_rowids)?;
        self.notify(Wake::Agent {
            agent_id: id.clone(),
        });
        Ok(deleted)
    }

    /// Set the internal communication and discovery scope (`"isolated"`, `"private"`, `"siblings"`, or `"public"`).
    pub fn set_agent_scope(&self, agent_id: &AgentId, scope: &str) -> Result<Agent, Error> {
        self.set_agent_scope_as(agent_id, scope, "system")
    }

    /// Set scope and atomically record its authenticated actor and before/after policy.
    pub fn set_agent_scope_as(
        &self,
        agent_id: &AgentId,
        scope: &str,
        actor: &str,
    ) -> Result<Agent, Error> {
        let normalized = parse_agent_scope(scope)?;
        self.update_agent_launch_env(agent_id, Some((actor, "scope")), |current| {
            scope_env(current, normalized)
        })
    }

    /// Set the agent lifecycle mode (`"task"` or `"standing"`).
    pub fn set_agent_lifecycle(&self, agent_id: &AgentId, lifecycle: &str) -> Result<Agent, Error> {
        self.set_agent_lifecycle_as(agent_id, lifecycle, "system")
    }

    /// Set lifecycle mode (`"task"` or `"standing"`).
    pub fn set_agent_lifecycle_as(
        &self,
        agent_id: &AgentId,
        lifecycle: &str,
        _actor: &str,
    ) -> Result<Agent, Error> {
        let normalized = parse_agent_lifecycle(lifecycle)?;
        self.update_agent_launch_env(agent_id, None, |current| lifecycle_env(current, normalized))
    }

    /// Set whether a sub-agent accepts external messages from agents outside its
    /// direct parent/child lineage (`ANYAGENTS_PUBLIC=1` in `launch_env`).
    pub fn set_agent_public(&self, agent_id: &AgentId, public: bool) -> Result<Agent, Error> {
        self.set_agent_public_as(agent_id, public, "system")
    }

    /// The legacy boolean only grants/revokes public reachability. Revocation
    /// restores the default for public agents and preserves all nonpublic scopes.
    pub fn set_agent_public_as(
        &self,
        agent_id: &AgentId,
        public: bool,
        actor: &str,
    ) -> Result<Agent, Error> {
        self.update_agent_launch_env(agent_id, Some((actor, "public")), |current| {
            if public {
                scope_env(current, "public")
            } else if current.scope() == "public" {
                scope_env(current, current.default_scope())
            } else {
                current.launch_env.clone()
            }
        })
    }
}

/// `{workspaces}/{id}` when `ANYAGENTS_WORKSPACES_DIR` is configured,
/// otherwise `{data}/agents/{id}/work`.
pub fn agent_work_dir(data_dir: &Path, id: &AgentId) -> PathBuf {
    if let Some(workspaces) = crate::storage::configured_workspaces_dir() {
        return workspaces.join(id.as_str());
    }
    legacy_agent_work_dir(data_dir, id)
}

/// Single-root `{data}/agents/{id}/work` path.
pub fn legacy_agent_work_dir(data_dir: &Path, id: &AgentId) -> PathBuf {
    data_dir.join("agents").join(id.as_str()).join("work")
}

/// `{data}/agents/{id}/config`.
pub fn agent_config_dir(data_dir: &Path, id: &AgentId) -> PathBuf {
    data_dir.join("agents").join(id.as_str()).join("config")
}

/// `{data}/locks/{id}.lock`: the agent's process lock, held by whatever holds
/// its vendor process (a delivery loop, a keeper).
pub fn agent_lock_path(data_dir: &Path, id: &AgentId) -> PathBuf {
    data_dir.join("locks").join(format!("{}.lock", id.as_str()))
}

fn open_roster_conn(data_dir: &Path) -> Option<ReadConn<'static>> {
    if let Some(url) = crate::db::configured_database_url_for_data_dir(data_dir) {
        return crate::db::shared_pg_handle(&url)
            .ok()
            .map(|h| ReadConn::Postgres(h.pool));
    }
    let db_path = data_dir.join("state.db");
    if !db_path.is_file() {
        return None;
    }
    if let Some(reader) = registry::reader_if_open(&db_path) {
        return Some(reader);
    }
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = Connection::open_with_flags(&db_path, flags).ok()?;
    let _ = conn.busy_timeout(Duration::ZERO);
    let _ = conn.pragma_update(None, "query_only", "ON");
    Some(ReadConn::Owned(Some(conn)))
}

fn roster_entry(
    name: String,
    role: Option<String>,
    description: Option<String>,
    instructions: Option<String>,
) -> (String, Option<String>, Option<String>) {
    let dummy = Agent {
        id: AgentId::generate(),
        name: name.clone(),
        role: role.clone(),
        parent_id: None,
        harness: String::new(),
        runtime: ExecutionRuntime::Local,
        remote_budget_micros: None,
        model: None,
        workdir: None,
        description,
        instructions,
        is_admin: false,
        state: "running".to_string(),
        created_at_ms: 0,
        launch_env: Vec::new(),
    };
    (name, role, dummy.public_description())
}

/// Reads the active root agent roster `(name, role, public_description)` from
/// `{data}/state.db` or PostgreSQL read-only when present, without creating or locking the database.
pub fn read_root_roster(data_dir: &Path) -> Vec<(String, Option<String>, Option<String>)> {
    let Some(conn) = open_roster_conn(data_dir) else {
        return Vec::new();
    };
    let query_with_desc = "SELECT name, role, description, instructions
         FROM agents
         WHERE parent_id IS NULL AND state = 'running'
         ORDER BY name ASC";
    let mut roster = Vec::new();
    if let Ok(rows) = conn.db_query_vec(query_with_desc, &[], |row| {
        Ok((
            row.get::<String>(0)?,
            row.get::<Option<String>>(1)?,
            row.get::<Option<String>>(2)?,
            row.get::<Option<String>>(3)?,
        ))
    }) {
        for (name, role, description, instructions) in rows {
            roster.push(roster_entry(name, role, description, instructions));
        }
        return roster;
    }
    if let Ok(rows) = conn.db_query_vec(
        "SELECT name, role, instructions
         FROM agents
         WHERE parent_id IS NULL AND state = 'running'
         ORDER BY name ASC",
        &[],
        |row| {
            Ok((
                row.get::<String>(0)?,
                row.get::<Option<String>>(1)?,
                row.get::<Option<String>>(2)?,
            ))
        },
    ) {
        for (name, role, instructions) in rows {
            roster.push(roster_entry(name, role, None, instructions));
        }
    }
    roster
}

/// Reads the active direct child sub-agents `(name, role, public_description)` for `parent_id`
/// from `{data}/state.db` or PostgreSQL read-only, regardless of role label.
pub fn read_children_roster(
    data_dir: &Path,
    parent_id: &str,
) -> Vec<(String, Option<String>, Option<String>)> {
    let Some(conn) = open_roster_conn(data_dir) else {
        return Vec::new();
    };
    let query = "SELECT name, role, description, instructions
         FROM agents
         WHERE parent_id = ?1 AND state != 'archived'
         ORDER BY name ASC";
    let mut roster = Vec::new();
    if let Ok(rows) = conn.db_query_vec(query, &db_params![parent_id], |row| {
        Ok((
            row.get::<String>(0)?,
            row.get::<Option<String>>(1)?,
            row.get::<Option<String>>(2)?,
            row.get::<Option<String>>(3)?,
        ))
    }) {
        for (name, role, description, instructions) in rows {
            roster.push(roster_entry(name, role, description, instructions));
        }
    }
    roster
}
