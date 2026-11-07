//! Authenticated group mutations. Read authority and commit every requested
//! membership/visibility change under the same SQLite write transaction.

use std::collections::HashSet;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{Agent, AgentId, Error, Store, agent_by_id, now_ms};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Group {
    pub owner_agent_id: Option<AgentId>,
    pub public: bool,
}

/// Set only from the authenticated principal, never from a request body.
#[derive(Clone, Debug)]
pub enum GroupActor {
    Operator,
    Agent(AgentId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupAction {
    Join,
    Leave,
    Members,
}

impl Store {
    pub fn group(&self, name: &str) -> Result<Option<Group>, Error> {
        group(&self.conn(), name)
    }

    /// Preflight agent creation before creating its runtime. The actual join
    /// rechecks current authority in its write transaction.
    pub fn check_group_joins(&self, actor: &GroupActor, names: &[&str]) -> Result<(), Error> {
        let conn = self.conn();
        let caller = caller(&conn, actor)?;
        for name in names {
            validate_name(name)?;
            authorize_join(&conn, name, group(&conn, name)?.as_ref(), caller.as_ref())?;
        }
        Ok(())
    }

    /// Join every group atomically (used by create-agent's `group` option).
    pub fn join_groups_as(
        &self,
        actor: &GroupActor,
        names: &[&str],
        targets: &[AgentId],
    ) -> Result<(), Error> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let caller = caller(&tx, actor)?;
        for name in names {
            apply(&tx, caller.as_ref(), name, GroupAction::Join, targets, None)?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn group_action_as(
        &self,
        actor: &GroupActor,
        name: &str,
        action: GroupAction,
        targets: &[AgentId],
        public: Option<bool>,
    ) -> Result<(), Error> {
        let mut conn = self.conn();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let caller = caller(&tx, actor)?;
        apply(&tx, caller.as_ref(), name, action, targets, public)?;
        tx.commit()?;
        Ok(())
    }
}

fn group(conn: &Connection, name: &str) -> Result<Option<Group>, Error> {
    let row: Option<(Option<String>, bool)> = conn
        .query_row(
            "SELECT owner_agent_id, public FROM groups WHERE name = ?1",
            [name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(owner, public)| {
        Ok(Group {
            owner_agent_id: owner.as_deref().map(AgentId::parse).transpose()?,
            public,
        })
    })
    .transpose()
}

fn caller(conn: &Connection, actor: &GroupActor) -> Result<Option<Agent>, Error> {
    match actor {
        GroupActor::Operator => Ok(None),
        GroupActor::Agent(id) => agent_by_id(conn, id).map(Some),
    }
}

fn validate_name(name: &str) -> Result<(), Error> {
    if name.is_empty() || name.contains(':') || name.chars().any(char::is_whitespace) {
        return Err(Error::Message(format!("invalid group name: {name}")));
    }
    Ok(())
}

fn forbidden(name: &str, reason: &'static str) -> Error {
    Error::GroupForbidden {
        group: name.to_string(),
        reason,
    }
}

fn supervises(conn: &Connection, caller: &AgentId, target: &AgentId) -> Result<bool, Error> {
    let mut cursor = Some(target.clone());
    let mut seen = HashSet::new();
    while let Some(id) = cursor {
        if &id == caller {
            return Ok(true);
        }
        if !seen.insert(id.clone()) {
            break;
        }
        cursor = agent_by_id(conn, &id)?.parent_id;
    }
    Ok(false)
}

fn authorize_join(
    conn: &Connection,
    name: &str,
    existing: Option<&Group>,
    caller: Option<&Agent>,
) -> Result<(), Error> {
    let (Some(existing), Some(caller)) = (existing, caller) else {
        return Ok(());
    };
    let is_member: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM group_agents WHERE group_name = ?1 AND agent_id = ?2)",
        params![name, caller.id.as_str()],
        |r| r.get(0),
    )?;
    if existing.public || is_member || existing.owner_agent_id.as_ref() == Some(&caller.id) {
        return Ok(());
    }
    Err(forbidden(
        name,
        "is private; joining requires an existing member's invitation, the owner, or the operator",
    ))
}

fn apply(
    conn: &Connection,
    caller: Option<&Agent>,
    name: &str,
    action: GroupAction,
    targets: &[AgentId],
    public: Option<bool>,
) -> Result<(), Error> {
    validate_name(name)?;
    let existing = group(conn, name)?;
    // `public` on join configures a new group only. It never grants access to
    // or changes visibility of an existing group, even for an owner.
    let visibility_change = public.is_some() && action != GroupAction::Join;
    if visibility_change && let (Some(existing), Some(caller)) = (&existing, caller) {
        let owner_authority = match &existing.owner_agent_id {
            Some(owner) => supervises(conn, &caller.id, owner)?,
            None => false,
        };
        if !owner_authority && !(caller.parent_id.is_none() && caller.is_admin) {
            return Err(forbidden(
                name,
                "changing visibility requires the owner, its ancestor, the operator, or a root admin",
            ));
        }
    }
    if action == GroupAction::Join {
        if targets.is_empty() {
            return Err(Error::Message("join needs an agent name".into()));
        }
        authorize_join(conn, name, existing.as_ref(), caller)?;
    }
    if action == GroupAction::Leave && targets.is_empty() {
        return Err(Error::Message("leave needs an agent name".into()));
    }
    // Resolve and authorize the entire batch before its first mutation.
    for target in targets {
        agent_by_id(conn, target)?;
        if action == GroupAction::Leave
            && let Some(caller) = caller
        {
            let is_owner =
                existing.as_ref().and_then(|g| g.owner_agent_id.as_ref()) == Some(&caller.id);
            if !is_owner && !supervises(conn, &caller.id, target)? {
                return Err(forbidden(
                    name,
                    "removing another member requires the target's ancestor, the group owner, or the operator",
                ));
            }
        }
    }
    if existing.is_none() && (action == GroupAction::Join || public.is_some()) {
        conn.execute(
            "INSERT INTO groups (name, created_at_ms, public, owner_agent_id) VALUES (?1, ?2, ?3, ?4)",
            params![name, now_ms(), public.unwrap_or(false), caller.map(|a| a.id.as_str())],
        )?;
    } else if visibility_change {
        conn.execute(
            "UPDATE groups SET public = ?2 WHERE name = ?1",
            params![name, public],
        )?;
    }
    match action {
        GroupAction::Join => {
            // New agent-created groups include their creator. Ownership remains
            // unchanged when the creator later leaves or the group is emptied.
            if existing.is_none()
                && let Some(caller) = caller
            {
                conn.execute(
                    "INSERT OR IGNORE INTO group_agents (group_name, agent_id) VALUES (?1, ?2)",
                    params![name, caller.id.as_str()],
                )?;
            }
            for target in targets {
                conn.execute(
                    "INSERT OR IGNORE INTO group_agents (group_name, agent_id) VALUES (?1, ?2)",
                    params![name, target.as_str()],
                )?;
            }
        }
        GroupAction::Leave => {
            for target in targets {
                conn.execute(
                    "DELETE FROM group_agents WHERE group_name = ?1 AND agent_id = ?2",
                    params![name, target.as_str()],
                )?;
            }
        }
        GroupAction::Members => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NewAgent, migrate};

    fn fixture() -> (tempfile::TempDir, Store, Agent, Agent, Agent) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        let owner = store
            .create_agent(NewAgent::new("owner", "claude-code"))
            .unwrap();
        let member = store
            .create_agent(NewAgent::new("member", "claude-code"))
            .unwrap();
        let other = store
            .create_agent(NewAgent::new("other", "claude-code"))
            .unwrap();
        (dir, store, owner, member, other)
    }

    fn actor(agent: &Agent) -> GroupActor {
        GroupActor::Agent(agent.id.clone())
    }

    fn denied(result: Result<(), Error>, reason: &str) {
        match result.unwrap_err() {
            Error::GroupForbidden {
                group,
                reason: actual,
            } => {
                assert_eq!(group, "private-room");
                assert!(actual.contains(reason), "{actual}");
            }
            other => panic!("expected group permission refusal, got {other:?}"),
        }
    }

    #[test]
    fn group_owner_migration_preserves_legacy_membership_without_inventing_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let mut conn = Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at_ms INTEGER NOT NULL);").unwrap();
        for (version, sql) in migrate::MIGRATIONS.iter().filter(|(v, _)| *v < 23) {
            let tx = conn.transaction().unwrap();
            tx.execute_batch(sql).unwrap();
            tx.execute("INSERT INTO schema_migrations VALUES (?1, 0)", [version])
                .unwrap();
            tx.commit().unwrap();
        }
        // The previous Store::open added public outside the migration ledger.
        conn.execute_batch(
            "ALTER TABLE groups ADD COLUMN public INTEGER NOT NULL DEFAULT 0;
            INSERT INTO groups (name, created_at_ms, public) VALUES ('legacy', 123, 0);",
        )
        .unwrap();
        let id = AgentId::generate();
        conn.execute("INSERT INTO agents (id, name, harness, state, created_at_ms) VALUES (?1, 'legacy-member', 'claude-code', 'idle', 456)", [id.as_str()]).unwrap();
        conn.execute(
            "INSERT INTO group_agents VALUES ('legacy', ?1)",
            [id.as_str()],
        )
        .unwrap();
        drop(conn);
        let store = Store::open(&path).unwrap();
        assert_eq!(
            store.group("legacy").unwrap(),
            Some(Group {
                owner_agent_id: None,
                public: false
            })
        );
        assert_eq!(store.group_members("legacy").unwrap(), ["legacy-member"]);
        assert_eq!(
            store
                .conn()
                .query_row(
                    "SELECT created_at_ms FROM groups WHERE name = 'legacy'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            123
        );
        drop(store);
        let reopened = Store::open(&path).unwrap();
        assert_eq!(
            reopened.group("legacy").unwrap().unwrap().owner_agent_id,
            None
        );
        assert!(matches!(
            reopened.group_action_as(
                &GroupActor::Agent(id),
                "legacy",
                GroupAction::Members,
                &[],
                Some(true)
            ),
            Err(Error::GroupForbidden { .. })
        ));
        reopened
            .group_action_as(
                &GroupActor::Operator,
                "legacy",
                GroupAction::Members,
                &[],
                Some(true),
            )
            .unwrap();
        assert!(reopened.group("legacy").unwrap().unwrap().public);
    }

    #[test]
    fn group_join_requires_invitation_and_never_changes_existing_visibility() {
        let (_dir, store, owner, member, other) = fixture();
        store
            .group_action_as(
                &actor(&owner),
                "private-room",
                GroupAction::Join,
                std::slice::from_ref(&member.id),
                None,
            )
            .unwrap();
        assert_eq!(
            store.group("private-room").unwrap().unwrap().owner_agent_id,
            Some(owner.id.clone())
        );
        denied(
            store.group_action_as(
                &actor(&other),
                "private-room",
                GroupAction::Join,
                std::slice::from_ref(&other.id),
                Some(true),
            ),
            "is private",
        );
        assert_eq!(
            store.group_members("private-room").unwrap(),
            ["member", "owner"]
        );
        assert!(!store.is_group_public("private-room"));
        // Members can invite. Even the owner's join request cannot flip public.
        store
            .group_action_as(
                &actor(&member),
                "private-room",
                GroupAction::Join,
                std::slice::from_ref(&other.id),
                Some(true),
            )
            .unwrap();
        store
            .group_action_as(
                &actor(&owner),
                "private-room",
                GroupAction::Join,
                std::slice::from_ref(&owner.id),
                Some(true),
            )
            .unwrap();
        assert!(!store.is_group_public("private-room"));
        denied(
            store.group_action_as(
                &actor(&member),
                "private-room",
                GroupAction::Members,
                &[],
                Some(true),
            ),
            "changing visibility",
        );
        // Ownership survives leaving and persists across Store instances.
        store
            .group_action_as(
                &actor(&owner),
                "private-room",
                GroupAction::Leave,
                &[owner.id.clone(), member.id.clone(), other.id.clone()],
                None,
            )
            .unwrap();
        assert!(store.group_members("private-room").unwrap().is_empty());
        denied(
            store.group_action_as(
                &actor(&other),
                "private-room",
                GroupAction::Join,
                std::slice::from_ref(&other.id),
                None,
            ),
            "is private",
        );
        let other_store = Store::open(&store.db_path).unwrap();
        other_store
            .group_action_as(
                &actor(&owner),
                "private-room",
                GroupAction::Join,
                std::slice::from_ref(&owner.id),
                None,
            )
            .unwrap();
        assert_eq!(
            other_store
                .group("private-room")
                .unwrap()
                .unwrap()
                .owner_agent_id,
            Some(owner.id)
        );
    }

    #[test]
    fn group_leave_and_visibility_batches_refuse_without_partial_changes() {
        let (_dir, store, owner, member, other) = fixture();
        store
            .group_action_as(
                &actor(&owner),
                "private-room",
                GroupAction::Join,
                &[member.id.clone(), other.id.clone()],
                None,
            )
            .unwrap();
        denied(
            store.group_action_as(
                &actor(&other),
                "private-room",
                GroupAction::Leave,
                std::slice::from_ref(&member.id),
                None,
            ),
            "removing another member",
        );
        // The first target is permitted; the second refusal must preserve both.
        denied(
            store.group_action_as(
                &actor(&member),
                "private-room",
                GroupAction::Leave,
                &[member.id.clone(), other.id.clone()],
                None,
            ),
            "removing another member",
        );
        denied(
            store.group_action_as(
                &actor(&member),
                "private-room",
                GroupAction::Leave,
                std::slice::from_ref(&member.id),
                Some(true),
            ),
            "changing visibility",
        );
        assert_eq!(
            store.group_members("private-room").unwrap(),
            ["member", "other", "owner"]
        );
        assert!(!store.is_group_public("private-room"));
        let unknown = AgentId::generate();
        assert!(matches!(
            store.group_action_as(
                &actor(&owner),
                "private-room",
                GroupAction::Join,
                &[member.id.clone(), unknown],
                Some(true)
            ),
            Err(Error::AgentNotFound(_))
        ));
        // A failure in a later requested group rolls back creation/join of the first.
        let outsider = store
            .create_agent(NewAgent::new("outsider", "claude-code"))
            .unwrap();
        denied(
            store.join_groups_as(
                &actor(&outsider),
                &["would-be-new", "private-room"],
                std::slice::from_ref(&outsider.id),
            ),
            "is private",
        );
        assert!(store.group("would-be-new").unwrap().is_none());
        store
            .group_action_as(
                &actor(&member),
                "private-room",
                GroupAction::Leave,
                std::slice::from_ref(&member.id),
                None,
            )
            .unwrap();
        store
            .group_action_as(
                &GroupActor::Operator,
                "private-room",
                GroupAction::Leave,
                std::slice::from_ref(&other.id),
                None,
            )
            .unwrap();
        assert_eq!(store.group_members("private-room").unwrap(), ["owner"]);
    }

    #[test]
    fn group_visibility_uses_live_owner_ancestry_and_explicit_root_admin() {
        let (_dir, store, owner, member, other) = fixture();
        store.set_agent_parent(&owner.id, Some(&member.id)).unwrap();
        let mut grandparent = NewAgent::new("grandparent", "claude-code");
        grandparent.parent_id = None;
        let grandparent = store.create_agent(grandparent).unwrap();
        store
            .set_agent_parent(&member.id, Some(&grandparent.id))
            .unwrap();
        store
            .group_action_as(
                &actor(&owner),
                "private-room",
                GroupAction::Join,
                std::slice::from_ref(&owner.id),
                Some(true),
            )
            .unwrap();
        for supervising in [&owner, &member, &grandparent] {
            store
                .group_action_as(
                    &actor(supervising),
                    "private-room",
                    GroupAction::Members,
                    &[],
                    Some(false),
                )
                .unwrap();
        }
        denied(
            store.group_action_as(
                &actor(&other),
                "private-room",
                GroupAction::Members,
                &[],
                Some(true),
            ),
            "changing visibility",
        );
        store.set_agent_admin(&other.id, true).unwrap();
        store
            .group_action_as(
                &actor(&other),
                "private-room",
                GroupAction::Members,
                &[],
                Some(true),
            )
            .unwrap();
        store.set_agent_admin(&other.id, false).unwrap();
        denied(
            store.group_action_as(
                &actor(&other),
                "private-room",
                GroupAction::Members,
                &[],
                Some(false),
            ),
            "changing visibility",
        );
        // Being an ancestor of the removed target is independently sufficient.
        store
            .group_action_as(
                &actor(&member),
                "private-room",
                GroupAction::Leave,
                std::slice::from_ref(&owner.id),
                None,
            )
            .unwrap();
        assert!(store.group_members("private-room").unwrap().is_empty());
        // Reparenting revokes the old owner's ancestor immediately.
        store.set_agent_parent(&owner.id, Some(&other.id)).unwrap();
        denied(
            store.group_action_as(
                &actor(&member),
                "private-room",
                GroupAction::Members,
                &[],
                Some(false),
            ),
            "changing visibility",
        );
        store
            .group_action_as(
                &GroupActor::Operator,
                "private-room",
                GroupAction::Members,
                &[],
                Some(false),
            )
            .unwrap();
    }
}
