//! Persistent per-agent browser profile grants on SQLite and PostgreSQL.
//!
//! Enforces a pure Capability Attenuation Law with zero role-name hardcoding:
//! - Operator or root `is_admin=true` agent: may grant or revoke any valid
//!   `profile_name` on any agent.
//! - Non-admin parent agent: may grant or revoke `profile_name` on direct
//!   children (`child.parent_id == Some(caller.id)`) if and only if the parent
//!   itself holds `profile_name` (`child.browser_profiles ⊆ parent.browser_profiles`).
//! - Self-mutation denied: a non-admin agent cannot mutate its own grants.
//! - Default for every newly created agent: `[]`.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::db::{DbExec, DbExecExt as _, db_params};
use crate::error::Error;
use crate::id::AgentId;
use crate::migrate::now_ms;
use crate::store::Store;

/// Maximum length of a valid browser profile name.
pub const MAX_BROWSER_PROFILE_NAME_LEN: usize = 64;

/// Who is requesting a browser profile grant mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BrowserProfileActor {
    Operator,
    Agent(AgentId),
}

/// One durable grant associating an agent with a named browser profile.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrowserProfileGrant {
    pub agent_id: AgentId,
    pub profile_name: String,
    pub granted_by: Option<String>,
    pub granted_at_ms: i64,
}

/// Strictly validate a browser profile name (`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`,
/// rejecting path separators and `..`).
pub fn validate_browser_profile_name(raw: &str) -> Result<String, Error> {
    if raw.is_empty()
        || raw.len() > MAX_BROWSER_PROFILE_NAME_LEN
        || raw.contains("..")
        || raw.contains('/')
        || raw.contains('\\')
        || raw.contains('\0')
    {
        return Err(Error::Message(format!(
            "invalid browser profile name `{raw}`: must match ^[a-zA-Z0-9][a-zA-Z0-9._-]{{0,63}}$ with no `..`"
        )));
    }
    let bytes = raw.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() {
        return Err(Error::Message(format!(
            "invalid browser profile name `{raw}`: must start with an ASCII letter or digit"
        )));
    }
    if !bytes[1..]
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return Err(Error::Message(format!(
            "invalid browser profile name `{raw}`: allowed characters are ASCII letters, digits, `.`, `_`, `-`"
        )));
    }
    Ok(raw.to_string())
}

fn normalize_profile_set(profiles: &[String]) -> Result<Vec<String>, Error> {
    let mut set = BTreeSet::new();
    for raw in profiles {
        let valid = validate_browser_profile_name(raw)?;
        set.insert(valid);
    }
    Ok(set.into_iter().collect())
}

fn query_agent_profiles(
    exec: &(impl DbExec + ?Sized),
    agent_id: &AgentId,
) -> Result<Vec<String>, Error> {
    exec.db_query_vec(
        "SELECT profile_name
           FROM agent_browser_profile_grants
          WHERE agent_id = ?1
          ORDER BY profile_name ASC",
        &db_params![agent_id],
        |row| row.get::<String>(0),
    )
}

fn load_caller_meta(
    exec: &(impl DbExec + ?Sized),
    caller_id: &AgentId,
) -> Result<(String, Option<AgentId>, bool), Error> {
    exec.db_query_opt(
        "SELECT name, parent_id, is_admin FROM agents WHERE id = ?1",
        &db_params![caller_id],
        |row| {
            Ok((
                row.get::<String>(0)?,
                row.get::<Option<AgentId>>(1)?,
                row.get::<bool>(2)?,
            ))
        },
    )?
    .ok_or_else(|| Error::AgentNotFound(caller_id.to_string()))
}

impl Store {
    /// List all browser profile names currently granted to `agent_id` (sorted).
    pub fn list_agent_browser_profiles(&self, agent_id: &AgentId) -> Result<Vec<String>, Error> {
        let conn = self.read_conn();
        query_agent_profiles(&conn, agent_id)
    }

    /// Return `true` when `agent_id` currently holds a grant for `profile_name`.
    pub fn agent_has_browser_profile(
        &self,
        agent_id: &AgentId,
        profile_name: &str,
    ) -> Result<bool, Error> {
        let valid = validate_browser_profile_name(profile_name)?;
        let conn = self.read_conn();
        conn.db_query_one(
            "SELECT EXISTS(
                SELECT 1
                  FROM agent_browser_profile_grants
                 WHERE agent_id = ?1 AND profile_name = ?2
            )",
            &db_params![agent_id, valid],
            |row| row.get::<bool>(0),
        )
    }

    /// List every browser profile grant across all agents.
    pub fn list_all_browser_profile_grants(&self) -> Result<Vec<BrowserProfileGrant>, Error> {
        let conn = self.read_conn();
        conn.db_query_vec(
            "SELECT agent_id, profile_name, granted_by, granted_at_ms
               FROM agent_browser_profile_grants
              ORDER BY profile_name ASC, agent_id ASC",
            &[],
            |row| {
                Ok(BrowserProfileGrant {
                    agent_id: row.get::<AgentId>(0)?,
                    profile_name: row.get::<String>(1)?,
                    granted_by: row.get::<Option<String>>(2)?,
                    granted_at_ms: row.get::<i64>(3)?,
                })
            },
        )
    }

    /// Validate requested initial browser profile grants before creating a new
    /// agent under `parent_id`, enforcing the exact `child ⊆ parent` Capability
    /// Attenuation Law.
    pub fn validate_browser_profile_delegation(
        &self,
        actor: &BrowserProfileActor,
        parent_id: Option<&AgentId>,
        requested: &[String],
    ) -> Result<Vec<String>, Error> {
        let normalized = normalize_profile_set(requested)?;
        if normalized.is_empty() {
            return Ok(normalized);
        }
        let conn = self.read_conn();
        match actor {
            BrowserProfileActor::Operator => Ok(normalized),
            BrowserProfileActor::Agent(caller_id) => {
                let (_name, caller_parent, caller_is_admin) = load_caller_meta(&conn, caller_id)?;
                if caller_is_admin && caller_parent.is_none() {
                    return Ok(normalized);
                }
                if parent_id != Some(caller_id) {
                    return Err(Error::PermissionDenied(
                        "only an operator, admin, or direct parent may grant browser profiles"
                            .into(),
                    ));
                }
                let held: BTreeSet<String> = query_agent_profiles(&conn, caller_id)?
                    .into_iter()
                    .collect();
                for profile in &normalized {
                    if !held.contains(profile) {
                        return Err(Error::PermissionDenied(format!(
                            "cannot grant browser profile `{profile}`: parent does not hold grant for `{profile}`"
                        )));
                    }
                }
                Ok(normalized)
            }
        }
    }

    /// Replace `target_agent_id`'s browser profile grants atomically under the
    /// pure Capability Attenuation Law:
    /// - Operator or root `is_admin=true` agent: any valid profile set on any agent.
    /// - Non-admin agent: denied self-mutation; allowed only on direct children
    ///   (`target.parent_id == Some(caller_id)`) and strictly `profiles ⊆ caller.profiles`.
    pub fn set_agent_browser_profiles_as(
        &self,
        actor: &BrowserProfileActor,
        target_agent_id: &AgentId,
        profiles: &[String],
    ) -> Result<Vec<String>, Error> {
        let normalized = normalize_profile_set(profiles)?;
        let now = now_ms();
        self.write_tx(|tx| {
            let target_parent: Option<AgentId> = tx
                .db_query_opt(
                    "SELECT parent_id FROM agents WHERE id = ?1",
                    &db_params![target_agent_id],
                    |row| row.get::<Option<AgentId>>(0),
                )?
                .ok_or_else(|| Error::AgentNotFound(target_agent_id.to_string()))?;

            let granted_by = match actor {
                BrowserProfileActor::Operator => Some("operator".to_string()),
                BrowserProfileActor::Agent(caller_id) => {
                    let (caller_name, caller_parent, caller_is_admin) =
                        load_caller_meta(tx, caller_id)?;
                    if caller_is_admin && caller_parent.is_none() {
                        Some(format!("agent:{caller_name}"))
                    } else {
                        if caller_id == target_agent_id {
                            return Err(Error::PermissionDenied(
                                "non-admin agent cannot modify its own browser profile grants"
                                    .into(),
                            ));
                        }
                        if target_parent.as_ref() != Some(caller_id) {
                            return Err(Error::PermissionDenied(
                                "only an operator, admin, or the agent's direct parent may modify browser profile grants"
                                    .into(),
                            ));
                        }
                        let held: BTreeSet<String> =
                            query_agent_profiles(tx, caller_id)?.into_iter().collect();
                        for profile in &normalized {
                            if !held.contains(profile) {
                                return Err(Error::PermissionDenied(format!(
                                    "cannot grant browser profile `{profile}`: parent does not hold grant for `{profile}`"
                                )));
                            }
                        }
                        Some(format!("agent:{caller_name}"))
                    }
                }
            };

            let existing = query_agent_profiles(tx, target_agent_id)?;
            let desired: BTreeSet<&str> = normalized.iter().map(String::as_str).collect();
            for old in &existing {
                if !desired.contains(old.as_str()) {
                    tx.db_execute(
                        "DELETE FROM agent_browser_profile_grants
                          WHERE agent_id = ?1 AND profile_name = ?2",
                        &db_params![target_agent_id, old],
                    )?;
                }
            }
            for profile in &normalized {
                tx.db_execute(
                    "INSERT INTO agent_browser_profile_grants
                        (agent_id, profile_name, granted_by, granted_at_ms)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (agent_id, profile_name) DO NOTHING",
                    &db_params![target_agent_id, profile, granted_by, now],
                )?;
            }
            Ok(normalized)
        })
    }

    /// Delete all grants referencing `profile_name` across every agent.
    pub fn delete_browser_profile_grants(&self, profile_name: &str) -> Result<usize, Error> {
        let valid = validate_browser_profile_name(profile_name)?;
        self.write_tx(|tx| {
            tx.db_execute(
                "DELETE FROM agent_browser_profile_grants WHERE profile_name = ?1",
                &db_params![valid],
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NewAgent;

    #[test]
    fn profile_name_validation_rejects_traversal_and_bad_chars() {
        for ok in ["a", "alpha-1", "team_site.v2", "A0.b_c-d"] {
            assert_eq!(validate_browser_profile_name(ok).unwrap(), ok);
        }
        let max_len = "a".repeat(64);
        assert_eq!(validate_browser_profile_name(&max_len).unwrap(), max_len);

        let too_long = "a".repeat(65);
        for bad in [
            "", " ", ".hidden", "-dash", "_under", "a..b", "a/b", "a\\b", "a b", "a\0b", &too_long,
        ] {
            assert!(
                validate_browser_profile_name(bad).is_err(),
                "expected `{bad}` to be rejected"
            );
        }
    }

    #[test]
    fn capability_attenuation_law_enforces_subset_denies_self_and_allows_operator_admin() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();

        let admin = store
            .create_agent(NewAgent::new("admin-root", "claude-code"))
            .unwrap();
        store.set_agent_admin(&admin.id, true).unwrap();

        let parent = store
            .create_agent(NewAgent::new("parent-root", "claude-code"))
            .unwrap();
        let mut child_req = NewAgent::new("direct-child", "claude-code");
        child_req.parent_id = Some(parent.id.clone());
        let child = store.create_agent(child_req).unwrap();
        let mut grand_req = NewAgent::new("grandchild", "claude-code");
        grand_req.parent_id = Some(child.id.clone());
        let grandchild = store.create_agent(grand_req).unwrap();
        let other = store
            .create_agent(NewAgent::new("unrelated", "claude-code"))
            .unwrap();

        // Default for every new agent is empty [].
        assert!(
            store
                .list_agent_browser_profiles(&parent.id)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .list_agent_browser_profiles(&child.id)
                .unwrap()
                .is_empty()
        );

        // Non-admin self-mutation is denied (403 / PermissionDenied).
        let self_err = store
            .set_agent_browser_profiles_as(
                &BrowserProfileActor::Agent(parent.id.clone()),
                &parent.id,
                &["profile-a".into()],
            )
            .unwrap_err();
        assert!(matches!(self_err, Error::PermissionDenied(_)));

        // Operator grants parent [profile-a, profile-b] (deduped + sorted).
        let granted = store
            .set_agent_browser_profiles_as(
                &BrowserProfileActor::Operator,
                &parent.id,
                &["profile-b".into(), "profile-a".into(), "profile-a".into()],
            )
            .unwrap();
        assert_eq!(
            granted,
            vec!["profile-a".to_string(), "profile-b".to_string()]
        );
        assert!(
            store
                .agent_has_browser_profile(&parent.id, "profile-a")
                .unwrap()
        );
        assert!(
            !store
                .agent_has_browser_profile(&parent.id, "profile-c")
                .unwrap()
        );

        // Parent may delegate a subset to its direct child.
        let child_granted = store
            .set_agent_browser_profiles_as(
                &BrowserProfileActor::Agent(parent.id.clone()),
                &child.id,
                &["profile-a".into()],
            )
            .unwrap();
        assert_eq!(child_granted, vec!["profile-a".to_string()]);

        // Parent CANNOT grant a profile it does not hold (child ⊆ parent).
        let superset_err = store
            .set_agent_browser_profiles_as(
                &BrowserProfileActor::Agent(parent.id.clone()),
                &child.id,
                &["profile-a".into(), "profile-c".into()],
            )
            .unwrap_err();
        assert!(matches!(superset_err, Error::PermissionDenied(_)));

        // Non-admin parent CANNOT mutate grandchild or unrelated agent.
        let grand_err = store
            .set_agent_browser_profiles_as(
                &BrowserProfileActor::Agent(parent.id.clone()),
                &grandchild.id,
                &["profile-a".into()],
            )
            .unwrap_err();
        assert!(matches!(grand_err, Error::PermissionDenied(_)));

        let unrelated_err = store
            .set_agent_browser_profiles_as(
                &BrowserProfileActor::Agent(parent.id.clone()),
                &other.id,
                &["profile-a".into()],
            )
            .unwrap_err();
        assert!(matches!(unrelated_err, Error::PermissionDenied(_)));

        // Child CAN delegate its own granted profile down to grandchild, but NOT profile-b.
        assert!(
            store
                .set_agent_browser_profiles_as(
                    &BrowserProfileActor::Agent(child.id.clone()),
                    &grandchild.id,
                    &["profile-a".into()],
                )
                .is_ok()
        );
        assert!(matches!(
            store
                .set_agent_browser_profiles_as(
                    &BrowserProfileActor::Agent(child.id.clone()),
                    &grandchild.id,
                    &["profile-b".into()],
                )
                .unwrap_err(),
            Error::PermissionDenied(_)
        ));

        // Root admin agent can grant any profile to any agent (including itself).
        let admin_set = store
            .set_agent_browser_profiles_as(
                &BrowserProfileActor::Agent(admin.id.clone()),
                &other.id,
                &["profile-z".into()],
            )
            .unwrap();
        assert_eq!(admin_set, vec!["profile-z".to_string()]);

        // Parent can revoke from direct child cleanly.
        let revoked = store
            .set_agent_browser_profiles_as(
                &BrowserProfileActor::Agent(parent.id.clone()),
                &child.id,
                &[],
            )
            .unwrap();
        assert!(revoked.is_empty());

        // Deleting grants for profile-a removes both parent and grandchild grants.
        let removed = store.delete_browser_profile_grants("profile-a").unwrap();
        assert_eq!(removed, 2);
        assert_eq!(
            store.list_agent_browser_profiles(&parent.id).unwrap(),
            vec!["profile-b".to_string()]
        );
        assert!(
            store
                .list_agent_browser_profiles(&grandchild.id)
                .unwrap()
                .is_empty()
        );
    }
}
