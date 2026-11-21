//! Coordinator-only sandbox credentials. These never enter deployment secret
//! grants, guest environment assembly, profile rotation, or model auth.

use serde::{Deserialize, Serialize};

use crate::db::{DbExec, DbExecExt as _, db_params};
use crate::secrets_store::{
    hex_decode, hex_encode, require_deployment_master_key, validate_secret_name,
};
use crate::{AgentId, Error, Store};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxCredential {
    pub token: String,
    pub base_url: Option<String>,
    pub parent: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxCredentialSource {
    Default,
    Agent,
    Profile,
    Group,
    Inherited,
}

pub struct SandboxCredentialRef {
    pub name: String,
    pub source: SandboxCredentialSource,
}

fn unavailable() -> Error {
    Error::Message("bound sandbox credential is missing or unreadable".into())
}

fn validate_name(name: &str) -> Result<(), Error> {
    validate_secret_name(name)
        .map_err(|_| Error::Message("invalid sandbox configuration name".into()))
}

impl Store {
    pub fn put_sandbox_credential(
        &self,
        name: &str,
        credential: &SandboxCredential,
    ) -> Result<(), Error> {
        validate_name(name)?;
        let key = require_deployment_master_key()?;
        let aad = format!("sandbox_credentials:v1:{name}");
        let plaintext = serde_json::to_vec(credential).map_err(|_| unavailable())?;
        let ciphertext = hex_encode(&key.encrypt(aad.as_bytes(), &plaintext)?);
        self.write_tx(|tx| {
            tx.db_execute(
                "INSERT INTO sandbox_credentials (name, ciphertext) VALUES (?1, ?2)
                 ON CONFLICT(name) DO UPDATE SET ciphertext = excluded.ciphertext",
                &db_params![name, ciphertext],
            )?;
            Ok(())
        })
    }

    pub fn delete_sandbox_credential(&self, name: &str) -> Result<(), Error> {
        validate_name(name)?;
        self.write_tx(|tx| {
            tx.db_execute(
                "DELETE FROM sandbox_credentials WHERE name = ?1",
                &db_params![name],
            )?;
            Ok(())
        })
    }

    pub fn read_sandbox_credential(&self, name: &str) -> Result<SandboxCredential, Error> {
        let resolve = || -> Result<SandboxCredential, Error> {
            let ciphertext = self
                .read_conn()
                .db_query_opt(
                    "SELECT ciphertext FROM sandbox_credentials WHERE name = ?1",
                    &db_params![name],
                    |r| r.get::<String>(0),
                )?
                .ok_or_else(unavailable)?;
            let aad = format!("sandbox_credentials:v1:{name}");
            let key = require_deployment_master_key()?;
            let plaintext = key.decrypt(aad.as_bytes(), &hex_decode(&ciphertext)?)?;
            serde_json::from_slice(&plaintext).map_err(|_| unavailable())
        };
        resolve().map_err(|_| unavailable())
    }

    pub fn set_sandbox_profile(&self, profile: &str, credential: &str) -> Result<(), Error> {
        validate_name(profile)?;
        self.read_sandbox_credential(credential)?;
        self.write_tx(|tx| {
            tx.db_execute(
                "INSERT INTO sandbox_profiles (name, credential) VALUES (?1, ?2)
                 ON CONFLICT(name) DO UPDATE SET credential = excluded.credential",
                &db_params![profile, credential],
            )?;
            Ok(())
        })
    }

    /// A billing group must be privately owned by the operator. Its roster is
    /// frozen to ordinary callers while it carries a sandbox target.
    pub fn set_group_sandbox_profile(
        &self,
        group: &str,
        profile: Option<&str>,
    ) -> Result<(), Error> {
        self.write_tx(|tx| {
            lock_sandbox_group(tx, group)?;
            if let Some(profile) = profile {
                check_profile(tx, profile)?;
                let eligible: bool = tx.db_query_one(
                    "SELECT EXISTS (SELECT 1 FROM groups WHERE name = ?1 AND public = 0 AND owner_agent_id IS NULL)",
                    &db_params![group], |r| r.get(0),
                )?;
                if !eligible {
                    return Err(Error::Message("sandbox billing groups must be private operator groups".into()));
                }
                tx.db_execute("INSERT INTO sandbox_group_profiles (group_name, profile) VALUES (?1, ?2)
                    ON CONFLICT(group_name) DO UPDATE SET profile = excluded.profile", &db_params![group, profile])?;
            } else {
                tx.db_execute("DELETE FROM sandbox_group_profiles WHERE group_name = ?1", &db_params![group])?;
            }
            Ok(())
        })
    }

    pub fn set_agent_sandbox_target(
        &self,
        agent: &AgentId,
        profile: Option<&str>,
        credential: Option<&str>,
    ) -> Result<(), Error> {
        if let Some(credential) = credential {
            self.read_sandbox_credential(credential)?;
        }
        self.write_tx(|tx| {
            if let Some(profile) = profile {
                check_profile(tx, profile)?;
            }

            tx.db_execute(
                "INSERT INTO agent_sandbox_targets (agent_id, profile, credential) VALUES (?1, ?2, ?3)
                 ON CONFLICT(agent_id) DO UPDATE SET profile = excluded.profile, credential = excluded.credential",
                &db_params![agent.as_str(), profile, credential],
            )?;
            Ok(())
        })
    }

    /// A single statement provides a consistent snapshot of the ancestry and
    /// profile references. The bounded walk fails closed on missing/cyclic lineage.
    pub fn sandbox_credential_ref(
        &self,
        agent: &AgentId,
    ) -> Result<Option<SandboxCredentialRef>, Error> {
        let rows = self.read_conn().db_query_vec(
            "WITH RECURSIVE lineage(id, parent_id, depth) AS (
                SELECT id, parent_id, CAST(0 AS BIGINT) FROM agents WHERE id = ?1
                UNION ALL
                SELECT a.id, a.parent_id, l.depth + 1 FROM lineage l
                JOIN agents a ON a.id = l.parent_id WHERE l.depth < 256
             )
             SELECT l.depth, l.parent_id, t.credential, t.profile, p.credential, gp.profile, sp.credential
             FROM lineage l
             LEFT JOIN agent_sandbox_targets t ON t.agent_id = l.id
             LEFT JOIN sandbox_profiles p ON p.name = t.profile
             LEFT JOIN (group_agents ga JOIN sandbox_group_profiles gp ON gp.group_name = ga.group_name) ON ga.agent_id = l.id
             LEFT JOIN sandbox_profiles sp ON sp.name = gp.profile ORDER BY l.depth",
            &db_params![agent.as_str()],
            |r| Ok((r.get::<i64>(0)?, r.get::<Option<String>>(1)?, r.get::<Option<String>>(2)?, r.get::<Option<String>>(3)?, r.get::<Option<String>>(4)?, r.get::<Option<String>>(5)?, r.get::<Option<String>>(6)?)),
        )?;
        if !rows.last().is_some_and(|row| row.1.is_none()) {
            return Err(Error::Message(
                "sandbox ownership lineage is unavailable".into(),
            ));
        }
        for at_depth in rows.chunk_by(|a, b| a.0 == b.0) {
            let (depth, _, agent, profile, profile_credential, group, group_credential) =
                &at_depth[0];
            let (name, source) = match (agent, profile, profile_credential, group, group_credential)
            {
                (Some(name), _, _, _, _) => (name, SandboxCredentialSource::Agent),
                (None, Some(_), Some(name), _, _) => (name, SandboxCredentialSource::Profile),
                (None, Some(_), None, _, _) => return Err(unavailable()),
                (None, None, _, Some(_), Some(name))
                    if at_depth.iter().all(|row| row.6.as_ref() == Some(name)) =>
                {
                    (name, SandboxCredentialSource::Group)
                }
                (None, None, _, Some(_), _) => {
                    return Err(Error::Message(
                        "sandbox billing group target is ambiguous or unavailable".into(),
                    ));
                }
                _ => continue,
            };
            return Ok(Some(SandboxCredentialRef {
                name: name.clone(),
                source: if *depth == 0 {
                    source
                } else {
                    SandboxCredentialSource::Inherited
                },
            }));
        }
        Ok(None)
    }
}

fn check_profile(conn: &dyn DbExec, profile: &str) -> Result<(), Error> {
    validate_name(profile)?;
    if !conn.db_query_one(
        "SELECT EXISTS (SELECT 1 FROM sandbox_profiles WHERE name = ?1)",
        &db_params![profile],
        |r| r.get::<bool>(0),
    )? {
        return Err(Error::Message("sandbox profile does not exist".into()));
    }
    Ok(())
}

pub(crate) fn lock_sandbox_group(conn: &dyn DbExec, group: &str) -> Result<(), Error> {
    // Write transactions already serialize SQLite. On PostgreSQL, lock the
    // group row before changing either the billing target or membership.
    if conn.backend() == crate::db::DbBackend::Postgres {
        conn.db_query_opt(
            "SELECT name FROM groups WHERE name = ?1 FOR UPDATE",
            &db_params![group],
            |r| r.get::<String>(0),
        )?;
    }
    Ok(())
}

pub(crate) fn is_sandbox_group(conn: &dyn DbExec, group: &str) -> Result<bool, Error> {
    conn.db_query_one(
        "SELECT EXISTS (SELECT 1 FROM sandbox_group_profiles WHERE group_name = ?1)",
        &db_params![group],
        |r| r.get(0),
    )
}
