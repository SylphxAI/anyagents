//! Narrow reads on per-call paths: authenticating a tool server's call is one
//! indexed lookup on a read connection, with no agent row decoded.

use rusqlite::OptionalExtension;

use super::{AgentId, Error, Store, hash};

impl Store {
    /// The agent a presented capability authenticates, when the server minted
    /// it and it was not replaced: its id, and its name while the agent row
    /// exists. The comparison is on the stored hash, read from the store on
    /// every call, so a rotation or a revocation committed by any process
    /// takes effect on the next call.
    pub fn capability_holder(
        &self,
        token: &str,
    ) -> Result<Option<(AgentId, Option<String>)>, Error> {
        let digest = hash(&self.capability_key()?, token);
        let conn = self.read_conn();
        let mut stmt = conn.prepare_cached(
            "SELECT c.agent_id, a.name FROM agent_capabilities c
             LEFT JOIN agents a ON a.id = c.agent_id
             WHERE c.token_hash = ?1 AND c.revoked_at_ms IS NULL",
        )?;
        let found = stmt
            .query_row([digest.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .optional()?;
        found
            .map(|(id, name)| Ok((AgentId::parse(&id)?, name)))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use crate::{NewAgent, Store};

    #[test]
    fn a_capability_names_its_holder_until_it_is_replaced_or_retired() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        store.hold_capability_key(&"k".repeat(64)).unwrap();
        let agent = store
            .create_agent(NewAgent::new("holder", "codex"))
            .unwrap();
        let first = store.issue_capability(&agent.id).unwrap();
        assert_eq!(
            store.capability_holder(&first).unwrap(),
            Some((agent.id.clone(), Some("holder".to_string())))
        );
        let renamed = store.rename_agent(&agent.id, "renamed").unwrap();
        assert_eq!(
            store.capability_holder(&first).unwrap(),
            Some((agent.id.clone(), Some(renamed.name)))
        );
        let second = store.issue_capability(&agent.id).unwrap();
        assert_eq!(store.capability_holder(&first).unwrap(), None);
        assert!(store.capability_holder(&second).unwrap().is_some());
        store.retire_capabilities(&agent.id).unwrap();
        assert_eq!(store.capability_holder(&second).unwrap(), None);
        assert_eq!(store.capability_holder("not-minted").unwrap(), None);
    }

    #[test]
    fn a_process_without_the_key_cannot_check_a_capability() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        assert!(store.capability_holder("anything").is_err());
    }
}
