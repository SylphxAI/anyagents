//! Narrow fleet projection; never loads guest credentials or agent instructions.

use super::{AgentId, Error, Store};
use crate::db::{DbExecExt as _, db_params};

pub struct SandboxInventoryAgent {
    pub id: AgentId,
    pub name: String,
    pub state: String,
    pub harness: String,
    pub binding: Option<SandboxInventoryBinding>,
    pub has_open_turn: bool,
    pub inflight_children: i64,
    pub heartbeat_at_ms: Option<i64>,
    pub drain_requested: bool,
}

pub struct SandboxInventoryBinding {
    pub provider: String,
    pub image: Option<String>,
    pub template: Option<String>,
    pub spec_json: String,
    pub lease_key: Option<String>,
    pub lease_state: String,
}

impl SandboxInventoryBinding {
    pub fn active_lease_key(&self) -> Option<&str> {
        if matches!(self.lease_state.as_str(), "expired" | "detached" | "failed") {
            return None;
        }
        self.lease_key.as_deref()
    }
}

impl Store {
    /// Read fleet sandbox status in one SQL snapshot on PostgreSQL. The
    /// caller supplies the path of its plain-text, lease-scoped drain marker.
    pub fn sandbox_inventory(
        &self,
        marker_path: &str,
    ) -> Result<Vec<SandboxInventoryAgent>, Error> {
        if self.is_postgres() {
            let rows = self.read_conn().db_query_vec(
                "SELECT a.id, a.name, a.state, a.harness,
                        b.provider, b.image, b.template, b.spec_json,
                        COALESCE(b.lease_resource, b.lease_id), b.lease_state,
                        (COALESCE(r.opened_seq > r.completed_seq, FALSE) OR EXISTS (
                            SELECT 1 FROM public.pending_remote_turns t
                            WHERE t.agent_id = a.id AND t.status IN ('dispatched', 'running')
                        )),
                        COALESCE(h.inflight_children, 0), h.heartbeat_at_ms,
                        COALESCE(NOT m.encrypted AND m.content = convert_to(COALESCE(b.lease_resource, b.lease_id), 'UTF8'), FALSE),
                        COALESCE(m.encrypted, FALSE)
                 FROM public.agents a
                 LEFT JOIN public.agent_sandbox_bindings b ON b.agent_id = a.id
                 LEFT JOIN public.turn_recovery r ON r.agent_id = a.id
                 LEFT JOIN public.agent_runtime h ON h.agent_id = a.id
                 LEFT JOIN public.agent_files m ON m.agent_id = a.id
                      AND m.namespace = 'markers' AND m.path = ?1
                 ORDER BY a.name",
                &db_params![marker_path],
                |row| {
                    let binding = if let Some(provider) = row.get::<Option<String>>(4)? {
                        Some(SandboxInventoryBinding {
                            provider,
                            image: row.get(5)?,
                            template: row.get(6)?,
                            spec_json: row.get(7)?,
                            lease_key: row.get(8)?,
                            lease_state: row.get(9)?,
                        })
                    } else {
                        None
                    };
                    Ok((SandboxInventoryAgent {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        state: row.get(2)?,
                        harness: row.get(3)?,
                        binding,
                        has_open_turn: row.get(10)?,
                        inflight_children: row.get(11)?,
                        heartbeat_at_ms: row.get(12)?,
                        drain_requested: row.get(13)?,
                    }, row.get::<bool>(14)?))
                },
            )?;
            let blobs = self.blob_store();
            return rows
                .into_iter()
                .map(|(mut agent, encrypted_marker)| {
                    // The drain writer always uses plaintext. Honor legacy encrypted
                    // rows through the normal decrypting reader, not ciphertext comparison.
                    if encrypted_marker
                        && let Some(key) = agent
                            .binding
                            .as_ref()
                            .and_then(SandboxInventoryBinding::active_lease_key)
                    {
                        agent.drain_requested = blobs
                            .get_agent_file(&agent.id, "markers", marker_path)?
                            .is_some_and(|blob| blob.content == key.as_bytes());
                    }
                    Ok(agent)
                })
                .collect();
        }

        // On SQLite, remote turns, bindings and drain markers are files.
        // Scan the turn ledger once and batch the local SQL turn watermark.
        let mut agents = self.read_conn().db_query_vec(
            "SELECT a.id, a.name, a.state, a.harness,
                    COALESCE(r.opened_seq > r.completed_seq, FALSE)
             FROM agents a LEFT JOIN turn_recovery r ON r.agent_id = a.id
             ORDER BY a.name",
            &[],
            |row| {
                Ok(SandboxInventoryAgent {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    state: row.get(2)?,
                    harness: row.get(3)?,
                    binding: None,
                    has_open_turn: row.get(4)?,
                    inflight_children: 0,
                    heartbeat_at_ms: None,
                    drain_requested: false,
                })
            },
        )?;
        let turns = self
            .remote_turn_store()
            .list_active_fresh_by_agent(i64::MIN)?;
        let bindings = self.sandbox_binding_store();
        let blobs = self.blob_store();
        for agent in &mut agents {
            agent.has_open_turn |= turns.contains_key(agent.id.as_str());
            if let Some(binding) = bindings.get(&agent.id)? {
                let binding = SandboxInventoryBinding {
                    provider: binding.provider,
                    image: binding.image,
                    template: binding.template,
                    spec_json: binding.spec_json,
                    lease_key: binding.lease_resource.or(binding.lease_id),
                    lease_state: binding.lease_state,
                };
                if let Some(key) = binding.active_lease_key() {
                    agent.drain_requested = blobs
                        .get_agent_file(&agent.id, "markers", marker_path)?
                        .is_some_and(|blob| blob.content == key.as_bytes());
                }
                agent.binding = Some(binding);
            }
        }
        Ok(agents)
    }
}
