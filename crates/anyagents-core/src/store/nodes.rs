//! Durable remote user-device / edge nodes, scoped capability grants,
//! and append-only audit events across SQLite and PostgreSQL.

use std::collections::BTreeSet;
use std::io::Read as _;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::{Store, agent_exists, now_ms};
use crate::db::{DbExec, DbExecExt as _, DbRow, db_params};
use crate::error::Error;
use crate::id::{AgentId, NodeId};
use crate::masking::mask_known_secrets;
use crate::stateless::constant_time_eq;

/// Prefix for revocable device-bound node bearer tokens (`ndt_<64-hex>`).
pub const NODE_TOKEN_PREFIX: &str = "ndt_";
const NODE_TOKEN_BYTES: usize = 32;

/// Slice 1A runtime capability implemented end-to-end.
pub const NODE_CAPABILITY_EXEC: &str = "exec";

/// Extensible capability names accepted by `nodes.capabilities_json` and
/// `node_grants.capability`. Slice 1A runtime executes strictly `exec`.
pub const KNOWN_NODE_CAPABILITIES: &[&str] = &["exec", "exit", "files", "browser", "screen", "gpu"];

/// Lifecycle state of an enrolled edge node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Online,
    Offline,
    Paused,
    Revoked,
}

impl NodeStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Online => "online",
            Self::Offline => "offline",
            Self::Paused => "paused",
            Self::Revoked => "revoked",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, Error> {
        match raw.trim() {
            "online" => Ok(Self::Online),
            "offline" => Ok(Self::Offline),
            "paused" => Ok(Self::Paused),
            "revoked" => Ok(Self::Revoked),
            other => Err(Error::Message(format!("invalid node status `{other}`"))),
        }
    }
}

/// Public durable representation of a paired edge node (never includes `token_hash`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: NodeId,
    pub owner_id: String,
    pub name: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub capabilities: Vec<String>,
    pub status: NodeStatus,
    pub last_heartbeat_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub revoked_at_ms: Option<i64>,
}

/// Newly paired node plus the one-time plaintext device-bound token.
#[derive(Clone, PartialEq, Eq)]
pub struct PairedNode {
    pub node: NodeRecord,
    pub node_token: String,
}

impl std::fmt::Debug for PairedNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PairedNode")
            .field("node", &self.node)
            .field("node_token", &"<redacted>")
            .finish()
    }
}

/// Durable capability grant authorizing one agent to invoke one capability on one node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeGrant {
    pub id: String,
    pub node_id: NodeId,
    pub agent_id: AgentId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    pub capability: String,
    pub granted_by: String,
    pub created_at_ms: i64,
    pub revoked_at_ms: Option<i64>,
}

/// Append-only audit row for node control and capability dispatch events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAuditEvent {
    pub id: String,
    pub node_id: NodeId,
    pub agent_id: Option<String>,
    pub capability: String,
    pub action: String,
    pub summary: String,
    pub exit_code: Option<i64>,
    pub outcome: String,
    pub started_at_ms: i64,
    pub finished_at_ms: Option<i64>,
}

/// Input parameters to pair a new edge node.
#[derive(Clone, Debug)]
pub struct NewNodePair {
    pub owner_id: String,
    pub name: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub capabilities: Vec<String>,
}

/// Input parameters for recording an append-only node audit event.
#[derive(Clone, Debug)]
pub struct NewNodeAuditEvent {
    pub node_id: NodeId,
    pub agent_id: Option<String>,
    pub capability: String,
    pub action: String,
    pub summary: String,
    pub exit_code: Option<i64>,
    pub outcome: String,
    pub started_at_ms: i64,
    pub finished_at_ms: Option<i64>,
}

/// Whether `token` has the syntactic shape of a node bearer token (`ndt_<64-hex>`).
pub fn is_node_token(token: &str) -> bool {
    token.strip_prefix(NODE_TOKEN_PREFIX).is_some_and(|rest| {
        rest.len() == NODE_TOKEN_BYTES * 2 && rest.bytes().all(|b| b.is_ascii_hexdigit())
    })
}

/// Validate a node capability name against known extensible capability tokens.
pub fn validate_node_capability(raw: &str) -> Result<String, Error> {
    let clean = raw.trim().to_ascii_lowercase();
    if !KNOWN_NODE_CAPABILITIES.contains(&clean.as_str()) {
        return Err(Error::Message(format!(
            "unsupported node capability `{raw}` (supported: {})",
            KNOWN_NODE_CAPABILITIES.join(", ")
        )));
    }
    Ok(clean)
}

fn normalize_capabilities(raw: &[String]) -> Result<Vec<String>, Error> {
    if raw.is_empty() {
        return Err(Error::Message(
            "node must advertise at least one capability".into(),
        ));
    }
    let mut set = BTreeSet::new();
    for cap in raw {
        set.insert(validate_node_capability(cap)?);
    }
    Ok(set.into_iter().collect())
}

fn validate_node_field(value: &str, label: &str, max_len: usize) -> Result<String, Error> {
    let trimmed = value.trim();
    if trimmed.is_empty()
        || trimmed.len() > max_len
        || trimmed.chars().any(|c| c.is_control() || c == '\0')
    {
        return Err(Error::Message(format!(
            "invalid node {label}: must be 1..={max_len} non-control characters"
        )));
    }
    Ok(trimmed.to_string())
}

fn sha256_hex(input: &str) -> String {
    let mut out = String::with_capacity(64);
    for byte in Sha256::digest(input.as_bytes()) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn random_node_token() -> Result<String, Error> {
    let mut bytes = [0u8; NODE_TOKEN_BYTES];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let mut out = String::with_capacity(NODE_TOKEN_PREFIX.len() + NODE_TOKEN_BYTES * 2);
    out.push_str(NODE_TOKEN_PREFIX);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    Ok(out)
}

fn new_prefixed_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::now_v7().simple())
}

const NODE_COLS: &str = "id, owner_id, name, hostname, os, arch, capabilities_json, status, last_heartbeat_at_ms, created_at_ms, revoked_at_ms";

fn node_from_row(row: &DbRow<'_>) -> Result<NodeRecord, Error> {
    let caps_json: String = row.get(6)?;
    let capabilities: Vec<String> = serde_json::from_str(&caps_json)
        .map_err(|err| Error::Message(format!("corrupt node capabilities_json: {err}")))?;
    let status_raw: String = row.get(7)?;
    Ok(NodeRecord {
        id: row.parse_str(0)?,
        owner_id: row.get(1)?,
        name: row.get(2)?,
        hostname: row.get(3)?,
        os: row.get(4)?,
        arch: row.get(5)?,
        capabilities,
        status: NodeStatus::parse(&status_raw)?,
        last_heartbeat_at_ms: row.get(8)?,
        created_at_ms: row.get(9)?,
        revoked_at_ms: row.get(10)?,
    })
}

fn grant_from_row(row: &DbRow<'_>) -> Result<NodeGrant, Error> {
    Ok(NodeGrant {
        id: row.get(0)?,
        node_id: row.parse_str(1)?,
        agent_id: row.parse_str(2)?,
        agent_name: row.get(3)?,
        capability: row.get(4)?,
        granted_by: row.get(5)?,
        created_at_ms: row.get(6)?,
        revoked_at_ms: row.get(7)?,
    })
}

fn audit_from_row(row: &DbRow<'_>) -> Result<NodeAuditEvent, Error> {
    Ok(NodeAuditEvent {
        id: row.get(0)?,
        node_id: row.parse_str(1)?,
        agent_id: row.get(2)?,
        capability: row.get(3)?,
        action: row.get(4)?,
        summary: row.get(5)?,
        exit_code: row.get(6)?,
        outcome: row.get(7)?,
        started_at_ms: row.get(8)?,
        finished_at_ms: row.get(9)?,
    })
}

fn insert_audit_tx(
    tx: &(impl DbExec + ?Sized),
    event: &NewNodeAuditEvent,
) -> Result<NodeAuditEvent, Error> {
    let id = new_prefixed_id("na");
    let masked_summary = mask_known_secrets(&event.summary);
    let cap = event.capability.trim();
    let action = validate_node_field(&event.action, "audit action", 64)?;
    let outcome = validate_node_field(&event.outcome, "audit outcome", 64)?;
    tx.db_execute(
        "INSERT INTO node_audit_events
            (id, node_id, agent_id, capability, action, summary, exit_code, outcome, started_at_ms, finished_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        &db_params![
            id,
            event.node_id.as_str(),
            event.agent_id,
            cap,
            action,
            masked_summary,
            event.exit_code,
            outcome,
            event.started_at_ms,
            event.finished_at_ms
        ],
    )?;
    Ok(NodeAuditEvent {
        id,
        node_id: event.node_id.clone(),
        agent_id: event.agent_id.clone(),
        capability: cap.to_string(),
        action,
        summary: masked_summary.into_owned(),
        exit_code: event.exit_code,
        outcome,
        started_at_ms: event.started_at_ms,
        finished_at_ms: event.finished_at_ms,
    })
}

fn load_node_tx(tx: &(impl DbExec + ?Sized), node_id: &NodeId) -> Result<NodeRecord, Error> {
    tx.db_query_opt(
        &format!("SELECT {NODE_COLS} FROM nodes WHERE id = ?1"),
        &db_params![node_id.as_str()],
        node_from_row,
    )?
    .ok_or_else(|| Error::Message(format!("node not found: {node_id}")))
}

impl Store {
    /// Pair and enroll a new edge node, storing only the SHA-256 hash of its
    /// revocable `ndt_...` device-bound bearer token.
    pub fn pair_node(&self, input: NewNodePair) -> Result<PairedNode, Error> {
        let owner_id = validate_node_field(&input.owner_id, "owner_id", 256)?;
        let name = validate_node_field(&input.name, "name", 120)?;
        let hostname = validate_node_field(&input.hostname, "hostname", 255)?;
        let os = validate_node_field(&input.os, "os", 64)?;
        let arch = validate_node_field(&input.arch, "arch", 64)?;
        let capabilities = normalize_capabilities(&input.capabilities)?;
        let capabilities_json = serde_json::to_string(&capabilities)
            .map_err(|err| Error::Message(format!("failed to encode capabilities: {err}")))?;
        let id = NodeId::generate();
        let node_token = random_node_token()?;
        let token_hash = sha256_hex(&node_token);
        let now = now_ms();

        let node = self.write_tx(|tx| {
            tx.db_execute(
                "INSERT INTO nodes
                    (id, owner_id, name, hostname, os, arch, capabilities_json, token_hash, status, last_heartbeat_at_ms, created_at_ms, revoked_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'offline', NULL, ?9, NULL)",
                &db_params![
                    id.as_str(),
                    owner_id,
                    name,
                    hostname,
                    os,
                    arch,
                    capabilities_json,
                    token_hash,
                    now
                ],
            )?;
            let _ = insert_audit_tx(
                tx,
                &NewNodeAuditEvent {
                    node_id: id.clone(),
                    agent_id: None,
                    capability: "control".into(),
                    action: "pair".into(),
                    summary: format!("paired node {name} ({os}/{arch}) by {owner_id}"),
                    exit_code: None,
                    outcome: "ok".into(),
                    started_at_ms: now,
                    finished_at_ms: Some(now),
                },
            )?;
            load_node_tx(tx, &id)
        })?;
        Ok(PairedNode { node, node_token })
    }

    /// Authenticate an outbound WS reverse-tunnel bearer token fail-closed.
    pub fn authenticate_node_token(&self, token: &str) -> Result<NodeRecord, Error> {
        let trimmed = token.trim();
        if !is_node_token(trimmed) {
            return Err(Error::InvalidCredential("invalid node bearer token".into()));
        }
        let presented_hash = sha256_hex(trimmed);
        let conn = self.read_conn();
        let found: Option<(NodeRecord, String)> = conn.db_query_opt(
            &format!("SELECT {NODE_COLS}, token_hash FROM nodes WHERE token_hash = ?1"),
            &db_params![presented_hash],
            |row| Ok((node_from_row(row)?, row.get(11)?)),
        )?;
        let Some((node, stored_hash)) = found else {
            return Err(Error::InvalidCredential("unknown node bearer token".into()));
        };
        if !constant_time_eq(presented_hash.as_bytes(), stored_hash.as_bytes()) {
            return Err(Error::InvalidCredential(
                "node bearer token mismatch".into(),
            ));
        }
        if node.status == NodeStatus::Revoked || node.revoked_at_ms.is_some() {
            return Err(Error::PermissionDenied(format!(
                "node `{}` is revoked",
                node.id
            )));
        }
        Ok(node)
    }

    /// Mark a node connected over WS (`offline` -> `online`; preserves `paused`;
    /// rejects `revoked`) and refresh heartbeat/metadata.
    pub fn set_node_connected(
        &self,
        node_id: &NodeId,
        metadata: Option<(&str, &str, &str, &[String])>,
    ) -> Result<NodeRecord, Error> {
        let now = now_ms();
        let validated_meta = match metadata {
            Some((hostname, os, arch, caps)) => Some((
                validate_node_field(hostname, "hostname", 255)?,
                validate_node_field(os, "os", 64)?,
                validate_node_field(arch, "arch", 64)?,
                serde_json::to_string(&normalize_capabilities(caps)?)
                    .map_err(|err| Error::Message(err.to_string()))?,
            )),
            None => None,
        };
        self.write_tx(|tx| {
            let current = load_node_tx(tx, node_id)?;
            if current.status == NodeStatus::Revoked || current.revoked_at_ms.is_some() {
                return Err(Error::PermissionDenied(format!(
                    "node `{node_id}` is revoked"
                )));
            }
            let next_status = match current.status {
                NodeStatus::Offline => NodeStatus::Online,
                other => other,
            };
            if let Some((hostname, os, arch, caps_json)) = validated_meta {
                tx.db_execute(
                    "UPDATE nodes
                     SET status = ?2, last_heartbeat_at_ms = ?3, hostname = ?4, os = ?5, arch = ?6, capabilities_json = ?7
                     WHERE id = ?1 AND status != 'revoked'",
                    &db_params![
                        node_id.as_str(),
                        next_status.as_str(),
                        now,
                        hostname,
                        os,
                        arch,
                        caps_json
                    ],
                )?;
            } else {
                tx.db_execute(
                    "UPDATE nodes
                     SET status = ?2, last_heartbeat_at_ms = ?3
                     WHERE id = ?1 AND status != 'revoked'",
                    &db_params![node_id.as_str(), next_status.as_str(), now],
                )?;
            }
            load_node_tx(tx, node_id)
        })
    }

    /// Update `last_heartbeat_at_ms` for a non-revoked node and return its current record.
    pub fn touch_node_heartbeat(&self, node_id: &NodeId) -> Result<NodeRecord, Error> {
        let now = now_ms();
        self.write_tx(|tx| {
            tx.db_execute(
                "UPDATE nodes SET last_heartbeat_at_ms = ?2 WHERE id = ?1 AND status != 'revoked'",
                &db_params![node_id.as_str(), now],
            )?;
            load_node_tx(tx, node_id)
        })
    }

    /// Transition `online` -> `offline` on WS disconnect (preserving `paused` / `revoked`).
    pub fn set_node_offline(&self, node_id: &NodeId) -> Result<(), Error> {
        self.write_tx(|tx| {
            tx.db_execute(
                "UPDATE nodes SET status = 'offline' WHERE id = ?1 AND status = 'online'",
                &db_params![node_id.as_str()],
            )?;
            Ok(())
        })
    }

    /// Immediately switch a non-revoked node to `paused` and append an audit event.
    pub fn pause_node(&self, node_id: &NodeId, actor: &str) -> Result<NodeRecord, Error> {
        let now = now_ms();
        self.write_tx(|tx| {
            let current = load_node_tx(tx, node_id)?;
            if current.status == NodeStatus::Revoked || current.revoked_at_ms.is_some() {
                return Err(Error::PermissionDenied(format!(
                    "node `{node_id}` is revoked and cannot be paused"
                )));
            }
            tx.db_execute(
                "UPDATE nodes SET status = 'paused' WHERE id = ?1 AND status != 'revoked'",
                &db_params![node_id.as_str()],
            )?;
            let _ = insert_audit_tx(
                tx,
                &NewNodeAuditEvent {
                    node_id: node_id.clone(),
                    agent_id: None,
                    capability: "control".into(),
                    action: "pause".into(),
                    summary: format!("node paused by {actor}"),
                    exit_code: None,
                    outcome: "ok".into(),
                    started_at_ms: now,
                    finished_at_ms: Some(now),
                },
            )?;
            load_node_tx(tx, node_id)
        })
    }

    /// Resume a `paused` node back to `online` (if tunnel is active) or `offline`.
    pub fn resume_node(
        &self,
        node_id: &NodeId,
        is_ws_connected: bool,
        actor: &str,
    ) -> Result<NodeRecord, Error> {
        let now = now_ms();
        self.write_tx(|tx| {
            let current = load_node_tx(tx, node_id)?;
            if current.status == NodeStatus::Revoked || current.revoked_at_ms.is_some() {
                return Err(Error::PermissionDenied(format!(
                    "node `{node_id}` is revoked and cannot be resumed"
                )));
            }
            let target_status = if is_ws_connected {
                NodeStatus::Online
            } else {
                NodeStatus::Offline
            };
            tx.db_execute(
                "UPDATE nodes SET status = ?2 WHERE id = ?1 AND status != 'revoked'",
                &db_params![node_id.as_str(), target_status.as_str()],
            )?;
            let _ = insert_audit_tx(
                tx,
                &NewNodeAuditEvent {
                    node_id: node_id.clone(),
                    agent_id: None,
                    capability: "control".into(),
                    action: "resume".into(),
                    summary: format!("node resumed ({}) by {actor}", target_status.as_str()),
                    exit_code: None,
                    outcome: "ok".into(),
                    started_at_ms: now,
                    finished_at_ms: Some(now),
                },
            )?;
            load_node_tx(tx, node_id)
        })
    }

    /// Permanently revoke a node and all of its active grants.
    pub fn revoke_node(&self, node_id: &NodeId, actor: &str) -> Result<NodeRecord, Error> {
        let now = now_ms();
        self.write_tx(|tx| {
            let _ = load_node_tx(tx, node_id)?;
            tx.db_execute(
                "UPDATE nodes SET status = 'revoked', revoked_at_ms = COALESCE(revoked_at_ms, ?2) WHERE id = ?1",
                &db_params![node_id.as_str(), now],
            )?;
            tx.db_execute(
                "UPDATE node_grants SET revoked_at_ms = ?2 WHERE node_id = ?1 AND revoked_at_ms IS NULL",
                &db_params![node_id.as_str(), now],
            )?;
            let _ = insert_audit_tx(
                tx,
                &NewNodeAuditEvent {
                    node_id: node_id.clone(),
                    agent_id: None,
                    capability: "control".into(),
                    action: "revoke".into(),
                    summary: format!("node revoked by {actor}"),
                    exit_code: None,
                    outcome: "ok".into(),
                    started_at_ms: now,
                    finished_at_ms: Some(now),
                },
            )?;
            load_node_tx(tx, node_id)
        })
    }

    pub fn get_node(&self, node_id: &NodeId) -> Result<Option<NodeRecord>, Error> {
        self.read_conn().db_query_opt(
            &format!("SELECT {NODE_COLS} FROM nodes WHERE id = ?1"),
            &db_params![node_id.as_str()],
            node_from_row,
        )
    }

    pub fn list_nodes(&self) -> Result<Vec<NodeRecord>, Error> {
        self.read_conn().db_query_vec(
            &format!("SELECT {NODE_COLS} FROM nodes ORDER BY created_at_ms DESC, id DESC"),
            &[],
            node_from_row,
        )
    }

    /// Return only non-revoked nodes on which `agent_id` holds at least one active grant.
    pub fn list_nodes_for_agent(&self, agent_id: &AgentId) -> Result<Vec<NodeRecord>, Error> {
        self.read_conn().db_query_vec(
            &format!(
                "SELECT {NODE_COLS} FROM nodes n
                 WHERE n.status != 'revoked'
                   AND n.revoked_at_ms IS NULL
                   AND EXISTS (
                       SELECT 1 FROM node_grants g
                       WHERE g.node_id = n.id
                         AND g.agent_id = ?1
                         AND g.revoked_at_ms IS NULL
                   )
                 ORDER BY n.created_at_ms DESC, n.id DESC"
            ),
            &db_params![agent_id],
            node_from_row,
        )
    }

    /// Grant `(node_id, agent_id, capability)` idempotently.
    pub fn grant_node_capability(
        &self,
        node_id: &NodeId,
        agent_id: &AgentId,
        capability: &str,
        granted_by: &str,
    ) -> Result<NodeGrant, Error> {
        let cap = validate_node_capability(capability)?;
        let actor = validate_node_field(granted_by, "granted_by", 256)?;
        let now = now_ms();
        self.write_tx(|tx| {
            let node = load_node_tx(tx, node_id)?;
            if node.status == NodeStatus::Revoked || node.revoked_at_ms.is_some() {
                return Err(Error::PermissionDenied(format!(
                    "cannot grant capability on revoked node `{node_id}`"
                )));
            }
            agent_exists(tx, agent_id)?;
            if let Some(existing) = tx.db_query_opt(
                "SELECT g.id, g.node_id, g.agent_id, a.name, g.capability, g.granted_by, g.created_at_ms, g.revoked_at_ms
                 FROM node_grants g
                 LEFT JOIN agents a ON a.id = g.agent_id
                 WHERE g.node_id = ?1 AND g.agent_id = ?2 AND g.capability = ?3 AND g.revoked_at_ms IS NULL",
                &db_params![node_id.as_str(), agent_id, cap],
                grant_from_row,
            )? {
                return Ok(existing);
            }
            let grant_id = new_prefixed_id("ng");
            tx.db_execute(
                "INSERT INTO node_grants
                    (id, node_id, agent_id, capability, granted_by, created_at_ms, revoked_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
                &db_params![grant_id, node_id.as_str(), agent_id, cap, actor, now],
            )?;
            let _ = insert_audit_tx(
                tx,
                &NewNodeAuditEvent {
                    node_id: node_id.clone(),
                    agent_id: Some(agent_id.to_string()),
                    capability: cap.clone(),
                    action: "grant".into(),
                    summary: format!("granted {cap} to {agent_id} by {actor}"),
                    exit_code: None,
                    outcome: "ok".into(),
                    started_at_ms: now,
                    finished_at_ms: Some(now),
                },
            )?;
            tx.db_query_one(
                "SELECT g.id, g.node_id, g.agent_id, a.name, g.capability, g.granted_by, g.created_at_ms, g.revoked_at_ms
                 FROM node_grants g
                 LEFT JOIN agents a ON a.id = g.agent_id
                 WHERE g.id = ?1",
                &db_params![grant_id],
                grant_from_row,
            )
        })
    }

    /// Revoke an active node grant by ID.
    pub fn revoke_node_grant(
        &self,
        node_id: &NodeId,
        grant_id: &str,
        actor: &str,
    ) -> Result<bool, Error> {
        let now = now_ms();
        let actor = validate_node_field(actor, "actor", 256)?;
        self.write_tx(|tx| {
            let existing: Option<(String, String)> = tx.db_query_opt(
                "SELECT agent_id, capability FROM node_grants
                 WHERE id = ?1 AND node_id = ?2 AND revoked_at_ms IS NULL",
                &db_params![grant_id, node_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let Some((agent_id, capability)) = existing else {
                return Ok(false);
            };
            let updated = tx.db_execute(
                "UPDATE node_grants SET revoked_at_ms = ?3
                 WHERE id = ?1 AND node_id = ?2 AND revoked_at_ms IS NULL",
                &db_params![grant_id, node_id.as_str(), now],
            )?;
            if updated > 0 {
                let _ = insert_audit_tx(
                    tx,
                    &NewNodeAuditEvent {
                        node_id: node_id.clone(),
                        agent_id: Some(agent_id.clone()),
                        capability: capability.clone(),
                        action: "grant_revoke".into(),
                        summary: format!(
                            "revoked {capability} grant {grant_id} for {agent_id} by {actor}"
                        ),
                        exit_code: None,
                        outcome: "ok".into(),
                        started_at_ms: now,
                        finished_at_ms: Some(now),
                    },
                )?;
            }
            Ok(updated > 0)
        })
    }

    pub fn list_node_grants(&self, node_id: &NodeId) -> Result<Vec<NodeGrant>, Error> {
        self.read_conn().db_query_vec(
            "SELECT g.id, g.node_id, g.agent_id, a.name, g.capability, g.granted_by, g.created_at_ms, g.revoked_at_ms
             FROM node_grants g
             LEFT JOIN agents a ON a.id = g.agent_id
             WHERE g.node_id = ?1 AND g.revoked_at_ms IS NULL
             ORDER BY g.created_at_ms DESC, g.id DESC",
            &db_params![node_id.as_str()],
            grant_from_row,
        )
    }

    /// Fail-closed authorization check for dispatching a capability to `node_id` as `agent_id`
    /// (or as operator when `agent_id` is `None`).
    pub fn check_node_capability_authorized(
        &self,
        node_id: &NodeId,
        agent_id: Option<&AgentId>,
        capability: &str,
    ) -> Result<NodeRecord, Error> {
        let cap = validate_node_capability(capability)?;
        if cap != NODE_CAPABILITY_EXEC {
            return Err(Error::PermissionDenied(format!(
                "capability `{cap}` is not enabled on node runtime (Slice 1A supports `exec` only)"
            )));
        }
        let conn = self.read_conn();
        let node = conn
            .db_query_opt(
                &format!("SELECT {NODE_COLS} FROM nodes WHERE id = ?1"),
                &db_params![node_id.as_str()],
                node_from_row,
            )?
            .ok_or_else(|| Error::Message(format!("node not found: {node_id}")))?;
        if node.status != NodeStatus::Online {
            return Err(Error::StateConflict {
                machine: "node",
                expected: NodeStatus::Online.as_str().into(),
                actual: node.status.as_str().into(),
            });
        }
        if !node
            .capabilities
            .iter()
            .any(|advertised| advertised == &cap)
        {
            return Err(Error::PermissionDenied(format!(
                "node `{}` does not advertise capability `{cap}`",
                node.id
            )));
        }
        if let Some(agent_id) = agent_id {
            let has_grant: bool = conn.db_query_one(
                "SELECT EXISTS(
                    SELECT 1 FROM node_grants
                    WHERE node_id = ?1 AND agent_id = ?2 AND capability = ?3 AND revoked_at_ms IS NULL
                )",
                &db_params![node_id.as_str(), agent_id, cap],
                |row| row.get::<bool>(0),
            )?;
            if !has_grant {
                return Err(Error::PermissionDenied(format!(
                    "agent `{agent_id}` has no active `{cap}` grant on node `{node_id}`"
                )));
            }
        }
        Ok(node)
    }

    /// Check whether `agent_id` has ANY active non-revoked grant on `node_id` (for read visibility).
    pub fn agent_has_any_node_grant(
        &self,
        node_id: &NodeId,
        agent_id: &AgentId,
    ) -> Result<bool, Error> {
        self.read_conn().db_query_one(
            "SELECT EXISTS(
                SELECT 1 FROM node_grants
                WHERE node_id = ?1 AND agent_id = ?2 AND revoked_at_ms IS NULL
            )",
            &db_params![node_id.as_str(), agent_id],
            |row| row.get::<bool>(0),
        )
    }

    pub fn record_node_audit_event(
        &self,
        event: NewNodeAuditEvent,
    ) -> Result<NodeAuditEvent, Error> {
        self.write_tx(|tx| insert_audit_tx(tx, &event))
    }

    pub fn list_node_audit_events(
        &self,
        node_id: &NodeId,
        limit: usize,
    ) -> Result<Vec<NodeAuditEvent>, Error> {
        let limit = limit.clamp(1, 200) as i64;
        self.read_conn().db_query_vec(
            "SELECT id, node_id, agent_id, capability, action, summary, exit_code, outcome, started_at_ms, finished_at_ms
             FROM node_audit_events
             WHERE node_id = ?1
             ORDER BY started_at_ms DESC, id DESC
             LIMIT ?2",
            &db_params![node_id.as_str(), limit],
            audit_from_row,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NewAgent;

    fn test_pg_url() -> Option<String> {
        std::env::var("ANYAGENTS_TEST_POSTGRES_URL")
            .or_else(|_| std::env::var("TEST_DATABASE_URL"))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    fn exercise_nodes_store(store: &Store) {
        let alpha = store
            .create_agent(NewAgent::new(
                format!("alpha-{}", Uuid::now_v7().simple()),
                "codex",
            ))
            .unwrap();
        let beta = store
            .create_agent(NewAgent::new(
                format!("beta-{}", Uuid::now_v7().simple()),
                "codex",
            ))
            .unwrap();

        let paired = store
            .pair_node(NewNodePair {
                owner_id: "owner@example.test".into(),
                name: "workstation".into(),
                hostname: "workstation.local".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
                capabilities: vec!["exec".into()],
            })
            .unwrap();
        assert!(is_node_token(&paired.node_token));
        assert_eq!(paired.node.status, NodeStatus::Offline);
        assert_eq!(paired.node.capabilities, vec!["exec"]);

        // Auth works with raw token, never stores plaintext.
        let authed = store.authenticate_node_token(&paired.node_token).unwrap();
        assert_eq!(authed.id, paired.node.id);
        assert!(store.authenticate_node_token("ndt_0000").is_err());

        // Exit-only node is valid in schema/capabilities_json but fails closed on `exec`.
        let exit_only = store
            .pair_node(NewNodePair {
                owner_id: "owner@example.test".into(),
                name: "exit-relay".into(),
                hostname: "exit.local".into(),
                os: "linux".into(),
                arch: "aarch64".into(),
                capabilities: vec!["exit".into()],
            })
            .unwrap();
        store.set_node_connected(&exit_only.node.id, None).unwrap();
        let err = store
            .check_node_capability_authorized(&exit_only.node.id, Some(&alpha.id), "exec")
            .unwrap_err();
        assert!(err.to_string().contains("does not advertise capability"));
        let err = store
            .check_node_capability_authorized(&exit_only.node.id, Some(&alpha.id), "exit")
            .unwrap_err();
        assert!(err.to_string().contains("Slice 1A supports `exec` only"));

        // Offline node rejects exec even with grant; granting + online admits.
        let grant = store
            .grant_node_capability(&paired.node.id, &alpha.id, "exec", "owner")
            .unwrap();
        assert!(
            store
                .check_node_capability_authorized(&paired.node.id, Some(&alpha.id), "exec")
                .is_err()
        );
        assert!(
            store
                .check_node_capability_authorized(&paired.node.id, Some(&beta.id), "exec")
                .is_err()
        );

        let connected = store.set_node_connected(&paired.node.id, None).unwrap();
        assert_eq!(connected.status, NodeStatus::Online);
        assert!(
            store
                .check_node_capability_authorized(&paired.node.id, Some(&alpha.id), "exec")
                .is_ok()
        );
        assert!(
            store
                .check_node_capability_authorized(&paired.node.id, Some(&beta.id), "exec")
                .is_err()
        );

        // Agent visibility strictly scoped to granted nodes.
        let alpha_nodes = store.list_nodes_for_agent(&alpha.id).unwrap();
        assert_eq!(alpha_nodes.len(), 1);
        assert_eq!(alpha_nodes[0].id, paired.node.id);
        assert!(store.list_nodes_for_agent(&beta.id).unwrap().is_empty());

        // Pause fails closed on exec and preserves paused across reconnect/disconnect.
        let paused = store.pause_node(&paired.node.id, "owner").unwrap();
        assert_eq!(paused.status, NodeStatus::Paused);
        assert!(
            store
                .check_node_capability_authorized(&paired.node.id, Some(&alpha.id), "exec")
                .is_err()
        );
        let still_paused = store.set_node_connected(&paired.node.id, None).unwrap();
        assert_eq!(still_paused.status, NodeStatus::Paused);
        let resumed = store.resume_node(&paired.node.id, true, "owner").unwrap();
        assert_eq!(resumed.status, NodeStatus::Online);

        // Secret masking on audit events.
        let ev = store
            .record_node_audit_event(NewNodeAuditEvent {
                node_id: paired.node.id.clone(),
                agent_id: Some(alpha.id.to_string()),
                capability: "exec".into(),
                action: "exec".into(),
                summary: "curl -H Bearer secretcanarytoken123456".into(),
                exit_code: Some(0),
                outcome: "ok".into(),
                started_at_ms: 1000,
                finished_at_ms: Some(1100),
            })
            .unwrap();
        assert!(!ev.summary.contains("secretcanarytoken123456"));
        assert!(ev.summary.contains("[redacted]"));

        // Grant revocation denies exec immediately.
        assert!(
            store
                .revoke_node_grant(&paired.node.id, &grant.id, "owner")
                .unwrap()
        );
        assert!(
            store
                .check_node_capability_authorized(&paired.node.id, Some(&alpha.id), "exec")
                .is_err()
        );

        // Node revocation rejects token auth, resume, and new grants.
        let revoked = store.revoke_node(&paired.node.id, "owner").unwrap();
        assert_eq!(revoked.status, NodeStatus::Revoked);
        assert!(store.authenticate_node_token(&paired.node_token).is_err());
        assert!(store.resume_node(&paired.node.id, true, "owner").is_err());

        let events = store.list_node_audit_events(&paired.node.id, 50).unwrap();
        assert!(events.len() >= 5);
    }

    #[test]
    fn sqlite_nodes_lifecycle_grants_and_append_only_audit() {
        let dir = crate::test_tempdir();
        let store = Store::open(dir.path().join("state.db")).unwrap();
        exercise_nodes_store(&store);

        // Verify append-only trigger rejects UPDATE and DELETE on SQLite.
        let node = store
            .pair_node(NewNodePair {
                owner_id: "owner".into(),
                name: "immutable".into(),
                hostname: "host".into(),
                os: "linux".into(),
                arch: "x86_64".into(),
                capabilities: vec!["exec".into()],
            })
            .unwrap();
        let err = store.write_tx(|tx| {
            tx.db_execute(
                "UPDATE node_audit_events SET outcome = 'tampered' WHERE node_id = ?1",
                &db_params![node.node.id.as_str()],
            )?;
            Ok(())
        });
        assert!(err.is_err());
        let err = store.write_tx(|tx| {
            tx.db_execute(
                "DELETE FROM node_audit_events WHERE node_id = ?1",
                &db_params![node.node.id.as_str()],
            )?;
            Ok(())
        });
        assert!(err.is_err());
    }

    #[test]
    fn postgres_nodes_lifecycle_grants_and_append_only_audit() {
        let Some(url) = test_pg_url() else {
            eprintln!("skipping PostgreSQL nodes test: ANYAGENTS_TEST_POSTGRES_URL not set");
            return;
        };
        let dir = crate::test_tempdir();
        let store = Store::open_postgres(&url, dir.path()).unwrap();
        exercise_nodes_store(&store);
    }
}
