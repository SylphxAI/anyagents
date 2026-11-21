-- Native remote user-device / edge nodes, scoped capability grants, and
-- append-only execution audit log.
CREATE TABLE nodes (
    id TEXT PRIMARY KEY NOT NULL,
    owner_id TEXT NOT NULL,
    name TEXT NOT NULL,
    hostname TEXT NOT NULL,
    os TEXT NOT NULL,
    arch TEXT NOT NULL,
    capabilities_json TEXT NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('online', 'offline', 'paused', 'revoked')),
    last_heartbeat_at_ms BIGINT,
    created_at_ms BIGINT NOT NULL,
    revoked_at_ms BIGINT
);
CREATE INDEX idx_nodes_owner_status ON nodes(owner_id, status);

CREATE TABLE node_grants (
    id TEXT PRIMARY KEY NOT NULL,
    node_id TEXT NOT NULL REFERENCES nodes(id),
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    capability TEXT NOT NULL,
    granted_by TEXT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    revoked_at_ms BIGINT
);
CREATE UNIQUE INDEX uq_node_grants_active
    ON node_grants(node_id, agent_id, capability)
    WHERE revoked_at_ms IS NULL;
CREATE INDEX idx_node_grants_agent
    ON node_grants(agent_id, node_id)
    WHERE revoked_at_ms IS NULL;

CREATE TABLE node_audit_events (
    id TEXT PRIMARY KEY NOT NULL,
    node_id TEXT NOT NULL REFERENCES nodes(id),
    agent_id TEXT,
    capability TEXT NOT NULL,
    action TEXT NOT NULL,
    summary TEXT NOT NULL,
    exit_code BIGINT,
    outcome TEXT NOT NULL,
    started_at_ms BIGINT NOT NULL,
    finished_at_ms BIGINT
);
CREATE INDEX idx_node_audit_events_node_time
    ON node_audit_events(node_id, started_at_ms DESC, id DESC);

CREATE FUNCTION reject_node_audit_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'node audit events are append-only'; END;
$$;
CREATE TRIGGER node_audit_events_no_mutation BEFORE UPDATE OR DELETE OR TRUNCATE
ON node_audit_events FOR EACH STATEMENT EXECUTE FUNCTION reject_node_audit_mutation();
