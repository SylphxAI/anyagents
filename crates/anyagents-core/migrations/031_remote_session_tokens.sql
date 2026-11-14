-- The credential a remote (cloud) session presents when it calls back into
-- this server's MCP endpoint. Each token is bound to one agent and one remote
-- session, expires on its own, and is revoked when the session ends. Only the
-- token's SHA-256 digest is kept: the token itself lives in the agent's config
-- directory and with the remote vendor. A local mailbox capability
-- (`agent_capabilities`) is a different kind and never authenticates here.
CREATE TABLE remote_session_tokens (
    token_hash TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agents (id),
    session_id TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    revoked_at_ms INTEGER
);

CREATE INDEX remote_session_tokens_by_agent
    ON remote_session_tokens (agent_id, session_id, expires_at_ms);

-- Pausing or archiving an agent ends its remote session in the same commit,
-- whichever code path or process changed the state.
CREATE TRIGGER remote_session_tokens_revoke_on_stop
AFTER UPDATE OF state ON agents
WHEN NEW.state IN ('paused', 'archived')
BEGIN
    UPDATE remote_session_tokens
       SET revoked_at_ms = CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
     WHERE agent_id = NEW.id AND revoked_at_ms IS NULL;
END;
