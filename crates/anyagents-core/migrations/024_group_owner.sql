-- Ownership is explicit and independent of membership. Legacy groups stay
-- operator-owned (NULL); deleting an owner returns authority to the operator.
ALTER TABLE groups ADD COLUMN owner_agent_id TEXT REFERENCES agents (id) ON DELETE SET NULL;
