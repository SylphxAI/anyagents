ALTER TABLE agents ADD COLUMN parent_id TEXT REFERENCES agents(id);
CREATE INDEX IF NOT EXISTS idx_agents_parent ON agents(parent_id);
