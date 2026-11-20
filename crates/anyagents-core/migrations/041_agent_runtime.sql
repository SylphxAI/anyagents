-- First-class execution runtime (`local`, `sandbox`, `sandbox:<provider>`, `cloud`)
-- and optional per-agent remote spend budget cap in micro-USD (ADR 0010).
ALTER TABLE agents ADD COLUMN runtime TEXT NOT NULL DEFAULT 'local';
ALTER TABLE agents ADD COLUMN remote_budget_micros INTEGER;

UPDATE agents
SET runtime = 'cloud'
WHERE runtime = 'local'
  AND harness IN ('spiron-cloud', 'claude-cloud', 'openai-cloud', 'cursor-cloud');
