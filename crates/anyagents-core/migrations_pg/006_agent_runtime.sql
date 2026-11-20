-- AnyAgents PostgreSQL 18 schema migration 006: First-class execution runtime
-- (`local`, `sandbox`, `sandbox:<provider>`, `cloud`) and optional per-agent
-- remote spend budget cap in micro-USD (ADR 0010).
ALTER TABLE public.agents ADD COLUMN IF NOT EXISTS runtime TEXT NOT NULL DEFAULT 'local';
ALTER TABLE public.agents ADD COLUMN IF NOT EXISTS remote_budget_micros BIGINT;

UPDATE public.agents
SET runtime = 'cloud'
WHERE runtime = 'local'
  AND harness IN ('spiron-cloud', 'claude-cloud', 'openai-cloud', 'cursor-cloud');
