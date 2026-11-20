-- AnyAgents PostgreSQL 18 schema migration 007: Persisted external timer ID for
-- event-driven schedule cancellation across stateless serverless cold starts.
ALTER TABLE public.agent_schedules ADD COLUMN IF NOT EXISTS external_timer_id TEXT;
