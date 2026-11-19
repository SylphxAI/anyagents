-- AnyAgents PostgreSQL 18 schema migration 003: Pure Serverless remote turn ledger.
-- Tracks detached sandbox turns and asynchronous cloud harness turns across
-- coordinator scale-to-zero and replica restarts with constant-time per-turn
-- callback verification and idempotent webhook/tick settlement.

CREATE TABLE IF NOT EXISTS public.pending_remote_turns (
    turn_id             TEXT PRIMARY KEY,
    agent_id            TEXT NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    harness_kind        TEXT NOT NULL,
    provider_kind       TEXT,
    remote_run_id       TEXT,
    callback_token_hash TEXT NOT NULL,
    cursor              TEXT,
    status              TEXT NOT NULL CHECK (status IN (
                          'dispatched', 'running', 'completed', 'failed', 'cancelled')),
    result_text         TEXT,
    error_text          TEXT,
    usage_json          JSONB,
    metadata_json       JSONB,
    created_at_ms       BIGINT NOT NULL,
    updated_at_ms       BIGINT NOT NULL,
    last_event_at_ms    BIGINT NOT NULL,
    next_poll_at_ms     BIGINT
);

CREATE INDEX IF NOT EXISTS idx_pending_remote_turns_agent_status
    ON public.pending_remote_turns(agent_id, status, updated_at_ms DESC);

CREATE INDEX IF NOT EXISTS idx_pending_remote_turns_due_polls
    ON public.pending_remote_turns(next_poll_at_ms)
    WHERE status IN ('dispatched', 'running') AND next_poll_at_ms IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pending_remote_turns_remote_run
    ON public.pending_remote_turns(harness_kind, remote_run_id)
    WHERE remote_run_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pending_remote_turns_stale
    ON public.pending_remote_turns(status, last_event_at_ms)
    WHERE status IN ('dispatched', 'running');
