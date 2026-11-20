-- AnyAgents PostgreSQL 18 schema migration 007: Enforce at most one active
-- (`dispatched` / `running`) remote turn per agent via an atomic partial UNIQUE index.

WITH ranked AS (
    SELECT turn_id,
           ROW_NUMBER() OVER (
               PARTITION BY agent_id
               ORDER BY created_at_ms DESC, turn_id DESC
           ) AS rn
    FROM public.pending_remote_turns
    WHERE status IN ('dispatched', 'running')
)
UPDATE public.pending_remote_turns t
SET status = 'cancelled',
    error_text = COALESCE(t.error_text, 'superseded duplicate active turn'),
    updated_at_ms = GREATEST(t.updated_at_ms, t.last_event_at_ms),
    next_poll_at_ms = NULL
FROM ranked r
WHERE t.turn_id = r.turn_id
  AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_pending_remote_turns_active_agent
    ON public.pending_remote_turns (agent_id)
    WHERE status IN ('dispatched', 'running');
