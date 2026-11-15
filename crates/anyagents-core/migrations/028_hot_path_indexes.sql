-- Maintenance selects schedules by kind and state (restoring skipped `at`
-- ticks and fired PR watches). Without this index every pass scanned every
-- schedule ever created, most of them long fired or cancelled.
CREATE INDEX IF NOT EXISTS idx_agent_schedules_kind_state
    ON agent_schedules(kind, state, next_fire_at_ms);

-- The active-schedule listing reads only active rows, already in its order.
CREATE INDEX IF NOT EXISTS idx_agent_schedules_active
    ON agent_schedules(created_at_ms, id) WHERE state = 'active';

-- A parent's live children without reading its archived ones.
CREATE INDEX IF NOT EXISTS idx_agents_live_parent
    ON agents(parent_id, created_at_ms, name) WHERE state != 'archived';
