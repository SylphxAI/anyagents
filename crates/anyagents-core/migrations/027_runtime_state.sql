-- Explicit runtime state (ADR 0006), credential bindings (ADR 0004) and
-- harness version leases (ADR 0005). Schema only: the runtime backfill derives
-- rows from existing state through the store API, never in this migration.

CREATE TABLE IF NOT EXISTS agent_runtime (
    agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN (
        'released', 'starting', 'live', 'draining', 'backoff',
        'quota_blocked', 'paused', 'faulted', 'archived')),
    generation INTEGER NOT NULL DEFAULT 0,
    attempt INTEGER NOT NULL DEFAULT 0,
    until_ms INTEGER,
    fault_kind TEXT,
    fault_detail TEXT,
    vendor_session_id TEXT,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS turns (
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    turn_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open', 'interrupted', 'recovery_offered', 'closed')),
    first_seq INTEGER NOT NULL,
    last_seq INTEGER NOT NULL,
    opened_at_ms INTEGER NOT NULL,
    closed_at_ms INTEGER,
    outcome TEXT CHECK (outcome IS NULL OR outcome IN ('completed', 'cancelled', 'failed', 'recovered')),
    recovery_count INTEGER NOT NULL DEFAULT 0,
    recovers TEXT,
    PRIMARY KEY (agent_id, turn_id),
    CHECK (last_seq >= first_seq),
    CHECK ((state = 'closed') = (outcome IS NOT NULL))
);
-- At most one unfinished turn per agent.
CREATE UNIQUE INDEX IF NOT EXISTS idx_turns_one_unfinished
    ON turns(agent_id) WHERE state != 'closed';

CREATE TABLE IF NOT EXISTS steer_holds (
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    message_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    turn_id TEXT,
    state TEXT NOT NULL CHECK (state IN (
        'held', 'acked', 'unconsumed', 'consumed', 'reconciling', 'archived')),
    redelivered INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (agent_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_steer_holds_generation ON steer_holds(agent_id, generation, state);

CREATE TABLE IF NOT EXISTS session_leases (
    agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    vendor_session_id TEXT,
    generation INTEGER NOT NULL,
    holder_pid INTEGER NOT NULL,
    holder_start_time INTEGER NOT NULL,
    pgid INTEGER NOT NULL,
    acquired_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS quota_blocks (
    agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    until_ms INTEGER NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('known', 'estimated')),
    attempt INTEGER NOT NULL DEFAULT 0,
    observed_at_ms INTEGER NOT NULL
);

-- Journal of lifecycle commands. No foreign key: a spawn op exists before its
-- agent row and a delete op outlives it.
CREATE TABLE IF NOT EXISTS lifecycle_ops (
    op_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    agent_name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('spawn', 'pause', 'resume', 'reset', 'archive', 'delete')),
    phase TEXT NOT NULL CHECK (phase IN (
        'intent', 'quiesce', 'stop', 'commit', 'effects', 'done', 'rolled_back')),
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence TEXT NOT NULL DEFAULT '[]',
    error TEXT,
    started_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER
);
-- One unfinished op per agent, and an unfinished spawn reserves its name.
CREATE UNIQUE INDEX IF NOT EXISTS idx_lifecycle_ops_one_active
    ON lifecycle_ops(agent_id) WHERE phase NOT IN ('done', 'rolled_back');
CREATE UNIQUE INDEX IF NOT EXISTS idx_lifecycle_ops_spawn_name
    ON lifecycle_ops(agent_name) WHERE kind = 'spawn' AND phase NOT IN ('done', 'rolled_back');

ALTER TABLE agent_schedules ADD COLUMN pending_since_ms INTEGER;
ALTER TABLE agent_schedules ADD COLUMN pending_count INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS credential_bindings (
    agent_id TEXT NOT NULL,
    slot TEXT NOT NULL,
    target TEXT NOT NULL,
    set_by TEXT NOT NULL,
    set_at_ms INTEGER NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (agent_id, slot)
);

CREATE TABLE IF NOT EXISTS credential_binding_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL,
    slot TEXT NOT NULL,
    before_target TEXT,
    after_target TEXT,
    actor TEXT NOT NULL,
    reason TEXT NOT NULL,
    at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_credential_binding_audit_agent
    ON credential_binding_audit(agent_id, id);

-- Launch-time pick of a pool member; never a binding.
CREATE TABLE IF NOT EXISTS credential_observations (
    agent_id TEXT NOT NULL,
    slot TEXT NOT NULL,
    record TEXT NOT NULL,
    generation INTEGER NOT NULL,
    reason TEXT NOT NULL,
    at_ms INTEGER NOT NULL,
    PRIMARY KEY (agent_id, slot)
);

-- One managed-harness version lease per (agent, harness), held by one process.
CREATE TABLE IF NOT EXISTS harness_leases (
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    harness TEXT NOT NULL,
    version TEXT NOT NULL,
    holder_pid INTEGER NOT NULL,
    holder_start_time INTEGER NOT NULL,
    acquired_at_ms INTEGER NOT NULL,
    PRIMARY KEY (agent_id, harness)
);
CREATE INDEX IF NOT EXISTS idx_harness_leases_version ON harness_leases(harness, version);
