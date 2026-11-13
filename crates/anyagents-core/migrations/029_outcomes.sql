-- Outcomes of agent work and the skill reads beside them (ADR 0008).
--
-- `outcomes` is append-only. Each row is one fact from a source the measured
-- agent does not control (a GitHub pull request's state, an ancestor's or the
-- operator's verdict), recorded for the agent that owns it. `dedupe_key` makes
-- a repeated observation of one fact a single row. Which agent a shared fact
-- counts for, and which facts a verdict supersedes, is decided when a report
-- is read, so that policy can change without rewriting rows. `harness`,
-- `model`, `role` and `profile` are what the agent ran with when the fact was
-- recorded. The store validates `kind` and `source`; there is no CHECK, so a
-- new kind needs no table rebuild.
CREATE TABLE IF NOT EXISTS outcomes (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    source TEXT NOT NULL,
    subject TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    actor TEXT NOT NULL,
    harness TEXT NOT NULL,
    model TEXT,
    role TEXT,
    profile TEXT,
    note TEXT,
    claimed_at_ms INTEGER NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    UNIQUE (agent_id, dedupe_key)
);
CREATE INDEX IF NOT EXISTS idx_outcomes_observed ON outcomes(observed_at_ms);

-- One row per skill file an agent read through the `skills` tool, with the
-- SHA-256 of the bytes it read: the version of the skill it saw.
CREATE TABLE IF NOT EXISTS skill_reads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    skill TEXT NOT NULL,
    file TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    read_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_skill_reads_agent ON skill_reads(agent_id, read_at_ms);
