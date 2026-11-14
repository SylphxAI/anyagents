-- An agent whose harness is a vendor's remote session (ADR 0010 D6, D7) has
-- no local process: this row is what outlives a server restart. It names the
-- vendor session the agent runs in, the adapter's own handle on it, and what
-- the kernel already accounted for, so the next kernel reconnects instead of
-- starting over. One row per agent; a new vendor session replaces it.
--
-- `totals` is what finished turns used and cost, as the kernel was told at
-- each turn's end; `open_usage` is what the turn still running has used so
-- far. `resume` is where the next attach replays from (the last event cursor
-- applied, a `seq` or an event id) and the recent mailbox ids and host tool
-- calls the session already holds, so a replay is applied once.
--
-- `status`: `creating` while the vendor creates the session, `attached` while
-- a kernel holds (or must take over) its connection, `parked` while the agent
-- sleeps and nothing is connected, `ended` once the vendor session is over.
CREATE TABLE remote_executions (
    agent_id TEXT PRIMARY KEY REFERENCES agents (id),
    adapter TEXT NOT NULL,
    harness TEXT NOT NULL,
    model TEXT,
    vendor_session TEXT,
    adapter_state TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    vendor_status TEXT,
    token_session TEXT,
    budget_cents INTEGER,
    cost_cents INTEGER,
    totals TEXT NOT NULL DEFAULT '{}',
    open_usage TEXT NOT NULL DEFAULT '{}',
    base TEXT NOT NULL DEFAULT '{}',
    resume TEXT NOT NULL DEFAULT '{}',
    process_started_at_ms INTEGER,
    note TEXT,
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
