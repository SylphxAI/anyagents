-- Keep the first sampled agent id even if the agent is later deleted.
CREATE TABLE public.runtime_issues (
    id TEXT NOT NULL PRIMARY KEY,
    component TEXT NOT NULL CHECK (component IN ('coordinator', 'sandbox', 'harness', 'mcp_tool')),
    error_fingerprint TEXT NOT NULL,
    summary TEXT NOT NULL,
    exact_error TEXT NOT NULL,
    repro_steps TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('agent_report', 'auto_capture')),
    status TEXT NOT NULL CHECK (status IN ('open', 'resolved')),
    sample_agent_id TEXT,
    occurrence_count BIGINT NOT NULL CHECK (occurrence_count > 0),
    first_seen_at BIGINT NOT NULL,
    last_seen_at BIGINT NOT NULL CHECK (last_seen_at >= first_seen_at),
    UNIQUE (component, error_fingerprint)
);

CREATE INDEX runtime_issues_latest ON public.runtime_issues (last_seen_at DESC, id DESC);
