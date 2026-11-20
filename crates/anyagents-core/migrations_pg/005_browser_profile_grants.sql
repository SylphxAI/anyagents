-- AnyAgents PostgreSQL 18 schema migration 004: Persistent browser profile grants.
CREATE TABLE IF NOT EXISTS public.agent_browser_profile_grants (
    agent_id      TEXT NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    profile_name  TEXT NOT NULL,
    granted_by    TEXT,
    granted_at_ms BIGINT NOT NULL,
    PRIMARY KEY (agent_id, profile_name)
);

CREATE INDEX IF NOT EXISTS idx_agent_browser_profile_grants_profile
    ON public.agent_browser_profile_grants(profile_name);
