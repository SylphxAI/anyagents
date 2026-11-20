-- AnyAgents PostgreSQL 18 schema migration 004: Named encrypted deployment secrets
-- and per-agent capability secret grants.

CREATE TABLE IF NOT EXISTS public.deployment_secrets (
    name                    TEXT PRIMARY KEY,
    env_entries_ciphertext  TEXT NOT NULL,
    key_names_json          TEXT NOT NULL,
    updated_at_ms           BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS public.agent_secret_grants (
    agent_id      TEXT NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    secret_name   TEXT NOT NULL REFERENCES public.deployment_secrets(name) ON DELETE CASCADE,
    granted_by    TEXT,
    granted_at_ms BIGINT NOT NULL,
    PRIMARY KEY (agent_id, secret_name)
);

CREATE INDEX IF NOT EXISTS idx_agent_secret_grants_secret_name
    ON public.agent_secret_grants(secret_name, agent_id);
