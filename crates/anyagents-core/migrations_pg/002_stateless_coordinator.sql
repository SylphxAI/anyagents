-- AnyAgents PostgreSQL 18 schema migration 002: stateless multi-replica Thin Coordinator.
-- Moves per-agent state files, deployment blobs, protocol gateway routes,
-- sandbox volume/lease bindings, distributed coordinator leases, durable
-- obligation journals, and runtime heartbeats into PostgreSQL so `replicas: N`
-- coordinator pods hold zero durable local disk state.

-- 1. Unified per-agent state/memory/config/session/skill/marker/secret blobs
CREATE TABLE IF NOT EXISTS public.agent_files (
    agent_id      TEXT NOT NULL REFERENCES public.agents(id) ON DELETE CASCADE,
    namespace     TEXT NOT NULL CHECK (namespace IN (
                    'config', 'memory', 'memory_archive', 'session_pin',
                    'sessions', 'skills', 'spiron_cloud', 'acp', 'markers', 'secrets')),
    path          TEXT NOT NULL,
    content       BYTEA NOT NULL,
    sha256        TEXT NOT NULL,
    encrypted     BOOLEAN NOT NULL DEFAULT FALSE,
    version       BIGINT NOT NULL DEFAULT 1,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (agent_id, namespace, path)
);
CREATE INDEX IF NOT EXISTS idx_agent_files_ns
    ON public.agent_files(agent_id, namespace);

-- 2. Deployment-wide config, encrypted channel secrets, provider profiles, global skills, small media
CREATE TABLE IF NOT EXISTS public.deployment_blobs (
    namespace     TEXT NOT NULL CHECK (namespace IN (
                    'config', 'secrets', 'profiles', 'skills', 'media')),
    path          TEXT NOT NULL,
    content       BYTEA NOT NULL,
    sha256        TEXT NOT NULL,
    encrypted     BOOLEAN NOT NULL DEFAULT FALSE,
    version       BIGINT NOT NULL DEFAULT 1,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (namespace, path)
);

-- 3. O(1) cross-replica LLM Protocol Gateway route table
CREATE TABLE IF NOT EXISTS public.gateway_routes (
    token_hash    TEXT PRIMARY KEY,
    agent_id      TEXT NOT NULL UNIQUE REFERENCES public.agents(id) ON DELETE CASCADE,
    agent_name    TEXT NOT NULL DEFAULT '',
    harness       TEXT NOT NULL,
    protocol      TEXT NOT NULL,
    model         TEXT NOT NULL,
    base_url      TEXT NOT NULL,
    api_key_enc   BYTEA NOT NULL,
    token_enc     BYTEA NOT NULL,
    updated_at_ms BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gateway_routes_agent_id
    ON public.gateway_routes(agent_id);

-- 4. Durable per-agent persistent volume & ephemeral compute lease bindings
CREATE TABLE IF NOT EXISTS public.agent_sandbox_bindings (
    agent_id             TEXT PRIMARY KEY REFERENCES public.agents(id) ON DELETE CASCADE,
    provider             TEXT NOT NULL,
    image                TEXT,
    template             TEXT,
    remote_workdir       TEXT NOT NULL DEFAULT '/home/user/agent-data/workspace',
    spec_json            TEXT NOT NULL DEFAULT '{}',
    volume_id            TEXT NOT NULL DEFAULT '',
    volume_resource      TEXT NOT NULL DEFAULT '',
    volume_size_gib      BIGINT,
    lease_id             TEXT,
    lease_resource       TEXT,
    data_plane_resource  TEXT,
    guest_endpoint       TEXT,
    transport_url        TEXT,
    transport_token_enc  BYTEA,
    lease_state          TEXT NOT NULL DEFAULT 'detached' CHECK (lease_state IN (
                           'detached', 'provisioning', 'ready', 'releasing', 'expired', 'failed')),
    lease_expires_at_ms  BIGINT,
    idle_since_ms        BIGINT,
    idle_ttl_ms          BIGINT NOT NULL DEFAULT 300000,
    last_heartbeat_ms    BIGINT,
    bundle_manifest_json TEXT NOT NULL DEFAULT '{}',
    generation           BIGINT NOT NULL DEFAULT 1,
    updated_at_ms        BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sandbox_bindings_idle_due
    ON public.agent_sandbox_bindings(lease_state, idle_since_ms)
    WHERE lease_state = 'ready' AND idle_since_ms IS NOT NULL;

-- 5. Distributed coordinator locks & singleton leader leases
CREATE TABLE IF NOT EXISTS public.coordinator_leases (
    lease_key        TEXT PRIMARY KEY,
    owner_replica_id TEXT NOT NULL,
    fencing_token    BIGINT NOT NULL DEFAULT 1,
    acquired_at_ms   BIGINT NOT NULL,
    heartbeat_at_ms  BIGINT NOT NULL,
    expires_at_ms    BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_coordinator_leases_expires
    ON public.coordinator_leases(expires_at_ms);

-- 6. Durable cross-replica outbox/retry obligations
CREATE TABLE IF NOT EXISTS public.durable_obligations (
    kind          TEXT NOT NULL CHECK (kind IN (
                    'outbound_history', 'telegram_relay', 'reap_archived', 'spawn_finish')),
    key           TEXT NOT NULL,
    agent_id      TEXT REFERENCES public.agents(id) ON DELETE CASCADE,
    state         TEXT NOT NULL CHECK (state IN ('pending', 'uncertain', 'done', 'discarded')),
    payload_json  TEXT NOT NULL,
    attempts      INT NOT NULL DEFAULT 0,
    next_at_ms    BIGINT NOT NULL DEFAULT 0,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (kind, key)
);
CREATE INDEX IF NOT EXISTS idx_durable_obligations_due
    ON public.durable_obligations(kind, state, next_at_ms)
    WHERE state = 'pending';

-- 7. Extend agent_runtime with multi-replica ownership, heartbeat, idle & remote-child visibility
ALTER TABLE public.agent_runtime
    ADD COLUMN IF NOT EXISTS owner_replica_id TEXT,
    ADD COLUMN IF NOT EXISTS turn_state TEXT NOT NULL DEFAULT 'idle',
    ADD COLUMN IF NOT EXISTS retry_at_ms BIGINT,
    ADD COLUMN IF NOT EXISTS heartbeat_at_ms BIGINT,
    ADD COLUMN IF NOT EXISTS idle_since_ms BIGINT,
    ADD COLUMN IF NOT EXISTS inflight_children INT NOT NULL DEFAULT 0;
