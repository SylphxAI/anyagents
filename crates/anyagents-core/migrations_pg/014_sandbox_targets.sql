CREATE TABLE sandbox_credentials (
    name TEXT PRIMARY KEY,
    ciphertext TEXT NOT NULL
);

-- Deliberately no foreign key to the credential: deletion disables a binding,
-- rather than turning it into an implicit default.
CREATE TABLE sandbox_profiles (
    name TEXT PRIMARY KEY,
    credential TEXT NOT NULL
);

CREATE TABLE agent_sandbox_targets (
    agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    profile TEXT,
    credential TEXT
);

CREATE TABLE sandbox_group_profiles (
    group_name TEXT PRIMARY KEY REFERENCES groups(name) ON DELETE CASCADE,
    profile TEXT NOT NULL
);
