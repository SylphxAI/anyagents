-- A named group of agents. A group has its own membership, so a message to
-- `group:<name>` reaches exactly its members and nobody else. Membership is
-- never inferred: an agent is in a group only because a row here says so.
CREATE TABLE groups (
    name TEXT PRIMARY KEY,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE group_agents (
    group_name TEXT NOT NULL REFERENCES groups (name),
    agent_id TEXT NOT NULL REFERENCES agents (id),
    PRIMARY KEY (group_name, agent_id)
);
