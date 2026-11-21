-- A viewer link can be recognized after its live tunnel or lease pin is gone.
-- No foreign key: a link to a deleted agent should still explain why it cannot reopen.
CREATE TABLE takeover_links (
    session_id TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    expires_ms BIGINT,
    ended_ms BIGINT,
    retain_until_ms BIGINT NOT NULL,
    PRIMARY KEY (session_id, token_hash)
);
CREATE INDEX takeover_links_retention ON takeover_links (retain_until_ms);
