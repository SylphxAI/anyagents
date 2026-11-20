-- NULL identities are unclaimed channels and remain separate. Prefer the newest
-- claimed row; use the id to break ties. Preserve all exact agent attachments.
CREATE TEMP TABLE channel_identity_duplicates AS
SELECT id, keep_id FROM (
    SELECT id, FIRST_VALUE(id) OVER (
        PARTITION BY kind, identity ORDER BY created_at_ms DESC, id DESC
    ) AS keep_id
    FROM channels WHERE identity IS NOT NULL
) AS ranked WHERE id <> keep_id;

INSERT INTO channel_agents (channel_id, agent_id)
SELECT d.keep_id, a.agent_id
FROM channel_identity_duplicates AS d JOIN channel_agents AS a ON a.channel_id = d.id
WHERE true ON CONFLICT (channel_id, agent_id) DO NOTHING;

DELETE FROM channel_agents WHERE channel_id IN (SELECT id FROM channel_identity_duplicates);
DELETE FROM channels WHERE id IN (SELECT id FROM channel_identity_duplicates);
DROP TABLE channel_identity_duplicates;

CREATE UNIQUE INDEX uq_channels_kind_identity ON channels (kind, identity);
