-- Deleting a message cascades to its identities and aliases (ON DELETE
-- CASCADE), and SQLite finds the child rows by the child key. These keys had
-- no index, so every deleted message scanned message_identities (twice),
-- message_origin_aliases and message_source_aliases: about 33 ms a message
-- on the fleet's store, minutes to an hour for one agent's mailbox, all with
-- the Store writer held.
CREATE INDEX IF NOT EXISTS message_identities_identity
    ON message_identities (identity_id);
CREATE INDEX IF NOT EXISTS message_identities_target
    ON message_identities (target_id);
CREATE INDEX IF NOT EXISTS message_origin_aliases_identity
    ON message_origin_aliases (identity_id);
CREATE INDEX IF NOT EXISTS message_source_aliases_identity
    ON message_source_aliases (identity_id);
