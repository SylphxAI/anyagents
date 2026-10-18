ALTER TABLE channels ADD COLUMN config TEXT NOT NULL DEFAULT '{}';
ALTER TABLE channels ADD COLUMN credential_ref TEXT;
