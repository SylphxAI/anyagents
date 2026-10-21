-- Where a message came from, as the channel that received it saw it: the
-- provider chat, its forum topic, and the provider id of the message itself.
-- A reply is resolved against these; a message stored before this column
-- existed has none, and falls back to the convention its sender carries.
ALTER TABLE messages ADD COLUMN origin TEXT;
