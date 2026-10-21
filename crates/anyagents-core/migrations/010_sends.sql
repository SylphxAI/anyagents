-- One receipt per named send. A caller that retries after losing the answer
-- names the same send and gets this receipt back instead of a second delivery.
CREATE TABLE sends (
    id TEXT PRIMARY KEY,
    receipt TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
