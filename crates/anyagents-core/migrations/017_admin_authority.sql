ALTER TABLE agents ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0;
CREATE TABLE admin_audit (
    id INTEGER PRIMARY KEY,
    created_at INTEGER NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT NOT NULL
);
