DROP TRIGGER IF EXISTS agents_clear_terminal_archive_verdict;

-- A disposition authorizes cleanup only for the archive it finalized.
CREATE TRIGGER agents_clear_terminal_archive_verdict
AFTER UPDATE OF state ON agents
WHEN NEW.state <> 'archived' AND OLD.terminal_archive_verdict IS NOT NULL
BEGIN
    UPDATE agents SET terminal_archive_verdict = NULL WHERE id = NEW.id;
END;
