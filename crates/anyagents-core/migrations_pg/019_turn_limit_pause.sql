-- The SQLite-backed Coordinator uses a file ledger, with the same Rust status validation.
ALTER TABLE public.pending_remote_turns
    DROP CONSTRAINT pending_remote_turns_status_check,
    ADD CONSTRAINT pending_remote_turns_status_check
        CHECK (status IN ('dispatched', 'running', 'completed', 'failed', 'cancelled', 'turn_limit_paused'));
