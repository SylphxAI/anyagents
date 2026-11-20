-- Persisted external scheduler/delayed-run identifier for event-driven timer cancellation.
ALTER TABLE agent_schedules ADD COLUMN external_timer_id TEXT;
