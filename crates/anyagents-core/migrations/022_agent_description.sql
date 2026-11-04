-- Separate public capability/charter description (shared in team & group rosters)
-- from private operating instructions (injected only into the agent's own system prompt).
ALTER TABLE agents ADD COLUMN description TEXT;

-- Backfill concise public `description` from the first line of `instructions` (up to 300 chars)
-- so existing agents immediately expose a clean capability summary without leaking long private prompts.
UPDATE agents
SET description = CASE
    WHEN instr(trim(instructions), char(10)) > 0
        THEN substr(trim(instructions), 1, min(instr(trim(instructions), char(10)) - 1, 300))
    ELSE substr(trim(instructions), 1, 300)
END
WHERE description IS NULL
  AND instructions IS NOT NULL
  AND length(trim(instructions)) > 0;
