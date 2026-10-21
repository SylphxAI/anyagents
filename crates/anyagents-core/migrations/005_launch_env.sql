-- An agent's launch environment: the gateway origin and its own gateway token.
-- Never a provider key; the gateway holds that.
ALTER TABLE agents ADD COLUMN launch_env TEXT;
