ALTER TABLE public.agents ADD COLUMN IF NOT EXISTS terminal_archive_verdict TEXT;

CREATE OR REPLACE FUNCTION public.clear_terminal_archive_verdict()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state <> 'archived' THEN
        NEW.terminal_archive_verdict := NULL;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS agents_clear_terminal_archive_verdict ON public.agents;
CREATE TRIGGER agents_clear_terminal_archive_verdict
BEFORE UPDATE OF state ON public.agents
FOR EACH ROW EXECUTE FUNCTION public.clear_terminal_archive_verdict();
