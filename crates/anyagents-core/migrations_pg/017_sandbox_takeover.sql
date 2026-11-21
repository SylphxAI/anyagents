-- Coordinator-owned lease pins must not be included in guest-state sync.
ALTER TABLE public.agent_files DROP CONSTRAINT agent_files_namespace_check;
ALTER TABLE public.agent_files ADD CONSTRAINT agent_files_namespace_check CHECK (namespace IN (
    'config', 'memory', 'memory_archive', 'session_pin', 'sandbox-takeover',
    'sessions', 'skills', 'spiron_cloud', 'acp', 'markers', 'secrets'
));
