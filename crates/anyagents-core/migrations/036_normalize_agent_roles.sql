-- Normalize historical missing or blank agent roles to the built-in general role.
UPDATE agents
SET role = 'general'
WHERE role IS NULL OR trim(role) = '';
