-- Upgrade an existing metadata store without recreating tables or losing task state.
-- Run during a maintenance window: ALTER TABLE acquires a table lock.
BEGIN;

ALTER TABLE public.doris_task_info
    ALTER COLUMN rdate TYPE TEXT,
    ALTER COLUMN partition_filter TYPE TEXT;

COMMIT;
