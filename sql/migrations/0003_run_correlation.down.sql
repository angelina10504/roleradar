-- Reverts 0003. Drops run_id from all three tables, and with it the link
-- between every row and its run. The rows themselves stay.
DROP INDEX public.pipeline_errors_occurred_at_idx;
DROP INDEX public.pipeline_errors_run_id_idx;

ALTER TABLE public.pipeline_errors
    DROP COLUMN error_category,
    DROP COLUMN run_id;

ALTER TABLE public.dead_letter_log DROP COLUMN run_id;
ALTER TABLE public.jobs_extracted  DROP COLUMN run_id;
