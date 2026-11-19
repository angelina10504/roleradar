-- 0003 run correlation
--
-- Ties rows to the run that wrote them. Nullable everywhere: rows written
-- before this migration keep run_id NULL and are never backfilled. Guessing a
-- run for them would turn an inference into a recorded fact. The API keeps
-- grouping those rows by timestamp gap.
--
-- Adding a nullable column with no default does not rewrite the table.

ALTER TABLE public.jobs_extracted
    ADD COLUMN run_id uuid
    CONSTRAINT jobs_extracted_run_id_fkey REFERENCES public.pipeline_runs (run_id);

ALTER TABLE public.dead_letter_log
    ADD COLUMN run_id uuid
    CONSTRAINT dead_letter_log_run_id_fkey REFERENCES public.pipeline_runs (run_id);

-- An infra error can exist with no run: a failure in the trigger node, or in
-- the node that inserts the run row. So run_id is nullable here for a second
-- reason, and error_category lives on this table as well as on pipeline_runs.
ALTER TABLE public.pipeline_errors
    ADD COLUMN run_id uuid
    CONSTRAINT pipeline_errors_run_id_fkey REFERENCES public.pipeline_runs (run_id),
    ADD COLUMN error_category text;

CREATE INDEX pipeline_errors_run_id_idx
    ON public.pipeline_errors (run_id);

CREATE INDEX pipeline_errors_occurred_at_idx
    ON public.pipeline_errors (occurred_at DESC);
