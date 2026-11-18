-- 0004 one posting per run
--
-- Within one run, a posting is observed once. Replaying an insert inside the
-- same execution must not write it twice. Across runs nothing changes: the
-- same posting on three runs is still three rows, which is the history
-- decision (a) exists to keep.
--
-- Partial, so rows with run_id NULL are outside it. That matters: the live
-- dead_letter_log holds three identical rows for posting 2091105, all stamped
-- 2026-08-23 15:02:44.726. A full unique index would refuse to build.
--
-- The insert nodes need "Skip on Conflict" switched on, which makes a replayed
-- row a no-op. Without it a replay fails the node instead.

CREATE UNIQUE INDEX jobs_extracted_run_id_source_job_id_key
    ON public.jobs_extracted (run_id, source_job_id)
    WHERE run_id IS NOT NULL;

-- source_job_id is nullable on this table. Postgres treats NULLs as distinct,
-- so two rows that failed for a missing id in one run both land.
CREATE UNIQUE INDEX dead_letter_log_run_id_source_job_id_key
    ON public.dead_letter_log (run_id, source_job_id)
    WHERE run_id IS NOT NULL;
