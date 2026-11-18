-- Reverts 0004. No data is touched.
DROP INDEX public.dead_letter_log_run_id_source_job_id_key;
DROP INDEX public.jobs_extracted_run_id_source_job_id_key;
