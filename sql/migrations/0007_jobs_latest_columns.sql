-- 0007 jobs_latest gains publication_date and run_id
--
-- CREATE OR REPLACE VIEW may only append columns, so the first eighteen stay
-- in their existing order and the two new ones go on the end. Anything reading
-- the view by name or by the old positions is unaffected.
--
-- The inner query lists its columns instead of using j.*, so the view depends
-- only on the columns it names.

CREATE OR REPLACE VIEW public.jobs_latest AS
SELECT
    source_job_id, source_url, raw_title, raw_company, ingested_at,
    model, extraction_version, normalized_title, company, seniority,
    tech_stack, remote_type, salary_min, salary_max, salary_period,
    mentions_n8n_model, mentions_n8n_actual, ai_ml_role,
    publication_date, run_id
FROM (
    SELECT
        j.source_job_id, j.source_url, j.raw_title, j.raw_company,
        j.ingested_at, j.model, j.extraction_version,
        j.normalized_title, j.company, j.seniority, j.tech_stack,
        j.remote_type, j.salary_min, j.salary_max, j.salary_period,
        j.mentions_n8n_model, j.mentions_n8n_actual, j.ai_ml_role,
        j.publication_date, j.run_id,
        ROW_NUMBER() OVER (
            PARTITION BY j.source_job_id
            ORDER BY j.ingested_at DESC
        ) AS rn
    FROM public.jobs_extracted j
) ranked
WHERE rn = 1;
