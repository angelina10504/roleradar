-- Reverts 0007. A view cannot lose columns in place, so it is dropped and
-- recreated in its baseline shape. No data is touched.
DROP VIEW public.jobs_latest;

CREATE VIEW public.jobs_latest AS
SELECT
    source_job_id, source_url, raw_title, raw_company, ingested_at,
    model, extraction_version, normalized_title, company, seniority,
    tech_stack, remote_type, salary_min, salary_max, salary_period,
    mentions_n8n_model, mentions_n8n_actual, ai_ml_role
FROM (
    SELECT
        j.source_job_id, j.source_url, j.raw_title, j.raw_company,
        j.ingested_at, j.model, j.extraction_version,
        j.normalized_title, j.company, j.seniority, j.tech_stack,
        j.remote_type, j.salary_min, j.salary_max, j.salary_period,
        j.mentions_n8n_model, j.mentions_n8n_actual, j.ai_ml_role,
        ROW_NUMBER() OVER (
            PARTITION BY j.source_job_id
            ORDER BY j.ingested_at DESC
        ) AS rn
    FROM public.jobs_extracted j
) ranked
WHERE rn = 1;
