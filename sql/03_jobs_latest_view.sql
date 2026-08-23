-- jobs_latest
--
-- Current state, derived on read. jobs_extracted is an append-only ingestion
-- log, so the newest row per posting is the current one. A MERGE on write
-- would overwrite history; first-seen date and re-listing history are needed
-- for the postings-over-time chart and are not recoverable once gone.
--
-- ROW_NUMBER() in a subquery rather than DISTINCT ON, so the ranking is
-- explicit and the same shape as the window this was designed against.
--
-- NOTE: this view is currently a no-op. jobs_extracted has a primary key on
-- source_job_id, so no posting can have more than one row and ROW_NUMBER() is
-- always 1. The view only starts doing work once that key is dropped. See
-- README.md, "Known limitations".

CREATE OR REPLACE VIEW jobs_latest AS
SELECT
    source_job_id,
    source_url,
    raw_title,
    raw_company,
    ingested_at,
    model,
    extraction_version,
    normalized_title,
    company,
    seniority,
    tech_stack,
    remote_type,
    salary_min,
    salary_max,
    salary_period,
    mentions_n8n_model,
    mentions_n8n_actual,
    ai_ml_role
FROM (
    SELECT
        j.*,
        ROW_NUMBER() OVER (
            PARTITION BY j.source_job_id
            ORDER BY j.ingested_at DESC
        ) AS rn
    FROM jobs_extracted j
) ranked
WHERE rn = 1;
