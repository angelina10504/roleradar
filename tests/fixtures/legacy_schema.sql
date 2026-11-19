-- The warehouse schema as it stood before migrations, copied column for column
-- from a read-only inspection of the live database on 2026-11-11. The tests
-- load this, then migrate it, to prove an existing database survives.
--
-- Unlike 0001_baseline.sql this is not IF NOT EXISTS: it is a fixture, and it
-- should fail if the test database is not empty.

CREATE TABLE public.jobs_extracted (
    source_job_id       text NOT NULL,
    source_url          text NOT NULL,
    raw_title           text,
    raw_company         text,
    ingested_at         timestamp without time zone,
    model               text,
    extraction_version  text,
    normalized_title    text,
    company             text,
    seniority           text,
    tech_stack          jsonb,
    remote_type         text,
    salary_min          numeric,
    salary_max          numeric,
    salary_period       text,
    mentions_n8n_model  boolean,
    mentions_n8n_actual boolean,
    ai_ml_role          boolean,
    id                  bigserial,
    CONSTRAINT jobs_extracted_pkey PRIMARY KEY (id)
);

CREATE INDEX jobs_extracted_source_job_id_ingested_at_idx
    ON public.jobs_extracted (source_job_id, ingested_at DESC);

CREATE TABLE public.dead_letter_log (
    id                  bigserial,
    source_job_id       text,
    source_url          text,
    raw_title           text,
    raw_company         text,
    description         text,
    ingested_at         timestamp with time zone NOT NULL,
    model               text,
    extraction_version  text,
    error_codes         jsonb NOT NULL,
    errors_json         jsonb NOT NULL,
    raw_output          jsonb,
    CONSTRAINT dead_letter_log_pkey PRIMARY KEY (id)
);

CREATE TABLE public.pipeline_errors (
    id             bigserial,
    workflow_name  text,
    execution_id   text,
    failed_node    text,
    error_message  text,
    occurred_at    timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT pipeline_errors_pkey PRIMARY KEY (id)
);

-- As the live view was written: j.* in the inner query.
CREATE VIEW public.jobs_latest AS
SELECT
    source_job_id, source_url, raw_title, raw_company, ingested_at,
    model, extraction_version, normalized_title, company, seniority,
    tech_stack, remote_type, salary_min, salary_max, salary_period,
    mentions_n8n_model, mentions_n8n_actual, ai_ml_role
FROM (
    SELECT
        j.*,
        ROW_NUMBER() OVER (
            PARTITION BY j.source_job_id
            ORDER BY j.ingested_at DESC
        ) AS rn
    FROM public.jobs_extracted j
) ranked
WHERE rn = 1;
