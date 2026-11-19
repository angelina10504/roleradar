-- 0001 baseline
--
-- The schema as it stood before migrations existed: three tables and a view,
-- created by hand in psql. This file replaces sql/01, 02 and 03.
--
-- On a database that already has these objects it changes nothing. Every
-- statement is IF NOT EXISTS, and the view is only created when absent. It then
-- checks that what is there has the shape the later migrations assume, and
-- fails loudly if not. Without that check, an old copy of the schema would be
-- recorded as "baseline applied" and break three migrations later.
--
-- Every name is schema-qualified. See README, "Pooled connections do not carry
-- a search_path".

-- Append-only ingestion log. source_job_id is NOT NULL but deliberately not
-- unique: one posting seen on three runs is three rows. id is last because it
-- was added by ALTER TABLE after the fact, and this matches the live order.
CREATE TABLE IF NOT EXISTS public.jobs_extracted (
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

-- The key order the jobs_latest window needs.
CREATE INDEX IF NOT EXISTS jobs_extracted_source_job_id_ingested_at_idx
    ON public.jobs_extracted (source_job_id, ingested_at DESC);

-- Rows that failed validation. Provenance is nullable because a row can fail
-- precisely because it is missing.
CREATE TABLE IF NOT EXISTS public.dead_letter_log (
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

-- Infra failures, written by the n8n Error Trigger workflow. Existed in the
-- warehouse before this file did, with no DDL in the repo. These columns are
-- copied from the live table.
CREATE TABLE IF NOT EXISTS public.pipeline_errors (
    id             bigserial,
    workflow_name  text,
    execution_id   text,
    failed_node    text,
    error_message  text,
    occurred_at    timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT pipeline_errors_pkey PRIMARY KEY (id)
);

-- Current state, derived on read: the newest row per posting. Created only
-- when absent, so an existing view is never replaced by the baseline.
DO $$
BEGIN
    IF to_regclass('public.jobs_latest') IS NULL THEN
        EXECUTE $view$
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
            WHERE rn = 1
        $view$;
    END IF;
END
$$;

-- Shape check. Runs whether or not anything above created anything.
DO $$
DECLARE
    missing text;
BEGIN
    SELECT string_agg(e.t || '.' || e.c, ', ' ORDER BY e.t, e.c)
    INTO missing
    FROM (VALUES
        ('jobs_extracted', 'id'),
        ('jobs_extracted', 'source_job_id'),
        ('jobs_extracted', 'source_url'),
        ('jobs_extracted', 'ingested_at'),
        ('jobs_extracted', 'model'),
        ('jobs_extracted', 'extraction_version'),
        ('jobs_extracted', 'mentions_n8n_model'),
        ('jobs_extracted', 'mentions_n8n_actual'),
        ('dead_letter_log', 'id'),
        ('dead_letter_log', 'source_job_id'),
        ('dead_letter_log', 'description'),
        ('dead_letter_log', 'ingested_at'),
        ('dead_letter_log', 'error_codes'),
        ('dead_letter_log', 'errors_json'),
        ('dead_letter_log', 'raw_output'),
        ('pipeline_errors', 'id'),
        ('pipeline_errors', 'workflow_name'),
        ('pipeline_errors', 'execution_id'),
        ('pipeline_errors', 'failed_node'),
        ('pipeline_errors', 'error_message'),
        ('pipeline_errors', 'occurred_at')
    ) AS e(t, c)
    WHERE NOT EXISTS (
        SELECT 1
        FROM information_schema.columns ic
        WHERE ic.table_schema = 'public'
          AND ic.table_name = e.t
          AND ic.column_name = e.c
    );

    IF missing IS NOT NULL THEN
        RAISE EXCEPTION
            'baseline mismatch: existing tables are missing %. This database predates the schema the migrations assume. Fix it by hand before migrating.',
            missing;
    END IF;

    -- The upsert-shaped bug: a primary key on source_job_id rejects the second
    -- sighting of a posting. The key has to be on id alone.
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_attribute a
          ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
        WHERE c.conrelid = 'public.jobs_extracted'::regclass
          AND c.contype = 'p'
          AND array_length(c.conkey, 1) = 1
          AND a.attname = 'id'
    ) THEN
        RAISE EXCEPTION
            'baseline mismatch: jobs_extracted primary key is not (id). A key on source_job_id makes the table upsert-shaped. Fix it by hand before migrating.';
    END IF;
END
$$;
