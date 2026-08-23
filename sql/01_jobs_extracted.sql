-- jobs_extracted
--
-- Append-only ingestion log. Rows that passed validation, written by the true
-- branch of the IF node via n8n's Postgres node. Every run inserts. Nothing is
-- updated or deleted. Current state is derived on read by jobs_latest, in
-- sql/03_jobs_latest_view.sql.
--
-- Matches the live database. Verify with:
--   docker exec roleradar-pg pg_dump -U roleradar -d roleradar --schema-only

CREATE TABLE IF NOT EXISTS jobs_extracted (
    -- Provenance. source_job_id is text because the validator coerces
    -- Remotive's numeric id to a string, so the column type does not depend on
    -- what the API sends. It is NOT NULL but deliberately NOT unique: one
    -- posting seen on three runs is three rows, which is what makes re-listing
    -- history and first-seen date recoverable.
    source_job_id       text NOT NULL,
    source_url          text NOT NULL,
    raw_title           text,
    raw_company         text,
    ingested_at         timestamp without time zone,
    model               text,
    extraction_version  text,

    -- Extracted fields.
    normalized_title    text,
    company             text,
    seniority           text,
    -- jsonb, not text[]. n8n's Postgres node maps a JS array to json, not to a
    -- native array type. See README.md, "Known limitations".
    tech_stack          jsonb,
    remote_type         text,
    salary_min          numeric,
    salary_max          numeric,
    salary_period       text,

    -- Both sides of the n8n cross-check. Equal on this table by construction:
    -- disagreements route to dead_letter_log. Stored separately so model error
    -- rate stays a query rather than a number computed once at ingestion.
    mentions_n8n_model  boolean,
    mentions_n8n_actual boolean,

    ai_ml_role          boolean,

    -- Surrogate key. Last in the column order because it was added by ALTER
    -- TABLE after the fact, replacing an earlier primary key on source_job_id
    -- that made the table upsert-shaped and silently rejected a second sighting
    -- of the same posting. Kept in this position so the file matches the live
    -- column order.
    id                  bigserial,

    CONSTRAINT jobs_extracted_pkey PRIMARY KEY (id)
);

-- Supports jobs_latest. The view ranks rows within each source_job_id by
-- ingested_at descending, so this is the exact key order the window needs, and
-- it replaces the scan-and-sort the old primary key made unnecessary.
CREATE INDEX IF NOT EXISTS jobs_extracted_source_job_id_ingested_at_idx
    ON jobs_extracted (source_job_id, ingested_at DESC);

-- The full job description is deliberately not here. Descriptions are long HTML
-- and would dominate storage for no query benefit. They go to dead_letter_log,
-- where they are needed to diagnose the failure.
