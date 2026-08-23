-- dead_letter_log
--
-- Rows that failed validation. Written by the false branch of the IF node.
-- Separate from infra failures (timeouts, non-2xx, API outages), which are a
-- different event and are meant to be caught by an Error Trigger workflow.
--
-- Matches the live database.

CREATE TABLE IF NOT EXISTS dead_letter_log (
    -- Synthetic key. Unlike jobs_extracted, this table has no natural unique
    -- column: the same posting can fail on every run, and each failure is its
    -- own event worth keeping.
    id                  bigserial,

    -- Provenance. Nullable, because a row can fail validation precisely
    -- because these are missing.
    source_job_id       text,
    source_url          text,
    raw_title           text,
    raw_company         text,

    -- The full description, kept here and nowhere else, so a failure can be
    -- read against the text it came from.
    --
    -- NOTE: the validator emits this field as `description_clean`. This column
    -- is `description`. The names do not match and the column is currently
    -- NULL in every row. See README.md, "Known limitations".
    description         text,

    ingested_at         timestamp with time zone NOT NULL,
    model               text,
    extraction_version  text,

    -- error_codes is the queryable dimension: a jsonb array of the distinct
    -- codes on this row. Counting one failure mode is a containment test, not
    -- a substring search through a message blob.
    error_codes         jsonb NOT NULL,
    -- errors_json is the full [{field, code, message}] payload.
    errors_json         jsonb NOT NULL,
    -- raw_output is the whole Anthropic message, so the failure can be
    -- reproduced without re-calling the API.
    raw_output          jsonb,

    CONSTRAINT dead_letter_log_pkey PRIMARY KEY (id)
);

-- Counting one failure mode, for example the salary plausibility rule:
--
--   SELECT count(*) FROM dead_letter_log
--   WHERE error_codes @> '["SALARY_SPAN_IMPLAUSIBLE"]';
--
-- Model error rate, per model version:
--
--   SELECT model, extraction_version,
--          count(*) FILTER (WHERE error_codes @> '["N8N_MISMATCH"]') AS mismatches,
--          count(*) AS failures
--   FROM dead_letter_log
--   GROUP BY model, extraction_version;
