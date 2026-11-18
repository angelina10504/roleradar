-- preflight.sql
--
-- Read-only description of a database, to be run before migrating it. It
-- answers: is this the database I think it is, what does it already have, and
-- is there anything in it the migrations would collide with.
--
-- It changes nothing. The whole file runs inside a READ ONLY transaction that
-- is rolled back, so a statement that tried to write would be refused by
-- Postgres itself. It works on an empty database, on the pre-migration
-- schema and on a migrated one, because it never names a table directly: a
-- table that might not exist is counted through query_to_xml, guarded by
-- to_regclass.
--
-- Run it with psql and read every line:
--   psql "$TARGET_URL" -X -f sql/preflight.sql
--
-- STOP if: the server, database or user is not the one you expect; a
-- "collision" line says true; jobs_extracted_pk_is_id says false.

BEGIN TRANSACTION READ ONLY;

WITH facts(check_name, value) AS (
    SELECT 'server_version', current_setting('server_version')
    UNION ALL SELECT 'database', current_database()::text
    UNION ALL SELECT 'user', current_user::text
    UNION ALL SELECT 'session_timezone', current_setting('TimeZone')
    UNION ALL SELECT 'transaction_read_only', current_setting('transaction_read_only')

    -- What the migration ledger says, if there is one.
    UNION ALL SELECT 'ledger_present', (to_regclass('public.schema_migrations') IS NOT NULL)::text
    UNION ALL SELECT 'ledger_latest_version',
        CASE WHEN to_regclass('public.schema_migrations') IS NULL THEN '(no ledger)'
             ELSE COALESCE((xpath('/row/v/text()', query_to_xml(
                 'SELECT max(version) AS v FROM public.schema_migrations', false, true, '')))[1]::text,
                 '(empty)') END
    UNION ALL SELECT 'ledger_applied_count',
        CASE WHEN to_regclass('public.schema_migrations') IS NULL THEN '(no ledger)'
             ELSE (xpath('/row/c/text()', query_to_xml(
                 'SELECT count(*) AS c FROM public.schema_migrations', false, true, '')))[1]::text END

    -- Row counts. Write these down; they must be the same after migrating.
    UNION ALL
    SELECT 'rows.' || t.name,
        CASE WHEN to_regclass('public.' || t.name) IS NULL THEN '(absent)'
             ELSE (xpath('/row/c/text()', query_to_xml(
                 format('SELECT count(*) AS c FROM public.%I', t.name), false, true, '')))[1]::text END
    FROM (VALUES ('jobs_extracted'), ('dead_letter_log'), ('pipeline_errors'),
                 ('pipeline_runs')) AS t(name)

    -- The baseline refuses a jobs_extracted keyed on anything but id.
    UNION ALL SELECT 'jobs_extracted_pk_is_id',
        CASE WHEN to_regclass('public.jobs_extracted') IS NULL THEN '(absent)'
             ELSE EXISTS (
                SELECT 1 FROM pg_constraint c
                JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
                WHERE c.conrelid = to_regclass('public.jobs_extracted')
                  AND c.contype = 'p' AND array_length(c.conkey, 1) = 1 AND a.attname = 'id'
             )::text END

    -- The deferred migration depends on these two staying as they are.
    UNION ALL
    SELECT 'type.' || c.table_name || '.ingested_at', c.data_type
    FROM information_schema.columns c
    WHERE c.table_schema = 'public' AND c.column_name = 'ingested_at'
      AND c.table_name IN ('jobs_extracted', 'dead_letter_log')

    -- Columns the migrations add. 'present' before migrating means someone
    -- added it by hand, and the migration that adds it will fail.
    UNION ALL
    SELECT 'column.' || w.t || '.' || w.c,
        CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.columns ic
            WHERE ic.table_schema = 'public' AND ic.table_name = w.t AND ic.column_name = w.c
        ) THEN 'present' ELSE 'absent' END
    FROM (VALUES
        ('jobs_extracted', 'run_id'), ('jobs_extracted', 'publication_date'),
        ('dead_letter_log', 'run_id'), ('dead_letter_log', 'publication_date'),
        ('dead_letter_log', 'description_clean'),
        ('pipeline_errors', 'run_id'), ('pipeline_errors', 'error_category'),
        ('pipeline_errors', 'workflow_id'), ('pipeline_runs', 'n8n_workflow_id')
    ) AS w(t, c)

    UNION ALL SELECT 'function.complete_pipeline_run',
        CASE WHEN EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                          WHERE n.nspname = 'public' AND p.proname = 'complete_pipeline_run')
             THEN 'present' ELSE 'absent' END
    UNION ALL SELECT 'function.record_pipeline_error',
        CASE WHEN EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                          WHERE n.nspname = 'public' AND p.proname = 'record_pipeline_error')
             THEN 'present' ELSE 'absent' END

    -- True means an object the migrations create is already here although the
    -- ledger does not record the migration that creates it.
    UNION ALL SELECT 'collision.pipeline_runs_without_ledger',
        (to_regclass('public.pipeline_runs') IS NOT NULL
         AND to_regclass('public.schema_migrations') IS NULL)::text
    UNION ALL SELECT 'collision.run_id_without_ledger',
        (EXISTS (SELECT 1 FROM information_schema.columns ic
                 WHERE ic.table_schema = 'public' AND ic.column_name = 'run_id'
                   AND ic.table_name IN ('jobs_extracted', 'dead_letter_log'))
         AND to_regclass('public.schema_migrations') IS NULL)::text

    -- Anything else holding a lock would make the migration wait.
    UNION ALL SELECT 'other_sessions_on_this_database',
        (SELECT count(*) FROM pg_stat_activity
          WHERE datname = current_database() AND pid <> pg_backend_pid())::text
)
SELECT check_name, value FROM facts ORDER BY check_name;

ROLLBACK;
