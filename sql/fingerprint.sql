-- fingerprint.sql
--
-- One line per fact about the public schema, sorted, so two databases can be
-- compared with diff. Used to prove a backup restores: fingerprint the source,
-- restore the dump somewhere disposable, fingerprint that, and the two files
-- must be identical. A pg_dump that exited 0 is not that proof.
--
-- Lines are kind|name|value:
--   rows        row count, then an md5 over every row's text, in byte order
--   column      type, nullability, default
--   constraint  its definition
--   index       its definition
--   view        md5 of its definition
--   function    md5 of its definition
--   sequence    last value
--
-- Read-only. The whole file runs inside a READ ONLY transaction that is
-- rolled back. The SET LOCAL lines fix everything that changes how a value is
-- printed, so two servers with different locales or time zones print the same
-- text for the same data. Rows are ordered COLLATE "C" for the same reason:
-- byte order does not depend on the server's collation.
--
-- Run with:
--   psql "$URL" -XqAt -v ON_ERROR_STOP=1 -f sql/fingerprint.sql > out.fingerprint

BEGIN TRANSACTION READ ONLY;
SET LOCAL TimeZone = 'UTC';
SET LOCAL DateStyle = 'ISO, YMD';
SET LOCAL IntervalStyle = 'postgres';
SET LOCAL extra_float_digits = 1;

WITH tables AS (
    SELECT c.relname AS name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
),
fp(kind, name, value) AS (
    SELECT 'rows', t.name,
        (xpath('/row/c/text()', query_to_xml(format(
            'SELECT count(*) || '' '' || coalesce(md5(string_agg(x::text, E''\n'' '
            'ORDER BY x::text COLLATE "C")), ''-'') AS c FROM public.%I x', t.name),
            false, true, '')))[1]::text
    FROM tables t

    UNION ALL
    SELECT 'column', c.table_name || '.' || c.column_name,
           c.data_type || ' ' || c.is_nullable || ' ' || coalesce(c.column_default, '-')
    FROM information_schema.columns c
    WHERE c.table_schema = 'public'

    UNION ALL
    SELECT 'constraint', cl.relname || '.' || con.conname, pg_get_constraintdef(con.oid)
    FROM pg_constraint con
    JOIN pg_class cl ON cl.oid = con.conrelid
    JOIN pg_namespace n ON n.oid = cl.relnamespace
    WHERE n.nspname = 'public'

    UNION ALL
    SELECT 'index', i.indexname, i.indexdef
    FROM pg_indexes i
    WHERE i.schemaname = 'public'

    UNION ALL
    SELECT 'view', v.viewname,
           md5(pg_get_viewdef(format('public.%I', v.viewname)::regclass, true))
    FROM pg_views v
    WHERE v.schemaname = 'public'

    UNION ALL
    SELECT 'function', p.oid::regprocedure::text, md5(pg_get_functiondef(p.oid))
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = 'public' AND p.prokind = 'f'

    UNION ALL
    SELECT 'sequence', s.sequencename, coalesce(s.last_value::text, 'unused')
    FROM pg_sequences s
    WHERE s.schemaname = 'public'
)
SELECT kind || '|' || name || '|' || value
FROM fp
ORDER BY kind COLLATE "C", name COLLATE "C";

ROLLBACK;
