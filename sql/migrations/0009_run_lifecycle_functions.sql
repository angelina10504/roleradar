-- 0009 run lifecycle functions
--
-- Two functions, one for each way a run ends. They exist so the rules live in
-- the repository, where they are tested, and the n8n canvas only calls them.
-- A rule typed into a canvas expression has no test.
--
-- Every object is schema-qualified. Neon's pooled connections do not carry a
-- search_path.

-- ---------------------------------------------------------------------------
-- complete_pipeline_run
-- ---------------------------------------------------------------------------
-- Closes a run as 'completed', but only after checking what the caller says
-- happened against what is in the tables.
--
-- The caller passes five things:
--   p_run_id        the run
--   p_fetched       postings the source returned
--   p_submitted     items sent to the model, after the Limit node
--   p_accepted_ids  source_job_id of every record the validator accepted
--   p_rejected_ids  source_job_id of every record it rejected (null allowed)
--
-- The function cannot know what the validator did. It only knows what it is
-- told. So the design is to be told the same fact three ways, from three
-- different nodes, and refuse when they disagree:
--
--   the fetched count      comes from the source request
--   the submitted count    comes from the node that builds the model request
--   the two id lists       come from the validator
--   the rows               are in the tables
--
-- A caller that sends empty lists by mistake is caught, because the lists no
-- longer add up to the submitted count, or because rows exist that the lists
-- do not mention. A caller that runs before the pipeline has done anything is
-- caught, because it has no fetched count to send.
--
-- Checks, in order. Any failure raises and changes nothing:
--   1. p_fetched and p_submitted are present and not negative.
--   2. p_submitted <= p_fetched.
--   3. If anything was fetched, something was submitted.
--   4. accepted + rejected = p_submitted. Every submitted item got a verdict.
--   5. Every accepted id has a row in jobs_extracted under this run.
--   6. Every rejected id has a row in dead_letter_log under this run.
--   7. Rejected records with no id cannot be matched by id, so the number of
--      id-less rows must equal the number of id-less verdicts exactly.
--   8. No row exists under this run that the lists do not account for.
--
-- A legitimate zero-record run passes all of them: fetched 0, submitted 0,
-- both lists empty, no rows.
--
-- accepted_count and rejected_count are the lengths of the two lists: the
-- validator's verdicts. Rows actually written are returned alongside and can
-- always be recounted from the tables. A replayed insert is skipped by the
-- unique index from 0004, so it cannot inflate either number. The one
-- exception is a rejected record with no id, which that index cannot protect;
-- a replay of one of those trips check 7 instead of passing unnoticed.
--
-- What this does not do. It does not stop a row being inserted under this
-- run_id after the run has closed. Nothing in the workflow does that, and a
-- trigger to forbid it would also have to reject the harmless replay of a row
-- that is already there. A late row is detectable instead: the stored counts
-- then disagree with the rows, and the API reports rows_match_counts false.
--
-- State rules:
--   run does not exist            -> raises
--   run is already 'completed'    -> returns the stored row, changes nothing
--   run is 'running' or 'failed'  -> becomes 'completed' if every check passes
--
-- 'failed' may become 'completed' because n8n can retry a failed execution
-- from the node that failed. If the retry writes everything, the run did
-- complete. The earlier failure stays in pipeline_errors, and error_category
-- and error_message stay on the run row as the record that it happened.
--
-- The run row is locked for the whole call, so two closers, or a closer and
-- record_pipeline_error, take turns and each sees the other's result.
CREATE FUNCTION public.complete_pipeline_run(
    p_run_id       uuid,
    p_fetched      integer,
    p_submitted    integer,
    p_accepted_ids jsonb,
    p_rejected_ids jsonb
)
RETURNS TABLE (
    run_id          uuid,
    status          text,
    completed_at    timestamp with time zone,
    accepted_count  integer,
    rejected_count  integer,
    accepted_rows   bigint,
    rejected_rows   bigint,
    already_closed  boolean
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
    v_status            text;
    v_accepted          integer;
    v_rejected          integer;
    v_missing           bigint;
    v_null_expected     bigint;
    v_null_written      bigint;
    v_extra             bigint;
    v_already_closed    boolean := false;
BEGIN
    IF p_run_id IS NULL THEN
        RAISE EXCEPTION 'complete_pipeline_run: run id is null';
    END IF;
    IF p_accepted_ids IS NULL OR jsonb_typeof(p_accepted_ids) <> 'array'
       OR p_rejected_ids IS NULL OR jsonb_typeof(p_rejected_ids) <> 'array' THEN
        RAISE EXCEPTION 'complete_pipeline_run: accepted and rejected ids must be JSON arrays';
    END IF;

    -- Lock the run row. Two closers for one run then take turns.
    SELECT r.status INTO v_status
    FROM public.pipeline_runs r
    WHERE r.run_id = p_run_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'complete_pipeline_run: no run %', p_run_id;
    END IF;

    IF v_status = 'completed' THEN
        v_already_closed := true;
    ELSE
        v_accepted := jsonb_array_length(p_accepted_ids);
        v_rejected := jsonb_array_length(p_rejected_ids);

        -- 1. A missing count means the node it comes from has not run. That
        -- is what a completion called too early looks like.
        IF p_fetched IS NULL OR p_submitted IS NULL THEN
            RAISE EXCEPTION
                'complete_pipeline_run: run % was closed too early. The fetched or submitted count is missing, so the pipeline had not reached that point.',
                p_run_id;
        END IF;
        IF p_fetched < 0 OR p_submitted < 0 THEN
            RAISE EXCEPTION 'complete_pipeline_run: counts cannot be negative';
        END IF;

        -- 2.
        IF p_submitted > p_fetched THEN
            RAISE EXCEPTION
                'complete_pipeline_run: run % submitted % items but only % were fetched.',
                p_run_id, p_submitted, p_fetched;
        END IF;

        -- 3. The Limit node cannot be set below one, so postings in and
        -- nothing submitted means the items were lost or not yet there.
        IF p_fetched > 0 AND p_submitted = 0 THEN
            RAISE EXCEPTION
                'complete_pipeline_run: run % fetched % postings and submitted none. Closed too early, or items were lost before the model.',
                p_run_id, p_fetched;
        END IF;

        -- 4.
        IF v_accepted + v_rejected <> p_submitted THEN
            RAISE EXCEPTION
                'complete_pipeline_run: run % has % verdicts for % submitted items. Items did not reach the validator, or the id lists are incomplete.',
                p_run_id, v_accepted + v_rejected, p_submitted;
        END IF;

        -- 5. Accepted records always have an id. A null one counts as missing.
        SELECT count(*) INTO v_missing
        FROM (
            SELECT DISTINCT e.id
            FROM jsonb_array_elements_text(p_accepted_ids) AS e(id)
        ) want
        WHERE want.id IS NULL
           OR NOT EXISTS (
                SELECT 1 FROM public.jobs_extracted j
                WHERE j.run_id = p_run_id AND j.source_job_id = want.id
           );

        IF v_missing > 0 THEN
            RAISE EXCEPTION
                'complete_pipeline_run: run % is not fully written. % of % accepted records have no row in jobs_extracted.',
                p_run_id, v_missing, v_accepted;
        END IF;

        -- 6.
        SELECT count(*) INTO v_missing
        FROM (
            SELECT DISTINCT e.id
            FROM jsonb_array_elements_text(p_rejected_ids) AS e(id)
            WHERE e.id IS NOT NULL
        ) want
        WHERE NOT EXISTS (
            SELECT 1 FROM public.dead_letter_log d
            WHERE d.run_id = p_run_id AND d.source_job_id = want.id
        );

        -- 7.
        SELECT count(*) INTO v_null_expected
        FROM jsonb_array_elements_text(p_rejected_ids) AS e(id)
        WHERE e.id IS NULL;

        SELECT count(*) INTO v_null_written
        FROM public.dead_letter_log d
        WHERE d.run_id = p_run_id AND d.source_job_id IS NULL;

        IF v_missing > 0 OR v_null_written < v_null_expected THEN
            RAISE EXCEPTION
                'complete_pipeline_run: run % is not fully written. % of % rejected records have no row in dead_letter_log.',
                p_run_id,
                v_missing + GREATEST(v_null_expected - v_null_written, 0),
                v_rejected;
        END IF;

        -- 8. Rows the lists do not mention. Either the lists are wrong or a
        -- row was written twice where the unique index could not stop it.
        SELECT
            (SELECT count(*) FROM public.jobs_extracted j
              WHERE j.run_id = p_run_id
                AND NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements_text(p_accepted_ids) AS e(id)
                    WHERE e.id = j.source_job_id))
          + (SELECT count(*) FROM public.dead_letter_log d
              WHERE d.run_id = p_run_id
                AND d.source_job_id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements_text(p_rejected_ids) AS e(id)
                    WHERE e.id = d.source_job_id))
          + GREATEST(v_null_written - v_null_expected, 0)
        INTO v_extra;

        IF v_extra > 0 THEN
            RAISE EXCEPTION
                'complete_pipeline_run: run % has % rows the validator did not report. The id lists are incomplete, or a row was written twice.',
                p_run_id, v_extra;
        END IF;

        UPDATE public.pipeline_runs r
        SET status          = 'completed',
            completed_at    = now(),
            fetched_count   = p_fetched,
            processed_count = p_submitted,
            accepted_count  = v_accepted,
            rejected_count  = v_rejected
        WHERE r.run_id = p_run_id;
    END IF;

    RETURN QUERY
    SELECT r.run_id, r.status, r.completed_at, r.accepted_count, r.rejected_count,
           (SELECT count(*) FROM public.jobs_extracted j WHERE j.run_id = r.run_id),
           (SELECT count(*) FROM public.dead_letter_log d WHERE d.run_id = r.run_id),
           v_already_closed
    FROM public.pipeline_runs r
    WHERE r.run_id = p_run_id;
END
$$;

-- ---------------------------------------------------------------------------
-- record_pipeline_error
-- ---------------------------------------------------------------------------
-- Called by the Error Trigger workflow. Writes one pipeline_errors row, finds
-- the run the failure belongs to if there is one, and marks that run 'failed'
-- if it is still 'running'. It never touches any other run.
--
-- Finding the run. All of these must hold:
--   - the run's n8n_execution_id equals the failed execution's id, or the id
--     of the execution it is a retry of
--   - the workflow ids agree, when both sides have one
--   - the run started within the last 6 hours
-- and of the runs that pass, an exact execution-id match beats a retry match,
-- then the most recently started wins. The 6 hours is what stops execution 7
-- on a rebuilt n8n from closing a run that execution 7 opened a month ago. A
-- run takes seconds, so 6 hours is generous.
--
-- No run found is normal, not an error: the failure happened in the trigger,
-- or in Start Run itself, before a run row existed. The error is recorded
-- with run_id NULL.
--
-- Duplicates. The same failure reported twice writes one row. "The same"
-- means same execution id, workflow, node and message, within 6 hours. A
-- failure with no execution id is never treated as a duplicate: two trigger
-- failures with the same message are two failures.
--
-- Category. Derived from the type of the node that failed, which survives a
-- rename on the canvas. The node name is only used to tell the two HTTP
-- calls apart.
--
-- The message is stored as n8n sent it, cut to 4000 characters. It is for
-- diagnosis and is not public. The API redacts and shortens it on the way out.
CREATE FUNCTION public.record_pipeline_error(
    p_workflow_id    text,
    p_workflow_name  text,
    p_execution_id   text,
    p_retry_of       text,
    p_failed_node    text,
    p_node_type      text,
    p_error_message  text
)
RETURNS TABLE (
    error_id           bigint,
    run_id             uuid,
    error_category     text,
    run_marked_failed  boolean,
    duplicate          boolean
)
LANGUAGE plpgsql
AS $$
#variable_conflict use_column
DECLARE
    c_window   CONSTANT interval := interval '6 hours';
    v_workflow_id  text := NULLIF(btrim(p_workflow_id), '');
    v_execution_id text := NULLIF(btrim(p_execution_id), '');
    v_retry_of     text := NULLIF(btrim(p_retry_of), '');
    v_failed_node  text := NULLIF(btrim(p_failed_node), '');
    v_node_type    text := lower(COALESCE(p_node_type, ''));
    v_message      text := left(COALESCE(NULLIF(btrim(p_error_message), ''), '(no message)'), 4000);
    v_category     text;
    v_run_id       uuid;
    v_run_status   text;
    v_error_id     bigint;
    v_marked       boolean := false;
BEGIN
    v_category := CASE
        WHEN v_node_type LIKE '%postgres%' THEN 'database'
        WHEN v_node_type LIKE '%httprequest%'
             AND lower(COALESCE(v_failed_node, '')) ~ '(claude|anthropic)'
                                           THEN 'upstream_llm'
        WHEN v_node_type LIKE '%httprequest%' THEN 'upstream_source'
        WHEN v_node_type LIKE '%.code'        THEN 'pipeline_code'
        WHEN v_node_type LIKE '%trigger%'     THEN 'trigger'
        WHEN v_execution_id IS NULL           THEN 'trigger'
        ELSE 'unknown'
    END;

    IF v_execution_id IS NOT NULL OR v_retry_of IS NOT NULL THEN
        SELECT r.run_id, r.status INTO v_run_id, v_run_status
        FROM public.pipeline_runs r
        WHERE r.n8n_execution_id IN (v_execution_id, v_retry_of)
          AND (r.n8n_workflow_id IS NULL
               OR v_workflow_id IS NULL
               OR r.n8n_workflow_id = v_workflow_id)
          AND r.started_at >= now() - c_window
        ORDER BY (r.n8n_execution_id IS NOT DISTINCT FROM v_execution_id) DESC,
                 r.started_at DESC
        LIMIT 1
        FOR UPDATE;
    END IF;

    IF v_execution_id IS NOT NULL THEN
        SELECT e.id INTO v_error_id
        FROM public.pipeline_errors e
        WHERE e.execution_id = v_execution_id
          AND e.workflow_id   IS NOT DISTINCT FROM v_workflow_id
          AND e.failed_node   IS NOT DISTINCT FROM v_failed_node
          AND e.error_message IS NOT DISTINCT FROM v_message
          AND e.occurred_at >= now() - c_window
        ORDER BY e.id
        LIMIT 1;

        IF FOUND THEN
            RETURN QUERY
            SELECT e.id, e.run_id, e.error_category, false, true
            FROM public.pipeline_errors e
            WHERE e.id = v_error_id;
            RETURN;
        END IF;
    END IF;

    INSERT INTO public.pipeline_errors AS e
        (workflow_id, workflow_name, execution_id, failed_node,
         error_message, error_category, run_id)
    VALUES
        (v_workflow_id, NULLIF(btrim(p_workflow_name), ''), v_execution_id,
         v_failed_node, v_message, v_category, v_run_id)
    RETURNING e.id INTO v_error_id;

    IF v_run_id IS NOT NULL AND v_run_status = 'running' THEN
        UPDATE public.pipeline_runs r
        SET status         = 'failed',
            completed_at   = now(),
            error_category = v_category,
            error_message  = v_message
        WHERE r.run_id = v_run_id
          AND r.status = 'running';
        v_marked := FOUND;
    END IF;

    RETURN QUERY SELECT v_error_id, v_run_id, v_category, v_marked, false;
END
$$;
