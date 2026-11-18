-- 0008 workflow identity
--
-- An n8n execution id is not a key. n8n numbers executions from 1 again when
-- its volume is recreated, so execution 7 today and execution 7 after a
-- rebuild are different events. The id of the workflow narrows the match: an
-- error from one workflow must never close a run that another workflow
-- opened under the same execution number.
--
-- Nullable, no default, no backfill. Rows written before this stay NULL and
-- match on execution id and recency alone.

ALTER TABLE public.pipeline_runs
    ADD COLUMN n8n_workflow_id text;

ALTER TABLE public.pipeline_errors
    ADD COLUMN workflow_id text;
