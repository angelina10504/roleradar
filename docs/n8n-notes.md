# n8n notes

Node choices and wiring rationale. The workflow is built by hand in the canvas;
`workflows/job-pipeline.json` is an export kept for the record.

## Node chain

| node | type | why this one |
|---|---|---|
| Schedule Trigger | Schedule Trigger | Cron. Nothing about this pipeline is event-driven. |
| HTTP Request | HTTP Request | `GET` Remotive. No dedicated node exists, and the API needs no auth. |
| Split Out | Split Out | Turns the `jobs` array in the single response item into one item per posting. Everything downstream is per-posting. |
| Limit | Limit | Caps items. This is the cost control, not a convenience: one item is one billed Anthropic call. |
| Prep | Code | Builds the request body and strips HTML out of the description. |
| Claude Extract | HTTP Request | `POST /v1/messages` with forced tool use. Not the built-in AI nodes: those wrap the call and make `tool_choice` and the exact `input_schema` harder to pin down, and the forced tool schema is the thing this project is demonstrating. |
| Validator | Code | Runs a copy of `validator/validate.js`. |
| IF | IF | Routes on the validator's `valid` boolean. |
| Edit Fields | Set | Maps validator output to column names before insert. See below. |
| jobs_extracted / dead_letter_log | Postgres | Inserts. |

## Why forced tool use, not prompt-and-parse

`tool_choice: {"type":"tool","name":"extract_job_fields"}` makes the API return
a `tool_use` block conforming to the tool's `input_schema`. Nothing is parsed
out of free text, and there is no "return only JSON" instruction to be ignored.
The validator still checks everything, because conforming to a JSON schema and
being correct are different things — a `salary_min` of `0.2333` next to a
`salary_max` of `14` is schema-valid and wrong.

## The Edit Fields node before dead_letter_log

The validator emits the description as `description_clean`. The
`dead_letter_log` column is named `description`. The names do not match, so
without a rename the column inserts as NULL, which defeats the point of the
column: a dead-letter row exists to be read against the text it came from.

An **Edit Fields (Set)** node between the IF node's false branch and the
Postgres node maps `description_clean` to `description`.

The alternative was renaming one side so they match — either the column or the
validator's record key. That is cleaner and should probably happen. It was not
done because renaming the validator key means changing `validate.js` and the
record-shape assertions in `validate.test.js`, and renaming the column means a
migration against a table that already has rows.

**This mapping is not yet verified.** Every row currently in `dead_letter_log`
predates it and still has `description IS NULL`. Nothing has failed validation
since the node was added, so no row demonstrates the fix working. Confirm with:

```sql
SELECT id, ingested_at, description IS NULL AS still_null
FROM dead_letter_log ORDER BY id;
```

## Postgres node, not BigQuery node

The design called for BigQuery. Its no-card Sandbox blocks streaming inserts and
DML, and n8n's BigQuery node writes with `insertAll`, which is the streaming
insert API — so that node cannot write to a Sandbox project at all. See
`CLAUDE.md`, "Why Postgres".

The Postgres node maps a JS array to `json`, not to a native Postgres array
type. That is why `tech_stack` and `error_codes` are `jsonb` columns rather than
`text[]`: a `text[]` column rejects the insert.

## Validation failures and infra failures are different workflows

The IF node handles data validation failures: the model returned something
wrong. Infra failures — timeouts, non-2xx responses, an Anthropic 529 — are a
different event and belong in a separate **Error Trigger** workflow. Mixing them
makes both un-diagnosable.

That Error Trigger workflow is not built. Infra failures are currently uncaught.
