# n8n notes

Node choices and wiring rationale. The workflow is built by hand in the canvas;
`workflows/job-pipeline.json` is an export kept for the record.

This file has three parts. The first describes the workflow as it runs today.
The second, [Run-aware workflow](#run-aware-workflow-not-yet-built), and the
third, [Error Trigger workflow](#error-trigger-workflow-not-yet-built), are
instructions for changes that have **not** been made yet.

## Node chain (as deployed)

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
| Edit Fields | Set | Turns arrays into JSON strings before insert. See below. |
| jobs_extracted / dead_letter_log | Postgres | Inserts. |

## Why forced tool use, not prompt-and-parse

`tool_choice: {"type":"tool","name":"extract_job_fields"}` makes the API return
a `tool_use` block conforming to the tool's `input_schema`. Nothing is parsed
out of free text, and there is no "return only JSON" instruction to be ignored.
The validator still checks everything, because conforming to a JSON schema and
being correct are different things — a `salary_min` of `0.2333` next to a
`salary_max` of `14` is schema-valid and wrong.

## What the Edit Fields nodes are for

The Postgres node sends a JS array to Postgres as an array literal, and a
`jsonb` column rejects that. A JSON *string* is accepted and parsed. So any
array bound for a `jsonb` column has to be stringified first, exactly once:

| column | what the validator emits | needs stringifying in the canvas |
|---|---|---|
| `tech_stack` | array | yes |
| `error_codes` | array | yes |
| `errors_json` | JSON string | **no** |
| `raw_output` | JSON string | no |

The exported workflow stringifies `errors_json` as well. The validator had
already done it, so the column receives a string inside a string. That is the
double-encoding in the live table: `jsonb_typeof(errors_json)` is `string` on
all six rows, where `error_codes` is `array` and `raw_output` is `object`.

## The description never reached dead_letter_log

An earlier version of this file said an Edit Fields node maps the validator's
`description_clean` to the `description` column. The exported workflow does not
do that. Its Edit Fields node *excludes* `description_clean` and assigns nothing
to `description`, so the text is dropped. `description` is NULL in every live
row.

The fix is in the schema, not the canvas: migration `0006` adds a
`description_clean` column, so the validator's field name and the column name
agree and nothing needs mapping. See step 8 below.

## Postgres node, not BigQuery node

The design called for BigQuery. Its no-card Sandbox blocks streaming inserts and
DML, and n8n's BigQuery node writes with `insertAll`, which is the streaming
insert API — so that node cannot write to a Sandbox project at all. See
`CLAUDE.md`, "Why Postgres".

## Validation failures and infra failures are different workflows

The validator handles data validation failures: the model returned something
wrong. Infra failures — timeouts, non-2xx responses, an Anthropic 529 — are a
different event and belong in a separate **Error Trigger** workflow. Mixing them
makes both un-diagnosable.

That Error Trigger workflow is not built. Infra failures are currently uncaught.
The instructions for building it are the third part of this file.

---

# Run-aware workflow (not yet built)

Everything below is a change **you make by hand in the n8n canvas**. Nothing
here has been applied. None of it was tested in a running n8n.

What was checked, and against what:

- **Node behaviour** was read from the source of the n8n image on this machine,
  `docker.n8n.io/n8nio/n8n:latest`, which is n8n **2.35.7**. The image was run
  with no network and no volume, only to read files. If your running instance
  is a different version, check the options named below still exist.
- **Every SQL statement** in this section is run by `tests/` against Postgres
  16, copied out of this file by the test, so the text here is the text tested.
- **The record shapes** are produced by the real `validateBatch()` and inserted
  into the migrated schema by the same tests.
- **Not checked:** the canvas itself. Expressions like `$('Start Run')` are
  written from n8n's documentation. The existing export was treated as a
  description of the old workflow, not as proof of what your instance has now:
  it is marked inactive, its schedule is empty and it searches for `python`.

## Before you touch the canvas

**Migrate the database the Postgres credential points at.** The Postgres node
refuses any input field that is not a column (`Column 'run_id' does not exist in
selected table`). The new records carry `run_id` and `publication_date`, so
against an unmigrated database every insert fails. `Complete Run` and the Error
Trigger workflow call functions that migration `0009` creates, so the database
has to be migrated all the way to `0009`, not just far enough for the columns.

The reverse order is safe. Every new column is nullable, so today's workflow
keeps working against a migrated database. Migrate first, then change the
canvas, in that order, with as long a gap as you like.

## Target shape

```
Schedule Trigger
  -> Start Run
       |-> HTTP Request -> Split Out -> Limit -> Prep -> Claude Extract -> Validator
       |                     |-> Accepted Rows -> Edit Fields3 -> Insert jobs_extracted
       |                     |-> Rejected Rows -> Edit Fields1 -> Insert dead_letter_log
       |
       |-> Complete Run          (drawn BELOW everything above)
```

`Complete Run` asks the database to check the writes before it closes the run.
Its position on the canvas decides *when* it runs. It no longer decides whether
a run can be called complete. See step 10.

The IF node goes away. The Validator now emits one item describing the whole
batch, and two Split Out nodes fan its two lists out.

## 1. Where the run UUID is created — `Start Run`

**[manual] Add a Postgres node** named `Start Run` between Schedule Trigger and
HTTP Request.

- Operation: **Execute Query**
- Query:

<!-- sql:start-run -->
```sql
INSERT INTO public.pipeline_runs (n8n_execution_id, n8n_workflow_id)
VALUES ($1::text, $2::text)
RETURNING run_id, started_at;
```

- Options → Query Parameters: `{{ [ $execution.id, $workflow.id ] }}`

Postgres generates the UUID (`gen_random_uuid()` is the column default) and
`RETURNING` hands it back, so the node outputs one item with `run_id` in it.
The alternative was generating the UUID in a Code node. Rejected: it needs a
UUID function the Code node may not expose, and it lets a run id exist that the
database has never heard of.

Execute Query, not the Insert operation, so the statement is text that can be
tested. `CLAUDE.md` warns about hand-built SQL because of quoting. That risk is
string concatenation; this uses `$1` with a parameter, so nothing is quoted by
hand.

## 2. How the execution ID is captured

`$execution.id` and `$workflow.id` in the parameters above. They are stored in
`pipeline_runs.n8n_execution_id` and `n8n_workflow_id`. Those two values are
what an Error Trigger workflow is told about a failed run, so they are how an
error finds its run.

The execution id is not unique in the table and must not be treated as a key.
n8n numbers executions from 1 again if its Docker volume is recreated. The
workflow id narrows the match, and the matching rule also requires the run to
be recent. The full rule is in the Error Trigger part of this file.

## 3. Where the ingestion timestamp is generated — the `Validator` wrapper

Once, in the Code node, on the line before `validateBatch` is called. Not in a
loop. Every record in the run then carries the identical `ingested_at`.

It is not taken from `started_at`. That would be the database's clock at the
start of the run. `ingested_at` has always meant "when the validator saw it",
and keeping that meaning keeps new rows comparable with old ones.

The value must be UTC and end in `Z`. `validateBatch` throws otherwise.
`toISOString()` always produces that, whatever timezone the container is set to.

## 4. How source records reach validateBatch — the `Validator` node

**[manual] Replace the whole contents of the Validator Code node.**

- Mode: **Run Once for All Items**
- Paste all of `validator/validate.js`, unchanged.
- Under it, paste this wrapper in place of the old loop:

```js
const sources = $('Prep').all();
const responses = $input.all();

if (sources.length !== responses.length) {
  throw new Error(
    `Prep produced ${sources.length} items and Claude Extract ${responses.length}. They must pair one to one.`
  );
}

const pairs = responses.map((response, i) => ({
  source: sources[i].json,
  extracted: response.json,
}));

const ingestedAt = new Date().toISOString();
const runId = $('Start Run').first().json.run_id;

return [{ json: validateBatch(pairs, ingestedAt, runId) }];
```

The node now outputs **one item**, not one per posting:

```
run_id, ingested_at, extraction_version,
records_processed, records_accepted, records_rejected,
accepted_records: [ ... ], rejected_records: [ ... ], warnings: [ ... ]
```

Pairs are matched by position, as before. The length check is new: if the two
lists ever differ, positions no longer mean the same posting, and failing is
better than validating one job's extraction against another job's description.

This wrapper is the part of the pipeline with no test. It cannot have one: it
uses `$input` and `$()`, which exist only inside n8n. It is kept to wiring so
there is as little of it as possible.

**[manual] Delete the IF node** and the two No Operation nodes after it.

## 5. How accepted records reach jobs_extracted

**[manual] Add a Split Out node** named `Accepted Rows`, connected to Validator.

- Fields To Split Out: `accepted_records`
- Include: **No Other Fields**

Each element of the list becomes its own item, with the record's fields at the
top level. An empty list produces no items and the branch simply stops, which
is correct when nothing was accepted.

**[manual] Connect it to `Edit Fields3`**, then to the existing insert node.

In `Edit Fields3`: keep the `tech_stack` assignment
(`{{ JSON.stringify($json.tech_stack) }}`). Clear the "fields to exclude" box;
it lists `__valid`, which no longer exists.

In the `jobs_extracted` insert node: leave Mapping Column Mode on **Map
Automatically**. Open the column list and refresh it so `run_id` and
`publication_date` appear.

## 6. How rejected records reach dead_letter_log

**[manual] Add a second Split Out node** named `Rejected Rows`, also connected
to Validator. Place it below `Accepted Rows`.

- Fields To Split Out: `rejected_records`
- Include: **No Other Fields**

**[manual] Connect it to `Edit Fields1`**, then to the existing insert node.

In `Edit Fields1`:

- keep the `error_codes` assignment
- **delete the `errors_json` assignment** (step 8)
- **clear the "fields to exclude" box entirely**

That exclude list currently names `description_clean`, which is why the
description never arrived. It also names eleven extracted fields and `__valid`,
none of which are on a rejected record.

In the `dead_letter_log` insert node: refresh the column list so `run_id`,
`publication_date` and `description_clean` appear. If `id` is set as a matching
column, remove that; this is a plain insert.

## 7. How publication_date is mapped

No node is needed. `Prep` already copies Remotive's `publication_date` onto the
source item. `validateBatch` puts it on both kinds of record, and automatic
mapping sends it to the column of the same name.

The contract, enforced in `validateBatch`:

| Remotive sends | stored | effect on the row |
|---|---|---|
| `2026-08-21T05:54:39` (optionally with fractional seconds) | that exact text | none |
| nothing, `null` or `""` | `NULL` | none; a `PUBLICATION_DATE_MISSING` warning |
| anything else, including a value with `Z` or an offset | `NULL` | none; a `PUBLICATION_DATE_MALFORMED` warning |

A bad date never rejects a row and is never replaced with a guess. Warnings are
in the Validator node's output under `warnings`. They are not stored in the
database, so the only place to see one is the execution view.

## 8. How errors_json is written as proper JSON

By removing a step. `validateBatch` emits `errors_json` as a JSON string. The
Postgres node passes a string through as text and Postgres parses it into the
`jsonb` column as an array.

The bug was the second `JSON.stringify` in `Edit Fields1`. With that assignment
deleted in step 6, new rows land as arrays. The six existing rows stay as they
are; the dashboard query already reads both encodings.

Check after the first rejected row lands:

```sql
SELECT id, jsonb_typeof(errors_json) FROM public.dead_letter_log ORDER BY id;
```

New rows must say `array`.

## 9. How duplicate inserts are handled

**[manual] On both insert nodes, turn on Options → Skip on Conflict.**

In n8n 2.35.7 that appends `ON CONFLICT DO NOTHING` to the insert. Migration
`0004` adds a unique index on `(run_id, source_job_id)` to both tables, for rows
that have a `run_id`. Together: inserting the same posting twice under one run
writes it once, and the second attempt is a silent no-op.

Without the option, a replayed insert fails the node instead of being skipped.

What this covers and what it does not:

- **Covered:** re-running an insert node inside one execution, or a retry that
  reuses the same `Start Run` output.
- **Not a duplicate, by design:** the same posting in a *different* run. That is
  a new observation and it inserts. The table stays append-only across runs.
- **Not covered:** a rejected row whose `source_job_id` is NULL. Postgres treats
  NULLs as distinct, so replaying that row writes it again.
- **A consequence:** a skipped row is not returned by the node. The node's
  output count is rows actually written, which can be lower than
  `records_accepted`.

## 10. How the run is closed — `Complete Run`

**[manual] Add a Postgres node** named `Complete Run`. Connect it to **Start
Run**, as a second connection out of that node. **Drag it below every other
node on the canvas.**

- Operation: **Execute Query**
- Query:

<!-- sql:complete-run -->
```sql
SELECT * FROM public.complete_pipeline_run(
    $1::uuid, $2::integer, $3::integer, $4::jsonb, $5::jsonb
);
```

- Options → Query Parameters:

```
{{ [
  $json.run_id,
  $('HTTP Request').isExecuted ? $('HTTP Request').first().json.jobs.length : null,
  $('HTTP Request').isExecuted ? ($('Prep').isExecuted ? $('Prep').all().length : 0) : null,
  JSON.stringify($('Validator').isExecuted ? $('Validator').first().json.accepted_records.map(r => r.source_job_id) : []),
  JSON.stringify($('Validator').isExecuted ? $('Validator').first().json.rejected_records.map(r => r.source_job_id ?? null) : [])
] }}
```

The node hands the database five things, each from a different node:

| # | what | from |
|---|---|---|
| 1 | the run | `Start Run` |
| 2 | postings Remotive returned | `HTTP Request` |
| 3 | items sent to the model, after the Limit | `Prep` |
| 4 | `source_job_id` of every accepted record | `Validator` |
| 5 | `source_job_id` of every rejected record, `null` where it had none | `Validator` |

Parameters 2 and 3 are `null` when `HTTP Request` has not run. That is on
purpose. It is how the database tells "the feed was empty" (0) from "this node
ran before the pipeline did" (`null`).

**What the function does.** `complete_pipeline_run` is defined in migration
`0009`. It cannot see what the validator did; it only knows what this node
tells it. So it is told the same fact from several nodes and refuses when they
disagree with each other or with the tables:

1. Both counts are present. Missing means closed too early.
2. Submitted is not more than fetched.
3. If anything was fetched, something was submitted.
4. Accepted plus rejected equals submitted. Every item got a verdict.
5. Every accepted id has a row in `jobs_extracted` under this run.
6. Every rejected id has a row in `dead_letter_log` under this run.
7. Rejected records with no id are matched by number, exactly.
8. No row exists under this run that the lists do not mention.

Only if all eight hold does it set `status = 'completed'` and store the counts.
Otherwise it raises, changes nothing, and the node fails.

The earlier version of this step did only checks 5 and 6, against whatever
lists it was sent. That had a hole: a `Complete Run` drawn above the inserts
runs before `Validator`, the expressions above then produce two empty lists,
and two empty lists were accepted as a run with nothing to write. The run was
marked complete and the rows were inserted afterwards. Checks 1 to 4 and 8
close that. The tests reproduce the old hole and show it refused.

| situation | what the node sends | what happens |
|---|---|---|
| normal run | counts, both lists | every check passes, run closes |
| nothing accepted, or nothing rejected | one list empty, counts still add up | run closes |
| Remotive returned nothing | fetched 0, submitted 0, `[]`, `[]` | run closes with zero processed |
| Complete Run ran before everything | fetched `null` | check 1: refused |
| Complete Run ran after the fetch, before the Validator | fetched 13, submitted 0 or 5, `[]`, `[]` | check 3 or 4: refused |
| Complete Run ran after the Validator, before the inserts | full lists, no rows yet | check 5: refused |
| an insert node failed | the execution stopped, so Complete Run never ran | stays `running` until the error workflow marks it `failed` |
| the lists are wrong or empty but rows were written | rows the lists do not mention | check 8: refused |
| Complete Run executed twice | anything | second call returns the stored row, `already_closed = true` |
| an insert replayed | — | skipped by the unique index; counts unchanged |
| a rejected row with no id replayed | — | written twice, because the index cannot see it. Check 7 refuses the close |

**Why it hangs off Start Run and not off the insert nodes.** The obvious wiring
is insert → Complete Run. It breaks whenever a branch has nothing to do. A node
that receives no items does not run, so a run with no rejections never reaches a
node placed after the rejected insert, and a run where Remotive returned nothing
never reaches the Validator at all. Each of those would leave the run `running`
forever. `Start Run` always outputs exactly one item, so a node connected to it
always runs.

A Merge node after the two inserts was considered and rejected for the same
reason. A branch with no items produces nothing to merge, and so does an insert
whose rows were all skipped as duplicates. Making that work means turning on
"Always Output Data" along both branches and then telling real rows from
placeholder items downstream. That is more canvas configuration with no test,
to replace one rule about where a box sits. Merge behaviour with an empty input
was not verified in n8n; it was not needed once the database does the check.

**What still depends on the canvas.** With two connections out of one node, n8n
runs them one after the other: "completing one branch before starting another",
ordered "from topmost to bottommost". This is execution order `v1`, the setting
the exported workflow uses (Workflow Settings → Execution Order). Complete Run
has to be the lower branch so it runs after the inserts.

What changed is the cost of getting that wrong. Before, a misplaced Complete
Run closed the run early and recorded it as complete with nothing written. Now
it fails the execution, the run is marked `failed`, and the error says which
check refused it. Position can still break a run. It can no longer make a
broken run look finished.

That claim rests on the expressions above sending `null` and `[]` when a node
has not run, which is what `.isExecuted` is for. It was not run in n8n. Step 11
has a deliberate test for it.

Two things remain true and are not fixed by this:

- If the workflow is switched to the legacy execution order (`v0`), branches
  interleave and Complete Run will fail on every run that has records.
- The inserts and the closing update are separate statements, not one
  transaction. If n8n dies between them the rows exist and the run stays
  `running`. The API reports it as stale after 15 minutes.
- Nothing stops a row being inserted under a `run_id` after that run has
  closed. The workflow never does it. If it happens, the stored counts no
  longer match the rows and `/api/pipeline-runs` shows `rows_match_counts:
  false` for that run.
- A retried execution reuses data from the first attempt. Whether
  `$('HTTP Request')` and `$('Prep')` still resolve inside a retry was not
  checked. If they do not, the retry's Complete Run is refused by check 1 and
  the run stays `failed`, which is the safe direction.

**What the counts mean.** They are the validator's verdicts, not the database's
writes:

| column | meaning | where it comes from |
|---|---|---|
| `fetched_count` | postings Remotive returned for the search | length of `jobs` in the HTTP Request output |
| `processed_count` | items sent to the model, after the Limit node | number of items `Prep` produced |
| `accepted_count` | items the validator accepted | length of the accepted id list |
| `rejected_count` | items the validator rejected | length of the rejected id list |
| *(not stored)* rows written | rows actually in each table for this run | counted from the tables; the function returns them as `accepted_rows` and `rejected_rows`, and so does `/api/pipeline-runs` |

`accepted_count = 4` says the validator passed four. The number of rows is a
count over the table, done fresh each time it is asked for, so a replay cannot
inflate it. The two can differ in one legitimate case: Remotive returns the
same posting twice in one response, both copies are accepted, and the unique
index keeps one row. The run still closes, because every accepted id has a row.

**If any node fails**, the execution stops and Complete Run never runs. The row
stays `running` until the Error Trigger workflow marks it `failed`.

## 11. Check it worked

Run the workflow once with the Limit node still at 5. Then:

```sql
-- The run closed, and the counts add up.
SELECT run_id, n8n_execution_id, status, completed_at - started_at AS took,
       fetched_count, processed_count, accepted_count, rejected_count
FROM public.pipeline_runs ORDER BY started_at DESC LIMIT 1;

-- Validator counts against rows actually written. They should match.
SELECT r.accepted_count,
       (SELECT count(*) FROM public.jobs_extracted j WHERE j.run_id = r.run_id)  AS accepted_rows,
       r.rejected_count,
       (SELECT count(*) FROM public.dead_letter_log d WHERE d.run_id = r.run_id) AS rejected_rows
FROM public.pipeline_runs r ORDER BY r.started_at DESC LIMIT 1;

-- One timestamp for the whole run, and the dates arrived.
SELECT count(DISTINCT ingested_at) AS distinct_timestamps,
       count(publication_date)     AS with_publication_date,
       count(*)                    AS rows
FROM public.jobs_extracted
WHERE run_id = (SELECT run_id FROM public.pipeline_runs ORDER BY started_at DESC LIMIT 1);
```

In the execution view, confirm `Complete Run` is the last node to run. If it
failed with "is not fully written", it ran before the inserts: it is not the
lowest node on the canvas.

Then execute `Complete Run` a second time by hand. It must return
`already_closed = true` and the same `completed_at`.

**Test the early-close guard on purpose, once.** Duplicate the workflow so the
real one is untouched. In the copy, set the Limit to 1 and drag `Complete Run`
above `HTTP Request`. Execute it. `Complete Run` must fail with "was closed too
early", and the newest `pipeline_runs` row must still be `running`, not
`completed`. This costs at most one Claude call, and none if the failure stops
the execution first. Delete the copy afterwards. A manual execution is enough
here, because what is being tested is the database's refusal, not the error
workflow.

Then execute the `jobs_extracted` insert node a second time by hand. The row
count for the run must not change.

Then **export the workflow** to `workflows/` and commit the export.

## What this does not do

- **Nothing retries.** One failed Claude call still fails the run and loses the
  extractions already paid for in it.
- **Validator drift is unchanged.** The Code node holds a pasted copy of
  `validate.js`. Passing tests in the repo say nothing about what is pasted.
- **Writing and closing are not one transaction.** See step 10.
- **Without the Error Trigger workflow below, a failed run stays `running`.**

---

# Error Trigger workflow (not yet built)

A second, separate workflow. It runs when the ingestion workflow fails, writes
one row to `pipeline_errors`, and marks the run `failed`. Everything here is a
change **you make by hand**. None of it has been run in n8n.

What was checked, and against what:

- **How n8n calls an error workflow** was read from the source of n8n 2.35.7
  (`execute-error-workflow.js`, `execution-lifecycle-hooks.js`,
  `workflow-execution.service.js`), in the image on this machine, offline.
- **The SQL statement** is copied out of this file and run by `tests/`.
- **The function it calls** is tested in `tests/test_run_lifecycle.py`.
- **Not checked:** the expressions. They are written from the shape of the data
  in that source. `execution.error.node` in particular is what n8n's node
  errors normally carry, but it was not observed in a real failure.

## What n8n does and does not do

Read from the source, not assumed:

- **A manual execution never calls the error workflow.** The code is
  `dispatchesErrorWorkflow = !isManualMode && ...`. Pressing "Execute workflow"
  and watching a node fail proves nothing about this workflow. It will not run.
- **The error workflow must be published.** n8n loads its published version
  and logs `is not active and cannot be executed` if there is none. It runs the
  published version, not the draft on the canvas.
- **The data it receives** is one of two shapes:

  ```
  { execution: { id, url, error, lastNodeExecuted, mode, retryOf },
    workflow:  { id, name } }
  ```

  or, when the failure happened before an execution id existed:

  ```
  { trigger:  { error, mode },
    workflow: { id, name } }
  ```

- **It cannot see a run that never started.** If n8n is stopped, the schedule
  does not fire, nothing fails, and nothing calls this workflow. An absent run
  is found by the freshness check, not here.
- **If this workflow itself fails**, for example because Postgres is down,
  nothing records that. n8n does not call an error workflow for an error
  workflow. The run stays `running` and the API reports it stale.

## E1. Create the workflow

**[manual] Create a new workflow** named `RoleRadar errors`.

**[manual] Add an Error Trigger node.** It has no settings.

## E2. Record the error — `Record Error`

**[manual] Add a Postgres node** named `Record Error`, connected to the Error
Trigger. Use the same credential as the ingestion workflow.

- Operation: **Execute Query**
- Query:

<!-- sql:record-error -->
```sql
SELECT * FROM public.record_pipeline_error(
    $1::text, $2::text, $3::text, $4::text, $5::text, $6::text, $7::text
);
```

- Options → Query Parameters:

```
{{ [
  $json.workflow?.id ?? null,
  $json.workflow?.name ?? null,
  $json.execution?.id ?? null,
  $json.execution?.retryOf ?? null,
  $json.execution?.lastNodeExecuted ?? $json.trigger?.error?.node?.name ?? null,
  $json.execution?.error?.node?.type ?? $json.trigger?.error?.node?.type ?? null,
  $json.execution?.error?.message ?? $json.trigger?.error?.message ?? null
] }}
```

In order: workflow id, workflow name, execution id, the execution this one is a
retry of, the node that failed, that node's type, and the error message. Every
one may be null and the function accepts that.

The stack trace is deliberately not sent. It is long, it is not needed to find
the failing node, and the less that is stored the less there is to leak.

That is the whole workflow: two nodes.

## E3. What the function does with it

`record_pipeline_error` is defined in migration `0009`. The rules are there in
full; in short:

**Finding the run.** It looks for a `pipeline_runs` row where all of these hold:

1. `n8n_execution_id` equals the failed execution's id, or the id it is a retry
   of.
2. The workflow ids agree, when both sides have one.
3. The run started in the last 6 hours.

An exact execution-id match is preferred over a retry match, then the newest
run wins. Rule 3 is what makes a recycled execution number safe: execution 7 on
a rebuilt n8n cannot reach a run that execution 7 opened last month.

**Marking it failed.** Only that one run, and only if it is still `running`. A
run that is already `completed` or `failed` is left exactly as it is; the error
row is still written and still points at it.

**No run found.** The error is written with `run_id` NULL. This is the normal
result when `Start Run` itself failed, or the trigger did, because then no run
row exists to find.

**No execution id.** Same: written with `run_id` NULL, category `trigger`.

**The same error twice.** One row. "The same" means the same execution id,
workflow, node and message within 6 hours. The second call returns the first
row with `duplicate = true`. An error with no execution id is never treated as
a duplicate, because two trigger failures with one message are two failures.

**Category.** From the failed node's type, so renaming a node does not change
it:

| node type | category |
|---|---|
| Postgres | `database` |
| HTTP Request, node name contains "claude" or "anthropic" | `upstream_llm` |
| HTTP Request, any other | `upstream_source` |
| Code | `pipeline_code` |
| a trigger node, or no execution id | `trigger` |
| anything else, or type missing | `unknown` |

The one place a name matters is telling the two HTTP calls apart. If `Claude
Extract` is renamed to something without "claude" in it, its failures will be
filed as `upstream_source`.

**The message** is stored as n8n sent it, cut to 4000 characters. The API
redacts it and cuts it to 300 before returning it.

## E4. Connect the ingestion workflow to it

**[manual] Publish the `RoleRadar errors` workflow.**

**[manual] In the ingestion workflow:** Workflow Settings → Error Workflow →
choose `RoleRadar errors`. Save and publish.

Both workflows should belong to the same owner or project. The default caller
policy in 2.35.7 is "workflows from the same owner", and a blocked call is
logged and dropped.

## E5. Check it worked

There are two different things to check, and only the second is proof.

**The wiring.** Open `RoleRadar errors`, select the Error Trigger and execute
the workflow manually. n8n supplies sample data. `Record Error` should run and
a row should appear in `pipeline_errors` with `run_id` NULL. This shows the
query and the expressions are valid. It does **not** show that n8n will call
this workflow when the ingestion workflow fails. Delete nothing afterwards;
the row is an honest record of a test, and `workflow_name` will say so.

**A real failure.** This needs a production execution, and it can be done
without paying for a Claude call:

1. In the ingestion workflow, add a **Stop and Error** node between `Start Run`
   and `HTTP Request`, error message `deliberate test failure`. Being before the
   HTTP nodes, nothing is fetched and nothing is sent to Anthropic.
2. Set the Schedule Trigger to fire in the next couple of minutes. Publish.
3. **Wait for the schedule.** Do not press Execute.
4. Then:

```sql
SELECT run_id, n8n_execution_id, status, error_category, left(error_message, 60)
FROM public.pipeline_runs ORDER BY started_at DESC LIMIT 1;

SELECT id, run_id, workflow_name, execution_id, failed_node, error_category,
       left(error_message, 60)
FROM public.pipeline_errors ORDER BY occurred_at DESC LIMIT 1;
```

Expected: the run is `failed`. The error row carries the **same** `run_id`,
`failed_node` is the Stop and Error node, and the category is `unknown`,
because Stop and Error is not one of the mapped types. `completed_at` on the
run is set.

5. Remove the Stop and Error node, restore the schedule, publish.
6. Export both workflows to `workflows/`.

If the error row has `run_id` NULL, the execution id did not match: compare
`pipeline_runs.n8n_execution_id` with `pipeline_errors.execution_id`.

If no error row appeared at all, the error workflow was not called. Check it
is published and selected in the ingestion workflow's settings, then look at
the n8n container log for `Calling Error Workflow`.

## What the error workflow does not do

- **It does not see a missed schedule.** Covered above.
- **It does not alert anyone.** It writes a row. Something has to read it; that
  is `/api/pipeline-status` and the freshness check.
- **It does not record its own failures.**
- **It keeps one message per error, not the stack or the response body.**

---

# Staging verification

Prove the canvas does what these notes say, before it touches production.
**None of this has been run.** It needs your n8n.

There are two kinds of test here and they prove different things.

| | manual execution | triggered (production) execution |
|---|---|---|
| started by | pressing Execute workflow | the Schedule Trigger firing on a published workflow |
| pinned data | used: pinned nodes do not run, so no Remotive and no Claude call | **ignored: every node runs for real** |
| error workflow | **never called** in n8n 2.35.7 | called, if the workflow is published and has one set |
| proves | the canvas expressions, the SQL, the database's refusals | that n8n calls the error workflow and that the error finds its run |

A manual run that fails and leaves the run `running` is n8n behaving as
written, not a bug. Only a triggered run can test the error workflow.

## S0. Set up an isolated staging database

A disposable Postgres 18, the same major version as Neon, on port 5434. Its
data lives in memory and is gone when the container stops:

```
docker run -d --rm --name roleradar-pg-staging \
  -e POSTGRES_USER=roleradar_test -e POSTGRES_PASSWORD=roleradar_test \
  -e POSTGRES_DB=roleradar_staging -p 127.0.0.1:5434:5432 \
  --tmpfs /var/lib/postgresql postgres:18-alpine
cat tests/fixtures/legacy_schema.sql tests/fixtures/legacy_rows.sql \
  | docker exec -i roleradar-pg-staging psql -U roleradar_test -d roleradar_staging -q
python sql/migrate.py \
  --database-url postgresql://roleradar_test:roleradar_test@127.0.0.1:5434/roleradar_staging up
```

It now looks like Neon will after checkpoint 5 of the runbook: 25 + 6
historical rows, no tracked runs, every migration applied.

**[manual] In n8n, create a Postgres credential** named `RoleRadar staging`:
host `host.docker.internal`, port `5434`, database `roleradar_staging`, user
and password `roleradar_test`, SSL off. From inside the n8n container,
`host.docker.internal` is this Mac.

**[manual] Make staging copies of both workflows.** Duplicate the error
workflow as `RoleRadar errors (staging)` and the ingestion workflow, after
steps 1 to 10 above, as `RoleRadar (staging)`. In both copies, switch
**every** Postgres node to the `RoleRadar staging` credential. In
`RoleRadar (staging)`'s settings, set Error Workflow to
`RoleRadar errors (staging)`. Set its Limit node to 5.

Check twice that no node in either copy still uses the production
credential. A staging test that writes to Neon is a production test.

To watch the dashboard on staging data:

```
cd api && DATABASE_URL=postgresql://roleradar_test:roleradar_test@127.0.0.1:5434/roleradar_staging \
  uvicorn main:app --port 8000
```

## S1. Pin synthetic data, so nothing is fetched and nothing is paid for

The pins are built from the captured fixtures in `validator/fixtures/`: five
real postings and the five real Claude responses to them. The validator
accepts four and rejects one (posting `2091105`, `SALARY_SPAN_IMPLAUSIBLE`).
`IDX` picks which of the five, by position:

```
IDX=0,1,2,3,4 node -e '
  const i = process.env.IDX ? process.env.IDX.split(",").map(Number) : [];
  const s = require("./validator/fixtures/source-records.json");
  console.log(JSON.stringify({ jobs: i.map(n => ({ id: s[n].source_job_id, url: s[n].source_url,
    title: s[n].raw_title, company_name: s[n].raw_company,
    publication_date: s[n].publication_date, description: s[n].description_clean })) }))' | pbcopy
```

**[manual]** Open `HTTP Request` in `RoleRadar (staging)`, Edit Output, paste,
pin. Then the Claude responses, in the same order:

```
IDX=0,1,2,3,4 node -e '
  const i = process.env.IDX ? process.env.IDX.split(",").map(Number) : [];
  const o = require("./validator/fixtures/extraction-output.json");
  console.log(JSON.stringify(i.map(n => o[n])))' | pbcopy
```

Paste and pin on `Claude Extract`. The two pins must use the same `IDX`: the
Validator pairs them by position.

A pinned node does not run in a manual execution. With both pinned, a manual
run calls neither Remotive nor Anthropic.

## S2. The scenarios

Run each with **Execute workflow** unless it says triggered. After each, the
query under the table. Expected values are exact, because the input is fixed.

| # | scenario | how | expect in `pipeline_runs` | expect in the tables |
|---|---|---|---|---|
| 1 | successful ingestion | pins with `IDX=0,1,2,3,4` | `completed`, fetched 5, processed 5, accepted 4, rejected 1 | 4 rows in `jobs_extracted`, 1 in `dead_letter_log`, one `ingested_at`, `errors_json` an `array` |
| 2 | zero rejected | `IDX=1,2,3,4` | `completed`, 4 / 4 / 4 / 0 | `Rejected Rows` branch did not run |
| 3 | zero accepted | `IDX=0` | `completed`, 1 / 1 / 0 / 1 | `Accepted Rows` branch did not run |
| 4 | empty source feed | `IDX=` (empty) on `HTTP Request`. `Claude Extract`'s pin does not matter | `completed`, 0 / 0 / 0 / 0 | nothing written; `Split Out` output empty |
| 5 | replayed insertion | after 1, select the `jobs_extracted` insert node and execute that node only | the same run unchanged | no new rows. If a **new** `pipeline_runs` row appears, n8n re-ran `Start Run`, so this was a second run, not a replay: note it, it is not a failure |
| 6 | **premature completion** | drag `Complete Run` **above** `HTTP Request` on the canvas, execute | the new run stays **`running`**, never `completed` | `Complete Run` fails: "was closed too early" |
| 7 | completion with wrong lists | put `Complete Run` back. Temporarily change its 4th parameter to `JSON.stringify([])`. Execute with `IDX=0,1,2,3,4` | stays `running` | `Complete Run` fails: "4 verdicts" or "rows the validator did not report". The SQL error text is in the node's error panel |
| 8 | **triggered failure** | see S3 | `failed`, `completed_at` set, category `unknown` | one `pipeline_errors` row with the **same** `run_id` |
| 9 | error correlation | after 8, the correlation query below | | `n8n_execution_id = execution_id` and `n8n_workflow_id = workflow_id` |
| 10 | recovery after interruption | add a **Wait** node of 45 seconds between `Start Run` and `Complete Run`. Execute. While it waits, `docker stop roleradar-n8n`, then start it again | stays `running`; after 15 minutes `/api/pipeline-runs` shows `is_stale: true` | rows written. Nothing closes the run. The next run is a new row and completes |

```sql
SELECT status, fetched_count, processed_count, accepted_count, rejected_count,
       (SELECT count(*) FROM public.jobs_extracted j WHERE j.run_id = r.run_id)  AS accepted_rows,
       (SELECT count(*) FROM public.dead_letter_log d WHERE d.run_id = r.run_id) AS rejected_rows
FROM public.pipeline_runs r ORDER BY started_at DESC LIMIT 1;
```

Correlation, for 8 and 9:

```sql
SELECT r.run_id, r.n8n_execution_id, e.execution_id, r.n8n_workflow_id, e.workflow_id,
       r.status, e.error_category, e.failed_node
FROM public.pipeline_runs r
JOIN public.pipeline_errors e ON e.run_id = r.run_id
ORDER BY e.occurred_at DESC LIMIT 1;
```

**Scenario 6 decides the release.** It is the only test of the claim that the
`Complete Run` expressions send `null` when `HTTP Request` has not run.
Pinned nodes add a doubt: whether n8n reports a pinned node as executed is
not checked. In this scenario `Complete Run` runs before `HTTP Request` either
way, so the doubt does not matter here. If the run comes out `completed`,
stop: **do not release**. The database is doing its part and the canvas is
not.

Scenario 10's Wait must be under about a minute. n8n keeps a short wait in
memory, so stopping the container kills the execution. A longer wait is
saved to n8n's database and resumes after a restart, which would test
something else.

Undo every change after its scenario: move `Complete Run` back to the bottom,
restore its parameter, delete the Wait node.

## S3. The triggered failure, without calling Claude

Pinned data is **ignored** in a triggered execution. If this goes wrong, the
staging workflow fetches from Remotive and pays for Claude calls. So:

1. **[manual]** In `RoleRadar (staging)`, set the Limit node to **1**. That
   caps the damage at one Claude call if the next step is missed.
2. **[manual]** Add a **Stop and Error** node directly after `Start Run`, on
   the branch to `HTTP Request`. Message: `staging triggered failure`.
   Nothing after it on that branch can run.
3. **[manual]** Set the Schedule Trigger to every minute.
4. **[manual]** Publish `RoleRadar errors (staging)` first, then
   `RoleRadar (staging)`. The error workflow must be published; n8n 2.35.7
   only runs an error workflow's published version.
5. **Wait for one scheduled execution.** Do not press Execute.
6. **[manual]** Unpublish `RoleRadar (staging)` straight away, so it does not
   keep firing every minute.
7. Run the correlation query.

Expected: one new `pipeline_runs` row, `failed`; one `pipeline_errors` row
with the same `run_id`, `failed_node` = the Stop and Error node's name,
category `unknown` (Stop and Error is not one of the mapped node types).
`/api/pipeline-errors` on the staging API shows the category and node, and
`message: null`. No row in `jobs_extracted` or `dead_letter_log` for that run.

If there is no error row: open the execution in n8n's list. A failed
execution with no error-workflow execution beside it means the error workflow
was not called. Check it is published and selected in the settings.

## S4. Tear down

```
docker stop roleradar-pg-staging
```

The staging database is gone. Delete the two staging workflows and the
staging credential in n8n, or keep them unpublished for next time.

## What was verified about n8n, and how

Read from the source of the installed image, `docker.n8n.io/n8nio/n8n:latest`,
which is n8n **2.35.7**, run offline with no volume:

| claim | where it is in the source |
|---|---|
| The error workflow is not called for manual executions | `execution-lifecycle-hooks.js`: `dispatchesErrorWorkflow = !isManualMode && !suppressErrorWorkflow` |
| The error workflow must be published, and its published version is what runs | `workflow-execution.service.js`, `loadErrorWorkflowData`: no active version logs "is not active and cannot be executed" and returns |
| The data it receives: `execution.{id, url, error, lastNodeExecuted, mode, retryOf}` and `workflow.{id, name}`, or `trigger.{error, mode}` when there is no execution id | `execute-error-workflow.js` |
| The default caller policy is workflows from the same owner | `workflows.config.js`: `callerPolicyDefaultOption = 'workflowsFromSameOwner'` |
| An error workflow's own failure does not call an error workflow | `execute-error-workflow.js`: the `mode === 'error'` guard |
| "Skip on Conflict" appends `ON CONFLICT DO NOTHING` | Postgres node v2 insert, read in Milestone 2 |

Rechecked on 18 November 2026: the image on this machine is still
`docker.n8n.io/n8nio/n8n` 2.35.7, built 21 August 2026, and
`docker-compose.yml` now pins that version instead of `latest`.

Not verified, because it needs a running instance: every expression in this
file, the shape of `execution.error.node`, whether a pinned node counts as
executed for `.isExecuted`, Merge behaviour with an empty input, and what a
retried execution can read. Staging scenarios 1 to 10 check the ones that
matter.
