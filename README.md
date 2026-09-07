# RoleRadar

A scheduled pipeline that pulls remote job postings, uses an LLM to extract
structured fields, validates that output with deterministic code, and routes
failures to a dead-letter table instead of dropping them.

It tracks postings that mention n8n, and it is built with n8n.

![The RoleRadar workflow in the n8n canvas: Schedule Trigger, HTTP Request to
Remotive, Split Out, Limit, Prep, Claude Extract, Validator, and an IF node
branching to jobs_extracted and dead_letter_log.](docs/pipeline.png)

## What it does

The workflow runs end to end. One scheduled run:

1. **Schedule Trigger** fires the run. It runs daily, unattended. Verified
   before the interval was widened: two consecutive scheduled executions ran on
   their own at 15-minute intervals, with nobody pressing anything. The workflow
   is published, so the schedule is live.
2. **HTTP Request** does `GET https://remotive.com/api/remote-jobs?search=<term>`
   and returns one item containing the whole result set.
3. **Split Out** turns the `jobs` array into one item per posting.
4. **Limit** caps the item count. This is the cost control, not a convenience:
   one item is one billed Anthropic call.
5. **Prep** builds the request body and strips HTML out of the description.
6. **Claude Extract** does `POST https://api.anthropic.com/v1/messages` with
   `tool_choice: {"type":"tool","name":"extract_job_fields"}`. Forced tool use,
   so the response conforms to the tool's `input_schema`. Nothing is parsed out
   of free text.
7. **Validator** is a Code node running a copy of `validator/validate.js`.
8. **IF** routes on the validator's `valid` boolean, to `jobs_extracted` or
   `dead_letter_log`.

Both tables are populated, including dead-letter rows. A dead-letter table that
has never been exercised is not evidence of anything.

### What is actually in the warehouse

As of the last check:

```
jobs_extracted   16 rows  -- every observation, across every run
jobs_latest       7 rows  -- one per distinct posting, newest sighting only
dead_letter_log   3 rows  -- all source_job_id 2091105
```

The gap between the first two numbers is the point. `jobs_extracted` is an
immutable log of every observation: 16 sightings. `jobs_latest` resolves current
state by ranking within each `source_job_id` on `ingested_at` and keeping rank 1:
7 distinct postings.

7, not 4. Remotive's feed moves between runs, and the Limit node caps the item
count at 5 after Split Out, so different runs pick up different postings. The
distinct-posting count grows as the feed turns over, independently of how many
items any single run processes.

Nothing was overwritten to get there. Every earlier run's rows are still in the
table, so first-seen date and re-listing history are both recoverable. The
postings-over-time chart needs exactly that, and an upsert would have destroyed
it. Deriving current state on read costs a window function over 16 rows.

An earlier version had a primary key on `source_job_id`, which made the table
upsert-shaped: the second run's inserts were rejected and `jobs_latest` was a
no-op, since `ROW_NUMBER()` over a unique column is always 1. That key has been
replaced with a `BIGSERIAL` on `id`, plus an index on
`(source_job_id, ingested_at DESC)` matching the window's key order.

### The lifecycle query

This is the query the append-only table exists to make answerable, and its real
output:

```sql
SELECT source_job_id,
       COUNT(*)          AS times_seen,
       MIN(ingested_at)  AS first_seen,
       MAX(ingested_at)  AS last_seen
FROM jobs_extracted
GROUP BY source_job_id
ORDER BY times_seen DESC;
```

```
 source_job_id | times_seen |       first_seen        |        last_seen
---------------+------------+-------------------------+-------------------------
 2091098       |          4 | 2026-08-23 14:36:47.951 | 2026-09-02 15:00:37.808
 2091100       |          2 | 2026-09-02 14:45:35.098 | 2026-09-02 15:00:37.806
 2091101       |          2 | 2026-09-02 14:45:35.093 | 2026-09-02 15:00:37.805
 1919266       |          2 | 2026-08-23 14:36:47.951 | 2026-08-23 15:02:44.752
 2091099       |          2 | 2026-09-02 14:45:35.098 | 2026-09-02 15:00:37.806
 2091104       |          2 | 2026-08-23 14:36:47.951 | 2026-08-23 15:02:44.752
 2091097       |          2 | 2026-08-23 14:36:47.951 | 2026-08-23 15:02:44.752
```

Three lifecycle states, from one query over one table:

- **Still open.** 2091098 was first seen on Aug 23 and was still in the feed on
  Sep 2. Ten days open.
- **New.** 2091099, 2091100 and 2091101 have a first_seen of Sep 2. They entered
  the feed between the two dates.
- **Gone.** 1919266, 2091097 and 2091104 were last seen on Aug 23 and have not
  appeared since. They dropped out of the feed.

A MERGE-shaped table would report 7 rows too. It would not report any of this.
Each posting would carry one timestamp, overwritten on every sighting, so
first-seen date and re-listing history would both have been discarded on the
second run.

**One honest caveat.** The pipeline sat idle between Aug 23 and Sep 2, so
`times_seen` is a count of runs that saw the posting, not a measure of how
continuously the posting was listed. 2091098's 4 is four observations across two
clusters of runs ten days apart, not four days of presence. `first_seen` and
`last_seen` are real: those are the timestamps of actual observations. Anything
derived from the count between them is not.

All three dead-letter rows are the same TELUS Digital listing, whose salary the
model has repeatedly got wrong. In the captured fixtures it returned
`salary_min: 0.2333` and `salary_max: 14`, having read "$0.2333 per completed
task" and "$14 per hour" as one hourly range. Two real numbers in different
units, which passes every type and bounds check and is only detectable because
the range spans 60x. In the live runs it returned one bound and not the other,
which fails as `SALARY_PARTIAL`. The 20x plausibility rule that catches the
first case is exercised by the test suite, not by the rows currently in the
table.

## The warehouse is Postgres, not BigQuery

The design in `CLAUDE.md` says BigQuery. The implementation uses Postgres 16 in
Docker, alongside n8n. The switch was forced, not preferred.

BigQuery's Sandbox — the tier available without putting a card on file — blocks
streaming inserts and DML. n8n's BigQuery node writes with `insertAll`, which is
the streaming insert API. So the node cannot write to a Sandbox project at all.
The options were to attach a billing card, hand-build load jobs through an
Execute Query node, or use a warehouse that accepts ordinary inserts. Postgres
in the existing compose file was the smallest change that kept the pipeline
running end to end, and it costs nothing.

What this loses: partitioning on `DATE(ingested_at)` and clustering on
`source_job_id`, both of which were part of the append-only design. At 16 rows,
neither matters. At a scale where they would, Postgres wants an index rather
than a partition, and none is created yet.

What it does not change: the append-only model, the dedupe-on-read view, the
split between the two tables, and the validator contract. Those were warehouse
decisions, not BigQuery decisions.

`CLAUDE.md` and `.env.example` both still describe BigQuery and GCP setup. They
are out of date on this point.

## How to run it

### The validator

Zero npm dependencies. The tests use the built-in `node:test` runner. Nothing
to install.

```
cd validator
node --test
```

74 tests. They run against real captured output from five live pipeline runs in
`validator/fixtures/`, plus the Remotive records those runs came from. Failure
cases are single-line mutations of those fixtures. No test data is invented.

### The pipeline

1. Copy `.env.example` to `.env` and fill it in. `.env` is gitignored. Read the
   comments: n8n does not load that file, and values marked `[n8n UI]` are
   documentation of credentials you create by hand, not config that gets read.
   The file has not been updated for the Postgres switch and still documents
   GCP variables.

2. Generate an encryption key for n8n's credential store. Losing it means
   re-entering every credential.

   ```
   openssl rand -hex 24
   ```

3. Start n8n and Postgres.

   ```
   docker compose up -d
   ```

   n8n listens on `http://localhost:5678`. Postgres on `5432`, database
   `roleradar`, user `roleradar`.

4. Create the tables and the view.

   ```
   docker exec -i roleradar-pg psql -U roleradar -d roleradar < sql/01_jobs_extracted.sql
   docker exec -i roleradar-pg psql -U roleradar -d roleradar < sql/02_dead_letter_log.sql
   docker exec -i roleradar-pg psql -U roleradar -d roleradar < sql/03_jobs_latest_view.sql
   ```

   These match the live database. `sql/_actual_schema.sql` is a `pg_dump` of it,
   kept so the two can be diffed.

5. In the n8n UI, create the credentials: a Header Auth credential for Anthropic
   with header name `x-api-key`, and a Postgres credential pointing at the
   `roleradar` database. From n8n's container the host is `postgres`, not
   `localhost`.

6. Import or rebuild the workflow. `workflows/job-pipeline.json` is an export
   from the canvas, kept for the record.

   `docs/n8n-notes.md` records why each node was chosen and how the fields are
   mapped before insert.

7. Paste the contents of `validator/validate.js` into the Validator Code node
   and call `validate()` over the incoming items. The whole file pastes in
   unchanged: the export on the last line is guarded with
   `typeof module !== 'undefined'`, so it is a no-op inside the Code node.

### Costs

The Anthropic API is the only line item that is not free. No free tier, prepaid
credits, one call per job item. The Limit node is the ceiling on spend per run.
A dev loop that forgets the cap and pulls 500 postings costs real money.

Postgres and n8n are both self-hosted in Docker and cost nothing. The BigQuery
streaming-insert cost noted in `CLAUDE.md` no longer applies.

## Architecture decisions

### Data validation and infra failure are different events

An HTTP 529 from Anthropic is not the same event as a model returning a bad
`seniority` value. Mixing them makes both un-diagnosable. Validation failures
route through the IF node to `dead_letter_log`. Infra failures are meant to be
caught by a separate Error Trigger workflow, which is not built.

### jobs_extracted is append-only, by design

Every run inserts. Nothing is updated or deleted. Current state comes from
`jobs_latest`, which ranks rows per posting with `ROW_NUMBER() OVER (PARTITION
BY source_job_id ORDER BY ingested_at DESC)` in a subquery and keeps rank 1.

A MERGE or upsert would overwrite history. First-seen date and re-listing
history are needed for the postings-over-time chart, and once overwritten they
are gone. An immutable ingestion log keeps the raw record of what was seen when,
and the view derives current state on read. Reads are cheap at this scale; lost
history is not recoverable.

`source_job_id` is deliberately not unique. The primary key is a `BIGSERIAL` on
`id`, and one posting seen on three runs is three rows. The index on
`(source_job_id, ingested_at DESC)` is the key order the view's window needs.

### Source and model output stay separate

```js
validate({ source, extracted }, ingestedAt) -> { valid, errors, record }
// errors: [{ field, code, message }]
```

`source` and `extracted` are never flattened into one object. Flattening loses
the distinction between what Remotive gave us and what the model produced, which
is exactly the distinction the dead-letter table exists to record.

`ingestedAt` is an argument. The validator never calls `Date.now()`. It is a
pure function, so tests are deterministic and there is no clock to stub.

Every check produces a specific `{ field, code, message }`, never a bare
boolean. A dead-letter row has to be diagnosable, and `valid: false` tells you
nothing.

### `extracted` is the whole Anthropic message

Not just the tool input. It is the only place the `model` provenance value
exists, and unwrapping it inside `validate.js` keeps that step in tested code
rather than in the hand-pasted Code node. It also gives the validator somewhere
to check that a `tool_use` block for `extract_job_fields` actually came back — a
`max_tokens` stop returns a truncated message despite forced tool choice, and
that fails as `TOOL_USE_MISSING` rather than as ten confusing field errors.

### mentions_n8n is cross-checked, not trusted

The tool schema defines the field narrowly: true if the posting text contains
the token n8n. Not "the role involves n8n". The narrow definition makes a
mismatch unambiguously model error rather than a defensible difference of
interpretation.

The validator independently matches `/\bn8n\b/i` against the tag-stripped
description. If the model's boolean disagrees, the row goes to `dead_letter_log`
under its own code, `N8N_MISMATCH`, separate from schema failures. This makes
model error rate a measurable number rather than a vibe.

The validator does not silently correct the field. Overwriting would hide the
error rate, which is the point of measuring it.

Both values are stored, as `mentions_n8n_model` and `mentions_n8n_actual`.
Storing only the agreed value would make error rate countable at ingestion time
and nowhere else. With both columns, error rate is a query, and it can be
recomputed historically and sliced by `model` and `extraction_version`. Rows in
`jobs_extracted` are the ones that agreed, so the two are equal there by
construction. The disagreements are the interesting ones.

### The description is not stored with the extracted row

Descriptions are long HTML and would dominate the table's storage for no query
benefit. The design puts the description on the dead-letter row only, where it
is needed to diagnose the failure, alongside the raw LLM output. That column is
currently not being populated — see below.

### The validator has no npm dependencies

Load-bearing, not minimalism. The n8n Code node cannot `require` arbitrary
packages, so `validate.js` has to be one self-contained file. A
dependency-free package means the tests physically cannot pass using something
the Code node would not have. Jest or Vitest would be nicer to write and would
quietly let the tested code drift away from the deployed code.

## The dashboard

A FastAPI service in `api/` reads Postgres and serves four metric endpoints.
`api/static/index.html` is a single self-contained file — Chart.js from a CDN,
inline CSS and JS, no build step — that renders them. It can be served by
FastAPI for local work, or copied onto an nginx static root and pointed at the
API somewhere else. It holds no credentials and talks only to the API.

Aggregation happens in SQL. The browser receives rows that are already the
answer, never raw rows to add up. Every number on the page traces to one query
in `sql/queries.sql`, commented with the metric it serves.

### What a run is

A run is one n8n execution.

The design intent was that all rows in a run share an `ingested_at`, so
grouping would be `GROUP BY ingested_at`. That does not hold. The Validator
Code node calls `new Date().toISOString()` **inside** its per-item loop:

```js
for (let i = 0; i < $input.all().length; i++) {
  const result = validate({ ... }, new Date().toISOString());
```

Each item reads the clock separately. Five items in a tight loop usually land on
the same millisecond and are not guaranteed to. One straddled millisecond splits
a run in two, turning a 4/5 success rate into a 3/3 and a 1/2 — which does not
look like an error, it looks like a good run and a bad one.

So runs are recovered by gap rather than by equality. Order every row from both
tables by time, and start a new run wherever the gap from the previous row is
over two minutes. A run takes seconds; the schedule is daily. `LAG` finds the
boundaries, a running `SUM` turns them into a run number.

Moving the clock read above the loop would fix this at the source and is worth
doing. It would not fix the rows already written, so the gap logic stays either
way.

### Definitions

| term | definition |
|---|---|
| **run** | A gap-delimited cluster of rows across both tables, two-minute threshold. `run_started_at` is the earliest timestamp in the cluster. |
| **ingested** | Rows in `jobs_extracted` plus rows in `dead_letter_log` for that run. |
| **validated** | Rows in `jobs_extracted`. |
| **rejected** | Rows in `dead_letter_log`. |
| **success rate** | `validated / ingested` for the run, as a fraction 0–1. Formatted to a percentage in one place in the page. |
| **successful run** | A run with `validated > 0`. See below. |
| **mismatch** | A `dead_letter_log` row carrying `N8N_MISMATCH`. |

**"Ingested" means processed, not available.** It counts what the pipeline put
through the model, not what Remotive had. The Limit node caps each run at
`DEV_JOB_LIMIT`, currently 5, and a search for "data engineer" returns hundreds.
So an ingestion volume chart that sits flat at 5 is showing the cost control
working, not the feed drying up. The number to watch is when it drops *below*
the cap.

The cap is a current setting, not a property of the history. The run of
2026-08-23 15:02 ingested 7, because it predates the present value. Reading the
volume chart as "the cap has always been 5" would be wrong.

**"Successful" means the run produced data**, not that it was clean. A run with
four validated rows and one rejection counts as successful for the purpose of
"time since last successful run". The stricter reading — zero rejections — is
arguably more honest, but at five rows per run a fully clean run is not
guaranteed to be common, and a tile that reads "never" on a working pipeline
teaches you to ignore it. The tile is labelled "since last run that produced
data" so the weaker meaning is stated rather than implied.

**Run status** is `ok` when nothing was rejected, `failed` when the run
processed rows and none validated, and `degraded` in between.

**Current success rate** is the last run's, as the headline. The lifetime rate
and the run count sit under it in smaller text. At five rows per run the last
run's number is volatile by construction, so the steadier one stays visible next
to it rather than replacing it.

### The error-class chart counts rows, not code occurrences

`dead_letter_log.error_codes` is a JSONB array and a row can carry several
codes. Counting rows and counting code occurrences give different answers, and
the chart has to say which it is doing.

- **rows** — how many dead-lettered postings carry this code at all. A row with
  three bad enums counts once.
- **occurrences** — how many entries in `errors_json` carry this code. That same
  row counts three times.

The bar chart plots **rows** by default. "How many postings did this break" is
the triage question, and counting occurrences lets one badly-mangled row
dominate the chart. Both numbers are in the API response and the page has a
toggle, so the difference is visible in the data rather than only described
here.

Categories come from `errors_json`, not `error_codes`. `error_codes` is deduped
to distinct codes per row and drops the field name, and `MISSING` is emitted for
both a missing `source_job_id` and a missing `normalized_title`. Those are not
the same failure — a missing `source_job_id` is a pipeline fault and should not
inflate the model error rate — and only the field name separates them.

| category | codes |
|---|---|
| `provenance` | `MISSING` on `source`, `source_job_id`, `source_url`, `raw_title`, `raw_company`, `description_clean` |
| `envelope` | `TOOL_USE_MISSING` |
| `schema` | `TYPE`, `ENUM`, `EMPTY`, `RANGE`, `ARRAY_ITEM`, `ARRAY_LEN`, `UNKNOWN_FIELD`, and `MISSING` on extracted fields |
| `salary_logic` | `SALARY_ORDER`, `SALARY_PARTIAL`, `SALARY_PERIOD_MISMATCH`, `SALARY_SPAN_IMPLAUSIBLE` |
| `model_disagreement` | `N8N_MISMATCH` |

A code appearing under two categories returns two bars. That is the point, not a
grouping bug.

### Model disagreement rate

The denominator is everything the model processed, both tables, not just the
rejected rows. Over rejections alone a run with one rejection reads as a 100%
model error rate.

The number is a **floor**. The validator only runs the `mentions_n8n`
cross-check when that field passed its own boolean type check, so a row that
failed schema validation and would also have disagreed does not always carry the
code. It undercounts and never overcounts.

### What the dashboard cannot see

A run that died before either Postgres node wrote anything leaves no rows in
either table, so it is not a run here at all. The Error Trigger workflow does
not write to Postgres. This dashboard measures data quality, not pipeline
liveness, and the recent-runs table will look clean through an outage.

"Time since last successful run" going stale is the only signal on the page that
the pipeline stopped. That is thin, and it is the reason the tile is in the
summary strip rather than buried.

### Endpoints

Fractions are 0–1. Timestamps are naive UTC, ISO-8601. Every list endpoint
returns `row_count`, so an empty result is explicit rather than an empty array
that renders as zero.

| endpoint | serves | shape |
|---|---|---|
| `GET /api/health` | the API-reachable vs no-data distinction | `{database, checked_at}`, 503 with `error` when the database is unreachable |
| `GET /api/runs?limit=30` | volume chart, success-rate chart, runs table | `{generated_at, run_gap_minutes, row_count, runs[]}` |
| `GET /api/summary` | the summary strip | `{generated_at, run_count, totals, last_run{}, last_successful_run_at, seconds_since_last_successful_run}` |
| `GET /api/error-classes` | the error-class bar chart | `{generated_at, row_count, default_metric, total_rejected_rows, total_code_occurrences, classes[]}` |
| `GET /api/model-accuracy` | the model-disagreement panel | `{generated_at, row_count, by_version[], by_run[]}` |

A `runs[]` entry:

```json
{
  "run_started_at": "2026-09-05T03:00:01.412000",
  "run_ended_at":   "2026-09-05T03:00:01.907000",
  "ingested": 5, "validated": 4, "rejected": 1,
  "success_rate": 0.8,
  "success_rate_delta": -0.2,
  "success_rate_rolling3": 0.8667,
  "cumulative_ingested": 16,
  "cumulative_validated": 13,
  "status": "degraded"
}
```

`success_rate_delta` is null on the first run. The limit is applied after the
window functions, so `cumulative_ingested` stays a lifetime total and the delta
on the oldest visible run still refers to the run before it.

A `classes[]` entry:

```json
{ "code": "N8N_MISMATCH", "category": "model_disagreement",
  "rows": 2, "occurrences": 2, "pct_of_rejected_rows": 0.6667 }
```

### Failure states

Each panel is in one of four states and shows which: loading, error, empty, or
data. An endpoint that is unreachable or returns a non-2xx paints the panel with
the error, not with an empty chart. A query that returns no rows paints "no runs
recorded" over the plot area. An empty chart never looks like zero.

### Running it

The API reads connection details from the environment and has no credentials in
the file. `.env` currently sets only `POSTGRES_PASSWORD`; the rest fall back to
the values in `docker-compose.yml`.

```
POSTGRES_HOST=localhost   POSTGRES_PORT=5432
POSTGRES_DB=roleradar     POSTGRES_USER=roleradar
POSTGRES_PASSWORD=...     API_PORT=8000
```

`DATABASE_URL` overrides all of them if set.

```sh
docker compose up -d
python -m venv api/.venv && source api/.venv/bin/activate
pip install -r api/requirements.txt
set -a && source .env && set +a
uvicorn main:app --app-dir api --port 8000
```

Then open `http://localhost:8000/`, which serves `api/static/index.html`.

To serve the page from nginx instead, copy that one file to the static root and
set `API_BASE` at the top of its script block to wherever the API is. CORS is
open on the API for exactly this case.

## Known limitations

These are real weaknesses, not details.

**The schema is documented, not managed.** The tables were created by hand in
`psql`. The files in `sql/` describe the live database rather than build it, and
nothing enforces that the two stay in sync — `sql/03_jobs_latest_view.sql` had
to be applied manually after the fact. The current files do match the live
schema, column for column and index for index, but that is a fact about today,
verified by hand, not a guarantee. A migration tool would close the gap, and
would also give the primary-key change above a recorded, replayable history
instead of an `ALTER TABLE` someone ran once.

**The schedule only runs while the container does.** n8n holds the cron
in-process, so a daily trigger fires only if `docker compose up` is still
running at the time. Uptime is the laptop's uptime. Close the lid overnight and
that day's run does not happen and is not retried — there is no catch-up on
start. This is a consequence of self-hosting, not a bug, but it means gaps in
`jobs_extracted` are gaps in observation, not evidence about the feed.

**`validate.js` is hand-pasted into the n8n Code node.** The file in this repo
is the source of truth for the logic. The copy running in the canvas is a
deployed duplicate. There is no import path from the canvas into this repo, so
the two can drift. When the validator changes, the Code node has to be
re-pasted, and nothing enforces that. Passing tests here are not evidence that
the deployed copy matches.

**Every error is fatal to the row.** A single bad salary discards a perfectly
good title, company, and tech stack. Production would need severity levels, so a
plausibility failure nulls one field while the rest of the row still lands. The
current design is all-or-nothing because it is easier to reason about and
because `dead_letter_log` keeps the raw output, so nothing is lost — but it does
mean `jobs_extracted` under-counts.

**The validator is coupled to Anthropic's response shape by design.** It reaches
into `extracted.content[]` looking for a `tool_use` block. Changing model
providers means changing the validator, not just a credential. That coupling
buys the `model` provenance value and the `TOOL_USE_MISSING` check, and it was
chosen deliberately, but it is coupling.

**Fixtures pair positionally.** `fixtures/extraction-output.json` carries no
`source_job_id`, so the only join key between it and
`fixtures/source-records.json` is array index. Reordering or partially
re-capturing one file would silently validate one job's extraction against
another job's description. The tests assert the two arrays are the same length
so a reorder fails loudly, but equal length is not the same as correct
alignment.

**`UNKNOWN_FIELD` makes tool-schema changes a breaking change.** Any key in the
model's output not in the validator's field list dead-letters the row. So adding
a field to the tool schema dead-letters every row until the Code node is
re-pasted with a validator that knows about it. This is deliberate — a loud
failure beats a field silently vanishing — but schema changes have to be
deployed in the right order, and the wrong order costs a run.

**`tech_stack` and `error_codes` are `jsonb`, not `text[]`.** n8n's Postgres
node maps a JS array to json, not to a native Postgres array type, so a
`text[]` column rejects the insert. The cost is that the natural array
operations are not available: containment is `error_codes @> '["CODE"]'` rather
than `'CODE' = ANY(error_codes)`, and `tech_stack` needs
`jsonb_array_elements_text` to unnest for a tech-frequency chart. Either column
would be a native array if the write path allowed it.

**`dead_letter_log.description` is empty in every row, and the fix is
unverified.** The validator emits the field as `description_clean`; the column
is named `description`. An Edit Fields node now maps one to the other — see
`docs/n8n-notes.md` — but every row currently in the table predates it and still
has `description IS NULL`. Nothing has failed validation since, so no row
demonstrates the mapping working. Renaming one side so they match outright would
be cleaner than a mapping node.

**`errors_json` is double-encoded.** The validator stringifies it before
insert, so it lands in the `jsonb` column as a JSON *string* rather than an
array. Querying it means unwrapping with `#>> '{}'` and then parsing a second
time. `error_codes` and `raw_output` land as a proper array and object, so the
three are inconsistent with each other.

**`ingested_at` has two different types.** `timestamp` in `jobs_extracted`,
`timestamptz` in `dead_letter_log`. The same ISO-8601 string is written to both
and one drops the offset. Joining or comparing the two tables on time will not
behave until they agree.

**`EXTRACTION_VERSION` is defined in two places and they disagree.**
`validate.js` hardcodes `'1.0.0'`, which is what reaches the rows.
`.env.example` documents `EXTRACTION_VERSION=v1`, which is not wired to
anything.

**The dashboard SQL is duplicated into `api/main.py`.** `sql/queries.sql` is
the readable source of truth, with the reasoning in its comments. `main.py`
holds the same statements again as Python constants so the app has no file to
read at startup. This is the same hand-pasted-duplicate problem `validate.js`
has with the n8n Code node, at smaller scale, and nothing enforces that the two
stay in sync. Loading the `.sql` file at import and splitting it would remove
the duplication, at the cost of a fragile split on `;`.

**`dead_letter_log` has no index beyond its primary key.** It is most usefully
queried by `ingested_at` and by `error_codes` containment, and neither is
indexed. `jobs_extracted` has the index its view needs. At this row count none
of it matters. It would.

## Not yet built

- **The Remotive `publication_date` column.** It is in the source records and
  it belongs in `jobs_extracted`, because first-posted and first-seen are
  different dates. The validator does not emit it yet.
- **The Error Trigger workflow.** Infra failures — timeouts, non-2xx responses,
  API outages — are uncaught. The design calls for a separate n8n Error Trigger
  workflow, kept distinct from data validation failures.
