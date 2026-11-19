# RoleRadar

A scheduled pipeline that pulls remote job postings, uses an LLM to extract
structured fields, validates that output with deterministic code, and routes
failures to a dead-letter table instead of dropping them.

It tracks postings that mention n8n, and it is built with n8n.

![The RoleRadar workflow in the n8n canvas: Schedule Trigger, HTTP Request to
Remotive, Split Out, Limit, Prep, Claude Extract, Validator, and an IF node
branching to jobs_extracted and dead_letter_log.](docs/pipeline.png)

**Live dashboard: https://roleradar-snowy.vercel.app**

The dashboard and the warehouse are hosted. The pipeline above is not — n8n runs
in Docker on a laptop, so ingestion happens only while that machine is up. The
live URL is not evidence of continuous operation. See [Deployment](#deployment).

## Status

Two versions of this project exist, and this README describes both. Read this
table before anything else.

| | live today | built and tested in this repository, not released |
|---|---|---|
| Extraction | n8n workflow: Remotive → Claude with forced tool use → `validate()` in a Code node → IF → two inserts | `validateBatch()`: one call per run, one timestamp per run, a `run_id` on every row |
| Warehouse | Neon Postgres 18. Three tables and a view, created by hand. 25 observations, 6 dead letters, last write 11 September 2026 | Nine versioned, reversible migrations with a checksum ledger and a runner that refuses remote targets without confirmation |
| Run tracking | none. Runs are inferred from timestamp gaps | `pipeline_runs`, closed by a database function that refuses to mark a run complete until the counts and the rows agree |
| Failure tracking | none. A run that dies leaves no trace | a database function for an n8n Error Trigger workflow, correlating failures to runs without trusting execution ids alone |
| API | five read-only endpoints on Vercel | the same five unchanged, plus three monitoring endpoints, a public/operator split for diagnostics, and connections that cannot write |
| Dashboard | data quality: volume, success rate, rejection reasons, model disagreement | plus pipeline health, execution history, duration and infrastructure errors |
| Monitoring | none | an external freshness check for GitHub Actions, inert until configured |
| Tests | 149 validator tests | 523: validator, database (on Postgres 16 and 18), API, monitor, dashboard |

Nothing in the right-hand column has run against Neon, Vercel or the real
n8n workflow. The release is a manual, eighteen-step procedure in
[docs/release-runbook.md](docs/release-runbook.md), and the n8n changes are
written as instructions in [docs/n8n-notes.md](docs/n8n-notes.md) because the
workflow is built by hand. Where this README says "the function guarantees" or
"the API returns", it means in tests against a disposable database.

## Contents

- [Status](#status)
- [What it does](#what-it-does)
  - [What is actually in the warehouse](#what-is-actually-in-the-warehouse)
  - [The lifecycle query](#the-lifecycle-query)
- [The warehouse is Postgres, not BigQuery](#the-warehouse-is-postgres-not-bigquery)
- [How to run it](#how-to-run-it)
  - [The validator](#the-validator)
  - [The pipeline](#the-pipeline)
  - [Migrations](#migrations)
  - [Tests](#tests)
  - [The API and dashboard](#the-api-and-dashboard)
  - [Costs](#costs)
- [Architecture decisions](#architecture-decisions)
  - [Data validation and infra failure are different events](#data-validation-and-infra-failure-are-different-events)
  - [jobs_extracted is append-only, by design](#jobs_extracted-is-append-only-by-design)
  - [Source and model output stay separate](#source-and-model-output-stay-separate)
  - [One batch call per run](#one-batch-call-per-run)
  - [`extracted` is the whole Anthropic message](#extracted-is-the-whole-anthropic-message)
  - [mentions_n8n is cross-checked, not trusted](#mentions_n8n-is-cross-checked-not-trusted)
  - [The description is not stored with the extracted row](#the-description-is-not-stored-with-the-extracted-row)
  - [The validator has no npm dependencies](#the-validator-has-no-npm-dependencies)
- [The dashboard](#the-dashboard)
  - [What a run is](#what-a-run-is)
  - [Definitions](#definitions)
  - [The error-class chart counts rows, not code occurrences](#the-error-class-chart-counts-rows-not-code-occurrences)
  - [Model disagreement rate](#model-disagreement-rate)
  - [What the dashboard cannot see](#what-the-dashboard-cannot-see)
  - [Endpoints](#endpoints)
  - [Pipeline health](#pipeline-health)
  - [What the dashboard shows](#what-the-dashboard-shows)
  - [Security boundaries](#security-boundaries)
  - [Mixed historical and new data](#mixed-historical-and-new-data)
  - [What can go wrong, and where each kind is counted](#what-can-go-wrong-and-where-each-kind-is-counted)
  - [Failure states](#failure-states)
  - [Pooled connections do not carry a search_path](#pooled-connections-do-not-carry-a-search_path)
- [Deployment](#deployment)
  - [What is hosted and what is not](#what-is-hosted-and-what-is-not)
  - [Configuration](#configuration)
  - [External freshness check](#external-freshness-check)
  - [Releasing this](#releasing-this)
  - [Rolling back](#rolling-back)
- [Known limitations](#known-limitations)
  - [Correctness risks](#correctness-risks)
  - [Operational gaps](#operational-gaps)
  - [Accepted trade-offs](#accepted-trade-offs)
  - [Known bugs](#known-bugs)
- [Not yet built](#not-yet-built)
  - [1. The Error Trigger workflow](#1-the-error-trigger-workflow)
  - [2. The Remotive `publication_date` column](#2-the-remotive-publication_date-column)
  - [3. Explicit runs](#3-explicit-runs)

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

Counted on Neon, the live warehouse, by a read-only query in November 2026:

```
jobs_extracted   25 rows  -- every observation, across every run
jobs_latest      12 rows  -- one per distinct posting, newest sighting only
dead_letter_log   6 rows  -- all source_job_id 2091105
```

The last write was on 11 September 2026. The local Docker container was the
source of the move to Neon and has not been compared with it since.

The analysis in the rest of this section and the next was written on
3 September, when the counts were 20, 7 and 6. The reasoning holds; the
numbers in it are that day's.

The gap between the first two numbers is the point. `jobs_extracted` is an
immutable log of every observation: on 3 September, 20 sightings. `jobs_latest` resolves current
state by ranking within each `source_job_id` on `ingested_at` and keeping rank 1:
7 distinct postings.

7, not 4. Remotive's feed moves between runs, and the Limit node caps the item
count at 5 after Split Out, so different runs pick up different postings. The
distinct-posting count grows as the feed turns over, independently of how many
items any single run processes.

Nothing was overwritten to get there. Every earlier run's rows are still in the
table, so first-seen date and re-listing history are both recoverable. The
postings-over-time chart needs exactly that, and an upsert would have destroyed
it. Deriving current state on read costs a window function over 20 rows.

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
 2091098       |          5 | 2026-08-23 14:36:47.951 | 2026-09-03 15:03:56.865
 2091099       |          3 | 2026-09-02 14:45:35.098 | 2026-09-03 15:03:56.862
 2091100       |          3 | 2026-09-02 14:45:35.098 | 2026-09-03 15:03:56.862
 2091101       |          3 | 2026-09-02 14:45:35.093 | 2026-09-03 15:03:56.86
 1919266       |          2 | 2026-08-23 14:36:47.951 | 2026-08-23 15:02:44.752
 2091097       |          2 | 2026-08-23 14:36:47.951 | 2026-08-23 15:02:44.752
 2091104       |          2 | 2026-08-23 14:36:47.951 | 2026-08-23 15:02:44.752
```

Run against Neon on 3 September 2026, when the table held 20 rows.

Three lifecycle states, from one query over one table:

- **Still open.** 2091098 was first seen on Aug 23 and was still in the feed on
  Sep 3. Eleven days open, and the only posting present in all five runs.
- **New.** 2091099, 2091100 and 2091101 have a first_seen of Sep 2. They entered
  the feed after Aug 23 and were still there on Sep 3.
- **Gone.** 1919266, 2091097 and 2091104 were last seen on Aug 23 and have not
  appeared since. They dropped out of the feed.

A MERGE-shaped table would report 7 rows too. It would not report any of this.
Each posting would carry one timestamp, overwritten on every sighting, so
first-seen date and re-listing history would both have been discarded on the
second run.

**One honest caveat.** The pipeline sat idle between Aug 23 and Sep 2, so
`times_seen` is a count of runs that saw the posting, not a measure of how
continuously the posting was listed. 2091098's 5 is five observations across
three clusters of runs — two on Aug 23, two on Sep 2, one on Sep 3 — not five
days of presence. `first_seen` and `last_seen` are real: those are the
timestamps of actual observations. Anything derived from the count between them
is not.

All six dead-letter rows are the same TELUS Digital listing, whose salary the
model has repeatedly got wrong. It returned `salary_min: 0.2333` and
`salary_max: 14`, having read "$0.2333 per completed task" and "$14 per hour" as
one hourly range. Two real numbers in different units, which passes every type
and bounds check and is only detectable because the range spans 60x. On other
runs it returned one bound and not the other, which fails as `SALARY_PARTIAL`.

Five of the six rows are `SALARY_PARTIAL` and one is `SALARY_SPAN_IMPLAUSIBLE`,
so the 20x plausibility rule is now exercised by a real row in the table and not
only by the test suite. One posting, two distinct failure modes, both caught.

## The warehouse is Postgres, not BigQuery

The design in `CLAUDE.md` says BigQuery. The implementation uses Postgres. The
switch was forced, not preferred.

It started as Postgres 16 in Docker alongside n8n, and now runs on **Neon**
(serverless Postgres 18) so the hosted dashboard can reach it — a Vercel
function cannot connect to a container on a laptop. The local container is kept
as the pre-migration snapshot and the rollback path; it holds an identical copy
of every row, verified by matching MD5 over the full table.

Only the host changed. Neon is ordinary Postgres, so the append-only model, the
dedupe-on-read view and the validator contract all carried over untouched, and
the `pg_dump` restored into PG 18 without a single compatibility fix. The one
thing the move did change is documented in
[Pooled connections do not carry a search_path](#pooled-connections-do-not-carry-a-search_path).

BigQuery's Sandbox — the tier available without putting a card on file — blocks
streaming inserts and DML. n8n's BigQuery node writes with `insertAll`, which is
the streaming insert API. So the node cannot write to a Sandbox project at all.
The options were to attach a billing card, hand-build load jobs through an
Execute Query node, or use a warehouse that accepts ordinary inserts. Postgres
in the existing compose file was the smallest change that kept the pipeline
running end to end, and it costs nothing.

What this loses: partitioning on `DATE(ingested_at)` and clustering on
`source_job_id`, both of which were part of the append-only design. At 20 rows,
neither matters. At a scale where they would, Postgres wants an index rather
than a partition, and none is created yet.

What it does not change: the append-only model, the dedupe-on-read view, the
split between the two tables, and the validator contract. Those were warehouse
decisions, not BigQuery decisions.

`CLAUDE.md` still describes BigQuery and GCP setup, and still says the warehouse
is Postgres in Docker. It is out of date on both points.

## How to run it

### The validator

Zero npm dependencies. The tests use the built-in `node:test` runner. Nothing
to install.

```
cd validator
node --test
```

149 tests in two files. `validate.test.js` is the original 74 and covers
`validate()`, which judges one posting. `validateBatch.test.js` is 75 more and
covers `validateBatch()`, which runs a whole pipeline run through it.

They run against real captured output from five live pipeline runs in
`validator/fixtures/`, plus the Remotive records those runs came from. Failure
cases are single-line mutations of those fixtures. No test data is invented, and
no test calls Anthropic or Remotive.

### The pipeline

1. Copy `.env.example` to `.env` and fill it in. `.env` is gitignored. Read the
   comments: n8n does not load that file, and values marked `[n8n UI]` are
   documentation of credentials you create by hand, not config that gets read.
   The file still documents unused GCP variables.

   `NEON_DATABASE_URL` is the hosted warehouse. Leave it empty to run entirely
   against the local container; set it to point the API and the pipeline at
   Neon. Use the **pooled** connection string — the host contains `-pooler` —
   and keep `?sslmode=require`, because Neon refuses unencrypted connections.

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

4. Create the tables and the view, by applying the migrations to the local
   container.

   ```
   export MIGRATE_DATABASE_URL="postgresql://roleradar:<POSTGRES_PASSWORD>@localhost:5432/roleradar"
   python sql/migrate.py up
   ```

   See [Migrations](#migrations) for what this does, how to roll it back, and
   why it refuses to run against Neon unless told to twice.

5. In the n8n UI, create the credentials: a Header Auth credential for Anthropic
   with header name `x-api-key`, and a Postgres credential.

   For the local container: host `postgres` (not `localhost` — that is n8n's own
   container), database `roleradar`, user `roleradar`, SSL disabled.

   For Neon: host, database, user and password from `NEON_DATABASE_URL`, port
   `5432`, and **SSL set to `require`**. The database is `neondb`, not
   `roleradar`. Set the node's schema field to `public` explicitly rather than
   relying on `search_path`; see
   [Pooled connections do not carry a search_path](#pooled-connections-do-not-carry-a-search_path).

6. Import or rebuild the workflow. `workflows/job-pipeline.json` is an export
   from the canvas, kept for the record.

   `docs/n8n-notes.md` records why each node was chosen and how the fields are
   mapped before insert.

7. Paste the contents of `validator/validate.js` into the Validator Code node,
   followed by the short wrapper that calls it. The whole file pastes in
   unchanged: the export on the last line is guarded with
   `typeof module !== 'undefined'`, so it is a no-op inside the Code node.

   Which wrapper depends on which workflow you are building. The one deployed
   today loops over the items and calls `validate()`. The run-aware one calls
   `validateBatch()` once. `docs/n8n-notes.md` has both, and the second comes
   with node-by-node instructions.

### Migrations

The schema lives in `sql/migrations/` as numbered SQL files, applied in order by
`sql/migrate.py`. Each applied file is recorded in `public.schema_migrations`
with a checksum.

```
python sql/migrate.py status          # what is applied, what is pending
python sql/migrate.py up              # apply everything pending
python sql/migrate.py up --to 0004    # stop after 0004
python sql/migrate.py down --to 0003  # revert everything newer than 0003
```

The runner needs `psycopg`, which `api/requirements.txt` already installs.

| file | what it does |
|---|---|
| `0001_baseline` | The three tables and the view as they stood before migrations. Changes nothing on a database that already has them, then checks their shape and refuses to continue if it is wrong. |
| `0002_pipeline_runs` | New table, one row per n8n execution: `run_id`, `n8n_execution_id`, start and completion times, status, four counts, error category and message. |
| `0003_run_correlation` | Nullable `run_id` on `jobs_extracted`, `dead_letter_log` and `pipeline_errors`, plus `pipeline_errors.error_category`. |
| `0004_run_uniqueness` | One posting per run: a partial unique index on `(run_id, source_job_id)` where `run_id` is set. |
| `0005_publication_date` | `publication_date` on both data tables. |
| `0006_dead_letter_description_clean` | `dead_letter_log.description_clean`, the name the validator emits. The old `description` column stays. |
| `0007_jobs_latest_columns` | `jobs_latest` gains `publication_date` and `run_id` as its last two columns. |
| `0008_workflow_identity` | `pipeline_runs.n8n_workflow_id` and `pipeline_errors.workflow_id`. An execution id alone is not a key. |
| `0009_run_lifecycle_functions` | Two functions: `complete_pipeline_run`, which closes a run only after finding its rows, and `record_pipeline_error`, which records a failure and marks its run failed. |

Every one of these is additive. No existing column changes type, no row is
updated, and rows written before the migration keep `run_id` NULL. `0009` adds
no column at all, only the two functions; they change rows in `pipeline_runs`
and `pipeline_errors` when the workflow calls them, and never touch
`jobs_extracted` or `dead_letter_log`. They are not
backfilled: assigning a run to a row after the fact would record a guess as a
fact.

**How it avoids the wrong database.** The target comes from `--database-url` or
`MIGRATE_DATABASE_URL`, and from nowhere else. `DATABASE_URL` and
`NEON_DATABASE_URL` are never read, so a variable left exported in the shell
cannot aim a migration at the hosted warehouse. A host that is not this machine
is refused unless you pass both `--allow-remote` and `--confirm-host <hostname>`
with the exact hostname from the connection string.

**What it guarantees.** Each file runs in one transaction together with its
ledger row, so it applies completely or not at all. An applied file that has
since been edited is refused, by checksum. So is a new file numbered below one
that is already applied. Migration files must not contain `BEGIN` or `COMMIT`.

**Rollback.** Every migration except the baseline has a `.down.sql` beside it.
`down --to NNNN` reverts the newer ones, newest first. Reverting drops the
columns a migration added, and the values in them; it never deletes a row from
`jobs_extracted` or `dead_letter_log`. The baseline has no down file on purpose,
because reverting it would mean dropping the warehouse.

**Order of deployment matters.** Apply the migrations before changing the n8n
workflow. The Postgres nodes map input fields to columns by name, so a workflow
that sends `run_id` to a table without that column fails the insert. The other
order is safe: every new column is nullable, so the workflow as it runs today
keeps working against a migrated database. The tests check that.

**Neon has not been migrated.** To do it, when you decide to:

```
python sql/migrate.py --database-url "$NEON_DIRECT_URL" \
    --allow-remote --confirm-host <the host in that URL> status
```

Then the same with `up`. Use Neon's **direct** connection string for this, not
the pooled one: migrations take locks, and the pooled endpoint is a transaction
pooler. Take a `pg_dump` first. Run `status` before `up` and read what it says.

One migration is deliberately missing. `jobs_extracted.ingested_at` is still
`timestamp` and `dead_letter_log.ingested_at` is still `timestamptz`. Changing
the first rewrites the table and depends on the old values really being UTC, so
it waits until that is confirmed. See [Known limitations](#known-limitations).

### Tests

Three suites. None calls Anthropic or Remotive, and none touches Neon, Vercel,
n8n or the local warehouse.

```
cd validator && node --test
```

149 tests: the validator on its own.

```
node --test tests/frontend/*.test.js
```

45 tests: the dashboard. No browser and no dependencies.

| file | tests | what it checks |
|---|---|---|
| `pipeline.test.js` | 32 | The view functions in `api/static/pipeline.js`: status mapping, tiles, tables, formatting, escaping, and that a missing value is a dash and never a zero or a success. |
| `page.test.js` | 13 | The page's own inline script, unmodified, run against a stub DOM and canned API responses. Every panel lands in the right state; one failing endpoint does not blank the others; every element the script asks for exists. |

The second file is not a browser. It proves the script runs and what it writes
into the page. It cannot prove the page looks right.

```
docker compose -f docker-compose.test.yml up -d --wait
python -m venv .venv-test && source .venv-test/bin/activate
pip install -r tests/requirements.txt
python -m pytest tests
docker compose -f docker-compose.test.yml down
```

329 tests, in seven files:

| file | tests | what it checks |
|---|---|---|
| `test_migrations.py` | 43 | What the migrations do to a database shaped like the live one, and what they leave alone. |
| `test_migrate_runner.py` | 31 | The runner: ordering, atomicity, refusing a remote target. |
| `test_validator_records.py` | 27 | The join between the other two. It runs the real `validateBatch()` under Node and inserts what it returns into the migrated schema. Needs `node` on the PATH. |
| `test_run_lifecycle.py` | 61 | How a run ends. The two functions from `0009`, called through the SQL in `docs/n8n-notes.md`: closing, every way a close is refused, failing, duplicates, recycled execution ids, interruption and retry, and two callers at once on real connections. |
| `test_deployment_rehearsal.py` | 34 | The release, rehearsed. Fresh and legacy installs, the deployed API on the new schema, the new API on the old one, each documented rollback, the preflight script, and that no migration removes or rewrites data. |
| `test_api.py` | 105 | Every endpoint through FastAPI's test client against a real Postgres: the health rules, what is public and what needs the operator token, 503 bodies, pagination limits, and the old queries run beside the new ones on the same rows. |
| `test_freshness_check.py` | 28 | The external check. No database, no network. |

By area: 196 database tests (the first five files), 105 API, 28 monitoring.
With the validator and the dashboard, 523 in all.

The compose file starts Postgres 16, which is what the local warehouse runs.
Neon runs Postgres 18. To run the same suite against 18, start a second
disposable server and point the tests at it:

```
docker run -d --rm --name roleradar-pg18-test -p 127.0.0.1:5434:5432 \
  -e POSTGRES_USER=roleradar_test -e POSTGRES_PASSWORD=roleradar_test \
  -e POSTGRES_DB=roleradar_test --tmpfs /var/lib/postgresql postgres:18-alpine
TEST_DATABASE_URL=postgresql://roleradar_test:roleradar_test@127.0.0.1:5434/roleradar_test \
  python -m pytest tests
docker stop roleradar-pg18-test
```

The tmpfs path is `/var/lib/postgresql`, not `.../data`: Postgres 18 images
keep their data one level up, and the wrong path would leave it on disk.
On 18 November 2026 all 329 passed on Postgres 16 and on Postgres 18.7.

No test can reach a real database by accident. `tests/conftest.py` removes
`NEON_DATABASE_URL`, `MIGRATE_DATABASE_URL` and every `POSTGRES_*` variable
before any test module loads, and points `DATABASE_URL` at a port nothing
listens on. The test server itself must be on this machine and not on 5432,
or the suite refuses to start.

`.github/workflows/tests.yml` runs all three suites on every push and pull
request, the database suite twice, on Postgres 16 and 18. It uses no secrets.
**It has never run**: nothing has been pushed. Until it has, there is no CI
result to report.

That third file is the only place the validator and the schema meet before
production. It inserts the records the way n8n's Postgres node does, following
four rules read from the node's source in n8n 2.35.7: every field must be a
column, NULL may not go to a NOT NULL column, values are sent untyped, and
"Skip on Conflict" appends `ON CONFLICT DO NOTHING`. The two SQL statements the
new nodes run are copied out of `docs/n8n-notes.md` by the test, so the
instructions and the tests cannot drift apart.

It is an emulation of n8n, not n8n. It proves the records fit the columns and
that a replay within a run writes nothing. It does not prove the canvas is wired
the way the instructions say, and no test here can.

The same limit applies to failures. Every "failed run" in these tests is a row
the test wrote by calling `record_pipeline_error()` itself. That shows what the
database and the API do once such a row exists. It is not evidence that n8n
calls the Error Trigger workflow, which can only be shown in n8n, by a real
failed execution. The steps for that are in `docs/n8n-notes.md`, E5.

`tests/fixtures/api_main_at_ce0f286.py` is the whole API as last committed,
frozen. The rehearsal runs it against a migrated database, which is how "the
deployed API keeps working after the migrations" is a test and not a hope.

`tests/fixtures/queries_at_ce0f286.py` is the dashboard SQL as it was last
committed, frozen. The compatibility tests run it beside the current SQL on the
legacy rows and require identical answers. A last group of tests checks that
`sql/queries.sql` contains, whitespace aside, the exact text `api/main.py`
runs, so the readable copy cannot drift from the deployed one unnoticed.

The second suite needs a Postgres to run against. `docker-compose.test.yml`
starts a Postgres 16 on `127.0.0.1:5433` with its data directory on tmpfs: no
volume, nothing survives the container. It is a separate compose project from
the real one, so stopping it cannot touch `roleradar-pg` or its volume. Each
test creates its own database and drops it. The suite refuses to start if
pointed at port 5432 or at a host that is not this machine.

`tests/fixtures/legacy_schema.sql` is the live schema before migrations, and
`legacy_rows.sql` is 25 + 6 rows shaped like the live ones, including the three
awkward facts the migrations have to survive: a posting dead-lettered three
times at one millisecond, double-encoded `errors_json`, and `description` NULL
throughout. The tests migrate that database and compare an MD5 over every
original column of every row, before and after.

### The API and dashboard

The API reads `DATABASE_URL` if set, and falls back to the `POSTGRES_*` values
otherwise. Nothing is hardcoded and no credential is in the repo.

```
python -m venv api/.venv && source api/.venv/bin/activate
pip install -r api/requirements.txt
```

Against the hosted Neon warehouse — the same data the live URL serves:

```
export DATABASE_URL="$(grep '^NEON_DATABASE_URL=' .env | cut -d= -f2-)"
uvicorn main:app --app-dir api --port 8000
```

Against the local container instead:

```
set -a && source .env && set +a
uvicorn main:app --app-dir api --port 8000
```

Either way the dashboard is at `http://localhost:8000/`, which serves
`api/static/index.html`. That file is self-contained and reads the API only, so
copying it onto an nginx static root and setting `API_BASE` at the top of its
script block to the API's address is also supported.

To deploy: `vercel deploy --prod`. `DATABASE_URL` is set as an encrypted
environment variable in the Vercel project, never in the repo.

### Costs

The Anthropic API is the only line item that is not free. No free tier, prepaid
credits, one call per job item. The Limit node is the ceiling on spend per run.
A dev loop that forgets the cap and pulls 500 postings costs real money.

n8n is self-hosted in Docker and costs nothing. Neon and Vercel are both on
free tiers with no card attached: Neon suspends the compute when idle, and the
dashboard's traffic is far inside Vercel's hobby limits. The BigQuery
streaming-insert cost noted in `CLAUDE.md` no longer applies.

## Architecture decisions

### Data validation and infra failure are different events

An HTTP 529 from Anthropic is not the same event as a model returning a bad
`seniority` value. Mixing them makes both un-diagnosable. Validation failures
route through the IF node to `dead_letter_log`. Infra failures belong to a
separate Error Trigger workflow writing to `pipeline_errors`. Its database
side is built and tested; the workflow itself is not built in n8n. See
[Pipeline health](#pipeline-health).

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

### One batch call per run

```js
validateBatch(pairs, ingestedAt, runId) -> {
  run_id, ingested_at, extraction_version,
  records_processed, records_accepted, records_rejected,
  accepted_records, rejected_records, warnings
}
```

`validate()` judges one posting and knows nothing about runs. `validateBatch()`
calls it once per pair and adds the two things it does not know: which run this
is, and when the posting was published. It contains no validation rules of its
own, and `validate()` itself is unchanged, down to its original 74 tests.

**Run id.** Passed in, stamped on every record, accepted or rejected. It comes
from the database: the workflow's first node inserts a `pipeline_runs` row and
gets the UUID back.

**Timestamp.** Passed in once. Every record in a run carries the identical
`ingested_at`. The old Code node read the clock inside its loop, which is why
runs had to be recovered by gap; this removes the cause for new rows. It must be
UTC with a trailing `Z`, because `jobs_extracted.ingested_at` has no zone and
would store a local time as if it were UTC.

**Counts.** `records_processed = records_accepted + records_rejected`, always.
Every input produces exactly one record in exactly one of the two lists. These
are the validator's verdicts. They are not rows written:

| number | what it counts | who knows it |
|---|---|---|
| fetched | postings Remotive returned | the HTTP node |
| processed | items handed to the validator, after the Limit node | `records_processed` |
| accepted | items the validator passed | `records_accepted` |
| rejected | items the validator failed | `records_rejected` |
| written | rows actually in the table for the run | Postgres, by query |
| skipped | accepted or rejected, but already there for this run | accepted minus written |

A posting listed twice in one batch is accepted twice and written once. The
tests include exactly that case, and the stored count and the row count differ
by one.

**Publication date.** Copied through exactly as Remotive sent it when it looks
like `2026-08-21T05:54:39`. Anything else, including a missing value or one
carrying a `Z` or an offset, is stored as NULL and reported in `warnings`. It is
never guessed and never replaced with `ingested_at`. An offset is refused
because the column has no zone and Postgres would discard it silently.

A bad date does not reject the row. `publication_date` is Remotive's metadata,
not the model's output. Dead-lettering a correct extraction because an upstream
date format changed would count Remotive's drift as model error, and
`dead_letter_log` is what the model error rate is computed from.

**What throws and what does not.** A bad `runId`, a bad `ingestedAt` or a
`pairs` that is not an array throws. Those are wiring faults in the workflow,
and failing the node is right: nothing gets written under a run that does not
exist. A malformed *pair* does not throw. `null`, `{}`, a pair with no
extraction: each becomes one rejected record, so the counts still add up.

If `validate()` itself throws on a pair, that pair is dead-lettered with the
code `VALIDATOR_EXCEPTION` and the rest of the batch carries on. `validate()` is
written not to throw, so this is a bug being contained, not a case being
handled. JSON input cannot trigger it today.

**Empty input** is a valid batch: three zeros, two empty lists.

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

![The RoleRadar metrics dashboard: a summary strip showing 26 records processed,
80% success rate on the last run, 1 rejected, and time since the last run that
produced data; beside it an ingestion volume chart and an extraction success
rate chart with a tooltip open.](docs/dashboard.png)

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

**The observed rate is 0, across all 26 records.** Not a single dead-letter row
carries `N8N_MISMATCH`, and every one of the 20 rows in `jobs_extracted` has
`mentions_n8n_model = false` and `mentions_n8n_actual = false`. No posting
processed so far contains the token, so every comparison the cross-check has
ever made has been false against false.

That is a weak result and worth stating as one. A metric that has only ever
agreed is not evidence that it would catch a disagreement — it is evidence that
the sample has never contained one. The mechanism is exercised by the test
suite, which asserts both the matching and mismatching branches against real
captured fixtures; the live sample has not produced a positive case.

Fixing this needs data, not code: either a search term that actually returns
postings mentioning n8n, or a deliberately wrong `mentions_n8n` fed through the
validator, on the same argument as the deliberate dead-letter row. Until then
the panel is a chart of five zeroes, and reading it as "the model is accurate"
would be reading it wrong.

### What the dashboard cannot see

A run that died before either Postgres node wrote anything leaves no rows in
either table, so it is not a run here at all. The Error Trigger workflow does
not write to Postgres. This dashboard measures data quality, not pipeline
liveness, and the recent-runs table will look clean through an outage.

"Time since last successful run" going stale is the only signal on the page that
the pipeline stopped. That is thin, and it is the reason the tile is in the
summary strip rather than buried.

That describes the live system as it is today. The page in this repository
now has a pipeline-health band above the data-quality one, described under
[What the dashboard shows](#what-the-dashboard-shows). Until the database is
migrated and the workflows are changed it will say what is true: only
pre-tracking rows exist, and nothing has been tracked.

What no version of the page can see is a run that never started. Nothing is
written, so there is nothing to draw. The page shows the consequence, the last
completed run getting old, and the
[external check](#external-freshness-check) is what notices it on time.

### Endpoints

Fractions are 0–1. Every list endpoint returns `row_count`, so an empty result
is explicit rather than an empty array that renders as zero.

Timestamps come in two forms, and the difference is real. The five dashboard
endpoints return naive UTC (`2026-09-03T15:03:56.860000`), as they always have.
The three `pipeline-*` endpoints read `timestamptz` columns and return an
offset (`2026-11-12T06:00:01.412000+00:00`). `generated_at` is naive UTC
everywhere.

| endpoint | serves | shape |
|---|---|---|
| `GET /api/health` | the API-reachable vs no-data distinction, and whether the schema is new enough | `{database, checked_at, schema{version, required, compatible}}`, 503 when the database is unreachable |
| `GET /api/runs?limit=30` | volume chart, success-rate chart, runs table | `{generated_at, run_gap_minutes, row_count, runs[]}` |
| `GET /api/summary` | the summary strip | `{generated_at, run_count, totals, last_run{}, last_successful_run_at, seconds_since_last_successful_run}` |
| `GET /api/error-classes` | the error-class bar chart | `{generated_at, row_count, default_metric, total_rejected_rows, total_code_occurrences, classes[]}` |
| `GET /api/model-accuracy` | the model-disagreement panel | `{generated_at, row_count, by_version[], by_run[]}` |
| `GET /api/pipeline-runs?limit=30&offset=0&status=` | tracked runs, newest first | `{generated_at, limit, offset, total, row_count, stale_run_minutes, detail_level, runs[]}` |
| `GET /api/pipeline-errors?limit=30&offset=0&run_id=` | infra failures, newest first | `{generated_at, limit, offset, total, row_count, detail_level, errors[]}` |
| `GET /api/pipeline-status` | did the pipeline run | see [Pipeline health](#pipeline-health) |

Every 503 has the same body, on every endpoint:

```json
{ "error": "database_unavailable", "reason": "unreachable",
  "detail": "the database could not be reached", "generated_at": "..." }
```

`reason` is one of `waking`, `pool_busy`, `unreachable`, `schema_outdated`,
`query_failed`, and `detail` is the one fixed sentence for that reason. See
[Security boundaries](#security-boundaries).

Page sizes are capped: `limit` at 500 on `/api/runs` and 200 on the two
`pipeline-*` lists, `offset` at 100000. Past the cap is a 422, not a clamp.

A `runs[]` entry:

```json
{
  "run_started_at": "2026-09-03T15:03:56.860000",
  "run_ended_at":   "2026-09-03T15:03:56.865000",
  "ingested": 5, "validated": 4, "rejected": 1,
  "success_rate": 0.8,
  "success_rate_delta": 0.0,
  "success_rate_rolling3": 0.8,
  "cumulative_ingested": 26,
  "cumulative_validated": 20,
  "status": "degraded",
  "run_source": "legacy",
  "run_id": null
}
```

`run_source` and `run_id` are new and are the only change to this response. See
[Mixed historical and new data](#mixed-historical-and-new-data).

`success_rate_delta` is null on the first run. The limit is applied after the
window functions, so `cumulative_ingested` stays a lifetime total and the delta
on the oldest visible run still refers to the run before it.

A `classes[]` entry:

```json
{ "code": "SALARY_PARTIAL", "category": "salary_logic",
  "rows": 5, "occurrences": 5, "pct_of_rejected_rows": 0.8333 }
```

### Pipeline health

Everything above this point measures **data quality**: of the rows that
arrived, how many were good. This section is about a different question:
**did the pipeline run at all.** The two are kept apart on purpose, with
different words, because they fail independently. A run can complete perfectly
and reject every record. A run can die having written five good rows.

| | data quality | pipeline health |
|---|---|---|
| question | were the records good | did the run happen and finish |
| field | `status` on `/api/runs`, `last_run.status` on `/api/summary` | `pipeline_status` on `/api/pipeline-status` |
| values | `ok`, `degraded`, `failed` | `healthy`, `degraded`, `failed`, `no_data` |
| built from | rows in `jobs_extracted` and `dead_letter_log` | rows in `pipeline_runs` and `pipeline_errors` |

The data-quality status is unchanged. Nothing here redefines it.

#### The life of a run

```
Start Run            INSERT pipeline_runs            status = running
  ... fetch, extract, validate, insert rows carrying the run_id ...
Complete Run         complete_pipeline_run()         status = completed
                       only after the counts and the rows all agree

any node fails       n8n calls the error workflow
Record Error         record_pipeline_error()         status = failed
                       one pipeline_errors row, tied to the run if it can be

n8n killed           nothing runs                    status stays running
                       the API calls it stale after 15 minutes
n8n not running      nothing runs, nothing written   no row at all
                       only the freshness check can see this
```

**What `completed` guarantees.** `complete_pipeline_run()` cannot see what the
validator did. It only knows what the workflow tells it. So it is told the same
fact from several nodes and refuses when they disagree with each other or with
the tables:

| told by | what |
|---|---|
| the source request | how many postings were fetched |
| the node that builds the model request | how many items were submitted |
| the validator | the `source_job_id` of every accepted and every rejected record |
| the tables | the rows actually under this `run_id` |

It closes the run only if: both counts are present; submitted is not more than
fetched; something was submitted if anything was fetched; accepted plus
rejected equals submitted; every listed id has its row; rejected records with
no id are present in exactly the right number; and no row exists under the run
that the lists do not mention. Otherwise it raises and changes nothing.

So a run cannot be `completed` before its writes exist, a failed insert cannot
produce a completed run, a workflow that sends empty lists by mistake is
refused, and none of that depends on where a node sits on the n8n canvas. A
run that legitimately fetched nothing still closes: zero, zero, two empty
lists, no rows.

An earlier version checked only that the listed ids had rows. That was not
enough. A completion node that ran before the validator sent two empty lists,
and two empty lists were accepted as a run with nothing to write. The run was
marked complete and its rows arrived afterwards. The fetched and submitted
counts are what close that: a node that has not run has no count to send. The
tests reproduce the old failure and show it refused.

What the database guarantees, given what the workflow sends it:

- A run is `completed` only if every listed id has its row under that run,
  no row exists that the lists leave out, id-less rejections are present in
  exactly the right number, the verdicts add up to the submitted count, the
  submitted count is not more than the fetched count, and something was
  submitted if anything was fetched.
- A missing count is refused outright.
- Once `completed`, a run is never changed again by a second completion.
- An error report never reopens or rewrites a completed run, and never
  touches a run other than the one it matched.

What it has to trust the workflow for, because the database cannot see n8n:

- **The fetched count comes from `HTTP Request`, and is `null` when that node
  has not run.** This is the load-bearing one. A `Complete Run` that fires
  before the pipeline, and is sent `0, 0, [], []` instead of `null`, looks
  exactly like an honest empty feed, and the run closes. The documented
  expression sends `null`; a test states the boundary by showing the function
  cannot tell the two apart. Staging scenario 6 in `docs/n8n-notes.md` checks
  the expression in n8n.
- **Each number comes from a different node.** If the submitted count were
  taken from the validator's own output, the check that the verdicts add up
  would be the validator agreeing with itself.
- **`Complete Run` runs once, after the inserts.** It hangs off `Start Run`,
  which emits one item, and execution order `v1` runs it after the branch
  above it.

Even when that trust is misplaced, the mistake is not silent for long: rows
written after a run closed make the stored counts disagree with the tables,
and the API reports `rows_match_counts: false` and `degraded`.

What it still cannot do:
- **It does not stop a late insert.** A row written under a `run_id` after
  that run closed is not prevented. It is visible: the stored counts no longer
  match the rows, and the API reports `rows_match_counts: false` and
  `degraded`.
- **A rejected row with no `source_job_id` can be written twice by a replay**,
  because the unique index cannot see a NULL. The function then refuses to
  close the run, since there is one more row than there were verdicts. That is
  the honest outcome and it needs a person to look.
- **Writing and closing are separate statements.** See
  [Known limitations](#known-limitations).

Two closers for one run, or a closer and an error report, take turns on a row
lock. The tests run them on separate connections and check the loser waits and
then changes nothing.

`docs/n8n-notes.md` step 10 has the cases and what still depends on n8n's
execution order.

**How a failure finds its run.** By execution id, workflow id and recency
together. The execution id alone is not trusted: n8n numbers executions from 1
again when its volume is recreated. A match also needs the workflow ids to
agree and the run to have started in the last 6 hours. Only that one run is
touched, and only if it is still `running`. A failure with no matching run,
which is normal when the failure came before `Start Run` wrote anything, is
recorded with `run_id` NULL. The same failure reported twice writes one row.

#### The four values

`pipeline_status` is decided by `pipeline_health()` in `api/main.py`, a pure
function over a handful of counts.

| value | means | when |
|---|---|---|
| `healthy` | the latest finished run completed, recently, and nothing failed lately | none of the conditions below |
| `degraded` | working, but look at it | see reasons |
| `failed` | the latest news is bad, or there is no recent news | see reasons |
| `no_data` | nothing has ever been recorded | no runs, no errors, no rows at all |

The `reasons` array says why. `failed` reasons come first and outrank
`degraded` ones.

| reason | status | meaning |
|---|---|---|
| `last_run_failed` | failed | the most recent finished run is `failed` |
| `last_run_abandoned` | failed | the most recent run has been `running` past the stale threshold |
| `error_since_last_completed_run` | failed | a `pipeline_errors` row is newer than the last completed run. Catches a failure that never got a run row |
| `no_completed_run` | failed | runs or errors exist, and no run has ever completed |
| `completed_run_overdue` | failed | the last completed run is older than the expected interval plus grace |
| `data_overdue` | failed | only pre-tracking rows exist, and the newest is older than that limit |
| `legacy_data_only` | degraded, or failed with `data_overdue` | only pre-tracking rows exist. Fresh rows show something ran; without a run row they cannot show it finished |
| `stale_running_run` | degraded | an older run is stuck `running`, behind a newer good one |
| `recent_failures` | degraded | a failed run or an error inside the recent window, since recovered from |
| `rows_do_not_match_counts` | degraded | the last completed run's stored counts disagree with the rows now in the tables |
| `never_executed` | no_data | |

Three choices in there that are deliberate:

- **Silence is failure.** A scheduler that is switched off writes nothing, so
  the only sign is the last completed run getting old. That is
  `completed_run_overdue` and `failed`. A good last write does not keep the
  status `healthy` forever.
- **Zero records is not no data.** A run that completes having fetched nothing,
  or accepted nothing, is a completed run and `healthy`. `no_data` means no run
  was ever recorded. The response shows which: `last_successful_run` is an
  object with `accepted_count: 0` in one case and `null` in the other.
- **A run in progress decides nothing.** While a run is `running` and not yet
  stale, the status is whatever the last finished run made it.

Thresholds, all environment variables on the API:

| variable | default | |
|---|---|---|
| `EXPECTED_RUN_INTERVAL_HOURS` | 24 | how often a run is expected |
| `RUN_GRACE_MINUTES` | 60 | how late it may be |
| `STALE_RUN_MINUTES` | 15 | how long `running` is believable |
| `RECENT_FAILURE_HOURS` | 24 | how far back `recent_failures` looks |

The API only knows an interval. It does not know the schedule. The
[external check](#external-freshness-check) does, and is the stricter test.

#### Response shapes

A run, as it appears in `/api/pipeline-runs` and inside `/api/pipeline-status`:

```json
{
  "run_id": "3f2b8c1e-5d4a-4e6f-9a7b-0c1d2e3f4a5b",
  "n8n_execution_id": null,
  "status": "completed",
  "is_stale": false,
  "started_at": "2026-11-12T00:30:00.412000+00:00",
  "completed_at": "2026-11-12T00:30:09.127000+00:00",
  "duration_seconds": 8.715,
  "age_seconds": 5400,
  "fetched_count": 13, "processed_count": 5,
  "accepted_count": 4, "rejected_count": 1,
  "accepted_rows": 4, "rejected_rows": 1,
  "rows_match_counts": true,
  "error": null,
  "error_count": 0
}
```

The `*_count` fields are what the workflow recorded: the validator's verdicts.
They are null on a run that never closed. The `*_rows` fields are counted from
the tables on every request, so they are what is there now. A replayed insert
is skipped by the unique index and cannot raise either one.
`rows_match_counts` is null unless the run completed. `duration_seconds` is
null while a run is open. On a failed run `error` is
`{"category": "...", "message": null}`. `n8n_execution_id` and `error.message`
are filled only for the operator.

`status` accepts `running`, `completed` or `failed`. `limit` is 1 to 200.
Anything else is a 422.

An error, in `/api/pipeline-errors`:

```json
{
  "id": 7,
  "occurred_at": "2026-11-12T00:30:04.900000+00:00",
  "failed_node": "Claude Extract",
  "run_id": "3f2b8c1e-5d4a-4e6f-9a7b-0c1d2e3f4a5b",
  "category": "upstream_llm",
  "workflow_name": null,
  "execution_id": null,
  "message": null
}
```

The last three are null in public and filled for the operator. The keys are the
same either way, and the envelope says which you got in `detail_level`.

`run_id` filters to one run and must be a UUID. `category` is one of
`upstream_source`, `upstream_llm`, `database`, `pipeline_code`, `trigger`,
`unknown`; anything else stored in the column is reported as `unknown`.

`/api/pipeline-status`:

```json
{
  "generated_at": "2026-11-12T02:00:00.000000",
  "pipeline_status": "healthy",
  "reasons": [],
  "last_attempted_run": { "...a run..." },
  "last_successful_run": { "...a run, or null..." },
  "last_failed_run": { "...a run, or null..." },
  "running": { "count": 0, "stale_count": 0, "runs": [] },
  "recent_failures": {
    "window_hours": 24.0, "failed_runs": 0, "errors": 0,
    "uncorrelated_errors": 0, "by_category": {},
    "errors_since_last_successful_run": 0, "last_error_at": null
  },
  "freshness": {
    "evidence": "explicit",
    "expected_interval_hours": 24.0, "grace_minutes": 60.0,
    "last_successful_run_completed_at": "2026-11-12T00:30:09.127000+00:00",
    "seconds_since_last_successful_run": 5391,
    "overdue": false,
    "last_legacy_data_at": "2026-09-11T14:02:11.345000",
    "seconds_since_last_legacy_data": 5313468
  },
  "totals": { "tracked_runs": 1, "recorded_errors": 0, "legacy_rows": 31 },
  "thresholds": { "stale_run_minutes": 15.0, "recent_failure_hours": 24.0 }
}
```

It answers **200 whenever the database answered**, including when the status is
`failed`. A 503 with `{"error": "database_unavailable"}` means the API could
not reach the warehouse, and says nothing about the pipeline. A monitor has to
tell those apart, and the status code is how.

The response has the same keys in every state. An empty warehouse returns the
same structure with nulls and zeros. It is always the public view, whoever
asks: no messages, no execution ids.

### What the dashboard shows

The page is in three bands, and the first two answer different questions.

**Pipeline health.** One badge: Healthy, Degraded, Failed or No data, with the
reasons under it in words. Then five tiles:

| tile | shows | from |
|---|---|---|
| Last attempted run | when the newest run started, and its state | `last_attempted_run` |
| Last successful run | when the newest completed run finished, and how long it took | `last_successful_run` |
| Last failed run | when the newest failed run started, and its error category | `last_failed_run` |
| Freshness | On schedule or Overdue, and the age of the last completed run | `freshness` |
| Last successful run wrote | accepted + rejected rows in the tables, beside the validator's verdicts and the fetched count | `last_successful_run` |

**Data quality.** Everything the page had before, unchanged: the summary strip,
volume, success rate, rejection reasons, model disagreement, recent runs. The
runs table's last column is now headed "Data quality" so it cannot be read as
whether the run finished.

**Executions.** A table of tracked runs with state, duration, the four counts,
rows written and error category; a chart of duration for completed runs; and a
table of infrastructure errors by category and node.

Definitions the page relies on:

| term | means |
|---|---|
| **run state** | `completed`, `failed`, `running`, or `stale`: still `running` past `STALE_RUN_MINUTES` |
| **duration** | `completed_at - started_at`. A run that did not complete has none and is left off the chart, not drawn as zero |
| **accepted, rejected** | the validator's verdicts, stored when the run closed. Blank on a run that never closed |
| **rows written** | counted from the tables at request time. Marked when it disagrees with the verdicts |
| **freshness** | time since the last *completed* run. Before any run has completed the tile says "Not tracked" and gives the age of the newest pre-tracking row, labelled as that |
| **error category** | where the failure was: Remotive request, Anthropic request, database write, Code node, trigger, or unclassified |

Three words each mean one thing, and each has its own glyph and CSS class so
they cannot be confused on screen:

| | values | question |
|---|---|---|
| pipeline health | Healthy, Degraded, Failed, No data | did the pipeline run |
| run state | completed, failed, running, stale | what happened to this execution |
| data quality | ok, degraded, failed | were the rows good |

What the page will not do:

- **Fill a gap.** A value the API did not send is a dash. A run with no
  verdicts shows dashes, not zeros. A zero means the API said zero.
- **Round an unknown up.** A status the page does not recognise is shown as
  "Unknown", never as Healthy.
- **Invent history.** Rows from before run tracking appear under data quality
  only. The executions table says there is no tracked execution yet.
- **Show an error message.** The public gets a category and a node name.

Each panel is in one of its states and says which: loading, empty, error, or
data. The pipeline panels have a fifth, "not set up", for a database that has
not been migrated; it is neither an error nor an empty result. One endpoint
failing changes its own panel and no other.

The view logic is in `api/static/pipeline.js`: pure functions from an API
response to text or HTML, with no fetch and no DOM. That is what lets it be
tested under `node --test` with no browser. `index.html` does the loading and
places the strings. No framework was added; the page is still two static
files.

### Security boundaries

The dashboard and its API are public and have no login. The policy is one
sentence: **the public gets summaries, and diagnostics stay with the
operator.**

| data | public | operator | stored |
|---|---|---|---|
| Aggregate counts, rates, run times, statuses | yes | yes | |
| Error category, failed node, time, `run_id` | yes | yes | |
| Error message | no | redacted, 300 characters | whole, 4000 characters |
| n8n execution id, workflow name | no | yes | yes |
| Stack trace | no | no | not stored |
| Database driver error text | no | no | server log only |
| Job description text | no | no | `dead_letter_log` only |

**The operator** is whoever sends `X-Ops-Token` matching the `OPS_TOKEN`
environment variable. That is the whole mechanism: one shared secret, compared
in constant time, for the one person who reads diagnostics. No users, sessions
or login page, because there is one reader and they have `curl`.

- `OPS_TOKEN` unset, or shorter than 24 characters, means there is no operator
  mode. Messages are then readable only by querying the database. That is the
  default and a reasonable place to leave it.
- A wrong token is a 401, not a silent fall back to the public view. Otherwise
  a typo looks exactly like "there are no messages".
- The token is read from the header only. One in a query string is ignored, so
  it cannot end up in a URL in a log.
- Operator responses carry `Cache-Control: no-store`.
- `/api/pipeline-status` is always the public view. It is what a monitor
  polls, and a monitor's logs are no place for diagnostics.

**Database errors.** A driver's error message names hosts, users, tables and
columns. Clients get a `reason` code and one fixed sentence for it. The real
message is written to the server log, after redaction, under the logger
`roleradar.api`. On Vercel that is the function log.

This changed an existing response. The 503 `detail` used to be the driver's
own text, so the dashboard could show the real cause; the page now shows the
fixed sentence, and the screenshot under [Failure states](#failure-states)
predates that. The key is still there, and the `waking` sentence is unchanged
because the page matches on it.

**Redaction** still runs on everything, including the operator's copy and the
log: `scheme://user:password@`, `sk-` keys, Neon passwords, bearer tokens,
JWTs, `password=` / `token=` / `api_key=` pairs, URL query strings, and any
unbroken token of 40 characters or more. It is a net, not a proof. A secret in
a shape nobody anticipated gets through, which is why the public gets no
message at all instead of a redacted one.

**The API cannot write.** Every pooled connection is set read-only, so each
transaction starts with `BEGIN READ ONLY` and Postgres refuses any write. The
API never issues one, and a test checks that every SQL constant starts with
`SELECT` or `WITH`; the connection setting makes it hold even against a bug.
It matters most for preview deployments, which may be handed the production
`DATABASE_URL`. `default_transaction_read_only` as a connection option was
the alternative, and was rejected because Neon's pooler is PgBouncer in
transaction mode, which can refuse unknown startup parameters.

**SQL.** Every statement is a module constant. Values reach Postgres as bound
parameters. A test parses `api/main.py` and fails if any statement is built
from anything but constants. Query parameters are typed and bounded by
FastAPI before they reach a query.

What this does not cover:

- **No rate limiting.** Anyone can poll the endpoints. Each request is a few
  small queries against a database that sleeps when idle, so the cost of abuse
  is a warm database, but it is not prevented.
- **CORS is open**, as before, for GET. The operator header is allowed
  cross-origin too; the protection is the secret, not the origin.
- **A leaked token is valid until changed.** There is no expiry and no audit
  trail of its use.
- **Node names and categories are public.** They describe the workflow's
  shape, which this repository already publishes.

### Mixed historical and new data

Rows written before run tracking have `run_id` NULL. They stay that way. No run
is invented for them.

`/api/runs`, `/api/summary` and the per-run half of `/api/model-accuracy` now
group in two ways at once:

| rows | grouped by | `run_source` | `run_id` |
|---|---|---|---|
| `run_id` NULL | timestamp gap, exactly as before | `legacy` | `null` |
| `run_id` set | that `run_id` | `explicit` | the UUID |

Only legacy rows take part in the gap detection. An explicit run that lands
within two minutes of a legacy one is not merged into it, and two explicit runs
seconds apart are two runs. Internally the grouping key is `legacy:<n>` or
`run:<uuid>`, so an inferred run can never be read as a real UUID; the API
never returns that key, only `run_source` and `run_id`.

The metric definitions did not move. On the legacy rows, the new queries return
exactly what the committed ones return, field for field, and a test holds that.

One consequence worth knowing: `/api/runs` still lists a run only if it wrote
at least one row. A tracked run that fetched nothing or died before writing is
not there. It is in `/api/pipeline-runs`. The two endpoints answer the two
different questions at the top of this section.

### What can go wrong, and where each kind is counted

| what happened | whose fault | recorded in | reported as |
|---|---|---|---|
| The model returned a wrong or malformed value | the model | `dead_letter_log` | `/api/error-classes`: `schema`, `envelope`, `model_disagreement` |
| The values contradict a business rule | the model, or the posting | `dead_letter_log` | `/api/error-classes`: `salary_logic` |
| Remotive left out a field the row needs | the source | `dead_letter_log` | `/api/error-classes`: `provenance` |
| `validate()` itself threw | this project's code | `dead_letter_log`, code `VALIDATOR_EXCEPTION` | `/api/error-classes`: `validator_internal` |
| Remotive or Anthropic did not answer | the provider | `pipeline_errors` | `/api/pipeline-errors`: `upstream_source`, `upstream_llm` |
| A Postgres node failed | the database, or the schema | `pipeline_errors` | `/api/pipeline-errors`: `database` |
| A Code node threw outside `validate()` | this project's wiring | `pipeline_errors` | `/api/pipeline-errors`: `pipeline_code` |
| The run never started | the scheduler, or the machine | nothing | `/api/pipeline-status`: `completed_run_overdue`; the freshness check: `missed run` |

`validator_internal` is new. Before it, a `VALIDATOR_EXCEPTION` row would have
been counted under `schema`, as if the model had made a mistake. No stored row
carries that code, so adding the category moved no historical row, and a test
compares every other class against the committed query to hold that.

### Failure states

The error text in this screenshot is the driver's own message, which the API
no longer returns. The same failure now reads "the database could not be
reached". See [Security boundaries](#security-boundaries).

![The same dashboard with the warehouse unreachable: the header pill reads
"warehouse unreachable" in red, every summary tile reads "summary unavailable",
and each panel shows "Could not load this panel" with the underlying connection
error.](docs/dashboard-down.png)

Each panel is in one of four states and shows which: loading, error, empty, or
data. An endpoint that is unreachable or returns a non-2xx paints the panel with
the error, not with an empty chart. A query that returns no rows paints "no runs
recorded" over the plot area. An empty chart never looks like zero.

**Slow and broken are shown differently.** Neon suspends the compute when idle,
so the first request after a pause is slow rather than failed. A request still
outstanding after 1.4s switches the panel to "waking the database" with an
explanation, and the API's own cold-start timeout is retried once before
anything is reported as an error.

Telling those apart takes more than a timeout. `psycopg_pool` retries a failing
connection until its deadline, so a warehouse that is refusing connections and
one that is merely asleep both surface as the same `PoolTimeout` carrying the
same uninformative message — which would collapse the distinction entirely. On
that path the API probes once, directly and outside the pool, and reports what
it finds. A refused connection, an unresolvable host or a rejected password all
fail immediately and produce a real error worth showing; only a probe that also
times out is genuinely ambiguous, and that is the sole case reported as "may be
waking". The screenshot above is the refused case, which is why it reads
unreachable rather than waking.

### Pooled connections do not carry a search_path

Every query in `api/main.py` and `sql/queries.sql` writes
`public.jobs_extracted`, not `jobs_extracted`. That is not stylistic.

The warehouse moved to Neon, and the API connects through Neon's **pooled**
endpoint, which is PgBouncer in transaction mode. Restoring the `pg_dump` set
`search_path` to `''` for the duration of that session — standard `pg_dump`
behaviour, and harmless on a direct connection. The pooler then handed the same
backend to the next client with that setting still on it. Every unqualified
query started failing with `relation "jobs_extracted" does not exist`, while the
identical query on the direct endpoint worked.

`ALTER ROLE ... SET search_path` and `ALTER DATABASE ... SET search_path` both
applied and neither fixed it, because they take effect for new backends and the
pooler was reusing an existing one.

**The lesson is that session state is not yours on a pooled connection.**
Anything a client sets — `search_path`, `SET TIME ZONE`, prepared statements,
`SET ROLE` — can outlive that client and reach the next one, and can equally be
absent when you assume it. A transaction pooler guarantees you a transaction,
not a session. So the fix is not to set the search path more forcefully; it is
to write queries that do not depend on one.

Schema-qualifying is the durable form of that. It costs seven characters per
reference and removes a whole class of failure that only appears under load,
only in the deployed environment, and looks like the table has vanished.

## Deployment

**Live dashboard:** https://roleradar-snowy.vercel.app

Reads the hosted Neon warehouse. Always reachable. The first load after a quiet
period is slow because Neon suspends the compute when idle — the page says
"waking the database" rather than showing a spinner or reporting a failure.

### What is hosted and what is not

Two of the three pieces run in the cloud. The third does not, and the
distinction matters for reading the dashboard.

| piece | where it runs | up when |
|---|---|---|
| Dashboard page + metrics API | Vercel | always |
| Warehouse (Postgres 16 → Neon 18) | Neon | always, though the compute suspends when idle |
| **n8n pipeline** | **Docker on a laptop** | **only while that laptop is on** |

**The pipeline is not deployed.** n8n still runs in Docker locally and holds its
cron in-process. Ingestion happens when that machine is up and the container is
running, and not otherwise — there is no catch-up on start, so a day the laptop
was closed is a day with no run. The hosted half is the *reading* half.

The practical consequence: the live dashboard is always reachable and its
numbers are always real, but "time since last run that produced data" measures
laptop uptime as much as pipeline health. A stale value there usually means the
machine was off, not that anything broke. Do not read the deployed URL as
evidence of a continuously running pipeline, because it is not one.

Deploying n8n properly would mean hosting it somewhere always-on, which is a
paid tier on every provider worth using, and the ingestion cost is per-run
Anthropic spend. Neither is justified for a portfolio project on free tiers.

### Configuration

The API takes `DATABASE_URL` and it overrides every `POSTGRES_*` value. On
Vercel it is set as an environment variable and is never in the repo, never in
the HTML, and never in a commit. Locally the same string lives in `.env` as
`NEON_DATABASE_URL`, which is gitignored.

Use Neon's **pooled** connection string — the host contains `-pooler`. See
"Pooled connections do not carry a search_path" above for what that implies for
the SQL.

`api/static/index.html` reads from the API only. It contains no credentials and
holds nothing but an `API_BASE` constant, empty by default, meaning same origin.
It loads `pipeline.js` from beside it. Copying those two files to an nginx
static root and setting `API_BASE` to the Vercel URL is still a supported way
to serve it.

`OPS_TOKEN` is optional and unset by default. See
[Security boundaries](#security-boundaries).

### External freshness check

The pipeline cannot report that it did not run. If n8n is stopped, nothing
fails, nothing is written, and the Error Trigger workflow is never called.
Something outside has to notice the silence.

`monitor/check_freshness.py` is that. It is one file, standard library only.
It calls `/api/pipeline-status` and checks the answer against the schedule it
was told:

```
python monitor/check_freshness.py \
    --url https://<host>/api/pipeline-status \
    --schedule 06:00 --timezone Asia/Kolkata --grace-minutes 90
```

`--schedule` is the time, or comma-separated times, n8n is set to run each day,
in `--timezone`. The check works out the most recent run whose grace period has
ended and requires a completed run that started at or after it.

| exit | meaning |
|---|---|
| 0 | the run that was due completed |
| 1 | the pipeline is unhealthy: the due run is missing, a run is stuck `running`, or `pipeline_status` is `failed` or `no_data` |
| 2 | the API could not answer. Not evidence about the pipeline |
| 3 | the check is misconfigured |

`degraded` prints a warning and exits 0, unless `--strict`.

It retries three times, 15 seconds apart, before calling the API unhealthy,
because a suspended Neon compute answers 503 once and then wakes.

`.github/workflows/freshness.yml` runs it twice a day on GitHub's runners,
which are not the laptop. **It is inert as committed.** The job is skipped
until the repository variable `ROLERADAR_STATUS_URL` is set. It uses
repository *variables*, not secrets: the URL is public and read-only, and the
check sends no credential. A failed job is the alert; GitHub emails the
repository owner by its own default and nothing else is configured.

Limits of this check:

- It only knows daily times. A weekday-only or every-other-day schedule would
  need the script changed.
- `--schedule` is a second copy of what the n8n Schedule Trigger says. If one
  changes and the other does not, the check is wrong.
- GitHub disables scheduled workflows in a repository with no activity for 60
  days, and may run them late.
- It goes through the API. If Vercel or Neon is down it exits 2 and learns
  nothing about the pipeline.
- A red job twice a day is the whole alert, and it repeats until fixed.

### Releasing this

Nothing has been released. The procedure is in
[docs/release-runbook.md](docs/release-runbook.md): eighteen checkpoints, each
with an expected outcome, a check, a stop condition, a recovery, and who runs
it.

The order, and why it is this order:

1. **Migrate the database.** Every new column is nullable and the deployed API
   never mentions them, so the live dashboard keeps working at every point in
   the chain.
2. **Deploy the API.** Only now. Deployed against the old schema, the new API
   answers 503 on `/api/runs`, `/api/summary` and `/api/model-accuracy`.
3. **Build the error workflow**, so a failure in the next step is recorded.
4. **Change the ingestion workflow.**
5. **Prove both paths for real**: one successful run, and one scheduled
   failure placed before the paid call.
6. **Export the workflows and turn on the freshness check.**

**How a production deployment is triggered.** The Vercel project is connected
to this repository through Vercel's GitHub app: every commit pushed to `main`
since 7 September has a production deployment made by `vercel[bot]`. A push to
`main` goes live with no further step, and `main` has no branch protection. A
push to any other branch builds a preview. No other branch has ever been
pushed, so whether a preview would receive the production `DATABASE_URL` is
not known from here; checkpoint 2 of the runbook is to check, and to give
previews their own database, a Neon branch, if so. A preview cannot write
either way, because the API's connections are read-only.

**The backup is proven, not assumed.** `sql/fingerprint.sql` prints every
table's row count and a hash of its rows, and every column, constraint,
index, view, function and sequence. The runbook dumps Neon with Postgres 18
tools from a Docker image, restores the dump into a throwaway Postgres 18,
and requires the two fingerprints to be identical. That procedure was run
verbatim against a Postgres 18.7 stand-in loaded with the legacy-shaped
fixtures. Its first run failed, on a `CREATE SCHEMA public` that every
restore would have hit, and was fixed.

`GET /api/health` answers the question directly. Its `schema.compatible` is
`true` only when the database has the migrations this API needs. Against a
preview deployment that has the production `DATABASE_URL`, it is a rehearsal
of the real thing.

`sql/preflight.sql` describes a database before you migrate it, read-only,
inside a transaction Postgres will not let write.

### Rolling back

There are two ways back and they are not equally safe.

**Revert the application. Leave the schema.** This is the emergency move.
Promote the previous Vercel deployment; restore the previous version of the
n8n workflow. Both take seconds. Both old versions run unchanged on the new
schema, because neither ever mentions a new column. Nothing is lost.

**Revert the schema only when nothing has been written to it.**
`python sql/migrate.py down --to 0007` removes the two functions and the two
workflow-id columns. `down --to 0001` removes everything these milestones
added. A down migration drops the columns a migration added **and the values in
them**: every `run_id`, and all of `pipeline_runs`. It never deletes a row from
`jobs_extracted` or `dead_letter_log`, so the observations survive, but which
run each belonged to does not, and that cannot be rebuilt. Rows in
`pipeline_errors` survive too, without their `run_id` and category. Take a `pg_dump`
first, and roll the workflow and the API back before it, since both depend on
what it removes.

The rehearsal tests do all three: the old API on the new schema with tracked
runs present, `down --to 0007`, and `down --to 0001`. They check row counts
after each and that the historical rows are byte for byte unchanged.

- **The freshness check:** delete the `ROLERADAR_STATUS_URL` variable. The job
  goes back to being skipped.

## Known limitations

These are real weaknesses, not details.

### Correctness risks

Things that can produce wrong data or wrong conclusions without announcing themselves.

**`validate.js` is hand-pasted into the n8n Code node.** The file in this repo
is the source of truth for the logic. The copy running in the canvas is a
deployed duplicate. There is no import path from the canvas into this repo, so
the two can drift. When the validator changes, the Code node has to be
re-pasted, and nothing enforces that. Passing tests here are not evidence that
the deployed copy matches.

**Fixtures pair positionally.** `fixtures/extraction-output.json` carries no
`source_job_id`, so the only join key between it and
`fixtures/source-records.json` is array index. Reordering or partially
re-capturing one file would silently validate one job's extraction against
another job's description. The tests assert the two arrays are the same length
so a reorder fails loudly, but equal length is not the same as correct
alignment.

**`ingested_at` has two different types.** `timestamp` in `jobs_extracted`,
`timestamptz` in `dead_letter_log`. The same ISO-8601 string is written to both
and one drops the offset. Joining or comparing the two tables on time will not
behave until they agree.

**The extraction version covers the prompt, and the prompt is not in the repo.**
`EXTRACTION_VERSION` is defined once, as a constant in `validate.js`, and that
is what reaches the rows. The duplicate that used to sit in `.env.example` is
gone. But the version is meant to change when the tool schema or the prompt
changes, and both of those live in the Claude Extract node in the canvas.
Editing the prompt does not touch `validate.js`, so nothing prompts anyone to
bump the constant and re-paste it. Rows written under a changed prompt would
still say `1.0.0`.

**`validateBatch()` is tested; the six lines that call it are not.** The Code
node wrapper pairs each Remotive record with its Claude response by position
and reads the clock. It uses `$input` and `$()`, which exist only inside n8n, so
it cannot run under `node --test`. It checks that the two lists are the same
length and fails if not. Equal length is still not the same as correct pairing.

**Publication-date warnings are not stored.** A missing or malformed date
becomes NULL in the table and a warning in the batch result. The warning is
visible in the n8n execution view and nowhere else, and the original malformed
text is not kept.

### Operational gaps

Missing scaffolding. Nothing here is wrong today; each is something that has to be done by hand and therefore eventually will not be.

**Migrations exist, and nothing makes anyone run them.** The schema is now
built by `sql/migrate.py` from the files in `sql/migrations/`, and each database
records what it has had applied. That closes the old gap, where `sql/` described
tables someone had created by hand. It does not close this one: there are two
databases, Neon and the local snapshot, and each is only as current as the last
time someone ran `up` against it. `status` tells you; nothing checks for you.
`/api/health` reports whether the database the API sees is new enough for it,
which is the one place this is checked automatically.

As of this writing **neither database has been migrated**. Both still have the
pre-migration schema. The `pipeline_runs` table, the `run_id` columns and
`description_clean` exist in the migration files and in the test database only.

**The API in this repository must not be deployed before the database is
migrated.** Its run queries read columns that only exist after `0003`. See
[Releasing this](#releasing-this).

**The error path has never run in n8n.** The function, the endpoints and the
check are tested. The Error Trigger workflow is instructions. Whether n8n
calls it, with the data shape the instructions assume, is unverified until a
real scheduled execution fails with it attached.

**Writing rows and closing the run are separate statements.** There is no
transaction around a run. If n8n dies between the last insert and
`Complete Run`, the rows are there and the run stays `running`; the API calls
it stale after 15 minutes and nothing ever closes it. The row is left as it
is on purpose, as the record of what happened.

**A run's correctness still leans on n8n's execution order.** `Complete Run`
must be the lowest branch on the canvas under execution order `v1`. Getting
that wrong no longer produces a false `completed`; it fails the run instead.
It is still a rule held by where a box is drawn.

**If the error workflow cannot write, nothing records the failure.** Postgres
being down is exactly when a Postgres node fails, and the workflow that would
record it needs Postgres. The stale-run rule and the freshness check are what
catch that case.

**Late rows are detected, not prevented.** Nothing stops an insert under a
`run_id` whose run has closed. See [Pipeline health](#pipeline-health).

**The cross-check is only as independent as the canvas keeps it.**
`complete_pipeline_run()` compares numbers that the instructions take from
different nodes. If the canvas is later changed to derive them all from one
node, the check still passes and no longer means anything.

**The dashboard has been looked at once, not tested visually.** It was
rendered in headless Chrome against synthetic rows in the test database, in
two states: tracked runs present, and legacy rows only. That was one desktop
width in the dark theme. Light theme, narrow screens and real pipeline data
are unseen, and nothing automated checks layout or colour.

**A dropped database connection costs a 503 or two.** The pool hands out a
connection without checking it is alive. After Neon suspends, or anything else
closes the connections, the first requests fail as `unreachable` until the
pool replaces them. This predates the monitoring work. `psycopg_pool` can
check a connection on checkout; that is the likely fix and it is not applied.

**There is no rate limiting on the API.**

**Error redaction is by pattern.** See
[Security boundaries](#security-boundaries).

The baseline was written from a read-only inspection of Neon, not from a dump of
the local container. If the local snapshot still has the older primary key on
`source_job_id`, the baseline will refuse it and say why.

**`UNKNOWN_FIELD` makes tool-schema changes a breaking change.** Any key in the
model's output not in the validator's field list dead-letters the row. So adding
a field to the tool schema dead-letters every row until the Code node is
re-pasted with a validator that knows about it. This is deliberate — a loud
failure beats a field silently vanishing — but schema changes have to be
deployed in the right order, and the wrong order costs a run.

**The schedule only runs while the container does.** n8n holds the cron
in-process, so a daily trigger fires only if `docker compose up` is still
running at the time. Uptime is the laptop's uptime. Close the lid overnight and
that day's run does not happen and is not retried — there is no catch-up on
start. This is a consequence of self-hosting, not a bug, but it means gaps in
`jobs_extracted` are gaps in observation, not evidence about the feed.

Since the dashboard went to Vercel this is more visible, not less. The page is
public and always reachable, so it now looks like a running system whether or
not anything is ingesting. "Time since last run that produced data" measures
laptop uptime as much as pipeline health, and a stale value there usually means
the machine was off rather than that something broke.

**The API has open CORS and no rate limiting.** `allow_origins=["*"]`, so any
site can call it from a browser, and there is nothing throttling requests. It
was set that way so the single HTML file works when nginx serves it from a
different origin than the API. Every endpoint is read-only aggregate counts over
public job postings and there are no credentials to steal, so the exposure is
cost rather than data: a deployed URL can be called in a loop by anyone who
finds it, which burns Vercel invocations and wakes the Neon compute. Real
mitigations would be an origin allowlist and a rate limit; neither is present.

**The dashboard SQL is duplicated into `api/main.py`.** `sql/queries.sql` is
the readable source of truth, with the reasoning in its comments. `main.py`
holds the same statements again as Python constants so the app has no file to
read at startup. This is the same hand-pasted-duplicate problem `validate.js`
has with the n8n Code node, at smaller scale, and nothing enforces that the two
stay in sync. Loading the `.sql` file at import and splitting it would remove
the duplication, at the cost of a fragile split on `;`.

### Accepted trade-offs

These are deliberate decisions with known costs, not defects. Each was argued through, and each would be worth revisiting at a larger scale than this project has.

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

**`tech_stack` and `error_codes` are `jsonb`, not `text[]`.** n8n's Postgres
node maps a JS array to json, not to a native Postgres array type, so a
`text[]` column rejects the insert. The cost is that the natural array
operations are not available: containment is `error_codes @> '["CODE"]'` rather
than `'CODE' = ANY(error_codes)`, and `tech_stack` needs
`jsonb_array_elements_text` to unnest for a tech-frequency chart. Either column
would be a native array if the write path allowed it.

**`dead_letter_log` has no index beyond its primary key.** It is most usefully
queried by `ingested_at` and by `error_codes` containment, and neither is
indexed. `jobs_extracted` has the index its view needs. At this row count none
of it matters. It would.

### Known bugs

Unlike everything above, these are defects rather than trade-offs: the write
path emits something the schema did not intend. Both are in `dead_letter_log`.

**`dead_letter_log.description` is empty in every row.** The validator emits
the field as `description_clean`; the column is named `description`. An earlier
version of this README said an Edit Fields node maps one to the other. It does
not: the exported workflow excludes `description_clean` and assigns nothing to
`description`, so the text is dropped.

The fix is written and not deployed. Migration `0006` adds a `description_clean`
column, so the two names agree and no mapping is needed. The tests insert a real
rejected record into the migrated schema and check the description arrives. The
live table does not have the column yet, and the canvas still drops the field.

**`errors_json` is double-encoded.** It lands in the `jsonb` column as a JSON
*string* rather than an array, so querying it means unwrapping with `#>> '{}'`
and parsing again. `error_codes` and `raw_output` land as a proper array and
object.

The cause is in the canvas, not the validator. The validator stringifies
`errors_json` once, which is what the Postgres node needs. An Edit Fields node
then stringifies it a second time. The fix is deleting that one assignment; see
`docs/n8n-notes.md`, step 8. The tests reproduce the bug by applying the second
encoding, and show a proper array without it. The six existing rows will stay
double-encoded; they are not rewritten, and the dashboard query reads both.

## Not yet built

Three things, in priority order.

### 1. The Error Trigger workflow

**Designed and tested in the database. Not built in n8n.**

Today a run that dies before either Postgres node executes writes nothing, so
the dashboard has no idea it happened. That is still true of the live system.

What now exists in the repository: the function the error workflow calls, with
tests for correlation, duplicates and missing ids; the API endpoints that
report failures and a `pipeline_status`; an external check for the case no
workflow can see; and node-by-node instructions for the two-node workflow in
`docs/n8n-notes.md`.

What does not exist: the workflow itself, a migrated database for it to write
to, and any evidence from a real n8n failure. The tests write failure rows by
calling the function directly. They show the database and API behave correctly
given such rows. They do not show n8n produces them. Until step 13 of
[the runbook](docs/release-runbook.md) is done, that part is a design.

The dashboard page now draws all of it, and has only ever drawn it from
stubbed responses in tests. Against the live warehouse today it would show
Failed, `legacy_data_only`, and empty execution panels.

### 2. The Remotive `publication_date` column

**The volume chart currently measures when the pipeline ran, not when the market
moved.**

First-posted and first-seen are different dates, and only one of them is in the
warehouse. `ingested_at` records when this pipeline observed a posting, which is
a fact about the schedule — and about laptop uptime. `publication_date` records
when the posting went up, which is a fact about the job market. Right now a
ten-day gap in the ingestion chart says the machine was off; it cannot say
whether hiring paused.

**Half built.** `validateBatch()` now puts `publication_date` on every record,
migration `0005` adds the column to both tables, and the tests carry a real
Remotive date from the validator into Postgres and read it back unchanged. What
is missing is deployment: the migration has not been applied, and the workflow
still calls `validate()`, which does not emit the field. No chart uses it yet.

### 3. Explicit runs

Also half built, and for the same reason. The `pipeline_runs` table, the
`run_id` columns and `validateBatch()` exist and are tested together. The API
now groups by `run_id` wherever a row has one and by timestamp gap where it
does not. Nothing live writes a `run_id`, so on the live data every run is
still a `legacy` one. `docs/n8n-notes.md` has the node-by-node changes that
would switch it over.
