# RoleRadar

Context file for Claude Code sessions on this repo. Read it fully before working.

## What this is

A scheduled pipeline that pulls remote job postings, uses an LLM to extract
structured fields, validates that output with deterministic code, routes failures
to a dead-letter table, writes to BigQuery, and surfaces it in a dashboard.

The meta angle: it tracks postings that mention n8n, and it is built with n8n.

It is also a learning project. The owner is a final-year CS student who is strong
in Python, SQL, FastAPI, Docker, AWS, and React, and has never used n8n, GCP, or
BigQuery. The point is to close those gaps and be able to defend every design
decision in an interview. Explaining beats delivering.

## Done looks like

- n8n running in Docker, with the workflow built by hand in the canvas.
- Validator passing its tests against real captured fixtures, not invented ones.
- Both BigQuery tables populated, including at least one deliberate dead-letter
  row. A dead letter table that has never been exercised is not evidence of
  anything.
- Dashboard rendering from real warehouse data.
- README explaining the architecture and stating the validator duplication
  honestly.
- Workflow JSON exported to `workflows/`.

## Flow

```
Schedule Trigger (n8n cron)
  -> HTTP Request GET https://remotive.com/api/remote-jobs?search=<term>
  -> Split Out -> individual job items, capped during dev by DEV_JOB_LIMIT
  -> HTTP Request POST https://api.anthropic.com/v1/messages
       tool_choice: {"type":"tool","name":"extract_job_fields"}
       Forced tool use, so the response conforms to input_schema.
       Nothing is parsed out of free text.
  -> Code node (JS) running a copy of validator/validate.js
  -> IF valid -> BigQuery jobs_extracted
     else     -> BigQuery dead_letter_log
```

A separate n8n **Error Trigger** workflow catches infra failures: timeouts,
non-2xx responses, API outages. This is deliberately distinct from data
validation failures. An HTTP 529 from Anthropic is not the same event as a model
returning a bad `seniority` value, and mixing them makes both un-diagnosable.

Downstream: BigQuery -> FastAPI (`/stats`, `/jobs`) -> Chart.js page.

## Extraction schema

The `input_schema` of the `extract_job_fields` tool, and how it lands in
`jobs_extracted`.

| field | type | notes |
|---|---|---|
| `normalized_title` | string | required |
| `company` | string | required |
| `seniority` | enum | `junior` \| `mid` \| `senior` \| `lead` \| `unspecified` |
| `tech_stack` | string[] | 0 to 30 items |
| `remote_type` | enum | `remote` \| `hybrid` \| `onsite` \| `unspecified` |
| `salary_min` | number \| null | |
| `salary_max` | number \| null | |
| `salary_period` | enum | `hourly` \| `monthly` \| `yearly` \| `unspecified` |
| `mentions_n8n` | boolean | the model's claim. Stored as `mentions_n8n_model`. See (c) |
| `mentions_n8n_actual` | boolean | **not in the tool schema.** Computed by the validator. See (c) |
| `ai_ml_role` | boolean | |

## Decisions

These were argued through and settled. Do not quietly revisit them.

### (a) jobs_extracted is append-only

Every run inserts. Nothing is updated or deleted. Current state comes from a view
in `sql/03_jobs_latest_view.sql`:

```sql
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY source_job_id ORDER BY ingested_at DESC
) = 1
```

**Why:** a MERGE would overwrite history. First-seen date and re-listing history
are needed for the postings-over-time chart, and once overwritten they are gone.
An immutable ingestion log keeps the raw record of what was seen when, and the
view derives current state on read. Reads are cheap at this scale; lost history
is not recoverable.

**Secondary reason:** n8n's BigQuery node inserts. It does not MERGE. A MERGE
would mean an Execute Query node with hand-built SQL and string quoting risk.

The table is partitioned on `DATE(ingested_at)` and clustered on `source_job_id`.
Partitioning bounds the scan for time-range charts; clustering makes the
`PARTITION BY source_job_id` window in the view cheap.

### (b) Provenance columns and the validator contract

`jobs_extracted` carries the extracted fields plus provenance. Note that the
ten schema fields become eleven columns, because `mentions_n8n` splits into
`mentions_n8n_model` and `mentions_n8n_actual` per decision (c). Provenance:

| column | source | notes |
|---|---|---|
| `source_job_id` | Remotive | type unresolved. See below. |
| `source_url` | Remotive | |
| `raw_title` | Remotive | |
| `raw_company` | Remotive | |
| `publication_date` | Remotive | when the posting went up, distinct from `ingested_at` |
| `ingested_at` | argument to `validate()` | when this pipeline saw it |
| `model` | Anthropic response | |
| `extraction_version` | constant in `validate.js` | |

**Why:** without these, a row cannot be traced back to the posting it came from,
and a dead-letter row is an error message attached to nothing. `model` and
`extraction_version` mean rows stay comparable after the prompt or tool schema
changes. `extraction_version` currently has two conflicting definitions:
`validate.js` hardcodes `'1.0.0'` and `.env.example` documents
`EXTRACTION_VERSION=v1`, which is not wired to anything. Reconcile before
writing rows anywhere permanent.

**`publication_date` is not yet emitted by the validator.** It is in the
Remotive records and in `validator/fixtures/source-records.json`, and it belongs
here because first-posted and first-seen are different dates and the
postings-over-time chart needs to distinguish them. `validate.js` does not put
it on either record yet, and `validate.test.js` asserts the exact record shape,
so adding it is a code change plus a test change. Open item.

**`source_job_id` type is unresolved.** Remotive returns it as a number.
`validate.js` coerces it to a string on the record, so the column type does not
depend on what the API feels like sending, and so the `PARTITION BY
source_job_id` window in the view stays stable across a format change upstream.
The BigQuery DDL is not written yet. When it is, it has to agree with that
coercion — STRING, not INT64 — or the insert fails.

The full job description is **not** written to `jobs_extracted`. Descriptions are
long HTML and would dominate the table's storage for no query benefit. It is
written to `dead_letter_log` only, where it is needed to diagnose the failure.

`dead_letter_log` stores the raw LLM output as a JSON string plus the full
description.

**Validator signature:**

```js
validate({ source, extracted }, ingestedAt) -> { valid, errors, record }
// errors: [{ field, code, message }]
```

`source` and `extracted` stay separate and are never flattened. Flattening loses
the distinction between what the API gave us and what the model produced, which
is exactly the distinction the dead letter table exists to record.

`ingested_at` is passed in as an argument. The validator never calls `Date.now()`
internally. It is a pure function, so tests are deterministic and there is no
clock to stub.

Every check produces a specific `{ field, code, message }`, never a bare boolean.
The dead letter table has to be diagnosable, and `valid: false` tells you nothing.

### (c) mentions_n8n is cross-checked, not trusted

The tool schema defines the field narrowly:

> `true` if the posting text contains the token n8n

Not "the role involves n8n". The narrow definition makes a mismatch unambiguously
model error, rather than a defensible difference of interpretation.

The validator independently matches `/\bn8n\b/i` against the tag-stripped
description. If the model's boolean disagrees with the regex, the row routes to
`dead_letter_log` under its **own error code**, separate from schema failures.

**Why:** this makes model error rate a measurable number rather than a vibe. It
is a cheap ground-truth check on LLM output, which is the interesting thing to
defend about this pipeline. Keeping the error code distinct means schema failures
and model disagreement can be counted separately.

The validator does not silently correct the field. Overwriting would hide the
error rate, which is the whole point of measuring it.

**Both values are persisted.** `jobs_extracted` has two boolean columns:
`mentions_n8n_model` (what the model said) and `mentions_n8n_actual` (what the
regex found). Storing only the agreed value, or only a pass/fail flag, would make
model error rate countable at ingestion time and nowhere else. With both columns
in the warehouse, error rate is a query, and it can be recomputed historically,
sliced by `model` and `extraction_version`, and charted over time.

Rows reaching `jobs_extracted` are the ones that agreed, so the two columns are
equal there by construction. That is deliberate. The disagreements live in
`dead_letter_log`, and the rate is `dead_letter` count over total, per model
version. Keeping both columns in the main table means the join is not needed to
sanity-check the invariant.

## Division of labour

**Claude owns the repo:** validator and its tests, docker-compose for n8n,
BigQuery DDL, FastAPI dashboard, Chart.js frontend, README, this file.

**The owner owns:** building the n8n workflow by hand in the canvas, and all GCP
console setup (project, API enablement, service account, IAM).

**Do not generate n8n workflow JSON for import.** That is the part being learned.
Naming node types and explaining why to reach for them is fine and useful. Wiring
them is not Claude's job. Notes for that live in `docs/n8n-notes.md`.

## Working agreement

- One task at a time. Then stop and wait. Never chain ahead.
- Before writing code that involves a design choice, state the choice, the
  alternative rejected, and why. Three or four sentences, before the code.
- If the owner could reasonably write it themselves, ask before writing it.
- Do not scaffold files for later. Only what the current task needs.
- Flag anything that costs money before it runs. Everything is on free tiers.
- No secrets in the repo. `.env` plus `.env.example`, gitignored from the start.
- Write plainly. No "seamlessly", no "robust", no "leverage", no bullet-point
  filler in docs. Short sentences.
- If the owner is about to do something dumb, say so directly.

## Costs

Two line items are not free. Both are small here. Neither is zero.

- **Anthropic API.** No free tier, prepaid credits. One call per job item, so the
  Split node cap is the cost control. A dev loop that forgets the cap and pulls
  500 postings per run costs real money.
- **BigQuery streaming inserts.** n8n's BigQuery node uses `insertAll`, billed
  per GB with no free allowance. Fractions of a cent at this row volume, but it
  never rounds to zero.

BigQuery storage and queries stay inside the free tier at this scale. n8n
self-hosted in Docker is free. GCP project creation is free.

## Repo layout

```
CLAUDE.md              this file
README.md              setup and run instructions
docker-compose.yml     n8n, at root so `docker compose up` needs no -f
.env.example           documented config, no real values
validator/
  package.json         zero dependencies, on purpose
  validate.js          the module, self-contained
  validate.test.js     node:test
  fixtures/            captured API responses and LLM outputs
sql/
  01_jobs_extracted.sql
  02_dead_letter_log.sql
  03_jobs_latest_view.sql
api/
  requirements.txt
  main.py              FastAPI, /stats and /jobs
  static/              index.html, app.js, Chart.js page
docs/
  n8n-notes.md         node choices and wiring rationale
workflows/
  *.json               exported from the n8n canvas, for the record.
                       Export only. Never hand-generated by Claude.
```

Top level splits by runtime, not by feature: JS, SQL, Python, prose. A feature
split would scatter three languages into every folder for no gain at this size.

`validator/` has **zero npm dependencies** and uses the built-in `node:test`
runner. This is load-bearing. The n8n Code node cannot `require` arbitrary
packages, so `validate.js` has to be one self-contained file. A dependency-free
package means the tests physically cannot pass using something the Code node
would not have. Jest or Vitest would be nicer to write and would quietly let the
tested code drift away from the deployed code.

## Known duplication

`validator/validate.js` is the source of truth for the validation logic. The copy
running inside the n8n Code node is a **deployed duplicate**, pasted by hand.
There is no import path from the canvas into this repo.

This means the two can drift. That is a real weakness of the design, not a
detail to paper over. When the validator changes, the Code node has to be
re-pasted, and nothing enforces that. The README says this plainly. Do not write
docs that imply the repo is the single source of truth at runtime.
