# RoleRadar

A scheduled pipeline that pulls remote job postings, uses an LLM to extract
structured fields, validates that output with deterministic code, and routes
failures to a dead-letter branch instead of dropping them.

It tracks postings that mention n8n, and it is built with n8n.

![The RoleRadar workflow in the n8n canvas: Schedule Trigger, HTTP Request to
Remotive, Split Out, Limit, Prep, Claude Extract, Validator, and an IF node
branching to two placeholder nodes named jobs_extracted and dead_letter_log.](docs/pipeline.png)

## What it does

The workflow runs end to end in n8n. One scheduled run does this:

1. **Schedule Trigger** fires the run.
2. **HTTP Request** does `GET https://remotive.com/api/remote-jobs?search=<term>`
   and returns a single item containing the whole result set.
3. **Split Out** turns the `jobs` array into one item per posting.
4. **Limit** caps the item count. This is the cost control, not a convenience:
   one item is one billed Anthropic call.
5. **Prep** builds the request body and strips HTML out of the description.
6. **Claude Extract** does `POST https://api.anthropic.com/v1/messages` with
   `tool_choice: {"type":"tool","name":"extract_job_fields"}`. Forced tool use,
   so the response conforms to the tool's `input_schema`. Nothing is parsed out
   of free text.
7. **Validator** is a Code node running a copy of `validator/validate.js`.
8. **IF** routes on the validator's `valid` boolean.

The last run: 1 item from the API, 20 postings after Split Out, 5 after the
cap, 4 valid and 1 dead-lettered.

The row that dead-lettered is worth describing, because it is the case the
validator exists for. A TELUS Digital posting says the rate of pay is `$0.2333
per completed task` and gives an estimated earning potential of `$14 per hour`.
The model put both numbers into one `hourly` range: `salary_min: 0.2333`,
`salary_max: 14`. That passes every type and bounds check. Both numbers are
real, both are positive, and min is below max. It is only wrong because the two
bounds are in different units, and the only visible signal is that the range
spans 60x. So the validator rejects any range wider than 20x under its own
error code, `SALARY_SPAN_IMPLAUSIBLE`.

The two branches currently end at placeholder NoOp nodes named `jobs_extracted`
and `dead_letter_log`. Nothing is written to a warehouse yet. See
[Not yet built](#not-yet-built).

## How to run it

### The validator

Zero npm dependencies, and the tests use the built-in `node:test` runner.
Nothing to install.

```
cd validator
node --test
```

74 tests, all passing. They run against real captured output from five live
pipeline runs in `validator/fixtures/`, plus the Remotive records those runs
came from. Failure cases are single-line mutations of those fixtures. No test
data is invented.

### The pipeline

1. Copy `.env.example` to `.env` and fill it in. `.env` is gitignored. Read the
   comments in it: n8n does not load that file, and the values marked
   `[n8n UI]` are documentation of credentials you create by hand, not config
   that gets read.

2. Generate an encryption key for n8n's credential store. Losing it means
   re-entering every credential.

   ```
   openssl rand -hex 24
   ```

3. Start n8n.

   ```
   docker compose up -d
   ```

   It listens on `http://localhost:5678`.

4. In the n8n UI, create the credentials: a Header Auth credential for
   Anthropic with header name `x-api-key`, and the Google BigQuery credential
   when there is something to write to.

5. Build the workflow in the canvas. There is no importable workflow JSON in
   this repo, by design — see [Division of labour](#division-of-labour).

6. Paste the contents of `validator/validate.js` into the Validator Code node
   and call `validate()` over the incoming items. The whole file pastes in
   unchanged: the export on the last line is guarded with
   `typeof module !== 'undefined'`, so it is a no-op inside the Code node.

### Costs

Two line items are not free. Both are small here. Neither is zero.

- **Anthropic API.** No free tier, prepaid credits. One call per job item, so
  the Limit node is the ceiling on spend per run. A dev loop that forgets the
  cap and pulls 500 postings costs real money.
- **BigQuery streaming inserts.** n8n's BigQuery node uses `insertAll`, billed
  per GB with no free allowance. Fractions of a cent at this row volume, but it
  never rounds to zero. Not yet incurred, since nothing writes to BigQuery yet.

BigQuery storage and queries stay inside the free tier at this scale. n8n
self-hosted in Docker is free.

## Architecture decisions

### Data validation and infra failure are different events

An HTTP 529 from Anthropic is not the same event as a model returning a bad
`seniority` value. Mixing them makes both un-diagnosable. Validation failures
route through the IF node to the dead-letter branch. Infra failures are meant
to be caught by a separate Error Trigger workflow, which is not built yet.

### `jobs_extracted` will be append-only

Every run inserts. Nothing is updated or deleted. Current state comes from a
view using `QUALIFY ROW_NUMBER() OVER (PARTITION BY source_job_id ORDER BY
ingested_at DESC) = 1`.

A MERGE would overwrite history. First-seen date and re-listing history are
needed for the postings-over-time chart, and once overwritten they are gone. An
immutable ingestion log keeps the raw record of what was seen when, and the view
derives current state on read. Reads are cheap at this scale; lost history is
not recoverable.

Secondary reason: n8n's BigQuery node inserts. It does not MERGE. A MERGE would
mean an Execute Query node with hand-built SQL and string quoting risk.

### Source and model output stay separate

The validator's signature is:

```js
validate({ source, extracted }, ingestedAt) -> { valid, errors, record }
// errors: [{ field, code, message }]
```

`source` and `extracted` are never flattened into one object. Flattening loses
the distinction between what the Remotive API gave us and what the model
produced, which is exactly the distinction the dead-letter branch exists to
record.

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

### `mentions_n8n` is cross-checked, not trusted

The tool schema defines the field narrowly: true if the posting text contains
the token n8n. Not "the role involves n8n". The narrow definition makes a
mismatch unambiguously model error rather than a defensible difference of
interpretation.

The validator independently matches `/\bn8n\b/i` against the tag-stripped
description. If the model's boolean disagrees, the row routes to the dead-letter
branch under its own error code, `N8N_MISMATCH`, separate from schema failures.
This makes model error rate a measurable number rather than a vibe.

The validator does not silently correct the field. Overwriting would hide the
error rate, which is the point of measuring it.

Both values are meant to be persisted, as `mentions_n8n_model` and
`mentions_n8n_actual`. Storing only the agreed value would make error rate
countable at ingestion time and nowhere else. With both columns, error rate is a
query, and it can be recomputed historically and sliced by `model` and
`extraction_version`.

Rows on the valid branch are the ones that agreed, so the two values are equal
there by construction. The disagreements are the interesting ones.

### The description is not stored with the extracted row

Descriptions are long HTML and would dominate the table's storage for no query
benefit. The validator puts the description on the dead-letter record only,
where it is needed to diagnose the failure, alongside the raw LLM output as a
JSON string.

### The validator has no npm dependencies

This is load-bearing, not minimalism. The n8n Code node cannot `require`
arbitrary packages, so `validate.js` has to be one self-contained file. A
dependency-free package means the tests physically cannot pass using something
the Code node would not have. Jest or Vitest would be nicer to write and would
quietly let the tested code drift away from the deployed code.

### Division of labour

This is a learning project. Building the n8n workflow by hand in the canvas and
doing the GCP console setup are the parts being learned, so they are done by
hand and not generated. That is why there is no importable workflow JSON here.
Exports go to `workflows/` for the record once there is something worth
exporting.

## Known limitations

These are real weaknesses, not details.

**`validate.js` is hand-pasted into the n8n Code node.** The file in this repo
is the source of truth for the logic. The copy running in the canvas is a
deployed duplicate. There is no import path from the canvas into this repo, so
the two can drift. When the validator changes, the Code node has to be
re-pasted, and nothing enforces that. Passing tests in this repo are not
evidence that the deployed copy matches.

**Every error is fatal to the row.** A single bad salary discards a perfectly
good title, company, and tech stack. Production would need severity levels, so
that a plausibility failure downgrades or nulls one field while the rest of the
row still lands. The current design is all-or-nothing because it is easier to
reason about and because the dead-letter branch keeps the raw output, so nothing
is actually lost — but it does mean the valid table under-counts.

**The validator is coupled to Anthropic's response shape by design.** It reaches
into `extracted.content[]` looking for a `tool_use` block. Changing model
providers means changing the validator, not just a credential. That coupling
buys the `model` provenance value and the `TOOL_USE_MISSING` check, and it was
chosen deliberately, but it is coupling.

**Fixtures pair positionally.** `fixtures/extraction-output.json` carries no
`source_job_id`, so the only join key between it and `fixtures/source-records.json`
is array index. Reordering or partially re-capturing one file would silently
validate one job's extraction against another job's description. The test suite
asserts the two arrays are the same length so a reorder fails loudly, but equal
length is not the same as correct alignment.

**`UNKNOWN_FIELD` makes tool-schema changes a breaking change.** Any key in the
model's output that is not in the validator's field list dead-letters the row.
So adding a field to the tool schema dead-letters every row until the Code node
is re-pasted with a validator that knows about it. This is deliberate — a loud
failure beats a field silently vanishing — but it means schema changes have to
be deployed in the right order, and the wrong order costs a run.

**`EXTRACTION_VERSION` is defined in two places and they disagree.**
`validate.js` hardcodes `'1.0.0'`. `.env.example` documents
`EXTRACTION_VERSION=v1`. The constant in `validate.js` is the one that reaches
the record; the env value is not wired to anything. This needs reconciling
before rows are written anywhere permanent.

**`source_job_id` has an unresolved type.** Remotive returns it as a number. The
validator coerces it to a string on the record, so that the type does not depend
on what the API feels like sending, and so the `PARTITION BY source_job_id`
window in the view stays stable. The BigQuery DDL is not written yet and has to
agree with that choice.

## Not yet built

Nothing in this section exists. It is listed so the gap between the design and
the state of the repo is visible.

- **BigQuery tables.** `sql/` is empty. No DDL for `jobs_extracted`,
  `dead_letter_log`, or the `jobs_latest` view has been written, and no dataset
  has been created. The two branches of the IF node currently end at NoOp
  placeholders named after the tables they will eventually write to.
- **The dashboard.** `api/` contains an empty `static/` directory. There is no
  FastAPI app, no `/stats` or `/jobs` endpoint, and no Chart.js page.
- **The Error Trigger workflow.** Infra failures — timeouts, non-2xx responses,
  API outages — are currently uncaught. The design calls for a separate n8n
  Error Trigger workflow, kept distinct from data validation failures. It has
  not been built.
- **The workflow export.** `workflows/` does not exist. The workflow lives only
  in the local n8n instance and has not been exported to this repo, so it is not
  backed up and not reviewable here.
- **`docs/n8n-notes.md`.** Referenced by `CLAUDE.md` as the place node choices
  and wiring rationale are recorded. Not written.
