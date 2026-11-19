# Release runbook

How to put the run-tracking work into production without taking the dashboard
down. Eighteen checkpoints. Each says who runs it, what should happen, how to
check, when to stop, and how to get back.

**None of this has been done.** Neon, Vercel and n8n are as they were on
8 September 2026. What has been done locally, against disposable Postgres 16
and Postgres 18.7 servers, is a rehearsal of every database step, including
the backup and restore commands in [Appendix A](#appendix-a--back-up-and-prove-the-backup),
run verbatim against a Postgres 18 stand-in for Neon. That is evidence about
the migrations, the two API versions and the commands. It is not evidence
about Neon, Vercel or n8n.

## What is known about the deployment target

Read from GitHub on 18 November 2026, read-only.

| fact | evidence |
|---|---|
| Vercel is connected to the repository through its GitHub app | every commit on `main` since 7 September has a deployment created by `vercel[bot]` and a `Vercel` status check |
| The production branch is `main` | those deployments are all in the `Production` environment, and `main` is the default branch |
| A push to `main` deploys to production with no further step | four pushes, four production deployments, each within seconds |
| `main` has no branch protection | the branches API reports `protected: false` |
| The repository is public | the repository API |
| No branch other than `main` has ever been pushed | so it is **not known** whether Preview deployments get the production `DATABASE_URL` |

The last line matters. In Vercel's settings an environment variable can be
scoped to Production, Preview and Development, and the form ticks all three by
default. If `DATABASE_URL` was saved that way, the first push of any other
branch builds a preview that connects to production Neon.

That preview would run the new API against the old schema. It can only read:
every query is a `SELECT`, and since this milestone every pooled transaction
is `READ ONLY`, enforced by Postgres. So it cannot damage data. It would
answer 503 on most endpoints, wake the Neon compute, and prove nothing. Fix
it before pushing, at checkpoint 2.

## The one rule

**The database goes first. The API goes second.**

The new API reads columns and tables that only exist after the migrations.
Deployed against the old schema, `/api/runs`, `/api/summary`,
`/api/model-accuracy` and the three `pipeline-*` endpoints answer 503 with
`reason: schema_outdated`, and the live dashboard goes dark. The other order
is safe: the API deployed today never mentions the new columns, so it runs
unchanged on the new schema. The rehearsal tests both directions on Postgres
16 and 18.

## Two kinds of rollback

| | revert the application | revert the schema |
|---|---|---|
| how | Vercel: promote the previous deployment. n8n: restore the previous workflow version | `python sql/migrate.py down --to NNNN` |
| takes | seconds | seconds, plus a backup first |
| loses | nothing | every value in the dropped columns: all `run_id`s, all of `pipeline_runs` |
| safe because | the old API and the old workflow both run unchanged on the new schema | it is not; it is lossy by design |

**In an emergency, revert the application and leave the schema.** The new
columns are nullable and nothing old reads them. Dropping them destroys the
record of which run wrote which row, and that cannot be rebuilt. `down` is for
a schema that was applied by mistake and has nothing in it yet.

## Who does what

- **[local]** Can be run in this repository against disposable databases. No
  account, no live service. Claude can run these.
- **[owner]** Needs your accounts (GitHub, Vercel, Neon, n8n) or touches a live
  service. Yours to run and yours to decide. Claude does not run these.

## Before you start

- A clean branch, not `main`. See checkpoint 1.
- Docker, with about 400 MB free for the `postgres:18-alpine` image if it is
  not already pulled. It provides `pg_dump`, `pg_restore` and `psql` at
  version 18, which Neon needs: `pg_dump` refuses a server newer than itself,
  and Neon runs Postgres 18. Nothing needs installing on the Mac itself.
- Neon's **direct** connection string, the host without `-pooler`. Dumps and
  migrations hold a session; the pooled endpoint is a transaction pooler.
- A backup folder **outside** `~/Documents`. That folder is synced to iCloud,
  and iCloud has been evicting this repository's files under disk pressure.
  `~/roleradar-backups` is fine.
- n8n stopped, or its schedule not due for an hour.

Put the connection string in the shell without echoing it and without it
reaching your shell history:

```
read -rs TARGET && export TARGET
```

Paste, press Enter. Nothing is printed. Every command below reads `$TARGET`
from the environment and none prints it. When you are done: `unset TARGET`.

---

## The checklist

### 1. Verify a clean Git state and a tested commit — [local] then [owner]

**Do.** Commit the work on a branch (the suggested split is in the final
report), then run every suite on that exact commit:

```
git switch -c run-tracking        # before committing; never on main
git status --short                # must print nothing after committing
cd validator && node --test && cd ..
node --test tests/frontend/*.test.js
docker compose -f docker-compose.test.yml up -d --wait
python -m pytest tests
docker compose -f docker-compose.test.yml down
```

Then **[owner]**, after checkpoint 2: `git push origin run-tracking`. GitHub
Actions runs the same suites on Postgres 16 and 18.

**Expect.** 149 + 45 + 329 tests pass locally. The Actions run is green on both
Postgres versions.

**Stop if.** Anything fails, the working tree is not clean, or you are on
`main`.

**Recovery.** Nothing has happened. Fix and repeat.

### 2. Confirm the deployment target and production branch — [owner]

**Do.** In Vercel → the project → Settings:

- **Git**: confirm the connected repository is `angelina10504/roleradar` and
  the production branch is `main`.
- **Environment Variables**: find `DATABASE_URL`. If it is ticked for
  **Preview**, untick Preview, or give Preview a different value (see below).
- **Deployment Protection**: note whether previews require a Vercel login.

For a useful preview, give Preview its own database: a **Neon branch** of
production. In the Neon console, create a branch from the production branch
named `preview-run-tracking`. It is a copy-on-write copy of today's data that
costs nothing until written to. Migrate **that branch** to `0009` with the
commands in checkpoint 5, pointed at the branch's direct URL, and set it as
the Preview `DATABASE_URL`. The preview then shows the new API on real data
and the new schema, and production is untouched.

**Expect.** Production `DATABASE_URL` is scoped to Production only. Preview
has either nothing or the Neon branch.

**Stop if.** You cannot tell which database Preview would use.

**Recovery.** Nothing has been deployed. Do not push until this is clear.

### 3. Verify a Neon backup and restore — [owner]

**Do.** [Appendix A](#appendix-a--back-up-and-prove-the-backup), all of it.
Also create a Neon branch of production named `pre-run-tracking-<date>`: the
fast way back if something goes wrong in checkpoint 5.

**Expect.** The script ends with `BACKUP VERIFIED`. The fingerprint of the
restored copy is identical, line for line, to the fingerprint of Neon.

**Stop if.** The fingerprints differ, the restore fails, or the script stops
anywhere. A backup that has not been restored and compared is not a backup.

**Recovery.** Nothing has changed in Neon. Read the error. A restore failure
on an object Neon adds itself (an extension, a role) is worth understanding
before deciding it is harmless.

### 4. Record the existing row counts and schema — [owner]

**Do.** Appendix A already saved both: `preflight-<stamp>.txt` and
`source-<stamp>.fingerprint`. Open the preflight file and read it against
this list:

- `rows.jobs_extracted` and `rows.dead_letter_log`: write the two numbers
  down. As of 18 November the live tables held 25 and 6, the last write on
  11 September.
- `ledger_present` is `false`.
- `jobs_extracted_pk_is_id` is `true`.
- every `column.*` line says `absent`, and both `function.*` lines.
- both `collision.*` lines say `false`.
- `other_sessions_on_this_database` is small, and n8n is not one of them.

**Expect.** As above.

**Stop if.** A collision is `true`, a column is already `present`, or the
primary key check is `false`. Each means Neon is not in the state the
migrations were written against.

**Recovery.** Nothing has changed. Find out who created the object.

### 5. Apply the reviewed migrations — [owner]

**Do.** First see exactly what will run:

```
python sql/migrate.py --database-url "$TARGET" \
    --allow-remote --confirm-host <the host in TARGET> status
```

Nine lines, `0001` to `0009`, all `pending`. [Appendix C](#appendix-c--what-the-migrations-change-in-neon)
lists every object they add. Then apply, one file at a time, so you can stop
between them:

```
python sql/migrate.py --database-url "$TARGET" \
    --allow-remote --confirm-host <host> up --to 0001
```

and again with `--to 0002`, and so on to `0009`. Each file is one transaction
with its ledger row: it applies completely or not at all.

**Expect.** After the last one, `status` shows nine `applied`, and the ledger
matches [Appendix D](#appendix-d--the-expected-ledger). Run the preflight
again: the two row counts equal checkpoint 4's, `ledger_latest_version` is
`0009`, `rows.pipeline_runs` is `0`.

**Stop if.** A file fails, a checksum is refused, or a row count changed.

**Recovery.** A failed file left nothing behind and the files before it are
harmless to leave: the deployed API works at every point in the chain, which
the rehearsal checks at `0003`, `0005`, `0007` and `0008`. Do not reach for
`down` to tidy up; fix the cause and run `up` again. A changed row count
should be impossible; if it happens, stop everything and use the Neon branch
from checkpoint 3.

### 6. Verify the deployed API still works — [owner]

Production still runs the old code.

```
for p in health runs summary error-classes model-accuracy; do
  curl -s -o /dev/null -w "%{http_code} $p\n" https://roleradar-snowy.vercel.app/api/$p
done
```

Open the dashboard and look at it.

**Expect.** Five `200`s, and the same numbers as before.

**Stop if.** Anything other than `200` after one retry for a sleeping
database.

**Recovery.** The rehearsal says this cannot happen, so first rule out Neon
being asleep or down. If it is real, this is the one case where reverting the
schema is reasonable: nothing has been written to the new columns yet, so
`down --to 0001` loses nothing. Take a fresh dump first.

### 7. Deploy the new API — [owner]

**Do.** If checkpoint 2 gave Preview the migrated Neon branch, check the
preview first:

```
curl -s https://<preview host>/api/health
```

Then merge `run-tracking` into `main` and push, or promote the preview in
Vercel. Either one is a production deployment.

**Expect.** The preview's health says `"compatible": true`. The production
deployment finishes in Vercel without errors.

**Stop if.** The preview says `compatible: false`, or the build fails.

**Recovery.** Do not merge. Nothing in production has changed.

### 8. Verify /api/health and schema.compatible — [owner]

```
curl -s https://roleradar-snowy.vercel.app/api/health
```

**Expect.**
`{"database": "up", ..., "schema": {"version": "0009", "required": "0009", "compatible": true}}`

**Stop if.** `compatible` is `false`, or the response has no `schema` key
(the old code is still serving).

**Recovery.** `false` means the API is ahead of the database: promote the
previous production deployment in Vercel (Deployments → the previous one →
Promote, or Instant Rollback). After an instant rollback, Vercel may hold
further pushes back from production until you undo it in the project's
deployment settings.

### 9. Verify the old dashboard endpoints — [owner]

```
for p in runs summary error-classes model-accuracy; do
  curl -s -o /dev/null -w "%{http_code} $p\n" https://roleradar-snowy.vercel.app/api/$p
done
```

**Expect.** All `200`. The data-quality half of the dashboard shows the same
numbers as at checkpoint 6. `/api/runs` rows now carry `run_source: "legacy"`
and `run_id: null`; nothing else in them changed.

**Stop if.** Any is not `200`, or an old number moved.

**Recovery.** Promote the previous deployment. Leave the database alone.

### 10. Verify the new monitoring endpoints — [owner]

```
for p in pipeline-runs pipeline-errors pipeline-status; do
  curl -s -o /dev/null -w "%{http_code} $p\n" https://roleradar-snowy.vercel.app/api/$p
done
curl -s https://roleradar-snowy.vercel.app/api/pipeline-status
```

**Expect.** All `200`. `pipeline_status` is `failed` with reasons
`legacy_data_only` and `data_overdue`. That is correct: no run has been
tracked yet and the newest row is from September. The dashboard's pipeline
band says Failed, and the executions panels say there is nothing yet.

**Stop if.** A 503, or a status other than `failed` with those reasons.

**Recovery.** Promote the previous deployment.

### 11. Publish and configure the Error Trigger workflow — [owner]

**Do.** `docs/n8n-notes.md`, E1 to E4. Two nodes. **Publish** it: n8n 2.35.7
only runs the published version of an error workflow and logs "is not active
and cannot be executed" otherwise. Do this before the ingestion workflow, so a
failure at checkpoint 13 or 14 is recorded.

**Expect.** E5's wiring check, run by hand, writes one row to
`pipeline_errors` with `run_id` NULL.

**Stop if.** `Record Error` fails.

**Recovery.** Unpublish it. Nothing depends on it yet.

### 12. Update the ingestion workflow — [owner]

**Do.** Note the current version of the workflow in n8n's history, so there is
something to go back to. Then `docs/n8n-notes.md` steps 1 to 10, including
re-pasting `validator/validate.js` into the Validator node. Set its Error
Workflow to the one from checkpoint 11. Keep the Limit node at its current
value. Publish.

**Expect.** The canvas matches the target shape in the notes, and
`Complete Run` is the lowest node.

**Stop if.** Any step in the notes does not match what the canvas offers.

**Recovery.** Restore the previous version from the workflow's history. The
old workflow runs on the migrated database unchanged.

### 13. Run a controlled successful ingestion — [owner], costs money

One Claude call per item up to the Limit.

**Do.** Run it once, or let the schedule fire. Then the queries in
`docs/n8n-notes.md` step 11.

**Expect.** The newest `pipeline_runs` row is `completed`. Its counts equal the
rows written. `pipeline_status` is `healthy`, or `degraded` with only
`recent_failures` if checkpoint 11 left a test error inside the window.

**Stop if.** The run is `failed` or still `running`.

**Recovery.** Read `pipeline_errors` and the n8n execution before retrying,
because each retry costs Claude calls. "closed too early" or "not fully
written" means `Complete Run` is not the lowest node. To abandon, restore the
previous workflow version.

### 14. Run a controlled triggered failure — [owner], free

**Do.** `docs/n8n-notes.md`, E5, "A real failure": a Stop and Error node
between `Start Run` and `HTTP Request`, then **wait for the schedule**. Do not
press Execute: n8n 2.35.7 never calls the error workflow for a manual
execution. The node sits before both HTTP requests, so nothing is fetched and
no Claude call is made.

**Expect.** The run is `failed`. Its `pipeline_errors` row has the same
`run_id`.

**Stop if.** No error row appears.

**Recovery.** The error workflow was not called. Check it is published and
selected in the ingestion workflow's settings, and look in the n8n container
log for `Calling Error Workflow`. The run sits at `running` and turns stale,
which is harmless.

### 15. Verify run correlation and dashboard status — [owner]

```sql
SELECT r.run_id, r.n8n_execution_id, r.n8n_workflow_id, r.status,
       e.execution_id, e.workflow_id, e.error_category, e.failed_node
FROM public.pipeline_runs r
JOIN public.pipeline_errors e ON e.run_id = r.run_id
ORDER BY r.started_at DESC LIMIT 1;
```

and `https://roleradar-snowy.vercel.app/api/pipeline-status`.

**Expect.** `n8n_execution_id = execution_id` and
`n8n_workflow_id = workflow_id`. The status is `failed` with
`last_run_failed`. `/api/pipeline-errors` shows the error with a category and
a node, and `message: null`. The dashboard's pipeline band says Failed and the
last-failed tile names the category.

**Stop if.** The join returns nothing: the error was recorded without a run.

**Recovery.** Compare the two id pairs by hand. A mismatch in workflow id
means the error came from a different workflow than the run.

### 16. Remove temporary test nodes — [owner]

**Do.** Delete the Stop and Error node, restore the schedule, publish.

**Expect.** The next scheduled run completes, and `pipeline_status` returns to
`healthy`, or `degraded` with `recent_failures` until the 24-hour window
passes.

**Stop if.** The published version still contains a Stop and Error or a Wait
node: every run will fail.

**Recovery.** Remove it and publish again.

### 17. Export the verified workflows — [owner]

**Do.** Export both workflows to `workflows/` and commit them. Read the JSON
first: credentials are stored by reference, not by value, but check for
anything else that should not be public. The repository is public.

**Expect.** Two files whose node list matches the notes.

**Stop if.** An export contains a secret value.

**Recovery.** Remove it before committing. If it was pushed, rotate the secret.

### 18. Enable and verify the freshness monitor — [owner]

**Do.** GitHub → the repository → Settings → Secrets and variables → Actions
→ **Variables**: `ROLERADAR_STATUS_URL`, `ROLERADAR_SCHEDULE`,
`ROLERADAR_SCHEDULE_TZ`, matching the n8n Schedule Trigger. Then run
"Freshness check" once by hand from the Actions tab.

**Expect.** It exits 0 and prints the run from checkpoint 16.

**Stop if.** It exits 2. That is the API, not the pipeline.

**Recovery.** Delete `ROLERADAR_STATUS_URL`; the job goes back to being
skipped.

---

## Appendix A — back up and prove the backup

Run from the repository root, with `TARGET` set as in
[Before you start](#before-you-start). Read-only against Neon: `psql` runs
the two SQL files inside `READ ONLY` transactions, and `pg_dump` only reads.
It writes only to `BACKUP_DIR` and to a throwaway container on port 5435.

```
export REPO="$PWD" BACKUP_DIR="$HOME/roleradar-backups"
mkdir -p "$BACKUP_DIR"
sh -c "$(sed -n '/^<!-- shell:backup -->$/,/^<!-- \/shell:backup -->$/p' docs/release-runbook.md | sed '1,2d;$d' | sed '$d')"
```

That runs the block below, exactly as written here, so these docs and the
commands cannot drift. Or paste the block yourself.

<!-- shell:backup -->
```sh
set -eu
IMG=postgres:18-alpine
STAMP=$(date +%Y%m%d-%H%M%S)
DUMP="roleradar-$STAMP.dump"
cd "$BACKUP_DIR"

# Versions. pg_dump must be the server's major version or newer.
docker run --rm "$IMG" pg_dump --version
docker run --rm -e TARGET "$IMG" sh -c 'psql "$TARGET" -XAtc "SHOW server_version"'

# What is there now, read-only. Keep both files: they are checkpoint 4.
docker run --rm -i -e TARGET "$IMG" sh -c 'psql "$TARGET" -X -v ON_ERROR_STOP=1 -f -' \
  < "$REPO/sql/preflight.sql" > "preflight-$STAMP.txt"
docker run --rm -i -e TARGET "$IMG" sh -c 'psql "$TARGET" -XqAt -v ON_ERROR_STOP=1 -f -' \
  < "$REPO/sql/fingerprint.sql" > "source-$STAMP.fingerprint"

# The dump. Public schema only, and no owners or grants: those belong to the
# Neon project, and a restore anywhere else would fail on them.
docker run --rm -e TARGET -e DUMP="$DUMP" -v "$PWD":/backups "$IMG" sh -c \
  'pg_dump "$TARGET" --schema=public --format=custom --no-owner --no-privileges --file="/backups/$DUMP"'
shasum -a 256 "$DUMP" > "$DUMP.sha256"

# A readable archive, holding the tables it should.
docker run --rm -v "$PWD":/backups "$IMG" pg_restore --list "/backups/$DUMP" > "$DUMP.toc"
grep -E 'TABLE DATA public (jobs_extracted|dead_letter_log|pipeline_errors) ' "$DUMP.toc"

# Restore into a throwaway Postgres 18. tmpfs: nothing outlives the container.
docker run -d --rm --name rr-restore -e POSTGRES_PASSWORD=restore \
  -p 127.0.0.1:5435:5432 --tmpfs /var/lib/postgresql "$IMG" > /dev/null
until docker exec rr-restore pg_isready -h 127.0.0.1 -U postgres -q; do sleep 1; done
docker exec rr-restore createdb -h 127.0.0.1 -U postgres roleradar_restore
docker cp "$DUMP" rr-restore:/tmp/restore.dump
# A --schema=public dump starts with CREATE SCHEMA public, which every new
# database already has. Restore from the archive's own list minus that entry,
# so --exit-on-error can stay on and stop on anything real.
docker exec rr-restore sh -c 'pg_restore --list /tmp/restore.dump \
  | grep -v -E " SCHEMA - public | COMMENT - SCHEMA public " > /tmp/restore.list'
docker exec rr-restore pg_restore -h 127.0.0.1 -U postgres -d roleradar_restore \
  --exit-on-error --no-owner --no-privileges -L /tmp/restore.list /tmp/restore.dump

# The same fingerprint, or the backup is not a backup.
docker exec -i rr-restore psql -h 127.0.0.1 -U postgres -d roleradar_restore \
  -XqAt -v ON_ERROR_STOP=1 -f - < "$REPO/sql/fingerprint.sql" > "restored-$STAMP.fingerprint"
diff "source-$STAMP.fingerprint" "restored-$STAMP.fingerprint"
echo "BACKUP VERIFIED: $BACKUP_DIR/$DUMP"
echo "rr-restore is still running on 127.0.0.1:5435. Appendix B uses it. Stop it with: docker stop rr-restore"
```
<!-- /shell:backup -->

What it proves: the archive restores into a clean Postgres 18 with
`--exit-on-error`, and every table's row count and row content, every column,
constraint, index, view, function and sequence value is the same as in Neon at
the moment of the dump. The fingerprint is taken just before the dump, so a
write in between would show as a difference. With n8n stopped there are none.

What it does not prove: that the restored copy is complete for anything
outside the `public` schema. Nothing of this project lives outside it.

**Using it.** To recover, restore the same archive the same way into an
empty Neon database, then point the API at it:

```
docker run --rm -e TARGET -e DUMP=<file>.dump -v "$BACKUP_DIR":/backups postgres:18-alpine sh -c '
  pg_restore --list "/backups/$DUMP" | grep -v -E " SCHEMA - public | COMMENT - SCHEMA public " > /tmp/l
  pg_restore -d "$TARGET" --exit-on-error --no-owner --no-privileges -L /tmp/l "/backups/$DUMP"'
```

Here `TARGET` is the **new, empty** database, never the one the dump came
from. Nothing in this command drops anything, so pointed at a database that
already has the tables it stops at the first one with "already exists" and
changes nothing else.

Rehearsed on 18 November 2026 against a Postgres 18.7 server loaded with
`tests/fixtures/legacy_*.sql` as a stand-in for Neon: the block ran as
written and printed `BACKUP VERIFIED`, with 78 identical fingerprint lines.
The first rehearsal did not: it found the `CREATE SCHEMA public` problem
above, and an environment variable that never reached the container. Both are
fixed in the block. That is the case for rehearsing before the real thing.

## Appendix B — rehearse the migrations on the restored copy

While `rr-restore` is still running, it holds a copy of production. Migrating
that copy is the closest rehearsal there is, and it is local:

```
python sql/migrate.py --database-url postgresql://postgres:restore@127.0.0.1:5435/roleradar_restore status
python sql/migrate.py --database-url postgresql://postgres:restore@127.0.0.1:5435/roleradar_restore up
docker exec -i rr-restore psql -h 127.0.0.1 -U postgres -d roleradar_restore -X -f - < sql/preflight.sql
```

Then serve the new API from it and look at the dashboard:

```
cd api && DATABASE_URL=postgresql://postgres:restore@127.0.0.1:5435/roleradar_restore \
  uvicorn main:app --port 8000
```

`http://127.0.0.1:8000` should show the same data-quality numbers as the live
site, and pipeline health Failed with `legacy_data_only`. Then
`docker stop rr-restore`; the copy is gone.

## Appendix C — what the migrations change in Neon

Taken by fingerprinting a Postgres 18.7 copy of the legacy-shaped warehouse
before and after `up`, and diffing. Additions only. Nothing is removed,
renamed, retyped or rewritten.

| kind | added |
|---|---|
| tables | `pipeline_runs` (12 columns, 6 constraints), `schema_migrations` (the ledger) |
| columns, all nullable, no default | `jobs_extracted.run_id`, `jobs_extracted.publication_date`, `dead_letter_log.run_id`, `dead_letter_log.publication_date`, `dead_letter_log.description_clean`, `pipeline_errors.run_id`, `pipeline_errors.error_category`, `pipeline_errors.workflow_id`, `pipeline_runs.n8n_workflow_id` |
| foreign keys | `run_id` on `jobs_extracted`, `dead_letter_log` and `pipeline_errors`, to `pipeline_runs` |
| indexes | `jobs_extracted_run_id_source_job_id_key` and `dead_letter_log_run_id_source_job_id_key` (partial unique, `WHERE run_id IS NOT NULL`), `pipeline_errors_run_id_idx`, `pipeline_errors_occurred_at_idx`, `pipeline_runs_started_at_idx`, `pipeline_runs_n8n_execution_id_idx` |
| functions | `complete_pipeline_run(uuid, integer, integer, jsonb, jsonb)`, `record_pipeline_error(text × 7)` |
| view | `jobs_latest` is replaced with the same columns plus `publication_date` and `run_id` at the end |

Row counts are unchanged. Every original value in every original column is
byte-identical before and after; the tests check that with an MD5 over every
original column of every row. Adding a nullable column with no default does
not rewrite a table in Postgres 11 or later, so the locks are brief.

## Appendix D — the expected ledger

After checkpoint 5:

```sql
SELECT version, name, left(checksum, 16) FROM public.schema_migrations ORDER BY version;
```

| version | name | checksum starts |
|---|---|---|
| 0001 | baseline | `51b5d027f32a1a84` |
| 0002 | pipeline_runs | `7a2d9b4ced371b67` |
| 0003 | run_correlation | `bcb49daef9af2166` |
| 0004 | run_uniqueness | `cc69adbbebdf164e` |
| 0005 | publication_date | `8c8328a057827bf7` |
| 0006 | dead_letter_description_clean | `263e9410cde87e09` |
| 0007 | jobs_latest_columns | `476a9d7b777041d9` |
| 0008 | workflow_identity | `d0d745ccb2e9825b` |
| 0009 | run_lifecycle_functions | `cc22625ccb63112e` |

A different checksum means the migration file changed after this was written.
`migrate.py` refuses an applied file whose checksum has changed, so a
mismatch here is caught on the next run either way.

## Optional: the operator token

To read error messages through the API instead of through SQL, set `OPS_TOKEN`
in Vercel's environment variables to a random string of 24 characters or more
(`openssl rand -hex 24`), scoped to Production, and redeploy. Then:

```
curl -s -H "X-Ops-Token: $OPS_TOKEN" https://roleradar-snowy.vercel.app/api/pipeline-errors
```

Without it there is no operator mode and messages are reachable only in the
database. That is a fine place to leave it.

## If it goes wrong later

| symptom | first move |
|---|---|
| Dashboard panels say "schema is older than this API expects" | The API is ahead of the database. Promote the previous Vercel deployment, then find out why checkpoint 5 did not land |
| Every run fails at `Complete Run` | Its position on the canvas. Read the error text in `pipeline_errors` |
| Runs sit at `running` and go stale | n8n is dying mid-run, or the error workflow cannot write. Check the container log |
| `pipeline_status` is `failed` with `completed_run_overdue` | The scheduler did not run. Is the laptop on |
| An insert fails with "Column ... does not exist" | The Postgres credential points at a database that was not migrated |
| You want everything gone | Restore the previous workflow, promote the previous deployment. Stop there. Only then, with a fresh dump, consider `down` |
