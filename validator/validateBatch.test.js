'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { validate, validateBatch, EXTRACTION_VERSION } = require('./validate.js');

// The same captured fixtures validate.test.js uses. Nothing here calls
// Anthropic or Remotive. Failure cases are mutations of the real pairs.
// Fixture 0 is the TELUS posting, which really does dead-letter; 1 to 4 pass.
const SOURCES = require('./fixtures/source-records.json');
const OUTPUTS = require('./fixtures/extraction-output.json');

const INGESTED_AT = '2026-08-23T12:00:00.000Z';
const RUN_A = '3f2b8c1e-5d4a-4e6f-9a7b-0c1d2e3f4a5b';
const RUN_B = 'a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d';

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function pair(i) {
  return { source: clone(SOURCES[i]), extracted: clone(OUTPUTS[i]) };
}

function allPairs() {
  return SOURCES.map((_, i) => pair(i));
}

function withSource(i, fn) {
  const p = pair(i);
  fn(p.source);
  return p;
}

function withInput(i, fn) {
  const p = pair(i);
  fn(p.extracted.content[0].input);
  return p;
}

function codesOf(record) {
  return JSON.parse(record.errors_json).map((e) => e.code);
}

function reconciles(batch) {
  assert.equal(batch.records_processed, batch.records_accepted + batch.records_rejected);
  assert.equal(batch.records_accepted, batch.accepted_records.length);
  assert.equal(batch.records_rejected, batch.rejected_records.length);
}

/** A record with the two fields validateBatch adds taken off again. */
function withoutBatchFields(record) {
  const copy = Object.assign({}, record);
  delete copy.run_id;
  delete copy.publication_date;
  return copy;
}

// ---------------------------------------------------------------------------
// The result contract
// ---------------------------------------------------------------------------

test('the result has exactly the documented keys', () => {
  const batch = validateBatch(allPairs(), INGESTED_AT, RUN_A);
  assert.deepEqual(Object.keys(batch).sort(), [
    'accepted_records',
    'extraction_version',
    'ingested_at',
    'records_accepted',
    'records_processed',
    'records_rejected',
    'rejected_records',
    'run_id',
    'warnings',
  ]);
});

test('run_id and ingested_at are echoed, and the version is the constant', () => {
  const batch = validateBatch(allPairs(), INGESTED_AT, RUN_A);
  assert.equal(batch.run_id, RUN_A);
  assert.equal(batch.ingested_at, INGESTED_AT);
  assert.equal(batch.extraction_version, EXTRACTION_VERSION);
});

test('the five real pairs: four accepted, one rejected', () => {
  const batch = validateBatch(allPairs(), INGESTED_AT, RUN_A);
  assert.equal(batch.records_processed, 5);
  assert.equal(batch.records_accepted, 4);
  assert.equal(batch.records_rejected, 1);
  reconciles(batch);
  assert.deepEqual(codesOf(batch.rejected_records[0]), ['SALARY_SPAN_IMPLAUSIBLE']);
  assert.deepEqual(batch.warnings, []);
});

test('a batch of only valid records', () => {
  const batch = validateBatch([pair(1), pair(2), pair(3), pair(4)], INGESTED_AT, RUN_A);
  assert.equal(batch.records_processed, 4);
  assert.equal(batch.records_accepted, 4);
  assert.equal(batch.records_rejected, 0);
  assert.deepEqual(batch.rejected_records, []);
  reconciles(batch);
});

test('accepted records keep their input order', () => {
  const batch = validateBatch([pair(3), pair(1), pair(4)], INGESTED_AT, RUN_A);
  assert.deepEqual(
    batch.accepted_records.map((r) => r.source_job_id),
    [SOURCES[3], SOURCES[1], SOURCES[4]].map((s) => String(s.source_job_id))
  );
});

test('zero input records is a valid, empty batch', () => {
  const batch = validateBatch([], INGESTED_AT, RUN_A);
  assert.deepEqual(batch, {
    run_id: RUN_A,
    ingested_at: INGESTED_AT,
    extraction_version: EXTRACTION_VERSION,
    records_processed: 0,
    records_accepted: 0,
    records_rejected: 0,
    accepted_records: [],
    rejected_records: [],
    warnings: [],
  });
});

test('every record rejected', () => {
  const pairs = [1, 2, 3, 4].map((i) =>
    withInput(i, (input) => {
      input.seniority = 'principal';
    })
  );
  const batch = validateBatch(pairs, INGESTED_AT, RUN_A);
  assert.equal(batch.records_processed, 4);
  assert.equal(batch.records_accepted, 0);
  assert.equal(batch.records_rejected, 4);
  assert.deepEqual(batch.accepted_records, []);
  reconciles(batch);
  for (const record of batch.rejected_records) {
    assert.deepEqual(codesOf(record), ['ENUM']);
  }
});

// ---------------------------------------------------------------------------
// Count reconciliation
// ---------------------------------------------------------------------------

test('processed = accepted + rejected for every mix of good and bad pairs', () => {
  const good = (i) => pair(i);
  const bad = (i) =>
    withInput(i, (input) => {
      input.salary_min = -1;
    });

  // Every subset of the four passing fixtures, each member either left good
  // or made bad: 3^4 = 81 batches.
  let checked = 0;
  for (let mask = 0; mask < 81; mask++) {
    const pairs = [];
    let expectAccepted = 0;
    let expectRejected = 0;
    let m = mask;
    for (let i = 1; i <= 4; i++) {
      const choice = m % 3;
      m = Math.floor(m / 3);
      if (choice === 1) {
        pairs.push(good(i));
        expectAccepted++;
      } else if (choice === 2) {
        pairs.push(bad(i));
        expectRejected++;
      }
    }
    const batch = validateBatch(pairs, INGESTED_AT, RUN_A);
    reconciles(batch);
    assert.equal(batch.records_processed, pairs.length);
    assert.equal(batch.records_accepted, expectAccepted);
    assert.equal(batch.records_rejected, expectRejected);
    checked++;
  }
  assert.equal(checked, 81);
});

test('the counts say nothing about fetched or inserted rows', () => {
  // Fetched is known only to the HTTP node, inserted and skipped only to
  // Postgres. A validator that reported them would be guessing.
  const batch = validateBatch(allPairs(), INGESTED_AT, RUN_A);
  for (const key of Object.keys(batch)) {
    assert.doesNotMatch(key, /fetch|insert|skip|written|committed/);
  }
});

test('a posting listed twice is counted twice', () => {
  // The validator classifies what it is given. Collapsing duplicates is the
  // unique index's job, and the difference shows up as accepted minus inserted.
  const batch = validateBatch([pair(1), pair(1)], INGESTED_AT, RUN_A);
  assert.equal(batch.records_accepted, 2);
  reconciles(batch);
});

// ---------------------------------------------------------------------------
// Run identity and time
// ---------------------------------------------------------------------------

test('every record in a batch carries the run id', () => {
  const batch = validateBatch(allPairs(), INGESTED_AT, RUN_A);
  for (const record of batch.accepted_records.concat(batch.rejected_records)) {
    assert.equal(record.run_id, RUN_A);
  }
});

test('every record in a batch carries the same ingested_at', () => {
  const batch = validateBatch(allPairs(), INGESTED_AT, RUN_A);
  const stamps = batch.accepted_records.concat(batch.rejected_records).map((r) => r.ingested_at);
  assert.equal(stamps.length, 5);
  assert.deepEqual(new Set(stamps), new Set([INGESTED_AT]));
});

test('two executions differ in run_id and in nothing else', () => {
  const first = validateBatch(allPairs(), INGESTED_AT, RUN_A);
  const second = validateBatch(allPairs(), INGESTED_AT, RUN_B);

  assert.notEqual(first.run_id, second.run_id);
  assert.deepEqual(
    first.accepted_records.map((r) => Object.assign({}, r, { run_id: null })),
    second.accepted_records.map((r) => Object.assign({}, r, { run_id: null }))
  );
  for (const record of second.accepted_records.concat(second.rejected_records)) {
    assert.equal(record.run_id, RUN_B);
  }
});

test('the system clock is never read', () => {
  const RealDate = Date;
  class NoClock extends RealDate {
    constructor(...args) {
      if (args.length === 0) throw new Error('new Date() read the clock');
      super(...args);
    }
    static now() {
      throw new Error('Date.now() read the clock');
    }
  }
  global.Date = NoClock;
  try {
    const batch = validateBatch(allPairs(), INGESTED_AT, RUN_A);
    assert.equal(batch.records_processed, 5);
  } finally {
    global.Date = RealDate;
  }
});

test('the same inputs give the same result twice', () => {
  const pairs = allPairs();
  const first = validateBatch(pairs, INGESTED_AT, RUN_A);
  const second = validateBatch(pairs, INGESTED_AT, RUN_A);
  assert.deepEqual(first, second);
});

test('the inputs are not modified', () => {
  const pairs = allPairs();
  const before = clone(pairs);
  validateBatch(pairs, INGESTED_AT, RUN_A);
  assert.deepEqual(pairs, before);
});

// ---------------------------------------------------------------------------
// Invalid arguments: wiring faults throw
// ---------------------------------------------------------------------------

for (const [label, runId] of [
  ['undefined', undefined],
  ['null', null],
  ['an empty string', ''],
  ['a word', 'not-a-uuid'],
  ['a number', 12345],
  ['a UUID in braces', `{${RUN_A}}`],
  ['a UUID missing a character', RUN_A.slice(1)],
  ['a UUID with no dashes', RUN_A.replace(/-/g, '')],
  ['an n8n execution id', '231'],
  ['an unresolved expression', '={{ $json.run_id }}'],
]) {
  test(`run id ${label} throws and returns nothing`, () => {
    assert.throws(() => validateBatch(allPairs(), INGESTED_AT, runId), {
      name: 'TypeError',
      message: /runId must be a UUID/,
    });
  });
}

test('an uppercase UUID is accepted', () => {
  const batch = validateBatch([pair(1)], INGESTED_AT, RUN_A.toUpperCase());
  assert.equal(batch.run_id, RUN_A.toUpperCase());
});

for (const [label, ingestedAt] of [
  ['undefined', undefined],
  ['a Date object', new Date(0)],
  ['a number', 1755950400000],
  ['a local time with no zone', '2026-08-23T12:00:00.000'],
  ['a time with an offset', '2026-08-23T17:30:00.000+05:30'],
  ['a space-separated time', '2026-08-23 12:00:00Z'],
  ['a word', 'now'],
  ['an impossible date', '2026-13-45T00:00:00Z'],
]) {
  test(`ingestedAt ${label} throws`, () => {
    assert.throws(() => validateBatch(allPairs(), ingestedAt, RUN_A), {
      name: 'TypeError',
      message: /ingestedAt must be a UTC ISO-8601 string/,
    });
  });
}

test('toISOString output is accepted as ingestedAt', () => {
  const stamp = new Date(Date.UTC(2026, 10, 11, 9, 30, 0, 123)).toISOString();
  assert.equal(validateBatch([], stamp, RUN_A).ingested_at, stamp);
});

for (const [label, pairs] of [
  ['undefined', undefined],
  ['null', null],
  ['an object', { 0: pair(1) }],
  ['a string', 'pairs'],
]) {
  test(`pairs ${label} throws`, () => {
    assert.throws(() => validateBatch(pairs, INGESTED_AT, RUN_A), {
      name: 'TypeError',
      message: /pairs must be an array/,
    });
  });
}

// ---------------------------------------------------------------------------
// Malformed pairs: data faults are classified, not thrown
// ---------------------------------------------------------------------------

test('every malformed pair becomes exactly one rejected record', () => {
  const malformed = [
    null,
    undefined,
    42,
    'a string',
    [],
    {},
    { source: clone(SOURCES[1]) },
    { extracted: clone(OUTPUTS[1]) },
    { source: null, extracted: null },
  ];
  const batch = validateBatch(malformed, INGESTED_AT, RUN_A);

  assert.equal(batch.records_processed, malformed.length);
  assert.equal(batch.records_accepted, 0);
  assert.equal(batch.records_rejected, malformed.length);
  reconciles(batch);
  for (const record of batch.rejected_records) {
    assert.equal(record.run_id, RUN_A);
    assert.equal(record.ingested_at, INGESTED_AT);
    assert.ok(codesOf(record).length > 0);
  }
});

test('a malformed pair does not disturb the good pairs beside it', () => {
  const batch = validateBatch([pair(1), null, pair(2)], INGESTED_AT, RUN_A);
  assert.equal(batch.records_accepted, 2);
  assert.equal(batch.records_rejected, 1);
  reconciles(batch);
});

test('a pair with no extraction is rejected for the missing tool_use block', () => {
  const batch = validateBatch([{ source: clone(SOURCES[1]) }], INGESTED_AT, RUN_A);
  assert.deepEqual(codesOf(batch.rejected_records[0]), ['TOOL_USE_MISSING']);
  // Its provenance survives, so the dead-letter row can still be traced.
  assert.equal(batch.rejected_records[0].source_job_id, String(SOURCES[1].source_job_id));
});

// ---------------------------------------------------------------------------
// Unexpected exceptions inside validate()
// ---------------------------------------------------------------------------

/** An id that explodes when coerced. JSON cannot produce this; it stands in for
 *  a bug in validate() that nobody has found yet. */
function explosivePair(i) {
  const p = pair(i);
  p.source.source_job_id = {
    toString() {
      throw new Error('boom');
    },
  };
  return p;
}

test('a pair that makes validate() throw is dead-lettered, not fatal', () => {
  assert.throws(() => validate(explosivePair(1), INGESTED_AT), /boom/);

  const batch = validateBatch([pair(1), explosivePair(2), pair(3)], INGESTED_AT, RUN_A);

  assert.equal(batch.records_accepted, 2);
  assert.equal(batch.records_rejected, 1);
  reconciles(batch);

  const record = batch.rejected_records[0];
  assert.deepEqual(record.error_codes, ['VALIDATOR_EXCEPTION']);
  const errors = JSON.parse(record.errors_json);
  assert.equal(errors[0].field, 'validator');
  assert.match(errors[0].message, /validate\(\) threw: boom/);
  assert.equal(record.run_id, RUN_A);
  assert.equal(record.ingested_at, INGESTED_AT);
  assert.equal(record.extraction_version, EXTRACTION_VERSION);
});

test('an exception record has every dead-letter key', () => {
  const normal = validateBatch([pair(0)], INGESTED_AT, RUN_A).rejected_records[0];
  const exceptional = validateBatch([explosivePair(1)], INGESTED_AT, RUN_A).rejected_records[0];
  assert.deepEqual(Object.keys(exceptional).sort(), Object.keys(normal).sort());
});

// ---------------------------------------------------------------------------
// publication_date
// ---------------------------------------------------------------------------

test('a publication date is passed through exactly as Remotive sent it', () => {
  const batch = validateBatch(allPairs(), INGESTED_AT, RUN_A);
  const bySource = {};
  for (const s of SOURCES) bySource[String(s.source_job_id)] = s.publication_date;

  for (const record of batch.accepted_records.concat(batch.rejected_records)) {
    assert.equal(record.publication_date, bySource[record.source_job_id]);
    assert.match(record.publication_date, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/);
  }
});

test('a rejected record carries its publication date too', () => {
  const batch = validateBatch([pair(0)], INGESTED_AT, RUN_A);
  assert.equal(batch.rejected_records[0].publication_date, SOURCES[0].publication_date);
});

test('fractional seconds are accepted and kept', () => {
  const p = withSource(1, (s) => {
    s.publication_date = '2026-08-21T05:54:39.123456';
  });
  const batch = validateBatch([p], INGESTED_AT, RUN_A);
  assert.equal(batch.accepted_records[0].publication_date, '2026-08-21T05:54:39.123456');
  assert.deepEqual(batch.warnings, []);
});

for (const [label, mutate] of [
  ['absent', (s) => delete s.publication_date],
  ['null', (s) => (s.publication_date = null)],
  ['an empty string', (s) => (s.publication_date = '')],
]) {
  test(`publication date ${label}: stored as null, row still accepted, warned`, () => {
    const batch = validateBatch([withSource(1, mutate)], INGESTED_AT, RUN_A);

    assert.equal(batch.records_accepted, 1);
    assert.equal(batch.accepted_records[0].publication_date, null);
    assert.ok('publication_date' in batch.accepted_records[0]);
    assert.deepEqual(batch.warnings, [
      {
        index: 0,
        source_job_id: String(SOURCES[1].source_job_id),
        field: 'publication_date',
        code: 'PUBLICATION_DATE_MISSING',
        message: 'publication_date is absent; stored as null',
      },
    ]);
  });
}

for (const [label, value] of [
  ['a word', 'yesterday'],
  ['a date with no time', '2026-08-21'],
  ['a unix timestamp', 1755755679],
  ['a UTC-marked time', '2026-08-21T05:54:39Z'],
  ['a time with an offset', '2026-08-21T05:54:39+05:30'],
  ['a space-separated time', '2026-08-21 05:54:39'],
  ['February 30th', '2026-02-30T00:00:00'],
  ['hour 25', '2026-08-21T25:00:00'],
  ['month 13', '2026-13-01T00:00:00'],
  ['an object', { year: 2026 }],
  ['a boolean', true],
]) {
  test(`malformed publication date (${label}): null, warned, verdict unchanged`, () => {
    const batch = validateBatch(
      [withSource(1, (s) => (s.publication_date = value))],
      INGESTED_AT,
      RUN_A
    );

    assert.equal(batch.records_accepted, 1, 'a bad date must not reject a good extraction');
    assert.equal(batch.accepted_records[0].publication_date, null);
    assert.equal(batch.warnings.length, 1);
    assert.equal(batch.warnings[0].code, 'PUBLICATION_DATE_MALFORMED');
    assert.equal(batch.warnings[0].index, 0);
  });
}

test('a leap day is a real date', () => {
  const p = withSource(1, (s) => (s.publication_date = '2028-02-29T12:00:00'));
  const batch = validateBatch([p], INGESTED_AT, RUN_A);
  assert.equal(batch.accepted_records[0].publication_date, '2028-02-29T12:00:00');
});

test('a missing date is never replaced with ingested_at or any other value', () => {
  const p = withSource(1, (s) => delete s.publication_date);
  const record = validateBatch([p], INGESTED_AT, RUN_A).accepted_records[0];
  assert.equal(record.publication_date, null);
  assert.equal(record.ingested_at, INGESTED_AT);
});

test('a bad date does not rescue or add to a rejection', () => {
  const p = pair(0);
  p.source.publication_date = 'yesterday';
  const batch = validateBatch([p], INGESTED_AT, RUN_A);

  assert.equal(batch.records_rejected, 1);
  // The date problem is a warning. It is not written into the row's errors,
  // so it cannot be counted as a model failure.
  assert.deepEqual(codesOf(batch.rejected_records[0]), ['SALARY_SPAN_IMPLAUSIBLE']);
  assert.deepEqual(batch.rejected_records[0].error_codes, ['SALARY_SPAN_IMPLAUSIBLE']);
  assert.equal(batch.warnings[0].code, 'PUBLICATION_DATE_MALFORMED');
});

test('warnings point at the right pair by index', () => {
  const pairs = [pair(1), withSource(2, (s) => delete s.publication_date), pair(3)];
  const batch = validateBatch(pairs, INGESTED_AT, RUN_A);
  assert.deepEqual(
    batch.warnings.map((w) => [w.index, w.source_job_id, w.code]),
    [[1, String(SOURCES[2].source_job_id), 'PUBLICATION_DATE_MISSING']]
  );
});

// ---------------------------------------------------------------------------
// validate() is reused, not reimplemented
// ---------------------------------------------------------------------------

test('each batch record is the validate() record plus two fields', () => {
  const batch = validateBatch(allPairs(), INGESTED_AT, RUN_A);
  const fromBatch = {};
  for (const record of batch.accepted_records.concat(batch.rejected_records)) {
    fromBatch[record.source_job_id] = record;
  }

  for (let i = 0; i < SOURCES.length; i++) {
    const single = validate(pair(i), INGESTED_AT);
    const record = fromBatch[String(SOURCES[i].source_job_id)];

    assert.deepEqual(withoutBatchFields(record), single.record);
    assert.deepEqual(
      Object.keys(record).sort(),
      Object.keys(single.record).concat(['publication_date', 'run_id']).sort()
    );
  }
});

test('the batch reaches the same verdict as validate() on every kind of failure', () => {
  const cases = [
    withInput(1, (x) => (x.seniority = 'principal')),
    withInput(1, (x) => (x.remote_type = 'office')),
    withInput(1, (x) => delete x.normalized_title),
    withInput(1, (x) => (x.company = '   ')),
    withInput(1, (x) => (x.tech_stack = 'Python')),
    withInput(1, (x) => (x.tech_stack = new Array(31).fill('x'))),
    withInput(1, (x) => (x.mentions_n8n = 'no')),
    withInput(1, (x) => (x.mentions_n8n = true)),
    withInput(1, (x) => ((x.salary_min = 50000), (x.salary_max = null))),
    withInput(1, (x) => ((x.salary_min = 90000), (x.salary_max = 50000), (x.salary_period = 'yearly'))),
    withInput(1, (x) => ((x.salary_min = 1), (x.salary_max = 100), (x.salary_period = 'hourly'))),
    withInput(1, (x) => ((x.salary_min = 50000), (x.salary_max = 60000), (x.salary_period = 'unspecified'))),
    withInput(1, (x) => (x.invented_field = 1)),
    withSource(1, (s) => delete s.source_job_id),
    withSource(1, (s) => (s.source_url = '')),
    withSource(1, (s) => delete s.description_clean),
    { source: clone(SOURCES[1]), extracted: { model: 'm', stop_reason: 'max_tokens', content: [] } },
  ];

  for (const p of cases) {
    const single = validate(clone(p), INGESTED_AT);
    const batch = validateBatch([clone(p)], INGESTED_AT, RUN_A);

    assert.equal(single.valid, false);
    assert.equal(batch.records_rejected, 1);
    const record = batch.rejected_records[0];
    assert.deepEqual(JSON.parse(record.errors_json), single.errors);
    assert.deepEqual(record.error_codes, single.record.error_codes);
  }
});

test('an accepted record has no description, a rejected one does', () => {
  const batch = validateBatch(allPairs(), INGESTED_AT, RUN_A);
  for (const record of batch.accepted_records) {
    assert.equal('description_clean' in record, false);
  }
  assert.equal(batch.rejected_records[0].description_clean, SOURCES[0].description_clean);
});

test('errors_json is encoded once', () => {
  // A string that parses straight to the array. The second encoding that put
  // a JSON string inside the jsonb column happened in the canvas, not here.
  const record = validateBatch([pair(0)], INGESTED_AT, RUN_A).rejected_records[0];
  assert.equal(typeof record.errors_json, 'string');
  assert.ok(Array.isArray(JSON.parse(record.errors_json)));
});

// ---------------------------------------------------------------------------
// validate() itself is unchanged
// ---------------------------------------------------------------------------

test('validate() still returns exactly valid, errors and record', () => {
  const result = validate(pair(1), INGESTED_AT);
  assert.deepEqual(Object.keys(result).sort(), ['errors', 'record', 'valid']);
});

test('validate() records gained no fields', () => {
  const accepted = validate(pair(1), INGESTED_AT).record;
  const rejected = validate(pair(0), INGESTED_AT).record;
  for (const record of [accepted, rejected]) {
    assert.equal('run_id' in record, false);
    assert.equal('publication_date' in record, false);
  }
  assert.equal(Object.keys(accepted).length, 18);
  assert.equal(Object.keys(rejected).length, 11);
});

test('validate() still takes two arguments and ignores a third', () => {
  assert.equal(validate.length, 2);
  assert.deepEqual(validate(pair(1), INGESTED_AT, RUN_A), validate(pair(1), INGESTED_AT));
});

// ---------------------------------------------------------------------------
// The file still pastes into the n8n Code node
// ---------------------------------------------------------------------------

test('the whole file runs where module does not exist', () => {
  // The Code node has no `module`. Evaluate the source the same way, then call
  // validateBatch through the kind of wrapper the node uses.
  const source = fs.readFileSync(path.join(__dirname, 'validate.js'), 'utf8');
  const run = new Function(
    'pairs',
    'ingestedAt',
    'runId',
    `${source}\nreturn validateBatch(pairs, ingestedAt, runId);`
  );
  const batch = run(allPairs(), INGESTED_AT, RUN_A);
  assert.equal(batch.records_processed, 5);
  assert.equal(batch.records_accepted, 4);
});

test('the file has no require, no import and no clock read', () => {
  const source = fs.readFileSync(path.join(__dirname, 'validate.js'), 'utf8');
  const code = source
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/^\s*\/\/.*$/gm, '');
  assert.doesNotMatch(code, /\brequire\s*\(/);
  assert.doesNotMatch(code, /^\s*import\s/m);
  assert.doesNotMatch(code, /Date\.now\s*\(/);
  assert.doesNotMatch(code, /new Date\s*\(\s*\)/);
});
