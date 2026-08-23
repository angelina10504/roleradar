'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const { validate, EXTRACTION_VERSION, MAX_SALARY_SPAN_RATIO } = require('./validate.js');

// Real captured output from five live pipeline runs, and the Remotive records
// they came from. Nothing here is invented. Failure cases are mutations of
// these, and every mutation is a single visible line.
const SOURCES = require('./fixtures/source-records.json');
const OUTPUTS = require('./fixtures/extraction-output.json');

const INGESTED_AT = '2026-08-23T12:00:00.000Z';

// The two files pair positionally: extraction-output.json carries no
// source_job_id, so index is the only join key there is. Assert the lengths
// match so a reorder or a partial re-capture fails loudly here rather than
// silently validating job A's extraction against job B's description.
test('fixture arrays are the same length', () => {
  assert.equal(
    SOURCES.length,
    OUTPUTS.length,
    'source-records.json and extraction-output.json must stay index-aligned'
  );
  assert.ok(SOURCES.length > 0, 'fixtures must not be empty');
});

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

/** A real fixture pair, deep-copied so mutations cannot leak between tests. */
function pair(i) {
  return { source: clone(SOURCES[i]), extracted: clone(OUTPUTS[i]) };
}

/** A real fixture pair with one mutation applied to the tool input. */
function mutateInput(i, fn) {
  const p = pair(i);
  fn(p.extracted.content[0].input);
  return p;
}

/** A real fixture pair with one mutation applied to the source record. */
function mutateSource(i, fn) {
  const p = pair(i);
  fn(p.source);
  return p;
}

function codes(result) {
  return result.errors.map((e) => e.code);
}

// ---------------------------------------------------------------------------
// The five real runs
// ---------------------------------------------------------------------------

// Fixture 0 is the TELUS Digital posting. The description says "$0.2333 per
// completed task" and "$14 per hour". The model put both in one hourly range,
// spanning 60x. Every other check passes, which is why the span rule exists.
test('real run 0 fails only on the implausible salary span', () => {
  const result = validate(pair(0), INGESTED_AT);
  assert.equal(result.valid, false);
  assert.deepEqual(codes(result), ['SALARY_SPAN_IMPLAUSIBLE']);
  assert.equal(result.errors[0].field, 'salary_max');
});

for (const i of [1, 2, 3, 4]) {
  test(`real run ${i} validates clean`, () => {
    const result = validate(pair(i), INGESTED_AT);
    assert.deepEqual(result.errors, []);
    assert.equal(result.valid, true);
  });
}

test('exactly one of the five real runs dead-letters', () => {
  const failed = SOURCES.map((_, i) => validate(pair(i), INGESTED_AT)).filter((r) => !r.valid);
  assert.equal(failed.length, 1);
});

// ---------------------------------------------------------------------------
// Record shape
// ---------------------------------------------------------------------------

test('valid record carries provenance and both n8n booleans', () => {
  // Fixture 4 is A.Team: "$120-$170/hr", a 1.4x span with an explicit period.
  const result = validate(pair(4), INGESTED_AT);
  assert.equal(result.valid, true);

  assert.deepEqual(result.record, {
    source_job_id: '1919266',
    source_url: SOURCES[4].source_url,
    raw_title: 'Senior Independent AI Engineer / Architect',
    raw_company: 'A.Team',
    ingested_at: INGESTED_AT,
    model: 'claude-haiku-4-5-20251001',
    extraction_version: EXTRACTION_VERSION,

    normalized_title: 'Senior AI Engineer / Architect',
    company: 'A.Team',
    seniority: 'senior',
    tech_stack: ['AI', 'Machine Learning', 'LLM', 'Agents', 'Retrieval systems', 'ML platforms'],
    remote_type: 'remote',
    salary_min: 120,
    salary_max: 170,
    salary_period: 'hourly',
    mentions_n8n_model: false,
    mentions_n8n_actual: false,
    ai_ml_role: true,
  });
});

test('source_job_id is coerced to a string', () => {
  // Remotive emits it as a number. The jobs_latest view partitions on this
  // column, so the type must not depend on what the API felt like sending.
  assert.equal(typeof SOURCES[4].source_job_id, 'number');
  assert.equal(validate(pair(4), INGESTED_AT).record.source_job_id, '1919266');
});

test('the description is not written to the valid record', () => {
  const record = validate(pair(4), INGESTED_AT).record;
  assert.equal('description_clean' in record, false);
});

test('dead letter record carries the description, the raw output and the codes', () => {
  const result = validate(pair(0), INGESTED_AT);
  const record = result.record;

  assert.equal(record.source_job_id, '2091105');
  assert.equal(record.model, 'claude-haiku-4-5-20251001');
  assert.equal(record.extraction_version, EXTRACTION_VERSION);
  assert.equal(record.ingested_at, INGESTED_AT);
  assert.equal(record.description_clean, SOURCES[0].description_clean);
  assert.deepEqual(record.error_codes, ['SALARY_SPAN_IMPLAUSIBLE']);

  // raw_output is a JSON string, per CLAUDE.md, and round-trips to the message
  // that came back from the API.
  assert.equal(typeof record.raw_output, 'string');
  assert.deepEqual(JSON.parse(record.raw_output), OUTPUTS[0]);

  assert.deepEqual(JSON.parse(record.errors_json), result.errors);
});

test('dead letter error_codes are distinct', () => {
  const item = mutateInput(1, (input) => {
    input.tech_stack = [42, 43];
  });
  const result = validate(item, INGESTED_AT);
  assert.equal(result.errors.length, 2);
  assert.deepEqual(result.record.error_codes, ['ARRAY_ITEM']);
});

test('every error has a field, a code and a message', () => {
  const item = mutateInput(0, (input) => {
    delete input.normalized_title;
    input.seniority = 'principal';
  });
  const result = validate(item, INGESTED_AT);
  assert.ok(result.errors.length >= 3);
  for (const e of result.errors) {
    assert.equal(typeof e.field, 'string');
    assert.equal(typeof e.code, 'string');
    assert.equal(typeof e.message, 'string');
    assert.ok(e.message.length > 0);
  }
});

// ---------------------------------------------------------------------------
// Purity
// ---------------------------------------------------------------------------

test('validate is pure', () => {
  const item = pair(0);
  const before = JSON.stringify(item);

  const first = validate(item, INGESTED_AT);
  const second = validate(item, INGESTED_AT);

  assert.deepEqual(first, second);
  assert.equal(JSON.stringify(item), before, 'validate must not mutate its input');
});

test('ingested_at is echoed, never generated', () => {
  const other = '1999-12-31T23:59:59.000Z';
  assert.equal(validate(pair(4), other).record.ingested_at, other);
  assert.equal(validate(pair(0), other).record.ingested_at, other);
});

// ---------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------

test('a truncated response with no tool_use block dead-letters', () => {
  const p = pair(1);
  p.extracted.content = [{ type: 'text', text: 'Here is the extract' }];
  p.extracted.stop_reason = 'max_tokens';

  const result = validate(p, INGESTED_AT);
  assert.deepEqual(codes(result), ['TOOL_USE_MISSING']);
  assert.match(result.errors[0].message, /max_tokens/);
  // Still traceable: the dead letter row knows which posting it came from.
  assert.equal(result.record.source_job_id, '2091098');
  assert.equal(result.record.model, 'claude-haiku-4-5-20251001');
});

test('a tool_use block for a different tool does not count', () => {
  const p = pair(1);
  p.extracted.content[0].name = 'some_other_tool';
  assert.deepEqual(codes(validate(p, INGESTED_AT)), ['TOOL_USE_MISSING']);
});

test('schema checks do not run once the envelope has failed', () => {
  const p = pair(1);
  p.extracted.content = [];
  // One error, not eleven complaints about fields that were never sent.
  assert.equal(validate(p, INGESTED_AT).errors.length, 1);
});

// ---------------------------------------------------------------------------
// Source provenance
// ---------------------------------------------------------------------------

test('a missing source_job_id dead-letters', () => {
  const item = mutateSource(1, (s) => {
    delete s.source_job_id;
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['MISSING']);
  assert.equal(result.errors[0].field, 'source_job_id');
  assert.equal(result.record.source_job_id, null);
});

test('a numeric source_job_id is accepted, an empty one is not', () => {
  assert.equal(validate(pair(1), INGESTED_AT).valid, true);

  const item = mutateSource(1, (s) => {
    s.source_job_id = '   ';
  });
  assert.deepEqual(codes(validate(item, INGESTED_AT)), ['MISSING']);
});

for (const field of ['source_url', 'raw_title', 'raw_company', 'description_clean']) {
  test(`a missing ${field} dead-letters`, () => {
    const item = mutateSource(1, (s) => {
      delete s[field];
    });
    const result = validate(item, INGESTED_AT);
    assert.ok(result.errors.some((e) => e.field === field && e.code === 'MISSING'));
  });

  test(`an empty ${field} dead-letters`, () => {
    const item = mutateSource(1, (s) => {
      s[field] = '  ';
    });
    const result = validate(item, INGESTED_AT);
    assert.ok(result.errors.some((e) => e.field === field && e.code === 'EMPTY'));
  });
}

test('a non-string raw_title dead-letters', () => {
  const item = mutateSource(1, (s) => {
    s.raw_title = 12345;
  });
  const result = validate(item, INGESTED_AT);
  assert.ok(result.errors.some((e) => e.field === 'raw_title' && e.code === 'TYPE'));
});

// ---------------------------------------------------------------------------
// Schema, per field
// ---------------------------------------------------------------------------

test('a missing required string dead-letters', () => {
  const item = mutateInput(1, (input) => {
    delete input.normalized_title;
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['MISSING']);
  assert.equal(result.errors[0].field, 'normalized_title');
});

test('a non-string company dead-letters', () => {
  const item = mutateInput(1, (input) => {
    input.company = 42;
  });
  assert.deepEqual(codes(validate(item, INGESTED_AT)), ['TYPE']);
});

test('a whitespace-only company dead-letters', () => {
  const item = mutateInput(1, (input) => {
    input.company = '   ';
  });
  assert.deepEqual(codes(validate(item, INGESTED_AT)), ['EMPTY']);
});

test('a seniority outside the enum dead-letters', () => {
  const item = mutateInput(1, (input) => {
    input.seniority = 'principal';
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['ENUM']);
  assert.match(result.errors[0].message, /junior, mid, senior, lead, unspecified/);
});

test('a remote_type outside the enum dead-letters', () => {
  const item = mutateInput(1, (input) => {
    input.remote_type = 'wfh';
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['ENUM']);
  assert.equal(result.errors[0].field, 'remote_type');
});

test('a salary_period outside the enum dead-letters', () => {
  const item = mutateInput(1, (input) => {
    input.salary_period = 'weekly';
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['ENUM']);
  assert.equal(result.errors[0].field, 'salary_period');
});

test('all four salary periods are accepted', () => {
  for (const period of ['hourly', 'monthly', 'yearly']) {
    const item = mutateInput(4, (input) => {
      input.salary_period = period;
    });
    assert.equal(validate(item, INGESTED_AT).valid, true, period);
  }
  // 'unspecified' is only valid with no bounds, which is fixture 1.
  assert.equal(validate(pair(1), INGESTED_AT).record.salary_period, 'unspecified');
});

test('a missing salary_period dead-letters', () => {
  const item = mutateInput(1, (input) => {
    delete input.salary_period;
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['MISSING']);
  assert.equal(result.errors[0].field, 'salary_period');
});

test('a non-array tech_stack dead-letters', () => {
  const item = mutateInput(1, (input) => {
    input.tech_stack = 'Golang, Kubernetes';
  });
  assert.deepEqual(codes(validate(item, INGESTED_AT)), ['TYPE']);
});

test('an empty tech_stack is allowed', () => {
  const item = mutateInput(1, (input) => {
    input.tech_stack = [];
  });
  assert.equal(validate(item, INGESTED_AT).valid, true);
});

test('a tech_stack over 30 items dead-letters', () => {
  const item = mutateInput(3, (input) => {
    // Fixture 3 has 11 real entries; repeat them to cross the limit.
    const seed = OUTPUTS[3].content[0].input.tech_stack;
    while (input.tech_stack.length < 31) input.tech_stack = input.tech_stack.concat(seed);
    input.tech_stack = input.tech_stack.slice(0, 31);
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['ARRAY_LEN']);
  assert.match(result.errors[0].message, /got 31/);
});

test('exactly 30 tech_stack items is allowed', () => {
  const item = mutateInput(3, (input) => {
    const seed = OUTPUTS[3].content[0].input.tech_stack;
    while (input.tech_stack.length < 30) input.tech_stack = input.tech_stack.concat(seed);
    input.tech_stack = input.tech_stack.slice(0, 30);
  });
  assert.equal(validate(item, INGESTED_AT).valid, true);
});

test('a non-string tech_stack entry dead-letters with its index', () => {
  const item = mutateInput(1, (input) => {
    input.tech_stack[1] = 5;
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['ARRAY_ITEM']);
  assert.match(result.errors[0].message, /tech_stack\[1\]/);
});

test('an empty tech_stack entry dead-letters', () => {
  const item = mutateInput(1, (input) => {
    input.tech_stack[0] = '';
  });
  assert.deepEqual(codes(validate(item, INGESTED_AT)), ['ARRAY_ITEM']);
});

for (const field of ['mentions_n8n', 'ai_ml_role']) {
  test(`a missing ${field} dead-letters`, () => {
    const item = mutateInput(1, (input) => {
      delete input[field];
    });
    const result = validate(item, INGESTED_AT);
    assert.ok(result.errors.some((e) => e.field === field && e.code === 'MISSING'));
  });

  test(`a non-boolean ${field} dead-letters`, () => {
    const item = mutateInput(1, (input) => {
      input[field] = 'false';
    });
    const result = validate(item, INGESTED_AT);
    assert.ok(result.errors.some((e) => e.field === field && e.code === 'TYPE'));
  });
}

// ---------------------------------------------------------------------------
// Salary bounds
// ---------------------------------------------------------------------------

test('a string salary bound dead-letters', () => {
  const item = mutateInput(4, (input) => {
    input.salary_min = '120';
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['TYPE']);
  assert.equal(result.errors[0].field, 'salary_min');
});

test('an absent salary bound dead-letters as a type error', () => {
  const item = mutateInput(4, (input) => {
    delete input.salary_max;
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['TYPE']);
  assert.match(result.errors[0].message, /undefined/);
});

test('a zero salary bound dead-letters', () => {
  // Zero is not a lower bound, and rejecting it keeps the span ratio from
  // dividing by zero.
  const item = mutateInput(4, (input) => {
    input.salary_min = 0;
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['RANGE']);
  assert.equal(result.errors[0].field, 'salary_min');
});

test('a negative salary bound dead-letters', () => {
  const item = mutateInput(4, (input) => {
    input.salary_max = -170;
  });
  assert.deepEqual(codes(validate(item, INGESTED_AT)), ['RANGE']);
});

test('both bounds null is allowed', () => {
  // This is fixtures 1, 2 and 3 as captured.
  const result = validate(pair(2), INGESTED_AT);
  assert.equal(result.valid, true);
  assert.equal(result.record.salary_min, null);
  assert.equal(result.record.salary_max, null);
});

test('one bound null dead-letters as SALARY_PARTIAL', () => {
  const item = mutateInput(4, (input) => {
    input.salary_max = null;
  });
  assert.deepEqual(codes(validate(item, INGESTED_AT)), ['SALARY_PARTIAL']);
});

test('min above max dead-letters as SALARY_ORDER', () => {
  const item = mutateInput(4, (input) => {
    input.salary_min = 170;
    input.salary_max = 120;
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['SALARY_ORDER']);
});

test('SALARY_ORDER and SALARY_SPAN_IMPLAUSIBLE do not double-report', () => {
  const item = mutateInput(4, (input) => {
    input.salary_min = 5000;
    input.salary_max = 120;
  });
  assert.deepEqual(codes(validate(item, INGESTED_AT)), ['SALARY_ORDER']);
});

test('a bad bound suppresses the cross-field salary checks', () => {
  const item = mutateInput(4, (input) => {
    input.salary_min = 'a lot';
  });
  // One diagnosable error, not TYPE plus a nonsense comparison.
  assert.deepEqual(codes(validate(item, INGESTED_AT)), ['TYPE']);
});

// ---------------------------------------------------------------------------
// SALARY_SPAN_IMPLAUSIBLE
// ---------------------------------------------------------------------------

test('the real 60x range is caught and named', () => {
  const input = OUTPUTS[0].content[0].input;
  assert.equal(input.salary_min, 0.2333);
  assert.equal(input.salary_max, 14);
  assert.ok(input.salary_max / input.salary_min > MAX_SALARY_SPAN_RATIO);

  const result = validate(pair(0), INGESTED_AT);
  assert.deepEqual(codes(result), ['SALARY_SPAN_IMPLAUSIBLE']);
  assert.match(result.errors[0].message, /60\.0x/);
});

test('a span of exactly 20x is allowed', () => {
  const item = mutateInput(4, (input) => {
    input.salary_min = 120;
    input.salary_max = 2400;
  });
  assert.equal(validate(item, INGESTED_AT).valid, true);
});

test('a span just over 20x dead-letters', () => {
  const item = mutateInput(4, (input) => {
    input.salary_min = 120;
    input.salary_max = 2401;
  });
  assert.deepEqual(codes(validate(item, INGESTED_AT)), ['SALARY_SPAN_IMPLAUSIBLE']);
});

test('the span rule is countable on its own', () => {
  // The point of the separate code: hallucinated-bound rate is a filter on
  // error_codes, not a substring search through a message blob.
  const results = SOURCES.map((_, i) => validate(pair(i), INGESTED_AT));
  const spanFailures = results.filter(
    (r) => !r.valid && r.record.error_codes.includes('SALARY_SPAN_IMPLAUSIBLE')
  );
  assert.equal(spanFailures.length, 1);
});

// ---------------------------------------------------------------------------
// SALARY_PERIOD_MISMATCH
// ---------------------------------------------------------------------------

test('a bound with an unspecified period dead-letters', () => {
  const item = mutateInput(4, (input) => {
    input.salary_period = 'unspecified';
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['SALARY_PERIOD_MISMATCH']);
  assert.equal(result.errors[0].field, 'salary_period');
});

test('no bounds with an unspecified period is fine', () => {
  // Deliberately the half of the rule that was dropped: a period sitting on a
  // salary-less posting is harmless and must not discard a good extraction.
  const item = mutateInput(1, (input) => {
    input.salary_period = 'yearly';
  });
  assert.equal(validate(item, INGESTED_AT).valid, true);
});

// ---------------------------------------------------------------------------
// UNKNOWN_FIELD
// ---------------------------------------------------------------------------

test('a field outside the tool schema dead-letters', () => {
  const item = mutateInput(1, (input) => {
    input.salary_currency = 'USD';
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['UNKNOWN_FIELD']);
  assert.equal(result.errors[0].field, 'salary_currency');
});

test('mentions_n8n_actual is not a tool schema field', () => {
  // The validator computes it. If the model starts sending it, that is drift.
  const item = mutateInput(1, (input) => {
    input.mentions_n8n_actual = false;
  });
  assert.deepEqual(codes(validate(item, INGESTED_AT)), ['UNKNOWN_FIELD']);
});

test('the caller block outside input does not trip UNKNOWN_FIELD', () => {
  // Every captured fixture has content[0].caller. It sits beside input, not
  // inside it, and all five real runs validate on that basis.
  assert.ok('caller' in OUTPUTS[0].content[0]);
  assert.equal(validate(pair(1), INGESTED_AT).valid, true);
});

// ---------------------------------------------------------------------------
// N8N_MISMATCH
// ---------------------------------------------------------------------------

test('all five real runs agree on mentions_n8n', () => {
  // None of the captured descriptions contain the token, and the model said
  // false for all five. The fixtures do not exercise a disagreement, so the
  // tests below flip one value on a real record to force one.
  for (let i = 0; i < SOURCES.length; i++) {
    assert.equal(/\bn8n\b/i.test(SOURCES[i].description_clean), false, `source ${i}`);
    assert.equal(OUTPUTS[i].content[0].input.mentions_n8n, false, `output ${i}`);
  }
});

test('model says true, description has no n8n', () => {
  const item = mutateInput(1, (input) => {
    input.mentions_n8n = true;
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['N8N_MISMATCH']);
  assert.equal(result.errors[0].field, 'mentions_n8n');
  assert.match(result.errors[0].message, /does not contain/);
});

test('model says false, description has n8n', () => {
  const item = mutateSource(1, (s) => {
    s.description_clean += ' Familiarity with n8n is a plus.';
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['N8N_MISMATCH']);
  assert.match(result.errors[0].message, /model said false/);
});

test('agreement on true validates and stores both booleans', () => {
  const p = pair(1);
  p.source.description_clean += ' Familiarity with n8n is a plus.';
  p.extracted.content[0].input.mentions_n8n = true;

  const result = validate(p, INGESTED_AT);
  assert.equal(result.valid, true);
  assert.equal(result.record.mentions_n8n_model, true);
  assert.equal(result.record.mentions_n8n_actual, true);
});

test('the validator does not correct the model value', () => {
  const item = mutateInput(1, (input) => {
    input.mentions_n8n = true;
  });
  const result = validate(item, INGESTED_AT);
  // The row is rejected, not quietly fixed. The raw output still says true.
  assert.equal(JSON.parse(result.record.raw_output).content[0].input.mentions_n8n, true);
});

test('the token match is bounded, not a substring search', () => {
  const cases = [
    ['We use n8n daily', true],
    ['n8n-based automation', true],
    ['(n8n)', true],
    ['Experience with N8N', true],
    ['an8n', false],
    ['n8no', false],
    ['n8', false],
  ];
  for (const [text, expected] of cases) {
    const item = mutateSource(1, (s) => {
      s.description_clean = text;
    });
    const result = validate(item, INGESTED_AT);
    const mismatched = codes(result).includes('N8N_MISMATCH');
    // The model said false in this fixture, so a mismatch means a match.
    assert.equal(mismatched, expected, text);
  }
});

test('n8n inside a tag is not counted', () => {
  const item = mutateSource(1, (s) => {
    s.description_clean = 'Backend role <a href="https://n8n.io">apply</a> today.';
  });
  assert.equal(validate(item, INGESTED_AT).valid, true);
});

test('a mismatch is not reported when mentions_n8n failed its type check', () => {
  const item = mutateInput(1, (input) => {
    input.mentions_n8n = 'yes';
  });
  assert.deepEqual(codes(validate(item, INGESTED_AT)), ['TYPE']);
});

test('a mismatch is not reported when the description is missing', () => {
  const item = mutateSource(1, (s) => {
    delete s.description_clean;
  });
  const result = validate(item, INGESTED_AT);
  assert.deepEqual(codes(result), ['MISSING']);
});
