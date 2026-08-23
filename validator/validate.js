'use strict';

/**
 * RoleRadar extraction validator.
 *
 * Source of truth for the validation logic. A copy of this file runs inside the
 * n8n Code node, pasted by hand. There is no import path from the canvas into
 * this repo, so the two can drift. See CLAUDE.md, "Known duplication".
 *
 * The whole file is safe to paste into the Code node unchanged: the export at
 * the bottom is guarded, so it is a no-op where `module` does not exist.
 *
 *   validate({ source, extracted }, ingestedAt) -> { valid, errors, record }
 *
 * Pure. No clock, no I/O, no dependencies. `ingestedAt` is an argument so tests
 * are deterministic.
 *
 * `extracted` is the full Anthropic message, not the tool input. It is the only
 * place the `model` provenance value exists, and unwrapping it here keeps that
 * step inside the tested module rather than in the hand-pasted Code node.
 */

/** Bumped when the tool schema or the checks below change. Stored per row so
 *  rows stay comparable across versions. */
const EXTRACTION_VERSION = '1.0.0';

const TOOL_NAME = 'extract_job_fields';

const SENIORITY = ['junior', 'mid', 'senior', 'lead', 'unspecified'];
const REMOTE_TYPE = ['remote', 'hybrid', 'onsite', 'unspecified'];
const SALARY_PERIOD = ['hourly', 'monthly', 'yearly', 'unspecified'];

/** Keys the tool's input_schema may emit. Anything else is drift. */
const SCHEMA_FIELDS = [
  'normalized_title',
  'company',
  'seniority',
  'tech_stack',
  'remote_type',
  'salary_min',
  'salary_max',
  'salary_period',
  'mentions_n8n',
  'ai_ml_role',
];

/** Provenance fields the pipeline cannot trace a row without. */
const REQUIRED_SOURCE_STRINGS = [
  'source_url',
  'raw_title',
  'raw_company',
  'description_clean',
];

const MAX_TECH_STACK = 30;

/** Real salary ranges do not span 60x. A range wider than this is the model
 *  conflating two different numbers, not a band. See CLAUDE.md. */
const MAX_SALARY_SPAN_RATIO = 20;

/** Deliberately narrow: the token, not the concept. Non-global, so there is no
 *  lastIndex to reset between calls. */
const N8N_TOKEN = /\bn8n\b/i;

function err(field, code, message) {
  return { field, code, message };
}

/** Idempotent on the already-clean descriptions Remotive returns; present so
 *  the check still holds if raw HTML is ever piped in instead. */
function stripTags(text) {
  return String(text).replace(/<[^>]*>/g, ' ');
}

function isNonEmptyString(value) {
  return typeof value === 'string' && value.trim() !== '';
}

function checkRequiredString(errors, field, value) {
  if (value === undefined || value === null) {
    errors.push(err(field, 'MISSING', `${field} is required`));
    return false;
  }
  if (typeof value !== 'string') {
    errors.push(err(field, 'TYPE', `${field} must be a string, got ${typeof value}`));
    return false;
  }
  if (value.trim() === '') {
    errors.push(err(field, 'EMPTY', `${field} must not be empty`));
    return false;
  }
  return true;
}

function checkEnum(errors, field, value, allowed) {
  if (value === undefined || value === null) {
    errors.push(err(field, 'MISSING', `${field} is required`));
    return false;
  }
  if (typeof value !== 'string') {
    errors.push(err(field, 'TYPE', `${field} must be a string, got ${typeof value}`));
    return false;
  }
  if (allowed.indexOf(value) === -1) {
    errors.push(
      err(field, 'ENUM', `${field} must be one of ${allowed.join(', ')}, got ${value}`)
    );
    return false;
  }
  return true;
}

function checkBoolean(errors, field, value) {
  if (value === undefined || value === null) {
    errors.push(err(field, 'MISSING', `${field} is required`));
    return false;
  }
  if (typeof value !== 'boolean') {
    errors.push(err(field, 'TYPE', `${field} must be a boolean, got ${typeof value}`));
    return false;
  }
  return true;
}

function checkTechStack(errors, value) {
  if (value === undefined || value === null) {
    errors.push(err('tech_stack', 'MISSING', 'tech_stack is required'));
    return false;
  }
  if (!Array.isArray(value)) {
    errors.push(err('tech_stack', 'TYPE', `tech_stack must be an array, got ${typeof value}`));
    return false;
  }
  let ok = true;
  if (value.length > MAX_TECH_STACK) {
    errors.push(
      err(
        'tech_stack',
        'ARRAY_LEN',
        `tech_stack must have at most ${MAX_TECH_STACK} items, got ${value.length}`
      )
    );
    ok = false;
  }
  for (let i = 0; i < value.length; i++) {
    if (!isNonEmptyString(value[i])) {
      errors.push(
        err('tech_stack', 'ARRAY_ITEM', `tech_stack[${i}] must be a non-empty string`)
      );
      ok = false;
    }
  }
  return ok;
}

/**
 * A salary bound is a positive finite number or null. Zero is rejected: it is a
 * meaningless lower bound, and rejecting it means the span ratio below never
 * divides by zero.
 *
 * Returns 'number' | 'null' | 'invalid' so the cross-field checks know whether
 * they can trust the value.
 */
function checkSalaryBound(errors, field, value) {
  if (value === null) return 'null';
  if (typeof value !== 'number') {
    errors.push(
      err(field, 'TYPE', `${field} must be a number or null, got ${value === undefined ? 'undefined' : typeof value}`)
    );
    return 'invalid';
  }
  if (!Number.isFinite(value) || value <= 0) {
    errors.push(err(field, 'RANGE', `${field} must be a positive finite number, got ${value}`));
    return 'invalid';
  }
  return 'number';
}

/** Runs only on bounds that passed their own checks, so one bad number does not
 *  cascade into three misleading errors. */
function checkSalaryRelations(errors, input, minKind, maxKind, periodOk) {
  if (minKind === 'invalid' || maxKind === 'invalid') return;

  const min = input.salary_min;
  const max = input.salary_max;

  if (minKind === 'null' && maxKind === 'null') return;

  if (minKind === 'null' || maxKind === 'null') {
    errors.push(
      err(
        'salary_min',
        'SALARY_PARTIAL',
        'salary_min and salary_max must both be set or both be null'
      )
    );
  } else if (min > max) {
    errors.push(
      err('salary_min', 'SALARY_ORDER', `salary_min (${min}) must not exceed salary_max (${max})`)
    );
  } else if (max / min > MAX_SALARY_SPAN_RATIO) {
    errors.push(
      err(
        'salary_max',
        'SALARY_SPAN_IMPLAUSIBLE',
        `salary range spans ${(max / min).toFixed(1)}x (${min} to ${max}), over the ${MAX_SALARY_SPAN_RATIO}x limit; the bounds are probably different units`
      )
    );
  }

  // A number without a period is not a salary, it is a digit.
  if (periodOk && input.salary_period === 'unspecified') {
    errors.push(
      err(
        'salary_period',
        'SALARY_PERIOD_MISMATCH',
        'salary_period must not be unspecified when a salary bound is set'
      )
    );
  }
}

function checkUnknownFields(errors, input) {
  const keys = Object.keys(input);
  for (let i = 0; i < keys.length; i++) {
    if (SCHEMA_FIELDS.indexOf(keys[i]) === -1) {
      errors.push(
        err(keys[i], 'UNKNOWN_FIELD', `${keys[i]} is not in the ${TOOL_NAME} input_schema`)
      );
    }
  }
}

/** Pulls the forced tool_use block out of the Anthropic message. Forced
 *  tool_choice makes absence unlikely but not impossible: a max_tokens stop
 *  returns a truncated message with no usable block. */
function findToolInput(extracted) {
  if (!extracted || typeof extracted !== 'object') return null;
  const content = extracted.content;
  if (!Array.isArray(content)) return null;
  for (let i = 0; i < content.length; i++) {
    const block = content[i];
    if (
      block &&
      block.type === 'tool_use' &&
      block.name === TOOL_NAME &&
      block.input &&
      typeof block.input === 'object' &&
      !Array.isArray(block.input)
    ) {
      return block.input;
    }
  }
  return null;
}

function deadLetterRecord(source, extracted, ingestedAt, errors) {
  const seen = {};
  const codes = [];
  for (let i = 0; i < errors.length; i++) {
    if (!seen[errors[i].code]) {
      seen[errors[i].code] = true;
      codes.push(errors[i].code);
    }
  }
  return {
    source_job_id: source && source.source_job_id != null ? String(source.source_job_id) : null,
    source_url: source ? source.source_url ?? null : null,
    raw_title: source ? source.raw_title ?? null : null,
    raw_company: source ? source.raw_company ?? null : null,
    ingested_at: ingestedAt,
    model: extracted && typeof extracted.model === 'string' ? extracted.model : null,
    extraction_version: EXTRACTION_VERSION,
    // Kept here and nowhere else: too long for jobs_extracted, needed to
    // diagnose the failure.
    description_clean: source ? source.description_clean ?? null : null,
    raw_output: JSON.stringify(extracted ?? null),
    // Codes are the queryable dimension; the JSON is the payload you read when
    // you open a row.
    error_codes: codes,
    errors_json: JSON.stringify(errors),
  };
}

function validate(item, ingestedAt) {
  const source = item ? item.source : undefined;
  const extracted = item ? item.extracted : undefined;
  const errors = [];

  // --- source provenance -------------------------------------------------
  // Separate from model errors on purpose: a missing source_job_id is a
  // pipeline fault and should not inflate the model error rate.
  if (!source || typeof source !== 'object') {
    errors.push(err('source', 'MISSING', 'source is required'));
  } else {
    // Remotive emits a numeric id. The JSON type is theirs to choose, so this
    // checks presence rather than type; the record coerces it to a string.
    if (source.source_job_id === undefined || source.source_job_id === null || String(source.source_job_id).trim() === '') {
      errors.push(err('source_job_id', 'MISSING', 'source_job_id is required'));
    }
    for (let i = 0; i < REQUIRED_SOURCE_STRINGS.length; i++) {
      checkRequiredString(errors, REQUIRED_SOURCE_STRINGS[i], source[REQUIRED_SOURCE_STRINGS[i]]);
    }
  }

  // --- envelope ----------------------------------------------------------
  const input = findToolInput(extracted);
  if (input === null) {
    errors.push(
      err(
        'extracted',
        'TOOL_USE_MISSING',
        `no ${TOOL_NAME} tool_use block in the response (stop_reason: ${extracted && extracted.stop_reason ? extracted.stop_reason : 'unknown'})`
      )
    );
    return {
      valid: false,
      errors: errors,
      record: deadLetterRecord(source, extracted, ingestedAt, errors),
    };
  }

  // --- schema ------------------------------------------------------------
  checkRequiredString(errors, 'normalized_title', input.normalized_title);
  checkRequiredString(errors, 'company', input.company);
  checkEnum(errors, 'seniority', input.seniority, SENIORITY);
  checkEnum(errors, 'remote_type', input.remote_type, REMOTE_TYPE);
  const periodOk = checkEnum(errors, 'salary_period', input.salary_period, SALARY_PERIOD);
  checkTechStack(errors, input.tech_stack);
  const n8nOk = checkBoolean(errors, 'mentions_n8n', input.mentions_n8n);
  checkBoolean(errors, 'ai_ml_role', input.ai_ml_role);

  const minKind = checkSalaryBound(errors, 'salary_min', input.salary_min);
  const maxKind = checkSalaryBound(errors, 'salary_max', input.salary_max);
  checkSalaryRelations(errors, input, minKind, maxKind, periodOk);

  checkUnknownFields(errors, input);

  // --- model cross-check -------------------------------------------------
  // Independent ground truth for mentions_n8n. Never corrects the value:
  // overwriting would hide the error rate, which is the point of measuring it.
  const descriptionUsable = !!source && isNonEmptyString(source.description_clean);
  const mentionsN8nActual = descriptionUsable
    ? N8N_TOKEN.test(stripTags(source.description_clean))
    : false;

  if (n8nOk && descriptionUsable && input.mentions_n8n !== mentionsN8nActual) {
    errors.push(
      err(
        'mentions_n8n',
        'N8N_MISMATCH',
        `model said ${input.mentions_n8n}, description ${mentionsN8nActual ? 'contains' : 'does not contain'} the token n8n`
      )
    );
  }

  if (errors.length > 0) {
    return {
      valid: false,
      errors: errors,
      record: deadLetterRecord(source, extracted, ingestedAt, errors),
    };
  }

  return {
    valid: true,
    errors: [],
    record: {
      source_job_id: String(source.source_job_id),
      source_url: source.source_url,
      raw_title: source.raw_title,
      raw_company: source.raw_company,
      ingested_at: ingestedAt,
      model: typeof extracted.model === 'string' ? extracted.model : null,
      extraction_version: EXTRACTION_VERSION,

      normalized_title: input.normalized_title,
      company: input.company,
      seniority: input.seniority,
      tech_stack: input.tech_stack,
      remote_type: input.remote_type,
      salary_min: input.salary_min,
      salary_max: input.salary_max,
      salary_period: input.salary_period,
      // Equal by construction on this branch: disagreements route to
      // dead_letter_log. Both are stored so the rate stays a query.
      mentions_n8n_model: input.mentions_n8n,
      mentions_n8n_actual: mentionsN8nActual,
      ai_ml_role: input.ai_ml_role,
    },
  };
}

if (typeof module !== 'undefined' && module.exports) {
  module.exports = { validate, EXTRACTION_VERSION, MAX_SALARY_SPAN_RATIO, MAX_TECH_STACK };
}
