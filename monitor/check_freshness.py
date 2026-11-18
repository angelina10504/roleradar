#!/usr/bin/env python3
"""External freshness check for the RoleRadar pipeline.

Runs somewhere that is not the machine n8n runs on, and asks one question the
pipeline cannot answer about itself: did the run that was due actually happen.
If n8n is stopped, nothing fails and nothing is written, so no part of the
pipeline can report it. Something outside has to notice the silence.

It calls GET /api/pipeline-status and checks the answer against the schedule
it was given. Standard library only, so it needs no install step.

Exit codes, so a caller can tell the cases apart:

  0  the pipeline ran when it was due
  1  the pipeline did not: a run was missed, a run is stuck, or the API
     reports the pipeline failed
  2  the API could not answer. This says nothing about the pipeline.
  3  this check is misconfigured

Configuration, as flags or environment variables:

  --url            ROLERADAR_STATUS_URL     full URL of /api/pipeline-status
  --schedule       ROLERADAR_SCHEDULE       daily run times, HH:MM, comma separated
  --timezone       ROLERADAR_SCHEDULE_TZ    IANA zone the times are in
  --grace-minutes  ROLERADAR_GRACE_MINUTES  how late a run may finish (default 90)

No credentials. The endpoint is public and read-only, and this script sends
nothing but a GET.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, time as dtime, timedelta, timezone
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

OK, PIPELINE_UNHEALTHY, API_UNHEALTHY, MISCONFIGURED = 0, 1, 2, 3


class ConfigError(Exception):
    pass


class ApiError(Exception):
    pass


def parse_schedule(text: str) -> list[dtime]:
    """'06:00,18:30' -> [time(6, 0), time(18, 30)]."""
    times = []
    for part in (text or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            hour, minute = part.split(":")
            times.append(dtime(int(hour), int(minute)))
        except ValueError:
            raise ConfigError(f"schedule entry {part!r} is not HH:MM") from None
    if not times:
        raise ConfigError("no schedule given. Set --schedule, for example 06:00")
    return sorted(times)


def last_due_run(now: datetime, times: list[dtime], zone: ZoneInfo, grace: timedelta) -> datetime:
    """The most recent scheduled start whose grace period has already ended.

    A run that is scheduled but still inside its grace period is not due yet,
    so it is skipped and the one before it is the one that must exist.
    """
    local_now = now.astimezone(zone)
    candidates = []
    for days_back in range(0, 4):
        day = (local_now - timedelta(days=days_back)).date()
        for at in times:
            scheduled = datetime.combine(day, at, tzinfo=zone)
            if scheduled + grace <= now:
                candidates.append(scheduled)
    return max(candidates)


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def evaluate(
    status: dict,
    now: datetime,
    times: list[dtime],
    zone: ZoneInfo,
    grace: timedelta,
    early: timedelta = timedelta(minutes=5),
    strict: bool = False,
) -> tuple[list[str], list[str]]:
    """Return (problems, warnings). Pure: the clock is an argument."""
    try:
        pipeline_status = status["pipeline_status"]
        reasons = status.get("reasons") or []
        last_ok = status["last_successful_run"]
        stale = status["running"]["stale_count"]
    except (KeyError, TypeError) as exc:
        raise ApiError(f"status response is missing {exc}") from None

    problems: list[str] = []
    warnings: list[str] = []

    due = last_due_run(now, times, zone, grace)
    due_text = due.strftime("%Y-%m-%d %H:%M %Z")

    # The run that was due must exist and must have completed. `early` allows
    # for a scheduler that fires a little before the minute.
    if last_ok is None:
        problems.append(f"missed run: no completed run exists; one was due at {due_text}")
    else:
        try:
            started = parse_timestamp(last_ok["started_at"])
        except (KeyError, TypeError, ValueError):
            raise ApiError("last_successful_run has no usable started_at") from None
        if started < due - early:
            problems.append(
                f"missed run: the last completed run started {started.isoformat()}, "
                f"before the run due at {due_text}"
            )

    if stale:
        problems.append(f"stuck run: {stale} run(s) still marked running past the stale threshold")

    detail = f" ({', '.join(reasons)})" if reasons else ""
    if pipeline_status in ("failed", "no_data"):
        problems.append(f"pipeline_status is {pipeline_status}{detail}")
    elif pipeline_status == "degraded":
        (problems if strict else warnings).append(f"pipeline_status is degraded{detail}")
    elif pipeline_status != "healthy":
        raise ApiError(f"unknown pipeline_status {pipeline_status!r}")

    return problems, warnings


def fetch_status(url: str, timeout: float, attempts: int, pause: float, sleep=time.sleep) -> dict:
    """GET the status. Retries, because a suspended database answers 503 once
    and then wakes; one slow start should not page anyone."""
    last = "no attempt made"
    for attempt in range(1, attempts + 1):
        try:
            request = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read()
            try:
                parsed = json.loads(body)
            except ValueError:
                raise ApiError("the API answered 200 with something that is not JSON") from None
            if not isinstance(parsed, dict):
                raise ApiError("the API answered 200 with JSON that is not an object")
            return parsed
        except urllib.error.HTTPError as exc:
            last = f"HTTP {exc.code}"
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last = f"no response ({getattr(exc, 'reason', exc)})"
        if attempt < attempts:
            sleep(pause)
    raise ApiError(f"{last} after {attempts} attempt(s)")


def public_name(url: str) -> str:
    """Host and path only. A query string or userinfo is never printed."""
    parts = urlsplit(url)
    return f"{parts.hostname or '?'}{parts.path}"


def main(argv=None, now=None, fetch=fetch_status, out=print) -> int:
    parser = argparse.ArgumentParser(description="RoleRadar freshness check")
    parser.add_argument("--url", default=os.environ.get("ROLERADAR_STATUS_URL", ""))
    parser.add_argument("--schedule", default=os.environ.get("ROLERADAR_SCHEDULE", ""))
    parser.add_argument("--timezone", default=os.environ.get("ROLERADAR_SCHEDULE_TZ", ""))
    parser.add_argument("--grace-minutes", type=float,
                        default=float(os.environ.get("ROLERADAR_GRACE_MINUTES") or 90))
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--retry-pause", type=float, default=15.0)
    parser.add_argument("--strict", action="store_true",
                        help="treat pipeline_status degraded as a failure")
    args = parser.parse_args(argv)

    try:
        if not args.url.startswith(("https://", "http://")):
            raise ConfigError("no status URL. Set --url or ROLERADAR_STATUS_URL")
        times = parse_schedule(args.schedule)
        if not args.timezone:
            raise ConfigError("no timezone. Set --timezone, for example Asia/Kolkata")
        try:
            zone = ZoneInfo(args.timezone)
        except (ZoneInfoNotFoundError, ValueError):
            raise ConfigError(f"unknown timezone {args.timezone!r}") from None
        if args.grace_minutes < 0 or args.attempts < 1:
            raise ConfigError("grace must be >= 0 and attempts >= 1")
    except ConfigError as exc:
        out(f"MISCONFIGURED: {exc}")
        return MISCONFIGURED

    now = now or datetime.now(timezone.utc)
    grace = timedelta(minutes=args.grace_minutes)

    try:
        status = fetch(args.url, args.timeout, args.attempts, args.retry_pause)
        problems, warnings = evaluate(status, now, times, zone, grace, strict=args.strict)
    except ApiError as exc:
        out(f"API UNHEALTHY: {public_name(args.url)}: {exc}")
        out("This is a failure of the dashboard API, not evidence about the pipeline.")
        return API_UNHEALTHY

    for warning in warnings:
        out(f"warning: {warning}")
    if problems:
        out("PIPELINE UNHEALTHY:")
        for problem in problems:
            out(f"  - {problem}")
        return PIPELINE_UNHEALTHY

    last_ok = status["last_successful_run"]
    out(f"OK: last completed run started {last_ok['started_at']}, "
        f"pipeline_status {status['pipeline_status']}")
    return OK


if __name__ == "__main__":
    sys.exit(main())
