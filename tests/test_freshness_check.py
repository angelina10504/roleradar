"""The external freshness check. No database and no network: the clock and the
HTTP call are both arguments, so every case is a plain function call."""

from __future__ import annotations

import sys
import urllib.error
from datetime import datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from conftest import REPO

sys.path.insert(0, str(REPO / "monitor"))

import check_freshness as cf  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
SIX = [dtime(6, 0)]
GRACE = timedelta(minutes=90)
ARGS = ["--url", "https://example.test/api/pipeline-status?token=SECRETQUERY",
        "--schedule", "06:00", "--timezone", "Asia/Kolkata"]


def ist(day, hour, minute=0):
    return datetime(2026, 11, day, hour, minute, tzinfo=IST)


def status(started=None, pipeline_status="healthy", stale=0, reasons=()):
    run = None if started is None else {"started_at": started.astimezone(timezone.utc).isoformat()}
    return {
        "pipeline_status": pipeline_status,
        "reasons": list(reasons),
        "last_successful_run": run,
        "running": {"count": stale, "stale_count": stale, "runs": []},
    }


def run_main(payload=None, now=None, error=None, extra=()):
    lines = []

    def fetch(url, timeout, attempts, pause):
        if error:
            raise error
        return payload

    code = cf.main(ARGS + list(extra), now=now, fetch=fetch, out=lines.append)
    return code, "\n".join(lines)


# -- which run is due --------------------------------------------------------


def test_before_grace_ends_yesterdays_run_is_the_one_due():
    assert cf.last_due_run(ist(12, 7, 29), SIX, IST, GRACE) == ist(11, 6)


def test_once_grace_ends_todays_run_is_due():
    assert cf.last_due_run(ist(12, 7, 30), SIX, IST, GRACE) == ist(12, 6)


def test_the_schedule_is_read_in_its_own_timezone():
    # 01:00 UTC on the 12th is 06:30 IST: today's run is still inside grace.
    now = datetime(2026, 11, 12, 1, 0, tzinfo=timezone.utc)
    assert cf.last_due_run(now, SIX, IST, GRACE) == ist(11, 6)


def test_several_runs_a_day():
    times = cf.parse_schedule("18:00, 06:00")
    assert cf.last_due_run(ist(12, 20), times, IST, GRACE) == ist(12, 18)
    assert cf.last_due_run(ist(12, 19), times, IST, GRACE) == ist(12, 6)


# -- the verdict -------------------------------------------------------------


def test_the_due_run_completed():
    code, text = run_main(status(ist(12, 6, 0)), now=ist(12, 9))
    assert code == cf.OK and text.startswith("OK")


def test_a_run_a_little_early_still_counts():
    assert run_main(status(ist(12, 5, 58)), now=ist(12, 9))[0] == cf.OK


def test_yesterdays_run_does_not_cover_today():
    code, text = run_main(status(ist(11, 6, 0)), now=ist(12, 9))
    assert code == cf.PIPELINE_UNHEALTHY and "missed run" in text


def test_a_run_inside_its_grace_period_is_not_yet_missed():
    assert run_main(status(ist(11, 6, 0)), now=ist(12, 7, 0))[0] == cf.OK


def test_no_completed_run_at_all_is_a_missed_run():
    code, text = run_main(status(None, "no_data", reasons=["never_executed"]), now=ist(12, 9))
    assert code == cf.PIPELINE_UNHEALTHY
    assert "no completed run exists" in text and "no_data (never_executed)" in text


def test_a_stuck_run_fails_even_when_the_due_run_completed():
    code, text = run_main(status(ist(12, 6), "degraded", stale=1), now=ist(12, 9))
    assert code == cf.PIPELINE_UNHEALTHY and "stuck run" in text


def test_a_failed_pipeline_status_fails_even_with_a_fresh_run():
    code, text = run_main(status(ist(12, 6), "failed", reasons=["last_run_failed"]), now=ist(12, 9))
    assert code == cf.PIPELINE_UNHEALTHY and "last_run_failed" in text


def test_degraded_warns_and_passes_unless_strict():
    payload = status(ist(12, 6), "degraded", reasons=["recent_failures"])
    code, text = run_main(payload, now=ist(12, 9))
    assert code == cf.OK and "warning: pipeline_status is degraded" in text
    assert run_main(payload, now=ist(12, 9), extra=["--strict"])[0] == cf.PIPELINE_UNHEALTHY


# -- an unhealthy API is a different failure ---------------------------------


def test_an_api_that_cannot_answer_exits_2_not_1():
    code, text = run_main(error=cf.ApiError("HTTP 503 after 3 attempt(s)"), now=ist(12, 9))
    assert code == cf.API_UNHEALTHY
    assert "API UNHEALTHY" in text and "not evidence about the pipeline" in text


@pytest.mark.parametrize(
    "payload",
    [{}, {"pipeline_status": "healthy"}, {"error": "database_unavailable"},
     {"pipeline_status": "splendid", "last_successful_run": None, "running": {"stale_count": 0}},
     {"pipeline_status": "healthy", "last_successful_run": {"started_at": "soon"},
      "running": {"stale_count": 0}}],
)
def test_a_response_of_the_wrong_shape_is_an_api_problem(payload):
    assert run_main(payload, now=ist(12, 9))[0] == cf.API_UNHEALTHY


def test_the_url_is_printed_without_its_query_string():
    _, text = run_main(error=cf.ApiError("HTTP 500"), now=ist(12, 9))
    assert "example.test/api/pipeline-status" in text and "SECRETQUERY" not in text


def test_fetch_retries_then_gives_up(monkeypatch):
    calls, naps = [], []

    def refuse(request, timeout):
        calls.append(request.full_url)
        raise urllib.error.HTTPError(request.full_url, 503, "unavailable", None, None)

    monkeypatch.setattr(cf.urllib.request, "urlopen", refuse)
    with pytest.raises(cf.ApiError, match="HTTP 503 after 3 attempt"):
        cf.fetch_status("https://example.test/s", 1, 3, 15, sleep=naps.append)
    assert len(calls) == 3 and naps == [15, 15]


def test_fetch_stops_retrying_once_it_gets_an_answer(monkeypatch):
    attempts = []

    class Answer:
        def __enter__(self): return self
        def __exit__(self, *_): return False
        def read(self): return b'{"pipeline_status": "healthy"}'

    def wake_on_second(request, timeout):
        attempts.append(1)
        if len(attempts) == 1:
            raise urllib.error.HTTPError(request.full_url, 503, "waking", None, None)
        return Answer()

    monkeypatch.setattr(cf.urllib.request, "urlopen", wake_on_second)
    assert cf.fetch_status("https://example.test/s", 1, 3, 0, sleep=lambda _s: None) == {
        "pipeline_status": "healthy"}
    assert len(attempts) == 2


# -- configuration -----------------------------------------------------------


@pytest.mark.parametrize(
    "argv",
    [
        ["--schedule", "06:00", "--timezone", "Asia/Kolkata"],
        ["--url", "https://x.test/s", "--timezone", "Asia/Kolkata"],
        ["--url", "https://x.test/s", "--schedule", "06:00"],
        ["--url", "https://x.test/s", "--schedule", "6am", "--timezone", "Asia/Kolkata"],
        ["--url", "https://x.test/s", "--schedule", "25:00", "--timezone", "Asia/Kolkata"],
        ["--url", "https://x.test/s", "--schedule", "06:00", "--timezone", "Mars/Olympus"],
        ["--url", "ftp://x.test/s", "--schedule", "06:00", "--timezone", "Asia/Kolkata"],
    ],
)
def test_a_misconfigured_check_exits_3_without_calling_anything(argv, monkeypatch):
    for name in ("ROLERADAR_STATUS_URL", "ROLERADAR_SCHEDULE", "ROLERADAR_SCHEDULE_TZ"):
        monkeypatch.delenv(name, raising=False)

    def never(*_args):
        raise AssertionError("the API was called")

    assert cf.main(argv, fetch=never, out=lambda _line: None) == cf.MISCONFIGURED
