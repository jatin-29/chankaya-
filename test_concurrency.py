"""
Concurrency load test — simulates N users hitting /upload-paper/ at the same
time and tracks whether the pipeline handles it (success rate, latency,
errors, whether jobs silently hang).

Usage:
    pip install pytest requests --break-system-packages
    pytest test_concurrency.py -v -s

Config via environment variables (all optional):
    LOAD_TEST_URL          default: http://localhost:8000
    LOAD_TEST_USERS        default: 50
    LOAD_TEST_EXAM_ID      default: a1b2c3d4-e5f6-4789-a012-3456789abcde
    LOAD_TEST_PDF_URL      default: a small/fast raw GitHub PDF URL
    LOAD_TEST_POLL_TIMEOUT default: 900 (seconds per job)

WHAT THIS TESTS (and does NOT test):
- Tests: can the FastAPI process accept N simultaneous /upload-paper/ POSTs
  without crashing, and does each job eventually reach done/failed without
  hanging forever or silently disappearing.
- Does NOT test: real Mistral API rate limits at 50x concurrent load — if
  MAX_PARALLEL_CHUNK_CALLS=3 per job and 50 jobs run at once, that's up to
  150 simultaneous Mistral calls, which WILL likely hit Mistral's own
  account-level rate limit (a 429 loop) regardless of your code. This test
  will surface that as slow/failed jobs, which is real, useful signal --
  but the fix for it is a queue/semaphore in front of the job dispatcher,
  not something pytest can fix for you.
"""

import os
import time
import uuid
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import pytest
import requests

BASE_URL = os.getenv("LOAD_TEST_URL", "http://localhost:8000")
NUM_USERS = int(os.getenv("LOAD_TEST_USERS", "50"))
EXAM_ID = os.getenv("LOAD_TEST_EXAM_ID", "a1b2c3d4-e5f6-4789-a012-3456789abcde")
PDF_URL = os.getenv(
    "LOAD_TEST_PDF_URL",
    "https://raw.githubusercontent.com/jatin-29/testing/main/Science-SQP.pdf",
)
POLL_TIMEOUT = int(os.getenv("LOAD_TEST_POLL_TIMEOUT", "900"))
POLL_INTERVAL = 3


@dataclass
class UserResult:
    user_id: int
    submit_ok: bool = False
    submit_latency_s: float = None
    job_id: str = None
    final_status: str = None
    total_time_s: float = None
    error: str = None
    poll_count: int = 0


def _submit_job(user_id: int) -> UserResult:
    result = UserResult(user_id=user_id)
    payload = {
        "exam_id": EXAM_ID,
        "request_id": f"loadtest-{uuid.uuid4()}",
        "user_name": f"loadtest-user-{user_id}",
        "board_name": "CBSE",
        "grade": "Class 10",
        "subject": "Science",
        "topic": "Load Test",
        "exam_type": "WEEKLY_TEST",
        "difficulty_level": "Medium",
        "start_date": "2026-07-16",
        "end_date": "2026-07-16",
        "include_marks_breakdown": True,
        "chapter_json": [
            {"chapter_id": "c1000000-0000-0000-0000-000000000001", "chapter_name": "General"}
        ],
        "question_paper_url": [{"file_name": "test.pdf", "url": PDF_URL}],
    }

    t0 = time.time()
    try:
        resp = requests.post(f"{BASE_URL}/upload-paper/", json=payload, timeout=30)
        result.submit_latency_s = round(time.time() - t0, 2)
        if resp.status_code not in (200, 202):
            result.error = f"submit HTTP {resp.status_code}: {resp.text[:300]}"
            return result
        data = resp.json()
        result.job_id = data.get("job_id")
        result.submit_ok = bool(result.job_id)
        if not result.submit_ok:
            result.error = f"no job_id in response: {data}"
            return result
    except Exception as exc:
        result.submit_latency_s = round(time.time() - t0, 2)
        result.error = f"submit exception: {exc!r}"
        return result

    # Poll until done/failed or timeout
    poll_start = time.time()
    while time.time() - poll_start < POLL_TIMEOUT:
        time.sleep(POLL_INTERVAL)
        result.poll_count += 1
        try:
            status_resp = requests.get(f"{BASE_URL}/job-status/{result.job_id}", timeout=15)
            status_data = status_resp.json()
            status = status_data.get("status")
        except Exception as exc:
            result.error = f"poll exception: {exc!r}"
            continue

        if status in ("done", "failed"):
            result.final_status = status
            result.total_time_s = round(time.time() - t0, 2)
            if status == "failed":
                result.error = status_data.get("error", "unknown failure")
            return result

    result.final_status = "timeout"
    result.total_time_s = round(time.time() - t0, 2)
    result.error = f"exceeded POLL_TIMEOUT={POLL_TIMEOUT}s without reaching done/failed"
    return result


def _run_concurrent_users(n: int) -> list:
    results = []
    print(f"\n[loadtest] Firing {n} concurrent users at {BASE_URL} ...")
    with ThreadPoolExecutor(max_workers=n) as executor:
        futures = {executor.submit(_submit_job, i): i for i in range(1, n + 1)}
        for future in as_completed(futures):
            r = future.result()
            tag = "OK" if r.final_status == "done" else "FAIL"
            print(
                f"  [{tag}] user={r.user_id:>3} submit_ok={r.submit_ok} "
                f"submit_lat={r.submit_latency_s}s status={r.final_status} "
                f"total={r.total_time_s}s polls={r.poll_count} "
                f"{'error=' + r.error if r.error else ''}"
            )
            results.append(r)
    return results


def _print_summary(results: list):
    n = len(results)
    submitted = [r for r in results if r.submit_ok]
    done = [r for r in results if r.final_status == "done"]
    failed = [r for r in results if r.final_status == "failed"]
    timed_out = [r for r in results if r.final_status == "timeout"]
    submit_failed = [r for r in results if not r.submit_ok]

    print("\n" + "=" * 60)
    print(f"LOAD TEST SUMMARY — {n} simulated concurrent users")
    print("=" * 60)
    print(f"  Submitted successfully : {len(submitted)}/{n}")
    print(f"  Reached 'done'         : {len(done)}/{n}")
    print(f"  Reached 'failed'       : {len(failed)}/{n}")
    print(f"  Timed out (never done) : {len(timed_out)}/{n}")
    print(f"  Failed to even submit  : {len(submit_failed)}/{n}")

    submit_latencies = [r.submit_latency_s for r in results if r.submit_latency_s is not None]
    if submit_latencies:
        print(f"\n  Submit latency  — min={min(submit_latencies)}s "
              f"avg={round(statistics.mean(submit_latencies), 2)}s "
              f"max={max(submit_latencies)}s")

    total_times = [r.total_time_s for r in done]
    if total_times:
        print(f"  Job completion  — min={min(total_times)}s "
              f"avg={round(statistics.mean(total_times), 2)}s "
              f"max={max(total_times)}s "
              f"(p95={round(sorted(total_times)[int(len(total_times)*0.95)] if len(total_times) > 1 else total_times[0], 2)}s)")

    if failed or timed_out or submit_failed:
        print("\n  --- Failures / issues ---")
        for r in failed + timed_out + submit_failed:
            print(f"    user={r.user_id}: {r.error}")
    print("=" * 60 + "\n")


class TestConcurrency:
    def test_pipeline_handles_concurrent_users(self):
        """
        Fires NUM_USERS simultaneous /upload-paper/ requests and confirms
        every one of them reaches a terminal status (done/failed) within
        POLL_TIMEOUT — i.e. the server doesn't hang, crash, or silently drop
        jobs under concurrent load.
        """
        results = _run_concurrent_users(NUM_USERS)
        _print_summary(results)

        submit_failures = [r for r in results if not r.submit_ok]
        timeouts = [r for r in results if r.final_status == "timeout"]

        # Hard requirement: the SERVER must accept every submission (even if
        # the underlying job later fails due to e.g. a fake exam_id 404 at
        # create-paper/ -- that's a data issue, not a concurrency issue).
        assert not submit_failures, (
            f"{len(submit_failures)}/{NUM_USERS} requests failed to even be "
            f"accepted by the server under concurrent load — this indicates "
            f"the server itself is dropping/rejecting connections at this "
            f"concurrency level, not a downstream job failure."
        )

        # Soft-ish requirement: no job should hang forever (never reach a
        # terminal status). A few real failures (e.g. bad exam_id) are fine
        # and expected -- true hangs are not.
        assert not timeouts, (
            f"{len(timeouts)}/{NUM_USERS} jobs never reached done/failed "
            f"within {POLL_TIMEOUT}s — likely thread-pool starvation or a "
            f"deadlock under concurrent load. Check MAX_PARALLEL_CHUNK_CALLS "
            f"and whether the job dispatcher itself queues properly."
        )