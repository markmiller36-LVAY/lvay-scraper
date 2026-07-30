"""
Render Cron entry point.

The cron service cannot share the web service's persistent SQLite disk, so it
securely asks the web service to run the pipeline where /data is mounted.
"""

import os
import sys
import time

import requests


PIPELINE_URL = os.environ.get(
    "PIPELINE_URL",
    "https://lvay-scraper.onrender.com/api/pipeline/run",
)
PIPELINE_TOKEN = os.environ.get("PIPELINE_TOKEN", "")
POLL_INTERVAL = int(os.environ.get("PIPELINE_POLL_INTERVAL", "15"))
PIPELINE_TIMEOUT = int(os.environ.get("PIPELINE_TIMEOUT", "3600"))


def pipeline_status():
    status_url = PIPELINE_URL.rsplit("/", 1)[0] + "/status"
    response = requests.get(
        status_url,
        headers={"X-Pipeline-Token": PIPELINE_TOKEN},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def main():
    if not PIPELINE_TOKEN:
        print("[CRON] PIPELINE_TOKEN is not configured")
        return 2

    response = requests.post(
        PIPELINE_URL,
        headers={"X-Pipeline-Token": PIPELINE_TOKEN},
        timeout=60,
    )
    print(f"[CRON] Pipeline trigger returned HTTP {response.status_code}")
    print(response.text[:1000])

    if response.status_code not in (202, 409):
        return 1
    try:
        expected_started_at = response.json().get("started_at")
    except ValueError:
        expected_started_at = None

    deadline = time.monotonic() + PIPELINE_TIMEOUT
    while time.monotonic() < deadline:
        try:
            state = pipeline_status()
        except requests.RequestException as exc:
            print(f"[CRON] Status check failed; retrying: {exc}")
            time.sleep(POLL_INTERVAL)
            continue

        pipeline_state = state.get("status")
        print(f"[CRON] Pipeline status: {pipeline_state}")
        if (
            expected_started_at
            and state.get("started_at") != expected_started_at
        ):
            time.sleep(POLL_INTERVAL)
            continue
        if pipeline_state == "completed":
            return 0
        if pipeline_state == "failed":
            print(f"[CRON] Pipeline failed: {state.get('error')}")
            return 1
        time.sleep(POLL_INTERVAL)

    print(
        f"[CRON] Pipeline did not finish within {PIPELINE_TIMEOUT} seconds"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
