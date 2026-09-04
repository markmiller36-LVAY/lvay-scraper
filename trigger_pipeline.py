"""
Render Cron entry point.

The cron service cannot share the web service's persistent SQLite disk, so it
securely asks the web service to run the pipeline where /data is mounted.
"""

import os
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import requests


PIPELINE_URL = os.environ.get(
    "PIPELINE_URL",
    "https://lvay-scraper.onrender.com/api/pipeline/run",
)
PIPELINE_TOKEN = os.environ.get("PIPELINE_TOKEN", "")
POLL_INTERVAL = int(os.environ.get("PIPELINE_POLL_INTERVAL", "15"))
PIPELINE_TIMEOUT = int(os.environ.get("PIPELINE_TIMEOUT", "3600"))
CENTRAL = ZoneInfo("America/Chicago")


def should_trigger_now(now=None):
    """Apply the regular cadence plus Thu/Fri football game-night boosts."""
    now = (now or datetime.now(CENTRAL)).astimezone(CENTRAL)
    weekday, hour, minute = now.weekday(), now.hour, now.minute

    # Normal schedule: 3, 7, and 11 AM/PM Central every day.
    if minute == 0 and hour in {3, 7, 11, 15, 19, 23}:
        return True

    if now.month not in {8, 9, 10, 11}:
        return False

    # Thursday and Friday early-morning additions.
    if weekday in {3, 4} and minute == 0 and hour in {6, 8, 9}:
        return True
    # Thursday and Friday game nights: every 30 minutes, 10 PM-midnight.
    if weekday in {3, 4} and hour in {22, 23} and minute in {0, 30}:
        return True
    # Midnight immediately following Thursday and Friday game nights.
    if weekday in {4, 5} and hour == 0 and minute == 0:
        return True
    return False


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
    if (
        os.environ.get("SCHEDULE_GATED", "false").lower() == "true"
        and not should_trigger_now()
    ):
        print("[CRON] No pipeline run scheduled for this Central-time slot")
        return 0
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
