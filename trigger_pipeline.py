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


# Mark's schedule (Oct 15, 2026), Central time. Two groups take turns so
# each run stays short and the site stays quick:
#   - football + volleyball (and baseball/softball in spring) every 4 hours
#     starting at 5am: 1, 5, 9am, 1, 5, 9pm
#   - basketball (+ soccer) every 4 hours starting at 6am: 2, 6, 10am, 2, 6, 10pm
# Sports out of season are skipped by the web service. Thursday/Friday
# game-night refreshes (10pm-1am) come from a separate hourly scheduled task.
PRIMARY_HOURS = {1, 5, 9, 13, 17, 21}
WINTER_HOURS = {2, 6, 10, 14, 18, 22}
PRIMARY_SPORTS = ["football", "volleyball", "baseball", "softball"]
WINTER_SPORTS = ["boys_basketball", "girls_basketball", "boys_soccer", "girls_soccer"]
WINTER_MONTHS = {10, 11, 12, 1, 2, 3}


def sports_for_now(now=None):
    """The sports to run in this slot, or None when nothing is scheduled."""
    now = (now or datetime.now(CENTRAL)).astimezone(CENTRAL)
    # Render sometimes starts the :00 wake-up a few minutes late, so treat
    # the first ten minutes of the hour as that slot.
    if now.minute >= 10:
        return None
    if now.hour in PRIMARY_HOURS:
        return list(PRIMARY_SPORTS)
    if now.hour in WINTER_HOURS and now.month in WINTER_MONTHS:
        return list(WINTER_SPORTS)
    return None


def should_trigger_now(now=None):
    return sports_for_now(now) is not None


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

    sports = sports_for_now()
    print(f"[CRON] Sports this run: {sports}")
    response = requests.post(
        PIPELINE_URL,
        headers={"X-Pipeline-Token": PIPELINE_TOKEN},
        json={"sports": sports} if sports else None,
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
