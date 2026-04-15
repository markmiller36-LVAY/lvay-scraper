"""
LVAY - Scheduled Tasks
======================
Runs scraper + exports on a schedule.
Use this in a worker / cron job, not in the web service.
"""

import schedule
import time
from datetime import datetime


def scheduled_run():
    print(f"\n[SCHEDULER] Triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        from scraper import run_all_sports
        run_all_sports()

        from sheets_exporter import export_football_to_sheets
        export_football_to_sheets()
    except Exception as e:
        print(f"[SCHEDULER] ERROR: {e}")


def run_scheduler():
    schedule.every(4).hours.do(scheduled_run)
    schedule.every().tuesday.at("06:00").do(scheduled_run)
    print("[SCHEDULER] Schedule active — every 4 hours + Tuesday 6am")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    run_scheduler()
