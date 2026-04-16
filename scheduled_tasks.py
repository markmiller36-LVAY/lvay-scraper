"""
LVAY - Scheduled Tasks
======================
Runs scraper + rankings + exports on a schedule.
"""

import schedule
import time
from datetime import datetime
import os


def scheduled_run():
    print(f"\n[SCHEDULER] Triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # 1. SCRAPE / IMPORT
        print("[SCHEDULER] Running scraper...")
        from scraper import run_all_sports
        run_all_sports()

        # 2. RUN POWER RATINGS
        print("[SCHEDULER] Running football power rankings...")
        os.environ["RANKINGS_SPORT"] = "football"
        os.environ["RANKINGS_SEASON"] = "2025"

        from run_power_rankings import run_power_rankings
        run_power_rankings()

        # 3. EXPORT TO GOOGLE SHEETS
        print("[SCHEDULER] Exporting to Google Sheets...")
        from sheets_exporter import (
            export_football_to_sheets,
            export_football_scores,
            export_division_and_class_tabs,
        )

        export_football_to_sheets()
        export_football_scores()
        export_division_and_class_tabs()

        print("[SCHEDULER] COMPLETE")

    except Exception as e:
        print(f"[SCHEDULER] ERROR: {e}")


def run_scheduler():
    schedule.every(4).hours.do(scheduled_run)
    schedule.every().tuesday.at("06:00").do(scheduled_run)

    print("[SCHEDULER] Active — every 4 hours + Tuesday 6am")

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    run_scheduler()
