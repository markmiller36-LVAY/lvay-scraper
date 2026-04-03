"""
LVAY - Scheduler + Server
==========================
Runs the scraper on schedule and exports to Google Sheets.
Also serves the JSON API for WordPress.
"""

import schedule
import time
import threading
from datetime import datetime
from scraper import run_all_sports
from sheets_exporter import export_all_to_sheets
from server import app


def scheduled_run():
    """Full pipeline: scrape LHSAA then export to Google Sheets."""
    print(f"\n[SCHEDULER] Triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        # Step 1: Scrape LHSAA
        run_all_sports()
        # Step 2: Export to Google Sheets
        export_all_to_sheets()
    except Exception as e:
        print(f"[SCHEDULER] ERROR: {e}")


def setup_schedule():
    # Every 4 hours — catches scores as coaches enter them
    schedule.every(4).hours.do(scheduled_run)
    # Extra Tuesday 6am run for official power ratings PDF
    schedule.every().tuesday.at("06:00").do(scheduled_run)
    # Run immediately on startup
    print("[SCHEDULER] Running initial scrape on startup...")
    scheduled_run()
    print("[SCHEDULER] Schedule active — every 4 hours + Tuesday 6am")


def run_scheduler():
    setup_schedule()
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    # Start scheduler in background
    t = threading.Thread(target=run_scheduler, daemon=True)
    t.start()
    # Start web server
    import os
    port = int(os.environ.get("PORT", 5000))
    print(f"[SERVER] Starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
