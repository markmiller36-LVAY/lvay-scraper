"""
LVAY - Scheduler + Server
==========================
Runs the scraper on schedule and exports to Google Sheets.
Also serves the JSON API for WordPress.
"""
import os
import schedule
import time
import threading
from datetime import datetime


def scheduled_run():
    """Full pipeline: scrape LHSAA then export to Google Sheets."""
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


# Import app here so gunicorn can find it via main:app
from server import app  # noqa: E402

if __name__ == "__main__":
    t = threading.Thread(target=run_scheduler, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 5000))
    print(f"[SERVER] Starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
