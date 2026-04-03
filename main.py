"""
LVAY - Scheduler
=================
Runs the scraper automatically on a schedule.
During season: every 4 hours.
Tuesday 6am: extra run to catch new power ratings PDF.
Off season: once daily.
"""

import schedule
import time
import threading
from datetime import datetime
from scraper import run_all_sports as run_scrape
from server import app

FOOTBALL_SEASON_START_MONTH = 9   # September
FOOTBALL_SEASON_END_MONTH   = 12  # December


def is_football_season():
    month = datetime.now().month
    return FOOTBALL_SEASON_START_MONTH <= month <= FOOTBALL_SEASON_END_MONTH


def scheduled_scrape():
    print(f"\n[SCHEDULER] Triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        run_scrape()
    except Exception as e:
        print(f"[SCHEDULER] ERROR: {e}")


def setup_schedule():
    # Every 4 hours during season (catches scores as coaches enter them)
    schedule.every(4).hours.do(scheduled_scrape)

    # Extra Tuesday 6am run for official power ratings PDF
    schedule.every().tuesday.at("06:00").do(scheduled_scrape)

    # Always run once immediately on startup
    print("[SCHEDULER] Running initial scrape on startup...")
    scheduled_scrape()

    print("[SCHEDULER] Schedule set:")
    print("  - Every 4 hours (scores update)")
    print("  - Every Tuesday 6:00 AM (power ratings PDF)")
    print("  - Next run:", schedule.next_run())


def run_scheduler():
    """Background thread that keeps the schedule running."""
    setup_schedule()
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    # Start scheduler in background thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

    # Start web server in foreground (Render needs this to stay alive)
    port = int(__import__("os").environ.get("PORT", 5000))
    print(f"\n[SERVER] Starting on port {port}")
    app.run(host="0.0.0.0", port=port, debug=False)
