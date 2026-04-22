"""
LVAY - Scheduled Tasks
======================
Runs scraper + rankings + exports on a schedule.
Sport selection is season-aware — only runs sports currently in season.
"""
import schedule
import time
from datetime import datetime
import os

def get_active_sports():
    """Return list of sports currently in season based on month."""
    month = datetime.now().month
    sports = []
    if month in [8, 9, 10, 11]:        # Aug - Nov
        sports.append("football")
    if month in [2, 3, 4, 5]:          # Feb - May
        sports.append("baseball")
        sports.append("softball")
    # Basketball, soccer, volleyball to be added later
    return sports

def scheduled_run():
    print(f"\n[SCHEDULER] Triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    active = get_active_sports()
    print(f"[SCHEDULER] Active sports this month: {active}")

    try:
        # 1. SCRAPE
        print("[SCHEDULER] Running scraper...")
        from scraper import run_all_sports
        run_all_sports()

        # 2. FOOTBALL
        if "football" in active:
            print("[SCHEDULER] Running football pipeline...")
            from run_power_rankings import run_power_rankings
            run_power_rankings(sport="football", season="2025")
            from sheets_exporter import (
                export_football_to_sheets,
                export_football_scores,
                export_division_and_class_tabs,
            )
            export_football_to_sheets()
            export_football_scores()
            export_division_and_class_tabs()
            print("[SCHEDULER] Football pipeline complete")

        # 3. BASEBALL
        if "baseball" in active:
            print("[SCHEDULER] Running baseball pipeline...")
            from import_oos_baseball_2026 import run as import_oos_baseball
            import_oos_baseball()
            from run_power_rankings import run_power_rankings
            run_power_rankings(sport="baseball", season="2026")
            from sheets_exporter import export_baseball_to_sheets
            export_baseball_to_sheets()
            print("[SCHEDULER] Baseball pipeline complete")

        # 4. SOFTBALL
        if "softball" in active:
            print("[SCHEDULER] Running softball pipeline...")
            from import_oos_softball_2026 import run as import_oos_softball
            import_oos_softball()
            from run_power_rankings import run_power_rankings
            run_power_rankings(sport="softball", season="2026")
            from sheets_exporter import export_softball_to_sheets
            export_softball_to_sheets()
            print("[SCHEDULER] Softball pipeline complete")

        print("[SCHEDULER] ALL COMPLETE")

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
