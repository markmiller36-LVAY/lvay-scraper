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

    if (
        month in [8, 9, 10, 11]
        and os.environ.get("ENABLE_FOOTBALL", "true").lower() == "true"
    ):                                  # Aug - Nov
        sports.append("football")
    if month in [8, 9, 10, 11]:
        sports.append("volleyball")

    if month in [2, 3, 4, 5]:          # Feb - May
        sports.append("baseball")
        sports.append("softball")

    if month in [10, 11, 12, 1, 2, 3]:
        sports.extend([
            "boys_basketball", "girls_basketball",
            "boys_soccer", "girls_soccer",
        ])
    return sports


def scheduled_run():
    print(f"\n[SCHEDULER] Triggered at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    active = get_active_sports()
    print(f"[SCHEDULER] Active sports this month: {active}")

    try:
        # 1. SCRAPE
        print("[SCHEDULER] Running scraper...")
        from scraper import run_all_sports, resolve_season_year
        run_all_sports()

        # 2. FOOTBALL
        if "football" in active:
            season = resolve_season_year("football")
            print("[SCHEDULER] Running football pipeline...")
            from run_power_rankings import run_power_rankings
            run_power_rankings(sport="football", season=season)
            from sheets_exporter import (
                export_football_to_sheets,
                export_football_scores,
                export_division_and_class_tabs,
            )
            export_football_to_sheets(season=season)
            export_football_scores(season=season)
            export_division_and_class_tabs(season=season)
            print("[SCHEDULER] Football pipeline complete")

        # 3. VOLLEYBALL
        if "volleyball" in active:
            print("[SCHEDULER] Running volleyball pipeline...")
            from scraper_volleyball import run_volleyball_scraper
            run_volleyball_scraper()
            from run_power_rankings_volleyball import run_volleyball_rankings
            run_volleyball_rankings()
            from sheets_exporter import export_volleyball_to_sheets
            export_volleyball_to_sheets()
            print("[SCHEDULER] Volleyball pipeline complete")

        # 4. BASEBALL
        if "baseball" in active:
            season = resolve_season_year("baseball")
            print("[SCHEDULER] Running baseball pipeline...")
            from import_oos_baseball_2026 import run as import_oos_baseball
            import_oos_baseball()
            from run_power_rankings import run_power_rankings
            run_power_rankings(sport="baseball", season=season)
            from sheets_exporter import export_baseball_to_sheets
            export_baseball_to_sheets(season=int(season))
            print("[SCHEDULER] Baseball pipeline complete")

        # 5. SOFTBALL
        if "softball" in active:
            season = resolve_season_year("softball")
            print("[SCHEDULER] Running softball pipeline...")
            from import_oos_softball_2026 import run as import_oos_softball
            import_oos_softball()
            from run_power_rankings import run_power_rankings
            run_power_rankings(sport="softball", season=season)
            from sheets_exporter import export_softball_to_sheets
            export_softball_to_sheets(season=int(season))
            print("[SCHEDULER] Softball pipeline complete")

        # 6. BASKETBALL AND SOCCER
        for sport in (
            "boys_basketball", "girls_basketball",
            "boys_soccer", "girls_soccer",
        ):
            if sport not in active:
                continue
            season = resolve_season_year(sport)
            print(f"[SCHEDULER] Running {sport} pipeline...")
            from scraper import scrape_sport
            scrape_sport(sport)
            from run_power_rankings import run_power_rankings
            run_power_rankings(sport=sport, season=season)
            from sheets_exporter import export_winter_sport_to_sheets
            export_winter_sport_to_sheets(sport, season)
            print(f"[SCHEDULER] {sport} pipeline complete")

        print("[SCHEDULER] ALL COMPLETE")

    except Exception as e:
        print(f"[SCHEDULER] ERROR: {e}")
        raise


def run_scheduler():
    schedule.every(4).hours.do(scheduled_run)
    schedule.every().tuesday.at("06:00").do(scheduled_run)
    print("[SCHEDULER] Active — every 4 hours + Tuesday 6am")
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    run_scheduler()
