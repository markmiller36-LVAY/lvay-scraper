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


def require_success(result, label):
    """Turn a swallowed exporter failure into a failed scheduled run."""
    if result is False:
        raise RuntimeError(f"{label} reported failure")
    return result


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

    from pipeline_reporter import capture_snapshot
    before_snapshot = capture_snapshot(active)

    try:
        # 1. SCRAPE
        print("[SCHEDULER] Running scraper...")
        from scraper import run_all_sports, resolve_season_year
        run_all_sports()

        # 2. FOOTBALL
        if "football" in active:
            season = resolve_season_year("football")
            print("[SCHEDULER] Running football pipeline...")
            oos_summary = None
            if os.environ.get("ENABLE_FOOTBALL_OOS_VERIFIER", "true").lower() == "true":
                try:
                    from football_oos_verifier import run as verify_football_oos
                    oos_summary = verify_football_oos()
                except Exception as oos_error:
                    # Never erase the last verified records or stop LHSAA scores
                    # from publishing because a third-party site/Sheet is down.
                    print(f"[OOS] ERROR; retaining last verified records: {oos_error}")
                    oos_summary = {
                        "checked": 0, "verified": 0, "review": 0,
                        "imported": 0, "changes": [],
                        "issues": [f"Verifier failed: {oos_error}"],
                    }
            from run_power_rankings import run_power_rankings
            run_power_rankings(sport="football", season=season)
            print("[SCHEDULER] Football ratings complete")

        # 3. VOLLEYBALL
        if "volleyball" in active:
            print("[SCHEDULER] Running volleyball pipeline...")
            from scraper_volleyball import run_volleyball_scraper
            run_volleyball_scraper()
            from run_power_rankings_volleyball import run_volleyball_rankings
            run_volleyball_rankings()
            print("[SCHEDULER] Volleyball ratings complete")

        # 4. BASEBALL
        if "baseball" in active:
            season = resolve_season_year("baseball")
            print("[SCHEDULER] Running baseball pipeline...")
            from import_oos_baseball_2026 import run as import_oos_baseball
            import_oos_baseball()
            from run_power_rankings import run_power_rankings
            run_power_rankings(sport="baseball", season=season)
            print("[SCHEDULER] Baseball ratings complete")

        # 5. SOFTBALL
        if "softball" in active:
            season = resolve_season_year("softball")
            print("[SCHEDULER] Running softball pipeline...")
            from import_oos_softball_2026 import run as import_oos_softball
            import_oos_softball()
            from run_power_rankings import run_power_rankings
            run_power_rankings(sport="softball", season=season)
            print("[SCHEDULER] Softball ratings complete")

        # 6. BASKETBALL AND SOCCER
        for sport in (
            "boys_basketball", "girls_basketball",
            "boys_soccer", "girls_soccer",
        ):
            if sport not in active:
                continue
            season = resolve_season_year(sport)
            print(f"[SCHEDULER] Running {sport} pipeline...")
            # run_all_sports() already scraped every active winter sport.
            # Avoid a second LHSAA request and duplicate database rewrite.
            from run_power_rankings import run_power_rankings
            run_power_rankings(sport=sport, season=season)
            print(f"[SCHEDULER] {sport} ratings complete")

        # Send the score/rating report as soon as calculations are current.
        # Google Sheets exports follow afterward and can be materially slower;
        # they must not delay the report email on game nights.
        try:
            from pipeline_reporter import build_report, email_report
            after_snapshot = capture_snapshot(active)
            subject, body, summary = build_report(
                before_snapshot,
                after_snapshot,
                active,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                oos_summary=locals().get("oos_summary"),
            )
            print(f"[REPORT] {summary}")
            email_report(subject, body)
        except Exception as report_error:
            print(f"[REPORT] ERROR (pipeline remains successful): {report_error}")

        # 7. GOOGLE SHEETS EXPORTS
        from sheets_exporter import (
            export_football_to_sheets,
            export_football_scores,
            export_division_and_class_tabs,
            export_volleyball_to_sheets,
            export_baseball_to_sheets,
            export_softball_to_sheets,
            export_winter_sport_to_sheets,
        )
        if "football" in active:
            season = resolve_season_year("football")
            require_success(export_football_to_sheets(season=season), "Football Sheets export")
            require_success(export_football_scores(season=season), "Football scores export")
            require_success(export_division_and_class_tabs(season=season), "Football division/class export")
        if "volleyball" in active:
            require_success(export_volleyball_to_sheets(), "Volleyball Sheets export")
        if "baseball" in active:
            season = resolve_season_year("baseball")
            require_success(export_baseball_to_sheets(season=int(season)), "Baseball Sheets export")
        if "softball" in active:
            season = resolve_season_year("softball")
            require_success(export_softball_to_sheets(season=int(season)), "Softball Sheets export")
        for sport in ("boys_basketball", "girls_basketball", "boys_soccer", "girls_soccer"):
            if sport in active:
                season = resolve_season_year(sport)
                require_success(export_winter_sport_to_sheets(sport, season), f"{sport} Sheets export")

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
