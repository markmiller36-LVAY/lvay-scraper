"""
LVAY - Multi-Sport Power Rankings Runner
========================================
- Matches scraper logic
- Runs ONLY active sports
- Uses dynamic season handling
- Writes results to DB
"""

import sqlite3
import os
from datetime import datetime

from power_rating_engine import PowerRatingEngine, Team, GameResult
from school_database import get_school

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")

# SAME CONFIG AS SCRAPER
SPORTS = {
    "football": {
        "enabled": True,
        "season_mode": "calendar_year",
        "active_start": "08-01",
        "active_end": "12-31",
    },
    "baseball": {
        "enabled": True,
        "season_mode": "school_year",
        "active_start": "01-15",
        "active_end": "05-31",
    },
    "softball": {
        "enabled": True,
        "season_mode": "school_year",
        "active_start": "01-15",
        "active_end": "05-31",
    },
}


def get_season(sport):
    now = datetime.now()
    config = SPORTS[sport]

    # ENV override
    env = os.environ.get(f"{sport.upper()}_SEASON_YEAR")
    if env:
        return env

    if config["season_mode"] == "calendar_year":
        return str(now.year)

    # school year
    return str(now.year if now.month >= 8 else now.year - 1)


def in_season(sport):
    now = datetime.now()
    md = now.strftime("%m-%d")

    start = SPORTS[sport]["active_start"]
    end = SPORTS[sport]["active_end"]

    if start <= end:
        return start <= md <= end
    return md >= start or md <= end


def load_games(conn, sport, season):
    c = conn.cursor()
    c.execute("""
        SELECT *
        FROM games
        WHERE sport=? AND season=?
    """, (sport, season))
    return c.fetchall()


def run_sport(sport):
    if not SPORTS[sport]["enabled"]:
        print(f"--- SKIP {sport} (disabled)")
        return

    if not in_season(sport):
        print(f"--- SKIP {sport} (out of season)")
        return

    season = get_season(sport)

    print(f"\n{'='*50}")
    print(f"{sport.upper()} | Season {season}")
    print(f"{'='*50}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = load_games(conn, sport, season)

    if not rows:
        print("No data found")
        return

    engine = PowerRatingEngine()

    # Build records
    school_records = {}
    for r in rows:
        school = r["school"]
        wl = r["win_loss"]

        if school not in school_records:
            school_records[school] = {"wins": 0, "losses": 0}

        if wl == "W":
            school_records[school]["wins"] += 1
        elif wl == "L":
            school_records[school]["losses"] += 1

    # Add teams
    for school in school_records:
        info = get_school(school)
        engine.add_team(Team(
            name=school,
            division=info["division"] if info else "Unknown",
            classification=info["class"] if info else "",
            sport=sport,
        ))

    # Add games
    for r in rows:
        opponent = r["opponent"]

        opp_record = school_records.get(opponent, {"wins": 0, "losses": 0})
        opp_info = get_school(opponent)

        engine.add_game(GameResult(
            team=r["school"],
            opponent=opponent,
            result="T" if r["win_loss"] == "Tie" else r["win_loss"],
            sport=sport,
            opponent_wins=opp_record["wins"],
            opponent_losses=opp_record["losses"],
            opponent_division=opp_info["division"] if opp_info else "",
            opponent_class=opp_info["class"] if opp_info else "",
        ))

    ratings = engine.rate_all()

    print(f"\nTop 10:")
    for r in ratings[:10]:
        print(f"{r.rank}. {r.name} | {r.power_rating} | {r.record}")

    conn.close()


def run_all():
    print(f"\n{'='*60}")
    print(f"LVAY Power Rankings — {datetime.now()}")
    print(f"{'='*60}")

    for sport in SPORTS:
        run_sport(sport)


if __name__ == "__main__":
    run_all()
