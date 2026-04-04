"""
LVAY - Football Power Rankings Runner
=======================================
Step 3 of the pipeline:

  1. scraper.py        → raw games in DB
  2. (this script)     → enrich + calculate + write power_rankings table
  3. sheets_exporter.py → read power_rankings → write to Google Sheet

What this does:
  - Reads all football games from games table
  - Looks up each school in school_database.py → gets division + track
  - For each game, looks up the opponent's record to get opponent_wins/losses
  - Feeds everything into PowerRatingEngine
  - Writes results to power_rankings table
"""

import sqlite3
import os
from datetime import datetime
from power_rating_engine import PowerRatingEngine, Team, GameResult
from school_database import get_school, get_division

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")
SEASON  = "2025"
SPORT   = "football"


def init_power_rankings_table(conn):
    """Create power_rankings table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS power_rankings (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sport        TEXT,
            season       TEXT,
            school       TEXT,
            division     TEXT,
            track        TEXT,
            class_       TEXT,
            district     INTEGER,
            rank         INTEGER,
            power_rating REAL,
            wins         INTEGER,
            losses       INTEGER,
            ties         INTEGER,
            games_played INTEGER,
            calculated_at TEXT,
            UNIQUE(sport, season, school)
        )
    """)
    conn.commit()


def load_games(conn, season=SEASON, sport=SPORT):
    """Load all games for a sport/season from DB."""
    c = conn.cursor()
    c.execute("""
        SELECT school, opponent, win_loss, week, score,
               class_, district, district_class, out_of_state
        FROM games
        WHERE sport=? AND season=?
          AND win_loss IN ('W', 'L', 'Tie', 'T')
        ORDER BY school, CAST(REPLACE(week,'Week ','') AS INTEGER)
    """, (sport, season))
    return c.fetchall()


def build_school_records(rows):
    """
    Build a lookup of each school's wins/losses from the raw game rows.
    Used to calculate opponent quality.
    Returns: { school_name: {"wins": N, "losses": N, "gp": N} }
    """
    records = {}
    for r in rows:
        school = r["school"]
        wl     = r["win_loss"]
        if school not in records:
            records[school] = {"wins": 0, "losses": 0, "ties": 0}
        if wl == "W":
            records[school]["wins"] += 1
        elif wl == "L":
            records[school]["losses"] += 1
        elif wl in ("T", "Tie"):
            records[school]["ties"] += 1
    return records


def run_power_rankings(season=SEASON, sport=SPORT):
    print(f"\n{'='*54}")
    print(f"LVAY Power Rankings Calculator")
    print(f"Sport: {sport.upper()}  Season: {season}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*54}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_power_rankings_table(conn)

    # Load all games
    rows = load_games(conn, season, sport)
    if not rows:
        print(f"  No games found for {sport} season {season}")
        conn.close()
        return

    print(f"  Loaded {len(rows)} games")

    # Build school win/loss records for opponent quality
    school_records = build_school_records(rows)
    print(f"  {len(school_records)} school profiles loaded")

    # Initialize engine
    engine = PowerRatingEngine()

    # Add all teams with their division from school_database
    schools_seen = set()
    unmatched    = []

    for r in rows:
        school = r["school"]
        if school in schools_seen:
            continue
        schools_seen.add(school)

        db_info  = get_school(school)
        division = db_info["division"] if db_info else "Unknown"
        class_   = db_info["class"]    if db_info else (r["class_"] or "")
        track    = db_info["track"]    if db_info else "unknown"

        if not db_info or division == "Unknown":
            unmatched.append(school)

        engine.add_team(Team(
            name=school,
            division=division,
            classification=class_ or "",
            sport=sport,
        ))

    print(f"  {len(schools_seen)} schools registered")
    if unmatched:
        print(f"  ⚠️  {len(unmatched)} schools not found in school_database:")
        for s in unmatched:
            print(f"      - {s}")

    # Score validation
    validation_issues = 0

    # Add all games to engine
    for r in rows:
        school   = r["school"]
        opponent = r["opponent"]
        wl       = r["win_loss"]
        week_str = r["week"] or ""
        oos      = str(r["out_of_state"] or "").strip().upper() in ("Y", "YES", "1", "TRUE")

        # Normalize result
        if wl == "Tie":
            result = "T"
        elif wl in ("W", "L", "T"):
            result = wl
        else:
            continue

        # Week number
        try:
            week_num = int(week_str.replace("Week ", "").strip())
        except Exception:
            week_num = 0

        # Opponent record (for opponent quality)
        opp_record = school_records.get(opponent, {"wins": 0, "losses": 0})
        opp_wins   = opp_record["wins"]
        opp_losses = opp_record["losses"]

        # Opponent division from school_database
        opp_info     = get_school(opponent)
        opp_division = opp_info["division"] if opp_info else "Unknown"
        opp_class    = opp_info["class"]    if opp_info else ""

        engine.add_game(GameResult(
            team=school,
            opponent=opponent,
            result=result,
            sport=sport,
            opponent_wins=opp_wins,
            opponent_losses=opp_losses,
            opponent_division=opp_division,
            opponent_class=opp_class or "",
            opponent_out_of_state=oos,
            week=week_num,
        ))

    print(f"  Score validation: {validation_issues} games flagged")

    # Run engine
    print(f"  Calculating power ratings...")
    ratings = engine.rate_all()
    print(f"  Power ratings calculated for {len(ratings)} schools")

    # Write to power_rankings table
    now_str = datetime.now().isoformat()
    c = conn.cursor()

    # Clear old ratings for this sport/season
    c.execute("DELETE FROM power_rankings WHERE sport=? AND season=?", (sport, season))

    inserted = 0
    for r in ratings:
        db_info  = get_school(r.name)
        division = db_info["division"] if db_info else r.division
        track    = db_info["track"]    if db_info else "unknown"
        class_   = db_info["class"]    if db_info else ""
        district = db_info["district"] if db_info else None

        c.execute("""
            INSERT OR REPLACE INTO power_rankings
            (sport, season, school, division, track, class_, district,
             rank, power_rating, wins, losses, ties, games_played, calculated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sport, season, r.name, division, track, class_, district,
            r.rank, r.power_rating, r.wins, r.losses, r.ties, r.games_played,
            now_str
        ))
        inserted += 1

    conn.commit()
    conn.close()

    print(f"\n{'='*54}")
    print(f"DONE! Power rankings calculated.")
    print(f"  Schools ranked: {inserted}")
    print(f"  Unmatched schools: {len(unmatched)}")
    print(f"  Top 5:")
    for r in ratings[:5]:
        print(f"    #{r.rank} {r.name} | PR={r.power_rating} | {r.record} | {r.division}")
    print(f"{'='*54}\n")
    return ratings


if __name__ == "__main__":
    run_power_rankings()
