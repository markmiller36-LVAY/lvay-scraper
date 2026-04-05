"""
LVAY - Football Power Rankings Runner
=======================================
Reads games from DB, enriches with school_database divisions,
runs PowerRatingEngine, writes results to:

  power_rankings      — one row per school (rank, power_rating, W/L)
  game_power_points   — one row per game (base, div_bonus, opp_quality, total)
"""

import sqlite3
import os
from datetime import datetime
from power_rating_engine import PowerRatingEngine, Team, GameResult
from school_database import get_school

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")
SEASON  = "2025"
SPORT   = "football"


def init_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS power_rankings (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            sport         TEXT,
            season        TEXT,
            school        TEXT,
            division      TEXT,
            track         TEXT,
            class_        TEXT,
            district      INTEGER,
            rank          INTEGER,
            power_rating  REAL,
            wins          INTEGER,
            losses        INTEGER,
            ties          INTEGER,
            games_played  INTEGER,
            calculated_at TEXT,
            UNIQUE(sport, season, school)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS game_power_points (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            sport          TEXT,
            season         TEXT,
            school         TEXT,
            week           INTEGER,
            opponent       TEXT,
            result         TEXT,
            score          TEXT,
            opp_wins       INTEGER,
            opp_losses     INTEGER,
            opp_division   TEXT,
            base_pts       REAL,
            div_bonus      REAL,
            opp_quality    REAL,
            total_pts      REAL,
            is_district    INTEGER DEFAULT 0,
            calculated_at  TEXT,
            UNIQUE(sport, season, school, week)
        )
    """)
    conn.commit()


def load_games(conn, season=SEASON, sport=SPORT):
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


def load_scores(conn, season=SEASON, sport=SPORT):
    """Load scores for display purposes."""
    c = conn.cursor()
    c.execute("""
        SELECT school, week, score
        FROM games
        WHERE sport=? AND season=?
    """, (sport, season))
    scores = {}
    for r in c.fetchall():
        key = (r["school"], str(r["week"] or "").replace("Week ", "").strip())
        scores[key] = r["score"] or ""
    return scores


def load_oos_opponents(conn, season=SEASON, sport=SPORT):
    """Load OOS opponent records — keyed by (school, week)."""
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school, week, opponent, division, class_, opp_wins, opp_losses
            FROM oos_opponents
            WHERE sport=? AND season=?
        """, (sport, season))
        oos = {}
        for r in c.fetchall():
            oos[(r["school"], r["week"])] = {
                "opponent":   r["opponent"],
                "division":   r["division"],
                "class_":     r["class_"] if r["class_"] else "",
                "opp_wins":   r["opp_wins"],
                "opp_losses": r["opp_losses"],
            }
        return oos
    except Exception:
        return {}


def build_school_records(rows):
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
    init_tables(conn)

    rows = load_games(conn, season, sport)
    oos_lookup = load_oos_opponents(conn, season, sport)
    print(f'  OOS lookup: {len(oos_lookup)} games loaded')
    if not rows:
        print(f"  No games found for {sport} season {season}")
        conn.close()
        return

    print(f"  Loaded {len(rows)} games")

    scores_lookup  = load_scores(conn, season, sport)
    school_records = build_school_records(rows)
    print(f"  {len(school_records)} school profiles loaded")

    engine      = PowerRatingEngine()
    schools_seen = set()
    unmatched    = []

    # Register all teams
    for r in rows:
        school = r["school"]
        if school in schools_seen:
            continue
        schools_seen.add(school)

        db_info  = get_school(school)
        division = db_info["division"] if db_info else "Unknown"
        class_   = db_info["class"]    if db_info else (r["class_"] or "")

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
        print(f"  ⚠️  {len(unmatched)} unmatched schools")

    # Add all games
    game_meta = {}  # (school, week_num) -> {opp_wins, opp_losses, opp_division, score}

    for r in rows:
        school   = r["school"]
        opponent = r["opponent"]
        wl       = r["win_loss"]
        week_str = r["week"] or ""
        oos      = str(r["out_of_state"] or "").strip().upper() in ("Y", "YES", "1", "TRUE")

        if wl == "Tie":
            result = "T"
        elif wl in ("W", "L", "T"):
            result = wl
        else:
            continue

        try:
            week_num = int(week_str.replace("Week ", "").strip())
        except Exception:
            week_num = 0

        # Check OOS lookup first
        oos_key = (school, week_num)
        if oos_key in oos_lookup:
            oos_data     = oos_lookup[oos_key]
            opp_wins     = oos_data["opp_wins"]
            opp_losses   = oos_data["opp_losses"]
            opp_division = oos_data["division"]
            opp_class    = oos_data.get("class_", "")
            oos          = True
        else:
            opp_record   = school_records.get(opponent, {"wins": 0, "losses": 0})
            opp_wins     = opp_record["wins"]
            opp_losses   = opp_record["losses"]
            opp_info     = get_school(opponent)
            opp_division = opp_info["division"] if opp_info else "Unknown"
            opp_class    = opp_info["class"]    if opp_info else ""
        score        = scores_lookup.get((school, str(week_num)), "")

        game_meta[(school, week_num)] = {
            "opponent":     opponent,
            "result":       result,
            "score":        score,
            "opp_wins":     opp_wins,
            "opp_losses":   opp_losses,
            "opp_division": opp_division,
        }

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

    # Calculate ratings
    print(f"  Calculating power ratings...")
    ratings = engine.rate_all()
    print(f"  Power ratings calculated for {len(ratings)} schools")

    now_str = datetime.now().isoformat()
    c       = conn.cursor()

    # Clear old data
    c.execute("DELETE FROM power_rankings WHERE sport=? AND season=?",    (sport, season))
    c.execute("DELETE FROM game_power_points WHERE sport=? AND season=?", (sport, season))

    # Write power_rankings
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
            r.rank, r.power_rating, r.wins, r.losses, r.ties,
            r.games_played, now_str
        ))

        # Write per-game breakdown from engine's breakdown list
        for g in r.breakdown:
            week_num = g["week"]
            meta     = game_meta.get((r.name, week_num), {})
            # Check if district game using school_database
            school_info = get_school(r.name)
            opp_info    = get_school(meta.get("opponent", g["opponent"]))
            is_district = 0
            if school_info and opp_info:
                if (school_info.get("class") == opp_info.get("class") and
                    school_info.get("district") == opp_info.get("district")):
                    is_district = 1

            c.execute("""
                INSERT OR REPLACE INTO game_power_points
                (sport, season, school, week, opponent, result, score,
                 opp_wins, opp_losses, opp_division,
                 base_pts, div_bonus, opp_quality, total_pts, is_district, calculated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sport, season, r.name, week_num,
                meta.get("opponent", g["opponent"]),
                g["result"],
                meta.get("score", ""),
                meta.get("opp_wins", 0),
                meta.get("opp_losses", 0),
                meta.get("opp_division", ""),
                g["base"],
                g["div"],
                g["oppq"],
                g["total"],
                is_district,
                now_str
            ))

    conn.commit()
    conn.close()

    print(f"\n{'='*54}")
    print(f"DONE!")
    print(f"  Schools ranked:    {len(ratings)}")
    print(f"  Unmatched schools: {len(unmatched)}")
    print(f"  Top 5:")
    for r in ratings[:5]:
        print(f"    #{r.rank} {r.name} | PR={r.power_rating} | {r.record} | {r.division}")
    print(f"{'='*54}\n")
    return ratings


if __name__ == "__main__":
    run_power_rankings()
if __name__ == "__main__":
    run()
