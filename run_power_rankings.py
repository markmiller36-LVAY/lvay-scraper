"""
LVAY - Multi-Sport Power Rankings Runner
========================================
Reads games from DB, applies optional Google Sheets overrides,
enriches with school_database divisions, runs PowerRatingEngine,
and writes results to:

  power_rankings      — one row per school
  game_power_points   — one row per counted game
"""

import os
import sqlite3
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from power_rating_engine import PowerRatingEngine, Team, GameResult
from school_database import get_school

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")

# You can still override these in Render if needed.
SPORT = os.environ.get("RANKINGS_SPORT", "baseball")
SEASON = os.environ.get("RANKINGS_SEASON", "2025")

GOOGLE_SHEET_NAME = os.environ.get("GOOGLE_SHEET_NAME", "LVAY Master Data")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")


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


def get_override_tab_name(sport: str, season: str) -> str:
    return f"{sport.capitalize()} Overrides ({season})"


def get_gspread_client():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        print("  No GOOGLE_SERVICE_ACCOUNT_JSON found; skipping overrides")
        return None

    import json
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


def normalize_bool(value) -> bool:
    return str(value or "").strip().lower() in ("true", "1", "yes", "y")


def normalize_text(value) -> str:
    return str(value or "").strip()


def load_sheet_overrides(sport: str, season: str) -> dict:
    """
    Expected sheet headers:
      sport
      season
      school
      game_date
      opponent
      override_win_loss
      override_score
      override_home_away
      notes
      active
    """
    client = get_gspread_client()
    if not client:
        return {}

    tab_name = get_override_tab_name(sport, season)

    try:
        sheet_id = os.environ.get("GOOGLE_SHEET_ID")
        sheet = client.open_by_key(sheet_id)
        ws = sheet.worksheet(tab_name)
    except Exception as e:
        print(f"  Override tab not found or unreadable: {tab_name} ({e})")
        return {}

    rows = ws.get_all_records()
    overrides = {}

    for row in rows:
        row_sport = normalize_text(row.get("sport"))
        row_season = normalize_text(row.get("season"))
        school = normalize_text(row.get("school"))
        game_date = normalize_text(row.get("game_date"))
        opponent = normalize_text(row.get("opponent"))
        active = normalize_bool(row.get("active"))

        if not active:
            continue
        if row_sport != sport or row_season != season:
            continue
        if not school or not game_date or not opponent:
            continue

        key = (row_sport, row_season, school, game_date, opponent)
        overrides[key] = {
            "override_win_loss": normalize_text(row.get("override_win_loss")),
            "override_score": normalize_text(row.get("override_score")),
            "override_home_away": normalize_text(row.get("override_home_away")),
            "notes": normalize_text(row.get("notes")),
        }

    print(f"  Loaded {len(overrides)} active overrides from '{tab_name}'")
    return overrides


def load_games(conn, season=SEASON, sport=SPORT):
    c = conn.cursor()
    c.execute("""
        SELECT school, opponent, win_loss, week, score, game_date,
               class_, district, district_class, out_of_state, home_away
        FROM games
        WHERE sport=? AND season=?
          AND win_loss IN ('W', 'L', 'Tie', 'T')
        ORDER BY school, game_date
    """, (sport, season))
    return c.fetchall()


def load_scores(conn, season=SEASON, sport=SPORT):
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
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school, week, opponent, division, class_, opp_wins, opp_losses
            FROM oos_opponents
            WHERE sport=? AND season=?
        """, (sport, season))
        oos = {}
        for r in c.fetchall():
            oos[(r["school"], int(r["week"]))] = {
                "opponent": r["opponent"],
                "division": r["division"],
                "class_": r["class_"] if r["class_"] else "",
                "opp_wins": r["opp_wins"],
                "opp_losses": r["opp_losses"],
            }
        return oos
    except Exception:
        return {}


def build_school_records(rows):
    records = {}
    for r in rows:
        school = r["school"]
        wl = r["win_loss"]
        if school not in records:
            records[school] = {"wins": 0, "losses": 0, "ties": 0}
        if wl == "W":
            records[school]["wins"] += 1
        elif wl == "L":
            records[school]["losses"] += 1
        elif wl in ("T", "Tie"):
            records[school]["ties"] += 1
    return records


def apply_override_to_row(row, sport: str, season: str, overrides: dict) -> dict:
    row_data = dict(row)

    key = (
        sport,
        season,
        normalize_text(row_data.get("school")),
        normalize_text(row_data.get("game_date")),
        normalize_text(row_data.get("opponent")),
    )

    override = overrides.get(key)
    if not override:
        return row_data

    if override.get("override_win_loss"):
        row_data["win_loss"] = override["override_win_loss"]
    if override.get("override_score"):
        row_data["score"] = override["override_score"]
    if override.get("override_home_away"):
        row_data["home_away"] = override["override_home_away"]

    return row_data


def run_power_rankings(season=SEASON, sport=SPORT):
    print(f"\n{'='*54}")
    print("LVAY Power Rankings Calculator")
    print(f"Sport: {sport.upper()}  Season: {season}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*54}")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_tables(conn)

    raw_rows = load_games(conn, season, sport)
    if not raw_rows:
        print(f"  No games found for {sport} season {season}")
        conn.close()
        return []

    overrides = load_sheet_overrides(sport, season)
    rows = [apply_override_to_row(r, sport, season, overrides) for r in raw_rows]

    oos_lookup = load_oos_opponents(conn, season, sport)
    print(f"  OOS lookup: {len(oos_lookup)} games loaded")
    print(f"  Loaded {len(rows)} games after applying overrides")

    scores_lookup = load_scores(conn, season, sport)
    school_records = build_school_records(rows)
    print(f"  {len(school_records)} school profiles loaded")

    engine = PowerRatingEngine()
    schools_seen = set()
    unmatched = []
    oos_missing = []

    for r in rows:
        school = r["school"]
        if school in schools_seen:
            continue
        schools_seen.add(school)

        db_info = get_school(school)
        division = db_info["division"] if db_info else "Unknown"
        class_ = db_info["class"] if db_info else (r.get("class_") or "")

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
        print(f"  ⚠️ {len(unmatched)} unmatched schools")

    game_meta = {}

    for r in rows:
        school = r["school"]
        opponent = r["opponent"]
        wl = r["win_loss"]
        week_str = r["week"] or ""
        oos = str(r.get("out_of_state") or "").strip().upper() in ("Y", "YES", "1", "TRUE")

        if wl == "Tie":
            result = "T"
        elif wl in ("W", "L", "T"):
            result = wl
        else:
            continue

        try:
            week_num = int(str(week_str).replace("Week ", "").strip())
        except Exception:
            week_num = 0

        oos_key = (school, week_num)
        if oos_key in oos_lookup:
            oos_data = oos_lookup[oos_key]
            opp_wins = oos_data["opp_wins"]
            opp_losses = oos_data["opp_losses"]
            opp_division = oos_data["division"]
            opp_class = oos_data.get("class_", "")
            oos = True
        elif oos:
            opp_wins = 0
            opp_losses = 0
            opp_division = "Unknown"
            opp_class = ""
            oos_missing.append(f"{school} Wk{week_num} vs {opponent}")
        else:
            opp_record = school_records.get(opponent, {"wins": 0, "losses": 0, "ties": 0})
            opp_wins = opp_record["wins"]
            opp_losses = opp_record["losses"]
            opp_info = get_school(opponent)
            opp_division = opp_info["division"] if opp_info else "Unknown"
            opp_class = opp_info["class"] if opp_info else ""

        score = r.get("score") or scores_lookup.get((school, str(week_num)), "")

        game_meta[(school, week_num)] = {
            "opponent": opponent,
            "result": result,
            "score": score,
            "opp_wins": opp_wins,
            "opp_losses": opp_losses,
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

    if oos_missing:
        print(f"  ⚠️ {len(oos_missing)} OOS games flagged but missing from oos_opponents:")
        for m in oos_missing:
            print(f"      {m}")

    print("  Calculating power ratings...")
    ratings = engine.rate_all()
    print(f"  Power ratings calculated for {len(ratings)} schools")

    now_str = datetime.now().isoformat()
    c = conn.cursor()

    c.execute("DELETE FROM power_rankings WHERE sport=? AND season=?", (sport, season))
    c.execute("DELETE FROM game_power_points WHERE sport=? AND season=?", (sport, season))

    for r in ratings:
        db_info = get_school(r.name)
        division = db_info["division"] if db_info else r.division
        track = db_info["track"] if db_info else "unknown"
        class_ = db_info["class"] if db_info else ""
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

        for g in r.breakdown:
            week_num = g["week"]
            meta = game_meta.get((r.name, week_num), {})
            school_info = get_school(r.name)
            opp_info = get_school(meta.get("opponent", g["opponent"]))

            is_district = 0
            if school_info and opp_info:
                if (
                    school_info.get("class") == opp_info.get("class")
                    and school_info.get("district") == opp_info.get("district")
                ):
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
    print("DONE!")
    print(f"  Schools ranked:    {len(ratings)}")
    print(f"  Unmatched schools: {len(unmatched)}")
    if oos_missing:
        print(f"  OOS missing:       {len(oos_missing)} — add to oos_opponents table")
    print("  Top 5:")
    for r in ratings[:5]:
        print(f"    #{r.rank} {r.name} | PR={r.power_rating} | {r.record} | {r.division}")
    print(f"{'='*54}\n")

    return ratings


if __name__ == "__main__":
    run_power_rankings()
