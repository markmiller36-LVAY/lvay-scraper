"""
LVAY - Multi-Sport Power Rankings Runner
========================================
Reads games from DB, applies optional Google Sheets overrides,
enriches with school_database divisions, runs PowerRatingEngine,
and writes results to:

  power_rankings      — one row per school
  game_power_points   — one row per counted game
"""

import json
import os
import sqlite3
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from power_rating_engine import PowerRatingEngine, Team, GameResult
from school_database import get_school

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")

SPORT = os.environ.get("RANKINGS_SPORT", "football")
SEASON = os.environ.get("RANKINGS_SEASON", "2026")

GOOGLE_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")


def strip_district_prefix(class_str: str) -> str:
    """
    Convert LHSAA baseball/softball class format to plain class.
    e.g. '1-5A' -> '5A', '2-4A' -> '4A', '3-2A' -> '2A'
    Plain values like '5A', 'B', 'C' pass through unchanged.
    """
    if not class_str:
        return ""
    s = str(class_str).strip()
    if "-" in s:
        parts = s.split("-", 1)
        return parts[1].strip()
    return s


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

    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


def normalize_bool(value) -> bool:
    return str(value or "").strip().lower() in ("true", "1", "yes", "y")


def normalize_text(value) -> str:
    return str(value or "").strip()


def normalize_key_text(value) -> str:
    return str(value or "").strip().lower()


def load_sheet_overrides(sport: str, season: str) -> dict:
    """
    Expected sheet headers:
      sport, season, school, game_date, opponent,
      override_win_loss, override_score, override_home_away, notes, active
    """
    client = get_gspread_client()
    if not client:
        return {}

    tab_name = get_override_tab_name(sport, season)

    try:
        if not GOOGLE_SHEET_ID:
            print("  No GOOGLE_SHEET_ID found; skipping overrides")
            return {}
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        ws = sheet.worksheet(tab_name)
    except Exception as e:
        print(f"  Override tab not found or unreadable: {tab_name} ({e})")
        return {}

    rows = ws.get_all_records()
    overrides = {}

    for row in rows:
        row_sport = normalize_text(row.get("sport"))
        row_season = normalize_text(row.get("season"))
        school = normalize_key_text(row.get("school"))
        game_date = normalize_text(row.get("game_date"))
        opponent = normalize_key_text(row.get("opponent"))
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
               class_, district, district_class, out_of_state, home_away,
               opponent_class
        FROM games
        WHERE sport=? AND season=?
          AND win_loss IN ('W', 'L', 'Tie', 'T')
        ORDER BY school, 
            SUBSTR(game_date, INSTR(game_date,'/')+INSTR(SUBSTR(game_date,INSTR(game_date,'/')+1),'/')+1, 4),
            SUBSTR(game_date, 1, INSTR(game_date,'/')-1)*1,
            SUBSTR(game_date, INSTR(game_date,'/')+1, INSTR(SUBSTR(game_date,INSTR(game_date,'/')+1),'/')-1)*1
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
    """
    For football: keyed by (school, week) — legacy behavior
    For baseball/softball: keyed by (school, opponent) — opponent name match
    """
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school, opponent, opp_wins, opp_losses
            FROM oos_opponents
            WHERE sport=? AND season=?
        """, (sport, season))
        oos = {}
        for r in c.fetchall():
            # Key by (school, opponent) for baseball/softball
            # Also store a normalized version for fuzzy matching
            key = (r["school"], r["opponent"])
            oos[key] = {
                "opponent": r["opponent"],
                "division": "Unknown",
                "class_": "",
                "opp_wins": r["opp_wins"],
                "opp_losses": r["opp_losses"],
            }
        return oos
    except Exception as e:
        print(f"  OOS load error: {e}")
        return {}


def find_oos_record(oos_lookup, school, opponent):
    """
    Look up OOS record by school + opponent name.
    Tries exact match first, then partial match on the base school name
    (stripping state suffixes like '- TX - UIL').
    """
    # Exact match
    if (school, opponent) in oos_lookup:
        return oos_lookup[(school, opponent)]

    # Partial match — strip state/association suffixes from DB opponent name
    # DB has e.g. "Pine Tree - TX - UIL", sheet has "Pine Tree TX"
    opponent_base = opponent.split(" - ")[0].strip().lower()
    for (s, o), data in oos_lookup.items():
        if s != school:
            continue
        o_base = o.split(" - ")[0].strip().lower()
        if opponent_base == o_base or o_base in opponent_base or opponent_base in o_base:
            return data

    return None


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
        normalize_key_text(row_data.get("school")),
        normalize_text(row_data.get("game_date")),
        normalize_key_text(row_data.get("opponent")),
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


def print_football_division_dump(ratings):
    division_order = [
        "Non-Select Division I",
        "Non-Select Division II",
        "Non-Select Division III",
        "Non-Select Division IV",
        "Select Division I",
        "Select Division II",
        "Select Division III",
        "Select Division IV",
    ]

    print("\n" + "=" * 54)
    print("FULL DIVISION DUMP (FOR AUDIT)")
    print("=" * 54)

    for division in division_order:
        print(f"\n{division.upper()}")
        print("-" * 54)

        div_list = [r for r in ratings if getattr(r, "division", "") == division]
        div_list = sorted(
            div_list,
            key=lambda x: getattr(x, "power_rating", 0),
            reverse=True
        )

        if not div_list:
            print("  (no teams)")
            continue

        for i, r in enumerate(div_list, 1):
            print(f"{i:2}. {r.name:<30} PR={round(r.power_rating, 2):>6} | {r.record}")


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

        if not db_info:
            unmatched.append(school)

        engine.add_team(Team(
            name=school,
            division=division,
            classification=class_ or "",
            sport=sport,
        ))

    unmatched_unique = sorted(set(unmatched))

    print(f"  {len(schools_seen)} schools registered")
    if unmatched_unique:
        print(f"  ⚠️  {len(unmatched_unique)} unmatched schools")
        print("\nUNMATCHED SCHOOLS:")
        for name in unmatched_unique:
            print(f" - {name}")

    game_meta = {}
    date_counters = {}

    for r in rows:
        school = r["school"]
        opponent = r["opponent"]
        wl = r["win_loss"]
        week_str = r["week"] or ""
        game_date = r["game_date"] or ""

        oos_flag = str(r.get("out_of_state") or "").strip().upper() in ("Y", "YES", "1", "TRUE")
        opp_in_db = get_school(opponent) is not None
        oos = oos_flag or (not opp_in_db)

        if wl == "Tie":
            result = "T"
        elif wl in ("W", "L", "T"):
            result = wl
        else:
            continue

        # Week number
        if sport.lower() in ("baseball", "softball"):
            try:
                date_key = int(game_date.replace("-", "").replace("/", "").strip()[:8])
                date_count = date_counters.get((school, date_key), 0)
                date_counters[(school, date_key)] = date_count + 1
                week_num = date_key * 10 + date_count
            except Exception:
                week_num = 0
        else:
            try:
                week_num = int(str(week_str).replace("Week ", "").strip())
            except Exception:
                week_num = 0

        game_key = (school, week_num) if week_num else (school, game_date)

        # OOS record lookup — for baseball/softball use opponent name match
        oos_data = None
        if oos and sport.lower() in ("baseball", "softball"):
            oos_data = find_oos_record(oos_lookup, school, opponent)

        if oos_data:
            opp_wins = oos_data["opp_wins"]
            opp_losses = oos_data["opp_losses"]
            opp_division = oos_data.get("division", "Unknown")
            opp_class = strip_district_prefix(oos_data.get("class_", ""))
            oos = True
        elif oos:
            opp_wins = 0
            opp_losses = 0
            opp_division = "Unknown"
            raw_opp_class = r.get("opponent_class") or ""
            opp_class = strip_district_prefix(raw_opp_class)
            oos_missing.append(f"{school} vs {opponent} ({game_date})")
        else:
            opp_record = school_records.get(opponent, {"wins": 0, "losses": 0, "ties": 0})
            opp_wins = opp_record["wins"]
            opp_losses = opp_record["losses"]
            opp_info = get_school(opponent)
            opp_division = opp_info["division"] if opp_info else "Unknown"
            raw_opp_class = r.get("opponent_class") or ""
            opp_class = strip_district_prefix(raw_opp_class) or (opp_info["class"] if opp_info else "")

        score = r.get("score") or scores_lookup.get((school, str(week_num)), "")

        game_meta[game_key] = {
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
        print(f"  ⚠️  {len(oos_missing)} OOS games detected (no record data):")
        for m in oos_missing[:10]:
            print(f"      {m}")
        if len(oos_missing) > 10:
            print(f"      ... and {len(oos_missing) - 10} more")

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
            game_key = (r.name, week_num) if week_num else (r.name, "")
            meta = game_meta.get(game_key, {})
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
    print(f"DONE!")
    print(f"  Schools ranked:    {len(ratings)}")
    print(f"  Unmatched schools: {len(unmatched_unique)}")
    if oos_missing:
        print(f"  OOS detected:      {len(oos_missing)} games without record data")
    print(f"  Top 5:")
    for r in ratings[:5]:
        print(f"    #{r.rank} {r.name} | PR={r.power_rating} | {r.record} | {r.division}")

    if sport.lower() == "football":
        print_football_division_dump(ratings)

    print(f"{'='*54}\n")

    return ratings


if __name__ == "__main__":
    pass
