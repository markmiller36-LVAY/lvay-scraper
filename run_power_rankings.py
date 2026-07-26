"""
LVAY - Multi-Sport Power Rankings Runner
========================================
Reads games from DB, applies optional Google Sheets overrides,
enriches with school_database divisions, runs PowerRatingEngine,
and writes results to:

  power_rankings      — one row per school
  game_power_points   — one row per counted game

Forfeit handling:
  W(f) is normalized to W and L(f) is normalized to L before scoring.
  Per LHSAA practice (confirmed via GeauxPreps audit), a forfeit counts
  exactly like a regular win or loss for power-point purposes.
"""

import json
import os
import re
import sqlite3
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

from power_rating_engine import PowerRatingEngine, Team, GameResult
from official_record_overrides import (
    find_game_exclusion,
    find_record_override,
    get_game_exclusions,
    get_record_overrides,
)
from school_database import get_school

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")

SPORT = os.environ.get("RANKINGS_SPORT", "football")
SEASON = os.environ.get("RANKINGS_SEASON", "2026")

GOOGLE_SHEET_ID = os.environ.get(
    "GOOGLE_SHEET_ID",
    "1u_cJBAWTQJIAO36HZTYvPa7QfE0JoOEqx12c1U4t4mk",
)
GOOGLE_SERVICE_ACCOUNT_JSON = (
    os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    or os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
)


def normalize_result(wl):
    """Collapse forfeits into regular W/L and ties into T.
    Returns one of: 'W', 'L', 'T', or '' for anything unrecognized."""
    if wl is None:
        return ""
    s = str(wl).strip()
    if s in ("W", "W(f)"):
        return "W"
    if s in ("L", "L(f)"):
        return "L"
    if s in ("T", "Tie"):
        return "T"
    return ""


def football_week_number(week):
    """Return an integer LHSAA football week, or None when it is not a week."""
    match = re.search(r"\d+", str(week or ""))
    return int(match.group()) if match else None


FOOTBALL_EXCLUDED_GAMES = {
    # Crescent City closed before the Week 9 West St. John game. LHSAA's
    # final rating uses a 0-7 record and excludes this unplayed 0-1 entry.
    ("2025", "Crescent City", 9, "West St. John"),
}


def filter_regular_season_football_rows(rows, season="2025"):
    """Keep only unique regular-season schedule games, max 10 per school."""
    deduplicated = {}
    for row in rows:
        week = football_week_number(row.get("week"))
        exclusion_key = (
            str(season),
            row.get("school", ""),
            week,
            row.get("opponent", ""),
        )
        if exclusion_key in FOOTBALL_EXCLUDED_GAMES:
            continue
        if week is not None and not 1 <= week <= 10:
            continue
        key = (
            row.get("school", ""),
            str(row.get("game_date") or "").split()[0],
            row.get("opponent", ""),
            normalize_result(row.get("win_loss")),
            row.get("score", ""),
        )
        existing = deduplicated.get(key)
        if existing is None or (
            football_week_number(existing.get("week")) is None
            and week is not None
        ):
            deduplicated[key] = row

    by_school = {}
    for row in deduplicated.values():
        by_school.setdefault(row.get("school", ""), []).append(row)

    kept = []
    for school_rows in by_school.values():
        def sort_key(row):
            try:
                return (
                    parse_game_date(row.get("game_date") or ""),
                    football_week_number(row.get("week")) or 0,
                )
            except Exception:
                return (
                    datetime.max,
                    football_week_number(row.get("week")) or 0,
                )

        kept.extend(sorted(school_rows, key=sort_key)[:10])
    return kept


def strip_district_prefix(class_str: str) -> str:
    if not class_str:
        return ""
    s = str(class_str).strip()
    if "-" in s:
        parts = s.split("-", 1)
        return parts[1].strip()
    return s


def parse_game_date(game_date: str) -> datetime:
    """Parse LHSAA game dates robustly. Handles both:
      '2/28/2026 6:00:00 PMSat'  (space before day name suffix)
      '2/28/2026Sat'             (no space — tournament format)
    """
    if not game_date:
        raise ValueError("Empty game_date")
    date_part = re.split(r'\s', game_date)[0]        # grab up to first whitespace
    date_part = re.sub(r'[A-Za-z]+$', '', date_part)  # strip trailing day name (Mon, Tue, etc.)
    return datetime.strptime(date_part, "%m/%d/%Y")


def init_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS power_rankings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            sport           TEXT,
            season          TEXT,
            school          TEXT,
            division        TEXT,
            track           TEXT,
            class_          TEXT,
            district        INTEGER,
            rank            INTEGER,
            power_rating    REAL,
            wins            INTEGER,
            losses          INTEGER,
            ties            INTEGER,
            games_played    INTEGER,
            strength_factor REAL,
            calculated_at   TEXT,
            UNIQUE(sport, season, school)
        )
    """)
    for col in ["strength_factor REAL", "ties INTEGER", "games_played INTEGER", "rank INTEGER", "track TEXT"]:
        try:
            conn.execute(f"ALTER TABLE power_rankings ADD COLUMN {col}")
        except Exception:
            pass

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
            opp_ties       INTEGER DEFAULT 0,
            opp_division   TEXT,
            base_pts       REAL,
            div_bonus      REAL,
            opp_quality    REAL,
            total_pts      REAL,
            is_district    INTEGER DEFAULT 0,
            game_date      TEXT,
            home_away      TEXT,
            calculated_at  TEXT,
            UNIQUE(sport, season, school, week)
        )
    """)
    for col in ["game_date TEXT", "home_away TEXT", "opp_ties INTEGER DEFAULT 0"]:
        try:
            conn.execute(f"ALTER TABLE game_power_points ADD COLUMN {col}")
        except Exception:
            pass

    conn.commit()


def get_override_tab_name(sport: str, season: str) -> str:
    return f"{sport.replace('_', ' ').title()} Overrides ({season})"


def get_gspread_client():
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        print("  No GOOGLE_SERVICE_ACCOUNT_JSON found; skipping overrides")
        return None
    creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return gspread.authorize(creds)


def normalize_bool(value) -> bool:
    return str(value or "").strip().lower() in ("true", "1", "yes", "y")


def normalize_text(value) -> str:
    return str(value or "").strip()


def normalize_key_text(value) -> str:
    return str(value or "").strip().lower()


def load_sheet_overrides(sport: str, season: str) -> dict:
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
          AND TRIM(COALESCE(school, '')) <> ''
          AND TRIM(COALESCE(opponent, '')) <> ''
          AND win_loss IN ('W', 'L', 'Tie', 'T', 'W(f)', 'L(f)')
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
        columns = {
            row["name"] for row in c.execute("PRAGMA table_info(oos_opponents)")
        }
        optional = [
            name for name in ("opp_ties", "division", "class_")
            if name in columns
        ]
        select_columns = ", ".join(
            ["school", "opponent", "opp_wins", "opp_losses", *optional]
        )
        c.execute(
            f"SELECT {select_columns} FROM oos_opponents "
            "WHERE sport=? AND season=?",
            (sport, season),
        )

        oos = {}
        for r in c.fetchall():
            key = (r["school"], r["opponent"])
            oos[key] = {
                "opponent": r["opponent"],
                "division": r["division"] if "division" in optional else "Unknown",
                "class_": r["class_"] if "class_" in optional else "",
                "opp_wins": r["opp_wins"],
                "opp_losses": r["opp_losses"],
                "opp_ties": r["opp_ties"] if "opp_ties" in optional else 0,
            }
        return oos
    except Exception as e:
        print(f"  OOS load error: {e}")
        return {}


def find_oos_record(oos_lookup, school, opponent):
    if (school, opponent) in oos_lookup:
        return oos_lookup[(school, opponent)]
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
        wl = normalize_result(r["win_loss"])
        if school not in records:
            records[school] = {"wins": 0, "losses": 0, "ties": 0}
        if wl == "W":
            records[school]["wins"] += 1
        elif wl == "L":
            records[school]["losses"] += 1
        elif wl == "T":
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
        "Non-Select Division I", "Non-Select Division II",
        "Non-Select Division III", "Non-Select Division IV",
        "Select Division I", "Select Division II",
        "Select Division III", "Select Division IV",
    ]
    print("\n" + "=" * 54)
    print("FULL DIVISION DUMP (FOR AUDIT)")
    print("=" * 54)
    for division in division_order:
        print(f"\n{division.upper()}")
        print("-" * 54)
        div_list = [r for r in ratings if getattr(r, "division", "") == division]
        div_list = sorted(div_list, key=lambda x: getattr(x, "power_rating", 0), reverse=True)
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

    game_exclusions = get_game_exclusions(sport, season)
    excluded_games = []
    if game_exclusions:
        included_rows = []
        for row in rows:
            exclusion = find_game_exclusion(game_exclusions, row)
            if exclusion:
                excluded_games.append(exclusion)
            else:
                included_rows.append(row)
        rows = included_rows
        if excluded_games:
            print(f"  Excluded {len(excluded_games)} official non-counting games:")
            for exclusion in excluded_games:
                print(
                    "   - "
                    f"{exclusion['school']} vs {exclusion['opponent']} "
                    f"({exclusion['game_date']}): {exclusion['reason']}"
                )

    if sport.lower() == "football":
        all_football_rows = rows
        rows = filter_regular_season_football_rows(all_football_rows, season)
        excluded_rows = len(all_football_rows) - len(rows)
        if excluded_rows:
            print(
                f"  Excluded {excluded_rows} duplicate/postseason football rows "
                "(only the first 10 regular-season games count)"
            )

    # Schedule-table sports do not carry football week numbers.
    if sport.lower() != "football":
        def _sort_key(r):
            try:
                return (r.get("school", ""), parse_game_date(r.get("game_date") or ""))
            except Exception:
                return (r.get("school", ""), datetime.min)
        rows.sort(key=_sort_key)

    oos_lookup = load_oos_opponents(conn, season, sport)
    print(f"  OOS lookup: {len(oos_lookup)} games loaded")
    print(f"  Loaded {len(rows)} games after applying overrides")

    # Count forfeits for visibility in the run log
    forfeit_count = sum(1 for r in rows if str(r.get("win_loss") or "").strip() in ("W(f)", "L(f)"))
    if forfeit_count:
        print(f"  Forfeits normalized: {forfeit_count} W(f)/L(f) games treated as W/L")

    scores_lookup = load_scores(conn, season, sport)
    school_records = build_school_records(rows)
    record_overrides = get_record_overrides(sport, season)
    for school, (wins, losses, ties) in record_overrides.items():
        school_records[school] = {
            "wins": wins,
            "losses": losses,
            "ties": ties,
        }
    if record_overrides:
        print(
            f"  Applied {len(record_overrides)} official LHSAA "
            f"record overrides"
        )
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

        db_info = get_school(school, sport)
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
        opp_in_db = get_school(opponent, sport) is not None
        oos = oos_flag or (not opp_in_db)

        # Normalize forfeits (W(f) -> W, L(f) -> L) and ties (Tie -> T) here.
        # Anything that doesn't map to W/L/T is skipped.
        result = normalize_result(wl)
        if result not in ("W", "L", "T"):
            continue

        # Week number
        if sport.lower() != "football":
            try:
                parsed = parse_game_date(game_date)
                date_key = int(parsed.strftime("%Y%m%d"))
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

        oos_data = None
        if oos and sport.lower() in ("football", "baseball", "softball"):
            oos_data = find_oos_record(oos_lookup, school, opponent)

        if oos_data:
            opp_wins = oos_data["opp_wins"]
            opp_losses = oos_data["opp_losses"]
            opp_ties = oos_data.get("opp_ties", 0)
            opp_division = oos_data.get("division", "Unknown")
            opp_class = strip_district_prefix(oos_data.get("class_", ""))
            oos = True
        elif oos:
            opp_wins = 0
            opp_losses = 0
            opp_ties = 0
            opp_division = "Unknown"
            raw_opp_class = r.get("opponent_class") or ""
            opp_class = strip_district_prefix(raw_opp_class)
            oos_missing.append(f"{school} vs {opponent} ({game_date})")
        else:
            official_opp_record = find_record_override(
                record_overrides, opponent
            )
            if official_opp_record:
                ow, ol, ot = official_opp_record
                opp_record = {"wins": ow, "losses": ol, "ties": ot}
            else:
                opp_record = school_records.get(
                    opponent,
                    {"wins": 0, "losses": 0, "ties": 0},
                )
            opp_wins = opp_record["wins"]
            opp_losses = opp_record["losses"]
            opp_ties = opp_record["ties"]
            opp_info = get_school(opponent, sport)
            opp_division = opp_info["division"] if opp_info else "Unknown"
            raw_opp_class = r.get("opponent_class") or ""
            opp_class = strip_district_prefix(raw_opp_class) or (opp_info["class"] if opp_info else "")

        score = r.get("score") or scores_lookup.get((school, str(week_num)), "")

        game_meta[game_key] = {
            "opponent":    opponent,
            "result":      result,
            "score":       score,
            "opp_wins":    opp_wins,
            "opp_losses":  opp_losses,
            "opp_ties":    opp_ties,
            "opp_division": opp_division,
            "game_date":   game_date.split(" ")[0] if game_date else "",
            "home_away":   r.get("home_away") or "",
        }

        engine.add_game(GameResult(
            team=school,
            opponent=opponent,
            result=result,
            sport=sport,
            opponent_wins=opp_wins,
            opponent_losses=opp_losses,
            opponent_ties=opp_ties,
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
        db_info = get_school(r.name, sport)
        division = db_info["division"] if db_info else r.division
        track = db_info["track"] if db_info else "unknown"
        class_ = db_info["class"] if db_info else ""
        district = db_info["district"] if db_info else None

        opp_qualities = [g["oppq"] for g in r.breakdown if g.get("oppq") is not None]
        strength_factor = round(sum(opp_qualities) / len(opp_qualities), 2) if opp_qualities else 0.0
        official_record = find_record_override(record_overrides, r.name)
        if official_record:
            stored_wins, stored_losses, stored_ties = official_record
            stored_games = stored_wins + stored_losses + stored_ties
        else:
            stored_wins, stored_losses, stored_ties = (
                r.wins, r.losses, r.ties
            )
            stored_games = r.games_played

        c.execute("""
            INSERT OR REPLACE INTO power_rankings
            (sport, season, school, division, track, class_, district,
             rank, power_rating, wins, losses, ties, games_played,
             strength_factor, calculated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sport, season, r.name, division, track, class_, district,
            r.rank, r.power_rating, stored_wins, stored_losses, stored_ties,
            stored_games, strength_factor, now_str
        ))

        for g in r.breakdown:
            week_num = g["week"]
            game_key = (r.name, week_num) if week_num else (r.name, "")
            meta = game_meta.get(game_key, {})
            school_info = get_school(r.name, sport)
            opp_info = get_school(meta.get("opponent", g["opponent"]), sport)

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
                 opp_wins, opp_losses, opp_ties, opp_division,
                 base_pts, div_bonus, opp_quality, total_pts, is_district,
                 game_date, home_away, calculated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sport, season, r.name, week_num,
                meta.get("opponent", g["opponent"]),
                g["result"],
                meta.get("score", ""),
                meta.get("opp_wins", 0),
                meta.get("opp_losses", 0),
                meta.get("opp_ties", 0),
                meta.get("opp_division", ""),
                g["base"],
                g["div"],
                g["oppq"],
                g["total"],
                is_district,
                meta.get("game_date", ""),
                meta.get("home_away", ""),
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
