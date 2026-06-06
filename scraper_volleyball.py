"""
LHSAA Volleyball Scraper
========================
Scrapes volleyball schedules/results from lhsaaonline.org for all 5 divisions.
Season: 2025 (2025-2026 school year, fall sport)

POST endpoint: https://www.lhsaaonline.org/pr/vbpr/admin/ReportSchedule.asp?p=1
Form params:
    y      = 2025
    d      = I | II | III | IV | V
    n      = (blank - all schools)
    h      = (blank - all tournaments)
    resultdate = (blank)
    f      = (blank)
    Submit.x = 30
    Submit.y = 3

PR Formula (Bylaw 24.6.3):
    Win:  5 base points + opponent_wins * 1.0  (100%)
    Loss: 0 base points + opponent_wins * 0.33 (33%)
    PR = total_power_points / total_matches_played

Exclusions (do NOT count toward PR):
    - Out-of-state opponents (no valid LA division in Opponent District-Division)
    - Sub-varsity matches
    - District playoff tiebreaker matches (District or Tournament = "D" tiebreaker)
    
Inclusions:
    - Regular season matches
    - Tournament matches (T flag counts — only OOS/sub-varsity/tiebreakers excluded)
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import re
from datetime import datetime

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

SCRAPE_URL = "https://www.lhsaaonline.org/pr/vbpr/admin/ReportSchedule.asp?p=1"
SEASON     = "2025"
SPORT      = "volleyball"
DIVISIONS  = ["I", "II", "III", "IV", "V"]

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.lhsaaonline.org/pr/vbpr/admin/SearchVolleyballSchedule.asp",
    "Origin":  "https://www.lhsaaonline.org",
    "Content-Type": "application/x-www-form-urlencoded",
}

# Valid LA division patterns — used to detect OOS opponents
LA_DIVISION_PATTERN = re.compile(
    r"^\d+-(I{1,3}V?|V?I{0,3})$"  # e.g. 3-I, 7-II, 4-III, 5-IV, 2-V
)

# ──────────────────────────────────────────────────────────────────────────────
# DB SETUP
# ──────────────────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tables(conn):
    """Create volleyball tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS volleyball_games (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            sport            TEXT    DEFAULT 'volleyball',
            season           TEXT    NOT NULL,
            school           TEXT    NOT NULL,
            school_division  TEXT,
            school_district  TEXT,
            game_date        TEXT,
            opponent         TEXT,
            opp_division     TEXT,
            opp_district     TEXT,
            is_district      INTEGER DEFAULT 0,
            is_tournament    INTEGER DEFAULT 0,
            tournament_name  TEXT,
            match_num        INTEGER DEFAULT 1,
            home_away        TEXT,
            result           TEXT,
            score            TEXT,
            counts_for_pr    INTEGER DEFAULT 1,
            created_at       TEXT    DEFAULT (datetime('now')),
            UNIQUE(sport, season, school, game_date, opponent, match_num)
        );

        CREATE TABLE IF NOT EXISTS volleyball_rankings (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            sport         TEXT    DEFAULT 'volleyball',
            season        TEXT    NOT NULL,
            school        TEXT    NOT NULL,
            division      TEXT,
            class_        TEXT,
            district      INTEGER,
            wins          INTEGER DEFAULT 0,
            losses        INTEGER DEFAULT 0,
            games_played  INTEGER DEFAULT 0,
            power_rating  REAL    DEFAULT 0.0,
            rank          INTEGER,
            div_rank      INTEGER,
            updated_at    TEXT    DEFAULT (datetime('now')),
            UNIQUE(sport, season, school)
        );
    """)
    conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def normalize_name(name):
    """Strip whitespace and normalize school name."""
    if not name:
        return ""
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    # Strip trailing * or + markers from alignment doc
    name = name.rstrip("*+").strip()
    return name


def is_oos_opponent(opp_div_raw):
    """
    Returns True if the opponent is out of state or has no valid LA division.
    Valid LA divisions look like: 3-I, 7-II, 4-III, 5-IV, 2-V, 8-II, etc.
    """
    if not opp_div_raw or not opp_div_raw.strip():
        return True  # blank = OOS or unknown
    raw = opp_div_raw.strip()
    if LA_DIVISION_PATTERN.match(raw):
        return False
    return True  # doesn't match LA pattern = OOS


def is_tiebreaker(dist_or_tourn_raw):
    """
    District playoff tiebreaker matches are marked 'D' in District or Tournament column.
    Regular district games are blank. Tournament games are 'T'.
    Tiebreakers don't count toward PR.
    NOTE: 'D' = tiebreaker (exclude), 'T' = tournament (include), blank = regular (include)
    """
    if not dist_or_tourn_raw:
        return False
    return dist_or_tourn_raw.strip().upper() == "D"


def parse_school_division(div_str):
    """
    Parse '3-III' into district=3, division='III'.
    Returns (district, division) tuple.
    """
    if not div_str:
        return None, None
    div_str = div_str.strip()
    m = re.match(r"^(\d+)-([IVX]+)$", div_str)
    if m:
        return int(m.group(1)), m.group(2)
    return None, div_str


# ──────────────────────────────────────────────────────────────────────────────
# SCRAPER
# ──────────────────────────────────────────────────────────────────────────────

def scrape_division(division):
    """
    POST to LHSAA and fetch volleyball schedule for one division.
    Returns list of raw row dicts.
    """
    payload = {
        "y":          SEASON,
        "resultdate": "",
        "n":          "",
        "h":          "",
        "d":          division,
        "f":          "",
        "Submit.x":   "30",
        "Submit.y":   "3",
    }

    print(f"  [VB] Fetching Division {division}...")
    try:
        resp = requests.post(SCRAPE_URL, data=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [VB] ERROR fetching Division {division}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    rows = []

    # Find all tables — each school section is its own table or section
    # The report groups by school with a header row per school block
    # Rows: # | School | District-Division | Date | Opponent | Opp D-D | Dist/Tourn | Tournament | Match# | H/A | W/L | Score

    tables = soup.find_all("table")
    current_school = None
    current_school_div = None

    for table in tables:
        trs = table.find_all("tr")
        for tr in trs:
            tds = tr.find_all("td")
            if not tds:
                continue

            # Skip header rows (color headers)
            td_text = [td.get_text(strip=True) for td in tds]

            # Detect school data rows — first cell is a number (#)
            if not td_text[0].isdigit():
                continue

            # Expect at least 10 columns: # school dist date opp opp_div dist_t tourn match# h/a w/l [score]
            if len(tds) < 10:
                continue

            try:
                row_num    = td_text[0]
                school     = normalize_name(td_text[1]) if len(td_text) > 1 else ""
                school_dd  = td_text[2].strip() if len(td_text) > 2 else ""
                date_raw   = td_text[3].strip() if len(td_text) > 3 else ""
                opponent   = normalize_name(td_text[4]) if len(td_text) > 4 else ""
                opp_dd     = td_text[5].strip() if len(td_text) > 5 else ""
                dist_t     = td_text[6].strip() if len(td_text) > 6 else ""
                tournament = td_text[7].strip() if len(td_text) > 7 else ""
                match_num  = td_text[8].strip() if len(td_text) > 8 else "1"
                home_away  = td_text[9].strip() if len(td_text) > 9 else ""
                win_loss   = td_text[10].strip() if len(td_text) > 10 else ""
                score      = td_text[11].strip() if len(td_text) > 11 else ""

                # Skip if no school or opponent
                if not school or not opponent:
                    continue

                # Skip blank results (future games with no outcome)
                if not win_loss:
                    continue

                rows.append({
                    "school":      school,
                    "school_dd":   school_dd,
                    "date_raw":    date_raw,
                    "opponent":    opponent,
                    "opp_dd":      opp_dd,
                    "dist_t":      dist_t,
                    "tournament":  tournament,
                    "match_num":   match_num,
                    "home_away":   home_away,
                    "win_loss":    win_loss.upper(),
                    "score":       score,
                    "division":    division,
                })
            except (IndexError, ValueError):
                continue

    print(f"  [VB] Division {division}: {len(rows)} result rows found")
    return rows


def parse_date(date_raw):
    """Parse date like '9/2/2025 Tue' or '9/2/2025' into YYYY-MM-DD."""
    if not date_raw:
        return None
    # Strip day-of-week suffix
    date_part = date_raw.split("\n")[0].strip().split(" ")[0].strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(date_part, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_raw


def insert_games(conn, rows):
    """Insert scraped rows into volleyball_games table."""
    inserted = 0
    updated  = 0
    skipped  = 0

    for row in rows:
        school_dist, school_div = parse_school_division(row["school_dd"])
        opp_dist, opp_div       = parse_school_division(row["opp_dd"])

        game_date    = parse_date(row["date_raw"])
        is_tourn     = 1 if row["dist_t"].upper() == "T" else 0
        is_dist      = 1 if row["dist_t"].upper() == "D" else 0
        is_tiebreak  = is_tiebreaker(row["dist_t"])
        is_oos       = is_oos_opponent(row["opp_dd"])

        # Determine if this game counts toward PR
        counts = 1
        if is_oos:
            counts = 0
        elif is_tiebreak:
            counts = 0
        # Sub-varsity: harder to detect from this data — flag if opponent has no division
        # and is not a known OOS situation (already handled above)

        try:
            match_num = int(row["match_num"]) if row["match_num"].isdigit() else 1
        except (ValueError, AttributeError):
            match_num = 1

        try:
            conn.execute("""
                INSERT INTO volleyball_games
                    (sport, season, school, school_division, school_district,
                     game_date, opponent, opp_division, opp_district,
                     is_district, is_tournament, tournament_name,
                     match_num, home_away, result, score, counts_for_pr)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                SPORT, SEASON,
                row["school"],
                row["division"],   # volleyball division (I-V)
                school_dist,
                game_date,
                row["opponent"],
                opp_div,
                opp_dist,
                is_dist,
                is_tourn,
                row["tournament"] or None,
                match_num,
                row["home_away"],
                row["win_loss"],
                row["score"] or None,
                counts,
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            # Already exists — update result/score in case it changed
            conn.execute("""
                UPDATE volleyball_games
                SET result=?, score=?, counts_for_pr=?, updated_at=datetime('now')
                WHERE sport=? AND season=? AND school=? AND game_date=?
                  AND opponent=? AND match_num=?
            """, (
                row["win_loss"], row["score"] or None, counts,
                SPORT, SEASON, row["school"], game_date,
                row["opponent"], match_num,
            ))
            updated += 1
        except sqlite3.Error as e:
            print(f"  [VB] DB error on {row['school']} vs {row['opponent']}: {e}")
            skipped += 1

    conn.commit()
    return inserted, updated, skipped


# ──────────────────────────────────────────────────────────────────────────────
# MAIN RUN FUNCTION
# ──────────────────────────────────────────────────────────────────────────────

def run_volleyball_scraper():
    print(f"\n{'='*54}")
    print(f"LVAY Volleyball Scraper — Season {SEASON}")
    print(f"{'='*54}")

    conn = get_db()
    ensure_tables(conn)

    total_inserted = 0
    total_updated  = 0
    total_skipped  = 0
    total_rows     = 0

    for div in DIVISIONS:
        rows = scrape_division(div)
        total_rows += len(rows)
        if rows:
            ins, upd, skp = insert_games(conn, rows)
            total_inserted += ins
            total_updated  += upd
            total_skipped  += skp
            print(f"  [VB] Division {div}: inserted={ins} updated={upd} skipped={skp}")

    conn.close()

    print(f"\n{'='*54}")
    print(f"VOLLEYBALL SCRAPE COMPLETE")
    print(f"  Total rows scraped : {total_rows}")
    print(f"  Inserted           : {total_inserted}")
    print(f"  Updated            : {total_updated}")
    print(f"  Skipped (errors)   : {total_skipped}")
    print(f"{'='*54}\n")

    return {
        "rows":     total_rows,
        "inserted": total_inserted,
        "updated":  total_updated,
        "skipped":  total_skipped,
    }


if __name__ == "__main__":
    run_volleyball_scraper()
