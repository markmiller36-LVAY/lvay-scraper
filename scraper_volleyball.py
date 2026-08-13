"""
LHSAA Volleyball Scraper
========================
Scrapes volleyball schedules/results from lhsaaonline.org for all 5 divisions.
Season: 2025 (2025-2026 school year, fall sport)

POST endpoint: https://www.lhsaaonline.org/pr/vbpr/admin/ReportSchedule.asp?p=1
Form params:
    y      = 2025
    d      = I | II | III | IV | V

PR Formula (Bylaw 24.6.3):
    Win:  5 base points + opponent_wins * 1.0  (100%)
    Loss: 0 base points + opponent_wins * 0.33 (33%)
    PR = total_power_points / total_matches_played

Exclusions (counts_for_pr = 0):
    - OOS opponents (no valid LA division)

Inclusions:
    - District matches (dist_t = "D")
    - Regular season + tournament matches both count
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

SCRAPE_URL = "https://www.lhsaaonline.org/pr/vbpr/admin/ReportSchedule.asp"
SEARCH_URL = "https://www.lhsaaonline.org/pr/vbpr/admin/SearchVolleyballSchedule.asp"
SEASON     = os.environ.get("VOLLEYBALL_SEASON_YEAR", str(datetime.now().year))
SPORT      = "volleyball"
DIVISIONS  = ["I", "II", "III", "IV", "V"]

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": "https://www.lhsaaonline.org/pr/vbpr/admin/SearchVolleyballSchedule.asp",
}

LA_DIVISION_PATTERN = re.compile(r"^\d+-(I{1,3}V?|V?I{0,3}|IV|V)$")

# ──────────────────────────────────────────────────────────────────────────────
# DB SETUP
# ──────────────────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_tables(conn):
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

def is_oos_opponent(opp_div_raw):
    if not opp_div_raw or not opp_div_raw.strip():
        return True
    raw = opp_div_raw.strip()
    if LA_DIVISION_PATTERN.match(raw):
        return False
    return True


def parse_school_division(div_str):
    if not div_str:
        return None, None
    div_str = div_str.strip()
    m = re.match(r"^(\d+)-([IVX]+)$", div_str)
    if m:
        return int(m.group(1)), m.group(2)
    return None, div_str


def parse_date(date_raw):
    if not date_raw:
        return None
    parts = date_raw.split()
    for part in parts:
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(part, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    return date_raw


def resolve_lhsaa_season_token(season=SEASON):
    """Translate our season year into the opaque value used by LHSAA's form.

    LHSAA historically used the starting year itself (for example ``2025``),
    but its 2026-2027 volleyball option uses ``1``. Reading the live form keeps
    future selector changes from silently producing empty schedules.
    """
    override = os.environ.get("LHSAA_VOLLEYBALL_SEASON_TOKEN")
    if override:
        return override.strip()

    requested = str(season).strip()
    try:
        resp = requests.get(SEARCH_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        select = soup.find("select", attrs={"name": "y"})
        if select:
            for option in select.find_all("option"):
                label = option.get_text(" ", strip=True)
                value = option.get("value", "").strip()
                if value and re.match(rf"^\s*{re.escape(requested)}\s*[-–]", label):
                    if value != requested:
                        print(f"  [VB] LHSAA season {requested} maps to form token {value!r}")
                    return value
    except requests.RequestException as exc:
        print(f"  [VB] WARNING: could not read LHSAA season selector: {exc}")

    print(f"  [VB] WARNING: no selector match for {requested}; using legacy token")
    return requested


# ──────────────────────────────────────────────────────────────────────────────
# SCRAPER — mirrors baseball/softball parse pattern exactly
# ──────────────────────────────────────────────────────────────────────────────

def scrape_division(division, season=SEASON, lhsaa_season_token=None):
    lhsaa_season_token = lhsaa_season_token or resolve_lhsaa_season_token(season)
    payload = {
        "y":          lhsaa_season_token,
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
        resp = requests.post(
            SCRAPE_URL,
            params={"p": "1"},
            data=payload,
            headers=HEADERS,
            timeout=45,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [VB] ERROR fetching Division {division}: {e}")
        return None

    if "Invalid object name" in resp.text or "Microsoft OLE DB Provider" in resp.text:
        print(f"  [VB] ERROR: LHSAA rejected season token {lhsaa_season_token!r}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    rows = []

    # Mirror baseball/softball: loop tables → rows → cells
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 11:
                continue

            t = [c.get_text(strip=True) for c in cells]

            # Skip header rows — first cell is "#" or empty or "School"
            if not t[0] or t[0] in ("#", "School"):
                continue

            # First cell must be a row number like "1." or "1"
            if not re.match(r"^\d+\.?$", t[0]):
                continue

            # Must have a school name and opponent
            school   = t[1].strip() if len(t) > 1 else ""
            opponent = t[4].strip() if len(t) > 4 else ""
            if not school or not opponent:
                continue

            # Preseason schedules do not have results yet. Preserve those rows
            # so the website can publish the schedule before matches are played;
            # the rankings engine already ignores blank/unknown results.
            win_loss = t[10].strip().upper() if len(t) > 10 else ""
            if win_loss not in ("W", "L"):
                win_loss = ""

            rows.append({
                "school":      school,
                "school_dd":   t[2].strip() if len(t) > 2 else "",
                "date_raw":    t[3].strip() if len(t) > 3 else "",
                "opponent":    opponent,
                "opp_dd":      t[5].strip() if len(t) > 5 else "",
                "dist_t":      t[6].strip() if len(t) > 6 else "",
                "tournament":  t[7].strip() if len(t) > 7 else "",
                "match_num":   t[8].strip() if len(t) > 8 else "1",
                "home_away":   t[9].strip() if len(t) > 9 else "",
                "win_loss":    win_loss,
                "score":       t[11].strip() if len(t) > 11 else "",
                "division":    division,
            })

    print(f"  [VB] Division {division}: {len(rows)} result rows found")
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# DB INSERT
# ──────────────────────────────────────────────────────────────────────────────

def insert_games(conn, rows, season=SEASON):
    inserted = 0
    updated  = 0
    skipped  = 0

    for row in rows:
        school_dist, school_div = parse_school_division(row["school_dd"])
        opp_dist, opp_div       = parse_school_division(row["opp_dd"])

        game_date = parse_date(row["date_raw"])
        is_tourn  = 1 if row["dist_t"].upper() == "T" else 0
        is_dist   = 1 if row["dist_t"].upper() == "D" else 0
        # The LHSAA report sometimes omits an in-state opponent's district and
        # supplies only its Roman postseason division (for example, "II").
        # That incomplete metadata must not cause a Louisiana match to be
        # discarded. The report uses the explicit OUT OF STATE placeholder for
        # matches that should be excluded.
        is_oos = row["opponent"].strip().upper() == "OUT OF STATE"
        # The LHSAA report's "D" flag means a normal district match.
        # These matches count toward both the official record and power rating.
        counts    = 0 if is_oos else 1

        try:
            match_num = int(row["match_num"]) if str(row["match_num"]).isdigit() else 1
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
                SPORT, str(season),
                row["school"], row["division"], school_dist,
                game_date, row["opponent"], opp_div, opp_dist,
                is_dist, is_tourn, row["tournament"] or None,
                match_num, row["home_away"], row["win_loss"],
                row["score"] or None, counts,
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            conn.execute("""
                UPDATE volleyball_games
                SET result=?, score=?, counts_for_pr=?
                WHERE sport=? AND season=? AND school=? AND game_date=?
                  AND opponent=? AND match_num=?
            """, (
                row["win_loss"], row["score"] or None, counts,
                SPORT, str(season), row["school"], game_date,
                row["opponent"], match_num,
            ))
            updated += 1
        except sqlite3.Error as e:
            print(f"  [VB] DB error on {row['school']} vs {row['opponent']}: {e}")
            skipped += 1

    conn.commit()

    # Final safety pass: historical rows may have been created before OOS
    # placeholders were excluded. Reassert the rule after every scrape so a
    # rescrape repairs those rows even if the source formatting changes.
    conn.execute("""
        UPDATE volleyball_games
        SET counts_for_pr = CASE
            WHEN UPPER(TRIM(opponent))='OUT OF STATE' THEN 0
            ELSE 1
        END
        WHERE sport=? AND season=?
    """, (SPORT, str(season)))

    # The 2025 LHSAA Division III report contains Livingston Collegiate's
    # schedule a second time under the name Sarah T. Reed. Every affected
    # opponent row is duplicated byte-for-byte (date/result/score) with a
    # correct Livingston Collegiate row. Remove only those proven duplicates,
    # plus the copied Division III block from Reed's own schedule. Reed's real
    # schedule is Division IV and remains untouched.
    if str(season) == "2025":
        conn.execute("""
            DELETE FROM volleyball_games
            WHERE sport=? AND season=?
              AND school='Sarah T. Reed'
              AND school_division='III'
        """, (SPORT, str(season)))
        conn.execute("""
            DELETE FROM volleyball_games AS duplicate
            WHERE duplicate.sport=? AND duplicate.season=?
              AND duplicate.opponent='Sarah T. Reed'
              AND duplicate.opp_division='III'
              AND EXISTS (
                  SELECT 1
                  FROM volleyball_games AS correct
                  WHERE correct.sport=duplicate.sport
                    AND correct.season=duplicate.season
                    AND correct.school=duplicate.school
                    AND correct.game_date=duplicate.game_date
                    AND correct.opponent='Livingston Collegiate'
                    AND correct.result=duplicate.result
                    AND COALESCE(correct.score, '')=COALESCE(duplicate.score, '')
              )
        """, (SPORT, str(season)))
    conn.commit()

    return inserted, updated, skipped


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def run_volleyball_scraper(season=None):
    season = str(season or SEASON)
    print(f"\n{'='*54}")
    print(f"LVAY Volleyball Scraper — Season {season}")
    print(f"{'='*54}")

    conn = get_db()
    ensure_tables(conn)

    total_inserted = 0
    total_updated  = 0
    total_skipped  = 0
    total_rows     = 0
    failed_divisions = []
    lhsaa_season_token = resolve_lhsaa_season_token(season)

    for div in DIVISIONS:
        rows = scrape_division(div, season, lhsaa_season_token)
        if rows is None:
            failed_divisions.append(div)
            continue
        total_rows += len(rows)
        if rows:
            ins, upd, skp = insert_games(conn, rows, season)
            total_inserted += ins
            total_updated  += upd
            total_skipped  += skp
            print(f"  [VB] Division {div}: inserted={ins} updated={upd} skipped={skp}")

    conn.close()

    if failed_divisions:
        raise RuntimeError(
            "Volleyball scrape incomplete; failed divisions: "
            + ", ".join(failed_divisions)
        )

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
