"""
LVAY - Google Sheets Exporter
================================
NO formatting calls — pure data only.
Scores tab is built separately via /api/build/football-scores
to avoid Sheets API timeouts.

Main build (/api/build/football-sheets) writes:
  - Football Power Rankings (2025)  ← from power_rankings table
  - Football Needs Review
  - Football District Records
  - Instructions

Separate build (/api/build/football-scores) writes:
  - Football Scores (2025)          ← 2997 rows, slow
"""

import gspread
from google.oauth2.service_account import Credentials
import sqlite3
import json
import os
import time
from datetime import datetime

DB_PATH  = os.environ.get("DB_PATH", "/data/lvay_v2.db")
SHEET_ID = "1u_cJBAWTQJIAO36HZTYvPa7QfE0JoOEqx12c1U4t4mk"
SEASON   = 2025

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DIVISION_ORDER = [
    "Non-Select Division I",
    "Non-Select Division II",
    "Non-Select Division III",
    "Non-Select Division IV",
    "Select Division I",
    "Select Division II",
    "Select Division III",
    "Select Division IV",
]

DIVISION_LABELS = {
    "Non-Select Division I":   "NS I",
    "Non-Select Division II":  "NS II",
    "Non-Select Division III": "NS III",
    "Non-Select Division IV":  "NS IV",
    "Select Division I":       "S I",
    "Select Division II":      "S II",
    "Select Division III":     "S III",
    "Select Division IV":      "S IV",
}


# ─── AUTH ─────────────────────────────────────────────────────────────────────

def get_client():
    secret_path = "/etc/secrets/google-credentials.json"
    if os.path.exists(secret_path):
        with open(secret_path, "r") as f:
            creds_dict = json.load(f)
    else:
        creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
        if not creds_json:
            raise ValueError("No Google credentials found")
        creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def get_or_create_tab(sheet, tab_name, rows=3000, cols=20):
    try:
        ws = sheet.worksheet(tab_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
    time.sleep(2)
    return ws


def batch_write(ws, start_row, data, chunk_size=100):
    """Write data in small chunks with generous sleep to avoid 429s."""
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        ws.update(f"A{start_row + i}", chunk)
        time.sleep(2)


# ─── FOOTBALL POWER RANKINGS ──────────────────────────────────────────────────

def build_football_power_rankings(sheet, season=SEASON):
    tab_name = f"Football Power Rankings ({season})"
    print(f"  Building {tab_name}...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Read from power_rankings table
    all_schools = []
    try:
        c.execute("""
            SELECT school, division, track, class_, district,
                   wins, losses, ties, games_played, power_rating, rank
            FROM power_rankings
            WHERE sport='football' AND season=?
            ORDER BY rank ASC
        """, (str(season),))
        all_schools = [dict(r) for r in c.fetchall()]
        print(f"    Loaded {len(all_schools)} schools from power_rankings table")
    except Exception as e:
        print(f"    ERROR reading power_rankings: {e}")

    conn.close()

    if not all_schools:
        print(f"    No data — run /api/rankings/calculate first")
        return 0

    # Group by division
    by_division = {div: [] for div in DIVISION_ORDER}
    unmatched = []
    for s in all_schools:
        div = s.get("division") or ""
        if div in by_division:
            by_division[div].append(s)
        else:
            unmatched.append(s)

    # Already sorted by rank from DB, but re-sort within division by power_rating
    for div in DIVISION_ORDER:
        by_division[div].sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)

    now_str     = datetime.now().strftime("%m/%d/%Y %I:%M %p")
    col_headers = ["Div Rank", "School", "Division", "Class", "District", "W", "L", "Games", "Power Rating"]

    ws = get_or_create_tab(sheet, tab_name)

    all_rows = []
    all_rows.append([f"LVAY Football Power Rankings {season} — Updated {now_str}"] + [""] * 8)
    all_rows.append(col_headers)

    total = 0
    for division in DIVISION_ORDER:
        schools = by_division[division]
        if not schools:
            continue
        all_rows.append([f"=== {division.upper()} ==="] + [""] * 8)
        for rank, s in enumerate(schools, 1):
            all_rows.append([
                rank,
                s.get("school") or "",
                DIVISION_LABELS.get(s.get("division", ""), s.get("division") or ""),
                s.get("class_") or "",
                s.get("district") or "",
                s.get("wins") or 0,
                s.get("losses") or 0,
                s.get("games_played") or 0,
                round(float(s.get("power_rating") or 0), 2),
            ])
            total += 1
        all_rows.append([""] * 9)

    if unmatched:
        unmatched.sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)
        all_rows.append(["=== UNMATCHED / NO DIVISION ==="] + [""] * 8)
        for rank, s in enumerate(unmatched, 1):
            all_rows.append([
                rank,
                s.get("school") or "",
                s.get("division") or "Unknown",
                s.get("class_") or "",
                s.get("district") or "",
                s.get("wins") or 0,
                s.get("losses") or 0,
                s.get("games_played") or 0,
                round(float(s.get("power_rating") or 0), 2),
            ])
            total += 1

    batch_write(ws, 1, all_rows)
    print(f"    Written {total} school rankings")
    return total


# ─── NEEDS REVIEW ─────────────────────────────────────────────────────────────

def build_needs_review(sheet, season=SEASON):
    tab_name = "Football Needs Review"
    print(f"  Building {tab_name}...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT school, week, game_date, opponent, win_loss, score,
               class_, district, district_class, needs_review
        FROM games
        WHERE sport='football' AND season=?
          AND (win_loss IS NULL OR win_loss = ''
               OR score IS NULL OR score = ''
               OR needs_review = 1)
        ORDER BY school, week
    """, (str(season),))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name)
    ws.update("A1", [[
        "School", "Week", "Date", "Opponent", "W/L", "Score",
        "Class", "District", "District/Class", "Issue"
    ]])
    time.sleep(1)

    data = []
    for r in rows:
        issues = []
        if not r["win_loss"]:  issues.append("missing W/L")
        if not r["score"]:     issues.append("missing score")
        if r["needs_review"]:  issues.append("flagged")
        data.append([
            r["school"] or "", r["week"] or "", r["game_date"] or "",
            r["opponent"] or "", r["win_loss"] or "", r["score"] or "",
            r["class_"] or "", r["district"] or "", r["district_class"] or "",
            ", ".join(issues),
        ])

    if data:
        batch_write(ws, 2, data)
        print(f"    {len(data)} games need review")
    else:
        ws.update("A2", [["No issues found!"]])
        print(f"    No issues found!")

    return len(data)


# ─── DISTRICT RECORDS ─────────────────────────────────────────────────────────

def build_district_records(sheet, season=SEASON):
    tab_name = "Football District Records"
    print(f"  Building {tab_name}...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT g.school,
               pr.division, pr.track,
               g.class_, g.district, g.district_class,
            SUM(CASE WHEN g.win_loss='W' THEN 1 ELSE 0 END) as total_wins,
            SUM(CASE WHEN g.win_loss='L' THEN 1 ELSE 0 END) as total_losses,
            pr.power_rating, pr.rank
        FROM games g
        LEFT JOIN power_rankings pr
            ON pr.school = g.school
            AND pr.sport = 'football'
            AND pr.season = g.season
        WHERE g.sport='football' AND g.season=?
        GROUP BY g.school
        ORDER BY pr.division, g.district,
                 total_wins DESC, total_losses ASC
    """, (str(season),))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name)
    ws.update("A1", [[
        "Rank", "School", "Division", "Class", "District",
        "Total W", "Total L", "Power Rating"
    ]])
    time.sleep(1)

    data = []
    for r in rows:
        data.append([
            r["rank"] or "",
            r["school"] or "",
            DIVISION_LABELS.get(r["division"], r["division"] or ""),
            r["class_"] or "",
            r["district"] or "",
            r["total_wins"] or 0,
            r["total_losses"] or 0,
            round(float(r["power_rating"] or 0), 2),
        ])

    if data:
        batch_write(ws, 2, data)

    print(f"    Written {len(data)} school records")
    return len(data)


# ─── INSTRUCTIONS ─────────────────────────────────────────────────────────────

def build_instructions_tab(sheet):
    tab_name = "Instructions"
    print(f"  Building {tab_name}...")
    ws = get_or_create_tab(sheet, tab_name)
    now_str = datetime.now().strftime("%m/%d/%Y %I:%M %p CST")
    ws.update("A1", [
        ["LVAY Football Data — Google Sheet Guide"],
        [""],
        ["Last Updated:", now_str],
        ["Source:", "lhsaaonline.org — auto-scraped by lvay-scraper on Render"],
        [""],
        ["TAB", "CONTENTS"],
        [f"Football Power Rankings ({SEASON})", "304 schools ranked by power rating, grouped NS I thru S IV"],
        [f"Football Scores ({SEASON})", "Every game — build separately via /api/build/football-scores"],
        ["Football Needs Review", "Games flagged for missing W/L or score"],
        ["Football District Records", "W/L and power rating per school"],
        [""],
        ["POWER RATING FORMULA (LHSAA Football 14.12)"],
        ["Win", "10 pts + Opp Quality + Division Bonus"],
        ["Loss", "0 pts + Opp Quality + Division Bonus"],
        ["Tie", "5 pts + Opp Quality + Division Bonus"],
        ["Opp Quality", "(Opp Wins / Opp Games) x 10"],
        ["Div Bonus", "+2 pts per division level above you"],
        ["Power Rating", "Total Points / Games Played"],
        [""],
        ["DIVISION KEY", ""],
        ["NS I",   "Non-Select Division I   (largest non-select schools)"],
        ["NS II",  "Non-Select Division II"],
        ["NS III", "Non-Select Division III"],
        ["NS IV",  "Non-Select Division IV"],
        ["S I",    "Select Division I   (largest select/private schools)"],
        ["S II",   "Select Division II"],
        ["S III",  "Select Division III"],
        ["S IV",   "Select Division IV"],
    ])
    print(f"    Done")


# ─── FOOTBALL SCORES (separate slow build) ────────────────────────────────────

def build_football_scores(sheet, season=SEASON):
    tab_name = f"Football Scores ({season})"
    print(f"  Building {tab_name}...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT school, week, game_date, home_away, opponent,
               class_, district, district_class, win_loss, score
        FROM games
        WHERE sport='football' AND season=?
        ORDER BY school, CAST(REPLACE(week,'Week ','') AS INTEGER)
    """, (str(season),))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name)
    ws.update("A1", [[
        "School", "Week", "Date", "H/A", "Opponent",
        "Class", "District", "District/Class", "W/L", "Score"
    ]])
    time.sleep(2)

    data = []
    for r in rows:
        data.append([
            r["school"] or "", r["week"] or "", r["game_date"] or "",
            r["home_away"] or "", r["opponent"] or "",
            r["class_"] or "", r["district"] or "", r["district_class"] or "",
            r["win_loss"] or "", r["score"] or "",
        ])

    if data:
        batch_write(ws, 2, data)

    print(f"    Written {len(data)} games")
    return len(data)


# ─── MAIN EXPORTS ─────────────────────────────────────────────────────────────

def export_football_to_sheets(season=SEASON):
    """
    Main build — rankings, needs review, district records, instructions.
    Does NOT include scores tab (too slow — use export_football_scores separately).
    """
    print(f"\n{'='*54}")
    print(f"LVAY Football Sheets Export — Season {season}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*54}")

    try:
        client = get_client()
        sheet  = client.open_by_key(SHEET_ID)
        print(f"Connected: {sheet.title}")
    except Exception as e:
        print(f"ERROR connecting: {e}")
        return False

    try:
        rankings = build_football_power_rankings(sheet, season)
    except Exception as e:
        print(f"  ERROR rankings: {e}")
        rankings = 0

    try:
        flagged = build_needs_review(sheet, season)
    except Exception as e:
        print(f"  ERROR needs-review: {e}")
        flagged = 0

    try:
        districts = build_district_records(sheet, season)
    except Exception as e:
        print(f"  ERROR district records: {e}")
        districts = 0

    try:
        build_instructions_tab(sheet)
    except Exception as e:
        print(f"  ERROR instructions: {e}")

    print(f"\n{'='*54}")
    print(f"DONE! Football {season} Sheets complete.")
    print(f"  Rankings:         {rankings} schools")
    print(f"  Needs Review:     {flagged} flagged")
    print(f"  District Records: {districts} schools")
    print(f"Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"{'='*54}\n")
    return True


def export_football_scores(season=SEASON):
    """Separate slow build for scores tab only."""
    print(f"\nBuilding Football Scores tab...")
    try:
        client = get_client()
        sheet  = client.open_by_key(SHEET_ID)
        scores = build_football_scores(sheet, season)
        print(f"Done — {scores} games written")
        return True
    except Exception as e:
        print(f"ERROR: {e}")
        return False


if __name__ == "__main__":
    export_football_to_sheets()


# ─── DIVISION TABS ────────────────────────────────────────────────────────────

DIVISION_TAB_NAMES = {
    "Non-Select Division I":   "NS Division I",
    "Non-Select Division II":  "NS Division II",
    "Non-Select Division III": "NS Division III",
    "Non-Select Division IV":  "NS Division IV",
    "Select Division I":       "S Division I",
    "Select Division II":      "S Division II",
    "Select Division III":     "S Division III",
    "Select Division IV":      "S Division IV",
}

CLASS_ORDER = ["5A", "4A", "3A", "2A", "1A"]


def load_power_rankings(season=SEASON):
    """Load all schools from power_rankings table."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school, division, track, class_, district,
                   wins, losses, ties, games_played, power_rating, rank
            FROM power_rankings
            WHERE sport='football' AND season=?
            ORDER BY rank ASC
        """, (str(season),))
        rows = [dict(r) for r in c.fetchall()]
    except Exception as e:
        print(f"    ERROR loading power_rankings: {e}")
        rows = []
    conn.close()
    return rows


def write_rankings_tab(sheet, tab_name, schools, group_label=""):
    """Write a ranked list of schools to a tab."""
    ws = get_or_create_tab(sheet, tab_name)

    col_headers = ["Rank", "School", "Division", "Class", "District",
                   "W", "L", "Games", "Power Rating"]

    all_rows = []
    if group_label:
        all_rows.append([group_label] + [""] * 8)
    all_rows.append(col_headers)

    for rank, s in enumerate(schools, 1):
        all_rows.append([
            rank,
            s.get("school") or "",
            DIVISION_LABELS.get(s.get("division", ""), s.get("division") or ""),
            s.get("class_") or "",
            s.get("district") or "",
            s.get("wins") or 0,
            s.get("losses") or 0,
            s.get("games_played") or 0,
            round(float(s.get("power_rating") or 0), 2),
        ])

    batch_write(ws, 1, all_rows)
    print(f"    {tab_name}: {len(schools)} schools")
    return len(schools)


def build_division_tabs(sheet, season=SEASON):
    """Build 8 individual division tabs."""
    print(f"  Building division tabs...")
    all_schools = load_power_rankings(season)
    if not all_schools:
        print(f"    No data found")
        return 0

    total = 0
    for division in DIVISION_ORDER:
        tab_name = DIVISION_TAB_NAMES.get(division, division)
        schools  = [s for s in all_schools if s.get("division") == division]
        schools.sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)
        if schools:
            write_rankings_tab(sheet, tab_name, schools,
                               group_label=f"LVAY Football {season} — {tab_name}")
            total += len(schools)
        time.sleep(1)

    return total


def load_game_breakdowns(season=SEASON, sport="football"):
    """Load all per-game power point breakdowns from game_power_points table."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school, week, opponent, result, score,
                   opp_wins, opp_losses, opp_division,
                   base_pts, div_bonus, opp_quality, total_pts
            FROM game_power_points
            WHERE sport=? AND season=?
            ORDER BY school, week ASC
        """, (sport, str(season)))
        rows = c.fetchall()
    except Exception as e:
        print(f"    WARNING: game_power_points table not found: {e}")
        rows = []
    conn.close()

    # Group by school
    by_school = {}
    for r in rows:
        s = r["school"]
        if s not in by_school:
            by_school[s] = []
        by_school[s].append(dict(r))
    return by_school


def build_class_tabs(sheet, season=SEASON):
    """
    Build 5 class tabs (5A-1A).
    Each tab grouped by district, each school shown in Excel-style format:
      - School name header row
      - Summary row: Class, Division, Overall record, District record, Power Rating
      - Column headers
      - One row per game: Week, Date, H/A, Opponent (W-L), Opp Division, W/L, Score, Base, Div Bonus, OppQ, Total Pts
      - Spacer between schools
    """
    print(f"  Building class tabs (Excel style)...")
    all_schools   = load_power_rankings(season)
    game_breakdowns = load_game_breakdowns(season)

    if not all_schools:
        print(f"    No data found")
        return 0

    # Also load district W/L from games table
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school,
                SUM(CASE WHEN win_loss='W' THEN 1 ELSE 0 END) as total_wins,
                SUM(CASE WHEN win_loss='L' THEN 1 ELSE 0 END) as total_losses
            FROM games
            WHERE sport='football' AND season=? AND district_class IS NOT NULL AND district_class != ''
            GROUP BY school
        """, (str(season),))
        dist_records = {r["school"]: {"dw": r["total_wins"], "dl": r["total_losses"]} for r in c.fetchall()}
    except Exception:
        dist_records = {}

    # Load game dates and H/A from games table
    c.execute("""
        SELECT school, week, game_date, home_away, opponent, district_class
        FROM games
        WHERE sport='football' AND season=?
    """, (str(season),))
    game_details = {}
    for r in c.fetchall():
        week_num = str(r["week"] or "").replace("Week ", "").strip()
        game_details[(r["school"], week_num)] = {
            "date":    r["game_date"] or "",
            "ha":      r["home_away"] or "",
            "dist_class": r["district_class"] or "",
        }
    conn.close()

    now_str     = datetime.now().strftime("%m/%d/%Y %I:%M %p")
    game_headers = ["Week", "Date", "H/A", "Opponent", "Opp Record",
                    "Opp Division", "W/L", "Score",
                    "Base Pts", "Div Bonus", "Opp Quality", "Game Total"]

    total = 0
    for class_ in CLASS_ORDER:
        tab_name = f"Class {class_}"
        schools  = [s for s in all_schools if s.get("class_") == class_]

        if not schools:
            continue

        districts = sorted(set(
            int(s.get("district") or 0)
            for s in schools
            if s.get("district")
        ))

        ws = get_or_create_tab(sheet, tab_name)

        all_rows = []
        all_rows.append([f"LVAY Football {season} — {tab_name} — Updated {now_str}"] + [""] * 11)

        for dist in districts:
            dist_schools = [s for s in schools if int(s.get("district") or 0) == dist]
            dist_schools.sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)

            # District header
            all_rows.append([f"=== DISTRICT {dist} ==="] + [""] * 11)
            all_rows.append([""] * 12)  # spacer

            for s in dist_schools:
                name    = s.get("school") or ""
                div     = DIVISION_LABELS.get(s.get("division", ""), s.get("division") or "")
                cls     = s.get("class_") or ""
                wins    = s.get("wins") or 0
                losses  = s.get("losses") or 0
                pr      = round(float(s.get("power_rating") or 0), 2)
                dr      = dist_records.get(name, {})
                dw      = dr.get("dw", 0)
                dl      = dr.get("dl", 0)

                # School name header
                all_rows.append([name] + [""] * 11)

                # Summary row
                all_rows.append([
                    cls, div,
                    f"Overall: {wins} - {losses}", "",
                    f"District: {dw} - {dl}", "",
                    "PR:", pr,
                    "", "", "", ""
                ])

                # Column headers
                all_rows.append(game_headers)

                # Game rows
                games = game_breakdowns.get(name, [])
                for g in games:
                    week_num = str(g.get("week") or "")
                    detail   = game_details.get((name, week_num), {})
                    opp      = g.get("opponent") or ""
                    opp_w    = g.get("opp_wins") or 0
                    opp_l    = g.get("opp_losses") or 0
                    opp_div  = DIVISION_LABELS.get(g.get("opp_division", ""), g.get("opp_division") or "")

                    all_rows.append([
                        f"Wk{week_num}",
                        detail.get("date", ""),
                        detail.get("ha", ""),
                        opp,
                        f"{opp_w} - {opp_l}",
                        opp_div,
                        g.get("result") or "",
                        g.get("score") or "",
                        g.get("base_pts") or 0,
                        g.get("div_bonus") or 0,
                        g.get("opp_quality") or 0,
                        g.get("total_pts") or 0,
                    ])

                all_rows.append([""] * 12)  # spacer between schools
                total += 1

        # Schools with no district
        no_dist = [s for s in schools if not s.get("district")]
        if no_dist:
            no_dist.sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)
            all_rows.append(["=== NO DISTRICT ASSIGNED ==="] + [""] * 8)
            all_rows.append(col_headers)
            for rank, s in enumerate(no_dist, 1):
                all_rows.append([
                    rank,
                    s.get("school") or "",
                    DIVISION_LABELS.get(s.get("division", ""), s.get("division") or ""),
                    s.get("class_") or "",
                    "",
                    s.get("wins") or 0,
                    s.get("losses") or 0,
                    s.get("games_played") or 0,
                    round(float(s.get("power_rating") or 0), 2),
                ])

        batch_write(ws, 1, all_rows)
        print(f"    {tab_name}: {len(schools)} schools across {len(districts)} districts")
        total += len(schools)
        time.sleep(1)

    return total


def export_division_and_class_tabs(season=SEASON):
    """Build all 13 breakdown tabs — 8 division + 5 class."""
    print(f"\n{'='*54}")
    print(f"LVAY Football — Division & Class Tabs")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*54}")

    try:
        client = get_client()
        sheet  = client.open_by_key(SHEET_ID)
        print(f"Connected: {sheet.title}")
    except Exception as e:
        print(f"ERROR connecting: {e}")
        return False

    try:
        div_total = build_division_tabs(sheet, season)
    except Exception as e:
        print(f"  ERROR division tabs: {e}")
        div_total = 0

    try:
        class_total = build_class_tabs(sheet, season)
    except Exception as e:
        print(f"  ERROR class tabs: {e}")
        class_total = 0

    print(f"\n{'='*54}")
    print(f"DONE!")
    print(f"  Division tabs: {div_total} schools across 8 tabs")
    print(f"  Class tabs:    {class_total} schools across 5 tabs")
    print(f"Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"{'='*54}\n")
    return True
