"""
LVAY - Google Sheets Exporter
================================
Writes LHSAA football data to the LVAY Google Sheet.
NO formatting calls — pure data only for reliability.

Real DB columns used:
  school, week, game_date, home_away, opponent,
  class_, district, district_class, win_loss, score, season

Power rankings come from power_rankings table (school, rank, power_pts, class_, district).

Tabs built:
  Football Scores (2025)
  Football Power Rankings (2025)
  Football Needs Review
  Football District Records
  Instructions
"""

import gspread
from google.oauth2.service_account import Credentials
import sqlite3
import json
import os
import time
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")
SHEET_ID = "1u_cJBAWTQJIAO36HZTYvPa7QfE0JoOEqx12c1U4t4mk"
SEASON = 2025

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Division order for rankings tab
# class_ values in DB are like "5A", "4A", "1A", "B" etc.
# and division comes from power_rankings table as "Non-Select Division I" etc.
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
    time.sleep(1)
    return ws


def batch_write(ws, start_row, data, chunk_size=250):
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        ws.update(f"A{start_row + i}", chunk)
        time.sleep(1)


# ─── FOOTBALL SCORES ──────────────────────────────────────────────────────────

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

    data = []
    for r in rows:
        data.append([
            r["school"] or "",
            r["week"] or "",
            r["game_date"] or "",
            r["home_away"] or "",
            r["opponent"] or "",
            r["class_"] or "",
            r["district"] or "",
            r["district_class"] or "",
            r["win_loss"] or "",
            r["score"] or "",
        ])

    if data:
        batch_write(ws, 2, data)

    print(f"    Written {len(data)} games")
    return len(data)


# ─── FOOTBALL POWER RANKINGS ──────────────────────────────────────────────────

def build_football_power_rankings(sheet, season=SEASON):
    """
    Pull from power_rankings table if it exists,
    otherwise fall back to computing W/L from games table grouped by school.
    Organized: NS I -> NS II -> NS III -> NS IV -> S I -> S II -> S III -> S IV
    """
    tab_name = f"Football Power Rankings ({season})"
    print(f"  Building {tab_name}...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Try power_rankings table first
    all_schools = []
    try:
        c.execute("""
            SELECT school, division, district, class_,
                   wins, losses, power_pts, rank
            FROM power_rankings
            WHERE sport='football' AND season=?
            ORDER BY power_pts DESC
        """, (str(season),))
        all_schools = [dict(r) for r in c.fetchall()]
        print(f"    Using power_rankings table ({len(all_schools)} schools)")
    except Exception:
        pass

    # Fallback: compute from games table
    if not all_schools:
        print(f"    power_rankings table not found — computing from games table")
        c.execute("""
            SELECT school, class_, district, district_class,
                SUM(CASE WHEN win_loss='W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN win_loss='L' THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN win_loss='Tie' THEN 1 ELSE 0 END) as ties,
                COUNT(CASE WHEN win_loss IN ('W','L','Tie') THEN 1 END) as games_played
            FROM games
            WHERE sport='football' AND season=?
            GROUP BY school
            ORDER BY wins DESC, losses ASC
        """, (str(season),))
        rows = c.fetchall()
        for r in rows:
            all_schools.append({
                "school": r["school"],
                "division": "",
                "district": r["district"] or r["district_class"] or "",
                "class_": r["class_"] or "",
                "wins": r["wins"] or 0,
                "losses": r["losses"] or 0,
                "ties": r["ties"] or 0,
                "games_played": r["games_played"] or 0,
                "power_pts": 0,
                "rank": 0,
            })

    conn.close()

    # Group by division
    by_division = {div: [] for div in DIVISION_ORDER}
    unmatched = []
    for school in all_schools:
        div = school.get("division") or ""
        if div in by_division:
            by_division[div].append(school)
        else:
            unmatched.append(school)

    # Sort each division by power_pts desc
    for div in DIVISION_ORDER:
        by_division[div].sort(key=lambda x: float(x.get("power_pts") or 0), reverse=True)

    now_str = datetime.now().strftime("%m/%d/%Y %I:%M %p")
    col_headers = ["Div Rank", "School", "Division", "Class", "District", "W", "L", "Games", "Power Pts"]

    ws = get_or_create_tab(sheet, tab_name)

    all_rows = []
    all_rows.append([f"LVAY Football Power Rankings {season} — Updated {now_str}"] + [""] * 8)
    all_rows.append(col_headers)

    total_schools = 0
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
                s.get("games_played") or s.get("wins", 0) + s.get("losses", 0),
                round(float(s.get("power_pts") or 0), 4),
            ])
            total_schools += 1
        all_rows.append([""] * 9)

    # Unmatched (no division set) — sort by wins
    if unmatched:
        unmatched.sort(key=lambda x: float(x.get("power_pts") or 0), reverse=True)
        all_rows.append(["=== CLASS ONLY (DIVISION NOT YET ASSIGNED) ==="] + [""] * 8)
        for rank, s in enumerate(unmatched, 1):
            all_rows.append([
                rank,
                s.get("school") or "",
                s.get("class_") or "",
                s.get("class_") or "",
                s.get("district") or "",
                s.get("wins") or 0,
                s.get("losses") or 0,
                s.get("games_played") or s.get("wins", 0) + s.get("losses", 0),
                round(float(s.get("power_pts") or 0), 4),
            ])
            total_schools += 1

    batch_write(ws, 1, all_rows)
    print(f"    Written {total_schools} school rankings")
    return total_schools


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
          AND (
            win_loss IS NULL OR win_loss = ''
            OR score IS NULL OR score = ''
            OR needs_review = 1
          )
        ORDER BY school, week
    """, (str(season),))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name)
    ws.update("A1", [[
        "School", "Week", "Date", "Opponent", "W/L", "Score",
        "Class", "District", "District/Class", "Issue"
    ]])

    data = []
    for r in rows:
        issues = []
        if not r["win_loss"]:  issues.append("missing W/L")
        if not r["score"]:     issues.append("missing score")
        if r["needs_review"]:  issues.append("flagged")
        data.append([
            r["school"] or "",
            r["week"] or "",
            r["game_date"] or "",
            r["opponent"] or "",
            r["win_loss"] or "",
            r["score"] or "",
            r["class_"] or "",
            r["district"] or "",
            r["district_class"] or "",
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
    # district games identified by tournament='D' in DB
    c.execute("""
        SELECT school, class_, district, district_class,
            SUM(CASE WHEN win_loss='W' AND tournament='D' THEN 1 ELSE 0 END) as dist_wins,
            SUM(CASE WHEN win_loss='L' AND tournament='D' THEN 1 ELSE 0 END) as dist_losses,
            SUM(CASE WHEN win_loss='W' THEN 1 ELSE 0 END) as total_wins,
            SUM(CASE WHEN win_loss='L' THEN 1 ELSE 0 END) as total_losses
        FROM games
        WHERE sport='football' AND season=?
        GROUP BY school
        ORDER BY class_, district, dist_wins DESC, dist_losses ASC
    """, (str(season),))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name)
    ws.update("A1", [[
        "School", "Class", "District", "District/Class",
        "Dist W", "Dist L", "Total W", "Total L"
    ]])

    data = []
    for r in rows:
        data.append([
            r["school"] or "",
            r["class_"] or "",
            r["district"] or "",
            r["district_class"] or "",
            r["dist_wins"] or 0,
            r["dist_losses"] or 0,
            r["total_wins"] or 0,
            r["total_losses"] or 0,
        ])

    if data:
        batch_write(ws, 2, data)

    print(f"    Written {len(data)} school district records")
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
        [f"Football Scores ({SEASON})", "Every game — week, date, opponent, W/L, score"],
        [f"Football Power Rankings ({SEASON})", "Schools grouped NS I thru S IV, ranked by power pts"],
        ["Football Needs Review", "Games flagged for missing W/L or score"],
        ["Football District Records", "District and overall W/L per school"],
        [""],
        ["DIVISION KEY", ""],
        ["NS I",   "Non-Select Division I"],
        ["NS II",  "Non-Select Division II"],
        ["NS III", "Non-Select Division III"],
        ["NS IV",  "Non-Select Division IV"],
        ["S I",    "Select Division I"],
        ["S II",   "Select Division II"],
        ["S III",  "Select Division III"],
        ["S IV",   "Select Division IV"],
    ])
    print(f"    Done")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def export_football_to_sheets(season=SEASON):
    print(f"\n{'='*54}")
    print(f"LVAY Football Google Sheets Export — Season {season}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*54}")

    try:
        client = get_client()
        sheet = client.open_by_key(SHEET_ID)
        print(f"Connected: {sheet.title}")
    except Exception as e:
        print(f"ERROR connecting to Google Sheets: {e}")
        return False

    try:
        scores = build_football_scores(sheet, season)
    except Exception as e:
        print(f"  ERROR scores tab: {e}")
        scores = 0

    try:
        rankings = build_football_power_rankings(sheet, season)
    except Exception as e:
        print(f"  ERROR rankings tab: {e}")
        rankings = 0

    try:
        flagged = build_needs_review(sheet, season)
    except Exception as e:
        print(f"  ERROR needs-review tab: {e}")
        flagged = 0

    try:
        districts = build_district_records(sheet, season)
    except Exception as e:
        print(f"  ERROR district records tab: {e}")
        districts = 0

    try:
        build_instructions_tab(sheet)
    except Exception as e:
        print(f"  ERROR instructions tab: {e}")

    print(f"\n{'='*54}")
    print(f"DONE! Football {season} Google Sheets complete.")
    print(f"  Scores:           {scores} games")
    print(f"  Rankings:         {rankings} schools")
    print(f"  Needs Review:     {flagged} flagged")
    print(f"  District Records: {districts} schools")
    print(f"Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"{'='*54}\n")
    return True


if __name__ == "__main__":
    export_football_to_sheets()
