"""
LVAY - Google Sheets Exporter
================================
Writes scraped LHSAA sports data to the LVAY Google Sheet.
NO formatting calls — pure data only for reliability.

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
    # Try secret file first (Render Secret Files)
    secret_path = "/etc/secrets/google-credentials.json"
    if os.path.exists(secret_path):
        with open(secret_path, "r") as f:
            creds_dict = json.load(f)
    else:
        # Fallback to environment variable
        creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
        if not creds_json:
            raise ValueError("No Google credentials found — checked /etc/secrets/google-credentials.json and GOOGLE_CREDENTIALS_JSON env var")
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
    """Write data in chunks to avoid Sheets API limits."""
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
        SELECT
            school, week, game_date, home_away, opponent,
            division, district, win_loss, score, power_points,
            opponent_division, opponent_wins, opponent_losses
        FROM games
        WHERE sport='football' AND season=?
        ORDER BY school, CAST(REPLACE(week,'Week ','') AS INTEGER)
    """, (season,))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name)
    ws.update("A1", [[
        "School", "Week", "Date", "H/A", "Opponent",
        "Division", "District", "W/L", "Score", "Power Pts",
        "Opp Division", "Opp W", "Opp L"
    ]])

    data = []
    for r in rows:
        data.append([
            r["school"] or "",
            r["week"] or "",
            r["game_date"] or "",
            r["home_away"] or "",
            r["opponent"] or "",
            r["division"] or "",
            r["district"] or "",
            r["win_loss"] or "",
            r["score"] or "",
            r["power_points"] if r["power_points"] is not None else "",
            r["opponent_division"] or "",
            r["opponent_wins"] if r["opponent_wins"] is not None else "",
            r["opponent_losses"] if r["opponent_losses"] is not None else "",
        ])

    if data:
        batch_write(ws, 2, data)

    print(f"    Written {len(data)} games")
    return len(data)


# ─── FOOTBALL POWER RANKINGS ──────────────────────────────────────────────────

def build_football_power_rankings(sheet, season=SEASON):
    """
    Rankings organized by division:
    NS I -> NS II -> NS III -> NS IV -> S I -> S II -> S III -> S IV
    Each division gets a label row then schools ranked by power pts.
    NO formatting calls — plain data only.
    """
    tab_name = f"Football Power Rankings ({season})"
    print(f"  Building {tab_name}...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT
            school,
            division,
            district,
            SUM(CASE WHEN win_loss='W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN win_loss='L' THEN 1 ELSE 0 END) as losses,
            SUM(CASE WHEN win_loss='Tie' THEN 1 ELSE 0 END) as ties,
            ROUND(SUM(COALESCE(power_points, 0)), 4) as total_power_pts,
            COUNT(CASE WHEN win_loss IN ('W','L','Tie') THEN 1 END) as games_played
        FROM games
        WHERE sport='football' AND season=?
        GROUP BY school
        ORDER BY total_power_pts DESC
    """, (season,))
    all_schools = [dict(r) for r in c.fetchall()]
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

    # Sort each division by power pts descending
    for div in DIVISION_ORDER:
        by_division[div].sort(key=lambda x: x["total_power_pts"] or 0, reverse=True)

    col_headers = [
        "Div Rank", "School", "Division", "District",
        "W", "L", "T", "Games", "Power Pts"
    ]
    now_str = datetime.now().strftime("%m/%d/%Y %I:%M %p")

    ws = get_or_create_tab(sheet, tab_name)

    # Build one big flat list — no API calls per row, just data
    all_rows = []
    all_rows.append([f"LVAY Football Power Rankings {season} — Updated {now_str}"] + [""] * 8)
    all_rows.append(col_headers)

    total_schools = 0
    for division in DIVISION_ORDER:
        schools = by_division[division]
        if not schools:
            continue

        # Division label row (plain text, no formatting call)
        all_rows.append([f"=== {division.upper()} ==="] + [""] * 8)

        for rank, s in enumerate(schools, 1):
            all_rows.append([
                rank,
                s["school"] or "",
                DIVISION_LABELS.get(s["division"], s["division"] or ""),
                s["district"] or "",
                s["wins"] or 0,
                s["losses"] or 0,
                s["ties"] or 0,
                s["games_played"] or 0,
                round(s["total_power_pts"] or 0, 4),
            ])
            total_schools += 1

        all_rows.append([""] * 9)  # spacer

    if unmatched:
        all_rows.append(["=== UNMATCHED / MISSING DIVISION ==="] + [""] * 8)
        for rank, s in enumerate(unmatched, 1):
            all_rows.append([
                rank,
                s["school"] or "",
                s["division"] or "UNKNOWN",
                s["district"] or "",
                s["wins"] or 0,
                s["losses"] or 0,
                s["ties"] or 0,
                s["games_played"] or 0,
                round(s["total_power_pts"] or 0, 4),
            ])
            total_schools += 1

    # One bulk write — no per-row or per-division formatting
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
               division, district, power_points
        FROM games
        WHERE sport='football' AND season=?
          AND (
            division IS NULL OR division = ''
            OR district IS NULL OR district = ''
            OR win_loss IS NULL OR win_loss = ''
            OR score IS NULL OR score = ''
            OR power_points IS NULL
          )
        ORDER BY school, week
    """, (season,))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name)
    ws.update("A1", [[
        "School", "Week", "Date", "Opponent", "W/L", "Score",
        "Division", "District", "Power Pts", "Issue"
    ]])

    data = []
    for r in rows:
        issues = []
        if not r["division"]:         issues.append("missing division")
        if not r["district"]:         issues.append("missing district")
        if not r["win_loss"]:         issues.append("missing W/L")
        if not r["score"]:            issues.append("missing score")
        if r["power_points"] is None: issues.append("missing power pts")
        data.append([
            r["school"] or "",
            r["week"] or "",
            r["game_date"] or "",
            r["opponent"] or "",
            r["win_loss"] or "",
            r["score"] or "",
            r["division"] or "",
            r["district"] or "",
            r["power_points"] if r["power_points"] is not None else "",
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
        SELECT
            school, division, district,
            SUM(CASE WHEN win_loss='W' AND district_game=1 THEN 1 ELSE 0 END) as dist_wins,
            SUM(CASE WHEN win_loss='L' AND district_game=1 THEN 1 ELSE 0 END) as dist_losses,
            SUM(CASE WHEN win_loss='W' THEN 1 ELSE 0 END) as total_wins,
            SUM(CASE WHEN win_loss='L' THEN 1 ELSE 0 END) as total_losses,
            ROUND(SUM(COALESCE(power_points,0)), 4) as power_pts
        FROM games
        WHERE sport='football' AND season=?
        GROUP BY school
        ORDER BY division, district,
                 dist_wins DESC, dist_losses ASC,
                 power_pts DESC
    """, (season,))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name)
    ws.update("A1", [[
        "School", "Division", "District",
        "Dist W", "Dist L", "Total W", "Total L", "Power Pts"
    ]])

    data = []
    for r in rows:
        data.append([
            r["school"] or "",
            DIVISION_LABELS.get(r["division"], r["division"] or ""),
            r["district"] or "",
            r["dist_wins"] or 0,
            r["dist_losses"] or 0,
            r["total_wins"] or 0,
            r["total_losses"] or 0,
            round(r["power_pts"] or 0, 4),
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
        [f"Football Scores ({SEASON})", "Every game — week, date, opponent, W/L, score, power pts"],
        [f"Football Power Rankings ({SEASON})", "304 schools ranked by power pts grouped NS I thru S IV"],
        ["Football Needs Review", "Games flagged for missing data"],
        ["Football District Records", "District W/L record per school"],
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
