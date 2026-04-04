"""
LVAY - Google Sheets Exporter
================================
Writes scraped LHSAA sports data to the LVAY Google Sheet.

Sheet tabs built:
  Football Scores (2025)
  Football Power Rankings (2025)   ← organized by division
  Football Needs Review
  Football District Records
  Instructions

Credentials loaded from env var GOOGLE_CREDENTIALS_JSON.
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

# Division display order for Power Rankings tab
# NS = Non-Select, S = Select
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

# Short display labels for the sheet
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

# Header row background colors per division group (alternating for readability)
DIVISION_COLORS = {
    "Non-Select Division I":   {"red": 0.13, "green": 0.29, "blue": 0.53},  # dark blue
    "Non-Select Division II":  {"red": 0.18, "green": 0.40, "blue": 0.60},
    "Non-Select Division III": {"red": 0.24, "green": 0.52, "blue": 0.69},
    "Non-Select Division IV":  {"red": 0.29, "green": 0.62, "blue": 0.75},
    "Select Division I":       {"red": 0.50, "green": 0.19, "blue": 0.19},  # dark red
    "Select Division II":      {"red": 0.63, "green": 0.28, "blue": 0.21},
    "Select Division III":     {"red": 0.74, "green": 0.38, "blue": 0.27},
    "Select Division IV":      {"red": 0.82, "green": 0.49, "blue": 0.35},
}


# ─── AUTH ────────────────────────────────────────────────────────────────────

def get_client():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable not set")
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ─── TAB HELPERS ─────────────────────────────────────────────────────────────

def get_or_create_tab(sheet, tab_name, rows=3000, cols=20):
    """Get existing tab or create it. Returns cleared worksheet."""
    try:
        ws = sheet.worksheet(tab_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
    return ws


def write_header_row(ws, row_num, headers, bg_color=None):
    """Write a header row with optional background color."""
    if bg_color is None:
        bg_color = {"red": 0.13, "green": 0.29, "blue": 0.53}

    ws.update(f"A{row_num}", [headers])
    col_letter = chr(ord("A") + len(headers) - 1)
    ws.format(f"A{row_num}:{col_letter}{row_num}", {
        "backgroundColor": bg_color,
        "textFormat": {
            "bold": True,
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}
        },
        "horizontalAlignment": "CENTER"
    })


def write_division_header(ws, row_num, division_name, num_cols):
    """Write a division separator row (merged-style label)."""
    color = DIVISION_COLORS.get(division_name, {"red": 0.3, "green": 0.3, "blue": 0.3})
    label = division_name.upper()
    # Fill all columns with the label in col A, blanks in rest
    row_data = [label] + [""] * (num_cols - 1)
    ws.update(f"A{row_num}", [row_data])
    col_letter = chr(ord("A") + num_cols - 1)
    ws.format(f"A{row_num}:{col_letter}{row_num}", {
        "backgroundColor": color,
        "textFormat": {
            "bold": True,
            "fontSize": 11,
            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}
        },
        "horizontalAlignment": "LEFT"
    })


def batch_write(ws, start_row, data, chunk_size=200):
    """Write data in chunks to avoid Sheets API limits."""
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        ws.update(f"A{start_row + i}", chunk)
        if i + chunk_size < len(data):
            time.sleep(1)  # avoid 429


# ─── FOOTBALL SCORES TAB ─────────────────────────────────────────────────────

def build_football_scores(sheet, season=SEASON):
    """Write Football Scores tab — all games ordered by week."""
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

    headers = [
        "School", "Week", "Date", "H/A", "Opponent",
        "Division", "District", "W/L", "Score", "Power Pts",
        "Opp Division", "Opp W", "Opp L"
    ]

    ws = get_or_create_tab(sheet, tab_name)
    write_header_row(ws, 1, headers)

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


# ─── FOOTBALL POWER RANKINGS TAB ─────────────────────────────────────────────

def build_football_power_rankings(sheet, season=SEASON):
    """
    Write Football Power Rankings tab.
    Organized by division in this order:
      NS I → NS II → NS III → NS IV → S I → S II → S III → S IV

    Each division gets a colored header row followed by its schools
    ranked by power rating descending.
    """
    tab_name = f"Football Power Rankings ({season})"
    print(f"  Building {tab_name}...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Pull all schools with their computed power ratings
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

    # Group schools by division
    by_division = {div: [] for div in DIVISION_ORDER}
    unmatched = []

    for school in all_schools:
        div = school.get("division") or ""
        if div in by_division:
            by_division[div].append(school)
        else:
            unmatched.append(school)

    # Sort each division group by power points descending
    for div in DIVISION_ORDER:
        by_division[div].sort(key=lambda x: x["total_power_pts"] or 0, reverse=True)

    # Column headers (repeated under each division header)
    col_headers = [
        "Div Rank", "School", "Division", "District",
        "W", "L", "T", "Games", "Power Pts", "Updated"
    ]
    num_cols = len(col_headers)
    now_str = datetime.now().strftime("%m/%d/%Y %I:%M %p")

    ws = get_or_create_tab(sheet, tab_name)

    # Title row
    ws.update("A1", [[f"LVAY Football Power Rankings — {season} Season (Updated {now_str})"]])
    ws.format("A1", {
        "textFormat": {"bold": True, "fontSize": 13},
        "horizontalAlignment": "LEFT"
    })

    current_row = 2
    total_schools = 0

    for division in DIVISION_ORDER:
        schools = by_division[division]
        if not schools:
            continue  # skip empty divisions

        # Division header row
        write_division_header(ws, current_row, division, num_cols)
        current_row += 1

        # Column headers under each division
        write_header_row(ws, current_row, col_headers)
        current_row += 1

        # School rows
        data = []
        for rank, s in enumerate(schools, 1):
            data.append([
                rank,
                s["school"] or "",
                DIVISION_LABELS.get(s["division"], s["division"] or ""),
                s["district"] or "",
                s["wins"] or 0,
                s["losses"] or 0,
                s["ties"] or 0,
                s["games_played"] or 0,
                round(s["total_power_pts"] or 0, 4),
                now_str,
            ])

        if data:
            batch_write(ws, current_row, data)
            current_row += len(data)
            total_schools += len(data)

        # Blank spacer row between divisions
        current_row += 1
        time.sleep(0.5)  # be kind to Sheets API between divisions

    # Unmatched schools (bad/missing division data) at the bottom
    if unmatched:
        ws.update(f"A{current_row}", [["UNMATCHED / MISSING DIVISION"]])
        ws.format(f"A{current_row}", {"textFormat": {"bold": True}})
        current_row += 1
        write_header_row(ws, current_row, col_headers,
                         bg_color={"red": 0.5, "green": 0.5, "blue": 0.5})
        current_row += 1
        data = []
        for rank, s in enumerate(unmatched, 1):
            data.append([
                rank,
                s["school"] or "",
                s["division"] or "UNKNOWN",
                s["district"] or "",
                s["wins"] or 0,
                s["losses"] or 0,
                s["ties"] or 0,
                s["games_played"] or 0,
                round(s["total_power_pts"] or 0, 4),
                now_str,
            ])
        batch_write(ws, current_row, data)
        total_schools += len(data)

    print(f"    Written {total_schools} school rankings")
    return total_schools


# ─── NEEDS REVIEW TAB ────────────────────────────────────────────────────────

def build_needs_review(sheet, season=SEASON):
    """Flag games with missing/suspicious data."""
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

    headers = [
        "School", "Week", "Date", "Opponent", "W/L", "Score",
        "Division", "District", "Power Pts", "Issue"
    ]
    ws = get_or_create_tab(sheet, tab_name)
    write_header_row(ws, 1, headers,
                     bg_color={"red": 0.72, "green": 0.15, "blue": 0.15})

    data = []
    for r in rows:
        issues = []
        if not r["division"]:     issues.append("missing division")
        if not r["district"]:     issues.append("missing district")
        if not r["win_loss"]:     issues.append("missing W/L")
        if not r["score"]:        issues.append("missing score")
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
        print(f"    ⚠️  {len(data)} games need review")
    else:
        ws.update("A2", [["✅ No issues found!"]])
        print(f"    ✅ No issues found!")

    return len(data)


# ─── DISTRICT RECORDS TAB ────────────────────────────────────────────────────

def build_district_records(sheet, season=SEASON):
    """Write per-school district win/loss records."""
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

    headers = [
        "School", "Division", "District",
        "Dist W", "Dist L",
        "Total W", "Total L",
        "Power Pts"
    ]
    ws = get_or_create_tab(sheet, tab_name)
    write_header_row(ws, 1, headers)

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


# ─── INSTRUCTIONS TAB ────────────────────────────────────────────────────────

def build_instructions_tab(sheet):
    """Write a human-readable instructions/legend tab."""
    tab_name = "Instructions"
    print(f"  Building {tab_name}...")

    ws = get_or_create_tab(sheet, tab_name)
    now_str = datetime.now().strftime("%m/%d/%Y %I:%M %p CST")

    content = [
        ["LVAY Football Data — Google Sheet Guide"],
        [""],
        ["Last Updated:", now_str],
        ["Source:", "lhsaaonline.org — scraped automatically by lvay-scraper on Render"],
        [""],
        ["TABS IN THIS SHEET"],
        [f"Football Scores ({SEASON})", "Every game for every school — week, date, opponent, W/L, score, power pts"],
        [f"Football Power Rankings ({SEASON})", "All 304 schools ranked by power pts, organized by division (NS I → NS IV → S I → S IV)"],
        ["Football Needs Review", "Games flagged for missing division, district, score, or power pts data"],
        ["Football District Records", "Each school's district W/L record, total record, and power pts"],
        ["Instructions", "This tab — legend and guide"],
        [""],
        ["DIVISION ABBREVIATIONS"],
        ["NS I",  "Non-Select Division I   (largest non-select schools)"],
        ["NS II", "Non-Select Division II"],
        ["NS III","Non-Select Division III"],
        ["NS IV", "Non-Select Division IV  (smallest non-select schools)"],
        ["S I",   "Select Division I       (largest select/private schools)"],
        ["S II",  "Select Division II"],
        ["S III", "Select Division III"],
        ["S IV",  "Select Division IV      (smallest select/private schools)"],
        [""],
        ["POWER POINTS FORMULA (LHSAA Football)"],
        ["Win vs opponent", "Opponent's total games played × opponent win %  +  division bonus"],
        ["Loss vs opponent", "Opponent's total games played × opponent win % × 0.5  (half credit)"],
        ["Division bonus", "+2 pts per division level above you (unified I/II/III/IV regardless of track)"],
        [""],
        ["QUESTIONS?", "Contact LVAY admin — this sheet is auto-generated, do not manually edit data tabs"],
    ]

    ws.update("A1", content)
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})
    ws.format("A6", {"textFormat": {"bold": True, "fontSize": 12}})
    ws.format("A12", {"textFormat": {"bold": True, "fontSize": 12}})
    ws.format("A20", {"textFormat": {"bold": True, "fontSize": 12}})

    print(f"    Instructions tab written")


# ─── MAIN ────────────────────────────────────────────────────────────────────

def export_football_to_sheets(season=SEASON):
    """
    Main entry point — builds all football tabs in the Google Sheet.
    Called from server.py via /api/build/football-sheets
    """
    print(f"\n{'='*54}")
    print(f"LVAY Football Google Sheets Export — Season {season}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}")
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
        print(f"  Football Scores ({season})... {scores} games")
    except Exception as e:
        print(f"  ERROR building scores tab: {e}")
        scores = 0

    try:
        rankings = build_football_power_rankings(sheet, season)
        print(f"  Football Power Rankings ({season})... {rankings} schools")
    except Exception as e:
        print(f"  ERROR building rankings tab: {e}")
        rankings = 0

    try:
        flagged = build_needs_review(sheet, season)
    except Exception as e:
        print(f"  ERROR building needs-review tab: {e}")
        flagged = 0

    try:
        districts = build_district_records(sheet, season)
    except Exception as e:
        print(f"  ERROR building district records tab: {e}")
        districts = 0

    try:
        build_instructions_tab(sheet)
    except Exception as e:
        print(f"  ERROR building instructions tab: {e}")

    print(f"\n{'='*54}")
    print(f"DONE! Football {season} Google Sheets layer complete.")
    print(f"  Scores: {scores} games")
    print(f"  Rankings: {rankings} schools")
    print(f"  Needs Review: {flagged} flagged")
    print(f"  District Records: {districts} schools")
    print(f"Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"{'='*54}\n")
    return True


if __name__ == "__main__":
    export_football_to_sheets()
