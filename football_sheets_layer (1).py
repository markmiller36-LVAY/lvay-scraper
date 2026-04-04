"""
LVAY - Football 2025 Google Sheets Layer
==========================================
Builds the complete editorial layer for 2025 football in Google Sheets.

Tabs created:
  - Football Scores (2025)      — all games, editable, color coded
  - Football Power Rankings (2025) — calculated rankings
  - Football Needs Review       — flagged score/WL mismatches
  - Football District Records   — district W/L per school

The Sheets layer sits between the database and WordPress.
WordPress reads from Sheets, not directly from the database.
"""

import sqlite3
import os
import time
from datetime import datetime

DB_PATH = "/data/lvay_v2.db"
SHEET_ID = "1u_cJBAWTQJIAO36HZTYvPa7QfE0JoOEqx12c1U4t4mk"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# LVAY teal color
TEAL = {"red": 0.02, "green": 0.49, "blue": 0.49}
WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}
GREEN = {"red": 0.82, "green": 0.94, "blue": 0.82}
RED   = {"red": 0.98, "green": 0.82, "blue": 0.82}
YELLOW= {"red": 1.0,  "green": 1.0,  "blue": 0.8}


def get_client():
    import gspread
    from google.oauth2.service_account import Credentials
    secret_file = "/etc/secrets/google-credentials.json"
    creds = Credentials.from_service_account_file(secret_file, scopes=SCOPES)
    return gspread.authorize(creds)


def get_or_create_tab(sheet, tab_name, rows=5000, cols=20):
    import gspread
    try:
        ws = sheet.worksheet(tab_name)
        ws.clear()
        time.sleep(1)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
        time.sleep(1)
    return ws


def load_football_games():
    """Load all 2025 football games from database."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT 
            school, week, game_date, opponent,
            class_, district, home_away, out_of_state,
            win_loss, score, needs_review
        FROM games
        WHERE sport='football' AND season='2025'
        ORDER BY class_, district, school, week
    """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def load_school_profiles():
    """Load school division data."""
    try:
        from school_database import SCHOOLS
        return SCHOOLS
    except ImportError:
        try:
            import sys
            sys.path.insert(0, '/opt/render/project/src')
            from school_database import SCHOOLS
            return SCHOOLS
        except:
            print("  WARNING: school_database not found - division/class data will be empty")
            return {}


def calculate_records(games):
    """Calculate overall and district records for each school."""
    records = {}
    for g in games:
        school = g["school"]
        if school not in records:
            records[school] = {
                "wins": 0, "losses": 0,
                "dist_wins": 0, "dist_losses": 0,
                "games": []
            }
        wl = g["win_loss"].strip()
        is_district = g["district"] and g["district"].strip() not in ("", "None")

        if wl == "W":
            records[school]["wins"] += 1
            if is_district:
                records[school]["dist_wins"] += 1
        elif wl == "L":
            records[school]["losses"] += 1
            if is_district:
                records[school]["dist_losses"] += 1

        records[school]["games"].append(g)
    return records


def calculate_power_ratings(games, profiles):
    """Calculate football power ratings using LHSAA formula."""
    from power_rating_engine import (
        PowerRatingEngine, Team, GameResult, DIVISION_RANK
    )

    # Build school records for opponent quality
    records = calculate_records(games)

    engine = PowerRatingEngine()

    # Add all teams
    schools_in_data = set(g["school"] for g in games)
    for school in schools_in_data:
        profile = profiles.get(school, {})
        division = profile.get("division", "Non-Select Division III")
        class_ = profile.get("class", "3A") or "3A"
        engine.add_team(Team(
            name=school,
            division=division,
            classification=class_,
            sport="football",
        ))

    # Add all games
    for g in games:
        school = g["school"]
        opponent = g["opponent"]
        wl = g["win_loss"].strip()

        if wl not in ("W", "L", "T"):
            continue

        opp_record = records.get(opponent, {"wins": 0, "losses": 0})
        opp_profile = profiles.get(opponent, {})
        opp_division = opp_profile.get("division", "Non-Select Division III")

        # Out of state if not in our school list
        oos = opponent not in schools_in_data

        # Get week number
        week_str = g.get("week", "Week 1")
        try:
            week_num = int(week_str.replace("Week", "").strip())
        except:
            week_num = 1

        engine.add_game(GameResult(
            team=school,
            opponent=opponent,
            result=wl,
            sport="football",
            opponent_wins=opp_record["wins"],
            opponent_losses=opp_record["losses"],
            opponent_division=opp_division,
            opponent_out_of_state=oos,
            week=week_num,
        ))

    return engine.rate_all()


def validate_scores(games):
    """Flag games where score doesn't match W/L."""
    flagged = []
    for g in games:
        score = g.get("score", "").strip()
        wl = g.get("win_loss", "").strip()

        if not score or score == "-" or not wl:
            continue

        try:
            parts = score.split("-")
            if len(parts) != 2:
                continue
            t1 = int(parts[0].strip())
            t2 = int(parts[1].strip())

            # Score says lose but W/L says Win
            if t1 < t2 and wl == "W":
                flagged.append({**g, "issue": f"Score {score} suggests L but marked W"})
            # Score says win but W/L says Lose
            elif t1 > t2 and wl == "L":
                flagged.append({**g, "issue": f"Score {score} suggests W but marked L"})
        except:
            continue

    return flagged


# ─── TAB BUILDERS ─────────────────────────────────────────────────────────────

def build_scores_tab(sheet, games):
    """Build Football Scores (2025) tab."""
    print("  Building Football Scores (2025)...")
    ws = get_or_create_tab(sheet, "Football Scores (2025)", rows=5000)

    headers = [
        "School", "Class", "District", "Week", "Date",
        "H/A", "Opponent", "W/L", "Score", "OOS", "Needs Review"
    ]
    ws.update("A1", [headers])
    ws.format("A1:K1", {
        "textFormat": {"bold": True, "foregroundColor": WHITE},
        "backgroundColor": TEAL,
    })
    time.sleep(2)

    data = []
    for g in games:
        data.append([
            g["school"],
            g["class_"] or "",
            g["district"] or "",
            g["week"] or "",
            g["game_date"] or "",
            g["home_away"] or "",
            g["opponent"] or "",
            g["win_loss"] or "",
            g["score"] or "",
            "OOS" if g["out_of_state"] else "",
            "⚠️ Review" if g["needs_review"] else "",
        ])

    # Write in batches
    batch_size = 500
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        ws.update(f"A{i+2}", batch)
        if i + batch_size < len(data):
            time.sleep(2)

    print(f"    Written {len(data)} games")
    time.sleep(3)
    return len(data)


def build_power_rankings_tab(sheet, ratings, records, profiles):
    """Build Football Power Rankings (2025) tab."""
    print("  Building Football Power Rankings (2025)...")
    ws = get_or_create_tab(sheet, "Football Power Rankings (2025)", rows=500)

    headers = [
        "Rank", "School", "Class", "District", "Division",
        "Power Rating", "Overall W", "Overall L",
        "District W", "District L", "Games Played", "Last Updated"
    ]
    ws.update("A1", [headers])
    ws.format("A1:L1", {
        "textFormat": {"bold": True, "foregroundColor": WHITE},
        "backgroundColor": TEAL,
    })
    time.sleep(2)

    now = datetime.now().strftime("%m/%d/%Y %I:%M %p")
    data = []
    for r in ratings:
        rec = records.get(r.name, {})
        profile = profiles.get(r.name, {})
        data.append([
            r.rank,
            r.name,
            profile.get("class", ""),
            profile.get("district", ""),
            r.division,
            r.power_rating,
            rec.get("wins", r.wins),
            rec.get("losses", r.losses),
            rec.get("dist_wins", 0),
            rec.get("dist_losses", 0),
            r.games_played,
            now,
        ])

    if data:
        ws.update("A2", data)

    print(f"    Written {len(data)} school rankings")
    time.sleep(3)
    return len(data)


def build_needs_review_tab(sheet, flagged):
    """Build Football Needs Review tab."""
    print("  Building Football Needs Review tab...")
    ws = get_or_create_tab(sheet, "Football Needs Review", rows=200)

    headers = [
        "School", "Week", "Opponent", "W/L", "Score", "Issue", "Fixed?"
    ]
    ws.update("A1", [headers])
    ws.format("A1:G1", {
        "textFormat": {"bold": True, "foregroundColor": WHITE},
        "backgroundColor": {"red": 0.8, "green": 0.2, "blue": 0.2},
    })
    time.sleep(2)

    if flagged:
        data = [[
            g["school"], g["week"], g["opponent"],
            g["win_loss"], g["score"], g["issue"], ""
        ] for g in flagged]
        ws.update("A2", data)
        # Highlight flagged rows yellow
        ws.format(f"A2:G{len(data)+1}", {"backgroundColor": YELLOW})
        print(f"    ⚠️  {len(flagged)} games need review!")
    else:
        ws.update("A2", [["✅ No issues found — all scores match W/L"]])
        print("    ✅ No issues found!")

    time.sleep(2)
    return len(flagged)


def build_district_records_tab(sheet, records, profiles):
    """Build Football District Records tab."""
    print("  Building Football District Records tab...")
    ws = get_or_create_tab(sheet, "Football District Records (2025)", rows=500)

    headers = [
        "School", "Class", "District", "Division",
        "District W", "District L", "District Pct",
        "Overall W", "Overall L", "Overall Pct"
    ]
    ws.update("A1", [headers])
    ws.format("A1:J1", {
        "textFormat": {"bold": True, "foregroundColor": WHITE},
        "backgroundColor": TEAL,
    })
    time.sleep(2)

    data = []
    for school, rec in sorted(records.items()):
        profile = profiles.get(school, {})
        dw = rec["dist_wins"]
        dl = rec["dist_losses"]
        ow = rec["wins"]
        ol = rec["losses"]
        dist_pct = round(dw / (dw + dl), 3) if (dw + dl) > 0 else 0
        ovr_pct  = round(ow / (ow + ol), 3) if (ow + ol) > 0 else 0
        data.append([
            school,
            profile.get("class", ""),
            profile.get("district", ""),
            profile.get("division", ""),
            dw, dl, dist_pct,
            ow, ol, ovr_pct,
        ])

    # Sort by class, district, district pct
    data.sort(key=lambda x: (x[1], x[2], -x[6]))

    if data:
        ws.update("A2", data)

    print(f"    Written {len(data)} school district records")
    time.sleep(3)
    return len(data)


def add_apps_script_instructions(sheet):
    """
    Add instructions tab explaining how to add the
    Recalculate & Publish Apps Script button.
    """
    import gspread
    try:
        ws = sheet.worksheet("⚙️ Instructions")
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title="⚙️ Instructions", rows=50, cols=5)

    time.sleep(1)
    instructions = [
        ["LVAY Google Sheets — Instructions"],
        [""],
        ["HOW TO ADD THE RECALCULATE & PUBLISH BUTTON:"],
        ["1. Click Extensions → Apps Script"],
        ["2. Delete any existing code"],
        ["3. Paste the Apps Script code from the LVAY GitHub repo"],
        ["4. Save and run — authorize when prompted"],
        ["5. A 'LVAY Tools' menu will appear in your Sheets toolbar"],
        [""],
        ["HOW TO FIX A SCORE ERROR:"],
        ["1. Find the game in Football Scores (2025) tab"],
        ["2. Fix the W/L and/or Score columns directly"],
        ["3. Click LVAY Tools → Recalculate & Publish"],
        ["4. Power Rankings update automatically"],
        ["5. WordPress pulls the updated data within minutes"],
        [""],
        ["HOW TO READ THE NEEDS REVIEW TAB:"],
        ["- Yellow rows = score doesn't match W/L"],
        ["- Fix in Football Scores tab"],
        ["- Run Recalculate & Publish"],
        ["- Mark as Fixed in the Fixed? column"],
        [""],
        ["TABS EXPLAINED:"],
        ["Football Scores (2025)        — All 2,997 games. Edit here to fix errors."],
        ["Football Power Rankings (2025) — Calculated by LVAY engine. Do not edit."],
        ["Football Needs Review          — Auto-flagged score/WL mismatches."],
        ["Football District Records      — District W/L standings by school."],
    ]
    ws.update("A1", instructions)
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})
    ws.format("A3", {"textFormat": {"bold": True}})
    print("  Built Instructions tab")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def build_football_sheets_layer():
    """Build complete Google Sheets layer for 2025 football."""
    print(f"\n{'='*55}")
    print(f"LVAY Football 2025 — Google Sheets Layer Builder")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*55}")

    # Connect to Sheets
    client = get_client()
    sheet  = client.open_by_key(SHEET_ID)
    print(f"Connected to: {sheet.title}")

    # Load data
    print("\nLoading data from database...")
    games    = load_football_games()
    profiles = load_school_profiles()
    print(f"  {len(games)} games loaded")
    print(f"  {len(profiles)} school profiles loaded")

    # Calculate
    print("\nCalculating...")
    records = calculate_records(games)
    print(f"  Records calculated for {len(records)} schools")

    flagged = validate_scores(games)
    print(f"  Score validation: {len(flagged)} games flagged")

    print("\nCalculating power ratings...")
    try:
        ratings = calculate_power_ratings(games, profiles)
        print(f"  Power ratings calculated for {len(ratings)} schools")
    except Exception as e:
        print(f"  Power rating error: {e}")
        ratings = []

    # Build tabs
    print("\nBuilding Google Sheets tabs...")
    build_scores_tab(sheet, games)
    build_power_rankings_tab(sheet, ratings, records, profiles)
    build_needs_review_tab(sheet, flagged)
    build_district_records_tab(sheet, records, profiles)
    add_apps_script_instructions(sheet)

    print(f"\n{'='*55}")
    print(f"DONE! Football 2025 Google Sheets layer complete.")
    print(f"Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"{'='*55}\n")
    return True


if __name__ == "__main__":
    build_football_sheets_layer()
