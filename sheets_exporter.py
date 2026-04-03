"""
LVAY - Google Sheets Exporter
===============================
Writes scraped LHSAA sports data to the LVAY Sports Data 2026 Google Sheet.
Runs after every scrape cycle.

Sheet structure:
  Tab: Football Standings
  Tab: Football Scores
  Tab: Baseball Standings
  Tab: Baseball Scores
  Tab: Softball Standings
  Tab: Softball Scores
  Tab: Last Updated

Credentials loaded from environment variable GOOGLE_CREDENTIALS_JSON
(the contents of the service account JSON file).
"""

import gspread
from google.oauth2.service_account import Credentials
import sqlite3
import json
import os
from datetime import datetime

DB_PATH = "lvay_v2.db"
SHEET_ID = "1u_cJBAWTQJIAO36HZTYvPa7QfE0JoOEqx12c1U4t4mk"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def get_client():
    """Authenticate using Render secret file or environment variable fallback."""
    secret_file = "/etc/secrets/google-credentials.json"
    if os.path.exists(secret_file):
        creds = Credentials.from_service_account_file(secret_file, scopes=SCOPES)
    else:
        creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
        if not creds_json:
            raise ValueError("No Google credentials found")
        creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=SCOPES)
    return gspread.authorize(creds)


def get_or_create_tab(sheet, tab_name, headers):
    """Get existing tab or create it with headers."""
    try:
        ws = sheet.worksheet(tab_name)
        # Clear existing data but keep the sheet
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows=2000, cols=20)

    # Write headers with formatting
    ws.update("A1", [headers])
    # Bold the header row
    ws.format("A1:Z1", {
        "textFormat": {"bold": True},
        "backgroundColor": {"red": 0.1, "green": 0.2, "blue": 0.4},
        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}
    })
    return ws


def export_standings(sheet, sport):
    """Export win/loss standings for a sport."""
    tab_name = f"{sport.title()} Standings"
    headers = ["Rank", "School", "Class", "District/Class", "Wins", "Losses",
               "Games Played", "Win %", "Last Updated"]

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT
            school,
            class_,
            district_class,
            SUM(CASE WHEN win_loss='W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN win_loss IN ('L','Tie') THEN 1 ELSE 0 END) as losses,
            COUNT(*) as games_played
        FROM games
        WHERE sport=? AND win_loss IN ('W','L','Tie')
        GROUP BY school
        ORDER BY wins DESC, losses ASC
    """, (sport,))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name, headers)

    data = []
    for i, row in enumerate(rows, 1):
        games = row["wins"] + row["losses"]
        win_pct = round(row["wins"] / games, 3) if games > 0 else 0
        data.append([
            i,
            row["school"],
            row["class_"] or "",
            row["district_class"] or "",
            row["wins"],
            row["losses"],
            row["games_played"],
            win_pct,
            datetime.now().strftime("%m/%d/%Y %I:%M %p"),
        ])

    if data:
        ws.update(f"A2", data)

    print(f"  Exported {len(data)} schools to '{tab_name}'")
    return len(data)


def export_scores(sheet, sport):
    """Export all game scores for a sport."""
    tab_name = f"{sport.title()} Scores"

    if sport == "football":
        headers = ["School", "Week", "Date", "Opponent", "H/A",
                   "W/L", "Score", "Class", "District", "Out of State"]
    else:
        headers = ["School", "Date", "Opponent", "Opponent Class",
                   "H/A", "Tournament", "W/L", "Score", "District/Class"]

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT * FROM games
        WHERE sport=?
        ORDER BY school, game_date
    """, (sport,))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name, headers)

    data = []
    for row in rows:
        if sport == "football":
            data.append([
                row["school"],
                row["week"] or "",
                row["game_date"] or "",
                row["opponent"] or "",
                row["home_away"] or "",
                row["win_loss"] or "",
                row["score"] or "",
                row["class_"] or "",
                row["district"] or "",
                row["out_of_state"] or "",
            ])
        else:
            data.append([
                row["school"],
                row["game_date"] or "",
                row["opponent"] or "",
                row["opponent_class"] or "",
                row["home_away"] or "",
                row["tournament"] or "",
                row["win_loss"] or "",
                row["score"] or "",
                row["district_class"] or "",
            ])

    if data:
        # Write in batches of 500 to avoid API limits
        batch_size = 500
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            start_row = i + 2
            ws.update(f"A{start_row}", batch)

    print(f"  Exported {len(data)} games to '{tab_name}'")
    return len(data)


def update_last_updated_tab(sheet):
    """Write a summary tab showing when each sport was last updated."""
    try:
        ws = sheet.worksheet("Last Updated")
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title="Last Updated", rows=20, cols=5)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    headers = ["Sport", "Total Games", "Last Scrape", "Status"]
    ws.update("A1", [headers])
    ws.format("A1:D1", {"textFormat": {"bold": True}})

    data = []
    for sport in ["baseball", "softball", "football"]:
        c.execute("SELECT COUNT(*) as total FROM games WHERE sport=?", (sport,))
        total = c.fetchone()["total"]
        c.execute("""
            SELECT ran_at FROM scrape_log
            WHERE sport=? ORDER BY id DESC LIMIT 1
        """, (sport,))
        last = c.fetchone()
        last_scrape = last["ran_at"][:16].replace("T", " ") if last else "Never"
        status = "Active" if total > 0 else "No data yet"
        data.append([sport.title(), total, last_scrape, status])

    conn.close()
    ws.update("A2", data)
    print(f"  Updated 'Last Updated' summary tab")


def export_all_to_sheets():
    """Main export function — writes all sports data to Google Sheet."""
    print(f"\n{'='*50}")
    print(f"LVAY Google Sheets Export")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")

    try:
        client = get_client()
        sheet = client.open_by_key(SHEET_ID)
        print(f"Connected to: {sheet.title}")
    except Exception as e:
        print(f"ERROR connecting to Google Sheets: {e}")
        return False

    total = 0
    for sport in ["baseball", "softball", "football"]:
        print(f"\nExporting {sport}...")
        try:
            total += export_standings(sheet, sport)
            total += export_scores(sheet, sport)
        except Exception as e:
            print(f"  ERROR exporting {sport}: {e}")

    try:
        update_last_updated_tab(sheet)
    except Exception as e:
        print(f"  ERROR updating summary tab: {e}")

    print(f"\nDONE — {total} total rows exported to Google Sheets")
    print(f"Sheet URL: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"{'='*50}\n")
    return True


if __name__ == "__main__":
    export_all_to_sheets()
