"""
import_oos_baseball_2026.py
============================
Pulls OOS opponent win/loss records from the Google Sheet tab
"Baseball OOS Opponents (2026)" and stores them in the oos_opponents
table for use by the power rating engine.

Sheet columns: school | opponent | opp_wins | opp_losses

Run order:
  1. scraper (already done)
  2. THIS script
  3. run_power_rankings.py (sport=baseball, season=2026)
"""

import os
import sqlite3
import gspread
from google.oauth2.service_account import Credentials

DB_PATH     = os.environ.get("DB_PATH", "/data/lvay_v2.db")
SHEET_ID    = "1u_cJBAWTQJIAO36HZTYvPa7QfE0JoOEqx12c1U4t4mk"
TAB_NAME    = "Baseball OOS Opponents (2026)"
SPORT       = "baseball"
SEASON      = "2026"
CREDS_PATH  = os.environ.get("GOOGLE_CREDENTIALS_PATH", "/etc/secrets/google-credentials.json")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]


def get_sheet_data():
    creds  = Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(SHEET_ID).worksheet(TAB_NAME)
    rows   = sheet.get_all_records()  # returns list of dicts keyed by header row
    return rows


def run():
    print(f"\n--- OOS Baseball Import (season={SEASON}) ---")

    try:
        rows = get_sheet_data()
    except Exception as e:
        print(f"  Sheet read error: {e}")
        return

    print(f"  {len(rows)} rows read from sheet")

    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()

    # Ensure table exists
    c.execute("""
        CREATE TABLE IF NOT EXISTS oos_opponents (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            sport      TEXT NOT NULL,
            season     TEXT NOT NULL,
            school     TEXT NOT NULL,
            opponent   TEXT NOT NULL,
            opp_wins   INTEGER DEFAULT 0,
            opp_losses INTEGER DEFAULT 0,
            UNIQUE(sport, season, school, opponent)
        )
    """)

    inserted = 0
    updated  = 0
    skipped  = 0

    for row in rows:
        school     = str(row.get("school", "")).strip()
        opponent   = str(row.get("opponent", "")).strip()
        opp_wins   = row.get("opp_wins", "")
        opp_losses = row.get("opp_losses", "")

        # Skip blank or incomplete rows
        if not school or not opponent:
            skipped += 1
            continue
        if opp_wins == "" or opp_losses == "":
            skipped += 1
            continue

        try:
            opp_wins   = int(opp_wins)
            opp_losses = int(opp_losses)
        except (ValueError, TypeError):
            print(f"  Skipping bad record: {school} | {opponent} | {opp_wins} | {opp_losses}")
            skipped += 1
            continue

        c.execute("""
            INSERT INTO oos_opponents (sport, season, school, opponent, opp_wins, opp_losses)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(sport, season, school, opponent)
            DO UPDATE SET
                opp_wins   = excluded.opp_wins,
                opp_losses = excluded.opp_losses
        """, (SPORT, SEASON, school, opponent, opp_wins, opp_losses))

        if c.rowcount == 1:
            inserted += 1
        else:
            updated += 1

    conn.commit()
    conn.close()

    print(f"  Inserted: {inserted} | Updated: {updated} | Skipped: {skipped}")
    print(f"  Done.")


if __name__ == "__main__":
    run()
