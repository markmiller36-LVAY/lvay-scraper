"""
LVAY - Football 2025 Data Import Script
=========================================
Run this directly on the Render shell to import
2025 football season data into the database.

Usage:
  python3 import_football_2025.py

This imports 2,999 games from the 2025 season
across all 5 classes (1A-5A) and all 10 weeks.
"""

import sqlite3
import json
import os
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")

# All 2,999 2025 football games extracted from LAVAY_FOOTBALL_2025 Excel workbook
GAMES = [
{"school":"Acadiana","week":"Week 1","game_date":"2025-09-05","opponent":"Ruston","class_":"5A","district":"2 ","home_away":"H","out_of_state":"","win_loss":"L","score":"7-49","sport":"football","season":"2025","needs_review":False},
{"school":"Airline","week":"Week 1","game_date":"2025-09-05","opponent":"Barbe","class_":"5A","district":"3 ","home_away":"H","out_of_state":"","win_loss":"W","score":"56-27","sport":"football","season":"2025","needs_review":False},
{"school":"Alexandria","week":"Week 1","game_date":"2025-09-05","opponent":"West Feliciana","class_":"4A","district":"6 ","home_away":"H","out_of_state":"","win_loss":"W","score":"54-7","sport":"football","season":"2025","needs_review":False},
{"school":"Archbishop Rummel","week":"Week 1","game_date":"2025-09-05","opponent":"Lafayette Christian","class_":"2A","district":"6 ","home_away":"A","out_of_state":"","win_loss":"L","score":"26-27","sport":"football","season":"2025","needs_review":False},
]

# NOTE: Full dataset is loaded from the JSON file
# This file contains a sample — the real import reads from football_2025_clean.json


def ensure_columns(conn):
    """Add any missing columns to the games table."""
    c = conn.cursor()
    for col, type_ in [
        ("season", "TEXT"),
        ("needs_review", "INTEGER"),
        ("week", "TEXT"),
        ("district", "TEXT"),
    ]:
        try:
            c.execute(f"ALTER TABLE games ADD COLUMN {col} {type_} DEFAULT ''")
            print(f"  Added column: {col}")
        except sqlite3.OperationalError:
            pass
    conn.commit()


def import_games(games: list) -> dict:
    """Import games into database."""
    conn = sqlite3.connect(DB_PATH)
    ensure_columns(conn)
    c = conn.cursor()
    
    saved = skipped = 0
    now = datetime.now().isoformat()
    
    for game in games:
        wl = game.get("win_loss", "")
        if wl in ("Cancelled", "PPD", "", "None"):
            skipped += 1
            continue
        
        try:
            c.execute("""
                INSERT OR IGNORE INTO games 
                    (school, week, game_date, opponent, class_, district,
                     home_away, out_of_state, win_loss, score, sport,
                     season, needs_review, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                game["school"].strip(),
                game.get("week", ""),
                game.get("game_date", ""),
                game["opponent"].strip(),
                game.get("class_", "").strip(),
                game.get("district", "").strip(),
                game.get("home_away", ""),
                game.get("out_of_state", ""),
                wl.strip(),
                game.get("score", ""),
                "football",
                "2025",
                1 if game.get("needs_review") else 0,
                now,
            ))
            saved += 1
        except sqlite3.Error as e:
            print(f"  DB error on {game['school']} {game.get('week')}: {e}")
    
    conn.commit()
    
    # Verify
    c.execute("SELECT COUNT(*) FROM games WHERE sport='football' AND season='2025'")
    total = c.fetchone()[0]
    conn.close()
    
    return {"saved": saved, "skipped": skipped, "total_in_db": total}


def main():
    print(f"\n{'='*50}")
    print(f"LVAY Football 2025 Data Import")
    print(f"DB: {DB_PATH}")
    print(f"{'='*50}")
    
    # Load from JSON file if available
    json_path = os.path.join(os.path.dirname(__file__), "football_2025_clean.json")
    if os.path.exists(json_path):
        with open(json_path) as f:
            games = json.load(f)
        print(f"Loaded {len(games)} games from {json_path}")
    else:
        games = GAMES
        print(f"Using embedded sample data ({len(games)} games)")
    
    result = import_games(games)
    
    print(f"\nResults:")
    print(f"  Saved:          {result['saved']}")
    print(f"  Skipped:        {result['skipped']}")
    print(f"  Total in DB:    {result['total_in_db']}")
    print(f"\nDone!")


if __name__ == "__main__":
    main()
