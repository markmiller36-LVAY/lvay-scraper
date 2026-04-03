"""
LVAY - LHSAA Football Schedule & Scores Scraper
================================================
Scrapes all football schedules and scores from lhsaaonline.org
Runs on a schedule (every 4 hours during season).
Stores data in SQLite database.
Serves data via simple JSON API for WordPress to consume.
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import json
import os
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BASE_URL = "https://www.lhsaaonline.org/pr/fbpr/admin/ReportFootballSchedule.asp"
SEASON_YEAR = "2026"  # Update each season
DB_PATH = "lvay_football.db"
OUTPUT_JSON = "public/scores.json"  # This file gets served to WordPress

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.lhsaaonline.org/pr/fbpr/admin/RptSearchFootballSchedule.asp",
    "Content-Type": "application/x-www-form-urlencoded",
}

# Form data payload - exactly as captured from browser Network tab
# Leave fields blank to get ALL schools, ALL weeks, ALL districts
BASE_PAYLOAD = {
    "y":          SEASON_YEAR,
    "resultdate": "",
    "w":          "",   # week - blank = all weeks
    "n":          "",   # school name - blank = all schools
    "d":          "",   # district - blank = all districts
    "f":          "",   # classification - blank = all
    "tbd":        "-1",
    "Submit.x":   "49",
    "Submit.y":   "8",
    "s":          "",
    "n1":         "",
    "d1":         "",
    "y1":         "",
    "paging":     "",
}

# ─── DATABASE SETUP ───────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            school      TEXT,
            week        TEXT,
            game_date   TEXT,
            opponent    TEXT,
            location    TEXT,
            class_      TEXT,
            district    TEXT,
            home_away   TEXT,
            out_of_state TEXT,
            win_loss    TEXT,
            score       TEXT,
            season      TEXT,
            scraped_at  TEXT,
            UNIQUE(school, week, season)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ran_at      TEXT,
            pages       INTEGER,
            games_found INTEGER,
            status      TEXT
        )
    """)
    conn.commit()
    conn.close()
    print("Database ready.")

# ─── SCRAPER ─────────────────────────────────────────────────────────────────

def fetch_page(page_number):
    """POST to LHSAA and get back one page of results."""
    payload = BASE_PAYLOAD.copy()
    params = {"p": str(page_number)}

    try:
        response = requests.post(
            BASE_URL,
            params=params,
            data=payload,
            headers=HEADERS,
            timeout=30
        )
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"  ERROR fetching page {page_number}: {e}")
        return None


def parse_games(html):
    """Parse the HTML table and return list of game dicts."""
    soup = BeautifulSoup(html, "html.parser")
    games = []

    # Find all data rows - LHSAA uses standard <tr> rows in tables
    # Skip header rows (they contain <th> not <td>)
    tables = soup.find_all("table")

    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            # Data rows have 11 columns based on what we saw:
            # School, Week, Date, Opponent, Location, Class,
            # District, Home/Away, Out of State, Win/Loss, Score
            if len(cells) >= 10:
                text = [c.get_text(strip=True) for c in cells]
                # Skip rows that look like headers or dividers
                if text[0] in ("School", "") and text[1] in ("Week", ""):
                    continue
                game = {
                    "school":       text[0]  if len(text) > 0  else "",
                    "week":         text[1]  if len(text) > 1  else "",
                    "game_date":    text[2]  if len(text) > 2  else "",
                    "opponent":     text[3]  if len(text) > 3  else "",
                    "location":     text[4]  if len(text) > 4  else "",
                    "class_":       text[5]  if len(text) > 5  else "",
                    "district":     text[6]  if len(text) > 6  else "",
                    "home_away":    text[7]  if len(text) > 7  else "",
                    "out_of_state": text[8]  if len(text) > 8  else "",
                    "win_loss":     text[9]  if len(text) > 9  else "",
                    "score":        text[10] if len(text) > 10 else "",
                    "season":       SEASON_YEAR,
                    "scraped_at":   datetime.now().isoformat(),
                }
                # Only save rows where school name is present
                if game["school"] and game["week"]:
                    games.append(game)

    return games


def has_next_page(html):
    """Check if there's a next page of results."""
    soup = BeautifulSoup(html, "html.parser")
    # Look for pagination links or 'next' text
    text = soup.get_text().lower()
    return "next" in text or "page" in text


def save_games(games):
    """Upsert games into SQLite database."""
    if not games:
        return 0

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    saved = 0

    for g in games:
        try:
            c.execute("""
                INSERT INTO games
                    (school, week, game_date, opponent, location, class_,
                     district, home_away, out_of_state, win_loss, score,
                     season, scraped_at)
                VALUES
                    (:school, :week, :game_date, :opponent, :location, :class_,
                     :district, :home_away, :out_of_state, :win_loss, :score,
                     :season, :scraped_at)
                ON CONFLICT(school, week, season)
                DO UPDATE SET
                    win_loss   = excluded.win_loss,
                    score      = excluded.score,
                    scraped_at = excluded.scraped_at
            """, g)
            saved += 1
        except sqlite3.Error as e:
            print(f"  DB error for {g['school']} {g['week']}: {e}")

    conn.commit()
    conn.close()
    return saved


def export_json():
    """Export all current season data to JSON for WordPress to consume."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("""
        SELECT * FROM games
        WHERE season = ?
        ORDER BY school, week
    """, (SEASON_YEAR,))

    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    # Group by school for easier WordPress display
    by_school = {}
    for row in rows:
        school = row["school"]
        if school not in by_school:
            by_school[school] = []
        by_school[school].append(row)

    output = {
        "season":      SEASON_YEAR,
        "updated_at":  datetime.now().isoformat(),
        "total_games": len(rows),
        "schools":     by_school,
    }

    os.makedirs("public", exist_ok=True)
    with open(OUTPUT_JSON, "w") as f:
        json.dump(output, f, indent=2)

    print(f"  Exported {len(rows)} games for {len(by_school)} schools → {OUTPUT_JSON}")
    return len(rows)


# ─── MAIN RUN ─────────────────────────────────────────────────────────────────

def run_scrape():
    print(f"\n{'='*50}")
    print(f"LVAY Scraper starting — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Season: {SEASON_YEAR}")
    print(f"{'='*50}")

    init_db()
    total_games = 0
    page = 1

    while True:
        print(f"\nFetching page {page}...")
        html = fetch_page(page)

        if not html:
            print("  No response — stopping.")
            break

        games = parse_games(html)
        print(f"  Found {len(games)} games on page {page}")

        if not games:
            print("  No games found — end of data.")
            break

        saved = save_games(games)
        total_games += saved
        print(f"  Saved/updated {saved} records in database")

        # Check for next page
        if has_next_page(html) and len(games) > 0:
            page += 1
            # Be polite — don't hammer the server
            import time
            time.sleep(2)
        else:
            print(f"\n  All pages processed. Total: {page} page(s)")
            break

    # Export fresh JSON for WordPress
    print(f"\nExporting JSON...")
    exported = export_json()

    # Log this run
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO scrape_log (ran_at, pages, games_found, status)
        VALUES (?, ?, ?, ?)
    """, (datetime.now().isoformat(), page, total_games, "success"))
    conn.commit()
    conn.close()

    print(f"\nDONE. {total_games} records saved. JSON exported.")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    run_scrape()
