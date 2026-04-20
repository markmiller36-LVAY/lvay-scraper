"""
LVAY - Multi-Sport LHSAA Scraper
==================================
Current supported sports:
- Football
- Baseball
- Softball

Features:
- One master scheduled run
- Per-sport ENABLED flags
- Per-sport active season windows
- Sport-specific season-year handling
- Sport-specific class lists
- SQLite storage
- Scrape logging
"""

import os
import time
import sqlite3
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/x-www-form-urlencoded",
}

CLASSIFICATIONS_BY_SPORT = {
    "football": ["1A", "2A", "3A", "4A", "5A"],
    "baseball": ["1A", "2A", "3A", "4A", "5A", "B", "C"],
    "softball": ["1A", "2A", "3A", "4A", "5A", "B", "C"],
}

SPORTS = {
    "football": {
        "enabled": os.environ.get("ENABLE_FOOTBALL", "true").lower() == "true",
        "name": "Football",
        "base_url": "https://www.lhsaaonline.org/pr/fbpr/admin/ReportFootballSchedule.asp",
        "referer": "https://www.lhsaaonline.org/pr/fbpr/admin/RptSearchFootballSchedule.asp",
        "loop_by_class": False,
        "season_mode": "calendar_year",
        "active_start": "08-01",
        "active_end": "12-31",
        "payload_template": {
            "y": "{season}",
            "resultdate": "",
            "w": "",
            "n": "",
            "d": "",
            "f": "{classification}",
            "tbd": "-1",
            "Submit.x": "49",
            "Submit.y": "8",
            "s": "",
            "n1": "",
            "d1": "",
            "y1": "",
            "paging": "",
        },
        "query_params": {"p": "1"},
    },
    "baseball": {
        "enabled": os.environ.get("ENABLE_BASEBALL", "true").lower() == "true",
        "name": "Baseball",
        "base_url": "https://www.lhsaaonline.org/pr/bpr/admin/ReportSchedule.asp",
        "referer": "https://www.lhsaaonline.org/pr/bpr/admin/SearchBaseballSchedule.asp",
        "loop_by_class": True,
        "season_mode": "school_year",
        "active_start": "01-15",
        "active_end": "05-31",
        "payload_template": {
            "y": "1",
            "resultdate": "",
            "n": "",
            "h": "",
            "d": "{classification}",
            "f": "",
            "Submit.x": "37",
            "Submit.y": "11",
            "s": "",
            "paging": "",
            "n1": "",
            "d1": "{classification}",
            "y1": "1",
        },
        "query_params": {"p": "1", "bb": "1"},
    },
    "softball": {
        "enabled": os.environ.get("ENABLE_SOFTBALL", "true").lower() == "true",
        "name": "Softball",
        "base_url": "https://www.lhsaaonline.org/pr/sbpr/admin/ReportSchedule.asp",
        "referer": "https://www.lhsaaonline.org/pr/sbpr/admin/SearchSoftballSchedule.asp",
        "loop_by_class": True,
        "season_mode": "school_year",
        "active_start": "01-15",
        "active_end": "05-31",
        "payload_template": {
            "y": "1",
            "resultdate": "",
            "n": "",
            "h": "",
            "d": "{classification}",
            "f": "",
            "Submit.x": "37",
            "Submit.y": "11",
            "s": "",
            "paging": "",
            "n1": "",
            "d1": "{classification}",
            "y1": "1",
        },
        "query_params": {"p": "1", "sb": "1"},
    },
}


def now_local() -> datetime:
    return datetime.now()


def month_day(dt: datetime) -> str:
    return dt.strftime("%m-%d")


def is_in_active_window(active_start: str, active_end: str, dt: Optional[datetime] = None) -> bool:
    dt = dt or now_local()
    today_md = month_day(dt)

    if active_start <= active_end:
        return active_start <= today_md <= active_end

    return today_md >= active_start or today_md <= active_end


def resolve_season_year(sport_key: str, dt: Optional[datetime] = None) -> str:
    dt = dt or now_local()
    config = SPORTS[sport_key]

    env_override = os.environ.get(f"{sport_key.upper()}_SEASON_YEAR")
    if env_override:
        return env_override

    mode = config["season_mode"]

    if mode == "calendar_year":
        return str(dt.year)

    if mode == "school_year":
        # Store as end year of school year (e.g. "2026" for 2025-2026)
        # Aug-Dec: current year + 1 (e.g. Aug 2025 → "2026")
        # Jan-Jul: current year (e.g. Apr 2026 → "2026")
        if dt.month >= 8:
            return str(dt.year + 1)
        else:
            return str(dt.year)

    raise ValueError(f"Unknown season_mode for {sport_key}: {mode}")


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sport TEXT NOT NULL,
            school TEXT,
            game_date TEXT,
            opponent TEXT,
            home_away TEXT,
            win_loss TEXT,
            score TEXT,
            week TEXT,
            district TEXT,
            class_ TEXT,
            district_class TEXT,
            opponent_class TEXT,
            tournament TEXT,
            tournament_host TEXT,
            out_of_state TEXT,
            location TEXT,
            season TEXT,
            scraped_at TEXT,
            UNIQUE(sport, school, game_date, opponent, season)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sport TEXT,
            ran_at TEXT,
            games_found INTEGER,
            status TEXT,
            note TEXT
        )
    """)

    conn.commit()
    conn.close()


def save_games(games):
    if not games:
        return 0

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    saved = 0

    for g in games:
        try:
            c.execute("""
                INSERT INTO games (
                    sport, school, game_date, opponent, home_away, win_loss, score,
                    week, district, class_, district_class, opponent_class, tournament,
                    tournament_host, out_of_state, location, season, scraped_at
                ) VALUES (
                    :sport, :school, :game_date, :opponent, :home_away, :win_loss, :score,
                    :week, :district, :class_, :district_class, :opponent_class, :tournament,
                    :tournament_host, :out_of_state, :location, :season, :scraped_at
                )
                ON CONFLICT(sport, school, game_date, opponent, season)
                DO UPDATE SET
                    win_loss = excluded.win_loss,
                    score = excluded.score,
                    scraped_at = excluded.scraped_at
            """, g)
            saved += 1
        except sqlite3.Error as e:
            print(f"  DB save error: {e}")

    conn.commit()
    conn.close()
    return saved


def build_payload(template, season, classification=""):
    return {
        k: v.format(season=season, classification=classification)
        for k, v in template.items()
    }


def fetch_page(sport_key, season, classification=""):
    config = SPORTS[sport_key]
    payload = build_payload(config["payload_template"], season, classification)
    headers = HEADERS.copy()
    headers["Referer"] = config["referer"]

    try:
        resp = requests.post(
            config["base_url"],
            params=config["query_params"],
            data=payload,
            headers=headers,
            timeout=45,
        )
        resp.raise_for_status()

        if "Response Buffer Limit Exceeded" in resp.text:
            print(f"  Buffer overflow: {sport_key} {classification}")
            return None

        return resp.text
    except requests.RequestException as e:
        print(f"  Fetch error ({sport_key} {classification}): {e}")
        return None


def parse_football(html, season):
    soup = BeautifulSoup(html, "html.parser")
    games = []

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 10:
                continue

            t = [c.get_text(strip=True) for c in cells]
            if not t[0] or t[0] == "School":
                continue

            games.append({
                "sport": "football",
                "school": t[0] if len(t) > 0 else "",
                "week": t[1] if len(t) > 1 else "",
                "game_date": t[2] if len(t) > 2 else "",
                "opponent": t[3] if len(t) > 3 else "",
                "location": t[4] if len(t) > 4 else "",
                "class_": t[5] if len(t) > 5 else "",
                "district": t[6] if len(t) > 6 else "",
                "home_away": t[7] if len(t) > 7 else "",
                "out_of_state": t[8] if len(t) > 8 else "",
                "win_loss": t[9] if len(t) > 9 else "",
                "score": t[10] if len(t) > 10 else "",
                "district_class": "",
                "opponent_class": "",
                "tournament": "",
                "tournament_host": "",
                "season": season,
                "scraped_at": datetime.now().isoformat(),
            })

    return games


def parse_baseball_softball(html, sport, season):
    soup = BeautifulSoup(html, "html.parser")
    games = []

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 10:
                continue

            t = [c.get_text(strip=True) for c in cells]
            if len(t) < 2:
                continue
            if t[0] in ("#", "") or not t[1] or t[1] == "School":
                continue

            games.append({
                "sport": sport,
                "school": t[1] if len(t) > 1 else "",
                "district_class": t[2] if len(t) > 2 else "",
                "game_date": t[3] if len(t) > 3 else "",
                "opponent": t[4] if len(t) > 4 else "",
                "opponent_class": t[5] if len(t) > 5 else "",
                "tournament": t[6] if len(t) > 6 else "",
                "tournament_host": t[7] if len(t) > 7 else "",
                "home_away": t[9] if len(t) > 9 else "",
                "win_loss": t[10] if len(t) > 10 else "",
                "score": t[11] if len(t) > 11 else "",
                "week": "",
                "district": "",
                "class_": "",
                "out_of_state": "",
                "location": "",
                "season": season,
                "scraped_at": datetime.now().isoformat(),
            })

    return games


def log_scrape(sport, games_found, status, note=""):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT INTO scrape_log (sport, ran_at, games_found, status, note) VALUES (?, ?, ?, ?, ?)",
        (sport, datetime.now().isoformat(), games_found, status, note)
    )
    conn.commit()
    conn.close()


def should_scrape_sport(sport_key: str, dt: Optional[datetime] = None):
    dt = dt or now_local()
    config = SPORTS[sport_key]

    if not config.get("enabled", True):
        return False, "disabled"

    active_start = config.get("active_start")
    active_end = config.get("active_end")

    if active_start and active_end:
        if not is_in_active_window(active_start, active_end, dt):
            return False, f"out_of_window ({active_start} to {active_end})"

    return True, "active"


def scrape_football():
    sport_key = "football"
    season = resolve_season_year(sport_key)

    print(f"\n--- FOOTBALL (season={season}) ---")
    html = fetch_page(sport_key, season)
    if not html:
        log_scrape(sport_key, 0, "error", "No HTML returned")
        return 0

    games = parse_football(html, season)
    print(f"  Parsed {len(games)} games")
    saved = save_games(games)
    log_scrape(sport_key, saved, "success", f"season={season}")
    return saved


def scrape_class_loop_sport(sport_key: str):
    season = resolve_season_year(sport_key)

    print(f"\n--- {sport_key.upper()} (season={season}) ---")
    total = 0

    classifications = CLASSIFICATIONS_BY_SPORT.get(sport_key, [])

    for class_ in classifications:
        print(f"  Class {class_}...")
        html = fetch_page(sport_key, season, class_)
        if not html:
            continue

        games = parse_baseball_softball(html, sport_key, season)
        print(f"    Parsed {len(games)} games")
        total += save_games(games)
        time.sleep(2)

    log_scrape(sport_key, total, "success", f"season={season}")
    return total


def scrape_sport(sport_key: str):
    config = SPORTS[sport_key]

    if config["loop_by_class"]:
        return scrape_class_loop_sport(sport_key)

    if sport_key == "football":
        return scrape_football()

    raise ValueError(f"No scraper implemented for {sport_key}")


def run_all_sports():
    now = now_local()

    print(f"\n{'=' * 60}")
    print(f"LVAY Multi-Sport Scraper — {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")

    init_db()
    total = 0

    for sport_key in SPORTS:
        do_scrape, reason = should_scrape_sport(sport_key, now)

        if not do_scrape:
            print(f"\n--- SKIPPING {sport_key.upper()} ({reason}) ---")
            log_scrape(sport_key, 0, "skipped", reason)
            continue

        total += scrape_sport(sport_key)

    print(f"\nDONE — {total} records saved/updated")
    return total


if __name__ == "__main__":
    run_all_sports()
