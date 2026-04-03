"""
LVAY - Multi-Sport LHSAA Scraper
==================================
Handles Football, Baseball, and Softball.
- Football: one query gets all schools (manageable size)
- Baseball/Softball: loop by classification to avoid buffer overflow
Runs on a schedule. Stores to SQLite. Serves via Flask API.
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import os
import time
from datetime import datetime

DB_PATH = "lvay_sports.db"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/x-www-form-urlencoded",
}

CLASSIFICATIONS = ["1A", "2A", "3A", "4A", "5A"]

SPORTS = {
    "football": {
        "name":        "Football",
        "base_url":    "https://www.lhsaaonline.org/pr/fbpr/admin/ReportFootballSchedule.asp",
        "referer":     "https://www.lhsaaonline.org/pr/fbpr/admin/RptSearchFootballSchedule.asp",
        "season_year": os.environ.get("FOOTBALL_SEASON_YEAR", "2026"),
        "loop_by_class": False,
        "payload_template": {
            "y": "{season}", "resultdate": "", "w": "", "n": "", "d": "",
            "f": "{classification}", "tbd": "-1", "Submit.x": "49",
            "Submit.y": "8", "s": "", "n1": "", "d1": "", "y1": "", "paging": "",
        },
        "query_params": {"p": "1"},
    },
    "baseball": {
        "name":        "Baseball",
        "base_url":    "https://www.lhsaaonline.org/pr/bpr/admin/ReportSchedule.asp",
        "referer":     "https://www.lhsaaonline.org/pr/bpr/admin/SearchBaseballSchedule.asp",
        "season_year": os.environ.get("BASEBALL_SEASON_YEAR", "1"),
        "loop_by_class": True,
        "payload_template": {
            "y": "{season}", "resultdate": "", "n": "", "h": "",
            "d": "{classification}", "f": "", "Submit.x": "37", "Submit.y": "11",
            "s": "", "paging": "", "n1": "", "d1": "{classification}", "y1": "{season}",
        },
        "query_params": {"p": "1", "bb": "1"},
    },
    "softball": {
        "name":        "Softball",
        "base_url":    "https://www.lhsaaonline.org/pr/sbpr/admin/ReportSchedule.asp",
        "referer":     "https://www.lhsaaonline.org/pr/sbpr/admin/SearchSoftballSchedule.asp",
        "season_year": os.environ.get("SOFTBALL_SEASON_YEAR", "1"),
        "loop_by_class": True,
        "payload_template": {
            "y": "{season}", "resultdate": "", "n": "", "h": "",
            "d": "{classification}", "f": "", "Submit.x": "37", "Submit.y": "11",
            "s": "", "paging": "", "n1": "", "d1": "{classification}", "y1": "{season}",
        },
        "query_params": {"p": "1", "sb": "1"},
    },
}

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sport TEXT NOT NULL, school TEXT, game_date TEXT,
            opponent TEXT, home_away TEXT, win_loss TEXT, score TEXT,
            week TEXT, district TEXT, class_ TEXT, district_class TEXT,
            opponent_class TEXT, tournament TEXT, tournament_host TEXT,
            out_of_state TEXT, location TEXT, season TEXT, scraped_at TEXT,
            UNIQUE(sport, school, game_date, opponent, season)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sport TEXT, ran_at TEXT, games_found INTEGER, status TEXT, note TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_games(games, sport):
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
                DO UPDATE SET win_loss=excluded.win_loss, score=excluded.score,
                              scraped_at=excluded.scraped_at
            """, g)
            saved += 1
        except sqlite3.Error as e:
            pass
    conn.commit()
    conn.close()
    return saved

def build_payload(template, season, classification=""):
    return {k: v.format(season=season, classification=classification)
            for k, v in template.items()}

def fetch_page(sport_key, classification=""):
    config = SPORTS[sport_key]
    payload = build_payload(config["payload_template"], config["season_year"], classification)
    headers = HEADERS.copy()
    headers["Referer"] = config["referer"]
    try:
        resp = requests.post(config["base_url"], params=config["query_params"],
                             data=payload, headers=headers, timeout=45)
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
                "sport": "football", "school": t[0],
                "week": t[1] if len(t)>1 else "", "game_date": t[2] if len(t)>2 else "",
                "opponent": t[3] if len(t)>3 else "", "location": t[4] if len(t)>4 else "",
                "class_": t[5] if len(t)>5 else "", "district": t[6] if len(t)>6 else "",
                "home_away": t[7] if len(t)>7 else "", "out_of_state": t[8] if len(t)>8 else "",
                "win_loss": t[9] if len(t)>9 else "", "score": t[10] if len(t)>10 else "",
                "district_class": "", "opponent_class": "", "tournament": "",
                "tournament_host": "", "season": season, "scraped_at": datetime.now().isoformat(),
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
            if t[0] in ("#", "") or not t[1] or t[1] == "School":
                continue
            games.append({
                "sport": sport, "school": t[1] if len(t)>1 else "",
                "district_class": t[2] if len(t)>2 else "",
                "game_date": t[3] if len(t)>3 else "",
                "opponent": t[4] if len(t)>4 else "",
                "opponent_class": t[5] if len(t)>5 else "",
                "tournament": t[6] if len(t)>6 else "",
                "tournament_host": t[7] if len(t)>7 else "",
                "home_away": t[9] if len(t)>9 else "",
                "win_loss": t[10] if len(t)>10 else "",
                "score": t[11] if len(t)>11 else "",
                "week": "", "district": "", "class_": "",
                "out_of_state": "", "location": "",
                "season": season, "scraped_at": datetime.now().isoformat(),
            })
    return games

def log_scrape(sport, games_found, status, note=""):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO scrape_log (sport,ran_at,games_found,status,note) VALUES (?,?,?,?,?)",
              (sport, datetime.now().isoformat(), games_found, status, note))
    conn.commit()
    conn.close()

def scrape_football():
    print("\n--- FOOTBALL ---")
    config = SPORTS["football"]
    html = fetch_page("football")
    if not html:
        return 0
    games = parse_football(html, config["season_year"])
    print(f"  Parsed {len(games)} games")
    saved = save_games(games, "football")
    log_scrape("football", saved, "success")
    return saved

def scrape_baseball():
    print("\n--- BASEBALL ---")
    config = SPORTS["baseball"]
    total = 0
    for class_ in CLASSIFICATIONS:
        print(f"  Class {class_}...")
        html = fetch_page("baseball", class_)
        if not html:
            continue
        games = parse_baseball_softball(html, "baseball", config["season_year"])
        print(f"    {len(games)} games")
        total += save_games(games, "baseball")
        time.sleep(2)
    log_scrape("baseball", total, "success")
    return total

def scrape_softball():
    print("\n--- SOFTBALL ---")
    config = SPORTS["softball"]
    total = 0
    for class_ in CLASSIFICATIONS:
        print(f"  Class {class_}...")
        html = fetch_page("softball", class_)
        if not html:
            continue
        games = parse_baseball_softball(html, "softball", config["season_year"])
        print(f"    {len(games)} games")
        total += save_games(games, "softball")
        time.sleep(2)
    log_scrape("softball", total, "success")
    return total

def run_all_sports():
    print(f"\n{'='*50}")
    print(f"LVAY Multi-Sport Scraper — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    init_db()
    total = 0
    total += scrape_baseball()
    total += scrape_softball()
    total += scrape_football()
    print(f"\nDONE — {total} records saved/updated")
    return total

if __name__ == "__main__":
    run_all_sports()
