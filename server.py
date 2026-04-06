"""
LVAY Scraper - API Server
"""

from flask import Flask, jsonify
import sqlite3
import os
from datetime import datetime
import threading

app = Flask(__name__)
DB_PATH = "/data/lvay_v2.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            sport         TEXT NOT NULL DEFAULT 'football',
            season        TEXT NOT NULL DEFAULT '2025',
            school        TEXT,
            week          TEXT,
            game_date     TEXT,
            opponent      TEXT,
            win_loss      TEXT,
            score         TEXT,
            home_away     TEXT,
            district_class TEXT,
            tournament    TEXT,
            scraped_at    TEXT DEFAULT (datetime('now')),
            UNIQUE(sport, season, school, week)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS oos_opponents (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            sport      TEXT NOT NULL DEFAULT 'football',
            season     TEXT NOT NULL DEFAULT '2025',
            school     TEXT NOT NULL,
            week       INTEGER NOT NULL,
            opponent   TEXT NOT NULL,
            division   TEXT NOT NULL,
            class_     TEXT DEFAULT '',
            opp_wins   INTEGER DEFAULT 0,
            opp_losses INTEGER DEFAULT 0
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS power_rankings (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            sport      TEXT NOT NULL DEFAULT 'football',
            season     TEXT NOT NULL DEFAULT '2025',
            school     TEXT NOT NULL,
            division   TEXT,
            class_     TEXT,
            district   TEXT,
            rating     REAL,
            wins       INTEGER,
            losses     INTEGER,
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(sport, season, school)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS scrape_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            ran_at     TEXT,
            sport      TEXT,
            games_found INTEGER,
            status     TEXT
        )
    """)
    conn.commit()
    conn.close()


with app.app_context():
    init_db()


# ── STATUS ──────────────────────────────────────────────────

@app.route("/")
def index():
    return jsonify({"status": "ok", "service": "LVAY Scraper API"})


@app.route("/api/status")
def status():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT sport, COUNT(*) as total FROM games GROUP BY sport")
        by_sport = {r["sport"]: r["total"] for r in c.fetchall()}
    except Exception:
        by_sport = {}
    try:
        c.execute("SELECT ran_at, sport, games_found, status FROM scrape_log ORDER BY id DESC LIMIT 5")
        recent = [dict(r) for r in c.fetchall()]
    except Exception:
        recent = []
    conn.close()
    return jsonify({
        "status":           "ok",
        "server_time":      datetime.now().isoformat(),
        "records_by_sport": by_sport,
        "total_records":    sum(by_sport.values()),
        "recent_scrapes":   recent,
    })


# ── SCRAPE TRIGGERS ─────────────────────────────────────────

@app.route("/api/scrape/football")
def scrape_football():
    def run():
        try:
            from scraper import scrape_football
            scrape_football()
        except Exception as e:
            print(f"Football scrape error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "sport": "football", "message": "Football scrape running — check /api/status in 2-3 min"})


@app.route("/api/scrape/baseball")
def scrape_baseball():
    def run():
        try:
            from scraper import scrape_baseball
            scrape_baseball()
        except Exception as e:
            print(f"Baseball scrape error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "sport": "baseball", "message": "Baseball scrape running — check /api/status in 2-3 min"})


@app.route("/api/scrape/softball")
def scrape_softball():
    def run():
        try:
            from scraper import scrape_softball
            scrape_softball()
        except Exception as e:
            print(f"Softball scrape error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "sport": "softball", "message": "Softball scrape running — check /api/status in 2-3 min"})


# ── GOOGLE SHEETS BUILD ──────────────────────────────────────

@app.route("/api/build/football-sheets")
def build_football_sheets():
    def run():
        try:
            from sheets_exporter import export_football_to_sheets
            export_football_to_sheets()
        except Exception as e:
            print(f"Sheets build error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "message": "Football sheets building — check Google Sheet in 3-5 min"})


# ── DATA FIX ENDPOINTS ───────────────────────────────────────

@app.route("/api/fix/oberlin-bolton")
def fix_oberlin_bolton():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        DELETE FROM games
        WHERE sport='football' AND season='2025'
        AND school='Oberlin' AND week='Week 10'
        AND opponent LIKE '%Bolton%'
    """)
    rows = c.rowcount
    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "rows_deleted": rows, "message": "Oberlin Week 10 Bolton bad game removed"})


@app.route("/api/fix/glenbrook-opendate")
def fix_glenbrook_opendate():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        DELETE FROM games
        WHERE sport='football' AND season='2025'
        AND school='Glenbrook' AND win_loss NOT IN ('W','L','Tie')
    """)
    rows = c.rowcount
    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "rows_deleted": rows, "message": "Glenbrook open date row removed"})


@app.route("/api/fix/stfrederick-oos")
def fix_stfrederick_oos():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT OR IGNORE INTO games
            (sport, season, school, week, game_date, opponent, win_loss, score, home_away, district_class, tournament)
            VALUES ('football','2025','St. Frederick','Week 8','2025-10-24',
                    'Cypress Christian School, TX','L','21-35','A','S3','')
        """)
        rows = c.rowcount
        conn.commit()
        conn.close()
        return jsonify({"status": "ok", "rows_inserted": rows, "message": "St. Frederick Wk8 OOS game added (S3)"})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


# ── OOS IMPORT & RANKINGS ────────────────────────────────────

@app.route("/api/import/oos2025")
def import_oos_2025():
    def run():
        try:
            from import_oos_2025 import run as do_import
            do_import()
        except Exception as e:
            print(f"OOS import error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "message": "Importing OOS opponent records — check logs"})


@app.route("/api/rankings/calculate")
def calculate_rankings():
    def run():
        try:
            from run_power_rankings import run as do_rankings
            do_rankings()
        except Exception as e:
            print(f"Rankings calc error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "message": "Power rankings calculating — check logs"})


# ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
