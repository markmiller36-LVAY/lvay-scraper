"""
LVAY - Simple JSON API Server
==============================
Serves scraped football data as JSON endpoints.
WordPress site fetches from these endpoints to display live data.
"""

from flask import Flask, jsonify, send_from_directory
import sqlite3
import json
import os
from datetime import datetime

app = Flask(__name__)
DB_PATH = "lvay_football.db"
SEASON_YEAR = os.environ.get("SEASON_YEAR", "2026")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't exist yet — safe to call multiple times."""
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


with app.app_context():
    init_db()


@app.route("/")
def index():
    return jsonify({
        "name":    "LVAY Football Data API",
        "season":  SEASON_YEAR,
        "endpoints": [
            "/api/scores",
            "/api/scores/<school_name>",
            "/api/standings",
            "/api/status",
        ]
    })


@app.route("/api/scores")
def all_scores():
    """All games for the current season — grouped by school."""
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM games
        WHERE season = ?
        ORDER BY school, game_date
    """, (SEASON_YEAR,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    by_school = {}
    for row in rows:
        school = row["school"]
        if school not in by_school:
            by_school[school] = {"games": [], "wins": 0, "losses": 0}
        by_school[school]["games"].append(row)
        if row["win_loss"] == "W":
            by_school[school]["wins"] += 1
        elif row["win_loss"] == "L":
            by_school[school]["losses"] += 1

    return jsonify({
        "season":     SEASON_YEAR,
        "updated_at": get_last_updated(),
        "schools":    by_school,
    })


@app.route("/api/scores/<path:school_name>")
def school_scores(school_name):
    """Scores for one specific school."""
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM games
        WHERE season = ? AND LOWER(school) LIKE LOWER(?)
        ORDER BY game_date
    """, (SEASON_YEAR, f"%{school_name}%"))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    if not rows:
        return jsonify({"error": "School not found"}), 404

    wins   = sum(1 for r in rows if r["win_loss"] == "W")
    losses = sum(1 for r in rows if r["win_loss"] == "L")

    return jsonify({
        "school":     rows[0]["school"],
        "season":     SEASON_YEAR,
        "record":     f"{wins}-{losses}",
        "wins":       wins,
        "losses":     losses,
        "games":      rows,
        "updated_at": get_last_updated(),
    })


@app.route("/api/standings")
def standings():
    """Win/loss standings for all schools — sorted by wins."""
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT
            school,
            class_,
            district,
            SUM(CASE WHEN win_loss = 'W' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN win_loss = 'L' THEN 1 ELSE 0 END) as losses,
            COUNT(*) as games_played
        FROM games
        WHERE season = ? AND win_loss IN ('W','L')
        GROUP BY school, class_, district
        ORDER BY wins DESC, losses ASC
    """, (SEASON_YEAR,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    return jsonify({
        "season":     SEASON_YEAR,
        "updated_at": get_last_updated(),
        "standings":  rows,
    })


@app.route("/api/status")
def status():
    """Health check — shows last scrape time and record count."""
    conn = get_db()
    c = conn.cursor()

    c.execute("SELECT COUNT(*) as total FROM games WHERE season = ?", (SEASON_YEAR,))
    total = c.fetchone()["total"]

    c.execute("""
        SELECT ran_at, games_found, status
        FROM scrape_log
        ORDER BY id DESC LIMIT 1
    """)
    last = c.fetchone()
    conn.close()

    return jsonify({
        "status":        "ok",
        "season":        SEASON_YEAR,
        "total_records": total,
        "last_scrape":   dict(last) if last else None,
        "server_time":   datetime.now().isoformat(),
    })


def get_last_updated():
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT ran_at FROM scrape_log ORDER BY id DESC LIMIT 1")
        row = c.fetchone()
        conn.close()
        return row["ran_at"] if row else None
    except:
        return None


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
