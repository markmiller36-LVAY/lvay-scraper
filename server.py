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
DB_PATH = "lvay_sports.db"
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
@app.route("/api/scores/sport/<sport>")
def all_scores(sport=None):
    """All games grouped by school. Filter by sport: football, baseball, softball."""
    conn = get_db()
    c = conn.cursor()
    if sport:
        c.execute("SELECT * FROM games WHERE sport=? ORDER BY school, game_date", (sport,))
    else:
        c.execute("SELECT * FROM games ORDER BY sport, school, game_date")
    rows = [dict(r) for r in c.fetchall()]
    conn.close()

    by_school = {}
    for row in rows:
        key = f"{row['sport']}::{row['school']}"
        if key not in by_school:
            by_school[key] = {
                "school": row["school"], "sport": row["sport"],
                "games": [], "wins": 0, "losses": 0
            }
        by_school[key]["games"].append(row)
        if row["win_loss"] == "W":
            by_school[key]["wins"] += 1
        elif row["win_loss"] in ("L", "Tie"):
            by_school[key]["losses"] += 1

    return jsonify({
        "updated_at": get_last_updated(),
        "sport":      sport or "all",
        "schools":    list(by_school.values()),
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
@app.route("/api/standings/sport/<sport>")
def standings(sport=None):
    """Win/loss standings. Filter by sport."""
    conn = get_db()
    c = conn.cursor()
    if sport:
        c.execute("""
            SELECT sport, school, class_, district_class,
                SUM(CASE WHEN win_loss='W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN win_loss IN ('L','Tie') THEN 1 ELSE 0 END) as losses,
                COUNT(*) as games_played
            FROM games WHERE sport=? AND win_loss IN ('W','L','Tie')
            GROUP BY school ORDER BY wins DESC, losses ASC
        """, (sport,))
    else:
        c.execute("""
            SELECT sport, school, class_, district_class,
                SUM(CASE WHEN win_loss='W' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN win_loss IN ('L','Tie') THEN 1 ELSE 0 END) as losses,
                COUNT(*) as games_played
            FROM games WHERE win_loss IN ('W','L','Tie')
            GROUP BY sport, school ORDER BY sport, wins DESC
        """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({
        "updated_at": get_last_updated(),
        "sport":      sport or "all",
        "standings":  rows,
    })


@app.route("/api/status")
def status():
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT sport, COUNT(*) as total FROM games GROUP BY sport")
    by_sport = {r["sport"]: r["total"] for r in c.fetchall()}
    c.execute("SELECT ran_at, sport, games_found, status FROM scrape_log ORDER BY id DESC LIMIT 5")
    recent = [dict(r) for r in c.fetchall()]
    conn.close()
    return jsonify({
        "status":       "ok",
        "server_time":  datetime.now().isoformat(),
        "records_by_sport": by_sport,
        "total_records": sum(by_sport.values()),
        "recent_scrapes": recent,
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
