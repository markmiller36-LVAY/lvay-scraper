"""
LVAY - Multi-Sport JSON API Server
====================================
Serves scraped LHSAA sports data to WordPress.
Imports init_db from scraper.py to ensure consistent schema.
"""

from flask import Flask, jsonify
import sqlite3
import os
from datetime import datetime

app = Flask(__name__)
DB_PATH = "/data/lvay_v2.db"
SEASON_YEAR = os.environ.get("SEASON_YEAR", "2026")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create all tables with correct multi-sport schema."""
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


with app.app_context():
    init_db()


@app.route("/")
def index():
    return jsonify({
        "name": "LVAY Multi-Sport Data API",
        "endpoints": [
            "/api/status",
            "/api/scores",
            "/api/scores/sport/baseball",
            "/api/scores/sport/softball",
            "/api/scores/sport/football",
            "/api/scores/<school_name>",
            "/api/standings",
            "/api/standings/sport/baseball",
            "/api/standings/sport/softball",
            "/api/standings/sport/football",
        ]
    })


@app.route("/api/scores")
@app.route("/api/scores/sport/<sport>")
def all_scores(sport=None):
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
                "school": row["school"],
                "sport":  row["sport"],
                "games":  [],
                "wins":   0,
                "losses": 0,
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
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM games
        WHERE LOWER(school) LIKE LOWER(?)
        ORDER BY sport, game_date
    """, (f"%{school_name}%",))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    if not rows:
        return jsonify({"error": "School not found"}), 404
    wins   = sum(1 for r in rows if r["win_loss"] == "W")
    losses = sum(1 for r in rows if r["win_loss"] in ("L", "Tie"))
    return jsonify({
        "school":     rows[0]["school"],
        "record":     f"{wins}-{losses}",
        "wins":       wins,
        "losses":     losses,
        "games":      rows,
        "updated_at": get_last_updated(),
    })


@app.route("/api/standings")
@app.route("/api/standings/sport/<sport>")
def standings(sport=None):
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
    try:
        c.execute("SELECT sport, COUNT(*) as total FROM games GROUP BY sport")
        by_sport = {r["sport"]: r["total"] for r in c.fetchall()}
    except sqlite3.OperationalError:
        by_sport = {}
    try:
        c.execute("SELECT ran_at, sport, games_found, status FROM scrape_log ORDER BY id DESC LIMIT 5")
        recent = [dict(r) for r in c.fetchall()]
    except sqlite3.OperationalError:
        recent = []
    conn.close()
    return jsonify({
        "status":           "ok",
        "server_time":      datetime.now().isoformat(),
        "records_by_sport": by_sport,
        "total_records":    sum(by_sport.values()),
        "recent_scrapes":   recent,
    })


def get_last_updated():
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT ran_at FROM scrape_log ORDER BY id DESC LIMIT 1")
        row = c.fetchone()
        conn.close()
        return row["ran_at"] if row else None
    except Exception:
        return None


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)


@app.route("/api/scrape/now")
def scrape_now():
    """Trigger a full scrape in background — returns immediately."""
    import threading
    from scraper import run_all_sports
    from sheets_exporter import export_football_to_sheets  # ← fixed
    def run():
        try:
            run_all_sports()
            export_football_to_sheets()
        except Exception as e:
            print(f"Background scrape error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "message": "Full scrape running in background — check /api/status in 3-5 minutes"})


@app.route("/api/scrape/baseball")
def scrape_baseball_now():
    """Trigger baseball scrape in background — returns immediately."""
    import threading
    from scraper import scrape_baseball
    from sheets_exporter import export_football_to_sheets  # ← fixed
    def run():
        try:
            scrape_baseball()
            export_football_to_sheets()
        except Exception as e:
            print(f"Background baseball error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "sport": "baseball", "message": "Baseball scrape running in background — check /api/status in 2-3 minutes"})


@app.route("/api/scrape/softball")
def scrape_softball_now():
    """Trigger softball scrape in background — returns immediately."""
    import threading
    from scraper import scrape_softball
    from sheets_exporter import export_football_to_sheets  # ← fixed
    def run():
        try:
            scrape_softball()
            export_football_to_sheets()
        except Exception as e:
            print(f"Background softball error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "sport": "softball", "message": "Softball scrape running in background — check /api/status in 2-3 minutes"})


@app.route("/api/scrape/ratings")
def scrape_ratings_now():
    """Trigger official power ratings PDF scrape in background."""
    import threading
    from pdf_scraper import scrape_latest_ratings
    def run():
        try:
            scrape_latest_ratings()
        except Exception as e:
            print(f"PDF scrape error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "message": "PDF ratings scrape running in background"})


@app.route("/api/ratings")
def get_ratings():
    """Get official power ratings from database."""
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT * FROM official_power_ratings
            WHERE week = (SELECT MAX(week) FROM official_power_ratings)
            ORDER BY track, division, rank
        """)
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return jsonify({"ratings": rows, "total": len(rows)})
    except sqlite3.OperationalError:
        conn.close()
        return jsonify({"ratings": [], "total": 0, "note": "No ratings scraped yet"})


@app.route("/api/rankings/calculate")
def calculate_rankings():
    """Trigger power rankings calculation in background."""
    import threading
    from run_power_rankings import run_power_rankings
    def run():
        try:
            run_power_rankings()
        except Exception as e:
            print(f"Rankings calc error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({
        "status": "started",
        "message": "Power rankings calculating in background — check sheet in 3-5 minutes"
    })


@app.route("/api/rankings/<sport>")
def get_rankings(sport):
    """Get calculated power rankings from database."""
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT * FROM power_rankings
            WHERE sport=?
            ORDER BY rank ASC
        """, (sport,))
        rows = [dict(r) for r in c.fetchall()]
        conn.close()
        return jsonify({"sport": sport, "rankings": rows, "total": len(rows)})
    except sqlite3.OperationalError:
        conn.close()
        return jsonify({"sport": sport, "rankings": [], "note": "No rankings calculated yet"})


@app.route("/api/import/football2025")
def import_football_2025():
    """Import 2025 football season data from Excel into database."""
    import threading
    def run():
        try:
            from import_football_2025 import main
            main()
        except Exception as e:
            print(f"Import error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({
        "status": "started",
        "message": "Football 2025 import running — check /api/status in 30 seconds"
    })


@app.route("/api/build/football-sheets")
def build_football_sheets():
    """Build complete Football 2025 Google Sheets layer."""
    import threading
    from sheets_exporter import export_football_to_sheets  # ← fixed
    def run():
        try:
            export_football_to_sheets()
        except Exception as e:
            print(f"Football sheets error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({
        "status": "started",
        "message": "Building Football 2025 Google Sheets — check sheet in 5 minutes"
    })


@app.route("/api/build/football-scores")
def build_football_scores_tab():
    """Build Football Scores tab separately — slow, 2997 rows."""
    import threading
    from sheets_exporter import export_football_scores
    def run():
        try:
            export_football_scores()
        except Exception as e:
            print(f"Football scores error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({
        "status": "started",
        "message": "Building Football Scores tab — this takes 5-10 minutes, check sheet when done"
    })


@app.route("/api/build/football-divisions")
def build_football_divisions():
    """Build 8 division tabs + 5 class tabs for football rankings."""
    import threading
    from sheets_exporter import export_division_and_class_tabs
    def run():
        try:
            export_division_and_class_tabs()
        except Exception as e:
            print(f"Division tabs error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({
        "status": "started",
        "message": "Building 13 division/class tabs — check sheet in 5 minutes"
    })


@app.route("/api/gamepoints/<path:school_name>")
def get_game_points(school_name):
    """Get per-game power point breakdown for a school."""
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT week, opponent, result, score,
                   opp_wins, opp_losses, opp_division,
                   base_pts, div_bonus, opp_quality, total_pts
            FROM game_power_points
            WHERE sport='football' AND season='2025'
              AND LOWER(school) LIKE LOWER(?)
            ORDER BY week ASC
        """, (f"%{school_name}%",))
        games = [dict(r) for r in c.fetchall()]

        # Also get overall ranking
        c.execute("""
            SELECT school, rank, power_rating, wins, losses,
                   division, class_, district
            FROM power_rankings
            WHERE sport='football' AND season='2025'
              AND LOWER(school) LIKE LOWER(?)
        """, (f"%{school_name}%",))
        ranking = dict(c.fetchone() or {})
        conn.close()

        return jsonify({
            "school":       ranking.get("school", school_name),
            "rank":         ranking.get("rank"),
            "power_rating": ranking.get("power_rating"),
            "record":       f"{ranking.get('wins',0)}-{ranking.get('losses',0)}",
            "division":     ranking.get("division"),
            "class":        ranking.get("class_"),
            "district":     ranking.get("district"),
            "games":        games,
            "total_games":  len(games),
        })
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/schedules/football")
def get_all_schedules():
    """Get all game breakdowns for all schools — for the schedules page."""
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT g.school, g.week, g.game_date, g.home_away,
                   gp.opponent, gp.result, gp.score,
                   gp.opp_wins, gp.opp_losses, gp.opp_division,
                   gp.base_pts, gp.div_bonus, gp.opp_quality, gp.total_pts,
                   pr.division, pr.class_, pr.district,
                   pr.wins, pr.losses, pr.power_rating, pr.rank
            FROM game_power_points gp
            JOIN games g ON g.school = gp.school
                AND g.sport = 'football'
                AND g.season = gp.season
                AND CAST(REPLACE(g.week,'Week ','') AS INTEGER) = gp.week
            LEFT JOIN power_rankings pr ON pr.school = gp.school
                AND pr.sport = 'football'
                AND pr.season = gp.season
            WHERE gp.sport = 'football' AND gp.season = '2025'
            ORDER BY gp.school, gp.week ASC
        """)
        rows = [dict(r) for r in c.fetchall()]
        conn.close()

        # Group by school
        by_school = {}
        for r in rows:
            s = r['school']
            if s not in by_school:
                by_school[s] = {
                    'school':       s,
                    'division':     r['division'],
                    'class_':       r['class_'],
                    'district':     r['district'],
                    'wins':         r['wins'],
                    'losses':       r['losses'],
                    'power_rating': r['power_rating'],
                    'rank':         r['rank'],
                    'games':        []
                }
            by_school[s]['games'].append({
                'week':        r['week'],
                'game_date':   r['game_date'] or '',
                'home_away':   r['home_away'] or '',
                'opponent':    r['opponent'],
                'result':      r['result'],
                'score':       r['score'] or '',
                'opp_wins':    r['opp_wins'],
                'opp_losses':  r['opp_losses'],
                'opp_division':r['opp_division'] or '',
                'base_pts':    r['base_pts'],
                'div_bonus':   r['div_bonus'],
                'opp_quality': r['opp_quality'],
                'total_pts':   r['total_pts'],
            })

        return jsonify({
            'season': '2025',
            'total_schools': len(by_school),
            'schools': list(by_school.values())
        })
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500
