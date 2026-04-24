"""
LVAY Scraper - API Server
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os
from datetime import datetime
import threading

app = Flask(__name__)
CORS(app)
DB_PATH = "/data/lvay_v2.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def resolve_season(sport="baseball"):
    now = datetime.now()
    if sport == "football":
        return str(now.year)
    # Baseball/softball: current school year ends in spring
    # If Aug or later, next year's season; otherwise current year
    return str(now.year + 1 if now.month >= 8 else now.year)


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
            sport      TEXT NOT NULL,
            season     TEXT NOT NULL,
            school     TEXT NOT NULL,
            opponent   TEXT NOT NULL,
            opp_wins   INTEGER DEFAULT 0,
            opp_losses INTEGER DEFAULT 0,
            UNIQUE(sport, season, school, opponent)
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
    # Auto-import OOS opponent records on startup
    try:
        from import_oos_2025 import run as import_oos
        import_oos()
        print("OOS football opponents imported on startup")
    except Exception as e:
        print(f"OOS football import on startup error: {e}")
    try:
        from import_oos_baseball_2026 import run as import_oos_baseball
        import_oos_baseball()
        print("OOS baseball opponents imported on startup")
    except Exception as e:
        print(f"OOS baseball import on startup error: {e}")
    try:
        from import_oos_softball_2026 import run as import_oos_softball
        import_oos_softball()
        print("OOS softball opponents imported on startup")
    except Exception as e:
        print(f"OOS softball import on startup error: {e}")


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

@app.route("/api/fix/new-oos-games")
def fix_new_oos_games():
    """Insert missing OOS games that scraper dropped from games table."""
    conn = get_db()
    c = conn.cursor()
    missing_games = [
        ('football','2025','Neville',        'Week 3', '2025-09-19','Oak Grove, MS',                 'L','7-36', 'A','NS1',''),
        ('football','2025','DeRidder',        'Week 3', '2025-09-19','Newton High School, TX',        'L','16-36','A','NS4',''),
        ('football','2025','Northshore',      'Week 1', '2025-09-05','Picayune Memorial, MS',         'L','13-27','H','NS1',''),
        ('football','2025','North DeSoto',    'Week 2', '2025-09-12','Center High School, TX',        'W','49-20','A','NS2',''),
        ('football','2025','Ouachita Parish', 'Week 4', '2025-09-26','Port Gibson, MS',              'W','51-6', 'H','NS3',''),
        ('football','2025','West Monroe',     'Week 2', '2025-09-12','Pulaski Academy, AR',           'W','31-17','H','S1', ''),
        ('football','2025','Ruston',          'Week 4', '2025-09-25','Midland-Legacy High School, TX','W','49-21','H','NS1',''),
    ]
    inserted = 0
    for g in missing_games:
        c.execute("""
            INSERT OR IGNORE INTO games
            (sport, season, school, week, game_date, opponent, win_loss, score,
             home_away, district_class, tournament)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, g)
        inserted += c.rowcount
    conn.commit()
    conn.close()
    return jsonify({"status": "ok", "rows_inserted": inserted,
                    "message": f"Inserted {inserted} missing OOS game records"})


@app.route("/api/fix/haynesville-oos")
def fix_haynesville_oos():
    """Insert Haynesville WK2 Harmony Grove AR game (missed by scraper)."""
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT OR IGNORE INTO games
            (sport, season, school, week, game_date, opponent, win_loss, score, home_away, district_class, tournament)
            VALUES ('football','2025','Haynesville','Week 2','2025-09-12',
                    'Harmony Grove High School, AR','W','42-14','H','NS3','')
        """)
        rows = c.rowcount
        conn.commit()
        conn.close()
        return jsonify({"status": "ok", "rows_inserted": rows, "message": "Haynesville Wk2 OOS game inserted"})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/fix/stedmund-oos")
def fix_stedmund_oos():
    """Insert St. Edmund WK2 Muenster Sacred Heart TX game (missed by scraper)."""
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT OR IGNORE INTO games
            (sport, season, school, week, game_date, opponent, win_loss, score, home_away, district_class, tournament)
            VALUES ('football','2025','St. Edmund','Week 2','2025-09-13',
                    'Muenster Sacred Heart, TX','W','55-6','A','NS4','')
        """)
        rows = c.rowcount
        conn.commit()
        conn.close()
        return jsonify({"status": "ok", "rows_inserted": rows, "message": "St. Edmund Wk2 OOS game inserted"})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


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
    sport = request.args.get("sport", "football")
    season = request.args.get("season", "2025")
    def run():
        try:
            from run_power_rankings import run_power_rankings
            run_power_rankings(season=season, sport=sport)
        except Exception as e:
            print(f"Rankings calc error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "sport": sport, "season": season,
                    "message": f"{sport} rankings calculating — check logs"})

# ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)


@app.route("/api/schedules/football")
def schedules_football():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT pr.school, pr.division, pr.track, pr.class_, pr.district,
                   pr.power_rating, pr.wins, pr.losses, pr.ties, pr.games_played
            FROM power_rankings pr
            WHERE pr.sport='football' AND pr.season='2025'
            ORDER BY pr.class_ DESC, pr.district ASC, pr.school ASC
        """)
        schools = [dict(r) for r in c.fetchall()]

        for s in schools:
            c.execute("""
                SELECT gpp.week, gpp.opponent, gpp.result, gpp.score,
                       gpp.opp_wins, gpp.opp_losses, gpp.opp_division,
                       gpp.base_pts, gpp.div_bonus, gpp.opp_quality,
                       gpp.total_pts, gpp.is_district,
                       g.home_away, g.game_date
                FROM game_power_points gpp
                LEFT JOIN games g ON (
                    g.sport='football' AND g.season='2025'
                    AND g.school=gpp.school
                    AND CAST(REPLACE(g.week,'Week ','') AS INTEGER)=gpp.week
                )
                WHERE gpp.sport='football' AND gpp.season='2025' AND gpp.school=?
                ORDER BY gpp.week ASC
            """, (s['school'],))
            s['games'] = [dict(r) for r in c.fetchall()]

    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"sport": "football", "season": "2025", "count": len(schools), "schools": schools})


@app.route("/api/breakdown/football/<school>")
def breakdown_football(school):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT week, opponent, result, score,
                   opp_wins, opp_losses, opp_division,
                   base_pts, div_bonus, opp_quality, total_pts
            FROM game_power_points
            WHERE sport='football' AND season='2025' AND school=?
            ORDER BY week ASC
        """, (school,))
        rows = [dict(r) for r in c.fetchall()]
        total = sum(r["total_pts"] for r in rows)
        pr = round(total / len(rows), 2) if rows else 0
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"school": school, "calculated_pr": pr, "games": rows})

@app.route("/api/breakdown/baseball/<school>")
def breakdown_baseball(school):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT week, opponent, result, score,
                   opp_wins, opp_losses, opp_division,
                   base_pts, div_bonus, opp_quality, total_pts, is_district
            FROM game_power_points
            WHERE sport='baseball' AND season='2026' AND school=?
            ORDER BY week ASC
        """, (school,))
        rows = [dict(r) for r in c.fetchall()]
        total = sum(r["total_pts"] for r in rows)
        pr = round(total / len(rows), 2) if rows else 0
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"school": school, "calculated_pr": pr, "games": rows})


@app.route("/api/breakdown/softball/<school>")
def breakdown_softball(school):
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT week, opponent, result, score,
                   opp_wins, opp_losses, opp_division,
                   base_pts, div_bonus, opp_quality, total_pts, is_district
            FROM game_power_points
            WHERE sport='softball' AND season='2026' AND school=?
            ORDER BY week ASC
        """, (school,))
        rows = [dict(r) for r in c.fetchall()]
        total = sum(r["total_pts"] for r in rows)
        pr = round(total / len(rows), 2) if rows else 0
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"school": school, "calculated_pr": pr, "games": rows})


@app.route("/api/rankings/football")
def rankings_football():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school, division, track, class_, district,
                   rank, power_rating, wins, losses, ties, games_played
            FROM power_rankings
            WHERE sport='football' AND season='2025'
            ORDER BY rank ASC
        """)
        rows = [dict(r) for r in c.fetchall()]
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"sport": "football", "season": "2025", "count": len(rows), "rankings": rows})

@app.route("/api/rankings/baseball")
def rankings_baseball():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school, division, track, class_, district,
                   rank, power_rating, wins, losses, ties, games_played
            FROM power_rankings
            WHERE sport='baseball' AND season='2026'
            ORDER BY rank ASC
        """)
        rows = [dict(r) for r in c.fetchall()]
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"sport": "baseball", "season": "2026", "count": len(rows), "rankings": rows})


@app.route("/api/rankings/softball")
def rankings_softball():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school, division, track, class_, district,
                   rank, power_rating, wins, losses, ties, games_played
            FROM power_rankings
            WHERE sport='softball' AND season='2026'
            ORDER BY rank ASC
        """)
        rows = [dict(r) for r in c.fetchall()]
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"sport": "softball", "season": "2026", "count": len(rows), "rankings": rows})


@app.route("/control-panel")
def control_panel():
    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LVAY Football Control Panel</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; color: #1a1a1a; padding: 1.5rem; }
  h1 { font-size: 20px; font-weight: 600; margin-bottom: 1.5rem; color: #1a1a1a; }
  .section { background: #fff; border: 1px solid #e5e5e5; border-radius: 10px; padding: 1.25rem; margin-bottom: 1rem; }
  .section-title { font-size: 11px; font-weight: 600; color: #888; text-transform: uppercase; letter-spacing: 0.07em; margin-bottom: 12px; }
  .formula-row { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; font-size: 13px; }
  .formula-label { min-width: 170px; color: #333; }
  .formula-val { font-weight: 600; min-width: 30px; text-align: right; }
  .formula-note { font-size: 11px; color: #999; }
  .divtabs { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 12px; }
  .divtab { font-size: 12px; padding: 5px 11px; border-radius: 6px; border: 1px solid #ddd; background: transparent; color: #666; cursor: pointer; }
  .divtab.active { background: #EBF4FF; color: #1a6eb5; border-color: #1a6eb5; font-weight: 500; }
  .divtab:hover { background: #f5f5f5; }
  .status { font-size: 13px; color: #888; padding: 8px 0; }
  .status.err { color: #c0392b; }
  .rank-wrap { max-height: 400px; overflow-y: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { text-align: left; padding: 7px 10px; font-size: 11px; font-weight: 600; color: #888; text-transform: uppercase; letter-spacing: 0.05em; border-bottom: 1px solid #eee; position: sticky; top: 0; background: #fff; }
  td { padding: 8px 10px; border-bottom: 1px solid #f0f0f0; }
  tr:last-child td { border-bottom: none; }
  tr.clickable:hover { background: #f9f9f9; cursor: pointer; }
  .w { color: #27ae60; font-weight: 600; }
  .l { color: #c0392b; }
  .badge { display: inline-block; font-size: 10px; padding: 2px 6px; border-radius: 20px; margin-left: 4px; }
  .badge-ns { background: #EBF4FF; color: #1a6eb5; }
  .badge-s { background: #FFF3CD; color: #856404; }
  .search-row { display: flex; gap: 8px; margin-bottom: 1rem; }
  .search-row input { flex: 1; padding: 8px 12px; border: 1px solid #ddd; border-radius: 6px; font-size: 13px; outline: none; }
  .search-row input:focus { border-color: #1a6eb5; }
  .search-row button { padding: 8px 16px; background: #1a6eb5; color: white; border: none; border-radius: 6px; font-size: 13px; cursor: pointer; }
  .search-row button:hover { background: #155a9a; }
  .metric-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 1rem; }
  .metric { background: #f8f8f8; border-radius: 8px; padding: 12px; }
  .metric-label { font-size: 11px; color: #888; margin-bottom: 4px; }
  .metric-value { font-size: 20px; font-weight: 600; }
  .school-header { display: flex; align-items: flex-start; justify-content: space-between; margin-bottom: 12px; }
  .school-name { font-size: 18px; font-weight: 600; }
  .school-meta { font-size: 13px; color: #888; margin-top: 2px; }
  .school-pr { font-size: 28px; font-weight: 700; text-align: right; }
  .school-rank { font-size: 13px; color: #888; text-align: right; }
  .checklist-item { display: flex; align-items: center; gap: 10px; padding: 7px 0; border-bottom: 1px solid #f0f0f0; font-size: 13px; }
  .checklist-item:last-child { border-bottom: none; }
  .dot { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }
  .dot.done { background: #27ae60; }
  .dot.todo { background: transparent; border: 2px solid #ccc; }
  @media (max-width: 600px) { .metric-grid { grid-template-columns: repeat(2,1fr); } }
</style>
</head>
<body>
<h1>LVAY — Football Control Panel</h1>

<div class="section">
  <div class="section-title">Formula reference — LHSAA 14.12</div>
  <div class="formula-row"><span class="formula-label">Win base points</span><span class="formula-val">10</span><span class="formula-note">per game</span></div>
  <div class="formula-row"><span class="formula-label">Loss base points</span><span class="formula-val">0</span></div>
  <div class="formula-row"><span class="formula-label">Tie base points</span><span class="formula-val">5</span></div>
  <div class="formula-row"><span class="formula-label">In-state div bonus</span><span class="formula-val">+2</span><span class="formula-note">per div higher — requires both class AND div higher</span></div>
  <div class="formula-row"><span class="formula-label">OOS class bonus</span><span class="formula-val">+2</span><span class="formula-note">per class higher — class only, no div requirement</span></div>
  <div class="formula-row"><span class="formula-label">Opponent quality (OppQ)</span><span class="formula-val">×10</span><span class="formula-note">(opp wins ÷ opp games) × 10, added every game</span></div>
  <div class="formula-row"><span class="formula-label">Final power rating</span><span class="formula-val">÷ GP</span><span class="formula-note">total points ÷ games played, rounded to 2dp</span></div>
</div>

<div class="section">
  <div class="section-title">Live rankings by division</div>
  <div class="divtabs">
    <button class="divtab active" data-div="Non-Select Division I" onclick="setDiv(this)">NS Div I</button>
    <button class="divtab" data-div="Non-Select Division II" onclick="setDiv(this)">NS Div II</button>
    <button class="divtab" data-div="Non-Select Division III" onclick="setDiv(this)">NS Div III</button>
    <button class="divtab" data-div="Non-Select Division IV" onclick="setDiv(this)">NS Div IV</button>
    <button class="divtab" data-div="Select Division I" onclick="setDiv(this)">S Div I</button>
    <button class="divtab" data-div="Select Division II" onclick="setDiv(this)">S Div II</button>
    <button class="divtab" data-div="Select Division III" onclick="setDiv(this)">S Div III</button>
    <button class="divtab" data-div="Select Division IV" onclick="setDiv(this)">S Div IV</button>
  </div>
  <div id="rank-status" class="status">Loading rankings...</div>
  <div class="rank-wrap" id="rank-wrap" style="display:none">
    <table>
      <thead><tr><th>#</th><th>School</th><th>Class</th><th>Record</th><th>GP</th><th>PR</th></tr></thead>
      <tbody id="rank-body"></tbody>
    </table>
  </div>
</div>

<div class="section">
  <div class="section-title">School lookup — game-by-game breakdown</div>
  <div class="search-row">
    <input type="text" id="school-input" placeholder="e.g. Calvary Baptist" />
    <button onclick="lookupSchool()">Look up</button>
  </div>
  <div id="school-status" class="status" style="display:none"></div>
  <div id="school-result" style="display:none">
    <div class="school-header">
      <div>
        <div class="school-name" id="s-name"></div>
        <div class="school-meta" id="s-meta"></div>
      </div>
      <div>
        <div class="school-pr" id="s-pr"></div>
        <div class="school-rank" id="s-rank"></div>
      </div>
    </div>
    <div class="metric-grid">
      <div class="metric"><div class="metric-label">Record</div><div class="metric-value" id="s-record">—</div></div>
      <div class="metric"><div class="metric-label">Games played</div><div class="metric-value" id="s-gp">—</div></div>
      <div class="metric"><div class="metric-label">Class</div><div class="metric-value" id="s-class">—</div></div>
      <div class="metric"><div class="metric-label">District</div><div class="metric-value" id="s-district">—</div></div>
    </div>
    <div style="overflow-x:auto">
      <table style="min-width:500px">
        <thead><tr><th>Wk</th><th>Opponent</th><th>Result</th><th>Score</th><th>Base</th><th>Div+</th><th>OppQ</th><th>Total</th></tr></thead>
        <tbody id="s-games"></tbody>
      </table>
    </div>
  </div>
</div>

<div class="section">
  <div class="section-title">Pipeline status</div>
  <div class="checklist-item"><div class="dot done"></div><span>Scraper — LHSAA schedule pages → SQLite</span></div>
  <div class="checklist-item"><div class="dot done"></div><span>OOS import — out-of-state opponent records</span></div>
  <div class="checklist-item"><div class="dot done"></div><span>Power rating engine (power_rating_engine.py)</span></div>
  <div class="checklist-item"><div class="dot done"></div><span>Rankings calculate endpoint</span></div>
  <div class="checklist-item"><div class="dot done"></div><span>Google Sheets exporter — all 8 division tabs</span></div>
  <div class="checklist-item"><div class="dot done"></div><span>WordPress display via HTML iframe endpoints</span></div>
  <div class="checklist-item"><div class="dot done"></div><span>99.3% accuracy vs LHSAA (301/303 exact)</span></div>
  <div class="checklist-item"><div class="dot todo"></div><span style="color:#999">Add OppQ as visible column in rankings display</span></div>
</div>

<script>
  let allRankings = [];
  let currentDiv = 'Non-Select Division I';

  async function loadRankings() {
    const statusEl = document.getElementById('rank-status');
    try {
      const r = await fetch('/api/rankings/football');
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      allRankings = data.rankings || [];
      if (!allRankings.length) throw new Error('empty response');
      statusEl.style.display = 'none';
      renderDivision(currentDiv);
    } catch(e) {
      statusEl.textContent = 'Error: ' + e.message;
      statusEl.className = 'status err';
    }
  }

  function setDiv(btn) {
    currentDiv = btn.dataset.div;
    document.querySelectorAll('.divtab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');
    if (allRankings.length) renderDivision(currentDiv);
  }

  function renderDivision(div) {
    const statusEl = document.getElementById('rank-status');
    const wrapEl = document.getElementById('rank-wrap');
    const schools = allRankings.filter(s => s.division === div);
    if (!schools.length) {
      wrapEl.style.display = 'none';
      statusEl.style.display = 'block';
      statusEl.textContent = 'No schools for: ' + div;
      statusEl.className = 'status';
      return;
    }
    schools.sort((a,b) => b.power_rating - a.power_rating);
    const tbody = document.getElementById('rank-body');
    tbody.innerHTML = '';
    schools.forEach((s, i) => {
      const tr = document.createElement('tr');
      tr.className = 'clickable';
      const cls = s.class_ || s.class || '';
      const rec = (s.wins||0) + '-' + (s.losses||0);
      const badge = s.division.startsWith('Select')
        ? '<span class="badge badge-s">S</span>'
        : '<span class="badge badge-ns">NS</span>';
      tr.innerHTML = `<td>${i+1}</td><td>${s.school}${badge}</td><td>${cls}</td><td>${rec}</td><td>${s.games_played||'—'}</td><td><strong>${(+s.power_rating).toFixed(2)}</strong></td>`;
      tr.onclick = () => { document.getElementById('school-input').value = s.school; lookupSchool(); };
      tbody.appendChild(tr);
    });
    wrapEl.style.display = 'block';
    statusEl.style.display = 'none';
  }

  async function lookupSchool() {
    const name = document.getElementById('school-input').value.trim();
    if (!name) return;
    const ss = document.getElementById('school-status');
    const sr = document.getElementById('school-result');
    ss.style.display = 'block';
    ss.className = 'status';
    ss.textContent = 'Loading ' + name + '...';
    sr.style.display = 'none';
    try {
      const r = await fetch('/api/gamepoints/' + encodeURIComponent(name));
      if (!r.ok) throw new Error('not found');
      const d = await r.json();
      document.getElementById('s-name').textContent = d.school;
      document.getElementById('s-meta').textContent = (d.division||'') + ' · District ' + (d.district||'—');
      document.getElementById('s-pr').textContent = (+(d.power_rating||0)).toFixed(2);
      document.getElementById('s-rank').textContent = 'Rank #' + (d.rank||'—');
      document.getElementById('s-record').textContent = d.record||'—';
      document.getElementById('s-gp').textContent = d.total_games||'—';
      document.getElementById('s-class').textContent = d.class||d.class_||'—';
      document.getElementById('s-district').textContent = d.district||'—';
      const tbody = document.getElementById('s-games');
      tbody.innerHTML = '';
      (d.games||[]).forEach(g => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${g.week}</td><td>${g.opponent}</td><td class="${g.result==='W'?'w':'l'}">${g.result}</td><td>${g.score||'—'}</td><td>${(+(g.base_pts||0)).toFixed(1)}</td><td>${(+(g.div_bonus||0)).toFixed(1)}</td><td>${(+(g.opp_quality||0)).toFixed(2)}</td><td><strong>${(+(g.total_pts||0)).toFixed(2)}</strong></td>`;
        tbody.appendChild(tr);
      });
      ss.style.display = 'none';
      sr.style.display = 'block';
    } catch(e) {
      ss.textContent = 'School not found — check spelling, e.g. "Calvary Baptist"';
      ss.className = 'status err';
    }
  }

  document.getElementById('school-input').addEventListener('keydown', e => { if(e.key==='Enter') lookupSchool(); });
  loadRankings();
</script>
</body>
</html>"""
    return html


# ── BASEBALL / SOFTBALL SCHEDULES ────────────────────────────────────────────

@app.route("/api/schedules/baseball")
def schedules_baseball():
    return get_sport_schedules("baseball")

@app.route("/api/schedules/softball")
def schedules_softball():
    return get_sport_schedules("softball")

def get_sport_schedules(sport):
    conn = get_db()
    c = conn.cursor()
    school_filter = request.args.get("school")
    season = os.environ.get("SEASON_YEAR") or resolve_season(sport)

    # Get school list from power_rankings so we have division/class/district/PR
    if school_filter:
        c.execute("""
            SELECT pr.school, pr.division, pr.track, pr.class_, pr.district,
                   pr.power_rating, pr.wins, pr.losses, pr.ties, pr.games_played, pr.rank
            FROM power_rankings pr
            WHERE pr.sport=? AND pr.season=?
            AND LOWER(pr.school) LIKE LOWER(?)
            ORDER BY pr.class_ DESC, pr.district ASC, pr.school ASC
        """, (sport, season, f"%{school_filter}%"))
    else:
        c.execute("""
            SELECT pr.school, pr.division, pr.track, pr.class_, pr.district,
                   pr.power_rating, pr.wins, pr.losses, pr.ties, pr.games_played, pr.rank
            FROM power_rankings pr
            WHERE pr.sport=? AND pr.season=?
            ORDER BY pr.class_ DESC, pr.district ASC, pr.school ASC
        """, (sport, season))

    school_rows = [dict(r) for r in c.fetchall()]
    schools = []

    for s in school_rows:
        school = s['school']

        # Get game power points joined with game_date and home_away from games table
        c.execute("""
            SELECT gpp.week, gpp.opponent, gpp.result, gpp.score,
                   gpp.opp_wins, gpp.opp_losses, gpp.opp_division,
                   gpp.base_pts, gpp.div_bonus, gpp.opp_quality,
                   gpp.total_pts, gpp.is_district,
                   g.home_away, g.game_date
            FROM game_power_points gpp
            LEFT JOIN games g ON (
                g.sport=gpp.sport AND g.season=gpp.season
                AND g.school=gpp.school
                AND g.game_date IS NOT NULL
                AND g.opponent=gpp.opponent
            )
            WHERE gpp.sport=? AND gpp.season=? AND gpp.school=?
            ORDER BY g.game_date ASC
        """, (sport, season, school))

        games = [dict(r) for r in c.fetchall()]

        schools.append({
            "school":       school,
            "sport":        sport,
            "season":       season,
            "division":     s.get('division', ''),
            "track":        s.get('track', ''),
            "class_":       s.get('class_', ''),
            "district":     s.get('district', ''),
            "power_rating": s.get('power_rating', 0),
            "rank":         s.get('rank', 0),
            "wins":         s.get('wins', 0),
            "losses":       s.get('losses', 0),
            "ties":         s.get('ties', 0),
            "games_played": s.get('games_played', 0),
            "record":       f"{s.get('wins',0)}-{s.get('losses',0)}",
            "games":        games,
        })

    conn.close()
    return jsonify({
        "sport":   sport,
        "season":  season,
        "count":   len(schools),
        "schools": schools
    })

@app.route("/api/build/baseball-sheets")
def build_baseball_sheets():
    def run():
        try:
            from sheets_exporter import export_baseball_to_sheets
            export_baseball_to_sheets()
        except Exception as e:
            print(f"Baseball sheets build error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "message": "Baseball sheets building — check Google Sheet in 3-5 min"})


@app.route("/api/build/softball-sheets")
def build_softball_sheets():
    def run():
        try:
            from sheets_exporter import export_softball_to_sheets
            export_softball_to_sheets()
        except Exception as e:
            print(f"Softball sheets build error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "message": "Softball sheets building — check Google Sheet in 3-5 min"})
