"""
LVAY Scraper - API Server
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import gzip
import hmac
import json
import sqlite3
import os
import re
from datetime import datetime
import threading

app = Flask(__name__)
CORS(app)
DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")
PIPELINE_LOCK = threading.Lock()
PIPELINE_STATE = {
    "status": "idle",
    "started_at": None,
    "finished_at": None,
    "error": None,
}
FOOTBALL_ARCHIVE_PATH = os.path.join(
    os.path.dirname(__file__), "football_archives_2022_2024.json.gz"
)
_FOOTBALL_ARCHIVES = None


def parse_schedule_date(value):
    """Parse the mixed date formats returned by the LHSAA schedule pages."""
    text = str(value or "").strip()
    if not text:
        return None

    # Volleyball dates include a weekday suffix, for example 10/2/2025Thu.
    match = re.match(r"^(\d{1,2}/\d{1,2}/\d{2,4})", text)
    if match:
        text = match.group(1)

    for date_format in (
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%m-%d-%y",
        "%b %d, %Y",
        "%B %d, %Y",
    ):
        try:
            return datetime.strptime(text, date_format)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(
            tzinfo=None
        )
    except ValueError:
        return None


def sort_schedule_games(games):
    """Return games in chronological order with stable same-day ordering."""
    indexed_games = list(enumerate(games))

    def sort_key(indexed_game):
        original_index, game = indexed_game
        parsed_date = parse_schedule_date(game.get("game_date"))
        try:
            match_number = int(game.get("match_num") or 0)
        except (TypeError, ValueError):
            match_number = 0
        try:
            week_number = int(
                str(game.get("week") or "0").replace("Week", "").strip()
            )
        except ValueError:
            week_number = 0
        return (
            parsed_date is None,
            parsed_date or datetime.max,
            match_number,
            week_number,
            original_index,
        )

    sorted_games = [game for _, game in sorted(indexed_games, key=sort_key)]
    for game in sorted_games:
        parsed_date = parse_schedule_date(game.get("game_date"))
        if parsed_date is not None:
            game["game_date"] = (
                f"{parsed_date.month}/{parsed_date.day}/{parsed_date.year}"
            )
    return sorted_games


def annotate_current_opponent_records(games, opponent_records):
    """Fill missing opponent records from the sport's current rankings."""
    for game in games:
        opponent = str(game.get("opponent") or "").strip().casefold()
        record = opponent_records.get(opponent)
        if record is None:
            continue
        if game.get("opp_wins") is None or game.get("opp_losses") is None:
            game["opp_wins"] = record.get("wins", 0)
            game["opp_losses"] = record.get("losses", 0)
            game["opp_ties"] = record.get("ties", 0)
        game["opponent_internal"] = True
    return games


def annotate_volleyball_games(games, opponent_records):
    """Attach opponent records and per-match LHSAA power points."""
    for game in games:
        opponent = str(game.get("opponent") or "").strip().casefold()
        opp = opponent_records.get(opponent)
        if opp is None:
            game["opp_wins"] = None
            game["opp_losses"] = None
            game["opp_games_played"] = None
            game["opp_record"] = ""
        else:
            game["opp_wins"] = opp["wins"]
            game["opp_losses"] = opp["losses"]
            game["opp_games_played"] = opp["games_played"]
            game["opp_record"] = f'{opp["wins"]}-{opp["losses"]}'

        result = str(game.get("result") or "").strip().upper()
        counts = bool(game.get("counts_for_pr"))
        if not counts or opp is None or result not in ("W", "L"):
            game["power_points"] = None
        elif result == "W":
            game["power_points"] = round(5.0 + opp["wins"], 3)
        else:
            game["power_points"] = round(opp["wins"] / 3.0, 3)
        # Keep the established front-end field name while also exposing the
        # clearer API name for future consumers.
        game["total_pts"] = game["power_points"]
    return games


def load_football_archives():
    global _FOOTBALL_ARCHIVES
    if _FOOTBALL_ARCHIVES is None:
        if not os.path.exists(FOOTBALL_ARCHIVE_PATH):
            _FOOTBALL_ARCHIVES = {"seasons": {}}
        else:
            with gzip.open(
                FOOTBALL_ARCHIVE_PATH, "rt", encoding="utf-8"
            ) as source:
                _FOOTBALL_ARCHIVES = json.load(source)
    return _FOOTBALL_ARCHIVES


def football_archive_response(season, summary_only=False, school_filter=""):
    archive = load_football_archives().get("seasons", {}).get(str(season))
    if not archive:
        return None
    schools = archive["schools"]
    if school_filter:
        schools = [
            school for school in schools
            if school["school"].casefold() == school_filter.casefold()
        ]
    schools = [
        {
            **school,
            "games": (
                []
                if summary_only
                else sort_schedule_games(school.get("games", []))
            ),
        }
        for school in schools
    ]
    return {
        "sport": "football",
        "season": str(season),
        "status": archive.get("status", "final"),
        "source": "Airtable archive",
        "count": len(schools),
        "schools": schools,
    }


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def resolve_season(sport="baseball"):
    now = datetime.now()
    if sport == "football":
        return str(now.year)
    return str(now.year + 1 if now.month >= 8 else now.year)


def available_season(conn, sport, table="power_rankings"):
    """Use the configured season when populated, otherwise keep latest live data."""
    requested = request.args.get("season")
    # An explicit season is an archive/current-season contract.  Returning a
    # different populated season here makes an empty new-season page silently
    # display last year's data.
    if requested:
        return str(requested)
    configured = requested or os.environ.get(
        f"{sport.upper()}_SEASON_YEAR",
        os.environ.get("SEASON_YEAR", resolve_season(sport)),
    )
    row = conn.execute(
        f"SELECT 1 FROM {table} WHERE sport=? AND season=? LIMIT 1",
        (sport, str(configured)),
    ).fetchone()
    if row:
        return str(configured)
    row = conn.execute(
        f"SELECT season FROM {table} WHERE sport=? "
        "ORDER BY CAST(season AS INTEGER) DESC LIMIT 1",
        (sport,),
    ).fetchone()
    return str(row["season"]) if row else str(configured)


def available_schedule_season(conn, sport):
    """Resolve schedules independently from power rankings.

    Preseason schedules exist before a power-rating row does, so schedule
    pages must not fall back to the prior season merely because rankings are
    still empty.
    """
    requested = request.args.get("season")
    if requested:
        return str(requested)
    configured = requested or os.environ.get(
        f"{sport.upper()}_SEASON_YEAR",
        os.environ.get("SEASON_YEAR", resolve_season(sport)),
    )
    for table in ("season_schools", "games"):
        try:
            row = conn.execute(
                f"SELECT 1 FROM {table} WHERE sport=? AND season=? LIMIT 1",
                (sport, str(configured)),
            ).fetchone()
            if row:
                return str(configured)
        except sqlite3.OperationalError:
            continue

    seasons = []
    for table in ("season_schools", "games"):
        try:
            seasons.extend(
                str(row["season"])
                for row in conn.execute(
                    f"SELECT DISTINCT season FROM {table} WHERE sport=?",
                    (sport,),
                ).fetchall()
                if row["season"]
            )
        except sqlite3.OperationalError:
            continue
    return max(seasons, key=lambda value: int(value)) if seasons else str(configured)


def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS games (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            sport           TEXT NOT NULL DEFAULT 'football',
            season          TEXT NOT NULL DEFAULT '2025',
            school          TEXT,
            week            TEXT,
            game_date       TEXT,
            opponent        TEXT,
            win_loss        TEXT,
            score           TEXT,
            home_away       TEXT,
            district        TEXT,
            class_          TEXT,
            district_class  TEXT,
            opponent_class  TEXT,
            tournament      TEXT,
            tournament_host TEXT,
            out_of_state    TEXT,
            location        TEXT,
            scraped_at      TEXT DEFAULT (datetime('now')),
            source          TEXT,
            is_district     INTEGER DEFAULT 0,
            needs_review    INTEGER DEFAULT 0,
            UNIQUE(sport, school, game_date, opponent, season)
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
    for column in (
        "week INTEGER",
        "division TEXT",
        "class_ TEXT",
        "opp_ties INTEGER DEFAULT 0",
    ):
        try:
            c.execute(f"ALTER TABLE oos_opponents ADD COLUMN {column}")
        except sqlite3.OperationalError:
            pass
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
    scrape_log_columns = {
        row[1] for row in c.execute("PRAGMA table_info(scrape_log)")
    }
    if "note" not in scrape_log_columns:
        c.execute("ALTER TABLE scrape_log ADD COLUMN note TEXT")
    c.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at    TEXT NOT NULL,
            finished_at   TEXT,
            status        TEXT NOT NULL,
            active_sports TEXT,
            error         TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS season_schools (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            sport      TEXT NOT NULL,
            season     TEXT NOT NULL,
            school     TEXT NOT NULL,
            class_     TEXT,
            district   TEXT,
            division   TEXT,
            track      TEXT,
            source     TEXT,
            status     TEXT DEFAULT 'active',
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(sport, season, school)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS season_registry (
            sport     TEXT NOT NULL,
            season    TEXT NOT NULL,
            status    TEXT NOT NULL DEFAULT 'active',
            is_locked INTEGER NOT NULL DEFAULT 0,
            source    TEXT,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(sport, season)
        )
    """)
    c.execute("""
        INSERT INTO season_registry
            (sport, season, status, is_locked, source)
        VALUES ('football', '2025', 'final', 1, 'LHSAA')
        ON CONFLICT(sport, season) DO UPDATE SET
            status='final', is_locked=1
    """)
    game_columns = {row[1] for row in c.execute("PRAGMA table_info(games)")}
    for name, definition in (
        ("source", "TEXT"),
        ("is_district", "INTEGER DEFAULT 0"),
        ("needs_review", "INTEGER DEFAULT 0"),
    ):
        if name not in game_columns:
            c.execute(f"ALTER TABLE games ADD COLUMN {name} {definition}")
    conn.commit()
    conn.close()


with app.app_context():
    init_db()
    if os.environ.get("ENABLE_FOOTBALL", "true").lower() != "true":
        try:
            import json
            from season_schedule_importer import import_payload

            preseason_path = os.path.join(
                os.path.dirname(__file__), "football_2026_preseason.json"
            )
            if os.path.exists(preseason_path):
                with open(preseason_path, "r", encoding="utf-8") as source_file:
                    preseason_payload = json.load(source_file)
                imported = import_payload(
                    preseason_payload,
                    DB_PATH,
                    replace=True,
                )
                print(f"Football preseason schedules imported: {imported}")
        except Exception as e:
            print(f"Football preseason schedule import error: {e}")


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
        c.execute("""
            SELECT ran_at, sport, games_found, status, note
            FROM scrape_log ORDER BY id DESC LIMIT 20
        """)
        recent = [dict(r) for r in c.fetchall()]
    except Exception:
        recent = []
    try:
        latest_pipeline = c.execute("""
            SELECT id, started_at, finished_at, status, active_sports, error
            FROM pipeline_runs ORDER BY id DESC LIMIT 1
        """).fetchone()
        latest_pipeline = dict(latest_pipeline) if latest_pipeline else None
        if latest_pipeline and latest_pipeline.get("active_sports"):
            latest_pipeline["active_sports"] = json.loads(
                latest_pipeline["active_sports"]
            )
    except Exception:
        latest_pipeline = None
    conn.close()
    return jsonify({
        "status":           "ok",
        "server_time":      datetime.now().isoformat(),
        "records_by_sport": by_sport,
        "total_records":    sum(by_sport.values()),
        "recent_scrapes":   recent,
        "latest_pipeline":  latest_pipeline,
    })


@app.route("/api/health")
def health():
    """Render health check: prove the API can read its persistent database."""
    try:
        conn = get_db()
        conn.execute("SELECT 1").fetchone()
        conn.close()
    except Exception as exc:
        return jsonify({"status": "error", "database": str(exc)}), 503
    return jsonify({"status": "ok", "database": "reachable"})


# ── FULL PIPELINE TRIGGER ───────────────────────────────────

def _pipeline_authorized():
    configured_token = os.environ.get("PIPELINE_TOKEN", "")
    supplied_token = request.headers.get("X-Pipeline-Token", "")
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        supplied_token = auth_header[7:]
    return (
        bool(configured_token)
        and bool(supplied_token)
        and hmac.compare_digest(configured_token, supplied_token)
    )


@app.route("/api/pipeline/run", methods=["POST"])
def run_full_pipeline():
    if not os.environ.get("PIPELINE_TOKEN"):
        return jsonify({
            "status": "unavailable",
            "message": "PIPELINE_TOKEN is not configured",
        }), 503
    if not _pipeline_authorized():
        return jsonify({"status": "unauthorized"}), 401
    if not PIPELINE_LOCK.acquire(blocking=False):
        return jsonify({
            "status": "already_running",
            "started_at": PIPELINE_STATE["started_at"],
        }), 409

    started_at = datetime.now().isoformat()
    PIPELINE_STATE.update({
        "status": "running",
        "started_at": started_at,
        "finished_at": None,
        "error": None,
    })

    def run():
        run_id = None
        try:
            from scheduled_tasks import get_active_sports, scheduled_run
            active_sports = get_active_sports()
            conn = get_db()
            cursor = conn.execute("""
                INSERT INTO pipeline_runs
                    (started_at, status, active_sports)
                VALUES (?, 'running', ?)
            """, (
                PIPELINE_STATE["started_at"],
                json.dumps(active_sports),
            ))
            run_id = cursor.lastrowid
            conn.commit()
            conn.close()
            scheduled_run()
            PIPELINE_STATE["status"] = "completed"
        except Exception as exc:
            PIPELINE_STATE["status"] = "failed"
            PIPELINE_STATE["error"] = str(exc)
            print(f"[PIPELINE] ERROR: {exc}")
        finally:
            PIPELINE_STATE["finished_at"] = datetime.now().isoformat()
            if run_id is not None:
                try:
                    conn = get_db()
                    conn.execute("""
                        UPDATE pipeline_runs
                        SET finished_at=?, status=?, error=?
                        WHERE id=?
                    """, (
                        PIPELINE_STATE["finished_at"],
                        PIPELINE_STATE["status"],
                        PIPELINE_STATE["error"],
                        run_id,
                    ))
                    conn.commit()
                    conn.close()
                except Exception as persist_exc:
                    print(
                        "[PIPELINE] Could not persist final state: "
                        f"{persist_exc}"
                    )
            PIPELINE_LOCK.release()

    threading.Thread(target=run, daemon=True).start()
    return jsonify({
        "status": "started",
        "started_at": started_at,
        "message": "Full in-season pipeline started",
    }), 202


@app.route("/api/pipeline/status")
def pipeline_status():
    if not _pipeline_authorized():
        return jsonify({"status": "unauthorized"}), 401
    state = dict(PIPELINE_STATE)
    try:
        conn = get_db()
        latest = conn.execute("""
            SELECT id, started_at, finished_at, status, active_sports, error
            FROM pipeline_runs ORDER BY id DESC LIMIT 1
        """).fetchone()
        conn.close()
        if latest:
            state = dict(latest)
            if state.get("active_sports"):
                state["active_sports"] = json.loads(state["active_sports"])
    except Exception as exc:
        state["persistence_error"] = str(exc)
    return jsonify(state)


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


@app.route("/api/scrape/volleyball")
def scrape_volleyball():
    season = request.args.get("season") or os.environ.get(
        "VOLLEYBALL_SEASON_YEAR", resolve_season("volleyball")
    )

    def run():
        try:
            from scraper_volleyball import run_volleyball_scraper
            run_volleyball_scraper(season)
        except Exception as e:
            print(f"Volleyball scrape error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "sport": "volleyball", "message": "Volleyball scrape running — check logs in 2-3 min"})


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


# ── DATA FIX ENDPOINTS ───────────────────────────────────────

@app.route("/api/build/volleyball-sheets")
def build_volleyball_sheets():
    conn = get_db()
    season = available_season(conn, "volleyball", "volleyball_rankings")
    conn.close()

    def run():
        try:
            from sheets_exporter import export_volleyball_to_sheets
            print(export_volleyball_to_sheets(season))
        except Exception as e:
            print(f"Volleyball sheets build error: {e}")

    threading.Thread(target=run, daemon=True).start()
    return jsonify({
        "status": "started", "sport": "volleyball", "season": season,
        "message": "Volleyball sheets building in background",
    })


@app.route("/api/build/<sport>-review")
def build_sport_review(sport):
    if sport not in ("baseball", "softball"):
        return jsonify({"error": "Review build is available for baseball and softball"}), 400

    try:
        from sheets_exporter import (
            batch_write,
            ensure_sport_overrides_tab,
            get_client,
            get_or_create_tab,
        )
        sheet = get_client().open_by_key(
            os.environ.get(
                "GOOGLE_SHEET_ID",
                "1u_cJBAWTQJIAO36HZTYvPa7QfE0JoOEqx12c1U4t4mk",
            )
        )
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT school, week, game_date, opponent, win_loss, score,
                   class_, district, district_class, needs_review
            FROM games
            WHERE sport=? AND season='2026'
            ORDER BY school, game_date, week
        """, (sport,)).fetchall()
        conn.close()

        output = []
        import re
        for row in rows:
            school = str(row["school"] or "").strip()
            if school in ("", "#", "School"):
                continue
            result = str(row["win_loss"] or "").strip()
            score = str(row["score"] or "").strip()
            issues = []
            if not result and score:
                issues.append("missing W/L")
            elif result and result not in (
                "W", "L", "T", "Tie", "W(f)", "L(f)", "PPD", "OD", "JV"
            ):
                issues.append("unrecognized result")
            if result in ("W", "L", "T", "Tie", "W(f)", "L(f)") and not score:
                issues.append("missing score")
            if score:
                numbers = [int(n) for n in re.findall(r"\d+", score)]
                if len(numbers) < 2:
                    issues.append("malformed score")
                else:
                    school_score, opponent_score = numbers[:2]
                    if result in ("W", "W(f)") and school_score <= opponent_score:
                        issues.append("W conflicts with score")
                    elif result in ("L", "L(f)") and school_score >= opponent_score:
                        issues.append("L conflicts with score")
                    elif result in ("T", "Tie") and school_score != opponent_score:
                        issues.append("tie conflicts with score")
            if row["needs_review"]:
                issues.append("flagged")
            if issues:
                output.append([
                    school, row["week"] or "", row["game_date"] or "",
                    row["opponent"] or "", result, score, row["class_"] or "",
                    row["district"] or "", row["district_class"] or "",
                    ", ".join(issues),
                ])

        ws = get_or_create_tab(
            sheet, f"{sport.title()} Needs Review (2026)"
        )
        ws.update("A1", [[
            "School", "Game", "Date", "Opponent", "W/L", "Score",
            "Class", "District", "District/Class", "Issue",
        ]])
        if output:
            batch_write(ws, 2, output)
        else:
            ws.update("A2", [["No issues found!"]])
        flagged = len(output)
        ensure_sport_overrides_tab(sheet, sport, 2026)
        return jsonify({
            "status": "complete",
            "sport": sport,
            "games_needing_review": flagged,
            "validator": "school-score-first-v4-inline",
        })
    except Exception as e:
        return jsonify({"status": "error", "sport": sport, "error": str(e)}), 500


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
            from import_football_2025 import run as do_import
            do_import()
        except Exception as e:
            print(f"OOS import error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "message": "Importing OOS opponent records — check logs"})


# ── RANKINGS CALCULATE ───────────────────────────────────────

@app.route("/api/rankings/calculate")
def calculate_rankings():
    sport  = request.args.get("sport", "football")
    season = request.args.get("season") or os.environ.get(
        f"{sport.upper()}_SEASON_YEAR",
        os.environ.get("SEASON_YEAR", resolve_season(sport)),
    )

    def run():
        try:
            if sport == "volleyball":
                from run_power_rankings_volleyball import run_volleyball_rankings
                run_volleyball_rankings(season)
            else:
                from run_power_rankings import run_power_rankings
                run_power_rankings(season=season, sport=sport)
        except Exception as e:
            print(f"Rankings calc error: {e}")

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "sport": sport, "season": season,
                    "message": f"{sport} rankings calculating — check logs"})


# ── RANKINGS ENDPOINTS ───────────────────────────────────────

@app.route("/api/rankings/football")
def rankings_football():
    conn = get_db()
    c = conn.cursor()
    try:
        # An explicitly requested season must never fall forward or backward to
        # another year. This keeps archive pages historically accurate and lets
        # the upcoming-season page correctly report that rankings are not yet
        # available.
        requested_season = request.args.get("season", type=int)
        season = requested_season if requested_season is not None else available_season(conn, "football")
        c.execute("""
            SELECT school, division, track, class_, district,
                   rank, power_rating, wins, losses, ties, games_played,
                   COALESCE(strength_factor, 0) as strength_factor,
                   calculated_at
            FROM power_rankings
            WHERE sport='football' AND season=?
            ORDER BY rank ASC
        """, (season,))
        rows = [dict(r) for r in c.fetchall()]
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"sport": "football", "season": season, "count": len(rows), "rankings": rows})


@app.route("/api/rankings/baseball")
def rankings_baseball():
    conn = get_db()
    c = conn.cursor()
    try:
        season = available_season(conn, "baseball")
        c.execute("""
            SELECT school, division, track, class_, district,
                   rank, power_rating, wins, losses, ties, games_played,
                   COALESCE(strength_factor, 0) as strength_factor,
                   calculated_at
            FROM power_rankings
            WHERE sport='baseball' AND season=?
            ORDER BY rank ASC
        """, (season,))
        rows = [dict(r) for r in c.fetchall()]
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"sport": "baseball", "season": season, "count": len(rows), "rankings": rows})


@app.route("/api/rankings/softball")
def rankings_softball():
    conn = get_db()
    c = conn.cursor()
    try:
        season = available_season(conn, "softball")
        c.execute("""
            SELECT school, division, track, class_, district,
                   rank, power_rating, wins, losses, ties, games_played,
                   COALESCE(strength_factor, 0) as strength_factor,
                   calculated_at
            FROM power_rankings
            WHERE sport='softball' AND season=?
            ORDER BY rank ASC
        """, (season,))
        rows = [dict(r) for r in c.fetchall()]
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"sport": "softball", "season": season, "count": len(rows), "rankings": rows})


@app.route("/api/rankings/volleyball")
def rankings_volleyball():
    conn = get_db()
    c = conn.cursor()
    try:
        season = available_season(conn, "volleyball", "volleyball_rankings")
        c.execute("""
            SELECT school, division, class_, district,
                   rank, div_rank, power_rating, wins, losses, games_played
            FROM volleyball_rankings
            WHERE sport='volleyball' AND season=?
            ORDER BY rank ASC
        """, (season,))
        rows = [dict(r) for r in c.fetchall()]
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"sport": "volleyball", "season": season, "count": len(rows), "rankings": rows})


@app.route("/embed/volleyball-rankings")
def embed_volleyball_rankings():
    conn = get_db()
    season = available_season(conn, "volleyball", "volleyball_rankings")
    rows = conn.execute("""
        SELECT school, division, div_rank, power_rating, wins, losses
        FROM volleyball_rankings
        WHERE sport='volleyball' AND season=?
        ORDER BY division, div_rank
    """, (season,)).fetchall()
    conn.close()
    groups = {}
    for row in rows:
        groups.setdefault(row["division"], []).append(row)
    sections = []
    for division in sorted(groups):
        body = "".join(
            f"<tr><td>{r['div_rank']}</td><td>{r['school']}</td>"
            f"<td>{r['wins']}-{r['losses']}</td>"
            f"<td>{float(r['power_rating']):.3f}</td></tr>"
            for r in groups[division]
        )
        sections.append(
            f"<h2>{division}</h2><table><thead><tr><th>Rank</th>"
            f"<th>School</th><th>Record</th><th>Power Rating</th>"
            f"</tr></thead><tbody>{body}</tbody></table>"
        )
    return f"""<!doctype html><html><head><meta charset="utf-8">
    <style>body{{font-family:Arial,sans-serif;margin:0;color:#202124}}
    h1{{font-size:24px}}h2{{margin-top:28px}}table{{width:100%;
    border-collapse:collapse}}th,td{{padding:9px;border-bottom:1px solid
    #ddd;text-align:left}}th{{background:#f2f4f7;position:sticky;top:0}}
    </style></head><body><h1>{season} LHSAA Volleyball Power Ratings</h1>
    <p>{len(rows)} teams • Updated automatically</p>
    {''.join(sections)}</body></html>"""


# ── SCHEDULES ENDPOINTS ──────────────────────────────────────

@app.route("/api/schedules/football")
def schedules_football():
    requested_season = request.args.get("season")
    summary_only = request.args.get("summary") == "1"
    school_filter = (request.args.get("school") or "").strip()
    if requested_season:
        archived = football_archive_response(
            requested_season, summary_only, school_filter
        )
        if archived is not None:
            return jsonify(archived)

    conn = get_db()
    c = conn.cursor()
    try:
        season = available_schedule_season(conn, "football")
        roster_rows = c.execute("""
            SELECT school, division, track, class_, district, source, status
            FROM season_schools
            WHERE sport='football' AND season=?
            ORDER BY CAST(SUBSTR(class_,1,1) AS INTEGER) DESC,
                     CAST(district AS INTEGER), school
        """, (season,)).fetchall()

        if roster_rows:
            schools = [dict(row) for row in roster_rows]
        else:
            # Backward-compatible archive path for seasons created before the
            # season_schools table existed.
            schools = [
                dict(row)
                for row in c.execute("""
                    SELECT school, division, track, class_, district,
                           'LHSAA' AS source, 'final' AS status
                    FROM power_rankings
                    WHERE sport='football' AND season=?
                    ORDER BY class_ DESC, district ASC, school ASC
                """, (season,)).fetchall()
            ]

        football_roster_names = [
            str(school.get("school") or "").strip()
            for school in schools
            if school.get("school")
        ]

        if school_filter:
            schools = [
                school for school in schools
                if school["school"].casefold() == school_filter.casefold()
            ]

        ranking_rows = {
            row["school"]: dict(row)
            for row in c.execute("""
                SELECT school, power_rating, wins, losses, ties, games_played
                FROM power_rankings
                WHERE sport='football' AND season=?
            """, (season,)).fetchall()
        }
        ranking_rows_casefold = {
            name.casefold(): ranking
            for name, ranking in ranking_rows.items()
        }
        # Before the first completed football game there may be no ranking rows
        # yet.  The official season roster is still sufficient to identify an
        # in-state opponent and truthfully report its current 0-0 record.
        for roster_school_name in football_roster_names:
            ranking_rows_casefold.setdefault(
                roster_school_name.casefold(),
                {
                    "wins": 0,
                    "losses": 0,
                    "ties": 0,
                    "games_played": 0,
                },
            )

        for school in schools:
            ranking = ranking_rows.get(school["school"], {})
            school.update({
                "power_rating": ranking.get("power_rating"),
                "wins": ranking.get("wins", 0),
                "losses": ranking.get("losses", 0),
                "ties": ranking.get("ties", 0),
                "games_played": ranking.get("games_played", 0),
            })
            school["record"] = (
                f"{school['wins']}-{school['losses']}"
                + (f"-{school['ties']}" if school["ties"] else "")
            )
            if summary_only:
                school["games"] = []
                continue
            power_rows = {
                int(row["week"]): dict(row)
                for row in c.execute("""
                    SELECT week, result, score, opp_wins, opp_losses,
                           opp_division, base_pts, div_bonus, opp_quality,
                           total_pts, is_district
                    FROM game_power_points
                    WHERE sport='football' AND season=? AND school=?
                """, (season, school["school"])).fetchall()
            }
            games = []
            game_rows = c.execute("""
                SELECT week, opponent, win_loss, score, home_away, game_date,
                       district_class, is_district, needs_review, source
                FROM games
                WHERE sport='football' AND season=? AND school=?
                ORDER BY CAST(REPLACE(week,'Week ','') AS INTEGER)
            """, (season, school["school"])).fetchall()
            for row in game_rows:
                game = dict(row)
                try:
                    week_number = int(str(game["week"]).replace("Week", "").strip())
                except ValueError:
                    week_number = 0
                calculated = power_rows.get(week_number, {})
                opponent_name = str(game.get("opponent") or "").strip()
                opponent_ranking = ranking_rows_casefold.get(
                    opponent_name.casefold()
                )
                opp_wins = calculated.get("opp_wins")
                opp_losses = calculated.get("opp_losses")
                opp_ties = calculated.get("opp_ties")
                if (
                    (opp_wins is None or opp_losses is None)
                    and opponent_ranking is not None
                ):
                    opp_wins = opponent_ranking.get("wins", 0)
                    opp_losses = opponent_ranking.get("losses", 0)
                    opp_ties = opponent_ranking.get("ties", 0)
                parsed_game_date = parse_schedule_date(game.get("game_date"))
                game.update({
                    "week": week_number,
                    "game_date": (
                        f"{parsed_game_date.month}/{parsed_game_date.day}/"
                        f"{parsed_game_date.year}"
                        if parsed_game_date else game.get("game_date")
                    ),
                    "result": calculated.get("result") or game.pop("win_loss", None),
                    "score": calculated.get("score") or game.get("score"),
                    "opp_wins": opp_wins,
                    "opp_losses": opp_losses,
                    "opp_ties": opp_ties,
                    "opp_division": calculated.get("opp_division"),
                    "base_pts": calculated.get("base_pts"),
                    "div_bonus": calculated.get("div_bonus"),
                    "opp_quality": calculated.get("opp_quality"),
                    "total_pts": calculated.get("total_pts"),
                    "is_district": bool(
                        calculated.get("is_district", game.get("is_district"))
                    ),
                    "opponent_internal": opponent_ranking is not None,
                })
                games.append(game)
            school["games"] = sort_schedule_games(games)

    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({
        "sport": "football",
        "season": season,
        "status": schools[0].get("status", "active") if schools else "empty",
        "count": len(schools),
        "schools": schools,
    })


@app.route("/api/seasons/<sport>")
def sport_seasons(sport):
    conn = get_db()
    rows = conn.execute("""
        SELECT ss.season,
               COALESCE(sr.source, MAX(ss.source), '') AS source,
               COALESCE(sr.status, MAX(ss.status), 'active') AS status,
               COALESCE(sr.is_locked, 0) AS is_locked,
               COUNT(DISTINCT ss.school) AS school_count
        FROM season_schools ss
        LEFT JOIN season_registry sr
          ON sr.sport=ss.sport AND sr.season=ss.season
        WHERE ss.sport=?
        GROUP BY ss.season, sr.source, sr.status, sr.is_locked
    """, (sport,)).fetchall()
    known = {str(row["season"]): dict(row) for row in rows}
    for row in conn.execute("""
        SELECT season, COUNT(DISTINCT school) AS school_count
        FROM games WHERE sport=? GROUP BY season
    """, (sport,)).fetchall():
        season = str(row["season"])
        known.setdefault(season, {
            "season": season,
            "source": "LHSAA",
            "status": "final" if int(season) < int(resolve_season(sport)) else "active",
            "school_count": row["school_count"],
        })
    for row in conn.execute("""
        SELECT season, source, status, is_locked
        FROM season_registry WHERE sport=?
    """, (sport,)).fetchall():
        season = str(row["season"])
        known.setdefault(season, {
            "season": season,
            "school_count": 0,
        })
        known[season].update({
            "source": row["source"],
            "status": row["status"],
            "is_locked": bool(row["is_locked"]),
        })
    conn.close()
    if sport == "football":
        for season, archive in load_football_archives().get(
            "seasons", {}
        ).items():
            known.setdefault(season, {
                "season": season,
                "source": "Airtable archive",
                "status": archive.get("status", "final"),
                "is_locked": True,
                "school_count": archive.get("count", 0),
            })
    seasons = sorted(known.values(), key=lambda row: int(row["season"]), reverse=True)
    return jsonify({
        "sport": sport,
        "current_season": resolve_season(sport),
        "seasons": seasons,
    })


@app.route("/api/schedules/baseball")
def schedules_baseball():
    return get_sport_schedules("baseball")


@app.route("/api/schedules/softball")
def schedules_softball():
    return get_sport_schedules("softball")


@app.route("/api/schedules/volleyball")
def schedules_volleyball():
    conn = get_db()
    c = conn.cursor()
    try:
        season = available_season(conn, "volleyball", "volleyball_rankings")
        # Get all schools with their ranking info
        c.execute("""
            SELECT vr.school, vr.division, vr.class_, vr.district,
                   vr.power_rating, vr.wins, vr.losses, vr.games_played, vr.rank, vr.div_rank
            FROM volleyball_rankings vr
            WHERE vr.sport='volleyball' AND vr.season=?
            ORDER BY vr.division ASC, vr.district ASC, vr.school ASC
        """, (season,))
        school_rows = [dict(r) for r in c.fetchall()]
        # Reuse the calculated rankings table as the source of truth for every
        # opponent's live record.  Schedule rows previously omitted this data,
        # which also meant the front end could not show per-match power points.
        opponent_records = {
            str(row["school"] or "").strip().casefold(): {
                "wins": int(row["wins"] or 0),
                "losses": int(row["losses"] or 0),
                "games_played": int(row["games_played"] or 0),
            }
            for row in school_rows
        }
        schools = []

        for s in school_rows:
            c.execute("""
                SELECT game_date, opponent, opp_division, opp_district,
                       is_district, is_tournament, tournament_name,
                       match_num, home_away, result, score, counts_for_pr
                FROM volleyball_games
                WHERE sport='volleyball' AND season=? AND school=?
                ORDER BY game_date ASC, match_num ASC
            """, (season, s["school"]))
            games = sort_schedule_games([dict(r) for r in c.fetchall()])

            annotate_volleyball_games(games, opponent_records)

            schools.append({
                "school":       s["school"],
                "sport":        "volleyball",
                "season":       season,
                "division":     s.get("division", ""),
                "class_":       s.get("class_", ""),
                "district":     s.get("district"),
                "power_rating": s.get("power_rating", 0),
                "rank":         s.get("rank", 0),
                "div_rank":     s.get("div_rank", 0),
                "wins":         s.get("wins", 0),
                "losses":       s.get("losses", 0),
                "games_played": s.get("games_played", 0),
                "games":        games,
            })

    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500

    conn.close()
    return jsonify({
        "sport":   "volleyball",
        "season":  season,
        "count":   len(schools),
        "schools": schools,
    })


def get_sport_schedules(sport):
    conn = get_db()
    c = conn.cursor()
    school_filter = (request.args.get("school") or "").strip()
    summary_only = request.args.get("summary") == "1"
    requested_season = (request.args.get("season") or "").strip()
    season = requested_season if re.fullmatch(r"\d{4}", requested_season) else available_season(conn, sport)

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
    opponent_records = {
        str(row.get("school") or "").strip().casefold(): {
            "wins": int(row.get("wins") or 0),
            "losses": int(row.get("losses") or 0),
            "ties": int(row.get("ties") or 0),
            "games_played": int(row.get("games_played") or 0),
        }
        for row in school_rows
    }
    schools = []

    for s in school_rows:
        school = s['school']
        if summary_only:
            schools.append({
                **s,
                "sport": sport,
                "season": season,
                "record": (
                    f"{s.get('wins', 0)}-{s.get('losses', 0)}"
                    + (f"-{s.get('ties', 0)}" if s.get("ties") else "")
                ),
                "games": [],
            })
            continue
        c.execute("""
            SELECT gpp.opponent, gpp.result, gpp.score,
                   gpp.opp_wins, gpp.opp_losses, gpp.opp_ties, gpp.opp_division,
                   gpp.base_pts, gpp.div_bonus, gpp.opp_quality,
                   gpp.total_pts, gpp.is_district,
                   gpp.game_date, gpp.home_away
            FROM game_power_points gpp
            WHERE gpp.sport=? AND gpp.season=? AND gpp.school=?
            ORDER BY gpp.week ASC
        """, (sport, season, school))

        games = sort_schedule_games([dict(r) for r in c.fetchall()])
        annotate_current_opponent_records(games, opponent_records)

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
        "status":  "final" if int(season) < int(resolve_season(sport)) else ("active" if schools else "empty"),
        "count":   len(schools),
        "schools": schools
    })


# ── BREAKDOWN ENDPOINTS ──────────────────────────────────────

@app.route("/api/breakdown/football/<school>")
def breakdown_football(school):
    conn = get_db()
    c = conn.cursor()
    try:
        season = available_season(conn, "football", "game_power_points")
        c.execute("""
            SELECT week, opponent, result, score,
                   opp_wins, opp_losses, opp_division,
                   base_pts, div_bonus, opp_quality, total_pts
            FROM game_power_points
            WHERE sport='football' AND season=? AND school=?
            ORDER BY week ASC
        """, (season, school))
        rows = [dict(r) for r in c.fetchall()]
        total = sum(r["total_pts"] for r in rows)
        pr = round(total / len(rows), 2) if rows else 0
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"school": school, "season": season, "calculated_pr": pr, "games": rows})


@app.route("/api/breakdown/baseball/<school>")
def breakdown_baseball(school):
    conn = get_db()
    c = conn.cursor()
    try:
        season = available_season(conn, "baseball", "game_power_points")
        c.execute("""
            SELECT week, opponent, result, score,
                   opp_wins, opp_losses, opp_division,
                   base_pts, div_bonus, opp_quality, total_pts, is_district
            FROM game_power_points
            WHERE sport='baseball' AND season=? AND school=?
            ORDER BY week ASC
        """, (season, school))
        rows = [dict(r) for r in c.fetchall()]
        total = sum(r["total_pts"] for r in rows)
        pr = round(total / len(rows), 2) if rows else 0
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"school": school, "season": season, "calculated_pr": pr, "games": rows})


@app.route("/api/breakdown/softball/<school>")
def breakdown_softball(school):
    conn = get_db()
    c = conn.cursor()
    try:
        season = available_season(conn, "softball", "game_power_points")
        c.execute("""
            SELECT week, opponent, result, score,
                   opp_wins, opp_losses, opp_division,
                   base_pts, div_bonus, opp_quality, total_pts, is_district
            FROM game_power_points
            WHERE sport='softball' AND season=? AND school=?
            ORDER BY week ASC
        """, (season, school))
        rows = [dict(r) for r in c.fetchall()]
        total = sum(r["total_pts"] for r in rows)
        pr = round(total / len(rows), 2) if rows else 0
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({"school": school, "season": season, "calculated_pr": pr, "games": rows})


@app.route("/api/breakdown/volleyball/<school>")
def breakdown_volleyball(school):
    conn = get_db()
    c = conn.cursor()
    try:
        season = available_season(conn, "volleyball", "volleyball_rankings")
        c.execute("""
            SELECT game_date, opponent, opp_division,
                   is_district, is_tournament, tournament_name,
                   home_away, result, score, counts_for_pr
            FROM volleyball_games
            WHERE sport='volleyball' AND season=? AND school=?
            ORDER BY game_date ASC, match_num ASC
        """, (season, school))
        games = sort_schedule_games([dict(r) for r in c.fetchall()])

        c.execute("""
            SELECT power_rating, wins, losses, games_played, rank, div_rank, division
            FROM volleyball_rankings
            WHERE sport='volleyball' AND season=? AND school=?
        """, (season, school))
        row = c.fetchone()
        pr_info = dict(row) if row else {}

    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500
    conn.close()
    return jsonify({
        "school":       school,
        "season":       season,
        "power_rating": pr_info.get("power_rating", 0),
        "wins":         pr_info.get("wins", 0),
        "losses":       pr_info.get("losses", 0),
        "games_played": pr_info.get("games_played", 0),
        "rank":         pr_info.get("rank"),
        "div_rank":     pr_info.get("div_rank"),
        "division":     pr_info.get("division", ""),
        "games":        games,
    })


# ── CONTROL PANEL ────────────────────────────────────────────

WINTER_SPORTS = {
    "boys_basketball", "girls_basketball",
    "boys_soccer", "girls_soccer",
}


@app.route("/api/pipeline/winter", methods=["POST"])
def run_winter_pipeline():
    if not PIPELINE_LOCK.acquire(blocking=False):
        return jsonify({
            "status": "already_running",
            "started_at": PIPELINE_STATE["started_at"],
        }), 409
    season = request.args.get("season") or resolve_season("boys_basketball")

    def run():
        PIPELINE_STATE.update({
            "status": "running_winter",
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "error": None,
        })
        try:
            from scraper import scrape_sport
            from run_power_rankings import run_power_rankings
            from sheets_exporter import export_winter_sport_to_sheets
            for sport in sorted(WINTER_SPORTS):
                os.environ[f"{sport.upper()}_SEASON_YEAR"] = str(season)
                scrape_sport(sport)
                run_power_rankings(sport=sport, season=str(season))
                export_winter_sport_to_sheets(sport, str(season))
            PIPELINE_STATE["status"] = "completed"
        except Exception as exc:
            PIPELINE_STATE["status"] = "failed"
            PIPELINE_STATE["error"] = str(exc)
            print(f"Winter pipeline error: {exc}")
        finally:
            PIPELINE_STATE["finished_at"] = datetime.now().isoformat()
            PIPELINE_LOCK.release()

    threading.Thread(target=run, daemon=True).start()
    return jsonify({
        "status": "started", "season": str(season),
        "sports": sorted(WINTER_SPORTS),
    }), 202


@app.route("/api/scrape/winter/<sport>")
def scrape_winter_sport(sport):
    if sport not in WINTER_SPORTS:
        return jsonify({"error": "Unsupported sport"}), 404

    season = request.args.get("season") or resolve_season(sport)

    def run():
        try:
            from scraper import scrape_sport
            os.environ[f"{sport.upper()}_SEASON_YEAR"] = str(season)
            scrape_sport(sport)
        except Exception as exc:
            print(f"{sport} scrape error: {exc}")

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "sport": sport, "season": str(season)})


@app.route("/api/build/winter/<sport>")
def build_winter_sport_sheets(sport):
    if sport not in WINTER_SPORTS:
        return jsonify({"error": "Unsupported sport"}), 404
    conn = get_db()
    season = available_season(conn, sport)
    conn.close()

    def run():
        try:
            from sheets_exporter import export_winter_sport_to_sheets
            print(export_winter_sport_to_sheets(sport, season))
        except Exception as exc:
            print(f"{sport} sheets error: {exc}")

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "sport": sport, "season": season})


@app.route("/api/recalculate/winter/<sport>", methods=["POST"])
def recalculate_winter_sport(sport):
    """Rebuild one winter sport from the schedules already stored in the database."""
    if sport not in WINTER_SPORTS:
        return jsonify({"error": "Unsupported sport"}), 404
    if not PIPELINE_LOCK.acquire(blocking=False):
        return jsonify({
            "status": "already_running",
            "started_at": PIPELINE_STATE["started_at"],
        }), 409
    season = request.args.get("season") or resolve_season(sport)

    def run():
        PIPELINE_STATE.update({
            "status": f"recalculating_{sport}",
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "error": None,
        })
        try:
            from run_power_rankings import run_power_rankings
            run_power_rankings(sport=sport, season=str(season))
            PIPELINE_STATE["status"] = "completed"
        except Exception as exc:
            PIPELINE_STATE["status"] = "failed"
            PIPELINE_STATE["error"] = str(exc)
            print(f"{sport} recalculation error: {exc}")
        finally:
            PIPELINE_STATE["finished_at"] = datetime.now().isoformat()
            PIPELINE_LOCK.release()

    threading.Thread(target=run, daemon=True).start()
    return jsonify({
        "status": "started",
        "sport": sport,
        "season": str(season),
    }), 202


@app.route("/api/rankings/winter/<sport>")
def rankings_winter_sport(sport):
    if sport not in WINTER_SPORTS:
        return jsonify({"error": "Unsupported sport"}), 404
    conn = get_db()
    season = available_season(conn, sport)
    rows = conn.execute("""
        SELECT school, division, track, class_, district, rank, power_rating,
               wins, losses, ties, games_played, strength_factor, calculated_at
        FROM power_rankings
        WHERE sport=? AND season=?
        ORDER BY rank
    """, (sport, season)).fetchall()
    result = [dict(row) for row in rows]
    conn.close()
    return jsonify({
        "sport": sport, "season": season, "count": len(result),
        "rankings": result,
    })


@app.route("/api/schedules/winter/<sport>")
def schedules_winter_sport(sport):
    if sport not in WINTER_SPORTS:
        return jsonify({"error": "Unsupported sport"}), 404
    return get_sport_schedules(sport)


@app.route("/api/breakdown/winter/<sport>/<school>")
def breakdown_winter_sport(sport, school):
    if sport not in WINTER_SPORTS:
        return jsonify({"error": "Unsupported sport"}), 404
    conn = get_db()
    season = available_season(conn, sport, "game_power_points")
    rows = conn.execute("""
        SELECT week, game_date, opponent, result, score, opp_wins, opp_losses,
               opp_ties, opp_division, base_pts, div_bonus, opp_quality,
               total_pts, is_district
        FROM game_power_points
        WHERE sport=? AND season=? AND school=?
        ORDER BY week
    """, (sport, season, school)).fetchall()
    games = [dict(row) for row in rows]
    conn.close()
    total = sum(float(row["total_pts"] or 0) for row in games)
    return jsonify({
        "sport": sport, "school": school, "season": season,
        "calculated_pr": round(total / len(games), 2) if games else 0,
        "games": games,
    })


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
  <div class="formula-row"><span class="formula-label">In-state div bonus</span><span class="formula-val">+2</span><span class="formula-note">per div higher</span></div>
  <div class="formula-row"><span class="formula-label">OOS class bonus</span><span class="formula-val">+2</span><span class="formula-note">per class higher</span></div>
  <div class="formula-row"><span class="formula-label">Opponent quality (OppQ)</span><span class="formula-val">×10</span><span class="formula-note">(opp wins ÷ opp games) × 10</span></div>
  <div class="formula-row"><span class="formula-label">Final power rating</span><span class="formula-val">÷ GP</span><span class="formula-note">total points ÷ games played</span></div>
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
      <thead><tr><th>#</th><th>School</th><th>Class</th><th>Record</th><th>GP</th><th>PR</th><th>SF</th></tr></thead>
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
  <div class="checklist-item"><div class="dot done"></div><span>Google Sheets exporter</span></div>
  <div class="checklist-item"><div class="dot done"></div><span>WordPress display via shortcodes</span></div>
  <div class="checklist-item"><div class="dot done"></div><span>99.3% accuracy vs LHSAA (301/303 exact)</span></div>
  <div class="checklist-item"><div class="dot done"></div><span>Strength Factor (SF) tiebreaker column added</span></div>
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
      const sf = (+(s.strength_factor||0)).toFixed(2);
      const badge = s.division.startsWith('Select')
        ? '<span class="badge badge-s">S</span>'
        : '<span class="badge badge-ns">NS</span>';
      tr.innerHTML = `<td>${i+1}</td><td>${s.school}${badge}</td><td>${cls}</td><td>${rec}</td><td>${s.games_played||'—'}</td><td><strong>${(+s.power_rating).toFixed(2)}</strong></td><td>${sf}</td>`;
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
      const r = await fetch('/api/breakdown/football/' + encodeURIComponent(name));
      if (!r.ok) throw new Error('not found');
      const d = await r.json();
      document.getElementById('s-name').textContent = d.school;
      document.getElementById('s-pr').textContent = (+(d.calculated_pr||0)).toFixed(2);
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


# ── ENTRY POINT ──────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
