# ============================================================
# THREE FIX ENDPOINTS + TWO UTILITY ENDPOINTS
# Add these to your server.py BEFORE the if __name__ block
# ============================================================

@app.route("/api/fix/oberlin-bolton")
def fix_oberlin_bolton():
    """Delete the bad Bolton game from Oberlin Week 10."""
    conn = sqlite3.connect("/data/lvay_v2.db")
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
    """Fix Glenbrook open date miscoded as a real game."""
    conn = sqlite3.connect("/data/lvay_v2.db")
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
    """Insert St. Frederick WK8 Cypress Christian TX game (missed by scraper)."""
    conn = sqlite3.connect("/data/lvay_v2.db")
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
        return jsonify({"status": "ok", "rows_inserted": rows, "message": "St. Frederick Wk8 OOS game added (S3 corrected)"})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 500


@app.route("/api/import/oos2025")
def import_oos_2025():
    """Import 2025 OOS opponent records into DB."""
    import threading
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
    """Trigger power rankings calculation in background."""
    import threading
    def run():
        try:
            from run_power_rankings import run as do_rankings
            do_rankings()
        except Exception as e:
            print(f"Rankings calc error: {e}")
    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status": "started", "message": "Power rankings calculating — check logs"})
