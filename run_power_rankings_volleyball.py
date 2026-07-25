"""
LVAY Volleyball Power Rankings Engine
======================================
Calculates power ratings for all volleyball schools using the LHSAA formula.

Formula (Bylaw 24.6.3):
    For each match that counts (counts_for_pr = 1):
        Win:  5 base points + opponent_wins * 1.0  (100% of opp wins)
        Loss: 0 base points + opponent_wins * 0.33 (33% of opp wins)

    PR = total_power_points / total_matches_played

Opponent wins = total wins by that opponent across ALL their matches
(same logic as football — we look up each opponent's win total from the DB)

Exclusions already flagged in scraper (counts_for_pr = 0):
    - OOS opponents
    - District playoff tiebreaker matches (D flag)

Inclusions:
    - Regular season matches
    - Tournament matches (T flag)

Season: 2025 (fall sport, 2025-2026 school year)
"""

import sqlite3
import os
from datetime import datetime
from school_database import get_school

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────────────────

SPORT  = "volleyball"
SEASON = os.environ.get("VOLLEYBALL_SEASON_YEAR", str(datetime.now().year))

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")

# PR formula constants
WIN_BASE       = 5.0
LOSS_BASE      = 0.0
WIN_OPP_PCT    = 1.00   # 100% of opponent wins on a win
LOSS_OPP_PCT   = 0.33   # 33% of opponent wins on a loss

# Volleyball divisions I–V
DIVISION_ORDER = [
    "Division I",
    "Division II",
    "Division III",
    "Division IV",
    "Division V",
]


# ──────────────────────────────────────────────────────────────────────────────
# DB HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ──────────────────────────────────────────────────────────────────────────────
# STEP 1 — BUILD OPPONENT WIN TOTALS
# ──────────────────────────────────────────────────────────────────────────────

def apply_result_override(row, overrides):
    key = (
        SPORT,
        str(SEASON),
        str(row["school"] or "").strip().lower(),
        str(row["game_date"] or "").strip(),
        str(row["opponent"] or "").strip().lower(),
    )
    override = overrides.get(key, {})
    return override.get("override_win_loss") or row["result"]


def build_opponent_win_totals(conn, overrides=None):
    """
    For every school in volleyball_games, count their total wins
    across all matches that count_for_pr=1.
    Returns dict: {school_name: win_count}
    """
    rows = conn.execute("""
        SELECT school, game_date, opponent, result
        FROM volleyball_games
        WHERE sport=? AND season=? AND counts_for_pr=1
    """, (SPORT, SEASON)).fetchall()

    win_totals = {}
    for row in rows:
        school = row["school"]
        if school not in win_totals:
            win_totals[school] = 0
        if apply_result_override(row, overrides or {}) == "W":
            win_totals[school] += 1

    return win_totals


# ──────────────────────────────────────────────────────────────────────────────
# STEP 2 — CALCULATE PR PER SCHOOL
# ──────────────────────────────────────────────────────────────────────────────

def calculate_school_pr(school_name, games, opp_win_totals):
    """
    Calculate power rating for one school.

    games: list of Row objects from volleyball_games for this school
           where counts_for_pr = 1

    Returns dict with wins, losses, games_played, power_rating
    """
    wins         = 0
    losses       = 0
    total_points = 0.0

    for g in games:
        result   = g["result"]
        opponent = g["opponent"]
        opp_wins = opp_win_totals.get(opponent, 0)

        if result == "W":
            wins += 1
            pts   = WIN_BASE + (opp_wins * WIN_OPP_PCT)
        elif result == "L":
            losses += 1
            pts    = LOSS_BASE + (opp_wins * LOSS_OPP_PCT)
        else:
            continue  # skip blank/unknown results

        total_points += pts

    games_played = wins + losses
    if games_played == 0:
        pr = 0.0
    else:
        pr = total_points / games_played

    return {
        "wins":         wins,
        "losses":       losses,
        "games_played": games_played,
        "power_rating": round(pr, 3),
    }


# ──────────────────────────────────────────────────────────────────────────────
# STEP 3 — RESOLVE SCHOOL METADATA FROM school_database.py
# ──────────────────────────────────────────────────────────────────────────────

def resolve_school_metadata(school_name, vb_division_roman):
    """
    Get class, district, and full division label for a school.
    Uses school_database.py as source of truth for class/district.
    Volleyball division comes from the scraper (I–V).
    """
    info = get_school(school_name)

    division_label = f"Division {vb_division_roman}"

    if info:
        return {
            "division": division_label,
            "class_":   info.get("class") or "Unknown",
            "district": info.get("district"),
        }
    else:
        return {
            "division": division_label,
            "class_":   "Unknown",
            "district": None,
        }


# ──────────────────────────────────────────────────────────────────────────────
# STEP 4 — WRITE RANKINGS TO DB
# ──────────────────────────────────────────────────────────────────────────────

def upsert_ranking(conn, school, meta, stats):
    conn.execute("""
        INSERT INTO volleyball_rankings
            (sport, season, school, division, class_, district,
             wins, losses, games_played, power_rating, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(sport, season, school) DO UPDATE SET
            division     = excluded.division,
            class_       = excluded.class_,
            district     = excluded.district,
            wins         = excluded.wins,
            losses       = excluded.losses,
            games_played = excluded.games_played,
            power_rating = excluded.power_rating,
            updated_at   = datetime('now')
    """, (
        SPORT, SEASON, school,
        meta["division"], meta["class_"], meta["district"],
        stats["wins"], stats["losses"], stats["games_played"],
        stats["power_rating"],
    ))


def update_division_ranks(conn):
    """
    After all schools are written, assign rank (overall) and
    div_rank (within division) ordered by power_rating DESC.
    """
    # Overall rank
    schools = conn.execute("""
        SELECT school FROM volleyball_rankings
        WHERE sport=? AND season=?
        ORDER BY power_rating DESC
    """, (SPORT, SEASON)).fetchall()

    for i, row in enumerate(schools, 1):
        conn.execute("""
            UPDATE volleyball_rankings SET rank=?
            WHERE sport=? AND season=? AND school=?
        """, (i, SPORT, SEASON, row["school"]))

    # Division rank
    divisions = conn.execute("""
        SELECT DISTINCT division FROM volleyball_rankings
        WHERE sport=? AND season=?
    """, (SPORT, SEASON)).fetchall()

    for div_row in divisions:
        div = div_row["division"]
        div_schools = conn.execute("""
            SELECT school FROM volleyball_rankings
            WHERE sport=? AND season=? AND division=?
            ORDER BY power_rating DESC
        """, (SPORT, SEASON, div)).fetchall()

        for i, row in enumerate(div_schools, 1):
            conn.execute("""
                UPDATE volleyball_rankings SET div_rank=?
                WHERE sport=? AND season=? AND school=?
            """, (i, SPORT, SEASON, row["school"]))

    conn.commit()


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def run_volleyball_rankings():
    print(f"\n{'='*54}")
    print(f"LVAY Volleyball Power Rankings")
    print(f"Sport: {SPORT.upper()}  Season: {SEASON}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*54}")

    conn = get_db()
    try:
        from run_power_rankings import load_sheet_overrides
        overrides = load_sheet_overrides(SPORT, SEASON)
    except Exception as e:
        print(f"  Overrides unavailable: {e}")
        overrides = {}

    # Build opponent win totals first
    print("  Building opponent win totals...")
    opp_win_totals = build_opponent_win_totals(conn, overrides)
    print(f"  Win totals built for {len(opp_win_totals)} schools")

    # Get all distinct schools and their volleyball division
    school_rows = conn.execute("""
        SELECT DISTINCT school, school_division
        FROM volleyball_games
        WHERE sport=? AND season=? AND counts_for_pr=1
        ORDER BY school
    """, (SPORT, SEASON)).fetchall()

    print(f"  {len(school_rows)} schools found in game data")
    print("  Calculating power ratings...")

    unmatched = []
    ranked    = 0

    for sr in school_rows:
        school_name  = sr["school"]
        vb_div_roman = sr["school_division"] or "Unknown"

        # Fetch this school's countable games
        games = conn.execute("""
            SELECT school, game_date, result, opponent
            FROM volleyball_games
            WHERE sport=? AND season=? AND school=? AND counts_for_pr=1
        """, (SPORT, SEASON, school_name)).fetchall()

        # Calculate PR
        games = [dict(g) for g in games]
        for game in games:
            game["result"] = apply_result_override(game, overrides)
        stats = calculate_school_pr(school_name, games, opp_win_totals)

        # Resolve metadata
        meta = resolve_school_metadata(school_name, vb_div_roman)
        if meta["class_"] == "Unknown":
            unmatched.append(school_name)

        # Write to rankings table
        upsert_ranking(conn, school_name, meta, stats)
        ranked += 1

    # Assign ranks
    update_division_ranks(conn)
    conn.commit()

    # Pull top 5 per division for summary
    print(f"  Power ratings calculated for {ranked} schools")
    print(f"{'='*54}")
    print(f"DONE!")
    print(f"  Schools ranked    : {ranked}")
    print(f"  Unmatched schools : {len(unmatched)}")

    if unmatched:
        print(f"\nUNMATCHED SCHOOLS (not in school_database.py):")
        for s in unmatched:
            print(f"  - {s}")

    print(f"\nTop 3 per Division:")
    for div in DIVISION_ORDER:
        top = conn.execute("""
            SELECT school, power_rating, wins, losses
            FROM volleyball_rankings
            WHERE sport=? AND season=? AND division=?
            ORDER BY power_rating DESC
            LIMIT 3
        """, (SPORT, SEASON, div)).fetchall()

        if top:
            print(f"\n  {div}:")
            for i, row in enumerate(top, 1):
                print(f"    #{i} {row['school']} | PR={row['power_rating']:.3f} | "
                      f"{row['wins']}-{row['losses']}")

    print(f"\n{'='*54}\n")
    conn.close()

    return {"ranked": ranked, "unmatched": len(unmatched)}


if __name__ == "__main__":
    run_volleyball_rankings()
