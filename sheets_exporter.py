"""
LVAY - Google Sheets Exporter
================================
NO formatting calls — pure data only.
Scores tab is built separately via /api/build/football-scores
to avoid Sheets API timeouts.

Main build (/api/build/football-sheets) writes:
  - Football Power Rankings (2025)  ← from power_rankings table
  - Football Needs Review
  - Football District Records
  - Instructions

Separate build (/api/build/football-scores) writes:
  - Football Scores (2025)          ← 2997 rows, slow
"""

import gspread
from google.oauth2.service_account import Credentials
import sqlite3
import json
import os
import re
import time
from datetime import datetime

DB_PATH  = os.environ.get("DB_PATH", "/data/lvay_v2.db")
SHEET_ID = os.environ.get(
    "GOOGLE_SHEET_ID",
    "1u_cJBAWTQJIAO36HZTYvPa7QfE0JoOEqx12c1U4t4mk",
)
SEASON = int(os.environ.get("FOOTBALL_SEASON_YEAR", datetime.now().year))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

DIVISION_ORDER = [
    "Division I",
    "Division II",
    "Division III",
    "Division IV",
    "Division V",
    "Non-Select Division I",
    "Non-Select Division II",
    "Non-Select Division III",
    "Non-Select Division IV",
    "Select Division I",
    "Select Division II",
    "Select Division III",
    "Select Division IV",
]

DIVISION_LABELS = {
    "Division I":              "Div I",
    "Division II":             "Div II",
    "Division III":            "Div III",
    "Division IV":             "Div IV",
    "Division V":              "Div V",
    "Non-Select Division I":   "NS I",
    "Non-Select Division II":  "NS II",
    "Non-Select Division III": "NS III",
    "Non-Select Division IV":  "NS IV",
    "Select Division I":       "S I",
    "Select Division II":      "S II",
    "Select Division III":     "S III",
    "Select Division IV":      "S IV",
}


# ─── AUTH ─────────────────────────────────────────────────────────────────────

def get_client():
    secret_path = "/etc/secrets/google-credentials.json"
    if os.path.exists(secret_path):
        with open(secret_path, "r") as f:
            creds_dict = json.load(f)
    else:
        creds_json = (
            os.environ.get("GOOGLE_CREDENTIALS_JSON")
            or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        )
        if not creds_json:
            raise ValueError("No Google credentials found")
        creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def get_or_create_tab(sheet, tab_name, rows=3000, cols=20):
    try:
        ws = sheet.worksheet(tab_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
    time.sleep(2)
    return ws


def batch_write(ws, start_row, data, chunk_size=100):
    """Write data in small chunks with generous sleep to avoid 429s."""
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        ws.update(f"A{start_row + i}", chunk)
        time.sleep(2)


def ensure_sport_overrides_tab(sheet, sport, season):
    """Create a sport's manual-correction tab without clearing existing edits."""
    sport = sport.lower()
    tab_name = f"{sport.replace('_', ' ').title()} Overrides ({season})"
    headers = [
        "sport", "season", "school", "game_date", "opponent", "active",
        "override_win_loss", "override_score", "override_home_away", "notes",
    ]
    try:
        ws = sheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sheet.add_worksheet(title=tab_name, rows=1000, cols=len(headers))

    first_row = ws.row_values(1)
    if not first_row:
        ws.update("A1", [headers])
        ws.freeze(rows=1)
        ws.format("A1:J1", {
            "backgroundColor": {"red": 0.12, "green": 0.29, "blue": 0.48},
            "textFormat": {
                "bold": True,
                "foregroundColor": {"red": 1, "green": 1, "blue": 1},
            },
        })
        ws.update("A2", [[
            sport, str(season), "", "", "", False, "", "", "",
            "Enter one correction per row; set active to TRUE to apply it.",
        ]])
    elif first_row[:len(headers)] != headers:
        raise ValueError(
            f"{tab_name} has unexpected headers; manual corrections were not changed"
        )
    print(f"    Ready: {tab_name} (manual corrections preserved)")
    return ws


def ensure_football_overrides_tab(sheet, season=SEASON):
    return ensure_sport_overrides_tab(sheet, "football", season)


def game_review_issues(result, score, flagged=False, school_score_second=False):
    """Return actionable data-quality issues for a played game."""
    result = str(result or "").strip()
    score = str(score or "").strip()
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
        import re
        numbers = [int(n) for n in re.findall(r"\d+", score)]
        if len(numbers) < 2:
            issues.append("malformed score")
        school_score, opponent_score = (
            (numbers[1], numbers[0]) if school_score_second
            else (numbers[0], numbers[1])
        )
        if result in ("W", "W(f)") and school_score <= opponent_score:
            issues.append("W conflicts with score")
        elif result in ("L", "L(f)") and school_score >= opponent_score:
            issues.append("L conflicts with score")
        elif result in ("T", "Tie") and school_score != opponent_score:
            issues.append("tie conflicts with score")
    if flagged:
        issues.append("flagged")
    return issues


# ─── FOOTBALL POWER RANKINGS ──────────────────────────────────────────────────

def build_football_power_rankings(sheet, season=SEASON):
    tab_name = f"Football Power Rankings ({season})"
    print(f"  Building {tab_name}...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Read from power_rankings table
    all_schools = []
    try:
        c.execute("""
            SELECT school, division, track, class_, district,
                   wins, losses, ties, games_played, power_rating, rank
            FROM power_rankings
            WHERE sport='football' AND season=?
            ORDER BY rank ASC
        """, (str(season),))
        all_schools = [dict(r) for r in c.fetchall()]
        print(f"    Loaded {len(all_schools)} schools from power_rankings table")
    except Exception as e:
        print(f"    ERROR reading power_rankings: {e}")

    conn.close()

    if not all_schools:
        print(f"    No data — run /api/rankings/calculate first")
        return 0

    # Group by division
    by_division = {div: [] for div in DIVISION_ORDER}
    unmatched = []
    for s in all_schools:
        div = s.get("division") or ""
        if div in by_division:
            by_division[div].append(s)
        else:
            unmatched.append(s)

    # Already sorted by rank from DB, but re-sort within division by power_rating
    for div in DIVISION_ORDER:
        by_division[div].sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)

    now_str     = datetime.now().strftime("%m/%d/%Y %I:%M %p")
    col_headers = ["Div Rank", "School", "Division", "Class", "District", "W", "L", "Games", "Power Rating"]

    ws = get_or_create_tab(sheet, tab_name)

    all_rows = []
    all_rows.append([f"LVAY Football Power Rankings {season} — Updated {now_str}"] + [""] * 8)
    all_rows.append(col_headers)

    total = 0
    for division in DIVISION_ORDER:
        schools = by_division[division]
        if not schools:
            continue
        all_rows.append([f"=== {division.upper()} ==="] + [""] * 8)
        for rank, s in enumerate(schools, 1):
            all_rows.append([
                rank,
                s.get("school") or "",
                DIVISION_LABELS.get(s.get("division", ""), s.get("division") or ""),
                s.get("class_") or "",
                s.get("district") or "",
                s.get("wins") or 0,
                s.get("losses") or 0,
                s.get("games_played") or 0,
                round(float(s.get("power_rating") or 0), 2),
            ])
            total += 1
        all_rows.append([""] * 9)

    if unmatched:
        unmatched.sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)
        all_rows.append(["=== UNMATCHED / NO DIVISION ==="] + [""] * 8)
        for rank, s in enumerate(unmatched, 1):
            all_rows.append([
                rank,
                s.get("school") or "",
                s.get("division") or "Unknown",
                s.get("class_") or "",
                s.get("district") or "",
                s.get("wins") or 0,
                s.get("losses") or 0,
                s.get("games_played") or 0,
                round(float(s.get("power_rating") or 0), 2),
            ])
            total += 1

    batch_write(ws, 1, all_rows)
    print(f"    Written {total} school rankings")
    return total


# ─── NEEDS REVIEW ─────────────────────────────────────────────────────────────

def build_needs_review(sheet, season=SEASON):
    tab_name = "Football Needs Review"
    print(f"  Building {tab_name}...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT school, week, game_date, opponent, win_loss, score,
               class_, district, district_class, needs_review
        FROM games
        WHERE sport='football' AND season=?
        ORDER BY school, week
    """, (str(season),))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name)
    ws.update("A1", [[
        "School", "Week", "Date", "Opponent", "W/L", "Score",
        "Class", "District", "District/Class", "Issue"
    ]])
    time.sleep(1)

    data = []
    for r in rows:
        issues = []
        issues = game_review_issues(r["win_loss"], r["score"], r["needs_review"])
        if not issues:
            continue
        data.append([
            r["school"] or "", r["week"] or "", r["game_date"] or "",
            r["opponent"] or "", r["win_loss"] or "", r["score"] or "",
            r["class_"] or "", r["district"] or "", r["district_class"] or "",
            ", ".join(issues),
        ])

    if data:
        batch_write(ws, 2, data)
        print(f"    {len(data)} games need review")
    else:
        ws.update("A2", [["No issues found!"]])
        print(f"    No issues found!")

    return len(data)


# ─── DISTRICT RECORDS ─────────────────────────────────────────────────────────

def build_district_records(sheet, season=SEASON):
    tab_name = "Football District Records"
    print(f"  Building {tab_name}...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT g.school,
               pr.division, pr.track,
               g.class_, g.district, g.district_class,
            SUM(CASE WHEN g.win_loss='W' THEN 1 ELSE 0 END) as total_wins,
            SUM(CASE WHEN g.win_loss='L' THEN 1 ELSE 0 END) as total_losses,
            pr.power_rating, pr.rank
        FROM games g
        LEFT JOIN power_rankings pr
            ON pr.school = g.school
            AND pr.sport = 'football'
            AND pr.season = g.season
        WHERE g.sport='football' AND g.season=?
        GROUP BY g.school
        ORDER BY pr.division, g.district,
                 total_wins DESC, total_losses ASC
    """, (str(season),))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name)
    ws.update("A1", [[
        "Rank", "School", "Division", "Class", "District",
        "Total W", "Total L", "Power Rating"
    ]])
    time.sleep(1)

    data = []
    for r in rows:
        data.append([
            r["rank"] or "",
            r["school"] or "",
            DIVISION_LABELS.get(r["division"], r["division"] or ""),
            r["class_"] or "",
            r["district"] or "",
            r["total_wins"] or 0,
            r["total_losses"] or 0,
            round(float(r["power_rating"] or 0), 2),
        ])

    if data:
        batch_write(ws, 2, data)

    print(f"    Written {len(data)} school records")
    return len(data)


# ─── INSTRUCTIONS ─────────────────────────────────────────────────────────────

def build_instructions_tab(sheet, season=SEASON):
    tab_name = "Instructions"
    print(f"  Building {tab_name}...")
    ws = get_or_create_tab(sheet, tab_name)
    now_str = datetime.now().strftime("%m/%d/%Y %I:%M %p CST")
    ws.update("A1", [
        ["LVAY Football Data — Google Sheet Guide"],
        [""],
        ["Last Updated:", now_str],
        ["Source:", "lhsaaonline.org — auto-scraped by lvay-scraper on Render"],
        [""],
        ["TAB", "CONTENTS"],
        [f"Football Power Rankings ({season})", "Schools ranked by power rating, grouped NS I thru S IV"],
        [f"Football Scores ({season})", "Every game — rebuilt automatically by the scheduled pipeline"],
        ["Football Needs Review", "Missing, malformed, or conflicting results requiring review"],
        [f"Football Overrides ({season})", "Enter corrections here and set active=TRUE; the engine applies them next run"],
        ["Football District Records", "W/L and power rating per school"],
        [""],
        ["POWER RATING FORMULA (LHSAA Football 14.12)"],
        ["Win", "10 pts + Opp Quality + Division Bonus"],
        ["Loss", "0 pts + Opp Quality + Division Bonus"],
        ["Tie", "5 pts + Opp Quality + Division Bonus"],
        ["Opp Quality", "(Opp Wins / Opp Games) x 10"],
        ["Div Bonus", "+2 pts per division level above you"],
        ["Power Rating", "Total Points / Games Played"],
        [""],
        ["DIVISION KEY", ""],
        ["NS I",   "Non-Select Division I   (largest non-select schools)"],
        ["NS II",  "Non-Select Division II"],
        ["NS III", "Non-Select Division III"],
        ["NS IV",  "Non-Select Division IV"],
        ["S I",    "Select Division I   (largest select/private schools)"],
        ["S II",   "Select Division II"],
        ["S III",  "Select Division III"],
        ["S IV",   "Select Division IV"],
    ])
    print(f"    Done")


# ─── FOOTBALL SCORES (separate slow build) ────────────────────────────────────

def build_football_scores(sheet, season=SEASON):
    tab_name = f"Football Scores ({season})"
    print(f"  Building {tab_name}...")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT school, week, game_date, home_away, opponent,
               class_, district, district_class, win_loss, score
        FROM games
        WHERE sport='football' AND season=?
        ORDER BY school, CAST(REPLACE(week,'Week ','') AS INTEGER)
    """, (str(season),))
    rows = c.fetchall()
    conn.close()

    ws = get_or_create_tab(sheet, tab_name)
    ws.update("A1", [[
        "School", "Week", "Date", "H/A", "Opponent",
        "Class", "District", "District/Class", "W/L", "Score"
    ]])
    time.sleep(2)

    data = []
    for r in rows:
        data.append([
            r["school"] or "", r["week"] or "", r["game_date"] or "",
            r["home_away"] or "", r["opponent"] or "",
            r["class_"] or "", r["district"] or "", r["district_class"] or "",
            r["win_loss"] or "", r["score"] or "",
        ])

    if data:
        batch_write(ws, 2, data)

    print(f"    Written {len(data)} games")
    return len(data)


# ─── MAIN EXPORTS ─────────────────────────────────────────────────────────────

def export_football_to_sheets(season=SEASON):
    """
    Main build — rankings, needs review, district records, instructions.
    Does NOT include scores tab (too slow — use export_football_scores separately).
    """
    print(f"\n{'='*54}")
    print(f"LVAY Football Sheets Export — Season {season}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*54}")

    try:
        client = get_client()
        sheet  = client.open_by_key(SHEET_ID)
        print(f"Connected: {sheet.title}")
    except Exception as e:
        print(f"ERROR connecting: {e}")
        return False

    try:
        rankings = build_football_power_rankings(sheet, season)
    except Exception as e:
        print(f"  ERROR rankings: {e}")
        rankings = 0

    try:
        flagged = build_needs_review(sheet, season)
    except Exception as e:
        print(f"  ERROR needs-review: {e}")
        flagged = 0

    try:
        districts = build_district_records(sheet, season)
    except Exception as e:
        print(f"  ERROR district records: {e}")
        districts = 0

    try:
        build_instructions_tab(sheet, season)
    except Exception as e:
        print(f"  ERROR instructions: {e}")

    try:
        ensure_football_overrides_tab(sheet, season)
    except Exception as e:
        print(f"  ERROR overrides tab: {e}")

    print(f"\n{'='*54}")
    print(f"DONE! Football {season} Sheets complete.")
    print(f"  Rankings:         {rankings} schools")
    print(f"  Needs Review:     {flagged} flagged")
    print(f"  District Records: {districts} schools")
    print(f"Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"{'='*54}\n")
    return True


def export_winter_sport_to_sheets(sport, season):
    """Export rankings, every schedule row, review flags, and overrides."""
    sport = str(sport).lower()
    season = str(season)
    allowed = {
        "boys_basketball", "girls_basketball",
        "boys_soccer", "girls_soccer",
    }
    if sport not in allowed:
        raise ValueError(f"Unsupported winter sport: {sport}")

    label = sport.replace("_", " ").title()
    client = get_client()
    sheet = client.open_by_key(SHEET_ID)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rankings = conn.execute("""
        SELECT rank, school, division, class_, district, wins, losses, ties,
               games_played, power_rating, strength_factor
        FROM power_rankings
        WHERE sport=? AND season=?
        ORDER BY rank
    """, (sport, season)).fetchall()
    games = conn.execute("""
        SELECT school, game_date, opponent, district_class, opponent_class,
               tournament, tournament_host, home_away, win_loss, score
        FROM games
        WHERE sport=? AND season=?
        ORDER BY school, game_date, opponent
    """, (sport, season)).fetchall()
    conn.close()

    rankings_ws = get_or_create_tab(
        sheet, f"{label} Power Rankings ({season})",
        rows=max(1000, len(rankings) + 2), cols=11,
    )
    rankings_ws.update("A1", [[
        "Rank", "School", "Division", "Class", "District", "W", "L", "T",
        "Games", "Power Rating", "Strength Factor",
    ]])
    if rankings:
        batch_write(rankings_ws, 2, [[
            r["rank"], r["school"], r["division"], r["class_"], r["district"],
            r["wins"], r["losses"], r["ties"], r["games_played"],
            r["power_rating"], r["strength_factor"],
        ] for r in rankings])
    rankings_ws.freeze(rows=1)

    scores_ws = get_or_create_tab(
        sheet, f"{label} Scores ({season})",
        rows=max(3000, len(games) + 2), cols=10,
    )
    scores_ws.update("A1", [[
        "School", "Date", "Opponent", "District/Class", "Opponent District/Class",
        "Tournament", "Tournament Host", "Home/Away", "W/L", "Score",
    ]])
    if games:
        batch_write(scores_ws, 2, [[
            g["school"], g["game_date"], g["opponent"], g["district_class"],
            g["opponent_class"], g["tournament"], g["tournament_host"],
            g["home_away"], g["win_loss"], g["score"],
        ] for g in games])
    scores_ws.freeze(rows=1)

    review_ws = get_or_create_tab(
        sheet, f"{label} Needs Review ({season})",
        rows=max(1000, len(games) + 2), cols=7,
    )
    review_ws.update("A1", [[
        "School", "Date", "Opponent", "W/L", "Score", "Issue", "Resolution",
    ]])
    review_rows = []
    for g in games:
        issues = []
        result = str(g["win_loss"] or "").strip()
        score = str(g["score"] or "").strip()
        if result not in ("W", "L", "T", "W(f)", "L(f)"):
            issues.append("Missing/unrecognized result")
        if result and not score:
            issues.append("Missing score")
        if score and not re.match(r"^\d+\s*-\s*\d+$", score):
            issues.append("Malformed score")
        if issues:
            review_rows.append([
                g["school"], g["game_date"], g["opponent"], result, score,
                ", ".join(issues), "",
            ])
    if review_rows:
        batch_write(review_ws, 2, review_rows)
    else:
        review_ws.update("A2", [["No issues found!"]])
    review_ws.freeze(rows=1)
    ensure_sport_overrides_tab(sheet, sport, season)

    return {
        "sport": sport, "season": season, "rankings": len(rankings),
        "games": len(games), "needs_review": len(review_rows),
    }


def export_football_scores(season=SEASON):
    """Separate slow build for scores tab only."""
    print(f"\nBuilding Football Scores tab...")
    try:
        client = get_client()
        sheet  = client.open_by_key(SHEET_ID)
        scores = build_football_scores(sheet, season)
        print(f"Done — {scores} games written")
        return True
    except Exception as e:
        print(f"ERROR: {e}")
        return False


if __name__ == "__main__":
    export_football_to_sheets()


# ─── DIVISION TABS ────────────────────────────────────────────────────────────

DIVISION_TAB_NAMES = {
    "Division I":              "Division I",
    "Division II":             "Division II",
    "Division III":            "Division III",
    "Division IV":             "Division IV",
    "Division V":              "Division V",
    "Non-Select Division I":   "NS Division I",
    "Non-Select Division II":  "NS Division II",
    "Non-Select Division III": "NS Division III",
    "Non-Select Division IV":  "NS Division IV",
    "Select Division I":       "S Division I",
    "Select Division II":      "S Division II",
    "Select Division III":     "S Division III",
    "Select Division IV":      "S Division IV",
}

CLASS_ORDER = ["5A", "4A", "3A", "2A", "1A"]

# Baseball/softball include Class B and C (football does not)
BASEBALL_SOFTBALL_CLASS_ORDER = ["5A", "4A", "3A", "2A", "1A", "B", "C"]


def load_power_rankings(season=SEASON):
    """Load all schools from power_rankings table."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school, division, track, class_, district,
                   wins, losses, ties, games_played, power_rating, rank
            FROM power_rankings
            WHERE sport='football' AND season=?
            ORDER BY rank ASC
        """, (str(season),))
        rows = [dict(r) for r in c.fetchall()]
    except Exception as e:
        print(f"    ERROR loading power_rankings: {e}")
        rows = []
    conn.close()
    return rows


def write_rankings_tab(sheet, tab_name, schools, group_label=""):
    """Write a ranked list of schools to a tab."""
    ws = get_or_create_tab(sheet, tab_name)

    col_headers = ["Rank", "School", "Division", "Class", "District",
                   "W", "L", "Games", "Power Rating"]

    all_rows = []
    if group_label:
        all_rows.append([group_label] + [""] * 8)
    all_rows.append(col_headers)

    for rank, s in enumerate(schools, 1):
        all_rows.append([
            rank,
            s.get("school") or "",
            DIVISION_LABELS.get(s.get("division", ""), s.get("division") or ""),
            s.get("class_") or "",
            s.get("district") or "",
            s.get("wins") or 0,
            s.get("losses") or 0,
            s.get("games_played") or 0,
            round(float(s.get("power_rating") or 0), 2),
        ])

    batch_write(ws, 1, all_rows)
    print(f"    {tab_name}: {len(schools)} schools")
    return len(schools)


def build_division_tabs(sheet, season=SEASON):
    """Build 8 individual division tabs."""
    print(f"  Building division tabs...")
    all_schools = load_power_rankings(season)
    if not all_schools:
        print(f"    No data found")
        return 0

    total = 0
    for division in DIVISION_ORDER:
        tab_name = DIVISION_TAB_NAMES.get(division, division)
        schools  = [s for s in all_schools if s.get("division") == division]
        schools.sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)
        if schools:
            write_rankings_tab(sheet, tab_name, schools,
                               group_label=f"LVAY Football {season} — {tab_name}")
            total += len(schools)
        time.sleep(1)

    return total


def load_game_breakdowns(season=SEASON, sport="football"):
    """Load all per-game power point breakdowns from game_power_points table."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school, week, opponent, result, score,
                   opp_wins, opp_losses, opp_division,
                   base_pts, div_bonus, opp_quality, total_pts
            FROM game_power_points
            WHERE sport=? AND season=?
            ORDER BY school, week ASC
        """, (sport, str(season)))
        rows = c.fetchall()
    except Exception as e:
        print(f"    WARNING: game_power_points table not found: {e}")
        rows = []
    conn.close()

    # Group by school
    by_school = {}
    for r in rows:
        s = r["school"]
        if s not in by_school:
            by_school[s] = []
        by_school[s].append(dict(r))
    return by_school


def build_class_tabs(sheet, season=SEASON):
    """
    Build 5 class tabs (5A-1A).
    Each tab grouped by district, each school shown in Excel-style format:
      - School name header row
      - Summary row: Class, Division, Overall record, District record, Power Rating
      - Column headers
      - One row per game: Week, Date, H/A, Opponent (W-L), Opp Division, W/L, Score, Base, Div Bonus, OppQ, Total Pts
      - Spacer between schools
    """
    print(f"  Building class tabs (Excel style)...")
    all_schools   = load_power_rankings(season)
    game_breakdowns = load_game_breakdowns(season)

    if not all_schools:
        print(f"    No data found")
        return 0

    # Also load district W/L from games table
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school,
                SUM(CASE WHEN win_loss='W' THEN 1 ELSE 0 END) as total_wins,
                SUM(CASE WHEN win_loss='L' THEN 1 ELSE 0 END) as total_losses
            FROM games
            WHERE sport='football' AND season=? AND district_class IS NOT NULL AND district_class != ''
            GROUP BY school
        """, (str(season),))
        dist_records = {r["school"]: {"dw": r["total_wins"], "dl": r["total_losses"]} for r in c.fetchall()}
    except Exception:
        dist_records = {}

    # Load game dates and H/A from games table
    c.execute("""
        SELECT school, week, game_date, home_away, opponent, district_class
        FROM games
        WHERE sport='football' AND season=?
    """, (str(season),))
    game_details = {}
    for r in c.fetchall():
        week_num = str(r["week"] or "").replace("Week ", "").strip()
        game_details[(r["school"], week_num)] = {
            "date":    r["game_date"] or "",
            "ha":      r["home_away"] or "",
            "dist_class": r["district_class"] or "",
        }
    conn.close()

    now_str     = datetime.now().strftime("%m/%d/%Y %I:%M %p")
    col_headers = ["Rank", "School", "Division", "Class", "District",
                   "W", "L", "Games", "Power Rating"]
    game_headers = ["Week", "Date", "H/A", "Opponent", "Opp Record",
                    "Opp Division", "W/L", "Score",
                    "Base Pts", "Div Bonus", "Opp Quality", "Game Total"]

    total = 0
    for class_ in CLASS_ORDER:
        tab_name = f"Class {class_}"
        schools  = [s for s in all_schools if s.get("class_") == class_]

        if not schools:
            continue

        districts = sorted(set(
            int(s.get("district") or 0)
            for s in schools
            if s.get("district")
        ))

        ws = get_or_create_tab(sheet, tab_name)

        all_rows = []
        all_rows.append([f"LVAY Football {season} — {tab_name} — Updated {now_str}"] + [""] * 11)

        for dist in districts:
            dist_schools = [s for s in schools if int(s.get("district") or 0) == dist]
            dist_schools.sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)

            # District header
            all_rows.append([f"=== DISTRICT {dist} ==="] + [""] * 11)
            all_rows.append([""] * 12)  # spacer

            for s in dist_schools:
                name    = s.get("school") or ""
                div     = DIVISION_LABELS.get(s.get("division", ""), s.get("division") or "")
                cls     = s.get("class_") or ""
                wins    = s.get("wins") or 0
                losses  = s.get("losses") or 0
                pr      = round(float(s.get("power_rating") or 0), 2)
                dr      = dist_records.get(name, {})
                dw      = dr.get("dw", 0)
                dl      = dr.get("dl", 0)

                # School name header
                all_rows.append([name] + [""] * 11)

                # Summary row
                all_rows.append([
                    cls, div,
                    f"Overall: {wins} - {losses}", "",
                    f"District: {dw} - {dl}", "",
                    "PR:", pr,
                    "", "", "", ""
                ])

                # Column headers
                all_rows.append(game_headers)

                # Game rows
                games = game_breakdowns.get(name, [])
                for g in games:
                    week_num = str(g.get("week") or "")
                    detail   = game_details.get((name, week_num), {})
                    opp      = g.get("opponent") or ""
                    opp_w    = g.get("opp_wins") or 0
                    opp_l    = g.get("opp_losses") or 0
                    opp_div  = DIVISION_LABELS.get(g.get("opp_division", ""), g.get("opp_division") or "")

                    all_rows.append([
                        f"Wk{week_num}",
                        detail.get("date", ""),
                        detail.get("ha", ""),
                        opp,
                        f"{opp_w} - {opp_l}",
                        opp_div,
                        g.get("result") or "",
                        g.get("score") or "",
                        g.get("base_pts") or 0,
                        g.get("div_bonus") or 0,
                        g.get("opp_quality") or 0,
                        g.get("total_pts") or 0,
                    ])

                all_rows.append([""] * 12)  # spacer between schools
                total += 1

        # Schools with no district
        no_dist = [s for s in schools if not s.get("district")]
        if no_dist:
            no_dist.sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)
            all_rows.append(["=== NO DISTRICT ASSIGNED ==="] + [""] * 8)
            all_rows.append(col_headers)
            for rank, s in enumerate(no_dist, 1):
                all_rows.append([
                    rank,
                    s.get("school") or "",
                    DIVISION_LABELS.get(s.get("division", ""), s.get("division") or ""),
                    s.get("class_") or "",
                    "",
                    s.get("wins") or 0,
                    s.get("losses") or 0,
                    s.get("games_played") or 0,
                    round(float(s.get("power_rating") or 0), 2),
                ])

        batch_write(ws, 1, all_rows)
        print(f"    {tab_name}: {len(schools)} schools across {len(districts)} districts")
        total += len(schools)
        time.sleep(1)

    return total


def export_division_and_class_tabs(season=SEASON):
    """Build all 13 breakdown tabs — 8 division + 5 class."""
    print(f"\n{'='*54}")
    print(f"LVAY Football — Division & Class Tabs")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*54}")

    try:
        client = get_client()
        sheet  = client.open_by_key(SHEET_ID)
        print(f"Connected: {sheet.title}")
    except Exception as e:
        print(f"ERROR connecting: {e}")
        return False

    try:
        div_total = build_division_tabs(sheet, season)
    except Exception as e:
        print(f"  ERROR division tabs: {e}")
        div_total = 0

    try:
        class_total = build_class_tabs(sheet, season)
    except Exception as e:
        print(f"  ERROR class tabs: {e}")
        class_total = 0

    print(f"\n{'='*54}")
    print(f"DONE!")
    print(f"  Division tabs: {div_total} schools across 8 tabs")
    print(f"  Class tabs:    {class_total} schools across 5 tabs")
    print(f"Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"{'='*54}\n")
    return True

# ─── BASEBALL / SOFTBALL SHARED ───────────────────────────────────────────────

BASEBALL_SOFTBALL_DIVISION_ORDER = [
    "Division I",
    "Division II",
    "Division III",
    "Division IV",
    "Non-Select Division I",
    "Non-Select Division II",
    "Non-Select Division III",
    "Non-Select Division IV",
    "Select Division I",
    "Select Division II",
    "Select Division III",
    "Select Division IV",
    "Class B",
    "Class C",
]


def load_sport_rankings(sport, season):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    try:
        c.execute("""
            SELECT school, division, track, class_, district,
                   wins, losses, ties, games_played, power_rating, rank
            FROM power_rankings
            WHERE sport=? AND season=?
            ORDER BY rank ASC
        """, (sport, str(season)))
        rows = [dict(r) for r in c.fetchall()]
    except Exception as e:
        print(f"    ERROR loading {sport} rankings: {e}")
        rows = []
    conn.close()
    return rows


def build_sport_power_rankings(sheet, sport, season):
    tab_name = f"{sport.title()} Power Rankings ({season})"
    print(f"  Building {tab_name}...")

    all_schools = load_sport_rankings(sport, season)
    if not all_schools:
        print(f"    No data — run /api/rankings/calculate?sport={sport}&season={season} first")
        return 0

    by_division = {div: [] for div in BASEBALL_SOFTBALL_DIVISION_ORDER}
    unmatched = []
    for s in all_schools:
        div = s.get("division") or ""
        if div in by_division:
            by_division[div].append(s)
        else:
            unmatched.append(s)

    for div in BASEBALL_SOFTBALL_DIVISION_ORDER:
        by_division[div].sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)

    now_str     = datetime.now().strftime("%m/%d/%Y %I:%M %p")
    col_headers = ["Div Rank", "School", "Division", "Class", "District",
                   "W", "L", "Games", "Power Rating"]

    ws = get_or_create_tab(sheet, tab_name)

    all_rows = []
    all_rows.append([f"LVAY {sport.title()} Power Rankings {season} — Updated {now_str}"] + [""] * 8)
    all_rows.append(col_headers)

    total = 0
    for division in BASEBALL_SOFTBALL_DIVISION_ORDER:
        schools = by_division[division]
        if not schools:
            continue
        all_rows.append([f"=== {division.upper()} ==="] + [""] * 8)
        for rank, s in enumerate(schools, 1):
            all_rows.append([
                rank,
                s.get("school") or "",
                DIVISION_LABELS.get(s.get("division", ""), s.get("division") or ""),
                s.get("class_") or "",
                s.get("district") or "",
                s.get("wins") or 0,
                s.get("losses") or 0,
                s.get("games_played") or 0,
                round(float(s.get("power_rating") or 0), 2),
            ])
            total += 1
        all_rows.append([""] * 9)

    if unmatched:
        unmatched.sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)
        all_rows.append(["=== UNMATCHED / NO DIVISION ==="] + [""] * 8)
        for rank, s in enumerate(unmatched, 1):
            all_rows.append([
                rank,
                s.get("school") or "",
                s.get("division") or "Unknown",
                s.get("class_") or "",
                s.get("district") or "",
                s.get("wins") or 0,
                s.get("losses") or 0,
                s.get("games_played") or 0,
                round(float(s.get("power_rating") or 0), 2),
            ])
            total += 1

    batch_write(ws, 1, all_rows)
    print(f"    Written {total} school rankings")
    return total


def build_sport_division_tabs(sheet, sport, season):
    """Build individual per-division tabs for a sport (NS-I through S-IV,
    plus Class B and Class C as their own division-track tabs).
    Tab names look like: "Baseball NS Division I (2026)".
    Class B / Class C divisions fall through to their own names."""
    print(f"  Building {sport} division tabs...")
    all_schools = load_sport_rankings(sport, season)
    if not all_schools:
        print(f"    No data — run /api/rankings/calculate?sport={sport}&season={season} first")
        return 0

    total = 0
    for division in BASEBALL_SOFTBALL_DIVISION_ORDER:
        # Short label, e.g. "NS Division I"; Class B/C use their own name
        short = DIVISION_TAB_NAMES.get(division, division)
        tab_name = f"{sport.title()} {short} ({season})"

        schools = [s for s in all_schools if (s.get("division") or "") == division]
        schools.sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)

        if schools:
            write_rankings_tab(
                sheet, tab_name, schools,
                group_label=f"LVAY {sport.title()} {season} — {short}"
            )
            total += len(schools)
        time.sleep(1)  # respect Sheets write quota

    return total


def build_sport_class_tabs(sheet, sport, season):
    """Build per-class tabs (5A, 4A, 3A, 2A, 1A, B, C) for a sport.
    Each tab ranks every school in that class by power rating regardless
    of select/non-select track — a cross-track 'class' view.
    Tab names look like: "Baseball Class 5A (2026)"."""
    print(f"  Building {sport} class tabs...")
    all_schools = load_sport_rankings(sport, season)
    if not all_schools:
        print(f"    No data — run /api/rankings/calculate?sport={sport}&season={season} first")
        return 0

    total = 0
    for cls in BASEBALL_SOFTBALL_CLASS_ORDER:
        tab_name = f"{sport.title()} Class {cls} ({season})"

        schools = [s for s in all_schools if str(s.get("class_") or "").strip() == cls]
        schools.sort(key=lambda x: float(x.get("power_rating") or 0), reverse=True)

        if schools:
            write_rankings_tab(
                sheet, tab_name, schools,
                group_label=f"LVAY {sport.title()} {season} — Class {cls}"
            )
            total += len(schools)
        time.sleep(1)  # respect Sheets write quota

    return total


def build_sport_needs_review(sheet, sport, season):
    """Publish missing, malformed, and conflicting played-game data."""
    tab_name = f"{sport.title()} Needs Review ({season})"
    print(f"  Building {tab_name}...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT school, week, game_date, opponent, win_loss, score,
               class_, district, district_class, needs_review
        FROM games
        WHERE sport=? AND season=?
        ORDER BY school, game_date, week
    """, (sport, str(season))).fetchall()
    conn.close()

    output = []
    for row in rows:
        if str(row["school"] or "").strip() in ("", "#", "School"):
            continue
        issues = game_review_issues(
            row["win_loss"], row["score"], row["needs_review"],
        )
        if not issues:
            continue
        output.append([
            row["school"] or "", row["week"] or "", row["game_date"] or "",
            row["opponent"] or "", row["win_loss"] or "", row["score"] or "",
            row["class_"] or "", row["district"] or "",
            row["district_class"] or "", ", ".join(issues),
        ])

    ws = get_or_create_tab(sheet, tab_name)
    ws.update("A1", [[
        "School", "Game", "Date", "Opponent", "W/L", "Score",
        "Class", "District", "District/Class", "Issue",
    ]])
    if output:
        batch_write(ws, 2, output)
    else:
        ws.update("A2", [["No issues found!"]])
    print(f"    {len(output)} games need review")
    return len(output)


def export_baseball_to_sheets(season=2026):
    print(f"\n{'='*54}")
    print(f"LVAY Baseball Sheets Export — Season {season}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*54}")
    try:
        client = get_client()
        sheet  = client.open_by_key(SHEET_ID)
        print(f"Connected: {sheet.title}")
    except Exception as e:
        print(f"ERROR connecting: {e}")
        return False

    try:
        total = build_sport_power_rankings(sheet, "baseball", season)
    except Exception as e:
        print(f"  ERROR (master): {e}")
        total = 0

    try:
        div_total = build_sport_division_tabs(sheet, "baseball", season)
    except Exception as e:
        print(f"  ERROR (divisions): {e}")
        div_total = 0

    try:
        cls_total = build_sport_class_tabs(sheet, "baseball", season)
    except Exception as e:
        print(f"  ERROR (classes): {e}")
        cls_total = 0

    try:
        flagged = build_sport_needs_review(sheet, "baseball", season)
        ensure_sport_overrides_tab(sheet, "baseball", season)
    except Exception as e:
        print(f"  ERROR (review/corrections): {e}")
        flagged = 0

    print(f"\n{'='*54}")
    print(f"DONE! Baseball {season} Sheets complete")
    print(f"  Master: {total} schools | Division tabs: {div_total} | Class tabs: {cls_total}")
    print(f"  Needs Review: {flagged} games | Overrides tab ready")
    print(f"Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"{'='*54}\n")
    return True


def export_softball_to_sheets(season=2026):
    print(f"\n{'='*54}")
    print(f"LVAY Softball Sheets Export — Season {season}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*54}")
    try:
        client = get_client()
        sheet  = client.open_by_key(SHEET_ID)
        print(f"Connected: {sheet.title}")
    except Exception as e:
        print(f"ERROR connecting: {e}")
        return False

    try:
        total = build_sport_power_rankings(sheet, "softball", season)
    except Exception as e:
        print(f"  ERROR (master): {e}")
        total = 0

    try:
        div_total = build_sport_division_tabs(sheet, "softball", season)
    except Exception as e:
        print(f"  ERROR (divisions): {e}")
        div_total = 0

    try:
        cls_total = build_sport_class_tabs(sheet, "softball", season)
    except Exception as e:
        print(f"  ERROR (classes): {e}")
        cls_total = 0

    try:
        flagged = build_sport_needs_review(sheet, "softball", season)
        ensure_sport_overrides_tab(sheet, "softball", season)
    except Exception as e:
        print(f"  ERROR (review/corrections): {e}")
        flagged = 0

    print(f"\n{'='*54}")
    print(f"DONE! Softball {season} Sheets complete")
    print(f"  Master: {total} schools | Division tabs: {div_total} | Class tabs: {cls_total}")
    print(f"  Needs Review: {flagged} games | Overrides tab ready")
    print(f"Sheet: https://docs.google.com/spreadsheets/d/{SHEET_ID}")
    print(f"{'='*54}\n")
    return True


def export_volleyball_to_sheets(season=None):
    """Build Volleyball rankings, schedules, review, and correction tabs."""
    season = str(season or os.environ.get(
        "VOLLEYBALL_SEASON_YEAR", datetime.now().year
    ))
    client = get_client()
    sheet = client.open_by_key(SHEET_ID)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rankings = conn.execute("""
        SELECT div_rank, school, division, class_, district, wins, losses,
               games_played, power_rating
        FROM volleyball_rankings
        WHERE sport='volleyball' AND season=?
        ORDER BY division, div_rank
    """, (season,)).fetchall()
    games = conn.execute("""
        SELECT school, match_num, game_date, opponent, result, score,
               school_division, school_district, counts_for_pr
        FROM volleyball_games
        WHERE sport='volleyball' AND season=?
        ORDER BY school, game_date, match_num
    """, (season,)).fetchall()
    conn.close()

    rankings_ws = get_or_create_tab(
        sheet, f"Volleyball Power Rankings ({season})"
    )
    rankings_ws.update("A1", [[
        "Division Rank", "School", "Division", "Class", "District",
        "W", "L", "Matches", "Power Rating",
    ]])
    if rankings:
        batch_write(rankings_ws, 2, [[
            r["div_rank"] or "", r["school"] or "", r["division"] or "",
            r["class_"] or "", r["district"] or "", r["wins"] or 0,
            r["losses"] or 0, r["games_played"] or 0,
            round(float(r["power_rating"] or 0), 3),
        ] for r in rankings])

    scores_ws = get_or_create_tab(sheet, f"Volleyball Scores ({season})")
    scores_ws.resize(rows=max(3000, len(games) + 2), cols=9)
    scores_ws.update("A1", [[
        "School", "Match", "Date", "Opponent", "W/L", "Score",
        "Division", "District", "Counts for PR",
    ]])
    if games:
        batch_write(scores_ws, 2, [[
            g["school"] or "", g["match_num"] or "", g["game_date"] or "",
            g["opponent"] or "", g["result"] or "", g["score"] or "",
            g["school_division"] or "", g["school_district"] or "",
            bool(g["counts_for_pr"]),
        ] for g in games])

    review_ws = get_or_create_tab(
        sheet, f"Volleyball Needs Review ({season})"
    )
    review_ws.resize(rows=max(1000, len(games) + 2), cols=9)
    review_ws.update("A1", [[
        "School", "Match", "Date", "Opponent", "W/L", "Score",
        "Division", "District", "Issue",
    ]])
    review = []
    for g in games:
        issues = []
        result = str(g["result"] or "").strip()
        score = str(g["score"] or "").strip()
        if result not in ("W", "L"):
            issues.append("missing or unrecognized result")
        if result in ("W", "L") and not score:
            issues.append("missing score")
        elif score and not re.match(
            r"^\s*\d+\s*-\s*\d+(\s*,\s*\d+\s*-\s*\d+)*\s*$", score
        ):
            issues.append("malformed set scores")
        if issues:
            review.append([
                g["school"] or "", g["match_num"] or "",
                g["game_date"] or "", g["opponent"] or "", result, score,
                g["school_division"] or "", g["school_district"] or "",
                ", ".join(issues),
            ])
    if review:
        batch_write(review_ws, 2, review)
    else:
        review_ws.update("A2", [["No issues found!"]])

    ensure_sport_overrides_tab(sheet, "volleyball", season)
    return {
        "rankings": len(rankings),
        "games": len(games),
        "needs_review": len(review),
    }
