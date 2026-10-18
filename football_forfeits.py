"""
Football forfeits LVAY records before (or instead of) LHSAA's schedule report.

Each entry is a team that must forfeit specific games.  For every listed game:
  * the game shows as a forfeit right away (score "Forfeit"), so fans see it
    on schedules before it happens;
  * the result only counts (forfeiting team L(f), opponent W(f)) once the game
    date has passed (Central time), so records and power ratings never include
    a forfeit for a game that hasn't happened yet.
A game LHSAA posts with a real score is left alone.

Cancelled games (FOOTBALL_CANCELLED_GAMES) stay on both teams' schedules with
the result "Cancelled". That is not a W/L/T, so the game never counts toward
records, standings or power ratings (each team's rating uses one fewer game).

Run by the football scrape merge and again just before football ratings are
calculated, so the date switch happens even if the LHSAA scrape fails.
"""

import re
import sqlite3
from datetime import date, datetime
from zoneinfo import ZoneInfo

CENTRAL = ZoneInfo("America/Chicago")
FORFEIT_SCORE = "Forfeit"

# (season) -> list of {"team", "opponent", "game_date" (M/D/YYYY)}
FOOTBALL_FORFEITS = {
    "2026": [
        # Carroll ordered to forfeit the rest of its 2026 schedule (Week 5 on). Mark, Oct 5, 2026.
        {"team": "Carroll", "opponent": "Wossman", "game_date": "10/2/2026"},
        {"team": "Carroll", "opponent": "Bastrop", "game_date": "10/9/2026"},
        {"team": "Carroll", "opponent": "Loyola Prep", "game_date": "10/16/2026"},
        {"team": "Carroll", "opponent": "Abramson", "game_date": "10/23/2026"},
        {"team": "Carroll", "opponent": "Sterlington", "game_date": "10/30/2026"},
        {"team": "Carroll", "opponent": "North Webster", "game_date": "11/6/2026"},
        # Thrive Academy's season ended after Week 6; opponents receive forfeit wins. Mark, Oct 13, 2026.
        {"team": "Thrive Academy", "opponent": "St. John", "game_date": "10/15/2026"},
        {"team": "Thrive Academy", "opponent": "Southern Lab", "game_date": "10/22/2026"},
        {"team": "Thrive Academy", "opponent": "Acadiana Christian", "game_date": "10/30/2026"},
        {"team": "Thrive Academy", "opponent": "East Iberville", "game_date": "11/6/2026"},
        # Capitol's season ended after Week 6; opponents receive forfeit wins. Mark, Oct 14, 2026.
        # Week 7 at East Iberville is replaced by Highland Baptist (see FOOTBALL_SWAPPED_GAMES).
        {"team": "Capitol", "opponent": "St. John", "game_date": "10/23/2026"},
        {"team": "Capitol", "opponent": "White Castle", "game_date": "10/30/2026"},
        {"team": "Capitol", "opponent": "Southern Lab", "game_date": "11/6/2026"},
    ],
}

# Called-off games (not forfeits): shown as "Cancelled", never counted.
CANCELLED_RESULT = "Cancelled"
FOOTBALL_CANCELLED_GAMES = {
    "2026": [
        # Week 6 game cancelled; not a forfeit. Mark, Oct 10, 2026.
        {"team": "Booker T. Washington - N.O.", "opponent": "Bogalusa", "game_date": "10/9/2026"},
        # Week 6 vs Dumas (Ark.) ruled cancelled; not a forfeit. Mark, Oct 13, 2026.
        {"team": "General Trass", "opponent": "Dumas High School, AR", "game_date": "10/9/2026"},
        # Week 7 stopped (gunfire near the stadium); LHSAA ruled it a no contest. Mark, Oct 17, 2026.
        {"team": "Church Point", "opponent": "Ville Platte", "game_date": "10/16/2026"},
    ],
}

# Games re-arranged after LHSAA posted the schedule. Each listed school's game
# against old_opponent becomes an unplayed game against new_opponent.
# Mark, Oct 14, 2026: East Iberville plays Highland Baptist in Week 7 instead
# of taking Capitol's forfeit; Highland Baptist drops its JV game with False River.
FOOTBALL_SWAPPED_GAMES = {
    "2026": [
        {"school": "East Iberville", "old_opponent": "Capitol", "new_opponent": "Highland Baptist",
         "week": 7, "game_date": "10/16/2026", "home_away": "H"},
        {"school": "Highland Baptist", "old_opponent": "False River Academy", "new_opponent": "East Iberville",
         "week": 7, "game_date": "10/16/2026", "home_away": "A"},
    ],
}
# Games that no longer happen; the week becomes an open week for that school.
FOOTBALL_DROPPED_GAMES = {
    "2026": [
        {"school": "Capitol", "opponent": "East Iberville", "game_date": "10/16/2026"},
    ],
}


def _parse_date(text):
    """Parse LHSAA's mixed date formats (10/2/2026, 10/2/26, 10/2/2026Fri, ISO...)."""
    text = str(text or "").strip()
    match = re.match(r"^(\d{1,2}/\d{1,2}/\d{2,4})", text)
    if match:
        text = match.group(1)
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%m-%d-%y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text[:19]).date()
    except ValueError:
        return None


def _key(name):
    """Loose school key: case, spacing and punctuation don't matter."""
    try:
        from school_database import loose_school_key, resolve_school_spelling
        resolved = resolve_school_spelling(name) or name
        return loose_school_key(resolved)
    except Exception:
        return re.sub(r"[^a-z0-9]", "", str(name or "").casefold())


def _has_real_score(score):
    return len(re.findall(r"\d+", str(score or ""))) == 2


def _week_number(value):
    match = re.search(r"\d+", str(value or ""))
    return int(match.group()) if match else None


def apply_football_schedule_changes(conn, season):
    """Apply swapped and dropped games. Returns rows changed."""
    season = str(season)
    swaps = FOOTBALL_SWAPPED_GAMES.get(season, [])
    drops = FOOTBALL_DROPPED_GAMES.get(season, [])
    if not swaps and not drops:
        return 0
    try:
        rows = conn.execute(
            "SELECT rowid, school, opponent, game_date, week FROM games "
            "WHERE sport='football' AND season=?",
            (season,),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    changed = 0
    for drop in drops:
        day = _parse_date(drop["game_date"])
        for rowid, school, opponent, game_date, _week in rows:
            if (_key(school) == _key(drop["school"]) and _key(opponent) == _key(drop["opponent"])
                    and _parse_date(game_date) in (day, None)):
                conn.execute("DELETE FROM games WHERE rowid=?", (rowid,))
                changed += 1
    columns = {row[1] for row in conn.execute("PRAGMA table_info(games)")}
    for swap in swaps:
        day = _parse_date(swap["game_date"])
        mine = [r for r in rows if _key(r[1]) == _key(swap["school"])]
        has_new = any(
            _key(r[2]) == _key(swap["new_opponent"]) and _parse_date(r[3]) == day for r in mine
        )
        for rowid, school, opponent, game_date, week in mine:
            # Only that week's game: same date, or an undated placeholder for that week.
            if _key(opponent) != _key(swap["old_opponent"]):
                continue
            row_day = _parse_date(game_date)
            if row_day != day and not (row_day is None and _week_number(week) == swap["week"]):
                continue
            if has_new:
                # The new game is already on file (e.g. LHSAA posted it); drop the old one.
                conn.execute("DELETE FROM games WHERE rowid=?", (rowid,))
            elif "home_away" in columns:
                conn.execute(
                    "UPDATE games SET opponent=?, game_date=?, home_away=?, win_loss='', score='-' "
                    "WHERE rowid=?",
                    (swap["new_opponent"], swap["game_date"], swap["home_away"], rowid),
                )
            else:
                conn.execute(
                    "UPDATE games SET opponent=?, game_date=?, win_loss='', score='-' WHERE rowid=?",
                    (swap["new_opponent"], swap["game_date"], rowid),
                )
            has_new = True
            changed += 1
    return changed


def apply_football_cancellations(conn, season):
    """Mark listed cancelled games "Cancelled" on both schedules. Returns rows changed."""
    entries = FOOTBALL_CANCELLED_GAMES.get(str(season))
    if not entries:
        return 0
    try:
        rows = conn.execute(
            "SELECT rowid, school, opponent, game_date, score, win_loss FROM games "
            "WHERE sport='football' AND season=?",
            (str(season),),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    changed = 0
    for entry in entries:
        game_day = _parse_date(entry["game_date"])
        for school, opponent in ((entry["team"], entry["opponent"]), (entry["opponent"], entry["team"])):
            for rowid, row_school, row_opp, row_date, score, win_loss in rows:
                if (_key(row_school) != _key(school) or _key(row_opp) != _key(opponent)
                        or _parse_date(row_date) not in (game_day, None)):
                    continue
                if (win_loss or "") == CANCELLED_RESULT and not (score or "").strip():
                    continue
                conn.execute(
                    "UPDATE games SET score='', win_loss=? WHERE rowid=?",
                    (CANCELLED_RESULT, rowid),
                )
                changed += 1
    return changed


def apply_football_forfeits(conn, season, today=None):
    """Mark listed forfeits (and cancelled games) in the games table. Returns rows changed."""
    cancelled = apply_football_schedule_changes(conn, season)
    cancelled += apply_football_cancellations(conn, season)
    entries = FOOTBALL_FORFEITS.get(str(season))
    if not entries:
        return cancelled
    today = today or datetime.now(CENTRAL).date()
    changed = 0
    try:
        rows = conn.execute(
            "SELECT rowid, school, opponent, game_date, score, win_loss FROM games "
            "WHERE sport='football' AND season=?",
            (str(season),),
        ).fetchall()
    except sqlite3.OperationalError:
        return 0
    for entry in entries:
        game_day = _parse_date(entry["game_date"])
        if game_day is None:
            continue
        counted = game_day < today
        sides = (
            (entry["team"], entry["opponent"], "L(f)"),
            (entry["opponent"], entry["team"], "W(f)"),
        )
        for school, opponent, result in sides:
            for rowid, row_school, row_opp, row_date, score, win_loss in rows:
                if (_key(row_school) != _key(school) or _key(row_opp) != _key(opponent)
                        or _parse_date(row_date) not in (game_day, None)):
                    continue
                if _has_real_score(score):
                    continue  # LHSAA posted an actual result; never overwrite it
                new_result = result if counted else ""
                if score == FORFEIT_SCORE and (win_loss or "") == new_result:
                    continue
                conn.execute(
                    "UPDATE games SET score=?, win_loss=? WHERE rowid=?",
                    (FORFEIT_SCORE, new_result, rowid),
                )
                changed += 1
    changed += cancelled
    try:
        conn.execute(
            "INSERT INTO scrape_log (ran_at, sport, games_found, status, note) VALUES (?, 'football-forfeits', ?, 'success', ?)",
            (datetime.now().isoformat(), changed, f"season={season} rows_changed={changed} checked={len(rows)}" + (
                "" if changed else " sample=" + "; ".join(
                    f"{r[1]}|{r[2]}|{r[3]}|{r[4]}|{r[5]}" for r in rows
                    if _key(r[1]) == _key(entries[0]["team"]))[:400])),
        )
    except sqlite3.OperationalError:
        pass
    return changed
