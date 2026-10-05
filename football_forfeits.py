"""
Football forfeits LVAY records before (or instead of) LHSAA's schedule report.

Each entry is a team that must forfeit specific games.  For every listed game:
  * the game shows as a forfeit right away (score "Forfeit"), so fans see it
    on schedules before it happens;
  * the result only counts (forfeiting team L(f), opponent W(f)) once the game
    date has passed (Central time), so records and power ratings never include
    a forfeit for a game that hasn't happened yet.
A game LHSAA posts with a real score is left alone.

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
    ],
}


def _parse_date(text):
    text = str(text or "").strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _has_real_score(score):
    return len(re.findall(r"\d+", str(score or ""))) == 2


def apply_football_forfeits(conn, season, today=None):
    """Mark listed forfeits in the games table. Returns rows changed."""
    entries = FOOTBALL_FORFEITS.get(str(season))
    if not entries:
        return 0
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
                if row_school != school or row_opp != opponent or _parse_date(row_date) != game_day:
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
    return changed
