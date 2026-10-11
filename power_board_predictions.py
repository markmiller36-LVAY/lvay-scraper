"""
LVAY - Power Board frozen predictions (server side)
====================================================
The PREDICTED WIN chip on the Football Power Board used to be calculated by
JavaScript inside the page, which meant anyone could read the formula with
"View Source". The formula now lives only here, on Render.

How it works
------------
* Each pipeline run calls freeze_upcoming_predictions(). For every game that
  has not been played yet and kicks off within FREEZE_DAYS_AHEAD days, it
  calculates a win % once and stores it in the power_board_predictions table.
* A stored prediction is never recalculated or changed ("frozen before the
  game"), so the track record can be checked honestly later.
* /api/schedules/football adds pred_wl / pred_prob to each game that has one,
  and the WordPress feed mirror copies that to the Power Board.

Only results and percentages ever leave the server, never the weights.
"""

import os
import sqlite3
from datetime import datetime, date
from zoneinfo import ZoneInfo

CENTRAL = ZoneInfo("America/Chicago")

# ── PRIVATE FORMULA SETTINGS (do not copy into any WordPress page) ──────────
WIN_PCT_WEIGHT = 50.0      # win% x 50
RATING_WEIGHT = 2.0        # Power Rating x 2
HFA_BUMP = 2.5             # Strength Factor points added to the home team
COMMON_OPP_WEIGHT = 0.5    # weight of the common-opponent margin edge
PREDICTION_SCALE = 35.0    # logistic scale
MIN_PROB, MAX_PROB = 0.03, 0.97
FREEZE_DAYS_AHEAD = int(os.environ.get("PREDICTION_FREEZE_DAYS_AHEAD", "3"))

WIN_RESULTS = {"W", "W(F)"}
LOSS_RESULTS = {"L", "L(F)"}


def _round_half_up(value, places=0):
    """Match JavaScript Math.round (halves round up, not to even)."""
    factor = 10 ** places
    import math
    return math.floor(value * factor + 0.5) / factor


def ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS power_board_predictions (
            season    TEXT NOT NULL,
            school    TEXT NOT NULL,
            opponent  TEXT NOT NULL,
            week      INTEGER NOT NULL,
            pred_wl   TEXT NOT NULL,
            pred_prob INTEGER NOT NULL,
            frozen_at TEXT NOT NULL,
            PRIMARY KEY (season, school, opponent, week)
        )
    """)


def _week_number(raw):
    try:
        return int(str(raw).replace("Week", "").strip())
    except (TypeError, ValueError):
        return None


def _parse_date(raw):
    text = str(raw or "").strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _result_code(raw):
    text = str(raw or "").strip().upper().replace(" ", "")
    if text in WIN_RESULTS:
        return "W"
    if text in LOSS_RESULTS:
        return "L"
    return None


def _own_opp_score(score, result):
    """Scores in the feed are not always written winner-first; fix by result."""
    import re
    m = re.search(r"(\d+)\s*-\s*(\d+)", str(score or ""))
    if not m:
        return None
    a, b = int(m.group(1)), int(m.group(2))
    hi, lo = max(a, b), min(a, b)
    if result == "W":
        return hi, lo
    if result == "L":
        return lo, hi
    return None


def load_season_games(conn, season):
    """Every football game row for the season, one row per school's side."""
    from jv_only_schools import is_jv_only_game

    conn.row_factory = sqlite3.Row
    roster = {
        str(r["school"]).strip().casefold(): str(r["school"]).strip()
        for r in conn.execute(
            "SELECT school FROM season_schools WHERE sport='football' AND season=?",
            (season,),
        ).fetchall()
    }
    ratings = {
        str(r["school"]).strip().casefold(): float(r["power_rating"] or 0)
        for r in conn.execute(
            "SELECT school, power_rating FROM power_rankings "
            "WHERE sport='football' AND season=?",
            (season,),
        ).fetchall()
    }
    for key in ratings:
        roster.setdefault(key, key)

    override = {}
    for r in conn.execute(
        "SELECT school, week, result, score FROM game_power_points "
        "WHERE sport='football' AND season=?",
        (season,),
    ).fetchall():
        override[(str(r["school"]).strip().casefold(), _week_number(r["week"]))] = (
            r["result"], r["score"]
        )

    rows = []
    for r in conn.execute(
        "SELECT school, opponent, week, win_loss, score, home_away, game_date, "
        "out_of_state FROM games WHERE sport='football' AND season=?",
        (season,),
    ).fetchall():
        school = str(r["school"] or "").strip()
        opponent = str(r["opponent"] or "").strip()
        week = _week_number(r["week"])
        if not school or not opponent or week is None:
            continue
        if opponent.upper().startswith("OPEN DATE"):
            continue
        if is_jv_only_game("football", season, school, opponent):
            continue
        ov = override.get((school.casefold(), week))
        raw_result = (ov[0] if ov and ov[0] else r["win_loss"])
        raw_score = (ov[1] if ov and ov[1] else r["score"])
        result = _result_code(raw_result)
        rows.append({
            "school": school,
            "opponent": opponent,
            "week": week,
            "result": result,
            "cancelled": "CANCEL" in str(raw_result or "").upper()
                         or "CANCEL" in str(raw_score or "").upper(),
            "scores": _own_opp_score(raw_score, result) if result else None,
            "home_away": str(r["home_away"] or "").strip().upper(),
            "game_date": _parse_date(r["game_date"]),
            "out_of_state": str(r["out_of_state"] or "").strip().upper()
                            in {"Y", "YES", "1", "TRUE"},
        })
    return rows, roster, ratings


def strength_factors(rows, ratings):
    """Real-results-only Strength Factor per school (casefold key)."""
    acc = {}
    for g in rows:
        if not g["result"]:
            continue
        a = acc.setdefault(g["school"].casefold(), {"w": 0, "gp": 0, "pf": 0, "pa": 0})
        a["gp"] += 1
        if g["result"] == "W":
            a["w"] += 1
        if g["scores"]:
            a["pf"] += g["scores"][0]
            a["pa"] += g["scores"][1]
    out = {}
    for key, a in acc.items():
        if not a["gp"]:
            continue
        win_pct = a["w"] / a["gp"]
        avg_margin = (a["pf"] - a["pa"]) / a["gp"]
        value = (win_pct * WIN_PCT_WEIGHT) + avg_margin + (ratings.get(key, 0.0) * RATING_WEIGHT)
        out[key] = _round_half_up(value, 2)
    return out


def _margins(rows):
    by_school = {}
    for g in rows:
        if g["result"] and g["scores"]:
            by_school.setdefault(g["school"].casefold(), {})[g["opponent"].casefold()] = (
                g["scores"][0] - g["scores"][1]
            )
    return by_school


def common_opponent_edge(margins, team, other):
    mine, theirs = margins.get(team, {}), margins.get(other, {})
    shared = [k for k in mine if k in theirs and k not in (team, other)]
    if not shared:
        return 0.0
    return sum(mine[k] - theirs[k] for k in shared) / len(shared)


def win_probability(sf, margins, team, other, team_home):
    """team's win % (3-97), or None when either side has no real results."""
    if team not in sf or other not in sf:
        return None
    gap = sf[team] - sf[other]
    if team_home is True:
        gap += HFA_BUMP
    elif team_home is False:
        gap -= HFA_BUMP
    gap += common_opponent_edge(margins, team, other) * COMMON_OPP_WEIGHT
    prob = 1 / (1 + 10 ** (-gap / PREDICTION_SCALE))
    prob = max(MIN_PROB, min(MAX_PROB, prob))
    return int(_round_half_up(prob * 100))


def freeze_upcoming_predictions(conn, season, today=None, days_ahead=None):
    """Freeze predictions for unplayed in-state games kicking off soon.

    Existing predictions are never touched. Returns a short summary dict.
    """
    ensure_table(conn)
    today = today or datetime.now(CENTRAL).date()
    days_ahead = FREEZE_DAYS_AHEAD if days_ahead is None else days_ahead
    rows, roster, ratings = load_season_games(conn, season)
    sf = strength_factors(rows, ratings)
    margins = _margins(rows)

    existing = {
        (r[0].casefold(), r[1].casefold(), r[2])
        for r in conn.execute(
            "SELECT school, opponent, week FROM power_board_predictions WHERE season=?",
            (season,),
        ).fetchall()
    }
    frozen_at = datetime.now(CENTRAL).isoformat(timespec="seconds")
    written, skipped = 0, 0
    for g in rows:
        if g["result"] or g["cancelled"] or g["out_of_state"]:
            continue
        team, other = g["school"].casefold(), g["opponent"].casefold()
        if other not in roster:
            continue
        if not g["game_date"]:
            continue
        days = (g["game_date"] - today).days
        if days < 0 or days > days_ahead:
            continue
        key = (team, other, g["week"])
        if key in existing:
            continue
        home = True if g["home_away"] == "H" else (False if g["home_away"] == "A" else None)
        prob = win_probability(sf, margins, team, other, home)
        if prob is None:
            skipped += 1
            continue
        # Freeze both sides together so they always add up to 100 and
        # exactly one side is the predicted winner.
        opp_name = roster.get(other, g["opponent"])
        for name, opp, pct, wl in (
            (g["school"], opp_name, prob, "W" if prob >= 50 else "L"),
            (opp_name, g["school"], 100 - prob, "L" if prob >= 50 else "W"),
        ):
            conn.execute(
                "INSERT OR IGNORE INTO power_board_predictions "
                "(season, school, opponent, week, pred_wl, pred_prob, frozen_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (season, name, opp, g["week"], wl, pct, frozen_at),
            )
            existing.add((name.casefold(), opp.casefold(), g["week"]))
        written += 1
    conn.commit()
    return {"season": season, "written": written, "skipped": skipped,
            "as_of": today.isoformat()}


def predictions_lookup(conn, season):
    """{(school_cf, opponent_cf, week): (pred_wl, pred_prob)} for the feed."""
    try:
        ensure_table(conn)
        return {
            (r[0].casefold(), r[1].casefold(), int(r[2])): (r[3], int(r[4]))
            for r in conn.execute(
                "SELECT school, opponent, week, pred_wl, pred_prob "
                "FROM power_board_predictions WHERE season=?",
                (season,),
            ).fetchall()
        }
    except sqlite3.Error:
        return {}
