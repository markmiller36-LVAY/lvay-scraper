"""LVAY tournament finals -> games table, until LHSAA posts its own result.

Mark (Nov 20, 2026): a final score entered on an LVAY tournament page
(e.g. Bossier 4, Captain Shreve 1 at the Flyer Invitational) "should reflect on
the site until overridden by LHSAA" -- schedules, records, standings AND power
ratings.

Source: GET {LVAY_SITE}/wp-json/lvay/v1/tournament-finals?sport=<sport>
(Code Snippet #133). Each final: date (Y-m-d), time (HH:MM), a, b, sa, sb,
tournament, host, url, id.

Rules, run before every winter ratings pass:
  1. Undo last run: delete rows this module inserted, blank results it filled.
  2. For each final and each in-state side (team X vs Y on date D):
     - LHSAA already has a scored X-vs-Y row on D  -> LHSAA wins, skip.
     - LHSAA has the game on D but no score yet     -> fill win_loss/score.
     - LHSAA doesn't list the game at all           -> insert a row.
  A side whose school isn't in this sport's games table (out of state, unknown
  name) is skipped. If the site can't be reached, last run's tournament rows are
  kept as they are; a successful fetch with a final removed undoes it.
"""

from __future__ import annotations

import os
import re
from datetime import datetime

SOURCE_INSERT = "LVAY tournament"
SOURCE_FILL = "LVAY tournament (filled)"
COUNTED = {"W", "L", "T", "Tie", "W(f)", "L(f)", "L(df)", "L(DF)", "DF"}
SITE = os.environ.get("LVAY_SITE_URL", "https://louisianavsallyall.com").rstrip("/")


def _key(name) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name or "").lower())


def _date_key(game_date) -> str:
    """'11/19/2026 6:20:00 PM' or '2026-11-19' -> '2026-11-19'."""
    s = str(game_date or "").strip()
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        return f"{int(m.group(3)):04d}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return ""


def _lhsaa_date(date_ymd: str, time_hm: str) -> str:
    d = datetime.strptime(date_ymd, "%Y-%m-%d")
    hh, mm = 12, 0
    m = re.match(r"^(\d{1,2}):(\d{2})", str(time_hm or ""))
    if m:
        hh, mm = int(m.group(1)), int(m.group(2))
    ap = "PM" if hh >= 12 else "AM"
    h12 = hh % 12 or 12
    return f"{d.month}/{d.day}/{d.year} {h12}:{mm:02d}:00 {ap}"


def fetch_finals(sport: str, timeout: int = 20):
    """Return the site's finals list, or None if the site can't be reached."""
    import requests

    try:
        resp = requests.get(
            f"{SITE}/wp-json/lvay/v1/tournament-finals",
            params={"sport": sport},
            timeout=timeout,
            headers={"User-Agent": "lvay-scraper/1.0 (+tournament finals)"},
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # network, JSON, HTTP
        print(f"  Tournament finals: site unreachable ({exc}); none applied")
        return None
    finals = data.get("finals") if isinstance(data, dict) else None
    return finals if isinstance(finals, list) else []


def _ensure_columns(conn):
    cols = {row[1] for row in conn.execute("PRAGMA table_info(games)")}
    for name, definition in (
        ("source", "TEXT"),
        ("tournament", "TEXT"),
        ("tournament_host", "TEXT"),
        ("lvay_fill", "TEXT"),
    ):
        if name not in cols:
            conn.execute(f"ALTER TABLE games ADD COLUMN {name} {definition}")


def _undo(conn, sport: str, season: str) -> None:
    conn.execute(
        "DELETE FROM games WHERE sport=? AND season=? AND source=?",
        (sport, season, SOURCE_INSERT),
    )
    # Blank a filled result only if it is still ours: if LHSAA has since posted
    # its own score into that row, keep LHSAA's.
    conn.execute(
        "UPDATE games SET win_loss='', score='' "
        "WHERE sport=? AND season=? AND source=? "
        "AND lvay_fill = COALESCE(win_loss,'') || '|' || COALESCE(score,'')",
        (sport, season, SOURCE_FILL),
    )
    conn.execute(
        "UPDATE games SET source='LHSAA', lvay_fill=NULL "
        "WHERE sport=? AND season=? AND source=?",
        (sport, season, SOURCE_FILL),
    )


def apply_tournament_finals(conn, sport: str, season: str, finals=None) -> dict:
    """Merge LVAY tournament finals into games. Returns counts for the log."""
    sport = str(sport)
    season = str(season)
    _ensure_columns(conn)
    stats = {"inserted": 0, "filled": 0, "lhsaa": 0, "skipped": 0}
    if finals is None:
        finals = fetch_finals(sport)
        if finals is None:
            # Site unreachable: keep last run's tournament rows rather than
            # making ratings flicker. They are rebuilt on the next good fetch.
            conn.commit()
            return stats
    _undo(conn, sport, season)
    if not finals:
        conn.commit()
        return stats

    rows = conn.execute(
        "SELECT rowid, school, opponent, game_date, win_loss, class_, district, "
        "district_class, opponent_class FROM games WHERE sport=? AND season=?",
        (sport, season),
    ).fetchall()
    by_school = {}
    names = {}
    opp_class = {}
    for r in rows:
        k = _key(r[1])
        by_school.setdefault(k, []).append(r)
        names.setdefault(k, r[1])
        if r[8]:
            opp_class.setdefault(_key(r[2]), r[8])

    now = datetime.now().isoformat()
    for f in finals:
        try:
            date_ymd = _date_key(f.get("date"))
            sa, sb = int(f.get("sa")), int(f.get("sb"))
        except (TypeError, ValueError):
            stats["skipped"] += 1
            continue
        a, b = _key(f.get("a")), _key(f.get("b"))
        if not date_ymd or not a or not b or a == b:
            stats["skipped"] += 1
            continue
        for me, them, mine, theirs in ((a, b, sa, sb), (b, a, sb, sa)):
            if me not in by_school:
                stats["skipped"] += 1  # out of state / not an LHSAA school here
                continue
            if them not in by_school and them not in opp_class:
                stats["skipped"] += 1
                continue
            res = "W" if mine > theirs else "L" if mine < theirs else "T"
            score = f"{mine}-{theirs}"
            same = [
                r for r in by_school[me]
                if _key(r[2]) == them and _date_key(r[3]) == date_ymd
            ]
            if any(str(r[4] or "").strip() in COUNTED for r in same):
                stats["lhsaa"] += 1
                continue
            if same:
                conn.execute(
                    "UPDATE games SET win_loss=?, score=?, source=?, lvay_fill=?, "
                    "tournament=COALESCE(NULLIF(tournament,''),?), "
                    "tournament_host=COALESCE(NULLIF(tournament_host,''),?) "
                    "WHERE rowid=?",
                    (res, score, SOURCE_FILL, f"{res}|{score}", f.get("tournament") or "",
                     f.get("host") or "", same[0][0]),
                )
                stats["filled"] += 1
                continue
            ref = by_school[me][0]
            their_rows = by_school.get(them) or []
            o_class = opp_class.get(them) or (their_rows[0][7] if their_rows else "")
            conn.execute(
                "INSERT OR IGNORE INTO games (sport, school, game_date, opponent, "
                "home_away, win_loss, score, week, district, class_, district_class, "
                "opponent_class, tournament, tournament_host, out_of_state, location, "
                "season, scraped_at, source) VALUES "
                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    sport, ref[1], _lhsaa_date(date_ymd, f.get("time")),
                    names.get(them) or str(f.get("b") if me == a else f.get("a")),
                    "N", res, score, "", ref[6], ref[5], ref[7], o_class,
                    f.get("tournament") or "", f.get("host") or "", "", "",
                    season, now, SOURCE_INSERT,
                ),
            )
            stats["inserted"] += 1
    conn.commit()
    print(
        "  Tournament finals: "
        f"{stats['inserted']} added, {stats['filled']} filled, "
        f"{stats['lhsaa']} already on LHSAA, {stats['skipped']} skipped"
    )
    return stats
