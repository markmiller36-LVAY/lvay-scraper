"""
Volleyball playoff eligibility, matched to LHSAA.

LHSAA's volleyball power rating report (VBbyDivisionPR.html) lists each
division's ranked schools and, when a school has opted out or been ruled
ineligible, a separate "Schools not playing/participating in the Playoff"
table under that division.  LVAY ranks only playoff-eligible schools inside a
division and shows the others underneath, exactly like LHSAA (and exactly like
football -- see football_playoff_eligibility.py).

Bylaw 24.6.1: every volleyball division brackets 32 teams (district champions
plus wildcards), so the playoff cutoff is the 32nd eligible school.

`update_playoff_eligibility()` reads the LHSAA report and stores the result in
the shared `playoff_eligibility` table with sport='volleyball'.  If LHSAA can't
be read, the last stored list is kept.  MANUAL_NOT_PLAYING lets us add a school
LHSAA has announced before its report catches up.
"""

import json
import os
import re
import sqlite3
from datetime import datetime

import requests

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")
SPORT = "volleyball"
REPORT_URL = "https://www.lhsaaonline.org/rpt/VBbyDivisionPR.html"
PLAYOFF_FIELD = 32  # Bylaw 24.6.1: 32-team bracket in Divisions I-V
MIN_MATCHES = 20   # Bylaw 24.7.1: under 20 regular-season matches = seeded at the bottom
# Last day of the regular season (Bylaw 24.1).  The 20-match rule only moves
# seeds once the season is over -- mid-season most schools are still under 20.
REGULAR_SEASON_END = {"2026": "2026-10-31"}

NOT_PLAYING_MARKER = re.compile(
    r"schools\s+(?:not\s+(?:playing|participating)\s+in|excluded\s+from)\s+the\s+playoff", re.I
)
NO_EXCLUSIONS_MARKER = re.compile(r"there\s+are\s+no\s+schools", re.I)
DIVISION_RE = re.compile(r"Division\s+(V|IV|III|II|I)\b")
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Schools LHSAA has said are not playing in the playoffs but that its report
# does not show yet: {"2026": {"School Name": "reason"}}.  Names as they appear
# in the volleyball ratings.
MANUAL_NOT_PLAYING = {
    "2026": {},
}


def _strip_tags(html):
    return re.sub(r"<[^>]+>", " ", html)


def school_key(name):
    """Punctuation/case-insensitive key so LHSAA spellings match our rows."""
    try:
        from school_database import loose_school_key
        return loose_school_key(name)
    except Exception:
        return re.sub(r"[^a-z0-9]", "", str(name or "").lower())


def _number(text):
    try:
        return float(str(text).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def parse_report(html, details=None):
    """Return {"ranked": {div: [names]}, "not_playing": {div: [names]}}.

    When a `details` list is passed, one dict per school row is appended to it
    with LHSAA's own rank, power rating, wins and losses (for parity checks).

    The report is a run of tables; the text just before each table names its
    division and, for a division's second table, says the schools are not
    playing (or not participating) in the playoff.
    """
    from bs4 import BeautifulSoup

    ranked, not_playing = {}, {}
    parts = re.split(r"(?i)<table", html)
    division = None
    for index in range(1, len(parts)):
        before = _strip_tags(parts[index - 1])
        found = DIVISION_RE.findall(before)
        if found:
            division = found[-1]
        marker = NOT_PLAYING_MARKER.search(before)
        # "There are No Schools excluded from the Playoff." is not a list.
        is_not_playing = bool(marker) and not NO_EXCLUSIONS_MARKER.search(before[max(0, marker.start() - 40):marker.end()])
        table_html = "<table" + re.split(r"(?i)</table>", parts[index])[0] + "</table>"
        soup = BeautifulSoup(table_html, "html.parser")
        names = []
        for row in soup.find_all("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
            if len(cells) >= 4 and cells[0].isdigit() and cells[1]:
                name = re.sub(r"\s+", " ", cells[1]).strip()
                names.append(name)
                if details is not None:
                    details.append({
                        "division": division, "school": name, "rank": int(cells[0]),
                        "power": _number(cells[2]), "wins": _number(cells[3]),
                        "losses": _number(cells[4]) if len(cells) > 4 else None,
                        "low_games": "low-games" in str(row),
                        "not_playing": is_not_playing,
                        "cells": cells,
                    })
        if not names or division is None:
            continue
        target = not_playing if is_not_playing else ranked
        target.setdefault(division, []).extend(names)
    return {"ranked": ranked, "not_playing": not_playing}


def _ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS playoff_eligibility (
            sport       TEXT NOT NULL,
            season      TEXT NOT NULL,
            school      TEXT NOT NULL,
            track       TEXT,
            division    TEXT,
            eligible    INTEGER NOT NULL,
            source      TEXT,
            updated_at  TEXT,
            PRIMARY KEY (sport, season, school)
        )
    """)


def update_playoff_eligibility(season, db_path=None, fetch=None):
    """Read the LHSAA report and store each listed school's playoff status.

    Only replaces the stored list when the report produced ranked schools in
    at least three divisions, so a partial or failed read never wipes good data.
    """
    fetch = fetch or (lambda url: requests.get(url, headers=HEADERS, timeout=30).text)
    details = []
    try:
        parsed = parse_report(fetch(REPORT_URL), details)
    except Exception as exc:
        return {"updated": False, "error": f"volleyball report: {exc}"}
    if len(parsed["ranked"]) < 3:
        return {"updated": False, "error": "volleyball report had too few ranked divisions"}

    rows = []
    for key, eligible in (("ranked", 1), ("not_playing", 0)):
        for division, names in parsed[key].items():
            for name in names:
                rows.append((f"Division {division}", name, eligible))

    conn = sqlite3.connect(db_path or DB_PATH)
    try:
        _ensure_table(conn)
        now = datetime.now().isoformat()
        conn.execute("DELETE FROM playoff_eligibility WHERE sport=? AND season=?", (SPORT, str(season)))
        conn.executemany("""
            INSERT OR REPLACE INTO playoff_eligibility
                (sport, season, school, track, division, eligible, source, updated_at)
            VALUES (?, ?, ?, NULL, ?, ?, 'LHSAA power rating report', ?)
        """, [(SPORT, str(season), school, division, eligible, now) for division, school, eligible in rows])
        _store_official_snapshot(conn, season, details, now)
        conn.commit()
    finally:
        conn.close()
    not_playing = sorted(school for _, school, eligible in rows if not eligible)
    return {"updated": True, "schools": len(rows), "not_playing": not_playing}


def _store_official_snapshot(conn, season, details, now):
    """Keep LHSAA's own numbers so /api/parity/volleyball can compare them to ours."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lhsaa_official_ratings (
            sport TEXT NOT NULL, season TEXT NOT NULL, school TEXT NOT NULL,
            division TEXT, rank INTEGER, power REAL, wins REAL, losses REAL,
            low_games INTEGER, not_playing INTEGER, fetched_at TEXT, raw TEXT,
            PRIMARY KEY (sport, season, school)
        )
    """)
    try:
        conn.execute("ALTER TABLE lhsaa_official_ratings ADD COLUMN raw TEXT")
    except sqlite3.OperationalError:
        pass
    conn.execute("DELETE FROM lhsaa_official_ratings WHERE sport=? AND season=?", (SPORT, str(season)))
    conn.executemany("""
        INSERT OR REPLACE INTO lhsaa_official_ratings
            (sport, season, school, division, rank, power, wins, losses, low_games, not_playing, fetched_at, raw)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [(SPORT, str(season), d["school"], f"Division {d['division']}", d["rank"], d["power"],
           d["wins"], d["losses"], int(d["low_games"]), int(d["not_playing"]), now, json.dumps(d["cells"])) for d in details])


def parity_report(conn, season, tolerance=0.01):
    """Compare LVAY volleyball ratings with the last stored LHSAA report."""
    try:
        official = conn.execute(
            "SELECT school, division, rank, power, wins, losses, not_playing, fetched_at "
            "FROM lhsaa_official_ratings WHERE sport=? AND season=?", (SPORT, str(season))
        ).fetchall()
    except sqlite3.OperationalError:
        official = []
    if not official:
        return {"season": str(season), "available": False,
                "message": "No LHSAA volleyball report stored yet; run the volleyball pipeline."}
    ours = {school_key(r[0]): r for r in conn.execute(
        "SELECT school, division, power_rating, wins, losses, games_played "
        "FROM volleyball_rankings WHERE sport=? AND season=?", (SPORT, str(season))
    ).fetchall()}
    seen, exact, mismatches, missing = set(), 0, [], []
    for school, division, rank, power, wins, losses, not_playing, fetched_at in official:
        key = school_key(school)
        seen.add(key)
        mine = ours.get(key)
        if not mine:
            missing.append({"school": school, "division": division, "lhsaa_power": power})
            continue
        problems = []
        if power is not None and abs((mine[2] or 0) - power) > tolerance:
            problems.append("power")
        if wins is not None and int(wins) != int(mine[3] or 0):
            problems.append("wins")
        if losses is not None and int(losses) != int(mine[4] or 0):
            problems.append("losses")
        if division and mine[1] and division != mine[1]:
            problems.append("division")
        if problems:
            mismatches.append({
                "school": school, "lvay_school": mine[0], "problems": problems,
                "lhsaa": {"division": division, "rank": rank, "power": power, "wins": wins, "losses": losses},
                "lvay": {"division": mine[1], "power": round(mine[2] or 0, 3), "wins": mine[3],
                         "losses": mine[4], "games_played": mine[5]},
                "power_diff": None if power is None else round((mine[2] or 0) - power, 3),
            })
        else:
            exact += 1
    extra = sorted(r[0] for k, r in ours.items() if k not in seen)
    mismatches.sort(key=lambda m: -abs(m["power_diff"] or 0))
    return {
        "season": str(season), "available": True, "lhsaa_fetched_at": official[0][7],
        "lhsaa_schools": len(official), "exact_matches": exact, "mismatch_count": len(mismatches),
        "mismatches": mismatches, "missing_from_lvay": missing, "not_in_lhsaa_report": extra,
        "sample_lhsaa_row": _sample_row(conn, season),
    }


def _sample_row(conn, season):
    row = conn.execute(
        "SELECT raw FROM lhsaa_official_ratings WHERE sport=? AND season=? AND raw IS NOT NULL LIMIT 1",
        (SPORT, str(season))).fetchone()
    return json.loads(row[0]) if row and row[0] else None


def not_playing_keys(conn, season):
    """{school_key: reason} for schools NOT playing in the volleyball playoffs."""
    out = {}
    try:
        rows = conn.execute(
            "SELECT school, eligible, updated_at FROM playoff_eligibility "
            "WHERE sport=? AND season=? AND eligible=0",
            (SPORT, str(season)),
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for row in rows:
        out[school_key(row[0])] = "LHSAA: not playing in the playoff"
    for name, reason in MANUAL_NOT_PLAYING.get(str(season), {}).items():
        out[school_key(name)] = reason or "Not playing in the playoff"
    return out


def last_updated(conn, season):
    try:
        row = conn.execute(
            "SELECT MAX(updated_at) FROM playoff_eligibility WHERE sport=? AND season=?",
            (SPORT, str(season)),
        ).fetchone()
        return row[0] if row else None
    except sqlite3.OperationalError:
        return None


def season_over(season, today=None):
    end = REGULAR_SEASON_END.get(str(season))
    today = today or datetime.now().strftime("%Y-%m-%d")
    return bool(end) and today > end


def _low_matches(row):
    """Bylaw 24.7.1: fewer than 20 regular-season matches played."""
    gp = row.get("games_played")
    return gp is not None and int(gp) < MIN_MATCHES


def _roman(division):
    m = DIVISION_RE.search(f"Division {str(division or '').replace('Division', '').strip()}")
    return m.group(1) if m else None


def _division_play(conn, season):
    """{school_key: (wins, losses)} against schools in the same division (24.7.2 #2).

    volleyball_games stores opp_division as just the numeral ("V"), so both
    sides are reduced to the numeral before comparing.
    """
    out = {}
    try:
        rows = conn.execute(
            "SELECT school, school_division, opp_division, result FROM volleyball_games "
            "WHERE season=? AND counts_for_pr=1", (str(season),)
        ).fetchall()
    except sqlite3.OperationalError:
        return out
    for school, mine, theirs, result in rows:
        if not _roman(mine) or _roman(mine) != _roman(theirs):
            continue
        w, l = out.get(school_key(school), (0, 0))
        r = str(result or "").upper()[:1]
        out[school_key(school)] = (w + (r == "W"), l + (r == "L"))
    return out


def _head_to_head(conn, season, a, b):
    """Matches a won minus matches b won, from a's schedule (24.7.2 #1)."""
    try:
        rows = conn.execute(
            "SELECT opponent, result FROM volleyball_games WHERE season=? AND school=? AND counts_for_pr=1",
            (str(season), a)).fetchall()
    except sqlite3.OperationalError:
        return 0
    kb, net = school_key(b), 0
    for opp, result in rows:
        if school_key(opp) == kb:
            r = str(result or "").upper()[:1]
            net += (r == "W") - (r == "L")
    return net


def seed_field(field, conn, season, apply_low=True):
    """Order a division's playoff field the way LHSAA seeds it.

    Bylaw 24.7.1: schools with fewer than 20 matches are ranked separately and
    seeded below everyone else (they still make the field on power rating).
    Bylaw 24.7.2: power ratings are compared as published (2 decimals); a tie
    goes to head-to-head (two-team ties), then divisional play percentage.
    """
    dp = _division_play(conn, season)

    def pct(row):
        w, l = dp.get(school_key(row.get("school")), (0, 0))
        return w / (w + l) if w + l else 0.0

    low = _low_matches if apply_low else (lambda r: False)
    ordered = sorted(field, key=lambda r: (low(r), -round(r.get("power_rating") or 0, 2), -pct(r)))
    # head-to-head first for two-team ties
    i = 0
    while i < len(ordered) - 1:
        a, b = ordered[i], ordered[i + 1]
        same = (low(a) == low(b)
                and round(a.get("power_rating") or 0, 2) == round(b.get("power_rating") or 0, 2))
        three = (i + 2 < len(ordered) and low(ordered[i + 2]) == low(a)
                 and round(ordered[i + 2].get("power_rating") or 0, 2) == round(a.get("power_rating") or 0, 2))
        prev = (i > 0 and low(ordered[i - 1]) == low(a)
                and round(ordered[i - 1].get("power_rating") or 0, 2) == round(a.get("power_rating") or 0, 2))
        if same and not three and not prev:
            net = _head_to_head(conn, season, a.get("school"), b.get("school"))
            if net < 0:
                ordered[i], ordered[i + 1] = b, a
            i += 2
            continue
        i += 1
    return ordered


def annotate_rankings(rows, conn, season, today=None):
    """Flag ineligible schools and number each division the way LHSAA seeds it.

    Adds playoff_eligible, low_matches, division_rank (None when not playing)
    and in_playoff_field.  The field is the top 32 eligible schools by power
    rating; inside the field division_rank is the LHSAA seed (Bylaw 24.7:
    under-20-match schools at the bottom, 24.7.2 tiebreakers).  Schools outside
    the field keep power rating order from 33 down.  Rows must already be in
    power rating order within each division (div_rank / power_rating desc).
    """
    out = not_playing_keys(conn, season)
    apply_low = season_over(season, today)
    by_div = {}
    for row in sorted(rows, key=lambda r: (r.get("division") or "", r.get("div_rank") or 9999)):
        reason = out.get(school_key(row.get("school")))
        row["playoff_eligible"] = reason is None
        row["not_playing_reason"] = reason
        row["low_matches"] = apply_low and _low_matches(row)
        if reason is None:
            by_div.setdefault(row.get("division"), []).append(row)
        else:
            row["division_rank"] = None
            row["in_playoff_field"] = False
    for division, eligible in by_div.items():
        field = seed_field(eligible[:PLAYOFF_FIELD], conn, season, apply_low)
        for i, row in enumerate(field + eligible[PLAYOFF_FIELD:], 1):
            row["division_rank"] = i
            row["in_playoff_field"] = i <= PLAYOFF_FIELD
    return rows
