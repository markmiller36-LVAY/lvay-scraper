"""
Football playoff eligibility, matched to LHSAA.

LHSAA's football power rating reports list, under each division, a separate
"Schools not playing in the Playoff" table (non-district-honors schools, JV-only
programs, and schools that have opted out or been ruled ineligible).  LVAY must
rank only playoff-eligible schools inside a division, exactly like LHSAA.

`update_playoff_eligibility()` reads both LHSAA reports and stores the result in
the `playoff_eligibility` table.  If LHSAA can't be read, the last stored list
is kept.  If nothing has ever been stored for a season, `ineligible_schools()`
falls back to the alignment file (status non_district_honors / jv_only).
"""

import json
import os
import re
import sqlite3
from datetime import datetime

import requests

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")

REPORT_URLS = {
    "select": "https://www.lhsaaonline.org/rpt/FBSelectbyDivisionPR.html",
    "non-select": "https://www.lhsaaonline.org/rpt/FBNonselectByDivisionPR.html",
}
NOT_PLAYING_MARKER = re.compile(r"schools\s+not\s+playing\s+in\s+the\s+playoff", re.I)
DIVISION_RE = re.compile(r"Division\s+(IV|III|II|I)\b")
FALLBACK_STATUSES = {"non_district_honors", "jv_only"}
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _strip_tags(html):
    return re.sub(r"<[^>]+>", " ", html)


def parse_report(html):
    """Return {"ranked": {div: [names]}, "not_playing": {div: [names]}} from one LHSAA report.

    The report is a run of tables; the text just before each table names its
    division and, for the second table of a division, says
    "Schools not playing in the Playoff".
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
        is_not_playing = bool(NOT_PLAYING_MARKER.search(before))
        table_html = "<table" + re.split(r"(?i)</table>", parts[index])[0] + "</table>"
        soup = BeautifulSoup(table_html, "html.parser")
        names = []
        for row in soup.find_all("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
            if len(cells) >= 6 and cells[0].isdigit() and cells[1]:
                names.append(re.sub(r"\s+", " ", cells[1]).strip())
        if not names or division is None:
            continue
        target = not_playing if is_not_playing else ranked
        target.setdefault(division, []).extend(names)
    return {"ranked": ranked, "not_playing": not_playing}


def _canonical(name):
    try:
        from school_database import resolve_school_spelling
        resolved = resolve_school_spelling(name)
        if resolved:
            return resolved
    except Exception:
        pass
    return name


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
    """Read both LHSAA reports and store each listed school's playoff status.

    Returns a summary dict.  Only replaces the stored list when BOTH reports
    were read and each produced a ranked list, so a partial or failed read
    never wipes good data.
    """
    fetch = fetch or (lambda url: requests.get(url, headers=HEADERS, timeout=30).text)
    rows = []
    for track, url in REPORT_URLS.items():
        try:
            parsed = parse_report(fetch(url))
        except Exception as exc:
            return {"updated": False, "error": f"{track} report: {exc}"}
        if not parsed["ranked"]:
            return {"updated": False, "error": f"{track} report had no ranked schools"}
        prefix = "Select" if track == "select" else "Non-Select"
        for key, eligible in (("ranked", 1), ("not_playing", 0)):
            for division, names in parsed[key].items():
                for name in names:
                    rows.append((track, f"{prefix} Division {division}", _canonical(name), eligible))

    conn = sqlite3.connect(db_path or DB_PATH)
    try:
        _ensure_table(conn)
        now = datetime.now().isoformat()
        conn.execute("DELETE FROM playoff_eligibility WHERE sport='football' AND season=?", (str(season),))
        conn.executemany("""
            INSERT OR REPLACE INTO playoff_eligibility
                (sport, season, school, track, division, eligible, source, updated_at)
            VALUES ('football', ?, ?, ?, ?, ?, 'LHSAA power rating report', ?)
        """, [(str(season), school, track, division, eligible, now) for track, division, school, eligible in rows])
        conn.commit()
    finally:
        conn.close()
    not_playing = sorted(school for _, _, school, eligible in rows if not eligible)
    return {"updated": True, "schools": len(rows), "not_playing": not_playing}


def _alignment_fallback():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sport_alignments_2026_2027.json")
    try:
        with open(path, encoding="utf-8") as handle:
            football = json.load(handle)["sports"]["football"]
    except Exception:
        return set()
    schools = football.get("schools", football)
    names = set()
    for name, info in schools.items():
        if isinstance(info, dict) and info.get("alignment_status") in FALLBACK_STATUSES:
            names.add(name)
            names.add(_canonical(name))
    return names


def ineligible_schools(conn, season):
    """Set of school names that are NOT playing in the football playoffs this season."""
    try:
        rows = conn.execute(
            "SELECT school, eligible FROM playoff_eligibility WHERE sport='football' AND season=?",
            (str(season),),
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    if rows:
        return {row[0] for row in rows if not row[1]}
    return _alignment_fallback()
