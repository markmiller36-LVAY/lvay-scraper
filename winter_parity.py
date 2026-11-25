"""
LHSAA live power rating reports for basketball and soccer (parity checks).

Read-only: fetches LHSAA's own report, parses every school row and returns it.
Nothing is written to the database.  Used by /api/parity/lhsaa/<sport> so the
team-by-team comparison with LVAY can be run from anywhere (LHSAA blocks
cross-origin browser reads, and some networks can't reach lhsaaonline.org).

Basketball: static reports /rpt/BBbyClassPR.html (boys), /rpt/GBBbyClassPR.html (girls).
Soccer: /PR/sopr/admin/ReportPowerByDivision.asp -- LHSAA picks boys vs girls from
the session, so the sport's search page is opened first in the same session
(same trick as the soccer schedule scrape).  The report title is returned so the
caller can confirm the right gender came back.
"""

import re

import requests

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

SOCCER_REPORT = ("https://www.lhsaaonline.org/PR/sopr/admin/ReportPowerByDivision.asp"
                 "?d=0&ExcludeNewSchools=1&NotInPlayoff=1&p=1")

REPORTS = {
    "boys_basketball": {"url": "https://www.lhsaaonline.org/rpt/BBbyClassPR.html"},
    "girls_basketball": {"url": "https://www.lhsaaonline.org/rpt/GBBbyClassPR.html"},
    "boys_soccer": {
        "url": SOCCER_REPORT,
        "session_pages": ["https://www.lhsaaonline.org/pr/sopr/admin/SearchboyssoccerSchedule.asp"],
        "title_word": "boys",
    },
    "girls_soccer": {
        "url": SOCCER_REPORT,
        "session_pages": ["https://www.lhsaaonline.org/pr/sopr/admin/SearchgirlssoccerSchedule.asp"],
        "title_word": "girls",
    },
}

NOT_PLAYING_RE = re.compile(
    r"schools\s+(?:not\s+(?:playing|participating)\s+in|excluded\s+from)\s+the\s+playoff", re.I)
NO_EXCLUSIONS_RE = re.compile(r"there\s+are\s+no\s+schools", re.I)
GROUP_RE = re.compile(
    r"((?:Non-?Select|Select)?\s*(?:Division\s+(?:IV|III|II|I)\b|Class\s+(?:5A|4A|3A|2A|1A|B|C)\b)"
    r"|\b(?:5A|4A|3A|2A|1A)\b|\bClass\s+[BC]\b)", re.I)
DATE_RE = re.compile(r"Date:\s*([0-9/]+\s+[0-9:]+\s*[AP]M)", re.I)
TITLE_RE = re.compile(r"((?:Boys|Girls)\s+(?:Basketball|Soccer)[^<\n]{0,80}?(?:Ranking|Rating)[^<\n]{0,40})", re.I)


def _text(html):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html or ""))


def _num(value):
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def parse_report(html):
    """Return (rows, meta). One row per school line in every table."""
    from bs4 import BeautifulSoup

    rows = []
    parts = re.split(r"(?i)<table", html)
    group = None
    for index in range(1, len(parts)):
        before = _text(parts[index - 1])
        found = GROUP_RE.findall(before[-400:])
        if found:
            group = re.sub(r"\s+", " ", found[-1]).strip()
        marker = NOT_PLAYING_RE.search(before[-400:])
        not_playing = bool(marker) and not NO_EXCLUSIONS_RE.search(before[-400:])
        table_html = "<table" + re.split(r"(?i)</table>", parts[index])[0] + "</table>"
        for tr in BeautifulSoup(table_html, "html.parser").find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all("td")]
            if len(cells) < 4 or not cells[0].rstrip(".").isdigit() or not cells[1]:
                continue
            nums = cells[2:]
            row = {
                "group": group, "rank": int(cells[0].rstrip(".")),
                "school": re.sub(r"\s+", " ", cells[1]).strip(),
                "power": _num(nums[0]) if nums else None,
                "wins": _num(nums[1]) if len(nums) > 1 else None,
                "losses": _num(nums[2]) if len(nums) > 2 else None,
                "ties": None, "district": nums[-1] if nums else None,
                "not_playing": not_playing, "low_games": "red" in str(tr).lower(),
                "cells": cells,
            }
            # Soccer adds a Ties column: # School PR W L T District.
            if len(nums) >= 5:
                row["ties"] = _num(nums[3])
            rows.append(row)
    text = _text(html)
    date = DATE_RE.search(text)
    title = TITLE_RE.search(text)
    meta = {"as_of": date.group(1) if date else None,
            "title": title.group(1).strip() if title else None}
    return rows, meta


def fetch_report(sport, timeout=60):
    config = REPORTS.get(sport)
    if not config:
        raise ValueError(f"unknown sport {sport}")
    session = requests.Session()
    session.headers.update(HEADERS)
    for page in config.get("session_pages", []):
        session.get(page, timeout=timeout)
    response = session.get(config["url"], timeout=timeout,
                           headers={"Referer": (config.get("session_pages") or [config["url"]])[0]})
    response.raise_for_status()
    rows, meta = parse_report(response.text)
    word = config.get("title_word")
    meta["gender_ok"] = None if not word else bool(meta.get("title") and word in meta["title"].lower())
    return {"sport": sport, "url": config["url"], "count": len(rows), **meta, "rows": rows}
