"""Verify 2026 football out-of-state records and import approved rows.

The Google Sheet is the audit/control surface.  Source pages are checked before
ratings run, but only rows marked ``Include in Engine? = Yes`` are written to
``oos_opponents``.  A failed check never deletes the last known-good DB record.
"""

from __future__ import annotations

import os
import json
import re
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse

import gspread
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials


DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")
SHEET_ID = os.environ.get(
    "FOOTBALL_OOS_SHEET_ID", "1IWrrYD8YIjV_uXuZUSxIHJQxoooBrmGZqFjD3J6BgbE"
)
TAB_NAME = os.environ.get("FOOTBALL_OOS_TAB", "OOS Team Registry")
REGISTRY_RANGE = "A4:X1000"
SEASON = os.environ.get("FOOTBALL_SEASON_YEAR", os.environ.get("SEASON_YEAR", "2026"))
CREDS_PATH = os.environ.get(
    "GOOGLE_CREDENTIALS_PATH", "/etc/secrets/google-credentials.json"
)
TIMEOUT = int(os.environ.get("OOS_SOURCE_TIMEOUT_SECONDS", "20"))

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; LVAY-OOS-Verifier/1.0; "
        "+https://louisianavsallyall.com/)"
    )
}


@dataclass(frozen=True)
class Observation:
    wins: int
    losses: int
    ties: int
    url: str
    provider: str

    @property
    def record(self):
        return self.wins, self.losses, self.ties


def _provider(url):
    host = urlparse(url).netloc.lower().removeprefix("www.")
    if host == "scores.misshsaa.com":
        return "MHSAA/SBLive"
    if "maxpreps.com" in host:
        return "MaxPreps"
    if "nfhsnetwork.com" in host:
        return "NFHS Network"
    if host:
        return host
    return "Unknown"


def _current_season_present(text, season):
    year = int(season)
    short_next = str(year + 1)[-2:]
    markers = (f"{year}-{year + 1}", f"{str(year)[-2:]}-{short_next}")
    return any(marker in text for marker in markers)


def parse_record(html_text, url, season=SEASON):
    """Extract a current-season overall record from a supported page."""
    soup = BeautifulSoup(html_text, "lxml")
    text = " ".join(soup.stripped_strings)
    provider = _provider(url)

    # MHSAA's official SBLive team schedule pages display records as
    # ``1-0 Overall`` (the association's dated scoreboard links into these
    # pages).  Restrict this rule to team schedule/standings URLs so a global
    # scoreboard cannot accidentally be attributed to one school.
    if provider == "MHSAA/SBLive" and re.search(r"/teams/\d+/(?:schedule|standings)", url):
        match = re.search(
            r"\b(\d{1,2})-(\d{1,2})(?:-(\d{1,2}))?\s+Overall\b",
            text,
            re.IGNORECASE,
        )
        if match:
            return tuple(int(value or 0) for value in match.groups())

    # MaxPreps' visible team record is stable and unambiguous:
    # <h4>Overall</h4><div class="data">2-0</div>.
    if provider == "MaxPreps":
        overall = soup.find(
            lambda tag: tag.name in ("h3", "h4", "span", "div")
            and tag.get_text(" ", strip=True).lower() == "overall"
        )
        if overall:
            candidate = overall.find_next(
                lambda tag: tag.name in ("div", "span", "p")
                and re.fullmatch(r"\d{1,2}-\d{1,2}(?:-\d{1,2})?", tag.get_text(" ", strip=True))
            )
            if candidate and _current_season_present(text, season):
                parts = [int(value) for value in candidate.get_text(strip=True).split("-")]
                return tuple((parts + [0])[:3])

    # Conservative generic patterns for official team sites.  We require an
    # explicit Overall label and a current-season marker to avoid old records.
    patterns = (
        r"\bOverall(?:\s+Record)?\s*[:\-]?\s*(\d{1,2})-(\d{1,2})(?:-(\d{1,2}))?\b",
        r"\bRecord\s*[:\-]\s*(\d{1,2})-(\d{1,2})(?:-(\d{1,2}))?\b",
    )
    if _current_season_present(text, season):
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return tuple(int(value or 0) for value in match.groups())
    return None


def fetch_observation(url, season=SEASON, session=None):
    if not url:
        return None, "blank URL"
    client = session or requests.Session()
    try:
        response = client.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as exc:
        return None, f"request failed: {exc}"
    record = parse_record(response.text, response.url, season)
    if record is None:
        return None, "no current-season overall record found"
    return Observation(*record, url=response.url, provider=_provider(response.url)), None


def choose_verified_record(observations):
    """Return (record, reason) when evidence is safe enough to publish."""
    if not observations:
        return None, "No source returned a current-season record"
    # For Mississippi members, the association-branded scoreboard is the
    # controlling source.  MaxPreps and other URLs remain cross-checks, but a
    # lagging secondary feed must not override MHSAA's published record.
    mhsaa = [item for item in observations if item.provider == "MHSAA/SBLive"]
    if mhsaa:
        mhsaa_counts = Counter(item.record for item in mhsaa)
        record, count = mhsaa_counts.most_common(1)[0]
        if len(mhsaa_counts) == 1:
            return record, "MHSAA/SBLive primary; secondary sources used as cross-checks"
        if count > 1:
            return record, "MHSAA/SBLive primary consensus"
        return None, "MHSAA/SBLive sources reported conflicting records"
    counts = Counter(item.record for item in observations)
    record, count = counts.most_common(1)[0]
    if len(counts) > 1 and count == 1:
        return None, "Sources reported conflicting records"
    matching = [item for item in observations if item.record == record]
    distinct_providers = {item.provider for item in matching}
    if len(distinct_providers) >= 2:
        return record, "Matched by " + ", ".join(sorted(distinct_providers))
    # MaxPreps is an official data partner in several registry states.  Two
    # separate MaxPreps endpoints agreeing is accepted, but clearly disclosed.
    maxpreps_urls = {item.url for item in matching if item.provider == "MaxPreps"}
    if count >= 2 and len(maxpreps_urls) >= 2:
        return record, "Matched by two MaxPreps endpoints"
    if count == 1 and matching[0].provider == "MaxPreps":
        return record, "Single current-season MaxPreps record"
    return None, "Sources did not produce a publishable consensus"


def _truthy(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _split_louisiana_schools(value):
    return [part.strip() for part in re.split(r"[;\n]+", str(value or "")) if part.strip()]


def _ensure_oos_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oos_opponents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sport TEXT NOT NULL,
            season TEXT NOT NULL,
            school TEXT NOT NULL,
            opponent TEXT NOT NULL,
            opp_wins INTEGER DEFAULT 0,
            opp_losses INTEGER DEFAULT 0,
            opp_ties INTEGER DEFAULT 0,
            division TEXT DEFAULT '',
            class_ TEXT DEFAULT '',
            source_url TEXT DEFAULT '',
            verified_at TEXT DEFAULT '',
            UNIQUE(sport, season, school, opponent)
        )
    """)
    existing = {row[1] for row in conn.execute("PRAGMA table_info(oos_opponents)")}
    migrations = {
        "opp_ties": "INTEGER DEFAULT 0",
        "division": "TEXT DEFAULT ''",
        "class_": "TEXT DEFAULT ''",
        "source_url": "TEXT DEFAULT ''",
        "verified_at": "TEXT DEFAULT ''",
    }
    for name, definition in migrations.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE oos_opponents ADD COLUMN {name} {definition}")


def import_verified_row(conn, row, record, source_url, checked_at):
    wins, losses, ties = record
    opponent = str(row.get("OOS School") or "").strip()
    classification = str(row.get("LHSAA Class") or "").strip()
    imported = 0
    for school in _split_louisiana_schools(row.get("Louisiana Opponent(s)")):
        conn.execute("""
            INSERT INTO oos_opponents (
                sport, season, school, opponent, opp_wins, opp_losses,
                opp_ties, division, class_, source_url, verified_at
            ) VALUES ('football', ?, ?, ?, ?, ?, ?, '', ?, ?, ?)
            ON CONFLICT(sport, season, school, opponent) DO UPDATE SET
                opp_wins=excluded.opp_wins,
                opp_losses=excluded.opp_losses,
                opp_ties=excluded.opp_ties,
                division='',
                class_=excluded.class_,
                source_url=excluded.source_url,
                verified_at=excluded.verified_at
        """, (
            str(row.get("Season") or SEASON), school, opponent, wins, losses,
            ties, classification, source_url, checked_at,
        ))
        imported += 1
    return imported


def _open_registry():
    if os.path.exists(CREDS_PATH):
        creds = Credentials.from_service_account_file(CREDS_PATH, scopes=SCOPES)
    else:
        raw_credentials = (
            os.environ.get("GOOGLE_CREDENTIALS_JSON")
            or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        )
        if not raw_credentials:
            raise RuntimeError("No Google service-account credentials configured")
        creds = Credentials.from_service_account_info(
            json.loads(raw_credentials), scopes=SCOPES
        )
    return gspread.authorize(creds).open_by_key(SHEET_ID).worksheet(TAB_NAME)


def run(db_path=None, worksheet=None, session=None):
    """Check every registry row, update its audit cells, and import enabled rows."""
    ws = worksheet or _open_registry()
    # The managed registry is deliberately limited to A:X.  Some Sheets can
    # retain stale/duplicate headers in columns to the right; reading the whole
    # worksheet lets those duplicates overwrite the real control columns when
    # rows are converted to dictionaries.
    all_values = ws.get(REGISTRY_RANGE)
    if not all_values:
        raise RuntimeError("OOS registry does not contain its row-4 header")
    headers = all_values[0]
    rows = [dict(zip(headers, values + [""] * (len(headers) - len(values)))) for values in all_values[1:]]
    header_index = {name: index + 1 for index, name in enumerate(headers)}
    required = {
        "Season", "OOS School", "Louisiana Opponent(s)", "LHSAA Class",
        "Primary Record URL", "Secondary Record URL", "Backup Record URL",
        "Wins", "Losses", "Ties", "Games Played", "Record Through Date",
        "Verified By", "Verification Status", "Include in Engine?",
        "Last Checked", "Notes",
    }
    missing = required - set(header_index)
    if missing:
        raise RuntimeError(f"OOS registry missing columns: {sorted(missing)}")

    checked_at = datetime.now().strftime("%Y-%m-%d %I:%M %p")
    session = session or requests.Session()
    cache = {}
    updates = []
    summary = {"checked": 0, "verified": 0, "review": 0, "imported": 0, "changes": [], "issues": []}
    conn = sqlite3.connect(db_path or DB_PATH)
    _ensure_oos_table(conn)
    try:
        for offset, row in enumerate(rows, start=5):
            if str(row.get("Season") or "").strip() != str(SEASON):
                continue
            team = str(row.get("OOS School") or "").strip()
            if not team:
                continue
            summary["checked"] += 1
            observations, errors = [], []
            for label in ("Primary Record URL", "Secondary Record URL", "Backup Record URL"):
                url = str(row.get(label) or "").strip()
                if not url:
                    errors.append(f"{label}: blank")
                    continue
                if url not in cache:
                    cache[url] = fetch_observation(url, str(row.get("Season") or SEASON), session)
                observation, error = cache[url]
                if observation:
                    observations.append(observation)
                else:
                    errors.append(f"{label}: {error}")

            record, reason = choose_verified_record(observations)
            old_record = (
                str(row.get("Wins") or ""), str(row.get("Losses") or ""),
                str(row.get("Ties") or ""),
            )
            if record:
                wins, losses, ties = record
                try:
                    previous_games = sum(int(value or 0) for value in old_record)
                except (TypeError, ValueError):
                    previous_games = 0
                if wins + losses + ties < previous_games:
                    summary["review"] += 1
                    summary["issues"].append(
                        f"{team}: source record has fewer games than the last verified record"
                    )
                    record = None
                    wins, losses, ties = old_record
                    status = "Needs Review"
                    source = str(row.get("Record Source") or "")
                    record_through = str(row.get("Record Through Date") or "")
                    notes = "Source record has fewer games than the last verified record"
                else:
                    status = "Verified"
                    source = ", ".join(sorted({item.provider for item in observations if item.record == record}))
                    record_through = datetime.now().strftime("%Y-%m-%d")
                    notes = reason
                    summary["verified"] += 1
                    if old_record != (str(wins), str(losses), str(ties)):
                        summary["changes"].append(
                            f"{team}: {old_record[0] or '—'}-{old_record[1] or '—'} to {wins}-{losses}"
                        )
                    if _truthy(row.get("Include in Engine?")):
                        best_url = next(item.url for item in observations if item.record == record)
                        summary["imported"] += import_verified_row(
                            conn, row, record, best_url, checked_at
                        )
            else:
                wins, losses, ties = old_record
                status = "Needs Review"
                source = str(row.get("Record Source") or "")
                record_through = str(row.get("Record Through Date") or "")
                notes = reason
                summary["review"] += 1
                summary["issues"].append(f"{team}: {reason}")
            if errors:
                notes += "; " + " | ".join(errors)

            games = ""
            try:
                games = str(int(wins) + int(losses) + int(ties or 0))
            except (TypeError, ValueError):
                pass
            values = {
                "Record Source": source,
                "Wins": str(wins), "Losses": str(losses), "Ties": str(ties),
                "Games Played": games, "Record Through Date": record_through,
                "Verified By": "LVAY OOS Verifier", "Verification Status": status,
                "Last Checked": checked_at, "Notes": notes[:500],
            }
            for column, value in values.items():
                updates.append({"range": gspread.utils.rowcol_to_a1(offset, header_index[column]), "values": [[value]]})
        if updates:
            ws.batch_update(updates, value_input_option="USER_ENTERED")
        conn.commit()
    finally:
        conn.close()
    print(
        "[OOS] Checked {checked}; verified {verified}; needs review {review}; "
        "engine rows updated {imported}".format(**summary)
    )
    return summary


if __name__ == "__main__":
    run()
