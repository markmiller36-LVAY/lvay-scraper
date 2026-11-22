"""Soccer coach stat submissions for the pipeline report email.

Reads the WordPress feed /wp-json/lvay/v1/coach-stats (snippet #295), which
lists every team whose coach has saved stats on the private coach-stats form.
Uses the same LVAY_STATS_KEY header as the fan sign-up feed.
"""

from __future__ import annotations

import html
import os

COACH_STATS_URL = os.environ.get(
    "LVAY_COACH_STATS_URL",
    "https://louisianavsallyall.com/wp-json/lvay/v1/coach-stats",
)


def fetch_coach_stats():
    """Coach stat submissions from WordPress. None if unavailable."""
    key = os.environ.get("LVAY_STATS_KEY", "").strip()
    if not key:
        return None
    try:
        import requests
        response = requests.get(
            COACH_STATS_URL,
            headers={"X-LVAY-Key": key, "User-Agent": "LVAY-Pipeline-Report"},
            timeout=20,
        )
        response.raise_for_status()
        return response.json()
    except Exception as error:  # never let this block the report
        print(f"[REPORT] Coach stats unavailable: {error}")
        return None


def _esc(value):
    return html.escape(str(value if value is not None else ""))


def _int(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def coach_stats_section(data):
    """HTML section listing who has entered soccer stats. Empty string if no data."""
    if not data:
        return ""
    totals = data.get("totals") or {}
    teams = data.get("teams") or []

    tiles = "".join(
        f"<td style=\"border:1px solid #ccc;padding:8px;text-align:center\">"
        f"<div style=\"font-size:22px;font-weight:bold;color:#008584\">{_int(totals.get(key))}</div>"
        f"<div style=\"font-size:12px;color:#555\">{_esc(label)}</div></td>"
        for key, label in (
            ("teams", "Teams reporting"),
            ("boys", "Boys teams"),
            ("girls", "Girls teams"),
            ("updated_24h", "Updated last 24 hrs"),
            ("players", "Players entered"),
        )
    )

    rows = ""
    for team in teams:
        other = []
        if team.get("maxpreps"):
            other.append("MaxPreps OK")
        if _int(team.get("photos")):
            other.append(f"{_int(team.get('photos'))} photo(s)")
        played = _int(team.get("games_played"))
        games = f"{_int(team.get('games_with_stats'))}" + (f" of {played}" if played else "")
        coach = _esc(team.get("coach") or "—")
        if team.get("role"):
            coach += f" <span style=\"color:#777\">({_esc(team.get('role'))})</span>"
        if team.get("display_name"):
            found = "&#10003; account" if team.get("account") else "no account found"
            coach += (f"<br><span style=\"font-size:12px;color:#555\">LVAY: {_esc(team.get('display_name'))}"
                      f" ({found})</span>")
        new = " <strong style=\"color:#0a7a3b\">NEW</strong>" if team.get("new_24h") else ""
        rows += (
            f"<tr><td>{_esc(team.get('sport'))}</td><td>{_esc(team.get('school'))}{new}</td>"
            f"<td>{coach}</td><td>{games}</td><td>{_int(team.get('players'))}</td>"
            f"<td>{_int(team.get('goals'))}</td><td>{_int(team.get('assists'))}</td>"
            f"<td>{_esc(', '.join(other) or '—')}</td><td>{_esc(team.get('updated_ct'))}</td></tr>"
        )
    if not rows:
        rows = "<tr><td colspan=\"9\">No coaches have entered stats yet.</td></tr>"

    return f"""
    <h2>Soccer Coach Stats</h2>
    <table><tr>{tiles}</tr></table>
    <table><thead><tr><th>Sport</th><th>School</th><th>Entered by</th><th>Games with stats</th><th>Players</th><th>Goals</th><th>Assists</th><th>Other</th><th>Last saved (CT)</th></tr></thead><tbody>{rows}</tbody></table>
    <p style="font-family:Arial,sans-serif;color:#777;font-size:12px">NEW = saved in the last 24 hours. Newest first.</p>"""


def coach_stats_subject(data):
    """Short subject-line add-on, e.g. ' — 3 soccer stat teams (+1 today)'."""
    totals = (data or {}).get("totals") or {}
    if not _int(totals.get("teams")):
        return ""
    return f" — {_int(totals.get('teams'))} soccer stat teams (+{_int(totals.get('updated_24h'))} today)"
