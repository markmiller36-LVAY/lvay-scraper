"""Create and email a concise audit report after each LVAY pipeline run."""

from __future__ import annotations

import html
import os
import sqlite3
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")
REPORT_TO = os.environ.get("REPORT_EMAIL_TO", "markmiller36@gmail.com")
REPORT_FROM = os.environ.get(
    "REPORT_EMAIL_FROM", "LVAY Pipeline <onboarding@resend.dev>"
)


def _rows(conn, sql, params=()):
    try:
        return [dict(row) for row in conn.execute(sql, params)]
    except sqlite3.OperationalError:
        return []


def capture_snapshot(active_sports, db_path=None):
    """Capture completed games, game points, and rankings for change detection."""
    path = db_path or DB_PATH
    snapshot = {"games": {}, "points": {}, "ratings": {}}
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        for sport in active_sports:
            if sport == "volleyball":
                games = _rows(conn, """
                    SELECT sport, season, school, opponent, game_date,
                           result, score
                    FROM volleyball_games WHERE sport='volleyball'
                      AND result IN ('W','L')
                """)
                ratings = _rows(conn, """
                    SELECT sport, season, school, wins, losses,
                           games_played, power_rating
                    FROM volleyball_rankings WHERE sport='volleyball'
                """)
            else:
                games = _rows(conn, """
                    SELECT sport, season, school, opponent, game_date,
                           win_loss AS result, score
                    FROM games WHERE sport=?
                      AND win_loss IN ('W','L','T','Tie','W(f)','L(f)')
                """, (sport,))
                ratings = _rows(conn, """
                    SELECT sport, season, school, wins, losses, ties,
                           games_played, power_rating
                    FROM power_rankings WHERE sport=?
                """, (sport,))

            for row in games:
                key = tuple(str(row.get(k) or "") for k in
                            ("sport", "season", "school", "opponent", "game_date"))
                snapshot["games"][key] = row
            for row in ratings:
                key = tuple(str(row.get(k) or "") for k in
                            ("sport", "season", "school"))
                snapshot["ratings"][key] = row

        points = _rows(conn, """
            SELECT sport, season, school, opponent, game_date, result, score,
                   opp_wins, opp_losses, opp_ties, base_pts, div_bonus,
                   opp_quality, total_pts
            FROM game_power_points
        """)
        for row in points:
            if row.get("sport") not in active_sports:
                continue
            key = tuple(str(row.get(k) or "") for k in
                        ("sport", "season", "school", "opponent", "game_date"))
            snapshot["points"][key] = row
    finally:
        conn.close()
    return snapshot


def build_report(before, after, active_sports, started_at=None):
    game_changes = []
    for key, row in after["games"].items():
        previous = before["games"].get(key)
        if previous is None or (previous.get("result"), previous.get("score")) != (
            row.get("result"), row.get("score")
        ):
            item = dict(row)
            item.update(after["points"].get(key, {}))
            game_changes.append(item)

    rating_changes = []
    for key, row in after["ratings"].items():
        previous = before["ratings"].get(key)
        old = previous.get("power_rating") if previous else None
        new = row.get("power_rating")
        old_record = _record(previous) if previous else "—"
        new_record = _record(row)
        if previous is None or old != new or old_record != new_record:
            rating_changes.append((row, old, old_record, new_record))

    game_changes.sort(key=lambda r: (r.get("sport", ""), r.get("game_date", ""), r.get("school", "")))
    rating_changes.sort(key=lambda item: (item[0].get("sport", ""), item[0].get("school", "")))
    run_time = started_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    game_rows = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(value if value is not None else '—'))}</td>" for value in (
            r.get("sport"), r.get("game_date"), r.get("school"), r.get("opponent"),
            r.get("result"), r.get("score"), r.get("base_pts"), r.get("div_bonus"),
            r.get("opp_quality"), r.get("total_pts")
        )) + "</tr>" for r in game_changes
    ) or '<tr><td colspan="10">No new or corrected completed games.</td></tr>'

    rating_rows = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(value if value is not None else '—'))}</td>" for value in (
            row.get("sport"), row.get("school"), old_record, new_record,
            old, row.get("power_rating")
        )) + "</tr>" for row, old, old_record, new_record in rating_changes
    ) or '<tr><td colspan="6">No power-rating or record changes.</td></tr>'

    css = "table{border-collapse:collapse;width:100%;font-family:Arial,sans-serif}th,td{border:1px solid #ccc;padding:6px;text-align:left}th{background:#008584;color:#fff}h1,h2{font-family:Arial,sans-serif}"
    body = f"""<html><head><style>{css}</style></head><body>
    <h1>LVAY Pipeline Report</h1>
    <p><strong>Run:</strong> {html.escape(run_time)}<br>
    <strong>Sports:</strong> {html.escape(', '.join(active_sports))}<br>
    <strong>Game updates:</strong> {len(game_changes)} &nbsp; <strong>Rating updates:</strong> {len(rating_changes)}</p>
    <h2>New or Corrected Games</h2><table><thead><tr><th>Sport</th><th>Date</th><th>Team</th><th>Opponent</th><th>Result</th><th>Score</th><th>Base</th><th>Bonus</th><th>Opponent Quality</th><th>Total</th></tr></thead><tbody>{game_rows}</tbody></table>
    <h2>Record and Power-Rating Changes</h2><table><thead><tr><th>Sport</th><th>Team</th><th>Old Record</th><th>New Record</th><th>Old Rating</th><th>New Rating</th></tr></thead><tbody>{rating_rows}</tbody></table>
    </body></html>"""
    subject = f"LVAY scrape report — {len(game_changes)} game updates — {run_time}"
    return subject, body, {"game_changes": len(game_changes), "rating_changes": len(rating_changes)}


def _record(row):
    if not row:
        return "—"
    record = f"{int(row.get('wins') or 0)}-{int(row.get('losses') or 0)}"
    if int(row.get("ties") or 0):
        record += f"-{int(row['ties'])}"
    return record


def email_report(subject, body):
    """Send through Resend; skip safely until deployment credentials exist."""
    import requests

    api_key = os.environ.get("RESEND_API_KEY", "").strip()
    enabled = os.environ.get("REPORT_EMAIL_ENABLED", "false").lower() == "true"
    if not enabled or not api_key:
        print("[REPORT] Email skipped: REPORT_EMAIL_ENABLED/RESEND_API_KEY not configured")
        return False
    recipients = [address.strip() for address in REPORT_TO.split(",") if address.strip()]
    response = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"from": REPORT_FROM, "to": recipients, "subject": subject, "html": body},
        timeout=30,
    )
    response.raise_for_status()
    print(f"[REPORT] Email sent to {', '.join(recipients)}")
    return True
