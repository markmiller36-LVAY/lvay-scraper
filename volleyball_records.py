"""Keep volleyball schedule records separate from power-rating eligibility."""

import re


def is_out_of_state(opponent, division):
    name = str(opponent or "").strip().upper()
    division = str(division or "").strip().upper()
    # Named nonmember schools use 0- in the LHSAA volleyball report.
    # Missing metadata alone is not evidence that a Louisiana school is OOS.
    return name == "OUT OF STATE" or division == "0" or division.startswith("0-") or bool(
        re.search(r"\s-\s(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\s-\s", name)
    )


def repair_oos_eligibility(conn, season):
    """Exclude stored OOS rows without deleting schedules or lifting exclusions."""
    rows = conn.execute(
        "SELECT rowid, opponent, opp_division FROM volleyball_games "
        "WHERE sport='volleyball' AND season=? AND counts_for_pr=1", (str(season),)
    ).fetchall()
    conn.executemany(
        "UPDATE volleyball_games SET counts_for_pr=0 WHERE rowid=?",
        [(row[0],) for row in rows if is_out_of_state(row[1], row[2])],
    )


def schedule_record(games):
    """Count completed schedule results, including matches excluded from PR."""
    wins = sum(str(g.get("result") or "").strip().upper() == "W" for g in games)
    losses = sum(str(g.get("result") or "").strip().upper() == "L" for g in games)
    return {"wins": wins, "losses": losses, "games_played": wins + losses,
            "record": f"{wins}-{losses}", "record_label": "Overall Record"}
