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
    # LHSAA source checked 2026-09-23: Mt. Carmel has exactly one Clearwater
    # match on Sep 5, numbered 3. The old match-1 copy persisted after renumbering.
    # Remove only that proven stale copy when the identical canonical row exists.
    conn.execute("""
        DELETE FROM volleyball_games AS stale
        WHERE stale.sport='volleyball' AND stale.season='2026' AND stale.season=?
          AND stale.school='Mt. Carmel'
          AND stale.game_date IN ('9/5/2026', '2026-09-05')
          AND stale.opponent='Clearwater Central Catholic - FL - FHSAA'
          AND stale.match_num=1 AND stale.result='W'
          AND stale.score='25-17, 25-15'
          AND EXISTS (
              SELECT 1 FROM volleyball_games AS current
              WHERE current.sport=stale.sport AND current.season=stale.season
                AND current.school=stale.school AND current.game_date=stale.game_date
                AND current.opponent=stale.opponent AND current.match_num=3
                AND current.result=stale.result AND current.score=stale.score
          )
    """, (str(season),))
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
