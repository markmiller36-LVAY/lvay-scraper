import sqlite3

from football_forfeits import CANCELLED_RESULT, apply_football_forfeits
from run_power_rankings import COUNTED_RESULTS


def _db():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE games (sport, season, school, week, game_date, opponent, score, win_loss)")
    conn.executemany(
        "INSERT INTO games VALUES ('football', '2026', ?, ?, ?, ?, ?, ?)",
        [
            ("Booker T. Washington - N.O.", 6, "10/9/2026", "Bogalusa", "-", ""),
            ("Bogalusa", 6, "10/9/2026", "Booker T. Washington - N.O.", "-", ""),
            ("Booker T. Washington - N.O.", 5, "10/1/2026", "George Washington Carver", "24-18", "W"),
        ],
    )
    return conn


def _row(conn, school, opponent):
    return conn.execute(
        "SELECT score, win_loss FROM games WHERE school=? AND opponent=?", (school, opponent)
    ).fetchone()


def test_cancelled_game_stays_on_both_schedules_and_never_counts():
    conn = _db()
    apply_football_forfeits(conn, "2026")
    assert _row(conn, "Booker T. Washington - N.O.", "Bogalusa") == ("", "Cancelled")
    assert _row(conn, "Bogalusa", "Booker T. Washington - N.O.") == ("", "Cancelled")
    assert _row(conn, "Booker T. Washington - N.O.", "George Washington Carver") == ("24-18", "W")
    assert CANCELLED_RESULT not in COUNTED_RESULTS


def test_cancellation_survives_a_rescrape():
    conn = _db()
    apply_football_forfeits(conn, "2026")
    # Next LHSAA merge rewrites the row as unplayed; the next pass re-marks it.
    conn.execute("UPDATE games SET score='-', win_loss='' WHERE school='Bogalusa'")
    assert apply_football_forfeits(conn, "2026") >= 1
    assert _row(conn, "Bogalusa", "Booker T. Washington - N.O.") == ("", "Cancelled")
