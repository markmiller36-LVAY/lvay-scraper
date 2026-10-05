import sqlite3
from datetime import date

from football_forfeits import apply_football_forfeits


def _db():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE games (sport, season, school, week, game_date, opponent, score, win_loss)")
    rows = [
        ("Carroll", 5, "10/2/2026", "Wossman", "-", ""),
        ("Wossman", 5, "10/2/2026", "Carroll", "-", ""),
        ("Carroll", 6, "10/9/2026", "Bastrop", "-", ""),
        ("Bastrop", 6, "10/9/2026", "Carroll", "-", ""),
        ("Carroll", 4, "9/25/2026", "Assumption", "6-16", "L"),
    ]
    conn.executemany(
        "INSERT INTO games VALUES ('football', '2026', ?, ?, ?, ?, ?, ?)", rows
    )
    return conn


def _row(conn, school, opponent):
    return conn.execute(
        "SELECT score, win_loss FROM games WHERE school=? AND opponent=?", (school, opponent)
    ).fetchone()


def test_past_forfeits_count_future_ones_only_show():
    conn = _db()
    apply_football_forfeits(conn, "2026", today=date(2026, 10, 5))
    assert _row(conn, "Carroll", "Wossman") == ("Forfeit", "L(f)")
    assert _row(conn, "Wossman", "Carroll") == ("Forfeit", "W(f)")
    # Oct 9 hasn't happened: shown as a forfeit, not counted yet
    assert _row(conn, "Carroll", "Bastrop") == ("Forfeit", "")
    assert _row(conn, "Bastrop", "Carroll") == ("Forfeit", "")
    # earlier played game untouched
    assert _row(conn, "Carroll", "Assumption") == ("6-16", "L")


def test_forfeit_counts_the_day_after_the_game():
    conn = _db()
    apply_football_forfeits(conn, "2026", today=date(2026, 10, 9))
    assert _row(conn, "Bastrop", "Carroll") == ("Forfeit", "")
    apply_football_forfeits(conn, "2026", today=date(2026, 10, 10))
    assert _row(conn, "Bastrop", "Carroll") == ("Forfeit", "W(f)")
    assert _row(conn, "Carroll", "Bastrop") == ("Forfeit", "L(f)")


def test_real_lhsaa_score_is_never_overwritten():
    conn = _db()
    conn.execute("UPDATE games SET score='21-14', win_loss='W' WHERE school='Wossman'")
    apply_football_forfeits(conn, "2026", today=date(2026, 10, 5))
    assert _row(conn, "Wossman", "Carroll") == ("21-14", "W")
