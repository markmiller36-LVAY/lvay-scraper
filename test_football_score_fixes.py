import sqlite3

import football_forfeits as ff


def _db(rows):
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE games (sport TEXT, season TEXT, school TEXT, opponent TEXT, "
        "game_date TEXT, week TEXT, home_away TEXT, win_loss TEXT, score TEXT)"
    )
    conn.executemany("INSERT INTO games VALUES ('football','2026',?,?,?,'10',?,?,?)", rows)
    return conn


def _row(conn, school):
    return conn.execute("SELECT score, win_loss FROM games WHERE school=?", (school,)).fetchone()


def test_notre_dame_welsh_score_put_right():
    conn = _db([
        ("Notre Dame", "Welsh", "11/5/2026", "A", "W", "28-35"),
        ("Welsh", "Notre Dame", "11/5/2026", "H", "L", "35-28"),
    ])
    assert ff.apply_football_score_fixes(conn, "2026") == 2
    assert _row(conn, "Notre Dame") == ("35-28", "W")
    assert _row(conn, "Welsh") == ("28-35", "L")
    assert ff.apply_football_score_fixes(conn, "2026") == 0


def test_different_lhsaa_score_left_alone():
    conn = _db([("Notre Dame", "Welsh", "11/5/2026", "A", "W", "35-21")])
    assert ff.apply_football_score_fixes(conn, "2026") == 0
    assert _row(conn, "Notre Dame") == ("35-21", "W")
