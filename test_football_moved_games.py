import sqlite3

import football_forfeits as ff


def _db(rows):
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE games (sport TEXT, season TEXT, school TEXT, opponent TEXT, "
        "game_date TEXT, week TEXT, home_away TEXT, win_loss TEXT, score TEXT)"
    )
    conn.executemany(
        "INSERT INTO games VALUES ('football','2026',?,?,?,?,?,'','-')", rows
    )
    return conn


def _dates(conn, school):
    return sorted(r[0] for r in conn.execute(
        "SELECT game_date FROM games WHERE school=?", (school,)))


def test_patrick_taylor_cohen_moves_to_saturday():
    conn = _db([
        ("Patrick Taylor - Science/Tech.", "Walter L. Cohen", "10/30/2026", "9", "A"),
        ("Walter L. Cohen", "Patrick Taylor - Science/Tech.", "10/30/2026", "9", "H"),
        ("Patrick Taylor - Science/Tech.", "Walter L. Cohen", "9/4/2026", "1", "H"),
    ])
    ff.apply_football_schedule_changes(conn, "2026")
    assert _dates(conn, "Patrick Taylor - Science/Tech.") == ["10/31/2026", "9/4/2026"]
    assert _dates(conn, "Walter L. Cohen") == ["10/31/2026"]
    # Running again changes nothing.
    assert ff._apply_moves(conn, conn.execute(
        "SELECT rowid, school, opponent, game_date, week FROM games").fetchall(),
        ff.FOOTBALL_MOVED_GAMES["2026"]) == 0


def test_old_row_dropped_when_lhsaa_lists_new_date():
    conn = _db([
        ("Walter L. Cohen", "Patrick Taylor - Science/Tech.", "10/30/2026", "9", "H"),
        ("Walter L. Cohen", "Patrick Taylor - Science/Tech.", "10/31/2026", "9", "H"),
    ])
    ff.apply_football_schedule_changes(conn, "2026")
    assert _dates(conn, "Walter L. Cohen") == ["10/31/2026"]
