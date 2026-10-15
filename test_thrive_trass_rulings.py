import sqlite3
from datetime import date

from football_forfeits import apply_football_forfeits
from football_playoff_eligibility import ineligible_schools


def _db():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE games (sport, season, school, week, game_date, opponent, score, win_loss)")
    conn.executemany("INSERT INTO games VALUES ('football','2026',?,?,?,?,?,?)", [
        ("Thrive Academy", 6, "10/9/2026", "Capitol", "24-26", "L"),
        ("Thrive Academy", 7, "10/15/2026", "St. John", "-", ""),
        ("St. John", 7, "10/15/2026", "Thrive Academy", "-", ""),
        ("Southern Lab", 8, "10/22/2026", "Thrive Academy", "-", ""),
        ("General Trass", 6, "10/9/2026", "Dumas High School, AR", "-", "Cancelled"),
    ])
    return conn


def _row(conn, school, opponent):
    return conn.execute("SELECT score, win_loss FROM games WHERE school=? AND opponent=?",
                        (school, opponent)).fetchone()


def test_thrive_forfeits_count_once_each_date_passes():
    conn = _db()
    apply_football_forfeits(conn, "2026", today=date(2026, 10, 16))
    assert _row(conn, "Thrive Academy", "St. John") == ("Forfeit", "L(f)")
    assert _row(conn, "St. John", "Thrive Academy") == ("Forfeit", "W(f)")
    assert _row(conn, "Southern Lab", "Thrive Academy") == ("Forfeit", "")  # not played yet
    assert _row(conn, "Thrive Academy", "Capitol") == ("24-26", "L")       # played game kept


def test_general_trass_dumas_is_cancelled():
    conn = _db()
    apply_football_forfeits(conn, "2026", today=date(2026, 10, 16))
    assert _row(conn, "General Trass", "Dumas High School, AR") == ("", "Cancelled")


def test_thrive_not_playing_in_playoffs_even_if_lhsaa_lists_it():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE playoff_eligibility (sport, season, school, track, division, eligible, source, updated_at)")
    conn.execute("INSERT INTO playoff_eligibility VALUES ('football','2026','Thrive Academy','select','Select Division IV',1,'x','y')")
    conn.execute("INSERT INTO playoff_eligibility VALUES ('football','2026','Capitol','select','Select Division IV',1,'x','y')")
    out = ineligible_schools(conn, "2026")
    assert "Thrive Academy" in out
    assert "Capitol" in out


def test_capitol_forfeits_rest_of_season():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE games (sport, season, school, week, game_date, opponent, score, win_loss)")
    conn.executemany("INSERT INTO games VALUES ('football','2026',?,?,?,?,?,?)", [
        ("Capitol", 6, "10/9/2026", "Thrive Academy", "26-24", "W"),
        ("Capitol", 8, "10/23/2026", "St. John", "-", ""),
        ("St. John", 8, "10/23/2026", "Capitol", "-", ""),
        ("White Castle", 9, "10/30/2026", "Capitol", "-", ""),
    ])
    apply_football_forfeits(conn, "2026", today=date(2026, 10, 24))
    row = lambda s, o: conn.execute("SELECT score, win_loss FROM games WHERE school=? AND opponent=?", (s, o)).fetchone()
    assert row("Capitol", "St. John") == ("Forfeit", "L(f)")
    assert row("St. John", "Capitol") == ("Forfeit", "W(f)")
    assert row("White Castle", "Capitol") == ("Forfeit", "")
    assert row("Capitol", "Thrive Academy") == ("26-24", "W")


def test_east_iberville_plays_highland_baptist_instead_of_capitol_forfeit():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE games (sport, season, school, week, game_date, opponent, home_away, score, win_loss)")
    conn.executemany("INSERT INTO games VALUES ('football','2026',?,?,?,?,?,?,?)", [
        ("East Iberville", "7", "10/16/2026", "Capitol", "H", "1-0", "W(f)"),
        ("Capitol", "7", "10/16/2026", "East Iberville", "A", "0-1", "L(f)"),
        ("Highland Baptist", "7", "", "False River Academy", "H", "", ""),
        ("Capitol", "8", "10/23/2026", "St. John", "A", "0-1", "L(f)"),
    ])
    apply_football_forfeits(conn, "2026", today=date(2026, 10, 15))
    rows = conn.execute("SELECT school, opponent, game_date, home_away, score, win_loss FROM games "
                        "WHERE week='7' ORDER BY school").fetchall()
    assert rows == [
        ("East Iberville", "Highland Baptist", "10/16/2026", "H", "-", ""),
        ("Highland Baptist", "East Iberville", "10/16/2026", "A", "-", ""),
    ]
    # Capitol's other forfeits are untouched
    assert conn.execute("SELECT win_loss FROM games WHERE school='Capitol' AND week='8'").fetchone() == ("L(f)",)
    # Running again (after the next LHSAA scrape re-posts the forfeit) gives the same result
    conn.execute("INSERT INTO games VALUES ('football','2026','Capitol','7','10/16/2026','East Iberville','A','0-1','L(f)')")
    conn.execute("UPDATE games SET opponent='Capitol', score='1-0', win_loss='W(f)' WHERE school='East Iberville'")
    apply_football_forfeits(conn, "2026", today=date(2026, 10, 15))
    assert conn.execute("SELECT school, opponent, win_loss FROM games WHERE week='7' ORDER BY school").fetchall() == [
        ("East Iberville", "Highland Baptist", ""), ("Highland Baptist", "East Iberville", ""),
    ]
