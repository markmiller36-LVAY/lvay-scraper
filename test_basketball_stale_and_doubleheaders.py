import sqlite3

import scraper
from server import merge_unplayed_schedule_games


def g(date, opp, res="W", score="1-0"):
    return {"game_date": date, "opponent": opp, "result": res, "score": score}


def test_second_played_game_same_day_is_shown():
    rated = [g("11/23/2026", "East St. John")]
    scraped = [g("11/23/2026 3:00:00 PM", "East St. John"), g("11/23/2026 12:00:00 PM", "Blount - AL - AHSAA", "L", "42-59")]
    merged = merge_unplayed_schedule_games(rated, scraped)
    assert [x["opponent"] for x in merged].count("Blount - AL - AHSAA") == 1
    assert len(merged) == 2


def test_spelling_variant_same_day_still_deduped():
    rated = [g("11/23/2026", "St. Thomas More")]
    scraped = [g("11/23/2026 6:00:00 PM", "St Thomas More")]
    assert len(merge_unplayed_schedule_games(rated, scraped)) == 1


def test_remove_stale_keeps_lvay_rows(tmp_path, monkeypatch):
    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE season_registry (sport TEXT, season TEXT, is_locked INTEGER)")
    conn.execute("CREATE TABLE games (sport TEXT, season TEXT, school TEXT, game_date TEXT, opponent TEXT, source TEXT)")
    rows = [("Rayville", "11/23/2026 12:30:00 PM", "Ouachita Parish", None),
            ("Rayville", "11/23/2026 12:00:00 PM", "Bastrop", None),
            ("Rayville", "11/24/2026 12:00:00 PM", "Winnfield", "LVAY tournament")]
    conn.executemany("INSERT INTO games VALUES ('girls_basketball','2027',?,?,?,?)", rows)
    conn.commit(); conn.close()
    monkeypatch.setattr(scraper, "DB_PATH", str(db))
    seen = {("Rayville", "11/23/2026 12:00:00 PM", "Bastrop")}
    assert scraper.remove_stale_games("girls_basketball", "2027", seen) == 1
    left = sqlite3.connect(db).execute("SELECT opponent FROM games ORDER BY opponent").fetchall()
    assert left == [("Bastrop",), ("Winnfield",)]
