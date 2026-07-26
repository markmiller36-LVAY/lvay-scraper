import sqlite3

import scraper_volleyball


def test_2025_reed_livingston_source_duplicate_is_removed(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    scraper_volleyball.ensure_tables(conn)

    rows = [
        {
            "school": "John F. Kennedy",
            "school_dd": "8-III",
            "date_raw": "9/30/2025 Tue",
            "opponent": "Livingston Collegiate",
            "opp_dd": "8-III",
            "dist_t": "D",
            "tournament": "",
            "match_num": "1",
            "home_away": "A",
            "win_loss": "L",
            "score": "14-25, 28-25, 21-25, 25-17, 9-15",
            "division": "III",
        },
        {
            "school": "John F. Kennedy",
            "school_dd": "8-III",
            "date_raw": "9/30/2025 Tue",
            "opponent": "Sarah T. Reed",
            "opp_dd": "8-III",
            "dist_t": "D",
            "tournament": "",
            "match_num": "1",
            "home_away": "A",
            "win_loss": "L",
            "score": "14-25, 28-25, 21-25, 25-17, 9-15",
            "division": "III",
        },
        {
            "school": "Sarah T. Reed",
            "school_dd": "8-III",
            "date_raw": "9/30/2025 Tue",
            "opponent": "John F. Kennedy",
            "opp_dd": "8-III",
            "dist_t": "D",
            "tournament": "",
            "match_num": "1",
            "home_away": "H",
            "win_loss": "W",
            "score": "25-14, 25-28, 25-21, 17-25, 15-9",
            "division": "III",
        },
        {
            "school": "Sarah T. Reed",
            "school_dd": "10-IV",
            "date_raw": "10/14/2025 Tue",
            "opponent": "Walter L. Cohen",
            "opp_dd": "10-IV",
            "dist_t": "",
            "tournament": "",
            "match_num": "1",
            "home_away": "H",
            "win_loss": "W",
            "score": "25-10, 25-20, 25-15",
            "division": "IV",
        },
    ]

    scraper_volleyball.insert_games(conn, rows, "2025")

    games = conn.execute("""
        SELECT school, opponent, school_division, opp_division
        FROM volleyball_games
        ORDER BY school, opponent
    """).fetchall()
    assert [tuple(game) for game in games] == [
        ("John F. Kennedy", "Livingston Collegiate", "III", "III"),
        ("Sarah T. Reed", "Walter L. Cohen", "IV", "IV"),
    ]
