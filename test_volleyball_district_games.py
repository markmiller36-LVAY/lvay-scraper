import sqlite3

from scraper_volleyball import ensure_tables, insert_games


def test_district_matches_count_for_volleyball_power_rating(monkeypatch):
    monkeypatch.setattr("scraper_volleyball.SEASON", "2025")
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_tables(conn)

    insert_games(
        conn,
        [
            {
                "school": "Example High",
                "school_dd": "1-I",
                "date_raw": "10/01/2025",
                "opponent": "District Rival",
                "opp_dd": "1-I",
                "dist_t": "D",
                "tournament": "",
                "match_num": "1",
                "home_away": "H",
                "win_loss": "W",
                "score": "3-0",
                "division": "I",
            }
        ],
    )

    game = conn.execute(
        "SELECT is_district, counts_for_pr FROM volleyball_games"
    ).fetchone()
    assert dict(game) == {"is_district": 1, "counts_for_pr": 1}

    # Re-scraping an existing match must update it instead of failing on
    # a nonexistent volleyball_games.updated_at column.
    row = {
        "school": "Example High",
        "school_dd": "1-I",
        "date_raw": "10/01/2025",
        "opponent": "District Rival",
        "opp_dd": "1-I",
        "dist_t": "D",
        "tournament": "",
        "match_num": "1",
        "home_away": "H",
        "win_loss": "L",
        "score": "2-3",
        "division": "I",
    }
    inserted, updated, skipped = insert_games(conn, [row])
    assert (inserted, updated, skipped) == (0, 1, 0)
    refreshed = conn.execute(
        "SELECT result, score, counts_for_pr FROM volleyball_games"
    ).fetchone()
    assert dict(refreshed) == {
        "result": "L",
        "score": "2-3",
        "counts_for_pr": 1,
    }
