import sqlite3
import unittest

import run_power_rankings as rpr


def make_db(rows):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE games (
            sport TEXT, season TEXT, school TEXT, opponent TEXT,
            win_loss TEXT, week TEXT, score TEXT, game_date TEXT,
            class_ TEXT, district TEXT, district_class TEXT,
            out_of_state TEXT, home_away TEXT, opponent_class TEXT
        )
    """)
    for school, opponent, win_loss, score in rows:
        conn.execute(
            "INSERT INTO games (sport, season, school, opponent, win_loss, "
            "week, score, game_date, home_away) VALUES "
            "('football', '2026', ?, ?, ?, 'Week 6', ?, "
            "'10/9/2026 7:00:00 PM', 'H')",
            (school, opponent, win_loss, score),
        )
    return conn


FORM_OVERRIDE = {
    ("football", "2026", "airline", "10/9/2026 7:00:00 PM", "benton"): {
        "override_win_loss": "W",
        "override_score": "21-14",
        "override_home_away": "H",
        "notes": "[LVAY form] Week 6 entry",
    }
}


class FormScoreOverrideTests(unittest.TestCase):
    def test_default_load_still_skips_unscored_games(self):
        conn = make_db([("Airline", "Benton", "", "-")])
        self.assertEqual(rpr.load_games(conn, "2026", "football"), [])

    def test_form_score_fills_a_game_lhsaa_has_not_posted(self):
        conn = make_db([("Airline", "Benton", "", "-")])
        raw = rpr.load_games(conn, "2026", "football", include_unscored=True)
        row = rpr.apply_override_to_row(raw[0], "football", "2026", FORM_OVERRIDE)
        self.assertEqual(row["win_loss"], "W")
        self.assertEqual(row["score"], "21-14")

    def test_lhsaa_result_wins_once_posted(self):
        conn = make_db([("Airline", "Benton", "L", "14-21")])
        raw = rpr.load_games(conn, "2026", "football", include_unscored=True)
        row = rpr.apply_override_to_row(raw[0], "football", "2026", FORM_OVERRIDE)
        self.assertEqual(row["win_loss"], "L")
        self.assertEqual(row["score"], "14-21")

    def test_manual_corrections_still_beat_lhsaa(self):
        conn = make_db([("Airline", "Benton", "L", "21-14")])
        raw = rpr.load_games(conn, "2026", "football", include_unscored=True)
        correction = {
            key: dict(value, notes="Verified correction")
            for key, value in FORM_OVERRIDE.items()
        }
        row = rpr.apply_override_to_row(raw[0], "football", "2026", correction)
        self.assertEqual(row["win_loss"], "W")


if __name__ == "__main__":
    unittest.main()
