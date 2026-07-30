import unittest

from server import parse_schedule_date, sort_schedule_games


class ScheduleDateOrderingTests(unittest.TestCase):
    def test_parses_volleyball_weekday_suffix(self):
        parsed = parse_schedule_date("10/2/2025Thu")
        self.assertEqual(parsed.strftime("%Y-%m-%d"), "2025-10-02")

    def test_orders_mixed_months_chronologically(self):
        games = [
            {"game_date": "10/13/2025Mon", "opponent": "Iowa"},
            {"game_date": "10/2/2025Thu", "opponent": "St. Edmund"},
            {"game_date": "9/2/2025Tue", "opponent": "Sulphur"},
            {"game_date": "9/30/2025Tue", "opponent": "Iota"},
        ]

        ordered = sort_schedule_games(games)

        self.assertEqual(
            [game["opponent"] for game in ordered],
            ["Sulphur", "Iota", "St. Edmund", "Iowa"],
        )

    def test_orders_same_day_volleyball_matches_by_match_number(self):
        games = [
            {"game_date": "9/13/2025Sat", "match_num": 3},
            {"game_date": "9/13/2025Sat", "match_num": 1},
            {"game_date": "9/13/2025Sat", "match_num": 2},
        ]

        ordered = sort_schedule_games(games)

        self.assertEqual(
            [game["match_num"] for game in ordered],
            [1, 2, 3],
        )

    def test_keeps_unknown_dates_stable_at_bottom(self):
        games = [
            {"game_date": "", "opponent": "TBD 1"},
            {"game_date": "2025-09-01", "opponent": "Known"},
            {"game_date": "TBD", "opponent": "TBD 2"},
        ]

        ordered = sort_schedule_games(games)

        self.assertEqual(
            [game["opponent"] for game in ordered],
            ["Known", "TBD 1", "TBD 2"],
        )


if __name__ == "__main__":
    unittest.main()
