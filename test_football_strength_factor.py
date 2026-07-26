import unittest

from run_power_rankings import football_strength_factor


class FootballStrengthFactorTests(unittest.TestCase):
    def test_ruston_2025_handbook_checkpoint(self):
        opponent_wins = [7, 4, 6, 5, 10, 6, 7, 8, 4, 7]
        breakdown = [{"week": week} for week in range(1, 11)]
        game_meta = {
            ("Ruston", week): {"opp_class": "5A", "opp_wins": wins}
            for week, wins in enumerate(opponent_wins, start=1)
        }

        self.assertEqual(
            football_strength_factor("Ruston", breakdown, game_meta),
            11.4,
        )

    def test_adds_opponent_classification_and_wins(self):
        breakdown = [
            {"week": 1},
            {"week": 2},
        ]
        game_meta = {
            ("Ruston", 1): {"opp_class": "5A", "opp_wins": 7},
            ("Ruston", 2): {"opp_class": "4A", "opp_wins": 6},
        }

        self.assertEqual(
            football_strength_factor("Ruston", breakdown, game_meta),
            11.0,
        )

    def test_divides_by_counted_games_and_handles_prefixed_class(self):
        breakdown = [
            {"week": 1},
            {"week": 2},
            {"week": 3},
        ]
        game_meta = {
            ("Team", 1): {"opp_class": "1-5A", "opp_wins": 8},
            ("Team", 2): {"opp_class": "4A", "opp_wins": 4},
            ("Team", 3): {"opp_class": "3A", "opp_wins": 3},
        }

        self.assertEqual(
            football_strength_factor("Team", breakdown, game_meta),
            9.0,
        )

    def test_unknown_class_contributes_zero_but_wins_still_count(self):
        breakdown = [{"week": 1}]
        game_meta = {
            ("Team", 1): {"opp_class": "", "opp_wins": 6},
        }

        self.assertEqual(
            football_strength_factor("Team", breakdown, game_meta),
            6.0,
        )


if __name__ == "__main__":
    unittest.main()
