import unittest

from power_rating_engine import (
    GameResult,
    PowerRatingEngine,
    Team,
)


class BasketballSeasonRuleTests(unittest.TestCase):
    def rate(self, class_, season, games):
        engine = PowerRatingEngine()
        engine.add_team(
            Team(
                name="Test School",
                division="Non-Select Division III",
                classification=class_,
                sport="boys_basketball",
                season=season,
            )
        )
        for game in games:
            engine.add_game(game)
        return engine.rate_team("Test School")

    def game(self, *, oos=False):
        return GameResult(
            team="Test School",
            opponent="Opponent",
            result="W",
            sport="boys_basketball",
            opponent_wins=5,
            opponent_losses=5,
            opponent_out_of_state=oos,
        )

    def test_2025_26_uses_official_34_and_40_multipliers(self):
        self.assertEqual(self.rate("3A", 2026, [self.game()]).power_rating, 42)
        self.assertEqual(self.rate("B", 2026, [self.game()]).power_rating, 50)

    def test_2026_27_uses_current_30_and_40_multipliers(self):
        self.assertEqual(self.rate("3A", 2027, [self.game()]).power_rating, 40)
        self.assertEqual(self.rate("B", 2027, [self.game()]).power_rating, 50)

    def test_out_of_state_contests_do_not_count(self):
        rating = self.rate("3A", 2026, [self.game(), self.game(oos=True)])
        self.assertEqual(rating.games_played, 1)
        self.assertEqual(rating.power_rating, 42)


if __name__ == "__main__":
    unittest.main()
