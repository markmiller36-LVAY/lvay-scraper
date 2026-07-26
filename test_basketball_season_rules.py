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

    def higher_division_game(self):
        return GameResult(
            team="Test School",
            opponent="Opponent",
            result="W",
            sport="boys_basketball",
            opponent_wins=5,
            opponent_losses=5,
            opponent_class="5A",
            opponent_division="Non-Select Division I",
        )

    def test_2025_26_uses_archived_34_and_44_multipliers(self):
        self.assertEqual(self.rate("3A", 2026, [self.game()]).power_rating, 42)
        self.assertEqual(self.rate("B", 2026, [self.game()]).power_rating, 52)

    def test_2026_27_uses_current_30_and_40_multipliers(self):
        self.assertEqual(self.rate("3A", 2027, [self.game()]).power_rating, 40)
        self.assertEqual(self.rate("B", 2027, [self.game()]).power_rating, 50)

    def test_out_of_state_contests_do_not_count(self):
        rating = self.rate("3A", 2026, [self.game(), self.game(oos=True)])
        self.assertEqual(rating.games_played, 1)
        self.assertEqual(rating.power_rating, 42)

    def test_in_state_bonus_uses_paired_class_and_division_steps(self):
        # Test School is in Division III. A 5A Division I opponent is two
        # class and playoff-division steps higher, so the bonus is 4.
        rating = self.rate("3A", 2026, [self.higher_division_game()])
        self.assertEqual(rating.power_rating, 46)

        # A 4A Division I opponent is only one class step higher. Even though
        # it is two divisions higher, only one step satisfies both criteria.
        game = self.higher_division_game()
        game.opponent_class = "4A"
        rating = self.rate("3A", 2026, [game])
        self.assertEqual(rating.power_rating, 44)


if __name__ == "__main__":
    unittest.main()
