import unittest

from scraper import apply_verified_football_result, _football_result_from_score


class FootballResultReconciliationTests(unittest.TestCase):
    def test_result_is_inferred_when_source_has_score_only(self):
        self.assertEqual(_football_result_from_score("51-8"), "W")
        self.assertEqual(_football_result_from_score("8-51"), "L")
        self.assertEqual(_football_result_from_score("20-20"), "T")

    def test_verified_final_fills_missing_lhsaa_fields(self):
        game = {"score": "-", "win_loss": ""}
        corrected = apply_verified_football_result(
            game, "Bossier", "North Caddo", "2026"
        )
        self.assertEqual(corrected["score"], "42-7")
        self.assertEqual(corrected["win_loss"], "W")

    def test_verified_final_repairs_malformed_lhsaa_score(self):
        game = {"score": "-20", "win_loss": ""}
        corrected = apply_verified_football_result(
            game, "Jesuit", "Madison Prep", "2026"
        )
        self.assertEqual(corrected["score"], "28-20")
        self.assertEqual(corrected["win_loss"], "W")

    def test_existing_lhsaa_result_is_not_replaced(self):
        game = {"score": "21-7", "win_loss": "W"}
        corrected = apply_verified_football_result(
            game, "Bossier", "North Caddo", "2026"
        )
        self.assertEqual(corrected, game)

    def test_verified_finals_are_season_scoped(self):
        game = {"score": "-", "win_loss": ""}
        corrected = apply_verified_football_result(
            game, "Bossier", "North Caddo", "2025"
        )
        self.assertEqual(corrected["score"], "-")
        self.assertEqual(corrected["win_loss"], "")


if __name__ == "__main__":
    unittest.main()
