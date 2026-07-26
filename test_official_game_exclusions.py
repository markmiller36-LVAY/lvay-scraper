import unittest

from official_record_overrides import (
    find_game_exclusion,
    get_game_exclusions,
)


class OfficialGameExclusionTests(unittest.TestCase):
    def setUp(self):
        self.exclusions = get_game_exclusions("baseball", "2026")

    def test_confirmed_directional_exclusions_match(self):
        cases = [
            ("Huntington", "Mansfield"),
            ("Mansfield", "Huntington"),
            ("McDonogh #35", "New Orleans Military & Maritime"),
        ]
        for school, opponent in cases:
            with self.subTest(school=school, opponent=opponent):
                row = {
                    "school": school,
                    "opponent": opponent,
                    "game_date": "4/13/2026",
                }
                self.assertIsNotNone(
                    find_game_exclusion(self.exclusions, row)
                )

    def test_nomma_reciprocal_game_remains_counted(self):
        row = {
            "school": "New Orleans Military & Maritime",
            "opponent": "McDonogh #35",
            "game_date": "4/13/2026",
        }
        self.assertIsNone(find_game_exclusion(self.exclusions, row))

    def test_same_matchup_on_another_date_remains_counted(self):
        row = {
            "school": "Huntington",
            "opponent": "Mansfield",
            "game_date": "2/9/2026",
        }
        self.assertIsNone(find_game_exclusion(self.exclusions, row))


if __name__ == "__main__":
    unittest.main()
