import unittest

from basketball_first_contest import is_before_first_contest, normalize_class


class FirstContestTests(unittest.TestCase):
    def test_normalize(self):
        self.assertEqual(normalize_class("7-5A"), "5A")
        self.assertEqual(normalize_class("Class B"), "B")
        self.assertEqual(normalize_class("Non-Select Division I"), "")

    def test_5a_jamboree_dropped(self):
        self.assertTrue(is_before_first_contest("boys_basketball", "2027", "10/31/2026", "1-5A", "4A"))
        self.assertTrue(is_before_first_contest("girls_basketball", "2027", "11/21/2026Sat", "3A", ""))

    def test_5a_regular_season_kept(self):
        self.assertFalse(is_before_first_contest("boys_basketball", "2027", "11/23/2026", "5A", "4A"))

    def test_b_c_games_count_from_nov_9(self):
        self.assertFalse(is_before_first_contest("boys_basketball", "2027", "11/10/2026", "B", "C"))
        self.assertTrue(is_before_first_contest("boys_basketball", "2027", "11/6/2026", "B", "C"))

    def test_b_vs_1a_before_nov_23_dropped_for_both(self):
        self.assertTrue(is_before_first_contest("boys_basketball", "2027", "11/10/2026", "C", "1A"))

    def test_other_sports_and_seasons_untouched(self):
        self.assertFalse(is_before_first_contest("boys_soccer", "2027", "10/31/2026", "5A", ""))
        self.assertFalse(is_before_first_contest("boys_basketball", "2026", "10/31/2025", "5A", ""))


if __name__ == "__main__":
    unittest.main()
