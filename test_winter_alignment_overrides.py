import unittest

from school_database import WINTER_ALIGNMENT_OVERRIDES, get_school


class WinterAlignmentOverrideTests(unittest.TestCase):
    def test_official_basketball_reports_have_full_field(self):
        self.assertGreaterEqual(
            len(WINTER_ALIGNMENT_OVERRIDES["boys_basketball"]), 375
        )
        self.assertGreaterEqual(
            len(WINTER_ALIGNMENT_OVERRIDES["girls_basketball"]), 365
        )
        self.assertGreaterEqual(
            len(WINTER_ALIGNMENT_OVERRIDES["boys_soccer"]), 175
        )
        self.assertGreaterEqual(
            len(WINTER_ALIGNMENT_OVERRIDES["girls_soccer"]), 175
        )

    def test_girls_basketball_uses_official_2026_postseason_divisions(self):
        self.assertEqual(
            get_school("Sterlington", "girls_basketball", 2026)["division"],
            "Non-Select Division II",
        )
        self.assertEqual(
            get_school(
                "French Settlement", "girls_basketball", 2026
            )["division"],
            "Non-Select Division III",
        )

    def test_2026_overrides_do_not_leak_into_2027(self):
        self.assertEqual(
            get_school("Sterlington", "girls_basketball", 2027)["division"],
            "Non-Select Division III",
        )

    def test_soccer_uses_official_2026_division_and_district(self):
        mt_carmel = get_school("Mt. Carmel", "girls_soccer", 2026)
        self.assertEqual(mt_carmel["division"], "Division I")
        self.assertEqual(mt_carmel["district"], 8)


if __name__ == "__main__":
    unittest.main()
