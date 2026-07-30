import json
import unittest
from pathlib import Path

from power_rating_engine import DIVISION_RANK
from school_database import get_school


ALIGNMENT_PATH = Path(__file__).with_name(
    "sport_alignments_2026_2027.json"
)


class VersionedSportAlignment20262027Tests(unittest.TestCase):
    def test_dataset_covers_all_eight_sports(self):
        data = json.loads(ALIGNMENT_PATH.read_text(encoding="utf-8"))

        self.assertEqual("2026-2028", data["cycle"])
        self.assertEqual(
            {
                "football",
                "volleyball",
                "boys_basketball",
                "girls_basketball",
                "boys_soccer",
                "girls_soccer",
                "baseball",
                "softball",
            },
            set(data["sports"]),
        )
        self.assertTrue(
            all(len(rows) >= 180 for rows in data["sports"].values())
        )

    def test_dataset_has_no_blank_assignments_or_legend_markers(self):
        data = json.loads(ALIGNMENT_PATH.read_text(encoding="utf-8"))

        for sport, schools in data["sports"].items():
            for school, info in schools.items():
                with self.subTest(sport=sport, school=school):
                    self.assertFalse(school.endswith(("+", "*")))
                    self.assertIsNotNone(info.get("class"))
                    self.assertIsNotNone(info.get("district"))
                    self.assertIsNotNone(info.get("division"))

    def test_new_cycle_football_is_season_scoped(self):
        archived = get_school(
            "Evangel Christian", sport="football", season=2025
        )
        upcoming = get_school(
            "Evangel Christian", sport="football", season=2026
        )

        self.assertEqual("Select Division I", archived["division"])
        self.assertEqual("Division I", upcoming["division"])
        self.assertEqual("5A", upcoming["class"])
        self.assertEqual(1, upcoming["district"])
        self.assertEqual("combined", upcoming["track"])

    def test_new_cycle_soccer_uses_sport_specific_classification(self):
        upcoming = get_school(
            "Evangel Christian", sport="boys_soccer", season=2027
        )

        self.assertEqual("2A", upcoming["class"])
        self.assertEqual(1, upcoming["district"])
        self.assertEqual("Division IV", upcoming["division"])
        self.assertEqual("combined", upcoming["track"])

    def test_new_cycle_class_b_assignments_remain_small_school(self):
        for sport in ("boys_basketball", "baseball", "softball"):
            with self.subTest(sport=sport):
                upcoming = get_school(
                    "Harrisonburg", sport=sport, season=2027
                )
                self.assertEqual("B", upcoming["class"])
                self.assertEqual(4, upcoming["district"])
                self.assertEqual("Class B", upcoming["division"])
                self.assertEqual("small-school", upcoming["track"])

    def test_nonstandard_status_is_preserved_without_name_marker(self):
        info = get_school(
            "Word of God Academy", sport="volleyball", season=2026
        )

        self.assertEqual("non_district_honors", info["alignment_status"])
        self.assertEqual("Division V", info["division"])
        self.assertEqual("combined", info["track"])

    def test_division_v_has_lowest_generic_division_rank(self):
        self.assertEqual(0, DIVISION_RANK["Division V"])
        self.assertLess(
            DIVISION_RANK["Division V"],
            DIVISION_RANK["Division IV"],
        )

    def test_new_cycle_does_not_leak_into_unconfigured_future_season(self):
        upcoming = get_school(
            "Evangel Christian", sport="football", season=2026
        )
        future = get_school(
            "Evangel Christian", sport="football", season=2027
        )

        self.assertEqual("Division I", upcoming["division"])
        self.assertNotEqual(upcoming["division"], future["division"])


if __name__ == "__main__":
    unittest.main()
