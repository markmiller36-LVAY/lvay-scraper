import json
import unittest
from pathlib import Path

from school_database import get_school


ALIGNMENT_PATH = Path(__file__).with_name(
    "sport_alignments_2025_2026.json"
)


class VersionedSportAlignmentTests(unittest.TestCase):
    def test_official_alignment_dataset_covers_all_eight_sports(self):
        data = json.loads(ALIGNMENT_PATH.read_text(encoding="utf-8"))

        self.assertEqual(data["cycle"], "2024-2026")
        self.assertEqual(
            set(data["sports"]),
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
        )
        self.assertTrue(
            all(len(rows) >= 180 for rows in data["sports"].values())
        )

    def test_harrisonburg_uses_late_2025_26_class_b_reports(self):
        for sport in ("girls_basketball", "baseball", "softball"):
            info = get_school("Harrisonburg", sport=sport, season=2026)
            self.assertEqual(info["class"], "B")
            self.assertEqual(info["district"], 5)
            self.assertEqual(info["division"], "Class B")

    def test_baseball_helix_uses_sport_specific_district(self):
        info = get_school(
            "Helix Mentorship Academy", sport="baseball", season=2026
        )
        self.assertEqual(info["class"], "3A")
        self.assertEqual(info["district"], 3)

    def test_softball_booker_t_washington_uses_sport_specific_district(self):
        info = get_school(
            "Booker T. Washington - N.O.",
            sport="softball",
            season=2026,
        )
        self.assertEqual(info["class"], "3A")
        self.assertEqual(info["district"], 9)

    def test_soccer_alignment_is_independent_from_basic_classification(self):
        info = get_school(
            "Evangel Christian", sport="boys_soccer", season=2026
        )
        self.assertEqual(info["division"], "Division IV")
        self.assertEqual(info["district"], 1)

    def test_archived_alignment_does_not_leak_into_next_cycle(self):
        archived = get_school(
            "Helix Mentorship Academy", sport="baseball", season=2026
        )
        next_cycle = get_school(
            "Helix Mentorship Academy", sport="baseball", season=2027
        )
        self.assertEqual(archived["district"], 3)
        self.assertNotEqual(next_cycle["district"], archived["district"])


if __name__ == "__main__":
    unittest.main()
