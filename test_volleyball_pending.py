import unittest
from datetime import date

from volleyball_pending import PENDING_TOURNAMENTS, merge_pending_games, pending_by_school
from school_database import get_school


class PendingTournamentTests(unittest.TestCase):
    def setUp(self):
        self.pending = pending_by_school(today=date(2026, 10, 28))
        self.info = {"Calvary Baptist": {"division": "V", "district": "2"},
                     "St. Scholastica": {"division": "II", "district": "6"}}

    def test_both_schools_get_each_match(self):
        dunham = self.pending["Dunham"]
        self.assertEqual(len(dunham), 4)
        opps = [r["opponent"] for r in self.pending["Central - B.R."]]
        self.assertIn("Dunham", opps)

    def test_no_school_double_booked(self):
        for school, rows in self.pending.items():
            slots = [(r["date"], r["time"]) for r in rows]
            self.assertEqual(len(slots), len(set(slots)), school)

    def test_adds_unposted_matches_as_unplayed_tournament_rows(self):
        games = merge_pending_games("Calvary Baptist", [], self.info, self.pending)
        self.assertEqual(len(games), 4)
        first = games[0]
        self.assertEqual(first["game_date"], "10/30/2026")
        self.assertEqual(first["opponent"], "St. Scholastica")
        self.assertEqual(first["opp_division"], "II")
        self.assertEqual(first["is_tournament"], 1)
        self.assertEqual(first["tournament_name"], "Dunham")
        self.assertEqual(first["result"], "")
        self.assertEqual(first["match_time"], "4:00 PM")

    def test_lhsaa_row_replaces_placeholder(self):
        posted = [{"game_date": "10/30/2026", "opponent": "St. Scholastica",
                   "result": "W", "score": "25-20, 25-18", "match_num": 1}]
        games = merge_pending_games("Calvary Baptist", posted, self.info, self.pending)
        self.assertEqual(len(games), 4)  # 1 LHSAA + 3 placeholders
        self.assertEqual(sum(1 for g in games if g.get("source") == "lvay_pending"), 3)
        self.assertNotIn("St. Scholastica",
                         [g["opponent"] for g in games if g.get("source") == "lvay_pending"])

    def test_named_courts_and_other_tournaments(self):
        games = merge_pending_games("Iota", [], {}, self.pending)
        self.assertEqual([(g["opponent"], g["match_time"], g["location"]) for g in games],
                         [("Church Point", "7:00 PM", "Iota High School · Finals")])
        brusly = merge_pending_games("Brusly", [], {}, self.pending)
        self.assertEqual(len(brusly), 2)
        self.assertEqual(brusly[0]["location"], "Brusly High School · Main Gym")
        self.assertEqual(len(self.pending["Natchitoches Central"]), 2)

    def test_stale_placeholders_drop_off(self):
        self.assertEqual(pending_by_school(today=date(2026, 11, 5)), {})

    def test_school_names_are_known(self):
        for t in PENDING_TOURNAMENTS:
            for _, _, _, a, b in t["matches"]:
                self.assertIsNotNone(get_school(a), a)
                self.assertIsNotNone(get_school(b), b)


if __name__ == "__main__":
    unittest.main()
