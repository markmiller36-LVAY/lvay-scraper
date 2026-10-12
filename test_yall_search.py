"""Tests for the template-based "Y'all" football history search."""

import unittest

import yall_search as y


def g(school, opp, score, date, phase="Regular Season", ha="H", dist=False):
    return {"week": 1, "phase": phase, "game_date": date, "opponent": opp,
            "home_away": ha, "result": "", "score": score,
            "is_district": dist, "forfeit": ""}


ARCHIVE = {"seasons": {
    "2019": {"schools": [
        {"school": "West Monroe", "games": [
            g("West Monroe", "Neville", "28-52", "10/11/2019", dist=True),
            g("West Monroe", "Ruston", "35-0", "10/18/2019", ha="A"),
        ]},
        {"school": "Neville", "games": [
            g("Neville", "West Monroe", "52-28", "10/11/2019", ha="A"),
        ]},
        {"school": "Woodlawn - Shrev.", "games": [
            g("Woodlawn - Shrev.", "C.E. Byrd", "7-14", "9/6/2019"),
        ]},
        {"school": "Woodlawn - B.R.", "games": [
            g("Woodlawn - B.R.", "Many", "21-20", "9/6/2019"),
        ]},
        {"school": "Many", "games": [
            g("Many", "Woodlawn - B.R.", "20-21", "9/6/2019", ha="A"),
        ]},
        {"school": "John Curtis Christian", "games": [
            g("John Curtis Christian", "Jesuit", "42-0", "9/6/2019"),
        ]},
    ]},
    "2021": {"schools": [
        {"school": "West Monroe", "games": [
            g("West Monroe", "Neville", "21-14", "10/15/2021"),
            g("West Monroe", "Ruston", "10-56", "10/22/2021", ha="A"),
        ]},
    ]},
}}


class YallSearchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = y.Answerer(y.build_games(ARCHIVE))

    def ask(self, q):
        return self.a.answer(q)

    def test_marks_example_question(self):
        r = self.ask("When is the last time West Monroe has had 50 points "
                     "put on them?")
        self.assertEqual(r["intent"], "allowed_n")
        self.assertIn("Oct 22, 2021", r["answer"])
        self.assertEqual(len(r["rows"]), 2)

    def test_typo_in_team_name(self):
        r = self.ask("last time West Monore gave up 50")
        self.assertEqual(r["teams"], ["West Monroe"])

    def test_scored_n(self):
        r = self.ask("When is the last time Neville scored 50?")
        self.assertEqual(r["intent"], "scored_n")
        self.assertIn("Neville 52", r["answer"])

    def test_head_to_head(self):
        r = self.ask("West Monroe vs Neville")
        self.assertEqual(r["intent"], "head_to_head")
        self.assertIn("1-1", r["answer"])

    def test_last_win_over(self):
        r = self.ask("When is the last time West Monroe beat Neville?")
        self.assertEqual(r["intent"], "h2h_last_win")
        self.assertIn("Oct 15, 2021", r["answer"])

    def test_ambiguous_school(self):
        r = self.ask("Woodlawn record in 2019")
        self.assertEqual(r["status"], "ambiguous")
        self.assertEqual(len(r["choices"]), 2)

    def test_common_word_school_needs_capital(self):
        self.assertEqual(self.ask("How many times has Many scored 20")
                         ["teams"], ["Many"])
        self.assertEqual(self.ask("how many games")["status"], "no_team")

    def test_short_name_and_year_filter(self):
        r = self.ask("John Curtis record in 2019")
        self.assertEqual(r["teams"], ["John Curtis Christian"])
        self.assertIn("1-0", r["answer"])

    def test_home_filter(self):
        r = self.ask("last time West Monroe lost on the road")
        self.assertIn("Oct 22, 2021", r["answer"])

    def test_shutouts(self):
        r = self.ask("How many shutouts does West Monroe have")
        self.assertEqual(r["intent"], "shutouts_for")
        self.assertIn("1 shutout ", r["answer"])

    def test_unknown_wording_falls_back(self):
        r = self.ask("Who was West Monroe's quarterback")
        self.assertEqual(r["status"], "no_template")
        self.assertTrue(r["examples"])


if __name__ == "__main__":
    unittest.main()
