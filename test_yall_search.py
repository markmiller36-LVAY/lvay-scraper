"""Tests for the template-based "Y'all" football history search."""

import unittest

import yall_search as y


def g(school, opp, score, date, phase="Regular Season", ha="H", dist=False):
    return {"week": 1, "phase": phase, "game_date": date, "opponent": opp,
            "home_away": ha, "result": "", "score": score,
            "is_district": dist, "forfeit": ""}


ARCHIVE = {"seasons": {
    "2019": {"schools": [
        {"school": "West Monroe", "class_": "5A", "games": [
            g("West Monroe", "Neville", "28-52", "10/11/2019", dist=True),
            g("West Monroe", "Ruston", "35-0", "10/18/2019", ha="A"),
        ]},
        {"school": "Neville", "class_": "4A", "games": [
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
        self.assertEqual(self.ask("how many pickles")["status"], "no_team")

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


class OpponentClassTests(unittest.TestCase):
    def test_record_vs_class(self):
        a = y.Answerer(y.build_games(ARCHIVE))
        r = a.answer("Whats Neville all time record vs 5A teams?")
        self.assertEqual(r["intent"], "record")
        self.assertIn("vs 5A opponents", r["filters"])
        self.assertIn("1-0", r["answer"])  # beat 5A West Monroe in 2019
        r = a.answer("West Monroe record against 4A")
        self.assertIn("0-1", r["answer"])  # lost to 4A Neville in 2019


HIST = {"seasons": {
    str(y): {"schools": [
        {"school": "Evangel Christian", "class_": c, "division": d,
         "track": tr, "games": [g("Evangel Christian", "Many", "35-7",
                                  f"9/6/{y}")]},
        {"school": "Many", "class_": "1A", "division": "", "track": "",
         "games": [g("Many", "Evangel Christian", "7-35", f"9/6/{y}",
                     ha="A")]},
    ]}
    for y, c, d, tr in [
        (2012, "2A", "", ""), (2013, "3A", "", ""),
        (2016, "5A", "Division I", ""), (2019, "4A", "Division Ii", ""),
        (2022, "4A", "Select Division II", "select"),
        (2023, "5A", "Select Division I", "select"),
        (2024, "5A", "Select Division I", "select"),
    ]
}}


class FactTests(unittest.TestCase):
    def test_general_history_questions(self):
        a = y.Answerer(y.build_games(ARCHIVE))
        for q, want in [
            ("When did they add 5A?", "1991"),
            ("When did select start?", "2013"),
            ("When did non-select divisions start?", "2022"),
            ("What is select and non select?", "attendance zone"),
            ("How often do they reclass?", "COVID"),
            ("How many playoff brackets are there?", "8"),
        ]:
            r = a.answer(q)
            self.assertEqual(r["intent"], "fact", q)
            self.assertIn(want, r["answer"], q)


class HandbookTests(unittest.TestCase):
    def test_rules_questions_use_handbook(self):
        a = y.Answerer(y.build_games(ARCHIVE))
        r = a.answer("How are power ratings calculated?")
        self.assertEqual(r["intent"], "rules")   # plain-English answer
        self.assertIn("divide by the number of games", r["answer"])
        self.assertTrue(r["handbook"][0]["rule"].startswith("14.12"))
        r = a.answer("how old can a player be")
        self.assertEqual(r["handbook"][0]["rule"], "1.6")
        r = a.answer("what is the transfer rule")
        self.assertEqual(r["handbook"][0]["rule"], "1.13")

    def test_eligibility_answers_carry_ad_note(self):
        a = y.Answerer(y.build_games(ARCHIVE))
        r = a.answer("do incoming freshmen have to live in district")
        self.assertEqual(r["handbook"][0]["rule"], "1.3.2")
        self.assertIn("athletic director and head coach", r["note"])
        self.assertFalse(a.answer("is there a mercy rule")["note"])

    def test_unlisted_rules_fall_back_to_handbook(self):
        a = y.Answerer(y.build_games(ARCHIVE))
        r = a.answer("what are the rules for officials uniforms")
        self.assertEqual(r["intent"], "handbook")

    def test_game_questions_do_not_use_handbook(self):
        a = y.Answerer(y.build_games(ARCHIVE))
        self.assertEqual(a.answer("West Monroe vs Neville")["intent"],
                         "head_to_head")


class StreakAndCompareTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = y.Answerer(y.build_games(ARCHIVE))

    def test_all_teams_scoring_streak(self):
        r = self.a.answer("last time a team scored 20+ in 2 straight games")
        self.assertEqual(r["intent"], "points_streak")
        self.assertIn("West Monroe", r["answer"])
        self.assertTrue(r["table"]["rows"])

    def test_season_compare_two_schools(self):
        r = self.a.answer("West Monroe 2019 vs West Monroe 2021")
        self.assertEqual(r["intent"], "compare_seasons")
        self.assertEqual(r["table"]["cols"][1:],
                         ["West Monroe 2019", "West Monroe 2021"])

    def test_year_first_list_without_vs(self):
        r = self.a.answer("2019 West Monroe, 2021 Neville")
        self.assertEqual(r["intent"], "compare_seasons")

    def test_year_range_is_not_a_compare(self):
        r = self.a.answer("West Monroe record between 2019 and 2021")
        self.assertEqual(r["intent"], "record")


class LeaderTests(unittest.TestCase):
    def test_season_and_game_leaders(self):
        a = y.Answerer(y.build_games(ARCHIVE))
        r = a.answer("which team scored the most points in a single game")
        self.assertEqual(r["intent"], "game_leaders")
        self.assertIn("Neville 52", r["answer"])
        r = a.answer("best point differential in a season")
        self.assertIsNotNone(r)   # too few games here: falls back, no crash


class NoteTests(unittest.TestCase):
    def test_note_only_when_pertinent(self):
        a = y.Answerer(y.build_games(ARCHIVE))
        self.assertFalse(a.answer("West Monroe vs Neville").get("note"))
        self.assertTrue(a.answer("West Monroe record vs select schools")
                        .get("note"))


class ClassAndSelectTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a = y.Answerer(y.build_games(HIST))

    def test_how_long_in_class(self):
        r = self.a.answer("how long has Evangel been 5A???")
        self.assertEqual(r["intent"], "class_history")
        self.assertIn("5A since 2023", r["answer"])
        self.assertTrue(r["history"])

    def test_class_in_year(self):
        r = self.a.answer("what class was Evangel in 2012")
        self.assertIn("was 2A in 2012", r["answer"])

    def test_select_history(self):
        r = self.a.answer("Is Evangel select or non select?")
        self.assertEqual(r["intent"], "track_history")
        self.assertIn("Select", r["answer"])
        r = self.a.answer("what class was Evangel in 2016")
        self.assertIn("Division I (Select)", r["answer"])
        self.assertTrue(r["note"])
        r = self.a.answer("what class was Many in 2016")
        self.assertIn("Class 1A playoff bracket", r["answer"])

    def test_no_select_before_2013(self):
        r = self.a.answer("Was Evangel select in 2012?")
        self.assertIn("no Select/Non-Select split", r["answer"])

    def test_own_class_filter_is_not_history(self):
        r = self.a.answer("Evangel record when they were 4A")
        self.assertEqual(r["intent"], "record")
        self.assertIn("2-0", r["answer"])

    def test_opponent_and_own_track_filters(self):
        r = self.a.answer("Evangel record as a select school")
        self.assertIn("while Select", r["filters"])
        r = self.a.answer("Many record vs select schools")
        self.assertIn("0-", r["answer"])


if __name__ == "__main__":
    unittest.main()
