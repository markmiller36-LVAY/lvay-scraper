import os
import sqlite3
import tempfile
import unittest
from datetime import date

os.environ.setdefault("FOOTBALL_SEASON_YEAR", "2026")

from football_oos_verifier import (
    Observation, choose_verified_record, import_verified_row, parse_record,
)


class FootballOOSVerifierTests(unittest.TestCase):
    def test_arkansas_is_zero_before_first_official_playing_date(self):
        observations = [
            Observation(1, 0, 0, "https://www.maxpreps.com/ar/example/football/", "MaxPreps")
        ]

        record, reason = choose_verified_record(
            observations, state="AR", as_of=date(2026, 8, 26)
        )

        self.assertEqual((0, 0, 0), record)
        self.assertIn("before 2026-08-27", reason)

    def test_arkansas_maxpreps_controls_on_and_after_august_27(self):
        observations = [
            Observation(1, 0, 0, "https://www.maxpreps.com/ar/example/football/", "MaxPreps"),
            Observation(2, 0, 0, "https://example-school.org/football", "example-school.org"),
        ]

        record, reason = choose_verified_record(
            observations, state="AR", as_of=date(2026, 8, 27)
        )

        self.assertEqual((1, 0, 0), record)
        self.assertIn("MaxPreps primary", reason)

    def test_parses_dave_campbells_team_record(self):
        html = "<main><h2>2026 Schedule 2 - 0</h2></main>"

        self.assertEqual(
            (2, 0, 0),
            parse_record(
                html,
                "https://www.texasfootball.com/team/houston-st-john-s-mavericks",
                "2026",
            ),
        )

    def test_dave_campbells_record_controls_aggregator_conflict(self):
        observations = [
            Observation(
                2, 0, 0,
                "https://www.texasfootball.com/team/houston-st-john-s-mavericks",
                "Dave Campbell's Texas Football",
            ),
            Observation(
                2, 1, 0,
                "https://www.maxpreps.com/tx/houston/st-johns-mavericks/football/",
                "MaxPreps",
            ),
        ]

        record, reason = choose_verified_record(observations)

        self.assertEqual((2, 0, 0), record)
        self.assertIn("Dave Campbell's", reason)

    def test_parses_current_maxpreps_overall_record(self):
        html = """
        <div class='year'>26-27</div><div><h4>Overall</h4>
        <div class='data'>2-1</div></div><h4>District</h4><div>0-0</div>
        """
        self.assertEqual(
            parse_record(html, "https://www.maxpreps.com/example/football/", "2026"),
            (2, 1, 0),
        )

    def test_rejects_stale_season(self):
        html = "<div>25-26</div><h4>Overall</h4><div>10-2</div>"
        self.assertIsNone(
            parse_record(html, "https://www.maxpreps.com/example/football/", "2026")
        )

    def test_parses_mhsaa_team_overall_record(self):
        html = "<h1>Brandon Bulldogs</h1><div>1-2 Overall • 0-0 League</div>"
        self.assertEqual(
            parse_record(
                html,
                "https://scores.misshsaa.com/teams/244214/schedule",
                "2026",
            ),
            (1, 2, 0),
        )

    def test_mhsaa_record_controls_when_secondary_disagrees(self):
        observations = [
            Observation(2, 1, 0, "https://scores.misshsaa.com/teams/1/schedule", "MHSAA/SBLive"),
            Observation(1, 1, 0, "https://maxpreps.com/a", "MaxPreps"),
        ]
        record, reason = choose_verified_record(observations)
        self.assertEqual(record, (2, 1, 0))
        self.assertIn("primary", reason.lower())

    def test_distinct_provider_consensus(self):
        observations = [
            Observation(2, 0, 0, "https://maxpreps.com/a", "MaxPreps"),
            Observation(2, 0, 0, "https://school.org/a", "school.org"),
        ]
        record, reason = choose_verified_record(observations)
        self.assertEqual(record, (2, 0, 0))
        self.assertIn("MaxPreps", reason)

    def test_import_expands_multiple_louisiana_opponents_and_blanks_division(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "oos-test.db")
            conn = sqlite3.connect(db_path)
            from football_oos_verifier import _ensure_oos_table
            _ensure_oos_table(conn)
            row = {
                "Season": "2026", "OOS School": "Parklane Academy",
                "Louisiana Opponent(s)": "Parkview Baptist; Dunham",
                "LHSAA Class": "2A",
            }
            count = import_verified_row(
                conn, row, (1, 1, 0), "https://example.com", "now"
            )
            result = conn.execute(
                "SELECT school, opponent, opp_wins, opp_losses, division, class_ "
                "FROM oos_opponents ORDER BY school"
            ).fetchall()
            self.assertEqual(count, 2)
            self.assertEqual(result, [
                ("Dunham", "Parklane Academy", 1, 1, "", "2A"),
                ("Parkview Baptist", "Parklane Academy", 1, 1, "", "2A"),
            ])
            conn.close()

    def test_record_over_ten_games_is_never_imported(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = sqlite3.connect(os.path.join(tmp, "oos-cap.db"))
            from football_oos_verifier import _ensure_oos_table
            _ensure_oos_table(conn)
            row = {
                "Season": "2026", "OOS School": "Louisville High School",
                "Louisiana Opponent(s)": "John Curtis Christian",
                "LHSAA Class": "2A",
            }
            self.assertEqual(
                import_verified_row(conn, row, (9, 2, 0), "u", "now"), 0
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM oos_opponents").fetchone()[0], 0
            )
            self.assertEqual(
                import_verified_row(conn, row, (8, 2, 0), "u", "now"), 1
            )
            conn.close()

    def test_run_flags_source_record_over_ten_games(self):
        import football_oos_verifier as v

        headers = [
            "Season", "State", "OOS School", "Louisiana Opponent(s)",
            "LHSAA Class", "Primary Record URL", "Secondary Record URL",
            "Backup Record URL", "Record Source", "Wins", "Losses", "Ties",
            "Games Played", "Record Through Date", "Verified By",
            "Verification Status", "Include in Engine?", "Last Checked", "Notes",
        ]
        data = dict.fromkeys(headers, "")
        data.update({
            "Season": "2026", "State": "MS", "OOS School": "Louisville High School",
            "Louisiana Opponent(s)": "John Curtis Christian", "LHSAA Class": "2A",
            "Primary Record URL": "https://a", "Wins": "8", "Losses": "2",
            "Ties": "0", "Include in Engine?": "Yes",
        })

        class FakeWS:
            def __init__(self):
                self.updates = []

            def get(self, _range):
                return [headers, [data[h] for h in headers]]

            def batch_update(self, updates, value_input_option=None):
                self.updates.extend(updates)

        obs = Observation(9, 2, 0, "https://a", "MHSAA/SBLive")
        orig_fetch, orig_choose = v.fetch_observation, v.choose_verified_record
        v.fetch_observation = lambda url, season, session: (obs, None)
        v.choose_verified_record = lambda observations, state="": ((9, 2, 0), "ok")
        try:
            with tempfile.TemporaryDirectory() as tmp:
                ws = FakeWS()
                summary = v.run(db_path=os.path.join(tmp, "x.db"), worksheet=ws, session=object())
        finally:
            v.fetch_observation, v.choose_verified_record = orig_fetch, orig_choose
        self.assertEqual(summary["imported"], 0)
        self.assertEqual(summary["review"], 1)
        written = {u["range"]: u["values"][0][0] for u in ws.updates}
        self.assertIn("8", written.values())
        self.assertIn("Needs Review", written.values())


if __name__ == "__main__":
    unittest.main()


class StuckAlertTests(unittest.TestCase):
    def test_title_instead_of_url_is_flagged_even_when_recent(self):
        from football_oos_verifier import stuck_alert
        row = {"Primary Record URL": "West Point Varsity Football Schedule - MHSAA",
               "Louisiana Opponent(s)": "Ruston"}
        alert = stuck_alert("West Point High School", row, 0, 6, 0, "2026-11-02",
                            today=date(2026, 11, 3))
        self.assertIsNotNone(alert)
        self.assertIn("not web addresses", alert["reason"])
        self.assertEqual("0-6", alert["record"])

    def test_old_needs_review_record_is_flagged(self):
        from football_oos_verifier import stuck_alert
        row = {"Primary Record URL": "https://www.maxpreps.com/ms/x/football/"}
        alert = stuck_alert("X", row, 2, 5, 0, "2026-10-11", today=date(2026, 11, 3))
        self.assertEqual(23, alert["days"])

    def test_fresh_needs_review_with_good_links_is_quiet(self):
        from football_oos_verifier import stuck_alert
        row = {"Primary Record URL": "https://www.maxpreps.com/ms/x/football/"}
        self.assertIsNone(stuck_alert("X", row, 2, 5, 0, "2026-11-02", today=date(2026, 11, 3)))
