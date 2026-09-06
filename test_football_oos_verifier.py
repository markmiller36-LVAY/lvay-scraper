import os
import sqlite3
import tempfile
import unittest

os.environ.setdefault("FOOTBALL_SEASON_YEAR", "2026")

from football_oos_verifier import (
    Observation, choose_verified_record, import_verified_row, parse_record,
)


class FootballOOSVerifierTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
