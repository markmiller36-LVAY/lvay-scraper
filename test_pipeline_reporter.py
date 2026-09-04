import os
import sqlite3
import tempfile
import unittest

from pipeline_reporter import build_report, capture_snapshot


class PipelineReporterTests(unittest.TestCase):
    def test_reports_new_game_points_and_rating_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "test.db")
            conn = sqlite3.connect(db)
            conn.executescript("""
                CREATE TABLE games (sport TEXT, season TEXT, school TEXT, opponent TEXT,
                    game_date TEXT, win_loss TEXT, score TEXT);
                CREATE TABLE power_rankings (sport TEXT, season TEXT, school TEXT,
                    wins INTEGER, losses INTEGER, ties INTEGER, games_played INTEGER,
                    power_rating REAL);
                CREATE TABLE game_power_points (sport TEXT, season TEXT, school TEXT,
                    opponent TEXT, game_date TEXT, result TEXT, score TEXT,
                    opp_wins INTEGER, opp_losses INTEGER, opp_ties INTEGER,
                    base_pts REAL, div_bonus REAL, opp_quality REAL, total_pts REAL);
            """)
            before = capture_snapshot(["football"], db)
            conn.execute("INSERT INTO games VALUES (?,?,?,?,?,?,?)",
                         ("football", "2026", "Huntington", "Calvary Baptist", "9/3/2026", "L", "22-48"))
            conn.execute("INSERT INTO power_rankings VALUES (?,?,?,?,?,?,?,?)",
                         ("football", "2026", "Huntington", 0, 1, 0, 1, 10.0))
            conn.execute("INSERT INTO game_power_points VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                         ("football", "2026", "Huntington", "Calvary Baptist", "9/3/2026", "L", "22-48", 1, 0, 0, 0, 0, 10, 10))
            conn.commit()
            conn.close()
            after = capture_snapshot(["football"], db)
            subject, body, summary = build_report(before, after, ["football"], "test run")
            self.assertEqual(summary, {"game_changes": 1, "rating_changes": 1})
            self.assertIn("Huntington", body)
            self.assertIn("22-48", body)
            self.assertIn("10.0", body)
            self.assertIn("1 game updates", subject)


if __name__ == "__main__":
    unittest.main()
