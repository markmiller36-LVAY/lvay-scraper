import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from season_schedule_importer import import_payload, parse_schedule_text


SAMPLE = """
5A

Airline Vikings (1-5A, NS1)
W1 Wossman
W2 @ C.E. Byrd*
W3 Pulaski Academy (AR)
W4 --------

C.E. Byrd Yellow Jackets (1-5A, S1)
W1 @ Ouachita
W2 Airline*
W3 JV
"""


class SeasonScheduleImporterTests(unittest.TestCase):
    def test_parses_links_district_oos_and_byes(self):
        payload = parse_schedule_text(SAMPLE, "2026", "test source")
        self.assertEqual(payload["validation"]["school_count"], 2)
        airline = payload["schools"][0]
        self.assertEqual(airline["school"], "Airline")
        self.assertEqual(airline["division"], "Non-Select Division 1")
        self.assertTrue(airline["games"][1]["opponent_internal"])
        self.assertTrue(airline["games"][1]["is_district"])
        self.assertTrue(airline["games"][2]["out_of_state"])
        self.assertTrue(airline["games"][3]["is_bye"])

    def test_import_is_season_scoped_and_idempotent(self):
        payload = parse_schedule_text(SAMPLE, "2026", "test source")
        with tempfile.TemporaryDirectory() as folder:
            db_path = str(Path(folder) / "test.db")
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE games (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sport TEXT, season TEXT, school TEXT, week TEXT,
                    game_date TEXT, opponent TEXT, win_loss TEXT, score TEXT,
                    home_away TEXT, district_class TEXT, scraped_at TEXT,
                    UNIQUE(sport, season, school, week)
                )
                """
            )
            conn.commit()
            conn.close()

            first = import_payload(payload, db_path, replace=True)
            second = import_payload(payload, db_path, replace=False)
            self.assertEqual(first["schools"], 2)
            self.assertEqual(second["schools"], 2)

            conn = sqlite3.connect(db_path)
            school_count = conn.execute(
                "SELECT COUNT(*) FROM season_schools"
            ).fetchone()[0]
            game_count = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
            registry = conn.execute(
                """
                SELECT status, is_locked FROM season_registry
                WHERE sport='football' AND season='2026'
                """
            ).fetchone()
            conn.close()
            self.assertEqual(school_count, 2)
            self.assertEqual(game_count, 6)
            self.assertEqual(registry, ("preseason", 0))

    def test_generated_full_payload_is_valid_json(self):
        payload = parse_schedule_text(SAMPLE, "2026", "test source")
        serialized = json.dumps(payload)
        self.assertEqual(json.loads(serialized)["season"], "2026")


if __name__ == "__main__":
    unittest.main()
