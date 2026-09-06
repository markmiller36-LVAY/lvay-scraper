import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import server


class FootballStandingsTests(unittest.TestCase):
    def test_standings_totals_and_order(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            with mock.patch.object(server, "DB_PATH", path):
                server.init_db()
                conn = sqlite3.connect(path)
                for school in ("Alpha", "Beta"):
                    conn.execute("""INSERT INTO season_schools
                        (sport,season,school,class_,district,division,track,source,status)
                        VALUES ('football','2026',?,'5A','1','Division I','Non-Select','test','active')""", (school,))
                conn.executemany("""INSERT INTO games
                    (sport,season,school,week,game_date,opponent,win_loss,score,is_district)
                    VALUES ('football','2026',?,?,?,?,?,?,?)""", [
                    ("Alpha", "Week 1", "9/1/2026", "Beta", "W", "28-14", 1),
                    ("Alpha", "Week 2", "9/8/2026", "Gamma", "L", "10-7", 0),
                    ("Beta", "Week 1", "9/1/2026", "Alpha", "L", "14-28", 1),
                ])
                conn.commit(); conn.close()
                response = server.app.test_client().get(
                    "/api/standings/football?season=2026&class=5A&districts=1"
                )
                self.assertEqual(response.status_code, 200)
                teams = response.get_json()["districts"][0]["teams"]
                self.assertEqual([t["team"] for t in teams], ["Alpha", "Beta"])
                self.assertEqual(teams[0]["overall_record"], "1-1")
                self.assertEqual(teams[0]["district_record"], "1-0")
                self.assertEqual((teams[0]["pf"], teams[0]["pa"], teams[0]["point_differential"]), (35, 24, 11))
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
