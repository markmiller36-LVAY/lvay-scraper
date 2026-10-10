import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import run_power_rankings
import server
from jv_only_schools import is_jv_only_game


class JvOnlySchoolTests(unittest.TestCase):
    def test_either_side_marks_the_game(self):
        self.assertTrue(is_jv_only_game("football", "2026", "Highland Baptist", "False River Academy"))
        self.assertTrue(is_jv_only_game("football", "2026", "False River Academy", "Highland Baptist"))
        self.assertFalse(is_jv_only_game("football", "2026", "Highland Baptist", "Ascension Episcopal"))
        self.assertFalse(is_jv_only_game("football", "2025", "Highland Baptist", "False River Academy"))

    def test_jv_game_is_shown_but_not_counted(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            with mock.patch.object(server, "DB_PATH", path):
                conn = sqlite3.connect(path)
                run_power_rankings.init_tables(conn)
                conn.commit(); conn.close()
                server.init_db()
                conn = sqlite3.connect(path)
                conn.execute("""INSERT INTO season_schools
                    (sport,season,school,class_,district,division,track,source,status)
                    VALUES ('football','2026','Highland Baptist','1A','7','Division IV','Select','test','active')""")
                conn.executemany("""INSERT INTO games
                    (sport,season,school,week,game_date,opponent,win_loss,score,is_district,home_away)
                    VALUES ('football','2026','Highland Baptist',?,?,?,?,?,?,?)""", [
                    ("Week 1", "9/4/2026", "Ascension Episcopal", "L", "7-21", 1, "H"),
                    ("Week 7", "10/16/2026", "False River Academy", "W", "35-0", 1, "A"),
                ])
                conn.commit(); conn.close()
                client = server.app.test_client()

                standings = client.get(
                    "/api/standings/football?season=2026&class=1A&districts=7"
                ).get_json()
                team = standings["districts"][0]["teams"][0]
                self.assertEqual(team["overall_record"], "0-1")
                self.assertEqual(team["district_record"], "0-1")

                response = client.get("/api/schedules/football?season=2026")
                schedule = response.get_json()
                self.assertIn("schools", schedule, schedule)
                games = {g["week"]: g for g in schedule["schools"][0]["games"]}
                jv = games[7]
                self.assertEqual(jv["opponent"], "False River Academy (JV ONLY)")
                self.assertEqual(jv["result"], "W (JV)")
                self.assertEqual(jv["score"], "35-0")
                self.assertTrue(jv["jv_only"])
                self.assertFalse(jv["is_district"])
                self.assertEqual(games[1]["result"], "L")
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
