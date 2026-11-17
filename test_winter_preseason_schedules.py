import os
import sqlite3
import tempfile
import unittest

os.environ.setdefault("DB_PATH", os.path.join(tempfile.gettempdir(), "lvay_route_tests.db"))
import server


class PreseasonWinterScheduleTests(unittest.TestCase):
    """A new season's LHSAA schedules show before any power rating exists."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        path = os.path.join(self.tmp.name, "t.db")
        conn = sqlite3.connect(path)
        conn.executescript("""
            CREATE TABLE power_rankings (sport TEXT, season TEXT, school TEXT,
              division TEXT, track TEXT, class_ TEXT, district TEXT,
              power_rating REAL, wins INT, losses INT, ties INT,
              games_played INT, rank INT);
            CREATE TABLE game_power_points (sport TEXT, season TEXT, school TEXT,
              week INT, opponent TEXT, result TEXT, score TEXT, opp_wins INT,
              opp_losses INT, opp_ties INT, opp_division TEXT, base_pts REAL,
              div_bonus REAL, opp_quality REAL, total_pts REAL, is_district INT,
              game_date TEXT, home_away TEXT);
            CREATE TABLE season_schools (sport TEXT, season TEXT, school TEXT,
              class_ TEXT, district TEXT, division TEXT, track TEXT,
              source TEXT, status TEXT);
            CREATE TABLE games (sport TEXT, season TEXT, school TEXT, game_date TEXT,
              opponent TEXT, home_away TEXT, win_loss TEXT, score TEXT, week TEXT,
              opponent_class TEXT, tournament TEXT, tournament_host TEXT,
              location TEXT, out_of_state TEXT, is_district INT, district_class TEXT);
            INSERT INTO season_schools (sport, season, school)
              VALUES ('boys_basketball','2099','Bravo'), ('boys_basketball','2099','Alpha');
            INSERT INTO games (sport, season, school, game_date, opponent, home_away,
              win_loss, score, tournament, tournament_host, is_district, district_class, opponent_class)
              VALUES ('boys_basketball','2099','Alpha','12/2/2098','Bravo','H','','','D','',0,'2-5A','2-5A'),
                     ('boys_basketball','2099','Alpha','11/20/2098','Charlie','A','','','T','Turkey Classic',0,'2-5A','3-4A'),
                     ('boys_basketball','2099','Bravo','12/2/2098','Alpha','A','','','D','',0,'4-1A','2-5A');
        """)
        conn.commit()
        conn.close()
        self._old = server.DB_PATH
        server.DB_PATH = path
        self.client = server.app.test_client()

    def tearDown(self):
        server.DB_PATH = self._old
        self.tmp.cleanup()

    def test_preseason_schedules_show_before_power_ratings(self):
        d = self.client.get("/api/schedules/winter/boys_basketball?season=2099").get_json()
        self.assertTrue(d["preseason"])
        self.assertEqual(d["status"], "active")
        self.assertEqual([s["school"] for s in d["schools"]], ["Alpha", "Bravo"])
        alpha = d["schools"][0]
        self.assertEqual([g["opponent"] for g in alpha["games"]], ["Charlie", "Bravo"])
        self.assertTrue(alpha["games"][0]["is_tournament"])
        self.assertEqual(alpha["games"][1]["is_district"], 1)
        self.assertEqual(alpha["games"][1]["result"], "")
        self.assertEqual((alpha["class_"], alpha["district"]), ("5A", "2"))
        self.assertFalse(alpha["games"][1]["is_tournament"])
        self.assertEqual(alpha["games"][0]["is_district"], 0)
        one = self.client.get("/api/schedules/winter/boys_basketball?season=2099&school=Bravo").get_json()
        self.assertEqual([s["school"] for s in one["schools"]], ["Bravo"])

    def test_past_season_is_not_treated_as_preseason(self):
        d = self.client.get("/api/schedules/winter/boys_basketball?season=2001").get_json()
        self.assertEqual(d["schools"], [])


if __name__ == "__main__":
    unittest.main()


class FullScheduleAfterFirstResultTests(unittest.TestCase):
    """A rated school still shows its whole posted schedule, not just results."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        path = os.path.join(self.tmp.name, "t.db")
        conn = sqlite3.connect(path)
        conn.executescript("""
            CREATE TABLE power_rankings (sport TEXT, season TEXT, school TEXT,
              division TEXT, track TEXT, class_ TEXT, district TEXT,
              power_rating REAL, wins INT, losses INT, ties INT,
              games_played INT, rank INT);
            CREATE TABLE game_power_points (sport TEXT, season TEXT, school TEXT,
              week INT, opponent TEXT, result TEXT, score TEXT, opp_wins INT,
              opp_losses INT, opp_ties INT, opp_division TEXT, base_pts REAL,
              div_bonus REAL, opp_quality REAL, total_pts REAL, is_district INT,
              game_date TEXT, home_away TEXT);
            CREATE TABLE season_schools (sport TEXT, season TEXT, school TEXT,
              class_ TEXT, district TEXT, division TEXT, track TEXT,
              source TEXT, status TEXT);
            CREATE TABLE games (sport TEXT, season TEXT, school TEXT, game_date TEXT,
              opponent TEXT, home_away TEXT, win_loss TEXT, score TEXT, week TEXT,
              opponent_class TEXT, tournament TEXT, tournament_host TEXT,
              location TEXT, out_of_state TEXT, is_district INT, district_class TEXT);
            INSERT INTO power_rankings (sport, season, school, power_rating, wins,
              losses, ties, games_played, rank)
              VALUES ('boys_soccer','2099','Calvary Baptist',0.5,0,1,0,1,25);
            INSERT INTO game_power_points (sport, season, school, week, opponent,
              result, score, total_pts, is_district, game_date, home_away)
              VALUES ('boys_soccer','2099','Calvary Baptist',1,'Loyola Prep','L','1-5',
                      0.5,0,'11/16/2098','A');
            INSERT INTO games (sport, season, school, game_date, opponent, home_away,
              win_loss, score, tournament, tournament_host, is_district, district_class, opponent_class)
              VALUES ('boys_soccer','2099','Calvary Baptist','11/16/2098','Loyola Prep','A','L','1-5','','',0,'',''),
                     ('boys_soccer','2099','Calvary Baptist','11/20/2098','Captain Shreve','H','','','','',0,'',''),
                     ('boys_soccer','2099','Calvary Baptist','11/10/2098','Byrd','H','','','','',0,'','');
        """)
        conn.commit()
        conn.close()
        self._old = server.DB_PATH
        server.DB_PATH = path
        self.client = server.app.test_client()

    def tearDown(self):
        server.DB_PATH = self._old
        self.tmp.cleanup()

    def test_rated_school_keeps_unplayed_games(self):
        d = self.client.get("/api/schedules/winter/boys_soccer?season=2099").get_json()
        games = d["schools"][0]["games"]
        self.assertEqual(
            [g["opponent"] for g in games], ["Byrd", "Loyola Prep", "Captain Shreve"]
        )
        loyola = games[1]
        self.assertEqual((loyola["result"], loyola["score"]), ("L", "1-5"))
        self.assertIn("total_pts", loyola)
        self.assertEqual(games[2]["result"], "")
