import os
import sqlite3
import tempfile
import unittest
from datetime import date

_TMP = tempfile.mkdtemp()
os.environ["DB_PATH"] = os.path.join(_TMP, "lvay_predictions_test.db")

import server  # noqa: E402  (creates the base tables)
import power_board_predictions as pbp  # noqa: E402


SEASON = "2026"


def _setup(conn):
    # The live database uses the ratings engine's own schema for these two.
    from run_power_rankings import init_tables
    conn.execute("DROP TABLE IF EXISTS power_rankings")
    conn.execute("DROP TABLE IF EXISTS game_power_points")
    init_tables(conn)
    for t in ("games", "power_rankings", "season_schools",
              "game_power_points", "power_board_predictions"):
        try:
            conn.execute(f"DELETE FROM {t}")
        except sqlite3.Error:
            pass
    schools = {"Alpha": 20.0, "Bravo": 12.0, "Charlie": 8.0, "Delta": 5.0}
    for name, pr in schools.items():
        conn.execute(
            "INSERT INTO season_schools (sport, season, school, class_, district) "
            "VALUES ('football', ?, ?, '3A', '1')", (SEASON, name))
        conn.execute(
            "INSERT INTO power_rankings (sport, season, school, power_rating, "
            "wins, losses, ties, games_played, class_, district) "
            "VALUES ('football', ?, ?, ?, 0, 0, 0, 0, '3A', '1')",
            (SEASON, name, pr))

    def game(school, opp, wk, wl, score, ha, gd):
        conn.execute(
            "INSERT INTO games (sport, season, school, opponent, week, win_loss, "
            "score, home_away, game_date) VALUES ('football', ?, ?, ?, ?, ?, ?, ?, ?)",
            (SEASON, school, opp, f"Week {wk}", wl, score, ha, gd))

    # Week 1 (played): Alpha beat Charlie, Bravo beat Delta.
    # Score written loser-first on one row to prove we fix the order.
    game("Alpha", "Charlie", 1, "W", "14-35", "H", "9/4/2026")
    game("Charlie", "Alpha", 1, "L", "14-35", "A", "9/4/2026")
    game("Bravo", "Delta", 1, "W", "21-7", "A", "9/4/2026")
    game("Delta", "Bravo", 1, "L", "7-21", "H", "9/4/2026")
    # Week 2 (upcoming Friday): Alpha at Bravo; Charlie vs Delta far away.
    game("Alpha", "Bravo", 2, "", "", "A", "10/16/2026")
    game("Bravo", "Alpha", 2, "", "", "H", "10/16/2026")
    game("Charlie", "Delta", 3, "", "", "H", "10/30/2026")
    game("Delta", "Charlie", 3, "", "", "A", "10/30/2026")
    conn.commit()


class PowerBoardPredictionTests(unittest.TestCase):
    def setUp(self):
        self.conn = server.get_db()
        try:
            _setup(self.conn)
        except Exception:
            self.conn.close()
            raise

    def tearDown(self):
        self.conn.close()

    def test_strength_factor_formula(self):
        rows, _roster, ratings = pbp.load_season_games(self.conn, SEASON)
        sf = pbp.strength_factors(rows, ratings)
        # Alpha: 1-0, +21 margin, PR 20 -> 50 + 21 + 40 = 111
        self.assertEqual(sf["alpha"], 111.0)
        # Charlie: 0-1, -21, PR 8 -> 0 - 21 + 16 = -5
        self.assertEqual(sf["charlie"], -5.0)

    def test_freezes_only_games_kicking_off_soon(self):
        summary = pbp.freeze_upcoming_predictions(
            self.conn, SEASON, today=date(2026, 10, 13))
        self.assertEqual(summary["written"], 1)
        preds = pbp.predictions_lookup(self.conn, SEASON)
        a = preds[("alpha", "bravo", 2)]
        b = preds[("bravo", "alpha", 2)]
        self.assertEqual(a[1] + b[1], 100)
        self.assertEqual({a[0], b[0]}, {"W", "L"})
        self.assertNotIn(("charlie", "delta", 3), preds)

    def test_frozen_prediction_never_changes(self):
        pbp.freeze_upcoming_predictions(self.conn, SEASON, today=date(2026, 10, 13))
        before = pbp.predictions_lookup(self.conn, SEASON)[("alpha", "bravo", 2)]
        self.conn.execute(
            "UPDATE power_rankings SET power_rating=1 WHERE school='Alpha'")
        self.conn.commit()
        summary = pbp.freeze_upcoming_predictions(
            self.conn, SEASON, today=date(2026, 10, 14))
        self.assertEqual(summary["written"], 0)
        after = pbp.predictions_lookup(self.conn, SEASON)[("alpha", "bravo", 2)]
        self.assertEqual(before, after)

    def test_probability_is_clamped(self):
        sf = {"a": 500.0, "b": -500.0}
        self.assertEqual(pbp.win_probability(sf, {}, "a", "b", True), 97)
        self.assertEqual(pbp.win_probability(sf, {}, "b", "a", False), 3)

    def test_feed_publishes_result_but_not_formula(self):
        pbp.freeze_upcoming_predictions(self.conn, SEASON, today=date(2026, 10, 13))
        server._RATE_BUCKETS.clear()
        client = server.app.test_client()
        data = client.get("/api/schedules/football?season=2026").get_json()
        alpha = next(s for s in data["schools"] if s["school"] == "Alpha")
        wk2 = next(g for g in alpha["games"] if g["week"] == 2)
        self.assertIn(wk2["pred_wl"], ("W", "L"))
        self.assertIsInstance(wk2["pred_prob"], int)
        body = client.get("/api/schedules/football?season=2026").get_data(as_text=True)
        for secret in ("HFA", "PREDICTION_SCALE", "COMMON_OPP"):
            self.assertNotIn(secret, body)

    def test_build_endpoint_requires_token(self):
        client = server.app.test_client()
        old = os.environ.get("PIPELINE_TOKEN")
        os.environ["PIPELINE_TOKEN"] = "secret-token"
        try:
            self.assertEqual(
                client.get("/api/build/football-predictions").status_code, 401)
        finally:
            if old is None:
                os.environ.pop("PIPELINE_TOKEN", None)
            else:
                os.environ["PIPELINE_TOKEN"] = old


if __name__ == "__main__":
    unittest.main()
