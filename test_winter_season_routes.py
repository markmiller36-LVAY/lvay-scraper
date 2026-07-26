import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

os.environ.setdefault(
    "DB_PATH", os.path.join(tempfile.gettempdir(), "lvay_route_tests.db")
)
import server


class WinterSeasonRouteTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute(
            "CREATE TABLE power_rankings (sport TEXT, season TEXT)"
        )
        self.db.execute(
            "INSERT INTO power_rankings VALUES ('boys_basketball', '2026')"
        )

    def tearDown(self):
        self.db.close()

    def test_explicit_empty_season_does_not_fall_back(self):
        with server.app.test_request_context("/?season=2027"):
            self.assertEqual(
                server.available_season(
                    self.db, "boys_basketball", "power_rankings"
                ),
                "2027",
            )

    def test_no_explicit_season_keeps_latest_populated_data(self):
        with patch.dict(os.environ, {}, clear=True):
            with server.app.test_request_context("/"):
                self.assertEqual(
                    server.available_season(
                        self.db, "boys_basketball", "power_rankings"
                    ),
                    "2026",
                )


if __name__ == "__main__":
    unittest.main()
