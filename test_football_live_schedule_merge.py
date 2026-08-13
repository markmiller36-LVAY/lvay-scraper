import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import scraper
from season_schedule_importer import import_payload


class FootballLiveScheduleMergeTests(unittest.TestCase):
    def test_current_lhsaa_selector_uses_opaque_one(self):
        with mock.patch.object(scraper.requests, "post") as post:
            response = mock.Mock(text="<html></html>")
            response.raise_for_status.return_value = None
            post.return_value = response
            scraper.fetch_page("football", "2026")
        self.assertEqual(post.call_args.kwargs["data"]["y"], "1")

    def test_winter_watchers_use_opaque_current_selector(self):
        for sport in (
            "boys_basketball", "girls_basketball",
            "boys_soccer", "girls_soccer",
        ):
            with self.subTest(sport=sport), mock.patch.object(
                scraper.requests, "post"
            ) as post:
                response = mock.Mock(text="<html></html>")
                response.raise_for_status.return_value = None
                post.return_value = response
                scraper.fetch_page(sport, "2027", "5A")
            self.assertEqual(post.call_args.kwargs["data"]["yr"], "1")

    def test_winter_watchers_begin_in_august(self):
        august = scraper.datetime(2026, 8, 13)
        for sport in (
            "boys_basketball", "girls_basketball",
            "boys_soccer", "girls_soccer",
        ):
            with self.subTest(sport=sport):
                self.assertEqual(scraper.should_scrape_sport(sport, august), (True, "active"))

    def test_lhsaa_overlays_week_without_deleting_gap_fill(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = str(Path(temp_dir) / "merge.db")
            with mock.patch.object(scraper, "DB_PATH", db_path):
                scraper.init_db()
                payload = {
                    "sport": "football",
                    "season": "2026",
                    "status": "preseason",
                    "source": "Louisiana Sportsline preseason compilation",
                    "generated_at": "2026-08-13T00:00:00",
                    "schools": [{
                        "school": "Airline", "class_": "5A", "district": 1,
                        "division": "Non-Select Division 1", "track": "Non-Select",
                        "source_division": "NS1",
                        "games": [
                            {"week": 1, "game_date": None, "opponent": "Old Opponent",
                             "home_away": "H", "is_district": False, "is_bye": False,
                             "needs_review": False},
                            {"week": 2, "game_date": None, "opponent": "Gap Filler",
                             "home_away": "A", "is_district": False, "is_bye": False,
                             "needs_review": True},
                        ],
                    }],
                }
                import_payload(payload, db_path, replace=True)
                official = [{
                    "sport": "football", "season": "2026", "school": "Airline",
                    "week": "Week 1", "game_date": "9/4/2026 7:00:00 PM",
                    "opponent": "Wossman", "home_away": "H", "win_loss": "",
                    "score": "-", "district": "2", "class_": "4A",
                    "out_of_state": "", "location": "Airline", "scraped_at": "now",
                }]
                scraper.merge_football_games(official)
            conn = sqlite3.connect(db_path)
            rows = conn.execute(
                "SELECT week, opponent, source FROM games ORDER BY week"
            ).fetchall()
            conn.close()
            self.assertEqual(rows[0], ("Week 1", "Wossman", "LHSAA"))
            self.assertEqual(
                rows[1],
                ("Week 2", "Gap Filler", "Louisiana Sportsline preseason compilation"),
            )


if __name__ == "__main__":
    unittest.main()
