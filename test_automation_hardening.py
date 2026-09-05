import os
import sqlite3
import tempfile
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo
from unittest.mock import Mock, patch

import scheduled_tasks
import scraper
import server
import trigger_pipeline


class AutomationHardeningTests(unittest.TestCase):
    def test_football_peak_schedule_uses_central_time(self):
        central = ZoneInfo("America/Chicago")
        expected = [
            datetime(2026, 9, 3, 6, 0, tzinfo=central),   # Thursday morning
            datetime(2026, 9, 3, 14, 30, tzinfo=central), # Thursday afternoon
            datetime(2026, 9, 3, 22, 30, tzinfo=central), # Thursday game night
            datetime(2026, 9, 4, 0, 0, tzinfo=central),   # after Thursday
            datetime(2026, 9, 4, 17, 30, tzinfo=central), # Friday afternoon
            datetime(2026, 9, 4, 9, 0, tzinfo=central),   # Friday morning
            datetime(2026, 9, 5, 0, 0, tzinfo=central),   # after Friday
        ]
        for moment in expected:
            self.assertTrue(trigger_pipeline.should_trigger_now(moment))
        self.assertFalse(
            trigger_pipeline.should_trigger_now(
                datetime(2026, 9, 2, 22, 30, tzinfo=central)
            )
        )
        self.assertFalse(
            trigger_pipeline.should_trigger_now(
                datetime(2026, 12, 3, 22, 30, tzinfo=central)
            )
        )

    def test_export_failure_fails_scheduled_run(self):
        with self.assertRaisesRegex(RuntimeError, "Sheets export"):
            scheduled_tasks.require_success(False, "Sheets export")

    def test_complete_class_fetch_failure_is_not_logged_as_success(self):
        with (
            patch.object(scraper, "resolve_season_year", return_value="2026"),
            patch.object(scraper, "fetch_page", return_value=None),
            patch.object(scraper, "log_scrape") as log_scrape,
        ):
            with self.assertRaisesRegex(RuntimeError, "failed classes"):
                scraper.scrape_class_loop_sport("boys_soccer")
        self.assertEqual(log_scrape.call_args.args[2], "error")

    def test_cron_waits_for_pipeline_completion(self):
        trigger_response = Mock(status_code=202, text='{"status":"started"}')
        trigger_response.json.return_value = {
            "status": "started",
            "started_at": "2026-07-30T10:00:00",
        }
        running_response = Mock()
        running_response.raise_for_status.return_value = None
        running_response.json.return_value = {
            "status": "running",
            "started_at": "2026-07-30T10:00:00",
        }
        completed_response = Mock()
        completed_response.raise_for_status.return_value = None
        completed_response.json.return_value = {
            "status": "completed",
            "started_at": "2026-07-30T10:00:00",
        }
        with (
            patch.object(trigger_pipeline, "PIPELINE_TOKEN", "test-token"),
            patch.object(trigger_pipeline.requests, "post",
                         return_value=trigger_response),
            patch.object(
                trigger_pipeline.requests,
                "get",
                side_effect=[running_response, completed_response],
            ) as get_status,
            patch.object(trigger_pipeline.time, "sleep"),
        ):
            self.assertEqual(trigger_pipeline.main(), 0)
        self.assertEqual(get_status.call_count, 2)

    def test_fresh_database_supports_scraper_and_run_history(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "lvay.db")
            original_path = server.DB_PATH
            server.DB_PATH = db_path
            try:
                server.init_db()
            finally:
                server.DB_PATH = original_path

            conn = sqlite3.connect(db_path)
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(games)")
            }
            log_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(scrape_log)")
            }
            tables = {
                row[0] for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            game_indexes = conn.execute("PRAGMA index_list(games)").fetchall()
            unique_columns = set()
            for index in game_indexes:
                if index[2]:
                    unique_columns = {
                        row[2] for row in conn.execute(
                            f"PRAGMA index_info('{index[1]}')"
                        )
                    }
                    if {
                        "sport", "school", "game_date", "opponent", "season"
                    } == unique_columns:
                        break
            conn.close()

        self.assertIn("opponent_class", columns)
        self.assertIn("note", log_columns)
        self.assertIn("pipeline_runs", tables)
        self.assertEqual(
            unique_columns,
            {"sport", "school", "game_date", "opponent", "season"},
        )


if __name__ == "__main__":
    unittest.main()
