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
        for moment in (
            datetime(2026, 9, 3, 5, 0, tzinfo=central),
            datetime(2026, 9, 3, 17, 0, tzinfo=central),
            datetime(2026, 9, 3, 23, 0, tzinfo=central),
        ):
            self.assertEqual(
                trigger_pipeline.sports_for_now(moment)[0], "football"
            )
        for moment in (
            datetime(2026, 9, 3, 14, 30, tzinfo=central),
            datetime(2026, 9, 8, 0, 0, tzinfo=central),  # Tuesday (Fri/Sat midnight is a game-night run)
        ):
            self.assertFalse(trigger_pipeline.should_trigger_now(moment))

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

    def test_boot_closes_runs_orphaned_by_a_restart(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "lvay.db")
            original_path = server.DB_PATH
            server.DB_PATH = db_path
            try:
                server.init_db()
                conn = sqlite3.connect(db_path)
                conn.execute(
                    "INSERT INTO pipeline_runs (started_at, status) "
                    "VALUES ('2026-10-13T19:01:21', 'running')"
                )
                conn.execute(
                    "INSERT INTO pipeline_runs (started_at, finished_at, status) "
                    "VALUES ('2026-10-13T23:00:57', '2026-10-13T23:21:15', 'completed')"
                )
                conn.commit()
                conn.close()
                server.init_db()  # simulate the next boot
            finally:
                server.DB_PATH = original_path
            conn = sqlite3.connect(db_path)
            rows = conn.execute(
                "SELECT status, finished_at, error FROM pipeline_runs ORDER BY id"
            ).fetchall()
            conn.close()
        self.assertEqual(rows[0][0], "interrupted")
        self.assertIsNotNone(rows[0][1])
        self.assertIn("restarted", rows[0][2])
        self.assertEqual(rows[1], ("completed", "2026-10-13T23:21:15", None))


if __name__ == "__main__":
    unittest.main()
