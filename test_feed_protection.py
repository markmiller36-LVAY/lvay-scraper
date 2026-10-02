import os
import unittest

os.environ.setdefault("DB_PATH", "/tmp/lvay_feed_protection_test.db")

import server


class FeedProtectionTests(unittest.TestCase):
    def setUp(self):
        self.client = server.app.test_client()
        server._RATE_BUCKETS.clear()
        self._old_token = os.environ.get("PIPELINE_TOKEN")
        os.environ["PIPELINE_TOKEN"] = "secret-token"

    def tearDown(self):
        if self._old_token is None:
            os.environ.pop("PIPELINE_TOKEN", None)
        else:
            os.environ["PIPELINE_TOKEN"] = self._old_token
        server._RATE_BUCKETS.clear()

    def test_work_endpoints_require_token(self):
        for path in (
            "/api/scrape/football",
            "/api/build/football-sheets",
            "/api/fix/oberlin-bolton",
            "/api/import/oos2025",
            "/api/scrape/winter/boys_basketball",
        ):
            self.assertEqual(self.client.get(path).status_code, 401, path)

    def test_wrong_token_rejected(self):
        response = self.client.get("/api/fix/oberlin-bolton?key=nope")
        self.assertEqual(response.status_code, 401)

    def test_token_lets_request_through_protection(self):
        response = self.client.get(
            "/api/fix/does-not-exist", headers={"X-Pipeline-Token": "secret-token"}
        )
        self.assertEqual(response.status_code, 404)
        response = self.client.get("/api/fix/does-not-exist?key=secret-token")
        self.assertEqual(response.status_code, 404)

    def test_missing_server_token_fails_closed(self):
        os.environ.pop("PIPELINE_TOKEN", None)
        self.assertEqual(self.client.get("/api/scrape/football").status_code, 503)

    def test_cors_allows_lvay_and_blocks_others(self):
        ok = self.client.get(
            "/api/health", headers={"Origin": "https://louisianavsallyall.com"}
        )
        self.assertEqual(
            ok.headers.get("Access-Control-Allow-Origin"),
            "https://louisianavsallyall.com",
        )
        staging = self.client.get(
            "/api/health", headers={"Origin": "https://lvay-staging.wpcomstaging.com"}
        )
        self.assertEqual(
            staging.headers.get("Access-Control-Allow-Origin"),
            "https://lvay-staging.wpcomstaging.com",
        )
        bad = self.client.get(
            "/api/health", headers={"Origin": "https://copycat-sports.com"}
        )
        self.assertIsNone(bad.headers.get("Access-Control-Allow-Origin"))

    def test_public_reads_are_rate_limited_per_visitor(self):
        old_limit = server.PUBLIC_RATE_LIMIT
        server.PUBLIC_RATE_LIMIT = 3
        try:
            headers = {"X-Forwarded-For": "203.0.113.9"}
            codes = [
                self.client.get("/api/seasons/nope", headers=headers).status_code
                for _ in range(4)
            ]
            self.assertNotEqual(codes[2], 429)
            self.assertEqual(codes[3], 429)
            other = self.client.get(
                "/api/seasons/nope", headers={"X-Forwarded-For": "198.51.100.4"}
            )
            self.assertNotEqual(other.status_code, 429)
            trusted = self.client.get(
                "/api/seasons/nope",
                headers={**headers, "X-Pipeline-Token": "secret-token"},
            )
            self.assertNotEqual(trusted.status_code, 429)
            self.assertNotEqual(
                self.client.get("/api/health", headers=headers).status_code, 429
            )
        finally:
            server.PUBLIC_RATE_LIMIT = old_limit

    def test_api_responses_are_noindex(self):
        response = self.client.get("/api/health")
        self.assertIn("noindex", response.headers.get("X-Robots-Tag", ""))


if __name__ == "__main__":
    unittest.main()
