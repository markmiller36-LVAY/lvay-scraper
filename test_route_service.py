import os
import tempfile
import unittest
from unittest import mock

os.environ.setdefault("DB_PATH", os.path.join(tempfile.mkdtemp(), "t.db"))

import route_service  # noqa: E402
from server import app  # noqa: E402

OSRM_OK = {
    "code": "Ok",
    "routes": [{
        "distance": 160934.4, "duration": 7200,
        "geometry": {"coordinates": [[-93.7, 32.5], [-93.0, 31.0],
                                     [-93.4, 30.2]]},
        "legs": [{"steps": [
            {"distance": 3000, "ref": "LA 3227"},
            {"distance": 100000, "ref": "I 49"},
            {"distance": 1500, "ref": "US 71"},
            {"distance": 50000, "ref": "I 10;US 90"},
            {"distance": 4000, "ref": ""},
        ]}],
    }],
}


class FakeResp:
    def __init__(self, data):
        self._d = data

    def json(self):
        return self._d


class RouteTests(unittest.TestCase):
    def setUp(self):
        route_service.DB_PATH = os.path.join(tempfile.mkdtemp(), "r.db")
        self.c = app.test_client()

    def test_summary_keeps_long_highways_in_order(self):
        steps = OSRM_OK["routes"][0]["legs"][0]["steps"]
        self.assertEqual(route_service.highway_summary(steps), ["I-49", "I-10"])

    def test_route_is_fetched_once_then_cached(self):
        with mock.patch.object(route_service.requests, "get",
                               return_value=FakeResp(OSRM_OK)) as g:
            r1 = self.c.get("/api/route?a=32.5,-93.7&b=30.2,-93.4").get_json()
            r2 = self.c.get("/api/route?a=32.5,-93.7&b=30.2,-93.4").get_json()
        self.assertEqual(g.call_count, 1)
        self.assertEqual(r1["mi"], 100.0)
        self.assertEqual(r1["via"], ["I-49", "I-10"])
        self.assertEqual(r1["pts"][0], [32.5, -93.7])
        self.assertEqual(r2["mi"], 100.0)

    def test_rejects_points_outside_louisiana(self):
        r = self.c.get("/api/route?a=40.7,-74.0&b=30.2,-93.4")
        self.assertEqual(r.status_code, 400)

    def test_routing_failure_is_not_cached(self):
        with mock.patch.object(route_service.requests, "get",
                               return_value=FakeResp({"code": "NoRoute"})):
            r = self.c.get("/api/route?a=31.0,-92.0&b=30.0,-91.0")
        self.assertEqual(r.status_code, 502)
        self.assertIsNone(route_service._cache_get(
            route_service._cache_key((31.0, -92.0), (30.0, -91.0))))


if __name__ == "__main__":
    unittest.main()
