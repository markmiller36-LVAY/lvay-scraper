import gzip
import json

import server


def _run(path, accept="gzip", payload=None):
    payload = payload if payload is not None else {"rows": ["x" * 50] * 500}
    headers = {"Accept-Encoding": accept} if accept else {}
    with server.app.test_request_context(path, headers=headers):
        resp = server.jsonify(payload)
        return server.compress_and_cache_feeds(resp)


def test_large_json_feed_is_gzipped_with_cache():
    r = _run("/api/schedules/football")
    assert r.headers["Content-Encoding"] == "gzip"
    assert "Accept-Encoding" in r.headers["Vary"]
    assert "max-age=300" in r.headers["Cache-Control"]
    assert len(json.loads(gzip.decompress(r.get_data()))["rows"]) == 500


def test_no_gzip_without_accept_encoding():
    r = _run("/api/schedules/football", accept=None)
    assert "Content-Encoding" not in r.headers
    assert len(json.loads(r.get_data())["rows"]) == 500


def test_small_responses_untouched():
    r = _run("/api/rankings/football", payload={"ok": True})
    assert "Content-Encoding" not in r.headers


def test_non_feed_paths_not_cached():
    r = _run("/api/status/runs")
    assert "Cache-Control" not in r.headers
    assert r.headers["Content-Encoding"] == "gzip"
