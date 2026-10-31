import hashlib

import server


def test_wp_trigger_key_checks_hash(monkeypatch):
    monkeypatch.setattr(server, "WP_TRIGGER_KEY_SHA256", hashlib.sha256(b"right").hexdigest())
    with server.app.test_request_context("/api/pipeline/run", method="POST",
                                         headers={"X-LVAY-Trigger-Key": "right"}):
        assert server._wp_trigger_authorized()
    with server.app.test_request_context("/api/pipeline/run", method="POST",
                                         headers={"X-LVAY-Trigger-Key": "wrong"}):
        assert not server._wp_trigger_authorized()
    with server.app.test_request_context("/api/pipeline/run", method="POST"):
        assert not server._wp_trigger_authorized()


def test_wp_key_does_not_open_protected_feeds(monkeypatch):
    monkeypatch.setenv("PIPELINE_TOKEN", "real-token")
    client = server.app.test_client()
    r = client.post("/api/recalculate/football", headers={"X-LVAY-Trigger-Key": "anything"})
    assert r.status_code == 401
