import json
from datetime import datetime
from zoneinfo import ZoneInfo

import media_mailer

CT = ZoneInfo("America/Chicago")
ROWS = [
    {"school": "Ruston", "division": "Non-Select Division I", "rank": 4, "pr": 14.25,
     "calculated_at": "2026-10-17T05:02:00"},
    {"school": "Neville", "division": "Non-Select Division II", "rank": 1, "pr": 16.67,
     "calculated_at": "2026-10-17T05:02:00"},
    {"school": "Catholic - B.R.", "division": "Select Division I", "rank": 12, "pr": 11.0,
     "calculated_at": "2026-10-17T05:02:00"},
]
REGIONS = {"north": {"title": "North Louisiana", "schools": ["Ruston", "Neville", "Tioga"]}}


def test_media_slot_is_saturday_after_5am_football_run():
    assert media_mailer.is_media_slot(datetime(2026, 10, 24, 5, 40, tzinfo=CT), ["football"])
    assert not media_mailer.is_media_slot(datetime(2026, 10, 24, 0, 5, tzinfo=CT), ["football"])
    assert not media_mailer.is_media_slot(datetime(2026, 10, 23, 5, 40, tzinfo=CT), ["football"])
    assert not media_mailer.is_media_slot(datetime(2026, 10, 24, 5, 40, tzinfo=CT), ["volleyball"])


def test_region_edition_keeps_only_local_teams_and_flags_missing():
    edition = media_mailer.build_edition("football", ROWS, 2026, {"region": "north"}, REGIONS)
    names = [t["school"] for _, teams in edition["blocks"] for t in teams]
    assert names == ["Ruston", "Neville"]
    assert edition["missing"] == ["Tioga"]
    assert edition["area"] == "NORTH LOUISIANA"
    assert edition["updated"].hour == 5


def test_top10_edition_and_png(monkeypatch):
    monkeypatch.setattr(media_mailer, "_logo", lambda: None)
    edition = media_mailer.build_edition("football", ROWS, 2026, {"top10": True}, REGIONS)
    assert [d for d, _ in edition["blocks"]] == ["Non-Select Division I", "Non-Select Division II"]
    assert media_mailer.render_png(edition, scale=1).startswith(b"\x89PNG")


def test_preview_mode_never_emails_the_outlet(tmp_path, monkeypatch):
    media = tmp_path / "media.json"
    media.write_text(json.dumps({"regions": REGIONS, "recipients": [
        {"id": "k104", "name": "K104", "email": "fns@example.com", "active": True,
         "editions": [{"sport": "football", "region": "north"}]},
        {"id": "off", "name": "Off", "email": "off@example.com", "active": False,
         "editions": [{"sport": "football", "top10": True}]},
    ]}))
    sent = []
    monkeypatch.setattr(media_mailer, "MEDIA_LIST_PATH", media)
    monkeypatch.setattr(media_mailer, "DB_PATH", str(tmp_path / "t.db"))
    monkeypatch.setattr(media_mailer, "load_rankings", lambda sport: (2026, ROWS))
    monkeypatch.setattr(media_mailer, "_send", lambda to, *a: sent.append(to))
    monkeypatch.setattr(media_mailer, "_logo", lambda: None)
    monkeypatch.setenv("REPORT_EMAIL_TO", "mark@example.com,other@example.com")

    monkeypatch.setenv("MEDIA_EMAIL_LIVE", "false")
    media_mailer.send_media_emails(now=datetime(2026, 10, 24, 5, 30, tzinfo=CT))
    assert sent == [["mark@example.com"]]

    sent.clear()
    monkeypatch.setenv("MEDIA_EMAIL_LIVE", "true")
    media_mailer.send_media_emails(now=datetime(2026, 10, 24, 5, 30, tzinfo=CT))
    media_mailer.send_media_emails(now=datetime(2026, 10, 24, 6, 30, tzinfo=CT))
    assert sent == [["fns@example.com"]]  # once per Saturday, inactive skipped


def test_logo_falls_back_to_bundled_copy(tmp_path, monkeypatch):
    monkeypatch.setattr(media_mailer, "_LOGO", None)
    monkeypatch.setattr(media_mailer, "LOGO_URL", "https://invalid.invalid/logo.png")
    monkeypatch.setenv("MEDIA_CACHE_DIR", str(tmp_path))
    logo = media_mailer._logo()
    assert logo is not None and logo.mode == "L" and logo.width > 300
