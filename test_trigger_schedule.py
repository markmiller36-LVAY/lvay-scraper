from datetime import datetime
from zoneinfo import ZoneInfo

from trigger_pipeline import should_trigger_now, sports_for_now

CT = ZoneInfo("America/Chicago")
FB = ["football", "volleyball", "baseball", "softball"]
BB = ["boys_basketball", "girls_basketball", "boys_soccer", "girls_soccer"]


def at(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=CT)


def test_football_volleyball_every_4_hours_from_5am():
    for hour in (1, 5, 9, 13, 17, 21):
        assert sports_for_now(at(2026, 10, 20, hour, 0)) == FB


def test_basketball_every_4_hours_from_6am():
    for hour in (2, 6, 10, 14, 18, 22):
        assert sports_for_now(at(2026, 10, 20, hour, 0)) == BB


def test_slot_fires_when_render_starts_late():
    assert sports_for_now(at(2026, 10, 20, 5, 9)) == FB
    assert not should_trigger_now(at(2026, 10, 20, 5, 30))
    assert not should_trigger_now(at(2026, 10, 20, 5, 10))


def test_other_hours_stay_quiet():
    for hour in (0, 3, 4, 7, 8, 11, 12, 15, 16, 19, 20, 23):
        assert not should_trigger_now(at(2026, 10, 16, hour, 0))  # Friday


def test_no_basketball_slot_out_of_season():
    assert not should_trigger_now(at(2026, 7, 10, 6, 0))
    assert sports_for_now(at(2026, 7, 10, 5, 0)) == FB
