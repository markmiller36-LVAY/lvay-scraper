from datetime import datetime
from zoneinfo import ZoneInfo

from trigger_pipeline import should_trigger_now, sports_for_now

CT = ZoneInfo("America/Chicago")
FB = ["football", "volleyball", "baseball", "softball"]
BB = ["boys_basketball", "girls_basketball"]
SC = ["boys_soccer", "girls_soccer"]


def at(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=CT)


def test_football_volleyball_every_6_hours_from_5am():
    for hour in (5, 11, 17, 23):
        assert sports_for_now(at(2026, 10, 20, hour, 0)) == FB


def test_basketball_every_6_hours_from_6am():
    for hour in (0, 6, 12, 18):
        assert sports_for_now(at(2026, 10, 20, hour, 0)) == BB


def test_soccer_every_6_hours_from_7am():
    for hour in (1, 7, 13, 19):
        assert sports_for_now(at(2026, 10, 20, hour, 0)) == SC


def test_basketball_slots_wait_until_oct_19():
    assert not should_trigger_now(at(2026, 10, 15, 6, 0))
    assert sports_for_now(at(2026, 10, 19, 6, 0)) == BB


def test_slot_fires_when_render_starts_late():
    assert sports_for_now(at(2026, 10, 20, 5, 9)) == FB
    assert not should_trigger_now(at(2026, 10, 20, 5, 30))
    assert not should_trigger_now(at(2026, 10, 20, 5, 10))


def test_other_hours_stay_quiet():
    for hour in (2, 3, 4, 8, 9, 10, 14, 15, 16, 20, 21, 22):
        assert not should_trigger_now(at(2026, 10, 20, hour, 0))  # Tuesday


def test_game_nights_run_football_at_10pm_midnight_and_1am():
    # Thursday Oct 15 and Friday Oct 16, 2026
    for day in (15, 16):
        assert sports_for_now(at(2026, 10, day, 22, 0)) == ["football"]
        assert sports_for_now(at(2026, 10, day, 23, 0)) == FB
    # The midnight/1am runs after those nights fall on Friday and Saturday
    for day in (16, 17):
        assert sports_for_now(at(2026, 10, day, 0, 0)) == ["football"]
        assert sports_for_now(at(2026, 10, day, 1, 5)) == ["football"]
    # Not on other nights, and not at :30
    assert sports_for_now(at(2026, 10, 25, 0, 0)) == BB  # Sunday midnight
    assert not should_trigger_now(at(2026, 10, 14, 22, 0))  # Wednesday
    assert not should_trigger_now(at(2026, 10, 16, 22, 30))
    # Not outside football season
    assert not should_trigger_now(at(2027, 3, 4, 22, 0))


def test_no_basketball_slot_out_of_season():
    assert not should_trigger_now(at(2026, 7, 10, 6, 0))
    assert sports_for_now(at(2026, 7, 10, 5, 0)) == FB
