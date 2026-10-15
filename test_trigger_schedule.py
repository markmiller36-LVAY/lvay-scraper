from datetime import datetime
from zoneinfo import ZoneInfo

from trigger_pipeline import should_trigger_now

CT = ZoneInfo("America/Chicago")


def at(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=CT)


def test_regular_slot_fires_when_render_starts_late():
    assert should_trigger_now(at(2026, 10, 10, 3, 1))   # Saturday 3:01am
    assert should_trigger_now(at(2026, 10, 10, 15, 9))


def test_off_slot_half_hours_stay_quiet():
    assert not should_trigger_now(at(2026, 10, 10, 3, 30))  # Saturday
    assert not should_trigger_now(at(2026, 10, 10, 3, 15))


def test_no_all_day_game_day_boost():
    assert not should_trigger_now(at(2026, 10, 9, 21, 31))  # Friday
    assert not should_trigger_now(at(2026, 10, 8, 18, 0))   # Thursday 6pm
    assert not should_trigger_now(at(2026, 10, 15, 4, 30))  # Thursday 4:30am
    assert should_trigger_now(at(2026, 10, 15, 7, 2))       # regular 7am


def test_midnight_after_game_night():
    assert should_trigger_now(at(2026, 10, 10, 0, 2))   # Sat 12:02am
    assert not should_trigger_now(at(2026, 10, 11, 0, 0))  # Sun midnight


def test_no_game_night_boost_out_of_season():
    assert not should_trigger_now(at(2026, 12, 4, 21, 30))  # Friday in Dec
    assert should_trigger_now(at(2026, 12, 4, 19, 0))       # regular 7pm


def test_alternating_sports_by_slot():
    from trigger_pipeline import sports_for_now
    assert sports_for_now(at(2026, 10, 20, 3, 1)) is None    # full
    assert sports_for_now(at(2026, 10, 20, 11, 0)) is None   # full
    assert sports_for_now(at(2026, 10, 20, 19, 4)) is None   # full
    assert sports_for_now(at(2026, 10, 20, 7, 0)) == ["football", "volleyball"]
    assert sports_for_now(at(2026, 10, 20, 15, 2)) == ["football", "volleyball"]
    assert sports_for_now(at(2026, 10, 20, 23, 0)) == ["football", "volleyball"]
    assert sports_for_now(at(2026, 10, 17, 0, 1)) == ["football", "volleyball"]
