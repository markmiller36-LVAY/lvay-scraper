from datetime import datetime
from unittest import mock

import scheduled_tasks


def active_on(y, m, d):
    with mock.patch.object(scheduled_tasks, "datetime") as dt:
        dt.now.return_value = datetime(y, m, d, 7, 0)
        return scheduled_tasks.get_active_sports()


def test_basketball_waits_until_oct_19():
    assert "boys_basketball" not in active_on(2026, 10, 15)
    assert "girls_basketball" not in active_on(2026, 10, 18)
    assert "boys_basketball" in active_on(2026, 10, 19)
    assert "girls_basketball" in active_on(2026, 11, 2)
    assert "boys_basketball" in active_on(2027, 1, 10)


def test_football_and_soccer_unchanged_in_october():
    sports = active_on(2026, 10, 15)
    assert "football" in sports and "volleyball" in sports
    assert "boys_soccer" in sports and "girls_soccer" in sports
