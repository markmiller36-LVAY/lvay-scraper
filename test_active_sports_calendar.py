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


def test_scraper_basketball_window_starts_oct_19():
    from datetime import datetime as real_dt
    from scraper import should_scrape_sport
    assert not should_scrape_sport("boys_basketball", real_dt(2026, 10, 15, 7))[0]
    assert should_scrape_sport("girls_basketball", real_dt(2026, 10, 19, 7))[0]
    assert should_scrape_sport("boys_basketball", real_dt(2027, 2, 1, 7))[0]


def test_only_subset_skips_other_sports():
    import scraper
    called = []
    with mock.patch.object(scraper, "init_db"), \
         mock.patch.object(scraper, "log_scrape"), \
         mock.patch.object(scraper, "scrape_sport", side_effect=lambda k: called.append(k) or 0):
        scraper.run_all_sports(only=["football"])
    assert called == ["football"]
