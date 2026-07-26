from run_power_rankings import (
    exclude_unverified_soccer_oos,
    load_official_winter_rankings,
    reconcile_incomplete_winter_rating,
)
from power_rating_engine import TeamRating


def test_unverified_soccer_oos_is_excluded():
    assert exclude_unverified_soccer_oos("boys_soccer", True, None)
    assert exclude_unverified_soccer_oos("girls_soccer", True, None)
    assert not exclude_unverified_soccer_oos("boys_soccer", False, None)
    assert not exclude_unverified_soccer_oos(
        "boys_soccer", True, {"opp_wins": 10}
    )
    assert not exclude_unverified_soccer_oos("football", True, None)


def test_official_boys_soccer_fallback_contains_removed_schedules():
    official = load_official_winter_rankings("boys_soccer", "2026")
    assert official["Comeaux"]["official_power_rating"] == 7.39
    assert official["Comeaux"]["official_record"] == "4-11-4"
    assert load_official_winter_rankings("boys_soccer", "2027") == {}


def test_incomplete_archived_schedule_uses_official_final_rating():
    rating = TeamRating(
        name="Sarah T. Reed",
        sport="boys_soccer",
        power_rating=9.71,
        wins=9,
        losses=8,
        ties=2,
        games_played=19,
    )
    changed = reconcile_incomplete_winter_rating(
        rating,
        {"official_power_rating": 8.31},
        (9, 10, 2),
    )
    assert changed
    assert rating.power_rating == 8.31


def test_complete_schedule_keeps_calculated_rating():
    rating = TeamRating(
        name="Jesuit",
        sport="boys_soccer",
        power_rating=17.84,
        wins=16,
        losses=0,
        ties=1,
        games_played=17,
    )
    changed = reconcile_incomplete_winter_rating(
        rating,
        {"official_power_rating": 17.84},
        (16, 0, 1),
    )
    assert not changed
    assert rating.power_rating == 17.84
