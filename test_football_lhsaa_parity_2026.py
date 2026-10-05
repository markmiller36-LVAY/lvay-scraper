"""Football parity fixes verified against LHSAA's live 2026 ratings (Sep 28)."""

from power_rating_engine import (
    GameResult,
    PowerRatingEngine,
    Team,
    round_half_up,
)
from run_power_rankings import lookup_school_record, normalize_result
from school_database import get_school, resolve_school_spelling


def _rate(team, games):
    engine = PowerRatingEngine()
    engine.add_team(team)
    for game in games:
        engine.add_game(game)
    return engine.rate_team(team.name)


def _game(team, opp, result, wins, losses, division, klass, week):
    return GameResult(
        team=team, opponent=opp, result=result, sport="football",
        opponent_wins=wins, opponent_losses=losses,
        opponent_division=division, opponent_class=klass, week=week,
    )


def test_bonus_is_two_per_division_level_when_class_is_also_higher():
    # Vinton (2A, NS IV) beat Washington-Marion (3A, S II): one class up,
    # two divisions up. LHSAA awards +4 and lists Vinton at 14.75.
    vinton = Team("Vinton", "Non-Select Division IV", "2A", "football")
    rating = _rate(vinton, [
        _game("Vinton", "Oberlin", "W", 0, 4, "Non-Select Division IV", "1A", 1),
        _game("Vinton", "Elton", "W", 3, 1, "Non-Select Division IV", "1A", 2),
        _game("Vinton", "Washington-Marion", "W", 1, 3, "Select Division II", "3A", 3),
        _game("Vinton", "Grand Lake", "W", 2, 2, "Non-Select Division IV", "1A", 4),
    ])
    wm = [b for b in rating.breakdown if b["opponent"] == "Washington-Marion"][0]
    assert wm["div"] == 4
    assert rating.power_rating == 14.75


def test_no_bonus_when_only_division_is_higher():
    team = Team("Test", "Non-Select Division IV", "3A", "football")
    rating = _rate(team, [
        _game("Test", "Opp", "W", 0, 4, "Select Division II", "3A", 1),
    ])
    assert rating.breakdown[0]["div"] == 0


def test_half_cents_round_up_like_lhsaa():
    assert round_half_up(13.125) == 13.13
    assert round_half_up(10.625) == 10.63
    assert round_half_up(8.624999999999) == 8.63
    assert round_half_up(12.874) == 12.87


def test_football_double_forfeit_is_a_loss_for_both_teams():
    assert normalize_result("L(df)", "football") == "L"
    # Other sports keep their current behavior until verified.
    assert normalize_result("L(df)", "baseball") == ""
    assert normalize_result("W(f)", "football") == "W"


def test_double_forfeit_counts_as_game_with_oppq_and_bonus():
    # Jefferson Rise (3A, S III) vs Kenner Discovery (4A, S II), double
    # forfeit: LHSAA gives Jefferson Rise 0 win pts + 2 bonus + 0 OppQ,
    # and it counts as one of four games (LHSAA PR 6.38).
    jr = Team("Jefferson Rise Charter", "Select Division III", "3A", "football")
    rating = _rate(jr, [
        _game("Jefferson Rise Charter", "Riverdale", "L", 2, 2, "Select Division I", "5A", 1),
        _game("Jefferson Rise Charter", "Kenner Discovery Health Science",
              normalize_result("L(df)", "football"), 0, 4, "Select Division II", "4A", 2),
        _game("Jefferson Rise Charter", "Abramson", "L", 3, 1, "Select Division II", "4A", 3),
        _game("Jefferson Rise Charter", "Istrouma", "L", 2, 2, "Select Division III", "3A", 4),
    ])
    assert rating.games_played == 4
    assert rating.record == "0-4"
    assert rating.power_rating == 6.38


def test_punctuation_differences_resolve_to_the_same_school():
    assert get_school("J.S. Clark Leadership Academy", "football", 2026)["name"] \
        == "JS Clark Leadership Academy"
    assert resolve_school_spelling("St Edmund") == "St. Edmund"


def test_acadiana_christian_uses_lhsaa_name():
    # LHSAA lists the school as "Acadiana Christian" (Mark confirmed Oct 4, 2026).
    school = get_school("Acadiana Christian", "football", 2026)
    assert school is not None and school["class"] == "1A"


def test_opponent_record_found_across_spellings():
    records = {"J.S. Clark Leadership Academy": {"wins": 3, "losses": 1, "ties": 0}}
    assert lookup_school_record(records, "JS Clark Leadership Academy")["wins"] == 3
    assert lookup_school_record(records, "Nobody High") is None


def test_alias_listed_in_season_alignment_uses_current_class():
    # 2026 alignment lists "Acadiana Renaissance Charter" (4A); our games
    # say "...Charter Academy". Must not fall back to the old 3A listing,
    # or Abbeville loses its division bonus (LHSAA PR 10.50).
    info = get_school("Acadiana Renaissance Charter Academy", "football", 2026)
    assert info["class"] == "4A"
    assert info["division"] == "Select Division II"


def test_evangel_huntington_is_non_district_in_2026():
    from district_exceptions import is_non_district_game
    assert is_non_district_game("football", "2026", "Huntington", "Evangel Christian")
    assert is_non_district_game("football", 2026, "Evangel Christian", "Huntington")
    assert not is_non_district_game("football", "2025", "Huntington", "Evangel Christian")
    assert not is_non_district_game("football", "2026", "Huntington", "Captain Shreve")
