from school_database import get_school


def same_district(school, opponent, sport="boys_soccer", season=2026):
    school_info = get_school(school, sport, season)
    opponent_info = get_school(opponent, sport, season)
    same_alignment_group = (
        school_info.get("division") == opponent_info.get("division")
    )
    return (
        same_alignment_group
        and school_info.get("district") == opponent_info.get("district")
    )


def test_evangel_boys_soccer_district_uses_division_and_district():
    assert same_district("Evangel Christian", "Calvary Baptist")
    assert same_district("Evangel Christian", "North Caddo")
    assert same_district("Evangel Christian", "Providence Classical Academy")
    assert not same_district("Evangel Christian", "Airline")
    assert not same_district("Evangel Christian", "Haughton")
    assert not same_district("Evangel Christian", "Huntington")
