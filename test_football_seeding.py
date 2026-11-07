from football_seeding import build_game_index, seed_division


def _t(school, pr, sf, cls="5A"):
    return {"school": school, "power_rating": pr, "strength_factor": sf, "class_": cls}


def test_two_way_tie_uses_head_to_head_before_strength_factor():
    # Nov 7, 2026: Sulphur beat Sam Houston 55-54; both 9.90.
    teams = [_t("Sam Houston", 9.90, 10.8), _t("Sulphur", 9.90, 9.5), _t("East St. John", 9.20, 9.0)]
    games = build_game_index([
        {"school": "Sulphur", "opponent": "Sam Houston", "result": "W", "is_district": 1},
        {"school": "Sam Houston", "opponent": "Sulphur", "result": "L", "is_district": 1},
    ])
    order = [t["school"] for t in seed_division(teams, games, {})]
    assert order == ["Sulphur", "Sam Houston", "East St. John"]


def test_two_way_tie_without_meeting_uses_strength_factor():
    teams = [_t("A", 10.0, 8.0), _t("B", 10.0, 9.0)]
    assert [t["school"] for t in seed_division(teams, build_game_index([]), {})] == ["B", "A"]


def test_three_way_tie_uses_strength_factor_first_even_if_head_to_head():
    teams = [_t("A", 10.0, 8.0), _t("B", 10.0, 9.0), _t("C", 10.0, 7.0)]
    games = build_game_index([{"school": "A", "opponent": "B", "result": "W"}])
    assert [t["school"] for t in seed_division(teams, games, {})] == ["B", "A", "C"]


def test_three_way_tie_head_to_head_breaks_equal_strength_factor():
    teams = [_t("A", 10.0, 8.0), _t("B", 10.0, 8.0), _t("C", 10.0, 9.0)]
    games = build_game_index([{"school": "B", "opponent": "A", "result": "W"}])
    assert [t["school"] for t in seed_division(teams, games, {})] == ["C", "B", "A"]


def test_forfeit_win_counts_as_head_to_head_win():
    teams = [_t("A", 10.0, 9.0), _t("B", 10.0, 8.0)]
    games = build_game_index([{"school": "B", "opponent": "A", "result": "W(f)"}])
    assert [t["school"] for t in seed_division(teams, games, {})] == ["B", "A"]


def test_engine_overall_rank_uses_head_to_head(monkeypatch):
    import run_power_rankings as rpr

    class R:
        def __init__(self, name, pr, opp, result):
            self.name, self.power_rating, self.division = name, pr, "Non-Select Division I"
            self.breakdown = [{"opponent": opp, "result": result, "week": 4}]

    info = {"Sulphur": {"class": "5A", "district": 3, "division": "Non-Select Division I"},
            "Sam Houston": {"class": "5A", "district": 3, "division": "Non-Select Division I"}}
    monkeypatch.setattr(rpr, "get_school", lambda n, sp, se: info.get(n))
    ratings = [R("Sam Houston", 9.9, "Sulphur", "L"), R("Sulphur", 9.9, "Sam Houston", "W")]
    rpr._apply_football_seed_tiebreakers(
        ratings, {"Sam Houston": 10.8, "Sulphur": 9.5}, "football", "2026")
    assert [r.name for r in ratings] == ["Sulphur", "Sam Houston"]


def test_tiebreak_labels():
    from football_seeding import tiebreak_labels
    games = build_game_index([{"school": "Sulphur", "opponent": "Sam Houston", "result": "W"}])
    teams = [_t("Sulphur", 9.9, 9.5), _t("Sam Houston", 9.9, 10.8), _t("Oakdale", 9.47, 7.0),
             _t("Rayville", 9.47, 6.3), _t("Solo", 12.0, 5.0)]
    assert tiebreak_labels(teams, games) == {
        "Sulphur": "h2h", "Sam Houston": "h2h", "Oakdale": "sf", "Rayville": "sf"}
