from power_rating_engine import GameResult, PowerRatingEngine, Team, round_half_up


def test_round_half_up():
    assert round_half_up(6.665) == 6.67
    assert round_half_up(2 / 3 * 10) == 6.67
    assert round_half_up(13.125) == 13.13


def test_opponent_quality_is_rounded_per_game():
    # Opponents at 1-6, 2-4, 4-2 -> 1.43 + 3.33 + 6.67 (not 1.428.. + 3.333.. + 6.666..)
    div = "Non-Select Division III"
    team = Team("A", div, "3A", "football")
    engine = PowerRatingEngine()
    engine.add_team(team)
    for week, (opp, w, l) in enumerate((("B", 1, 6), ("C", 2, 4), ("D", 4, 2)), 1):
        engine.add_game(GameResult(team="A", opponent=opp, result="L", sport="football",
                                   opponent_wins=w, opponent_losses=l,
                                   opponent_division=div, opponent_class="3A", week=week))
    rating = engine.rate_team("A")
    assert [g["oppq"] for g in rating.breakdown] == [1.43, 3.33, 6.67]
    assert rating.power_rating == 3.81  # 11.43 / 3 = 3.81
