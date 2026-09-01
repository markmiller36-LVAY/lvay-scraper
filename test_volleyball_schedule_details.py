from server import annotate_volleyball_games


def test_volleyball_schedule_adds_opponent_record_and_win_points():
    games = [{"opponent": "West Ouachita", "result": "W", "counts_for_pr": 1}]
    records = {
        "west ouachita": {"wins": 4, "losses": 2, "games_played": 6}
    }

    annotated = annotate_volleyball_games(games, records)

    assert annotated[0]["opp_record"] == "4-2"
    assert annotated[0]["opp_wins"] == 4
    assert annotated[0]["opp_losses"] == 2
    assert annotated[0]["power_points"] == 9.0
    assert annotated[0]["total_pts"] == 9.0


def test_volleyball_schedule_uses_exact_one_third_for_loss_points():
    games = [{"opponent": "West Ouachita", "result": "L", "counts_for_pr": 1}]
    records = {
        "west ouachita": {"wins": 4, "losses": 2, "games_played": 6}
    }

    annotated = annotate_volleyball_games(games, records)

    assert annotated[0]["opp_record"] == "4-2"
    assert annotated[0]["power_points"] == 1.333


def test_volleyball_schedule_does_not_score_unplayed_or_excluded_match():
    games = [
        {"opponent": "West Ouachita", "result": "", "counts_for_pr": 1},
        {"opponent": "OUT OF STATE", "result": "W", "counts_for_pr": 0},
    ]
    records = {
        "west ouachita": {"wins": 4, "losses": 2, "games_played": 6}
    }

    annotated = annotate_volleyball_games(games, records)

    assert annotated[0]["power_points"] is None
    assert annotated[1]["opp_record"] == ""
    assert annotated[1]["power_points"] is None
