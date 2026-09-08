from server import football_archive_rankings_response, football_archive_response


def test_archived_seasons_are_available():
    expected = {
        "2015": 285,
        "2016": 282,
        "2017": 283,
        "2018": 284,
        "2019": 289,
        "2020": 297,
        "2021": 294,
        "2022": 292,
        "2023": 299,
        "2024": 303,
    }
    for season, school_count in expected.items():
        response = football_archive_response(season, summary_only=True)
        assert response["count"] == school_count
        assert response["status"] == "final"
        assert all(not school["games"] for school in response["schools"])


def test_archived_school_lookup_preserves_games():
    response = football_archive_response(
        "2024", school_filter="A.J. Ellender"
    )
    assert response["count"] == 1
    assert response["schools"][0]["games"]


def test_incomplete_seasons_are_not_published():
    assert football_archive_response("2008") is None
    assert football_archive_response("2014") is None


def test_legacy_archive_ratings_use_historical_groups():
    response = football_archive_rankings_response("2015")
    assert response["count"] > 250
    groups = {row["division"] for row in response["rankings"]}
    assert "Class 5A" in groups
    assert "Division I" in groups
