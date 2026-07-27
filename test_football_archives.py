from server import football_archive_response


def test_archived_seasons_are_available():
    expected = {"2022": 292, "2023": 299, "2024": 303}
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
