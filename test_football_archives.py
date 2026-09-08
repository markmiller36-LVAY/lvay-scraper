from server import football_archive_rankings_response, football_archive_response


def test_archived_seasons_are_available():
    expected = {
        "2015": 282,
        "2016": 281,
        "2017": 282,
        "2018": 283,
        "2019": 288,
        "2020": 285,
        "2021": 291,
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


def test_legacy_archives_have_no_blank_districts():
    for season in map(str, range(2015, 2022)):
        response = football_archive_response(season, summary_only=True)
        assert all(school["district"] for school in response["schools"])


def test_comeaux_2016_schedule_is_reconstructed():
    response = football_archive_response("2016", school_filter="Comeaux")
    school = response["schools"][0]
    assert school["district"] == "3"
    assert school["record"] == "5-6"
    assert len(school["games"]) == 11


def test_2021_districts_do_not_duplicate_class_suffix():
    response = football_archive_response("2021", summary_only=True)
    assert all(not school["district"].endswith(f"-{school['class_']}")
               for school in response["schools"])
