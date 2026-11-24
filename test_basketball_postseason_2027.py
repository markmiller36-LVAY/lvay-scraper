"""2026-27 basketball divisions follow the LHSAA post-season PDFs."""
import json
from pathlib import Path

import school_database as sd

DATA = json.loads(
    Path(__file__).with_name("winter_postseason_2026_2027.json").read_text()
)


def test_every_listed_school_matches_postseason_pdf():
    for sport in ("boys_basketball", "girls_basketball"):
        for school, label in DATA["sports"][sport].items():
            info = sd.get_school(school, sport, 2027)
            assert info["division"] == label, (sport, school)
            expected = "non-select" if label.startswith("Non-Select") else "select"
            assert info["track"] == expected, (sport, school)


def test_previously_wrong_schools():
    cases = {
        "Ponchatoula": "Non-Select Division I",
        "Leesville": "Non-Select Division II",
        "Pickering": "Non-Select Division IV",
        "Bell City": "Non-Select Division IV",
        "Baker": "Select Division III",
        "Glenmora": "Select Division IV",
        "False River": "Select Division IV",
        "GEO Next Generation": "Select Division II",
    }
    for sport in ("boys_basketball", "girls_basketball"):
        for school, label in cases.items():
            assert sd.get_school(school, sport, 2027)["division"] == label


def test_baker_district_six():
    for sport in ("boys_basketball", "girls_basketball"):
        assert sd.get_school("Baker", sport, 2027)["district"] == 6


def test_other_sports_and_seasons_untouched():
    assert sd.get_school("Ponchatoula", "football", 2026)["division"] == (
        "Non-Select Division I"
    )
    # Class B/C schools are not in the post-season PDFs and keep their class.
    assert sd._postseason_designation("boys_basketball", 2026, ("Baker",)) is None
