import json
from pathlib import Path

import server
from oos_class_corrections import oos_class_correction


def test_roster_corrections_match_official_alignment():
    official = json.loads(Path("sport_alignments_2026_2027.json").read_text())
    football = official["sports"]["football"]
    assert football["Carencro"]["division"] == "Select Division II"
    assert football["Hanson Memorial"]["division"] == "Select Division IV"
    assert str(football["Baker"]["district"]) == "6"

    carencro = server.football_roster_correction(2026, "carencro")
    assert carencro["division"] == "Select Division 2"
    assert carencro["track"] == "Select"
    assert carencro["district_class"] == "S2"
    hanson = server.football_roster_correction(2026, "Hanson Memorial")
    assert hanson["division"] == "Select Division 4"
    assert server.football_roster_correction(2026, "Baker")["district"] == "6"


def test_no_correction_for_other_schools_or_seasons():
    assert server.football_roster_correction(2026, "Opelousas") is None
    assert server.football_roster_correction(2025, "Carencro") is None
    assert server.football_roster_correction(None, "Carencro") is None


def test_preseason_roster_file_is_corrected():
    roster = json.loads(Path("football_2026_preseason.json").read_text())
    by_name = {s["school"]: s for s in roster["schools"]}
    assert by_name["Carencro"]["division"] == "Select Division 2"
    assert by_name["Hanson Memorial"]["division"] == "Select Division 4"
    assert by_name["Baker"]["district"] == 6


def test_oos_class_locks():
    assert oos_class_correction(2026, "St. John's School, TX")["class"] == "3A"
    assert oos_class_correction("2026", "St. John’s School")["class"] == "3A"
    assert oos_class_correction(2026, "Santa Fe Christian School, CA")["class"] == "2A"
    assert oos_class_correction(2026, "Vidor High School, TX")["class"] == "4A"
    assert oos_class_correction(2026, "Magnolia High School, AR")["class"] == "3A"
    # Louisiana's St. John and other names must not match.
    assert oos_class_correction(2026, "St. John") is None
    assert oos_class_correction(2026, "Magnolia School of Excellence") is None
    assert oos_class_correction(2025, "Vidor High School, TX") is None
