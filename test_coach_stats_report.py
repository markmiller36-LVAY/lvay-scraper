from coach_stats_report import coach_stats_section, coach_stats_subject


def _sample():
    return {
        "totals": {"teams": 2, "boys": 1, "girls": 1, "updated_24h": 1, "players": 9},
        "teams": [
            {"sport": "Boys Soccer", "school": "Test High", "coach": "Test Coach", "role": "Head Coach",
             "games_with_stats": 2, "games_played": 3, "players": 4, "goals": 2, "assists": 2,
             "maxpreps": True, "photos": 1, "updated_ct": "Nov 22 1:38 am", "new_24h": True},
            {"sport": "Girls Soccer", "school": "Other <School>", "coach": "", "role": "",
             "games_with_stats": 0, "games_played": 0, "players": 5, "goals": 0, "assists": 0,
             "maxpreps": False, "photos": 0, "updated_ct": "", "new_24h": False},
        ],
    }


def test_section_lists_who_entered_stats():
    html_out = coach_stats_section(_sample())
    assert "Soccer Coach Stats" in html_out
    assert "Test High" in html_out and "Test Coach" in html_out and "(Head Coach)" in html_out
    assert "2 of 3" in html_out
    assert "MaxPreps OK, 1 photo(s)" in html_out
    assert html_out.count(">NEW<") == 1
    assert "Other &lt;School&gt;" in html_out  # escaped


def test_empty_and_missing():
    assert coach_stats_section(None) == ""
    assert "No coaches have entered stats yet." in coach_stats_section({"totals": {}, "teams": []})
    assert coach_stats_subject(None) == ""
    assert coach_stats_subject({"totals": {"teams": 0}}) == ""


def test_subject():
    assert coach_stats_subject(_sample()) == " — 2 soccer stat teams (+1 today)"
