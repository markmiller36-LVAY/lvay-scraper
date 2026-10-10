"""Schools that play only a JV schedule in a season.

Games against them stay on schedules, labelled "(JV ONLY)" with their scores,
but they never count toward records, standings or power ratings.

Add a school by (sport, season) using the name as it appears in the schedule
data (spelling differences in punctuation are tolerated).
"""

import re

JV_ONLY_SCHOOLS = {
    ("football", "2026"): [
        # Renamed Bolton Academy; JV-only in the LHSAA 2026-27 football alignment.
        "False River Academy",
    ],
}

JV_ONLY_LABEL = " (JV ONLY)"


def _key(name):
    return re.sub(r"[^a-z0-9]", "", str(name or "").lower())


def is_jv_only_school(sport, season, school):
    names = JV_ONLY_SCHOOLS.get((str(sport or "").lower(), str(season or "")), [])
    return _key(school) in {_key(name) for name in names}


def is_jv_only_game(sport, season, school, opponent):
    """True when either side of the game plays a JV-only schedule this season."""
    return (
        is_jv_only_school(sport, season, school)
        or is_jv_only_school(sport, season, opponent)
    )
