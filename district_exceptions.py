"""District games that do not count toward district standings.

Some same-district matchups are played by special agreement and approved by
the LHSAA as non-district. Every place that decides "is this a district game?"
checks this list, so the game shows as non-district on schedules, standings
and the Sheets without any extra explanation.

Add a pair by (sport, season) with the two school names as they appear in the
schedule data (spelling differences in punctuation are tolerated).
"""

import re

NON_DISTRICT_GAMES = {
    ("football", "2026"): [
        # Same district (1-5A), played by agreement; LHSAA-approved non-district.
        ("Evangel Christian", "Huntington"),
    ],
}


def _key(name):
    return re.sub(r"[^a-z0-9]", "", str(name or "").lower())


def is_non_district_game(sport, season, school, opponent):
    """True when this matchup is approved as non-district for the season."""
    pairs = NON_DISTRICT_GAMES.get((str(sport or "").lower(), str(season or "")), [])
    this = {_key(school), _key(opponent)}
    return any({_key(a), _key(b)} == this for a, b in pairs)
