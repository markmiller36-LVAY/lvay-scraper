"""Official LHSAA final-record overrides for the 2026 spring audit.

These values are authoritative for published records and for opponent-win
strength calculations.  They do not invent or alter individual game scores.
"""

import re


OFFICIAL_RECORD_OVERRIDES = {
    ("baseball", "2026"): {
        "Mandeville": (23, 8, 2),
        "Covington": (10, 21, 0),
        "Pearl River": (18, 7, 0),
        "Livonia": (12, 13, 0),
        "Woodlawn - Shrev.": (2, 13, 0),
        "Mansfield": (1, 19, 0),
        "Varnado": (0, 14, 0),
        "Elton": (0, 11, 0),
        "Lafayette": (17, 11, 0),
        "Huntington": (8, 19, 0),
        "Southwood": (5, 14, 0),
        "New Orleans Military & Maritime": (4, 10, 0),
        "McDonogh #35": (8, 5, 1),
        "Isidore Newman": (21, 6, 0),
        "Lafayette Christian": (18, 13, 0),
        "Holy Savior Menard": (15, 10, 0),
        "De La Salle": (9, 23, 0),
        "Pickering": (1, 14, 0),
        "St. Edmund": (24, 6, 0),
        "Sacred Heart": (15, 13, 0),
        "Westminster Christian - Lafayette": (12, 10, 0),
    },
    ("softball", "2026"): {
        "Ringgold": (8, 12, 0),
        "Lakeview": (7, 16, 0),
    },
}


def _normalize(name):
    value = str(name or "").lower().replace("&", "and")
    return re.sub(r"[^a-z0-9]", "", value)


def get_record_overrides(sport, season):
    return OFFICIAL_RECORD_OVERRIDES.get(
        (str(sport).lower(), str(season)),
        {},
    )


def find_record_override(overrides, school):
    if school in overrides:
        return overrides[school]
    target = _normalize(school)
    for name, record in overrides.items():
        if _normalize(name) == target:
            return record
    return None
