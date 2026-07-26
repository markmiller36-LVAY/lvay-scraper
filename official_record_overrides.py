"""Official LHSAA final-record and game-counting corrections.

These values are authoritative for published records and for opponent-win
strength calculations. Game exclusions preserve the scraped result for audit
purposes while preventing a contest that LHSAA omitted from being rated.
"""

import json
import re
from pathlib import Path


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

# Each exclusion is directional because the official reports can count a
# contest for one team but omit it for the other.
OFFICIAL_GAME_EXCLUSIONS = {
    ("baseball", "2026"): (
        {
            "school": "Huntington",
            "opponent": "Mansfield",
            "game_date": "4/13/2026",
            "reason": "LHSAA final report is 8-19 and omits this win.",
        },
        {
            "school": "Mansfield",
            "opponent": "Huntington",
            "game_date": "4/13/2026",
            "reason": "LHSAA final report is 1-19 and omits this loss.",
        },
        {
            "school": "McDonogh #35",
            "opponent": "New Orleans Military & Maritime",
            "game_date": "4/13/2026",
            "reason": (
                "LHSAA final report is 8-5-1; excluding this loss reproduces "
                "the official 19.18 rating exactly."
            ),
        },
    ),
}


def _normalize(name):
    value = str(name or "").lower().replace("&", "and")
    return re.sub(r"[^a-z0-9]", "", value)


def get_record_overrides(sport, season):
    overrides = dict(OFFICIAL_RECORD_OVERRIDES.get(
        (str(sport).lower(), str(season)),
        {},
    ))
    sport_key = str(sport).lower()
    if str(season) == "2026" and sport_key in {
        "boys_basketball",
        "girls_basketball",
        "boys_soccer",
        "girls_soccer",
    }:
        path = Path(__file__).with_name("winter_alignment_2026.json")
        if path.exists():
            try:
                official = json.loads(path.read_text(encoding="utf-8"))
                for school, info in official.get(sport_key, {}).items():
                    parts = [
                        int(part)
                        for part in str(
                            info.get("official_record") or ""
                        ).split("-")
                        if part.strip().isdigit()
                    ]
                    if len(parts) == 2:
                        overrides[school] = (parts[0], parts[1], 0)
                    elif len(parts) == 3:
                        overrides[school] = tuple(parts)
            except (OSError, ValueError):
                pass
    return overrides


def find_record_override(overrides, school):
    if school in overrides:
        return overrides[school]
    target = _normalize(school)
    for name, record in overrides.items():
        if _normalize(name) == target:
            return record
    return None


def get_game_exclusions(sport, season):
    return OFFICIAL_GAME_EXCLUSIONS.get(
        (str(sport).lower(), str(season)),
        (),
    )


def find_game_exclusion(exclusions, row):
    school = _normalize(row.get("school"))
    opponent = _normalize(row.get("opponent"))
    game_date = str(row.get("game_date") or "").split()[0]
    for exclusion in exclusions:
        if (
            _normalize(exclusion["school"]) == school
            and _normalize(exclusion["opponent"]) == opponent
            and exclusion["game_date"] == game_date
        ):
            return exclusion
    return None
