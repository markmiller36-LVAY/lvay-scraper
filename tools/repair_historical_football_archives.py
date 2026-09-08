"""Repair legacy football archive identities and reciprocal schedules.

The Airtable export contains a handful of playoff-only school rows, historical
names that no longer appear in the current directory, and OCR-corrupted names
from the 2020 brackets.  This script merges clear aliases, restores historical
class/district assignments, and reconstructs missing schedules from the
opponents' copies of each game.
"""

import gzip
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / "football_archives_greenlit.json.gz"

ALIASES = {
    "2015": {
        "George Washington Carver High School": "George Washington Carver",
        "McDonogh 35": "McDonogh #35",
        "Woodlawn-B.R.": "Woodlawn - B.R.",
    },
    "2016": {"St. Helena": "St. Helena College & Career Acad."},
    "2017": {"St. Helena": "St. Helena College & Career Acad."},
    "2018": {"St. Helena": "St. Helena College & Career Acad."},
    "2019": {"St. Helena": "St. Helena College & Career Acad."},
    "2020": {
        "Booker T. Washington - 18 N.O.": "Booker T. Washington - N.O.",
        "George Washington 7 Carver": "George Washington Carver",
        "John Curtis 7 Christian": "John Curtis Christian",
        "L ogansport": "Logansport",
        "Lake Charles College 3 Prep": "Lake Charles College Prep",
        "Ouachita 3 Christian": "Ouachita Christian",
        "P laquemine": "Plaquemine",
        "Riverside 7 Academy": "Riverside Academy",
        "Slaughter Community 18 Charter": "Slaughter Community Charter",
        "St. Helena College & 4 Career Acad.": "St. Helena College & Career Acad.",
        "St. Michael the 13 Archangel": "St. Michael the Archangel",
        "St. Thomas 7 Aquinas": "St. Thomas Aquinas",
    },
    "2021": {
        "Kenner Discovery": "Kenner Discovery Health Science",
        "Magnolia School": "Magnolia School of Excellence",
        "St. Helena": "St. Helena College & Career Acad.",
    },
}

# Historical regular-season alignments. Values are (class, district).
ALIGNMENTS = {
    "2015": {
        "Eleanor McMain": ("3A", "11"), "Fair Park": ("4A", "1"),
        "Grambling": ("1A", "1"), "Hamilton Christian": ("1A", "4"),
        "Landry-Walker": ("5A", "8"), "South Cameron": ("1A", "4"),
        "Tensas": ("1A", "2"),
    },
    "2016": {
        "Comeaux": ("5A", "3"), "Eleanor McMain": ("3A", "11"),
        "False River": ("1A", "5"), "South Cameron": ("1A", "4"),
        "Tensas": ("1A", "2"),
    },
    "2017": {
        "Comeaux": ("5A", "3"), "Frederick A Douglass": ("3A", "9"),
        "Helen Cox": ("4A", "9"), "Sicily Island": ("1A", "2"),
        "Tensas": ("1A", "2"),
    },
    "2018": {
        "Cohen College Prep": ("3A", "9"), "Comeaux": ("5A", "3"),
        "Frederick A Douglass": ("3A", "9"), "Helen Cox": ("4A", "9"),
        "Sicily Island": ("1A", "2"), "Tensas": ("1A", "2"),
    },
    "2019": {
        "Comeaux": ("5A", "3"), "Frederick A Douglass": ("3A", "9"),
        "Hamilton Christian": ("1A", "4"),
    },
    "2020": {
        "Hamilton Christian": ("1A", "4"), "Landry-Walker": ("4A", "9"),
        "Tensas": ("1A", "2"),
    },
    "2021": {
        "Frederick A Douglass": ("3A", "9"),
        "Hamilton Christian": ("1A", "4"),
    },
}


def reverse_game(game, opponent):
    result = {"W": "L", "L": "W", "T": "T"}.get(game.get("result", ""), "")
    score = game.get("score", "")
    if re.fullmatch(r"\d+-\d+", str(score)):
        left, right = score.split("-")
        score = f"{right}-{left}"
    return {
        **game,
        "opponent": opponent["school"],
        "home_away": {"H": "A", "A": "H"}.get(game.get("home_away", ""), game.get("home_away", "")),
        "result": result,
        "score": score,
        "opp_division": opponent.get("division", ""),
        "opp_class": opponent.get("class_", ""),
        "opponent_internal": True,
    }


def game_key(game):
    return (game.get("phase"), game.get("week"), game.get("game_date"),
            game.get("opponent"), game.get("score"))


def refresh_school(school):
    games = school.get("games", [])
    wins = sum(g.get("result") == "W" for g in games)
    losses = sum(g.get("result") == "L" for g in games)
    ties = sum(g.get("result") == "T" for g in games)
    school.update(wins=wins, losses=losses, ties=ties,
                  games_played=wins + losses + ties,
                  record=f"{wins}-{losses}" + (f"-{ties}" if ties else ""))


def main():
    with gzip.open(ARCHIVE, "rt", encoding="utf-8") as source:
        archive = json.load(source)

    for season, payload in archive["seasons"].items():
        schools = payload["schools"]
        by_name = {s["school"]: s for s in schools}

        for alias, canonical in ALIASES.get(season, {}).items():
            source = by_name.get(alias)
            target = by_name.get(canonical)
            if not source or not target:
                continue
            existing = {game_key(g) for g in target.get("games", [])}
            for game in source.get("games", []):
                if game_key(game) not in existing:
                    target.setdefault("games", []).append(game)
            schools.remove(source)
            by_name.pop(alias)
            refresh_school(target)

        # Airtable may store full labels such as 1-5A. The archive frontend
        # already appends the class, so its API contract uses the number only.
        for school in schools:
            match = re.match(r"^(\d+)-[1-5]A$", str(school.get("district", "")), re.I)
            if match:
                school["district"] = match.group(1)

        for school_name, (class_name, district) in ALIGNMENTS.get(season, {}).items():
            school = by_name.get(school_name)
            if school:
                school["class_"] = class_name
                school["district"] = district

        # Recover omitted regular-season rows from the opponent's copy.
        for school in schools:
            if school.get("district") and len(school.get("games", [])) <= 2:
                existing = {game_key(g) for g in school.get("games", [])}
                for opponent in schools:
                    if opponent is school:
                        continue
                    for game in opponent.get("games", []):
                        if game.get("opponent", "").casefold() != school["school"].casefold():
                            continue
                        recovered = reverse_game(game, opponent)
                        if game_key(recovered) not in existing:
                            school.setdefault("games", []).append(recovered)
                            existing.add(game_key(recovered))

        identities = {s["school"].casefold(): s for s in schools}
        for school in schools:
            for game in school.get("games", []):
                opponent = identities.get(game.get("opponent", "").casefold())
                if opponent:
                    game["opp_division"] = opponent.get("division", "")
                    game["opp_class"] = opponent.get("class_", "")
                game["is_district"] = bool(
                    opponent and school.get("district") and
                    school.get("district") == opponent.get("district") and
                    school.get("class_") == opponent.get("class_") and
                    str(game.get("phase", "")).lower().startswith("regular")
                )
            school["games"].sort(key=lambda g: (g.get("phase") != "Regular Season", str(g.get("game_date", "")), str(g.get("week", ""))))
            refresh_school(school)
        payload["count"] = len(schools)
        payload["game_count"] = sum(len(s.get("games", [])) for s in schools)

    with gzip.open(ARCHIVE, "wt", encoding="utf-8", compresslevel=9) as output:
        json.dump(archive, output, separators=(",", ":"), ensure_ascii=False)


if __name__ == "__main__":
    main()
