"""Emit Airtable upsert rows for recovered historical regular-season games."""

import gzip
import json
from pathlib import Path

from repair_historical_football_archives import ALIGNMENTS


ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / "football_archives_greenlit.json.gz"

FIELDS = {
    "school": "fldrpUTYFnm2cnF7p",
    "class_": "fld6TmyWYfSlEZCVC",
    "district": "fldlhgMU3lRTbUhqu",
    "division": "fldlswV4wnFtCwWwh",
    "season": "fld8vXYVZsN4kkhVJ",
    "phase": "fldGX4GnIe6GzJPco",
    "date": "fldN0lZE7TidZZwId",
    "week": "fldTL5gr7Oo4cZ8xi",
    "opponent": "fld0WO5ggDi4IZRbE",
    "opp_division": "fldzTK7m30Ddqk0Wt",
    "opp_class": "fldfd2g8y2yPGKnh2",
    "home_away": "fldptftmzPqJOjz9z",
    "result": "fldnZSftt5Re6hgAM",
    "team_score": "fldJSOy6zfUJU09sR",
    "opp_score": "fldqNlpr8GswCVBpC",
    "margin": "fld8xEQ7C7HrUujww",
    "record_id": "fldCU6L1aHUtprTsv",
    "record_id_static": "fldIqwXXXM9A3F9VY",
}


def score_parts(score):
    try:
        left, right = str(score).split("-", 1)
        return int(left), int(right)
    except (TypeError, ValueError):
        return None, None


def main():
    with gzip.open(ARCHIVE, "rt", encoding="utf-8") as source:
        seasons = json.load(source)["seasons"]
    output = []
    for season, schools in ALIGNMENTS.items():
        by_name = {s["school"]: s for s in seasons[season]["schools"]}
        for school_name in schools:
            # Comeaux was entered interactively before this exporter was added.
            if season == "2016" and school_name == "Comeaux":
                continue
            school = by_name.get(school_name)
            if not school:
                continue
            for game in school.get("games", []):
                if game.get("phase") != "Regular Season":
                    continue
                team_score, opp_score = score_parts(game.get("score"))
                key = f"{season}|{school_name}|RS|{game.get('week')}|{game.get('opponent')}"
                fields = {
                    FIELDS["school"]: school_name,
                    FIELDS["class_"]: school.get("class_", ""),
                    FIELDS["district"]: school.get("district", ""),
                    FIELDS["division"]: school.get("division", ""),
                    FIELDS["season"]: int(season),
                    FIELDS["phase"]: "Regular Season",
                    FIELDS["date"]: game.get("game_date", ""),
                    FIELDS["week"]: str(game.get("week", "")),
                    FIELDS["opponent"]: game.get("opponent", ""),
                    FIELDS["home_away"]: game.get("home_away", ""),
                    FIELDS["result"]: game.get("result", ""),
                    FIELDS["record_id"]: key,
                    FIELDS["record_id_static"]: key,
                }
                if game.get("opp_division"):
                    fields[FIELDS["opp_division"]] = game["opp_division"]
                if game.get("opp_class"):
                    fields[FIELDS["opp_class"]] = game["opp_class"]
                if team_score is not None:
                    fields[FIELDS["team_score"]] = team_score
                    fields[FIELDS["opp_score"]] = opp_score
                    fields[FIELDS["margin"]] = team_score - opp_score
                output.append({"fields": fields})
    print(json.dumps(output, separators=(",", ":")))


if __name__ == "__main__":
    main()
