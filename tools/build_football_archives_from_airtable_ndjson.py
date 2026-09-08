"""Merge newline-delimited Airtable football seasons into the site archive.

Each input line is a JSON object containing ``season`` and ``records``.  A final
line containing ``__END__`` finishes the import and writes the gzip archive.
"""

import gzip
import json
import re
import sys
from collections import defaultdict
from pathlib import Path


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
    "forfeit": "fldFq9mX7JNwNdixb",
    "out_of_state": "fldVAFzxiuXHwZsEr",
}


def value(fields, name, default=""):
    raw = fields.get(FIELDS[name], default)
    if isinstance(raw, dict):
        return raw.get("name", default)
    return raw


def week_value(raw):
    text = str(raw or "").strip()
    match = re.search(r"\d+", text)
    return int(match.group()) if match else text


def division_track(division):
    text = str(division or "")
    if text.lower().startswith("non-select"):
        return "non-select"
    if text.lower().startswith("select"):
        return "select"
    return ""


def build_season(season, records):
    raw_rows = [record.get("cellValuesByFieldId", {}) for record in records]
    identities = {}
    for fields in raw_rows:
        school = str(value(fields, "school")).strip()
        if school:
            identities[school.casefold()] = {
                "class_": str(value(fields, "class_")).strip(),
                "district": str(value(fields, "district")).strip(),
                "division": str(value(fields, "division")).strip(),
            }

    grouped = defaultdict(list)
    for fields in raw_rows:
        school = str(value(fields, "school")).strip()
        opponent = str(value(fields, "opponent")).strip()
        if not school or not opponent:
            continue
        identity = identities.get(school.casefold(), {})
        opponent_identity = identities.get(opponent.casefold())
        result = str(value(fields, "result")).strip().upper()
        team_score = value(fields, "team_score", None)
        opp_score = value(fields, "opp_score", None)
        score = ""
        if team_score is not None and opp_score is not None:
            score = f"{team_score}-{opp_score}"
        is_district = bool(
            opponent_identity
            and identity.get("district")
            and identity.get("district") == opponent_identity.get("district")
            and identity.get("class_") == opponent_identity.get("class_")
            and str(value(fields, "phase")).lower().startswith("regular")
        )
        grouped[school].append({
            "week": week_value(value(fields, "week")),
            "phase": str(value(fields, "phase")).strip(),
            "game_date": str(value(fields, "date")).strip(),
            "opponent": opponent,
            "home_away": str(value(fields, "home_away")).strip(),
            "result": result,
            "score": score,
            "opp_division": str(value(fields, "opp_division")).strip(),
            "opp_class": str(value(fields, "opp_class")).strip(),
            "is_district": is_district,
            "opponent_internal": opponent_identity is not None,
            "forfeit": str(value(fields, "forfeit")).strip(),
            "out_of_state": bool(str(value(fields, "out_of_state")).strip()),
        })

    schools = []
    game_count = 0
    for school in sorted(grouped, key=str.casefold):
        identity = identities.get(school.casefold(), {})
        games = grouped[school]
        wins = sum(game["result"] == "W" for game in games)
        losses = sum(game["result"] == "L" for game in games)
        ties = sum(game["result"] == "T" for game in games)
        game_count += len(games)
        record = f"{wins}-{losses}" + (f"-{ties}" if ties else "")
        schools.append({
            "school": school,
            "class_": identity.get("class_", ""),
            "district": identity.get("district", ""),
            "division": identity.get("division", ""),
            "track": division_track(identity.get("division", "")),
            "wins": wins,
            "losses": losses,
            "ties": ties,
            "games_played": wins + losses + ties,
            "record": record,
            "games": games,
        })
    return {
        "status": "final",
        "count": len(schools),
        "game_count": game_count,
        "schools": schools,
    }


def main():
    with gzip.open(ARCHIVE, "rt", encoding="utf-8") as source:
        archive = json.load(source)
    imported = []
    for line in sys.stdin:
        line = line.rstrip("\r\n")
        if line == "__END__":
            break
        if not line:
            continue
        payload = json.loads(line)
        season = str(payload["season"])
        archive.setdefault("seasons", {})[season] = build_season(
            season, payload["records"]
        )
        imported.append(season)
    archive["source"] = "Airtable greenlit football archive"
    with gzip.open(ARCHIVE, "wt", encoding="utf-8", compresslevel=9) as output:
        json.dump(archive, output, separators=(",", ":"), ensure_ascii=False)
    print(json.dumps({"imported": imported, "path": str(ARCHIVE)}), flush=True)


if __name__ == "__main__":
    main()
