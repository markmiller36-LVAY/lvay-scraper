"""Repair broken playoff game dates in football_archives_greenlit.json.gz.

Problems fixed (Oct 11, 2026):
  * "NaN/NaN/11/18/2016"-style dates (bad Airtable import of 2016-2018 playoff rows):
    the real date is the part after "NaN/NaN/".
  * Blank playoff dates (2016-2021) and Thu-Sat ranges like "11/21/2024-11/23/2024" (2024):
    filled from HSFHA-LA (https://hsfha.org/w/Special:LA/seasons), matched by season,
    final score and team names. List: tools/data_playoff_dates/hsfha_playoff_dates_2016_2024.tsv
    (one row per game; a blank date = HSFHA has no record, left as is).

Only `game_date` is changed. Usage:
    python3 tools/fix_playoff_dates_2016_2024.py            # dry run
    python3 tools/fix_playoff_dates_2016_2024.py write
"""
import csv
import gzip
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / "football_archives_greenlit.json.gz"
FIXES = ROOT / "tools" / "data_playoff_dates" / "hsfha_playoff_dates_2016_2024.tsv"
GOOD = re.compile(r"\d{1,2}/\d{1,2}/\d{4}")


def main():
    write = len(sys.argv) > 1 and sys.argv[1] == "write"
    fixes = {}
    with open(FIXES, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            if row["date"]:
                pair = tuple(sorted([row["school"], row["opponent"]]))
                fixes[(row["season"],) + pair + (row["round"],)] = row["date"]

    with gzip.open(ARCHIVE, "rt", encoding="utf-8") as fh:
        archive = json.load(fh)

    stripped = filled = left = 0
    for season, data in archive["seasons"].items():
        for school in data["schools"]:
            for game in school["games"]:
                date = str(game.get("game_date") or "")
                if GOOD.fullmatch(date):
                    continue
                if date.startswith("NaN/NaN/") and GOOD.fullmatch(date[8:]):
                    game["game_date"] = date[8:]
                    stripped += 1
                    continue
                pair = tuple(sorted([school["school"], game["opponent"]]))
                new = fixes.get((season,) + pair + (str(game.get("week")),))
                if new:
                    game["game_date"] = new
                    filled += 1
                else:
                    left += 1
    print(f"NaN dates repaired: {stripped}  blank/range dates filled from HSFHA: {filled}  still without a date: {left}")
    if write:
        with gzip.open(ARCHIVE, "wt", encoding="utf-8", compresslevel=9) as out:
            json.dump(archive, out, separators=(",", ":"), ensure_ascii=False)
        print("wrote", ARCHIVE.name)


if __name__ == "__main__":
    main()
