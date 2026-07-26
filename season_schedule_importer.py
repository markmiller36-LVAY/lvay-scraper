#!/usr/bin/env python3
"""Build and import season-specific preseason football schedules.

The source format is the copy/pasted Louisiana Sportsline post:

    5A
    Airline Vikings (1-5A, NS1)
    W1 Wossman
    W2 @ C.E. Byrd*

The normalized JSON is intentionally independent from power rankings.  That
allows a new season to be published before scores exist while prior seasons
remain frozen and queryable.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import school_database as sdb


HEADER_RE = re.compile(
    r"^(.+?)\s*\((\d+)-([1-5]A),\s*(NS|S)([1-4])(?:,\s*[^)]+)?\)\s*$",
    re.IGNORECASE,
)
WEEK_RE = re.compile(r"^W(10|[1-9])(?:\s+D\s+)?\s*(.*)$", re.IGNORECASE)
STATE_RE = re.compile(r"\(([A-Z]{2})\)\s*$")
CLASS_RE = re.compile(r"^[1-5]A$", re.IGNORECASE)
BYE_MARKERS = {"", "-", "----", "--------", "—", "——", "————", "bye", "open"}

SOURCE_HEADER_ALIASES = {
    "acadiana christian": "Acadiana Christian School",
    "acadiana renaissance": "Acadiana Renaissance Charter Academy",
    "berchman's academy": "Berchmans Academy",
    "btw no": "Booker T. Washington - N.O.",
    "btw sh": "Booker T. Washington - Shr.",
    "carver": "George Washington Carver",
    "catholic b.r.": "Catholic - B.R.",
    "catholic br": "Catholic - B.R.",
    "catholic ni": "Catholic - N.I.",
    "catholic n.i.": "Catholic - N.I.",
    "catholic pc": "Catholic - P.C.",
    "catholic p.c.": "Catholic - P.C.",
    "central": "Central - B.R.",
    "cohen": "Walter L. Cohen",
    "collegiate br": "Collegiate Baton Rouge",
    "country day": "Metairie Park Country Day",
    "d'arbonne woods": "D'Arbonne Woods Charter",
    "ellender": "A.J. Ellender",
    "evangel": "Evangel Christian",
    "hannan": "Archbishop Hannan",
    "haynes": "Haynes Academy",
    "helix mentorship": "Helix Mentorship Academy",
    "higgins": "L. W. Higgins",
    "jefferson rise": "Jefferson Rise Charter",
    "john curtis": "John Curtis Christian",
    "js clark": "J.S. Clark Leadership Academy",
    "karr": "Edna Karr",
    "kennedy": "John F. Kennedy",
    "kenner discovery": "Kenner Discovery Health Science",
    "lafayette renaissance": "Lafayette Renaissance Charter Academy",
    "landry": "Lord Beaconsfield Landry",
    "lincoln prep": "Lincoln Preparatory School",
    "magnolia charter": "Magnolia School of Excellence",
    "menard": "Holy Savior Menard",
    "mcdonogh 35": "McDonogh #35",
    "mcmain": "Eleanor McMain",
    "newman": "Isidore Newman",
    "northwood lena": "Northwood - Lena",
    "northwood sh": "Northwood - Shrev.",
    "ouachita": "Ouachita Parish",
    "patrick taylor": "Patrick Taylor - Science/Tech.",
    "riverside": "Riverside Academy",
    "rummel": "Archbishop Rummel",
    "sarah reed": "Sarah T. Reed",
    "shaw": "Archbishop Shaw",
    "slaughter charter": "Slaughter Community Charter",
    "st. helena": "St. Helena College & Career Acad.",
    "st. louis": "St. Louis Catholic",
    "st. martin's": "St. Martin's Episcopal",
    "st. mary's nat": "St. Mary's",
    "st. michael": "St. Michael the Archangel",
    "st. michael's": "St. Michael the Archangel",
    "university": "University Lab",
    "westminster laf": "Westminster Christian - Lafayette",
    "westminster op": "Westminster Christian",
    "westminster christian op": "Westminster Christian",
    "willow": "The Willow School",
    "woodlawn b.r.": "Woodlawn - B.R.",
    "woodlawn br": "Woodlawn - B.R.",
    "woodlawn sh": "Woodlawn - Shrev.",
    "young audiences": "Young Audiences Charter",
}
NORMALIZED_SOURCE_HEADER_ALIASES = {
    re.sub(r"[^a-z0-9]+", " ", key.casefold()).strip(): value
    for key, value in SOURCE_HEADER_ALIASES.items()
}


def clean_text(value: str) -> str:
    """Repair common copy/paste mojibake and normalize whitespace."""
    replacements = {
        "â€™": "’",
        "â€“": "–",
        "â€”": "—",
        "Â": "",
    }
    for bad, good in replacements.items():
        value = value.replace(bad, good)
    if "â" in value:
        try:
            value = value.encode("cp1252").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    return re.sub(r"\s+", " ", value).strip()


def canonical_school_from_header(label: str):
    """Strip a mascot by matching progressively shorter prefixes."""
    cleaned = clean_text(label)
    key = re.sub(r"[^a-z0-9]+", " ", cleaned.casefold()).strip()
    alias = NORMALIZED_SOURCE_HEADER_ALIASES.get(key)
    if alias:
        record = sdb.get_school(alias) or {"name": alias}
        return record["name"], record
    words = cleaned.split()
    for cut in range(len(words), 0, -1):
        candidate = " ".join(words[:cut])
        key = re.sub(r"[^a-z0-9]+", " ", candidate.casefold()).strip()
        alias = NORMALIZED_SOURCE_HEADER_ALIASES.get(key)
        if alias:
            record = sdb.get_school(alias) or {"name": alias}
            return record["name"], record
        record = sdb.get_school(candidate)
        if record:
            return record["name"], record
    return None, None


def parse_week(raw: str) -> dict:
    value = clean_text(raw)
    away = value.startswith("@")
    if away:
        value = value[1:].strip()

    district = value.endswith("*")
    value = value.rstrip("*").strip()

    state = None
    state_match = STATE_RE.search(value)
    if state_match:
        state = state_match.group(1)
        value = STATE_RE.sub("", value).strip()

    is_bye = value.lower() in BYE_MARKERS or not value
    return {
        "opponent_source": "" if is_bye else value,
        "home_away": "" if is_bye else ("A" if away else "H"),
        "is_district": district,
        "state": state,
        "is_bye": is_bye,
    }


def parse_schedule_text(text: str, season: str, source: str) -> dict:
    lines = [clean_text(line) for line in text.splitlines()]

    # First collect every explicit team header so opponent names can be linked
    # even when the local school database uses a different alias.
    source_headers = {}
    source_prefix_candidates = {}
    header_failures = []
    for line in lines:
        match = HEADER_RE.match(line)
        if not match:
            continue
        canonical, record = canonical_school_from_header(match.group(1))
        if canonical:
            label = clean_text(match.group(1))
            source_headers[label.casefold()] = canonical
            words = label.split()
            for cut in range(1, len(words) + 1):
                prefix = " ".join(words[:cut]).casefold()
                source_prefix_candidates.setdefault(prefix, set()).add(canonical)
        else:
            header_failures.append(line)
    source_prefixes = {
        prefix: next(iter(canonical))
        for prefix, canonical in source_prefix_candidates.items()
        if len(canonical) == 1
    }

    schools = []
    current_class = None
    current_school = None

    for line in lines:
        if not line:
            continue
        if CLASS_RE.match(line):
            current_class = line.upper()
            current_school = None
            continue

        header = HEADER_RE.match(line)
        if header:
            canonical, record = canonical_school_from_header(header.group(1))
            if not canonical:
                current_school = None
                continue
            district_number = int(header.group(2))
            source_class = header.group(3).upper()
            track = header.group(4).upper()
            division_number = int(header.group(5))
            current_school = {
                "school": canonical,
                "source_label": clean_text(header.group(1)),
                "class_": source_class,
                "district": district_number,
                "track": "Non-Select" if track == "NS" else "Select",
                "division": f"{'Non-Select' if track == 'NS' else 'Select'} Division {division_number}",
                "source_division": f"{track}{division_number}",
                "games": [],
            }
            schools.append(current_school)
            continue

        week_match = WEEK_RE.match(line)
        if not week_match or current_school is None:
            continue

        week = int(week_match.group(1))
        game = parse_week(week_match.group(2))
        opponent_source = game.pop("opponent_source")
        opponent = None
        opponent_internal = False
        needs_review = False

        if not game["is_bye"]:
            opponent_key = re.sub(
                r"[^a-z0-9]+", " ", opponent_source.casefold()
            ).strip()
            alias_name = NORMALIZED_SOURCE_HEADER_ALIASES.get(opponent_key)
            source_match = (
                source_headers.get(opponent_source.casefold())
                or source_prefixes.get(opponent_source.casefold())
                or alias_name
            )
            db_match = sdb.get_school(opponent_source)
            if source_match:
                opponent = source_match
                opponent_internal = True
            elif db_match:
                opponent = db_match["name"]
                opponent_internal = True
            else:
                opponent = opponent_source
                # An explicit state suffix is authoritative OOS.  An unmatched
                # name without one must be reviewed rather than guessed OOS.
                needs_review = game["state"] is None

        current_school["games"].append(
            {
                "week": week,
                "game_date": None,
                "opponent": opponent,
                "opponent_source": opponent_source or None,
                "opponent_internal": opponent_internal,
                "state": game["state"],
                "out_of_state": bool(game["state"]),
                "home_away": game["home_away"],
                "is_district": game["is_district"],
                "is_bye": game["is_bye"],
                "result": None,
                "score": None,
                "needs_review": needs_review,
            }
        )

    duplicate_schools = sorted(
        school for school, count in Counter(s["school"] for s in schools).items() if count > 1
    )
    known_schools = {s["school"] for s in schools}
    unmatched_opponents = sorted(
        {
            g["opponent"]
            for school in schools
            for g in school["games"]
            if g["opponent"] and not g["opponent_internal"] and not g["out_of_state"]
        }
    )
    missing_weeks = {
        school["school"]: sorted(set(range(1, 11)) - {g["week"] for g in school["games"]})
        for school in schools
        if set(range(1, 11)) - {g["week"] for g in school["games"]}
    }
    reciprocal_mismatches = []
    schedule_index = {
        (school["school"], game["week"], game["opponent"])
        for school in schools
        for game in school["games"]
        if game["opponent_internal"]
    }
    for school in schools:
        for game in school["games"]:
            opponent = game["opponent"]
            if (
                game["opponent_internal"]
                and opponent in known_schools
                and (opponent, game["week"], school["school"]) not in schedule_index
            ):
                reciprocal_mismatches.append(
                    {
                        "school": school["school"],
                        "week": game["week"],
                        "opponent": opponent,
                    }
                )

    return {
        "sport": "football",
        "season": str(season),
        "status": "preseason",
        "source": source,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "schools": schools,
        "validation": {
            "school_count": len(schools),
            "game_rows": sum(len(s["games"]) for s in schools),
            "bye_rows": sum(g["is_bye"] for s in schools for g in s["games"]),
            "header_failures": header_failures,
            "duplicate_schools": duplicate_schools,
            "unmatched_opponents": unmatched_opponents,
            "missing_weeks": missing_weeks,
            "reciprocal_mismatches": reciprocal_mismatches,
        },
    }


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS season_registry (
            sport TEXT NOT NULL,
            season TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            is_locked INTEGER NOT NULL DEFAULT 0,
            source TEXT,
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY(sport, season)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS season_schools (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sport TEXT NOT NULL,
            season TEXT NOT NULL,
            school TEXT NOT NULL,
            class_ TEXT,
            district TEXT,
            division TEXT,
            track TEXT,
            source TEXT,
            status TEXT DEFAULT 'active',
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(sport, season, school)
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(games)")}
    for name, definition in (
        ("source", "TEXT"),
        ("is_district", "INTEGER DEFAULT 0"),
        ("needs_review", "INTEGER DEFAULT 0"),
    ):
        if name not in columns:
            conn.execute(f"ALTER TABLE games ADD COLUMN {name} {definition}")


def import_payload(payload: dict, db_path: str, replace: bool = False) -> dict:
    conn = sqlite3.connect(db_path)
    ensure_schema(conn)
    sport = payload["sport"]
    season = payload["season"]
    conn.execute(
        """
        INSERT INTO season_registry
            (sport, season, status, is_locked, source, updated_at)
        VALUES (?, ?, ?, 0, ?, ?)
        ON CONFLICT(sport, season) DO UPDATE SET
            status=excluded.status, source=excluded.source,
            updated_at=excluded.updated_at
        """,
        (
            sport,
            season,
            payload["status"],
            payload["source"],
            payload["generated_at"],
        ),
    )
    if replace:
        conn.execute("DELETE FROM games WHERE sport=? AND season=?", (sport, season))
        conn.execute("DELETE FROM season_schools WHERE sport=? AND season=?", (sport, season))

    school_rows = 0
    game_rows = 0
    for school in payload["schools"]:
        conn.execute(
            """
            INSERT INTO season_schools
                (sport, season, school, class_, district, division, track, source, status, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sport, season, school) DO UPDATE SET
                class_=excluded.class_, district=excluded.district,
                division=excluded.division, track=excluded.track,
                source=excluded.source, status=excluded.status,
                updated_at=excluded.updated_at
            """,
            (
                sport,
                season,
                school["school"],
                school["class_"],
                str(school["district"]),
                school["division"],
                school["track"],
                payload["source"],
                payload["status"],
                payload["generated_at"],
            ),
        )
        school_rows += 1
        for game in school["games"]:
            if game["is_bye"]:
                continue
            existing = conn.execute(
                """
                UPDATE games SET
                    game_date=?, opponent=?, win_loss=NULL, score=NULL,
                    home_away=?, district_class=?, scraped_at=?, source=?,
                    is_district=?, needs_review=?
                WHERE sport=? AND season=? AND school=? AND week=?
                """,
                (
                    game["game_date"],
                    game["opponent"],
                    game["home_away"],
                    school["source_division"],
                    payload["generated_at"],
                    payload["source"],
                    int(game["is_district"]),
                    int(game["needs_review"]),
                    sport,
                    season,
                    school["school"],
                    f"Week {game['week']}",
                ),
            )
            if existing.rowcount == 0:
                conn.execute(
                    """
                    INSERT INTO games
                        (sport, season, school, week, game_date, opponent,
                         win_loss, score, home_away, district_class, scraped_at,
                         source, is_district, needs_review)
                    VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        sport,
                        season,
                        school["school"],
                        f"Week {game['week']}",
                        game["game_date"],
                        game["opponent"],
                        game["home_away"],
                        school["source_division"],
                        payload["generated_at"],
                        payload["source"],
                        int(game["is_district"]),
                        int(game["needs_review"]),
                    ),
                )
            game_rows += 1
    conn.commit()
    conn.close()
    return {"schools": school_rows, "games": game_rows}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source_file")
    parser.add_argument("--season", default="2026")
    parser.add_argument("--source", default="Louisiana Sportsline preseason compilation")
    parser.add_argument("--output")
    parser.add_argument("--db")
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args()

    payload = parse_schedule_text(
        Path(args.source_file).read_text(encoding="utf-8"),
        season=args.season,
        source=args.source,
    )
    serialized = json.dumps(payload, indent=2, ensure_ascii=False)
    if args.output:
        Path(args.output).write_text(serialized + "\n", encoding="utf-8")
    else:
        print(serialized)
    if args.db:
        print(json.dumps(import_payload(payload, args.db, replace=args.replace), indent=2))


if __name__ == "__main__":
    main()
