#!/usr/bin/env python3
"""
LVAY Manual 2026 Football Schedule Importer
=============================================
Parses raw copy/pasted LHSAA-style schedule text (grouped by class, then by
school, then by week) and inserts rows into the `games` table so the manual
2026 schedules display on WordPress exactly like scraped data does.

USAGE (on Render Shell):
    1. Paste your raw class text into a file, e.g.:
         cat > /tmp/schedule_5A.txt << 'EOF'
         <paste raw text here>
         EOF
    2. Run:
         python3 /opt/render/project/src/import_manual_schedule.py /tmp/schedule_5A.txt

SAFETY / RESET FLAGS:
    --reset
        Before importing, deletes any EXISTING manual rows (needs_review=1,
        sport='football', season='2026') that belong to the same class(es)
        found in this file. Use this when re-running a file you already
        imported (e.g. you fixed a typo and want a clean re-import) so you
        don't end up with duplicate rows.

        Example:
            python3 import_manual_schedule.py /tmp/schedule_5A.txt --reset

    --reset-all
        Deletes ALL manual rows (needs_review=1, sport='football',
        season='2026') across every class, before importing this file.
        Use with caution -- this wipes every manual 2026 football row you've
        imported so far, not just the current file's class.

        Example:
            python3 import_manual_schedule.py /tmp/schedule_5A.txt --reset-all

    Neither flag touches scraped data (rows with needs_review=0/NULL) --
    only rows this script itself created are ever deleted.

Behavior:
    - Detects class headers (lines like "5A", "4A", "Select 3A", etc.)
    - Detects school header lines (e.g. "Airline Vikings") by matching the
      start of the line against school_database.py (tries progressively
      shorter prefixes to strip the mascot).
    - Detects week lines (e.g. "W1 Wossman", "W2 @ C.E. Byrd*", "W9 ————")
    - Skips bye weeks (blank or "————" opponent)
    - Sets home_away ('H' if no @, 'A' if @ present)
    - Sets out_of_state='Yes' + appends state code to opponent name when a
      "(XX)" suffix is present, OR when the opponent can't be matched in
      school_database.py at all
    - Looks up opponent_class via get_class() when opponent is a known LA school
    - Inserts ONE row per school's own listed game (does NOT auto-mirror the
      other team's perspective -- if Team B's section also appears later in
      the same paste, that generates Team B's own row naturally)
    - Tags every row needs_review=1, scraped_at=<import time>, season='2026'
    - Prints a summary of any schools it could NOT match, so you can fix
      aliases in school_database.py or correct typos in the source text
"""

import sys
import re
import sqlite3
from datetime import datetime, timezone

sys.path.insert(0, "/opt/render/project/src")
import school_database as sdb  # noqa: E402

DB_PATH = "/data/lvay_v2.db"
SEASON = "2026"
SPORT = "football"

WEEK_LINE_RE = re.compile(r"^W(\d+)\s*(.*)$", re.IGNORECASE)
STATE_SUFFIX_RE = re.compile(r"\(([A-Z]{2})\)\s*$")
BYE_MARKERS = {"", "————", "----", "—", "-", "bye"}


def is_class_header(line: str) -> bool:
    """Lines like '5A', '4A', 'Select 3A', 'Division I', etc. with nothing else."""
    stripped = line.strip()
    if not stripped:
        return False
    if WEEK_LINE_RE.match(stripped):
        return False
    # Class headers are short, no lowercase school-name words, no '@'
    if "@" in stripped:
        return False
    return bool(re.match(r"^(Select\s+)?[1-5BC]A?$|^Division\s+[IVX]+$", stripped, re.IGNORECASE))


def try_match_school(line: str):
    """Try to match a 'School Mascot' line against school_database.
    Strips trailing words one at a time until a match is found."""
    words = line.strip().split()
    for cut in range(len(words), 0, -1):
        candidate = " ".join(words[:cut])
        school = sdb.get_school(candidate)
        if school:
            return candidate, school
    return None, None


def parse_opponent(raw: str):
    """Parse a week-line remainder into (opponent_name, home_away, out_of_state, state_code)."""
    raw = raw.strip()
    home_away = "H"
    if raw.startswith("@"):
        home_away = "A"
        raw = raw[1:].strip()

    state_code = None
    m = STATE_SUFFIX_RE.search(raw)
    if m:
        state_code = m.group(1)
        raw = STATE_SUFFIX_RE.sub("", raw).strip()

    # district marker
    raw = raw.rstrip("*").strip()

    return raw, home_away, state_code


def find_classes_in_file(lines):
    """First pass: collect every class header found in the file."""
    classes = set()
    for raw_line in lines:
        line = raw_line.strip()
        if line and is_class_header(line):
            classes.add(line)
    return classes


def reset_rows(cur, classes=None):
    """Delete manual (needs_review=1) football/2026 rows.
    If classes is provided, only delete rows matching those class_ values.
    If classes is None, delete ALL manual football/2026 rows."""
    if classes:
        placeholders = ",".join("?" for _ in classes)
        cur.execute(
            f"""
            DELETE FROM games
            WHERE sport=? AND season=? AND needs_review=1
              AND class_ IN ({placeholders})
            """,
            (SPORT, SEASON, *classes),
        )
    else:
        cur.execute(
            """
            DELETE FROM games
            WHERE sport=? AND season=? AND needs_review=1
            """,
            (SPORT, SEASON),
        )
    return cur.rowcount


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 import_manual_schedule.py <raw_text_file> [--reset|--reset-all]")
        sys.exit(1)

    path = sys.argv[1]
    flags = set(sys.argv[2:])
    do_reset = "--reset" in flags
    do_reset_all = "--reset-all" in flags

    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if do_reset_all:
        deleted = reset_rows(cur, classes=None)
        print(f"--reset-all: deleted {deleted} existing manual row(s) across ALL classes.")
        conn.commit()
    elif do_reset:
        classes_in_file = find_classes_in_file(lines)
        if not classes_in_file:
            print("--reset: no class headers detected in this file, nothing to reset.")
        else:
            deleted = reset_rows(cur, classes=classes_in_file)
            print(f"--reset: deleted {deleted} existing manual row(s) for class(es): {sorted(classes_in_file)}")
        conn.commit()

    current_class = None
    current_school = None
    current_school_record = None

    inserted = 0
    skipped_bye = 0
    unmatched_schools = set()
    now = datetime.now(timezone.utc).isoformat()

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        # 1) Class header?
        if is_class_header(line):
            current_class = line.strip()
            current_school = None
            current_school_record = None
            continue

        # 2) Week line?
        wm = WEEK_LINE_RE.match(line)
        if wm:
            if current_school is None:
                print(f"  ! Skipping week line before any school header: {line!r}")
                continue

            week_num = wm.group(1)
            remainder = wm.group(2)

            opponent_raw, home_away, state_code = parse_opponent(remainder)

            if opponent_raw.lower() in BYE_MARKERS or opponent_raw == "":
                skipped_bye += 1
                continue

            opp_school = sdb.get_school(opponent_raw)
            if opp_school:
                opponent_class = opp_school.get("class", "Unknown")
                out_of_state = ""
                opponent_name = opponent_raw
            else:
                opponent_class = None
                out_of_state = "Yes"
                opponent_name = opponent_raw if not state_code else f"{opponent_raw} ({state_code})"
                unmatched_schools.add(opponent_name)

            district = current_school_record.get("district") if current_school_record else None

            cur.execute(
                """
                INSERT INTO games
                    (sport, school, game_date, opponent, home_away, win_loss,
                     score, week, district, class_, district_class,
                     opponent_class, tournament, tournament_host, out_of_state,
                     location, season, scraped_at, needs_review)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    SPORT,
                    current_school,
                    None,  # game_date -- unknown until you supply actual dates
                    opponent_name,
                    home_away,
                    None,  # win_loss
                    None,  # score
                    f"Week {week_num}",
                    district,
                    current_class,
                    None,  # district_class
                    opponent_class,
                    None,  # tournament
                    None,  # tournament_host
                    out_of_state,
                    None,  # location
                    SEASON,
                    now,
                    1,  # needs_review
                ),
            )
            inserted += 1
            continue

        # 3) Otherwise, try as a school header line
        matched_name, school_record = try_match_school(line)
        if matched_name:
            current_school = matched_name
            current_school_record = school_record
        else:
            print(f"  ! Could not match school header line: {line!r}")
            current_school = None
            current_school_record = None

    conn.commit()
    conn.close()

    print("\n=== Import Summary ===")
    print(f"Rows inserted:        {inserted}")
    print(f"Bye weeks skipped:    {skipped_bye}")
    print(f"Unmatched opponents:  {len(unmatched_schools)}")
    if unmatched_schools:
        print("  (tagged out_of_state='Yes' automatically -- review these):")
        for s in sorted(unmatched_schools):
            print(f"    - {s}")


if __name__ == "__main__":
    main()
