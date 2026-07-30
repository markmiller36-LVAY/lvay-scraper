#!/usr/bin/env python3
"""Build the versioned 2025-26 LHSAA sport alignment dataset.

The source PDFs are official LHSAA reports saved outside the repository.
This script converts their tables into a deterministic JSON artifact that
can be reviewed, tested, and used without depending on the current LHSAA
website retaining prior-cycle documents.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pdfplumber


REGULAR_REPORTS = {
    "football": ("Regular Season Alignments.pdf", "class"),
    "volleyball": ("Volleyball_Class_Designations.pdf", "division"),
    "boys_basketball": ("Boys_Basketball_Class_Designations.pdf", "class"),
    "girls_basketball": ("Girls_Basketball_Class_Designations.pdf", "class"),
    "boys_soccer": ("Boys_Soccer_Class_Designations.pdf", "division"),
    "girls_soccer": ("Girls_Soccer_Class_Designations.pdf", "division"),
    "baseball": ("All_Schools_Class_Designations.pdf", "class"),
    "softball": ("Softball_Regular_Season_Alignmnets.pdf", "class"),
}

SEASON_KEYS = {
    "football": 2025,
    "volleyball": 2025,
    "boys_basketball": 2026,
    "girls_basketball": 2026,
    "boys_soccer": 2026,
    "girls_soccer": 2026,
    "baseball": 2026,
    "softball": 2026,
}

DIVISION_PAGES = {
    3: ("football", "Non-Select"),
    4: ("football", "Select"),
    5: ("boys_basketball", "Non-Select"),
    6: ("boys_basketball", "Select"),
    7: ("girls_basketball", "Non-Select"),
    8: ("girls_basketball", "Select"),
    9: ("softball", "Non-Select"),
    10: ("softball", "Select"),
    11: ("baseball", "Non-Select"),
    12: ("baseball", "Select"),
}

ROW_RE = re.compile(
    r"^(5A|4A|3A|2A|1A|B|C|I|II|III|IV|V)\s+(\d+)\s+(.+?)\s*$"
)


def parse_regular_report(path: Path, group_type: str) -> dict[str, dict]:
    schools: dict[str, dict] = {}
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            for raw_line in (page.extract_text() or "").splitlines():
                match = ROW_RE.match(raw_line.strip())
                if not match:
                    continue
                group, district, school = match.groups()
                # Legend markers describe alignment notes and are not part
                # of the school's canonical name.
                school = re.sub(r"\s+[+*]+$", "", school).strip()
                record = {
                    "district": int(district),
                    "source": path.name,
                }
                if group_type == "division":
                    record["division"] = f"Division {group}"
                else:
                    record["class"] = group
                schools[school] = record
    return schools


def parse_playoff_divisions(path: Path) -> dict[str, dict[str, str]]:
    divisions = {sport: {} for sport, _track in DIVISION_PAGES.values()}
    with pdfplumber.open(path) as pdf:
        for page_index, (sport, track) in DIVISION_PAGES.items():
            for table in pdf.pages[page_index].extract_tables():
                for row in table[1:]:
                    for offset in range(0, len(row), 3):
                        if offset + 2 >= len(row):
                            continue
                        school = (row[offset] or "").strip()
                        division = (row[offset + 2] or "").strip()
                        if school and division in {"I", "II", "III", "IV"}:
                            divisions[sport][school] = (
                                f"{track} Division {division}"
                            )
    return divisions


def build_dataset(pdf_root: Path) -> dict:
    sports = {}
    for sport, (filename, group_type) in REGULAR_REPORTS.items():
        path = pdf_root / filename
        if not path.exists():
            raise FileNotFoundError(f"Required LHSAA report not found: {path}")
        sports[sport] = parse_regular_report(path, group_type)

    division_path = pdf_root / "2024 - 2026 Divisions 4-22-2024.pdf"
    if not division_path.exists():
        raise FileNotFoundError(
            f"Required LHSAA division report not found: {division_path}"
        )
    playoff_divisions = parse_playoff_divisions(division_path)

    for sport, assignments in playoff_divisions.items():
        for school, division in assignments.items():
            if school in sports[sport]:
                sports[sport][school]["division"] = division

    # The later 2025-26 sport reports supersede the April 2024 master report
    # for Class B/C. In particular, Harrisonburg is Class B in the final
    # basketball and spring reports, not Class C as shown in the older file.
    for sport in ("boys_basketball", "girls_basketball", "baseball", "softball"):
        for record in sports[sport].values():
            if record.get("class") in {"B", "C"}:
                record["division"] = f"Class {record['class']}"

    return {
        "cycle": "2024-2026",
        "label": "2025-2026 archived seasons",
        "season_keys": SEASON_KEYS,
        "sources": sorted(
            {filename for filename, _group_type in REGULAR_REPORTS.values()}
            | {division_path.name}
        ),
        "sports": sports,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdf-root", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "sport_alignments_2025_2026.json",
    )
    args = parser.parse_args()

    dataset = build_dataset(args.pdf_root)
    args.output.write_text(
        json.dumps(dataset, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    counts = {
        sport: len(rows) for sport, rows in dataset["sports"].items()
    }
    print(f"Wrote {args.output}")
    print(json.dumps(counts, sort_keys=True))


if __name__ == "__main__":
    main()
