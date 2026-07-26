"""Build the 2025-26 basketball alignment/audit data from LHSAA reports."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pdfplumber


ROOT = Path(__file__).resolve().parents[1]
PDF_DIR = ROOT / "tmp" / "pdfs" / "basketball"
OUTPUT = ROOT / "winter_alignment_2026.json"
SOCCER_PDF_DIR = ROOT / "tmp" / "pdfs" / "soccer"

REPORTS = {
    "boys_basketball": {
        "boys_ns1.pdf": "Non-Select Division I",
        "boys_ns2.pdf": "Non-Select Division II",
        "boys_ns3.pdf": "Non-Select Division III",
        "boys_ns4.pdf": "Non-Select Division IV",
        "boys_s1.pdf": "Select Division I",
        "boys_s2.pdf": "Select Division II",
        "boys_s3.pdf": "Select Division III",
        "boys_s4.pdf": "Select Division IV",
        "boys_b.pdf": "Class B",
        "boys_c.pdf": "Class C",
    },
    "girls_basketball": {
        "girls_ns1.pdf": "Non-Select Division I",
        "girls_ns2.pdf": "Non-Select Division II",
        "girls_ns3.pdf": "Non-Select Division III",
        "girls_ns4.pdf": "Non-Select Division IV",
        "girls_s1.pdf": "Select Division I",
        "girls_s2.pdf": "Select Division II",
        "girls_s3.pdf": "Select Division III",
        "girls_s4.pdf": "Select Division IV",
        "girls_b.pdf": "Class B",
        "girls_c.pdf": "Class C",
    },
}


def clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def parse_district(value: object) -> tuple[int | None, str]:
    text = clean(value)
    match = re.match(r"(\d+)\s*-\s*([1-5]A|B|C)$", text)
    if not match:
        return None, ""
    return int(match.group(1)), match.group(2)


def extract_report(path: Path, division: str) -> dict[str, dict]:
    schools: dict[str, dict] = {}
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                for row in table:
                    if len(row) < 12:
                        continue
                    rank = clean(row[0])
                    school = clean(row[1])
                    rating = clean(row[2])
                    record = clean(row[9])
                    district, class_ = parse_district(row[10])
                    if not rank.isdigit() or not school or not rating:
                        continue
                    try:
                        power_rating = float(rating)
                    except ValueError:
                        continue
                    schools[school] = {
                        "division": division,
                        "class": class_,
                        "district": district,
                        "official_power_rating": power_rating,
                        "official_record": record,
                        "official_rank": int(rank),
                    }
    return schools


def extract_soccer_report(path: Path) -> dict[str, dict]:
    schools: dict[str, dict] = {}
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                division = ""
                for row in table:
                    first = clean(row[0]) if row else ""
                    match = re.fullmatch(r"Division\s+(I|II|III|IV)", first)
                    if match:
                        division = f"Division {match.group(1)}"
                        continue
                    if len(row) < 12 or not division:
                        continue
                    rank = clean(row[0])
                    school = clean(row[1])
                    district_text = clean(row[2])
                    rating = clean(row[3])
                    record = clean(row[9])
                    district_match = re.match(
                        r"(\d+)\s*-\s*(I|II|III|IV)$", district_text
                    )
                    if (
                        not rank.isdigit()
                        or not school
                        or not district_match
                    ):
                        continue
                    try:
                        power_rating = float(rating)
                    except ValueError:
                        continue
                    schools[school] = {
                        "division": division,
                        "class": "",
                        "district": int(district_match.group(1)),
                        "official_power_rating": power_rating,
                        "official_record": record,
                        "official_rank": int(rank),
                    }
    return schools


def main() -> None:
    payload: dict[str, dict[str, dict]] = {}
    for sport, reports in REPORTS.items():
        schools: dict[str, dict] = {}
        for filename, division in reports.items():
            path = PDF_DIR / filename
            if not path.exists():
                raise FileNotFoundError(path)
            report_schools = extract_report(path, division)
            overlap = set(schools) & set(report_schools)
            if overlap:
                raise ValueError(f"Duplicate schools in {sport}: {sorted(overlap)}")
            schools.update(report_schools)
        payload[sport] = dict(sorted(schools.items()))

    payload["boys_soccer"] = dict(
        sorted(
            extract_soccer_report(
                SOCCER_PDF_DIR / "boys_soccer_seeds.pdf"
            ).items()
        )
    )
    payload["girls_soccer"] = dict(
        sorted(
            extract_soccer_report(
                SOCCER_PDF_DIR / "girls_soccer_seeds.pdf"
            ).items()
        )
    )

    OUTPUT.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    for sport, schools in payload.items():
        print(f"{sport}: {len(schools)} official teams")
    print(OUTPUT)


if __name__ == "__main__":
    main()
