"""LHSAA basketball first-contest dates.

Games dated before a class's first official contest are jamborees or
scrimmages, not regular-season games. Mark (Nov 22, 2026): only Class B and C
teams should have records before Nov 23; Classes 5A-1A must show no record and
those early games come off their schedules.

2026-27 (LHSAA season "2027"), from the 2026-27 Basketball General Information
Bulletin and Handbook section 11:
  - Classes B & C: 1st contest Monday, November 9, 2026
  - Classes 5A-1A (Divisions I-IV): 1st contest Monday, November 23, 2026

A game is dropped when it is played before the first contest date of either
team's class, so a B or C team that met a 5A-1A team before Nov 23 does not
count that game either (it was not a legal regular-season contest).
"""
import re
from datetime import date, datetime

BASKETBALL_SPORTS = {"boys_basketball", "girls_basketball"}

# season -> {"BC": first contest for Classes B & C, "BIG": first contest 5A-1A}
FIRST_CONTEST = {
    "2027": {"BC": date(2026, 11, 9), "BIG": date(2026, 11, 23)},
}

BIG_CLASSES = {"5A", "4A", "3A", "2A", "1A"}
SMALL_CLASSES = {"B", "C"}


def normalize_class(value):
    """'7-5A' / 'Class B' / ' 5a ' -> '5A' / 'B' / '5A'. Unknown -> ''."""
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if "-" in text:
        text = text.split("-", 1)[1].strip()
    text = re.sub(r"^CLASS\s+", "", text).strip()
    if text in BIG_CLASSES or text in SMALL_CLASSES:
        return text
    return ""


def _parse_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    match = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{2,4})", str(value or ""))
    if not match:
        return None
    month, day, year = (int(x) for x in match.groups())
    if year < 100:
        year += 2000
    try:
        return date(year, month, day)
    except ValueError:
        return None


def first_contest(season, class_):
    """First legal contest date for a class in a season, or None if unknown."""
    dates = FIRST_CONTEST.get(str(season))
    cls = normalize_class(class_)
    if not dates or not cls:
        return None
    return dates["BIG"] if cls in BIG_CLASSES else dates["BC"]


def is_before_first_contest(sport, season, game_date, class_, opponent_class=""):
    """True when a basketball game predates either team's first contest date."""
    if str(sport or "").lower() not in BASKETBALL_SPORTS:
        return False
    dates = FIRST_CONTEST.get(str(season))
    if not dates:
        return False
    played = _parse_date(game_date)
    if not played:
        return False
    if played < dates["BC"]:
        return True  # before anyone's first contest
    for cls in (class_, opponent_class):
        start = first_contest(season, cls)
        if start and played < start:
            return True
    return False


def row_is_before_first_contest(sport, season, row):
    """Same check for a games-table row (dict-like)."""
    get = row.get if hasattr(row, "get") else (lambda k, d=None: row[k] if k in row.keys() else d)
    own = get("class_") or get("district_class") or ""
    if not normalize_class(own):
        own = get("district_class") or ""
    return is_before_first_contest(
        sport, season, get("game_date"), own, get("opponent_class") or "",
    )
