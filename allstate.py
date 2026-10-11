"""All-State honors lists for the team pages (boys basketball so far).

Source file: honors_data/<sport>_allstate.txt, one player per line:
    School - Last, First - Grade - Year - Class
Year is the spring (end) year of the season, e.g. 2026 = 2025-26.
"""
import os
import re

from winter_archive import core_name

HERE = os.path.dirname(os.path.abspath(__file__))
FILES = {"boys_basketball": "boys_basketball_allstate.txt"}

# List spelling -> (LVAY school name, name the school had then or None)
ALIASES = {
    "Avoyelles Charter": ("Avoyelles Public Charter", None),
    "B.T. Washington NO": ("Booker T. Washington - N.O.", None),
    "B.T. Washington SH": ("Booker T. Washington - Shr.", None),
    "Bryd": ("C.E. Byrd", None),
    "Byrd": ("C.E. Byrd", None),
    "Caldwell": ("Caldwell Parish", None),
    "Capitol BR": ("Capitol", None),
    "Carver": ("George Washington Carver", None),
    "Catholic BR": ("Catholic - B.R.", None),
    "Central BR": ("Central - B.R.", None),
    "Cohen": ("Walter L. Cohen", None),
    "Country Day MP": ("Metairie Park Country Day", None),
    "Ebard": ("Ebarb", None),
    "Ellender": ("A.J. Ellender", None),
    "Episcopal BR": ("Episcopal", None),
    "Episcopal - Acadiana": ("Episcopal of Acadiana", None),
    "Hannan": ("Archbishop Hannan", None),
    "Rummel": ("Archbishop Rummel", None),
    "Shaw": ("Archbishop Shaw", None),
    "Hanson": ("Hanson Memorial", None),
    "Higgins": ("L. W. Higgins", None),
    "J.F. Kennedy": ("John F. Kennedy", None),
    "J.S. Clark": ("JS Clark Leadership Academy", None),
    "Jesuit NO": ("Jesuit", None),
    "Jesuit SH": ("Loyola Prep", "Jesuit"),
    "John Curtis": ("John Curtis Christian", None),
    "L.B. Landry": ("Lord Beaconsfield Landry", None),
    "Landry-Walker": ("Lord Beaconsfield Landry", None),
    "O.P. Walker": ("Lord Beaconsfield Landry", "O.P. Walker"),
    "Lincoln Prep": ("Lincoln Preparatory School", None),
    "McMain": ("Eleanor McMain", None),
    "Menard": ("Holy Savior Menard", None),
    "New Living World": ("New Living Word", None),
    "Newman": ("Isidore Newman", None),
    "North Vermillion": ("North Vermilion", None),
    "Northwood SH": ("Northwood - Shrev.", None),
    "Ouachita": ("Ouachita Parish", None),
    "Robert E. Lee": ("Liberty", "Robert E. Lee"),
    "Scotlandvile": ("Scotlandville", None),
    "Simboro": ("Simsboro", None),
    "St. Louis": ("St. Louis Catholic", None),
    "St. Mary": ("St. Mary's", None),
    "St. Michael": ("St. Michael the Archangel", None),
    "St. Paul": ("St. Paul's", None),
    "Southwood SH": ("Southwood", None),
    "Trafton": ("Dunham", "Chapel Trafton"),
    "University": ("University Lab", None),
    "Woodlawn BR": ("Woodlawn - B.R.", None),
    "Woodlawn SH": ("Woodlawn - Shrev.", None),
    "Woodlawn Sh": ("Woodlawn - Shrev.", None),
}

_CACHE = {}


def _display_name(raw):
    """'Frazier Jr, Marvin' -> 'Marvin Frazier Jr'; fixes an all-lowercase first name."""
    raw = re.sub(r"\s+", " ", raw).strip()
    if "," in raw:
        last, first = [x.strip() for x in raw.split(",", 1)]
        name = f"{first} {last}".strip()
    else:
        name = raw
    return " ".join(w[:1].upper() + w[1:] if w[:1].islower() else w for w in name.split(" "))


def load(sport):
    path = os.path.join(HERE, "honors_data", FILES[sport])
    mtime = os.path.getmtime(path)
    hit = _CACHE.get(sport)
    if hit and hit[0] == mtime:
        return hit[1]
    rows = []
    with open(path, encoding="utf-8-sig") as fh:
        for line in fh:
            text = line.replace("​", "").strip()
            if not text:
                continue
            parts = [p.strip() for p in re.split(r"\s+-\s+", text)]
            if len(parts) < 5 or not re.fullmatch(r"\d{4}", parts[-2]):
                continue
            school = " - ".join(parts[:-4])
            player, grade, year, cls = parts[-4:]
            lvay, former = ALIASES.get(school, (school, None))
            rows.append({
                "school": lvay,
                "key": core_name(lvay),
                "listed_as": school,
                "former": former,
                "player": _display_name(player),
                "grade": grade,
                "year": int(year),
                "season": f"{int(year) - 1}-{str(year)[-2:]}",
                "class": cls,
            })
    _CACHE[sport] = (mtime, rows)
    return rows


def school_allstate(sport, name):
    key = core_name(name)
    out = [dict((k, v) for k, v in r.items() if k != "key") for r in load(sport) if key and r["key"] == key]
    out.sort(key=lambda r: (-r["year"], r["player"]))
    return out
