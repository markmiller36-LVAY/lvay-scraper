"""Build football seasons from the HSFHA-LA scrape and merge them into the
site archive (football_archives_greenlit.json.gz). Used for 2000-2009 and 1990-1999.

Source: High School Football Historians Association, Louisiana section
(hsfha.org/w/Special:LA/seasons), pulled Oct 11, 2026 as one TSV row per game
(see tools/data_2000_2009/README.md for the columns and how it was pulled).

Usage:
  python3 tools/build_football_from_hsfha.py <hsfha.tsv> <site_names.json> [write] [first-last]
  (run tools/dedupe_hsfha.py on the TSV first; HSFHA lists some old games twice)

site_names.json: list of every school name already in the archive (2010+), used to
match HSFHA names to LVAY names.
"""
import csv, gzip, json, re, sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / "football_archives_greenlit.json.gz"

# HSFHA school code -> LVAY school name, where the plain name differs or is ambiguous.
CODE_NAME = {
    "LA.CO-008": "Woodlawn - Shrev.", "LA.EB-008": "Woodlawn - B.R.",
    "LA.EB-002": "Catholic - B.R.", "LA.II-001": "Catholic - N.I.",
    "LA.PO-001": "Catholic - P.C.",
    "LA.CO-009": "Booker T. Washington - Shr.", "LA.OR-011": "Booker T. Washington - N.O.",
    "LA.CO-006": "Northwood - Shrev.", "LA.RA-007": "Northwood - Lena",
    "LA.EB-001": "Central - B.R.",
    "LA.EB-014": "St. Michael the Archangel",   # was Bishop Sullivan
    "LA.WR-002": "Lakeside",                    # listed as Sibley in early years
    "LA.SH-001": "St. Helena College & Career Acad.",
    "LA.OR-008": "Isidore Newman", "LA.LN-002": "Grambling",
    "LA.JF-012": "Metairie Park Country Day", "LA.JF-019": "Crescent City",
    "LA.OR-022": "Ben Franklin", "LA.JF-023": "Ridgewood",
    "LA.OR-019": "Sarah T. Reed", "LA.OR-014": "Frederick A Douglass",
    "LA.OR-016": "George Washington Carver", "LA.WO-006": "Mt. Hermon",
    "LA.SO-002": "St. Charles",
    "LA.EB-004": "Dunham",                      # Chapel Trafton until 1995
}
ROUND = {"Bi-District": "1", "First round": "1", "Regional": "Regional",
         "Second round": "Regional", "Quarterfinal": "Quarterfinal",
         "Semifinal": "Semifinal", "Final": "State Championship"}
ORD = {"1": 1, "Regional": 2, "Quarterfinal": 3, "Semifinal": 4, "State Championship": 5}


LETTERS = {"A": "1A", "AA": "2A", "AAA": "3A", "AAAA": "4A", "AAAAA": "5A"}


def cls(c):
    c = c.replace("Class ", "") if c.startswith("Class ") else ""
    return LETTERS.get(c, c)


def dist(d):
    m = re.match(r"^District (\d+)-", d or "")
    return m.group(1) if m else ""


def is_bowl(x):
    # Named postseason bowl games (Shrimp Bowl, Jaycee Bowl...) are not LHSAA playoffs.
    return x["gametype"] != "Regular" and "bowl" in x["round"].lower()


def monday(s):
    d = date.fromisoformat(s)
    return d - timedelta(days=d.weekday())


def fill_missing_affiliations(R):
    """HSFHA has no class/district for some school-seasons (54 schools in 1990, a few
    later). Fill them from the same school's nearest season (1 year away, then 2) in the
    same class system (letters through 1990, 5A-1A from 1991), when it was LHSAA then."""
    era = lambda y: y <= 1990
    known = {}
    for x in R:
        for side in ("team", "opp"):
            if x[side + "_org"] == "LHSAA" and x[side + "_class"]:
                known[(int(x["season"]), x[side + "_code"])] = (x[side + "_class"], x[side + "_district"])
    filled = set()
    for x in R:
        y = int(x["season"])
        for side in ("team", "opp"):
            code = x[side + "_code"]
            if x[side + "_org"] or not code.startswith("LA."):
                continue
            for dy in (-1, 1, -2, 2):
                k = (y + dy, code)
                if k in known and era(y + dy) == era(y):
                    x[side + "_org"] = "LHSAA"
                    x[side + "_class"], x[side + "_district"] = known[k]
                    filled.add((y, x[side], y + dy))
                    break
    by = Counter(f[0] for f in filled)
    print("class/district filled from a nearby season:", dict(sorted(by.items())))
    return filled


def main(src, names_path, write=False, years=None):
    R = list(csv.DictReader(open(src, encoding="utf-8"), delimiter="\t"))
    fill_missing_affiliations(R)
    if years:
        R = [x for x in R if years[0] <= int(x["season"]) <= years[1]]
    site = set(json.load(open(names_path)))
    norm = lambda s: re.sub(r"[^a-z0-9]", "", s.lower())
    site_norm = {norm(n): n for n in site}

    def lvay(name, code):
        if code in CODE_NAME:
            return CODE_NAME[code]
        if name in site:
            return name
        return site_norm.get(norm(name))

    # Week 1 = first Mon-Sun week with >=25 LHSAA-vs-LHSAA regular-season games.
    cnt = Counter()
    for x in R:
        if x["gametype"] == "Regular" and x["team_org"] == "LHSAA" and x["opp_org"] == "LHSAA" and x["date"]:
            cnt[(x["season"], monday(x["date"]))] += 1
    wk1 = {}
    for (s, m), n in sorted(cnt.items()):
        if n >= 25 and s not in wk1:
            wk1[s] = m
    # Round for unlabeled LHSAA playoff games: same season + class + week as labeled ones.
    lab = {}
    for x in R:
        if x["gametype"] != "Regular" and x["round"] in ROUND and x["team_org"] == "LHSAA" and x["date"]:
            lab[(x["season"], cls(x["team_class"]), monday(x["date"]))] = ROUND[x["round"]]

    rows = defaultdict(list)   # (season, school) -> games
    ident = {}                 # (season, school) -> (class, district)
    skipped = 0
    for x in R:
        played = x["played"] == "1"
        forfeit = (not played) and x["team_dec"] == "W" and x["opp_dec"] == "L"
        if not played and not forfeit:
            skipped += 1
            continue
        s = x["season"]
        bowl = is_bowl(x)
        post = x["gametype"] != "Regular" and not bowl
        if post:
            wk = ROUND.get(x["round"])
            if wk is None and x["team_org"] == "LHSAA":
                wk = lab.get((s, cls(x["team_class"]), monday(x["date"]))) if x["date"] else None
            wk = wk or "1"
        else:
            wk = max((monday(x["date"]) - wk1[s]).days // 7 + 1, 0) if x["date"] else 0
        d = date.fromisoformat(x["date"]) if x["date"] else None
        for side, other in (("team", "opp"), ("opp", "team")):
            code = x[side + "_code"]
            if not code.startswith("LA."):
                continue
            org = x[side + "_org"]
            name = lvay(x[side], code)
            if org != "LHSAA" and not name:
                continue          # non-LHSAA school with no LVAY page: opponent only
            name = name or x[side]
            if org == "LHSAA":
                ident[(s, name)] = (cls(x[side + "_class"]), dist(x[side + "_district"]))
            else:
                ident.setdefault((s, name), ("", ""))
            ts, os_ = x[side + "_score"], x[other + "_score"]
            dec = x[side + "_dec"]
            if dec == "X":       # adjusted on HSFHA (2005 4A final): keep on-field result
                dec = "W" if ts and os_ and int(ts) > int(os_) else "L"
            if x["team_dec"] == "T":
                dec = "T"
            ocode = x[other + "_code"]
            oname = (lvay(x[other], ocode) if ocode.startswith("LA.") else None) or x[other]
            site_code = x["team_site"] if side == "team" else {"h": "a", "a": "h"}.get(x["team_site"], x["team_site"])
            rows[(s, name)].append({
                "d": d, "week": wk, "phase": "Playoffs" if post else "Regular Season",
                "opponent": oname, "ocode": ocode, "oorg": x[other + "_org"],
                "oclass": cls(x[other + "_class"]) if x[other + "_org"] == "LHSAA" else "",
                "odist": dist(x[other + "_district"]),
                "ha": {"h": "H", "a": "A"}.get(site_code, ""), "r": dec,
                "score": f"{ts}-{os_}" if ts and os_ else "",
                "ff": "FF" if forfeit else "",
            })

    out = {}
    for s in sorted({k[0] for k in rows}):
        schools, gc = [], 0
        names_this = {k[1] for k in rows if k[0] == s}
        for (yy, name), gs in rows.items():
            if yy != s:
                continue
            c, dd = ident[(s, name)]
            gs.sort(key=lambda g: (g["d"] or date(int(s), 12, 31), ORD.get(str(g["week"]), 0)))
            games = []
            for g in gs:
                internal = g["opponent"] in names_this
                is_d = bool(c and g["phase"] == "Regular Season" and g["oorg"] == "LHSAA"
                            and g["oclass"] == c and g["odist"] == dd)
                games.append({
                    "week": g["week"], "phase": g["phase"],
                    "game_date": f"{g['d'].month}/{g['d'].day}/{g['d'].year}" if g["d"] else "",
                    "opponent": g["opponent"], "home_away": g["ha"], "result": g["r"],
                    "score": g["score"], "opp_division": "", "opp_class": g["oclass"],
                    "is_district": is_d, "opponent_internal": internal,
                    "forfeit": g["ff"], "out_of_state": not g["ocode"].startswith("LA."),
                })
            w = sum(g["result"] == "W" for g in games)
            l = sum(g["result"] == "L" for g in games)
            t = sum(g["result"] == "T" for g in games)
            gc += len(games)
            schools.append({"school": name, "class_": c, "district": dd, "division": "",
                            "track": "", "wins": w, "losses": l, "ties": t,
                            "games_played": w + l + t,
                            "record": f"{w}-{l}" + (f"-{t}" if t else ""), "games": games})
        schools.sort(key=lambda x: x["school"].casefold())
        out[s] = {"status": "final", "count": len(schools), "game_count": gc,
                  "source": "HSFHA-LA (hsfha.org), Oct 2026 import", "schools": schools}

    print("skipped canceled/unplayed games", skipped)
    for s, S in out.items():
        champs = sorted((x["class_"], x["school"]) for x in S["schools"] for g in x["games"]
                        if g["week"] == "State Championship" and g["result"] == "W" and x["class_"])
        nm = sum(1 for x in S["schools"] if x["school"] not in site)
        print(s, "schools", S["count"], "(not on site yet:", nm, ") team-games", S["game_count"],
              "champs:", champs)

    if write:
        data = json.load(gzip.open(ARCHIVE, "rt", encoding="utf-8"))
        for s, S in out.items():
            data["seasons"][s] = S
        data["seasons"] = dict(sorted(data["seasons"].items()))
        with gzip.open(ARCHIVE, "wt", encoding="utf-8", compresslevel=9) as fh:
            json.dump(data, fh, separators=(",", ":"), ensure_ascii=False)
        print("archive written", sorted(data["seasons"]))
    return out


if __name__ == "__main__":
    yr = next((a for a in sys.argv[3:] if "-" in a), None)
    main(sys.argv[1], sys.argv[2], "write" in sys.argv[3:],
         tuple(int(v) for v in yr.split("-")) if yr else None)
