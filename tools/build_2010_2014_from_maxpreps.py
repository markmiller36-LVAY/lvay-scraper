"""Build football seasons 2010-2014 from a MaxPreps crawl and merge them into the
site archive (football_archives_greenlit.json.gz). Also writes an Airtable-ready CSV.

Usage:
  python3 tools/build_2010_2014_from_maxpreps.py <crawl.json> <names.json> <out_csv> [write]

crawl.json: {"G": {gameId: [[slug, "10-11", "M-D-YYYY", oppSlug, oppState, oppName,
             "H"/"A", "W 21-19", isDistrict, isPlayoff], ...]}, "ST": {"slug|10-11": "5A District 1"},
             "NM": {slug: page title}}
names.json: {slug: "LVAY school name"}
"""
import csv, gzip, json, re, sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / "football_archives_greenlit.json.gz"
YRS = {"10-11": 2010, "11-12": 2011, "12-13": 2012, "13-14": 2013, "14-15": 2014}
ROUND = {0: "State Championship", 1: "Semifinal", 2: "Quarterfinal", 3: "Regional"}
# Schools whose site history starts later than 2010 (Mark's call). Their games still
# appear on opponents' schedules; they just get no season row of their own.
FIRST_SEASON = {"houma/covenant-christian-academy-lions": 2013}
ORD = {"1": 1, "Regional": 2, "Quarterfinal": 3, "Semifinal": 4, "State Championship": 5}


def parse_date(s, year):
    m = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{4})$", s or "")
    if not m:
        return None
    d = date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
    if not (date(year, 8, 15) <= d <= date(year, 12, 22)):
        return None
    return d


def parse_result(s):
    m = re.match(r"^\s*([WLTwlt])\s*(\d+)\s*-\s*(\d+)(.*)$", s or "")
    if not m:
        return None
    r, a, b, rest = m.group(1).upper(), int(m.group(2)), int(m.group(3)), m.group(4)
    ff = "FF" if "FF" in rest.upper() else ""
    if r == "W":
        return r, a, b, ff
    if r == "L":
        return r, b, a, ff
    return r, a, b, ff


def flip(r):
    return {"W": "L", "L": "W", "T": "T"}[r]


def main(crawl_path, names_path, out_csv, write=False):
    C = json.load(open(crawl_path))
    NAMES = json.load(open(names_path))
    ST = C["ST"]

    def ident(slug, yk):
        v = (ST.get(f"{slug}|{yk}") or "").strip()
        m = re.match(r"^(\d)A District (\d+)$", v)
        if m:
            return (f"{m.group(1)}A", m.group(2))
        # Class B / C and unclassified LHSAA schools: MaxPreps shows "_" or nothing.
        # Keep them (no class/district) when they are known LVAY schools and not MAIS.
        if slug in NAMES and not v.startswith("MAIS"):
            return ("", "")
        return None

    # 1. collect perspectives per (year, pair)
    persp = []
    for gid, ps in C["G"].items():
        for p in ps:
            slug, yk, ds, opp, ost, oname, ha, res, di, po = p
            year = YRS[yk]
            d = parse_date(ds, year)
            pr = parse_result(res)
            if not d or not pr:
                continue
            okey = opp if (ost == "la" and opp) else "x:" + oname.lower()
            persp.append(dict(slug=slug, yk=yk, year=year, d=d, opp=opp, ost=ost, oname=oname,
                              okey=okey, ha=ha, r=pr[0], ts=pr[1], os=pr[2], ff=pr[3], di=di, po=po))

    # 2. group into games: same year, same pair, dates within 2 days
    bypair = defaultdict(list)
    for x in persp:
        bypair[(x["year"], frozenset([x["slug"], x["okey"]]))].append(x)
    games = []
    for key, xs in bypair.items():
        xs.sort(key=lambda x: x["d"])
        cur = []
        for x in xs:
            if cur and (x["d"] - cur[0]["d"]).days > 2:
                games.append(cur)
                cur = []
            cur.append(x)
        if cur:
            games.append(cur)

    # 3. per-year week numbering
    rows = defaultdict(list)  # (year, slug) -> list of game dicts
    by_year_dates = defaultdict(list)
    for g in games:
        if not any(x["po"] for x in g):
            by_year_dates[g[0]["year"]].append(g[0]["d"])
    week1 = {}
    for y, ds in by_year_dates.items():
        cnt = defaultdict(int)
        for d in ds:
            cnt[d - timedelta(days=d.weekday())] += 1
        mondays = sorted(m for m, c in cnt.items() if c >= 40)
        week1[y] = mondays[0]
    # Each playoff bracket is a connected group of teams; its last week is its final.
    # (Brackets did not always finish the same weekend - e.g. 2014 Select finals were
    # Dec 5, a week before the Non-Select finals.)
    parent = {}

    def find(a):
        parent.setdefault(a, a)
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    for g in games:
        if any(x["po"] for x in g):
            y = g[0]["year"]
            a, b = find((y, g[0]["slug"])), find((y, g[0]["okey"]))
            parent[a] = b
    comp_final = {}
    for g in games:
        if any(x["po"] for x in g):
            y = g[0]["year"]
            wk = (g[0]["d"] - week1[y]).days // 7
            r = find((y, g[0]["slug"]))
            comp_final[r] = max(comp_final.get(r, 0), wk)
    year_final = {}
    for r, v in comp_final.items():
        year_final[r[0]] = max(year_final.get(r[0], 0), v)

    # Drop junk one-sided entries: (a) a team listed with two games on the same date
    # where another one is confirmed by both schools' pages; (b) "regular season"
    # games after the playoffs started (mostly basketball scores) that only one side lists.
    def confirmed(g):
        return len({x["slug"] for x in g}) >= 2 or g[0]["okey"].startswith("x:")
    po_start = {}
    pc = defaultdict(lambda: defaultdict(int))
    for g in games:
        if any(x["po"] for x in g):
            pc[g[0]["year"]][(g[0]["d"] - week1[g[0]["year"]]).days // 7] += 1
    for y, c in pc.items():
        po_start[y] = min(w for w, n in c.items() if n >= 40)
    by_day = defaultdict(list)
    for g in games:
        for t in (g[0]["slug"], g[0]["okey"]):
            by_day[(g[0]["year"], t, g[0]["d"])].append(id(g))
    conf_ids = {id(g) for g in games if confirmed(g)}
    drop = set()
    for g in games:
        if confirmed(g):
            continue
        y = g[0]["year"]
        if not any(x["po"] for x in g) and (g[0]["d"] - week1[y]).days // 7 >= po_start[y]:
            drop.add(id(g))
            continue
        for t in (g[0]["slug"], g[0]["okey"]):
            if any(o != id(g) and o in conf_ids for o in by_day[(y, t, g[0]["d"])]):
                drop.add(id(g))
    print("dropped one-sided junk games", len(drop))
    games = [g for g in games if id(g) not in drop]

    conflicts = []
    for g in games:
        y = g[0]["year"]
        yk = g[0]["yk"]
        po = any(x["po"] for x in g)
        wk = (g[0]["d"] - week1[y]).days // 7 + 1
        if po:
            cf = comp_final[find((y, g[0]["slug"]))]
            if year_final[y] - cf > 1:  # partial / stray bracket piece: use the season final
                cf = year_final[y]
            off = cf - (wk - 1)
            week = ROUND.get(off, "1")
        else:
            week = max(wk, 0)
        sides = {}
        for x in g:
            sides.setdefault(x["slug"], x)
        if len({(x["ts"], x["os"]) if x["slug"] == g[0]["slug"] else (x["os"], x["ts"]) for x in g}) > 1:
            conflicts.append((y, g[0]["slug"], g[0]["okey"], [(x["slug"], x["r"], x["ts"], x["os"]) for x in g]))
        base = g[0]
        teams = {base["slug"], base["okey"]}
        for t in teams:
            if t.startswith("x:") or not ident(t, yk):
                continue
            if y < FIRST_SEASON.get(t, 0):
                continue
            if t in sides:
                x = sides[t]
                r, ts, os_, ha, okey = x["r"], x["ts"], x["os"], x["ha"], x["okey"]
                oname, ost = x["oname"], x["ost"]
            else:
                x = base
                r, ts, os_ = flip(x["r"]), x["os"], x["ts"]
                ha = {"H": "A", "A": "H"}.get(x["ha"], "")
                okey, oname, ost = x["slug"], "", "la"
            d = x["d"]
            rows[(y, t)].append(dict(d=d, week=week, phase="Playoffs" if po else "Regular Season",
                                    okey=okey, oname=oname, ost=ost, ha=ha, r=r, ts=ts, os=os_,
                                    ff=x["ff"]))

    def oname_of(okey, oname):
        if okey.startswith("x:"):
            return oname
        return NAMES.get(okey) or oname or C.get("NM", {}).get(okey) or okey.split("/")[1]

    # 4. build archive seasons + csv rows
    out = {}
    csv_rows = []
    for y in sorted({k[0] for k in rows}):
        yk = [k for k, v in YRS.items() if v == y][0]
        schools = []
        gc = 0
        for (yy, slug), gs in rows.items():
            if yy != y:
                continue
            cls, dist = ident(slug, yk)
            name = NAMES.get(slug) or slug.split("/")[1]
            gs.sort(key=lambda g: (g["d"], ORD.get(str(g["week"]), 0)))
            games_out = []
            for g in gs:
                oi = ident(g["okey"], yk) if not g["okey"].startswith("x:") else None
                on = oname_of(g["okey"], g["oname"])
                is_d = bool(cls and oi and oi == (cls, dist) and g["phase"] == "Regular Season")
                games_out.append({
                    "week": g["week"], "phase": g["phase"],
                    "game_date": f"{g['d'].month}/{g['d'].day}/{g['d'].year}",
                    "opponent": on, "home_away": g["ha"], "result": g["r"],
                    "score": f"{g['ts']}-{g['os']}", "opp_division": "",
                    "opp_class": oi[0] if oi else "", "is_district": is_d,
                    "opponent_internal": oi is not None, "forfeit": g["ff"],
                    "out_of_state": g["ost"] not in ("la", ""),
                })
                csv_rows.append({
                    "School": name, "Classification": cls, "District": f"{dist}-{cls}" if cls else "",
                    "Season": y, "RS/Playoffs": g["phase"],
                    "Date": f"{g['d'].month}/{g['d'].day}/{g['d'].year}",
                    "Week": (f"Week {g['week']}" if g["phase"] == "Regular Season"
                             else ("Round 1" if g["week"] == "1" else g["week"])),
                    "Opponent": on, "Opp Class": oi[0] if oi else "", "H/A": g["ha"],
                    "W/L": g["r"], "Team Score": g["ts"], "Opp Score": g["os"],
                    "Margin": g["ts"] - g["os"], "District Game": "Y" if is_d else "",
                    "Source": "MaxPreps (Oct 2026 import)",
                })
            w = sum(x["result"] == "W" for x in games_out)
            l = sum(x["result"] == "L" for x in games_out)
            t = sum(x["result"] == "T" for x in games_out)
            gc += len(games_out)
            schools.append({"school": name, "class_": cls, "district": dist, "division": "",
                            "track": "", "wins": w, "losses": l, "ties": t,
                            "games_played": w + l + t,
                            "record": f"{w}-{l}" + (f"-{t}" if t else ""), "games": games_out})
        schools.sort(key=lambda s: s["school"].casefold())
        out[str(y)] = {"status": "final", "count": len(schools), "game_count": gc,
                       "schools": schools}

    # 5. report
    for y, S in sorted(out.items()):
        champs = sorted({s["school"] for s in S["schools"] for g in s["games"]
                         if g["week"] == "State Championship" and g["result"] == "W"})
        print(y, "schools", S["count"], "team-games", S["game_count"], "champs:", champs)
    print("score conflicts", len(conflicts))
    for c in conflicts[:15]:
        print("  ", c)

    with open(out_csv, "w", newline="") as f:
        wtr = csv.DictWriter(f, fieldnames=list(csv_rows[0].keys()))
        wtr.writeheader()
        wtr.writerows(csv_rows)
    print("csv rows", len(csv_rows))

    if write:
        data = json.load(gzip.open(ARCHIVE, "rt", encoding="utf-8"))
        for y, S in out.items():
            data["seasons"][y] = S
        data["seasons"] = dict(sorted(data["seasons"].items()))
        with gzip.open(ARCHIVE, "wt", encoding="utf-8", compresslevel=9) as fh:
            json.dump(data, fh, separators=(",", ":"), ensure_ascii=False)
        print("archive written", sorted(data["seasons"]))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], len(sys.argv) > 4 and sys.argv[4] == "write")
