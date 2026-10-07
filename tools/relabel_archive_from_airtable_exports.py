"""Relabel class/district in football_archives_greenlit.json.gz from the corrected
LHSAA FOOTBALL HISTORY Airtable exports (one JSON per season), then recompute
opponent_internal / opp_class / is_district flags and W-L records.

Usage: python3 tools/relabel_archive_from_airtable_exports.py <export_dir>
<export_dir> holds <season>.json files as returned by Airtable list_records.
"""
import gzip, json, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE = ROOT / "football_archives_greenlit.json.gz"
F_SCHOOL, F_CLASS, F_DIST = "fldrpUTYFnm2cnF7p", "fld6TmyWYfSlEZCVC", "fldlhgMU3lRTbUhqu"


def nm(v):
    return v.get("name") if isinstance(v, dict) else v


def dist_num(raw):
    m = re.match(r"^(\d+)", str(raw or ""))
    return m.group(1) if m else str(raw or "")


def archive_name(base_name):
    return base_name.replace("-Closed", "").strip()


def main(export_dir):
    data = json.load(gzip.open(ARCHIVE, "rt"))
    for season, S in data["seasons"].items():
        path = Path(export_dir) / f"{season}.json"
        if not path.exists():
            print(season, "no export, skipped")
            continue
        labels = {}
        for r in json.load(open(path))["records"]:
            f = r["cellValuesByFieldId"]
            sch = archive_name(f.get(F_SCHOOL) or "")
            c, d = nm(f.get(F_CLASS)), dist_num(nm(f.get(F_DIST)))
            if sch and c and d:
                labels.setdefault(sch, (c, d))
        changed, flips, missing = 0, 0, []
        for s in S["schools"]:
            lab = labels.get(s["school"])
            if not lab:
                missing.append(s["school"])
                continue
            if (s["class_"], str(s["district"])) != lab:
                changed += 1
            s["class_"], s["district"] = lab
        ident = {x["school"]: (x["class_"], str(x["district"])) for x in S["schools"]}
        for x in S["schools"]:
            me = ident[x["school"]]
            for g in x["games"]:
                o = ident.get(g["opponent"])
                if o:
                    g["opponent_internal"] = True
                    g["opp_class"] = o[0]
                    nd = bool(me[1] and me == o and str(g["phase"]).lower().startswith("regular"))
                    flips += nd != g["is_district"]
                    g["is_district"] = nd
            w = sum(g["result"] == "W" for g in x["games"])
            l = sum(g["result"] == "L" for g in x["games"])
            t = sum(g["result"] == "T" for g in x["games"])
            x.update(wins=w, losses=l, ties=t, games_played=w + l + t,
                     record=f"{w}-{l}" + (f"-{t}" if t else ""))
        print(season, "relabeled", changed, "district-flag flips", flips, "not in export:", missing)
    json.dump(data, gzip.open(ARCHIVE, "wt"), separators=(",", ":"))


if __name__ == "__main__":
    main(sys.argv[1])
