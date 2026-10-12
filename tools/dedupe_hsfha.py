"""Merge games HSFHA lists twice (same season, date and the same two schools).
Keeps the lower game id; when the other listing has a different score it is noted.
Usage: python3 tools/dedupe_hsfha.py <in.tsv> <out.tsv>"""
import csv, sys
from collections import defaultdict
src, out = sys.argv[1], sys.argv[2]
R = list(csv.DictReader(open(src, encoding="utf-8"), delimiter="\t"))
groups = defaultdict(list)
for x in R:
    key = (x["season"], x["date"], frozenset([x["team_code"] or x["team"], x["opp_code"] or x["opp"]]))
    groups[key].append(x)
keep, merged, differ = [], 0, 0
for key, xs in groups.items():
    if not key[1] or len(xs) == 1:
        keep.extend(xs); continue
    xs.sort(key=lambda x: int(x["id"]))
    k = xs[0]
    for o in xs[1:]:
        merged += 1
        same_order = o["team_code"] == k["team_code"]
        ts, os_ = (o["team_score"], o["opp_score"]) if same_order else (o["opp_score"], o["team_score"])
        if (ts, os_) != (k["team_score"], k["opp_score"]):
            differ += 1
            note = f"HSFHA also lists this game as {k['team']} {ts}, {k['opp']} {os_} (game {o['id']})"
            k["notes"] = (k["notes"] + "; " if k["notes"] else "") + note
        if k["played"] != "1" and o["played"] == "1":
            for f in ("team_score", "opp_score", "team_dec", "opp_dec", "played"):
                k[f] = o[f] if same_order or f == "played" else o[{"team_score": "opp_score", "opp_score": "team_score", "team_dec": "opp_dec", "opp_dec": "team_dec"}[f]]
    keep.append(k)
keep.sort(key=lambda x: (x["season"], x["date"], int(x["id"])))
with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(R[0]), delimiter="\t"); w.writeheader(); w.writerows(keep)
print("games in", len(R), "out", len(keep), "merged", merged, "with different score", differ)
