"""List the games most likely to be wrong in one HSFHA football season, by week,
so newspaper hunting can target them instead of re-checking whole weeks.

Usage: python3 tools/hsfha_suspects.py <season> [first_week] [last_week]

Flags (all after the newspaper fixes in tools/hsfha_fixes.tsv are applied):
  DOUBLE  a school has two games within 3 days: one of them is on the wrong date,
          is a duplicate, or belongs to a different school with the same name
  REPEAT  the same two schools with the same score on two dates (one is a copy)
  GAP     an LHSAA school has no game this week but plays the weeks before and after
Each week shows the Thu-Sun range to search and which paper covers each school."""
import collections
import contextlib
import csv
import datetime as D
import io
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from build_football_from_hsfha import apply_fixes  # noqa: E402

SRC = Path(__file__).parent / "data_1980_1999" / "hsfha_la_football_1980_1999_dedup.tsv"
PAPER = {}
for paper, parishes in {
    "Shreveport Times": "CO BO WR DE RE CI BI SB NA",
    "Monroe News-Star": "OU LN UN MO RI FR TN MA EC WC CW JA CT LA",
    "Alexandria Town Talk": "RA AV GR VN WI EV AL BE",
    "Lafayette Advertiser": "LY VM SM AD II SL SR JD",
    "Lake Charles American Press": "CC CM",
    "Baton Rouge Advocate": "EB LV AE IV PO WB EF WF SH AS",
    "Hammond Daily Star": "TA WO ST",
    "Times-Picayune": "OR JF PL SE SC SJ SO",
    "Houma Courier": "TR LO",
}.items():
    for p in parishes.split():
        PAPER[p] = paper


def paper(code):
    return PAPER.get(code[3:5], "?") if code.startswith("LA.") else ""


def main():
    season = sys.argv[1]
    first = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    last = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    R = list(csv.DictReader(open(SRC, encoding="utf-8"), delimiter="\t"))
    with contextlib.redirect_stdout(io.StringIO()):
        apply_fixes(R)
    R = [x for x in R if x["season"] == season and x["date"] and (x["played"] == "1" or x["team_dec"] == "W")]
    reg = [x for x in R if x["gametype"] == "Regular"]
    # week 1 = first Mon-Sun week with 25+ LHSAA-vs-LHSAA regular games (same rule as the site)
    mon = lambda d: D.date.fromisoformat(d) - D.timedelta(days=D.date.fromisoformat(d).weekday())
    cnt = collections.Counter(mon(x["date"]) for x in reg if x["team_org"] == "LHSAA" and x["opp_org"] == "LHSAA")
    wk1 = min(m for m, n in cnt.items() if n >= 25)
    week = lambda d: (mon(d) - wk1).days // 7 + 1

    name, org, games = {}, {}, collections.defaultdict(list)
    for x in R:
        for s, o in (("team", "opp"), ("opp", "team")):
            c = x[s + "_code"]
            if not c.startswith("LA."):
                continue
            name[c], org[c] = x[s], x[s + "_org"]
            games[c].append(x)

    def line(x):
        return f"{x['date'][5:]}  {x['team']} {x['team_score']}, {x['opp']} {x['opp_score']}  (id {x['id']})"

    out = collections.defaultdict(lambda: {"DOUBLE": [], "REPEAT": [], "GAP": []})
    for c, gs in games.items():
        gs.sort(key=lambda x: x["date"])
        for a, b in zip(gs, gs[1:]):
            if (D.date.fromisoformat(b["date"]) - D.date.fromisoformat(a["date"])).days <= 3 and a["id"] != b["id"]:
                out[week(b["date"])]["DOUBLE"].append((paper(c), f"{name[c]}: two games\n      {line(a)}\n      {line(b)}"))
    seen = collections.defaultdict(list)
    for x in R:
        seen[(frozenset([x["team_code"], x["opp_code"]]), frozenset([x["team_score"], x["opp_score"]]))].append(x)
    for xs in seen.values():
        if len(xs) > 1 and len({x["date"] for x in xs}) > 1:
            xs.sort(key=lambda x: x["date"])
            out[week(xs[-1]["date"])]["REPEAT"].append(
                (paper(xs[0]["team_code"]), "same game twice\n      " + "\n      ".join(line(x) for x in xs)))
    for c, gs in games.items():
        if org[c] != "LHSAA":
            continue
        wks = {week(x["date"]) for x in gs if x["gametype"] == "Regular"}
        for w in range(max(first, 2), min(last, 10)):
            if w not in wks and w - 1 in wks and w + 1 in wks:
                out[w]["GAP"].append((paper(c), name[c]))

    total = collections.Counter()
    print(f"HSFHA suspects, {season} football, weeks {first}-{last}\n")
    for w in range(first, last + 1):
        fri = wk1 + D.timedelta(days=7 * (w - 1) + 4)
        o = out[w]
        if not any(o.values()):
            continue
        print(f"=== Week {w}: Fri {fri:%b %d %Y}  (search Thu {fri - D.timedelta(1):%m/%d} - Sun {fri + D.timedelta(2):%m/%d})")
        for kind in ("DOUBLE", "REPEAT"):
            for p, text in sorted(o[kind]):
                print(f"  {kind:6} [{p}] {text}")
                total[kind] += 1
        if o["GAP"]:
            by = collections.defaultdict(list)
            for p, n in o["GAP"]:
                by[p].append(n)
            print("  GAP    no game on file:")
            for p in sorted(by):
                print(f"         [{p}] {', '.join(sorted(by[p]))}")
                total["GAP"] += len(by[p])
        print()
    print("Totals:", dict(total))


if __name__ == "__main__":
    main()
