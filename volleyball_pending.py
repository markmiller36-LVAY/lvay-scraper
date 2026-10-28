"""Volleyball tournament matches that LHSAA hasn't posted yet.

LHSAA only lists tournament matches after they're played (the match rows are
entered with the results). Before that, the site would show nothing for a
tournament weekend, so fans and the Powerboard can't see what's coming.

This file holds tournament schedules Mark gets straight from the host school
(Oct 28, 2026: Dunham tournament at Team Sportsplex). The schedules feed adds
each match to BOTH schools as an unplayed tournament match, until LHSAA posts
the real row for that pairing on that date. LHSAA rows always win; these are
display-only placeholders and never touch the database or the power ratings
(unplayed matches earn no points).

To add another tournament: append a dict to PENDING_TOURNAMENTS using the
school names exactly as the site spells them (volleyball_rankings.school).
"""
from collections import defaultdict
from datetime import date, datetime, timedelta

# A placeholder stays visible this many days after its date, in case schools
# are slow to enter results. After that it disappears even if LHSAA never
# posts the match.
STALE_AFTER_DAYS = 3

PENDING_TOURNAMENTS = [
    {
        # Host's schedule "VERSION THREE" (sent by Mark, Oct 28, 2026).
        "tournament": "Dunham",
        "venue": "Team Sportsplex, 7122 Perkins Rd, Baton Rouge",
        "matches": [
            # Friday, Oct 30 (Pink Day)
            ("2026-10-30", "4:00 PM", 1, "Central Private", "Fontainebleau"),
            ("2026-10-30", "4:00 PM", 2, "Prairieville", "Northlake Christian"),
            ("2026-10-30", "4:00 PM", 3, "Denham Springs", "Acad. of Sacred Heart - N.O."),
            ("2026-10-30", "4:00 PM", 4, "St. Scholastica", "Calvary Baptist"),
            ("2026-10-30", "4:00 PM", 5, "Riverside Academy", "Archbishop Hannan"),
            ("2026-10-30", "5:00 PM", 1, "Northshore", "St. Michael the Archangel"),
            ("2026-10-30", "5:00 PM", 2, "Prairieville", "Fontainebleau"),
            ("2026-10-30", "5:00 PM", 3, "Archbishop Hannan", "Central Lafourche"),
            ("2026-10-30", "5:00 PM", 4, "Walker", "Hahnville"),
            ("2026-10-30", "5:00 PM", 5, "Central Private", "Riverside Academy"),
            ("2026-10-30", "6:00 PM", 1, "St. Scholastica", "Hahnville"),
            ("2026-10-30", "6:00 PM", 2, "Northlake Christian", "Northshore"),
            ("2026-10-30", "6:00 PM", 3, "Dunham", "Central - B.R."),
            ("2026-10-30", "6:00 PM", 4, "Calvary Baptist", "East Ascension"),
            ("2026-10-30", "6:00 PM", 5, "Acad. of Sacred Heart - N.O.", "Walker"),
            ("2026-10-30", "7:00 PM", 2, "Central - B.R.", "St. Michael the Archangel"),
            ("2026-10-30", "7:00 PM", 3, "Dunham", "Central Lafourche"),
            # Saturday, Oct 31 (Costumes)
            ("2026-10-31", "9:00 AM", 1, "South Lafourche", "St. Michael the Archangel"),
            ("2026-10-31", "9:00 AM", 2, "Terrebonne", "Walker"),
            ("2026-10-31", "9:00 AM", 3, "Archbishop Chapelle", "East Ascension"),
            ("2026-10-31", "9:00 AM", 4, "Central Private", "Hahnville"),
            ("2026-10-31", "9:00 AM", 5, "Thomas Jefferson", "Denham Springs"),
            ("2026-10-31", "10:00 AM", 1, "St. Michael the Archangel", "St. Scholastica"),
            ("2026-10-31", "10:00 AM", 2, "South Lafourche", "Northlake Christian"),
            ("2026-10-31", "10:00 AM", 3, "East Ascension", "Thomas Jefferson"),
            ("2026-10-31", "10:00 AM", 4, "Archbishop Chapelle", "Terrebonne"),
            ("2026-10-31", "10:00 AM", 5, "Archbishop Hannan", "Prairieville"),
            ("2026-10-31", "11:00 AM", 1, "Central - B.R.", "Central Private"),
            ("2026-10-31", "11:00 AM", 2, "Calvary Baptist", "Walker"),
            ("2026-10-31", "11:00 AM", 3, "Acad. of Sacred Heart - N.O.", "Northshore"),
            ("2026-10-31", "11:00 AM", 4, "Central Lafourche", "Riverside Academy"),
            ("2026-10-31", "11:00 AM", 5, "Denham Springs", "Archbishop Hannan"),
            ("2026-10-31", "12:00 PM", 1, "Hahnville", "Fontainebleau"),
            ("2026-10-31", "12:00 PM", 2, "Northlake Christian", "Archbishop Chapelle"),
            ("2026-10-31", "12:00 PM", 3, "East Ascension", "St. Scholastica"),
            ("2026-10-31", "12:00 PM", 4, "Central - B.R.", "Thomas Jefferson"),
            ("2026-10-31", "12:00 PM", 5, "Terrebonne", "Denham Springs"),
            ("2026-10-31", "1:00 PM", 1, "Prairieville", "Acad. of Sacred Heart - N.O."),
            ("2026-10-31", "1:00 PM", 2, "Fontainebleau", "South Lafourche"),
            ("2026-10-31", "1:00 PM", 3, "Dunham", "Riverside Academy"),
            ("2026-10-31", "1:00 PM", 4, "Terrebonne", "Northshore"),
            ("2026-10-31", "1:00 PM", 5, "Central Lafourche", "Calvary Baptist"),
            ("2026-10-31", "2:00 PM", 3, "Dunham", "South Lafourche"),
        ],
    },
]


def _time_key(value):
    try:
        return datetime.strptime(value, "%I:%M %p").time()
    except ValueError:
        return datetime.max.time()


def _parse_date(value):
    text = str(value or "").strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    return None


def pending_by_school(today=None, tournaments=None):
    """{school: [match dict, ...]} for every placeholder still in its window."""
    today = today or date.today()
    tournaments = PENDING_TOURNAMENTS if tournaments is None else tournaments
    out = defaultdict(list)
    for t in tournaments:
        for day, time_, court, a, b in t["matches"]:
            d = datetime.strptime(day, "%Y-%m-%d").date()
            if d < today - timedelta(days=STALE_AFTER_DAYS):
                continue
            for me, opp in ((a, b), (b, a)):
                out[me].append({
                    "date": d, "time": time_, "court": court, "opponent": opp,
                    "tournament": t["tournament"], "venue": t.get("venue", ""),
                })
    for rows in out.values():
        rows.sort(key=lambda r: (r["date"], _time_key(r["time"]), r["court"]))
    return out


def merge_pending_games(school, games, school_info, pending=None):
    """Append this school's not-yet-posted tournament matches to its games.

    `games` are the LHSAA rows for the school. For each opponent/date pair,
    placeholders are only added beyond the number of matches LHSAA already
    lists, so once LHSAA posts a match its placeholder disappears.
    `school_info` maps school name -> {"division", "district"}.
    """
    pending = pending_by_school() if pending is None else pending
    mine = pending.get(school) or []
    if not mine:
        return games
    posted = defaultdict(int)
    for g in games:
        d = _parse_date(g.get("game_date"))
        posted[(d, str(g.get("opponent") or "").strip().casefold())] += 1
    used = defaultdict(int)
    per_day = defaultdict(int)
    for g in games:
        per_day[_parse_date(g.get("game_date"))] += 1
    added = []
    for row in mine:
        key = (row["date"], row["opponent"].casefold())
        used[key] += 1
        if used[key] <= posted[key]:
            continue
        per_day[row["date"]] += 1
        info = school_info.get(row["opponent"]) or {}
        added.append({
            "game_date": f'{row["date"].month}/{row["date"].day}/{row["date"].year}',
            "opponent": row["opponent"],
            "opp_division": info.get("division") or "",
            "opp_district": str(info.get("district") or ""),
            "is_district": 0,
            "is_tournament": 1,
            "tournament_name": row["tournament"],
            "match_num": per_day[row["date"]],
            "home_away": "",
            "result": "",
            "score": None,
            "counts_for_pr": 1,
            "match_time": row["time"],
            "location": f'{row["venue"]} · Court {row["court"]}' if row["venue"] else f'Court {row["court"]}',
            "source": "lvay_pending",
        })
    return games + added
