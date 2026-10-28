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
    {
        # District 2 tournament bracket (sent by Mark, Oct 28, 2026). Tuesday's
        # games are already on LHSAA: Crowley beat Opelousas then lost to
        # Iota; Eunice beat Northwest then lost to Church Point. Thursday's
        # consolation = losers of games 1 & 2, finals = the two semifinal winners.
        "tournament": "Iota",
        "venue": "Iota High School",
        "matches": [
            ("2026-10-29", "5:00 PM", "Consolation", "Opelousas", "Northwest"),
            ("2026-10-29", "7:00 PM", "Finals", "Iota", "Church Point"),
        ],
    },
    {
        # "Brusly Round Robin" (sent by Mark, Oct 28, 2026). Saturday morning.
        "tournament": "Brusly",
        "venue": "Brusly High School",
        "matches": [
            ("2026-10-31", "9:00 AM", "Main Gym", "Covington", "Woodlawn - B.R."),
            ("2026-10-31", "9:00 AM", "Aux Gym", "Houma Christian", "Baton Rouge"),
            ("2026-10-31", "10:00 AM", "Main Gym", "Brusly", "Houma Christian"),
            ("2026-10-31", "10:00 AM", "Aux Gym", "Woodlawn - B.R.", "Baton Rouge"),
            ("2026-10-31", "11:00 AM", "Main Gym", "Brusly", "Covington"),
            ("2026-10-31", "12:00 PM", "Main Gym", "Houma Christian", "Covington"),
        ],
    },
    {
        # Natchitoches Central "Saturday Volleyball Tournament" (sent by Mark,
        # Oct 28, 2026). Best 2 of 3.
        "tournament": "Natchitoches Central",
        "venue": "Natchitoches Central High School",
        "matches": [
            ("2026-10-31", "9:00 AM", "Gym 1", "Marksville", "Natchitoches Central"),
            ("2026-10-31", "9:00 AM", "Gym 2", "Neville", "Northwood - Shrev."),
            ("2026-10-31", "10:00 AM", "Gym 1", "Neville", "Natchitoches Central"),
            ("2026-10-31", "10:00 AM", "Gym 2", "Marksville", "Acadiana"),
            ("2026-10-31", "11:00 AM", "Gym 1", "Wossman", "Northwood - Shrev."),
            ("2026-10-31", "12:00 PM", "Gym 1", "Acadiana", "Wossman"),
        ],
    },
    {
        # Episcopal of Acadiana "Spooktacular" (PDF sent by Mark, Oct 28, 2026).
        # Pool play only. Saturday's Gold/Silver/Bronze brackets depend on pool
        # finishes, so add those matches once Friday's results are in.
        # The PDF says "Ascension Catholic" in Pool A (LHSAA's participant list
        # shows Ascension Episcopal instead); the host's sheet is used here.
        "tournament": "Episcopal of Acadiana",
        "venue": "Episcopal School of Acadiana",
        "matches": [
            # Thursday, Oct 29: Pools A, B, C
            ("2026-10-29", "4:30 PM", "Court 1", "Ascension Catholic", "Acad. of Sacred Heart - G.C."),
            ("2026-10-29", "4:30 PM", "Court 2", "Highland Baptist", "Westlake"),
            ("2026-10-29", "4:30 PM", "Court 3 (MS Gym)", "Assumption", "Berwick"),
            ("2026-10-29", "5:30 PM", "Court 1", "Ascension Catholic", "St. Thomas More"),
            ("2026-10-29", "5:30 PM", "Court 2", "Westlake", "E.D. White"),
            ("2026-10-29", "5:30 PM", "Court 3 (MS Gym)", "Assumption", "Barbe"),
            ("2026-10-29", "6:30 PM", "Court 1", "St. Thomas More", "Acad. of Sacred Heart - G.C."),
            ("2026-10-29", "6:30 PM", "Court 2", "E.D. White", "Highland Baptist"),
            ("2026-10-29", "6:30 PM", "Court 3 (MS Gym)", "Berwick", "Barbe"),
            # Friday, Oct 30: Pools D, E, F, G, H
            ("2026-10-30", "4:30 PM", "Court 1", "Lafayette Christian", "Acadiana Renaissance Charter"),
            ("2026-10-30", "4:30 PM", "Court 2", "Teurlings Catholic", "Vandebilt Catholic"),
            ("2026-10-30", "4:30 PM", "Court 3 (MS Gym)", "Notre Dame", "Catholic - N.I."),
            ("2026-10-30", "4:30 PM", "Court 4 (MS Gym)", "Cabrini", "Southside"),
            ("2026-10-30", "5:30 PM", "Court 1", "Dominican", "Episcopal of Acadiana"),
            ("2026-10-30", "5:30 PM", "Court 2", "Lafayette Christian", "St. Joseph's Academy"),
            ("2026-10-30", "5:30 PM", "Court 3 (MS Gym)", "Notre Dame", "Parkview Baptist"),
            ("2026-10-30", "5:30 PM", "Court 4 (MS Gym)", "Cabrini", "Westminster Christian"),
            ("2026-10-30", "6:30 PM", "Court 1", "Iowa", "Dominican"),
            ("2026-10-30", "6:30 PM", "Court 2", "Dutchtown", "Vandebilt Catholic"),
            ("2026-10-30", "6:30 PM", "Court 3 (MS Gym)", "St. Joseph's Academy", "Acadiana Renaissance Charter"),
            ("2026-10-30", "6:30 PM", "Court 4 (MS Gym)", "Westminster Christian", "Southside"),
            ("2026-10-30", "7:30 PM", "Court 1", "Iowa", "Episcopal of Acadiana"),
            ("2026-10-30", "7:30 PM", "Court 2", "Dutchtown", "Teurlings Catholic"),
            ("2026-10-30", "7:30 PM", "Court 3 (MS Gym)", "Parkview Baptist", "Catholic - N.I."),
        ],
    },
]


def _court_label(court):
    """Dunham numbers its courts; other hosts name gyms ("Main Gym", "Finals")."""
    return f"Court {court}" if isinstance(court, int) else str(court)


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
        rows.sort(key=lambda r: (r["date"], _time_key(r["time"]), str(r["court"])))
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
            "location": (f'{row["venue"]} · ' if row["venue"] else "") + _court_label(row["court"]),
            "source": "lvay_pending",
        })
    return games + added
