"""
"Y'all" football history search — no AI, fully internal.

A fan types a question in plain English ("When is the last time West Monroe
has had 50 points put on them?"). This module:

  1. finds the team name(s) in the question, typo-tolerant, using the school
     names in our own football archive (closed schools included);
  2. pulls out numbers and filters (a year, "since 2010", home/away,
     district, playoffs);
  3. matches the wording to one of a fixed set of question TEMPLATES;
  4. answers from the archive data only — it never guesses.

If no template fits, it says so and shows example questions. Every question
is logged (see server.py) so we can see which templates to add next.

Data: football_archives_greenlit.json.gz (1990-2024, every game, both teams'
side) plus football_2025_clean.json (2025 regular season).
"""

from __future__ import annotations

import difflib
import gzip
import json
import os
import re
import threading
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ARCHIVE_PATH = os.path.join(HERE, "football_archives_greenlit.json.gz")
SEASON_2025_PATH = os.path.join(HERE, "football_2025_clean.json")

EXAMPLES = [
    "When is the last time West Monroe had 50 points put on them?",
    "When is the last time Neville scored 60?",
    "West Monroe vs Neville",
    "When is the last time Ruston beat West Monroe?",
    "Longest winning streak for John Curtis",
    "When was Haynesville's last undefeated season?",
    "Most points Evangel ever gave up",
    "When is the last time Acadiana was shut out?",
    "Barbe record in 2018",
    "Biggest win in Southwood history",
]


# ──────────────────────────────────────────────────────────────────────────
# GAME ROWS
# ──────────────────────────────────────────────────────────────────────────

class Game:
    __slots__ = (
        "season", "date", "school", "opponent", "pf", "pa", "result",
        "phase", "week", "home_away", "is_district", "playoff", "forfeit",
    )

    def __init__(self, **kw):
        for key in self.__slots__:
            setattr(self, key, kw.get(key))

    @property
    def sort_key(self):
        return (self.season, self.date or datetime(self.season, 12, 31),
                self.week or 0)

    def as_row(self):
        return {
            "season": self.season,
            "date": _fmt_date(self.date),
            "team": self.school,
            "opponent": self.opponent,
            "score": f"{self.pf}-{self.pa}",
            "result": self.result,
            "where": {"H": "Home", "A": "Away", "N": "Neutral"}.get(
                (self.home_away or "").upper(), ""),
            "phase": self.phase,
        }


def _parse_date(text):
    text = str(text or "").strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _fmt_date(date):
    if not date:
        return ""
    return f"{date.strftime('%b')} {date.day}, {date.year}"


def _parse_score(score):
    match = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", str(score or ""))
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def _clean_result(raw, pf, pa):
    raw = str(raw or "").upper().replace("(F)", "").strip()
    if raw in ("W", "L", "T"):
        return raw
    if pf > pa:
        return "W"
    if pf < pa:
        return "L"
    return "T"


def _week_number(value):
    match = re.search(r"\d+", str(value or ""))
    return int(match.group()) if match else 0


def build_games(archive, season_2025_rows=None):
    """Turn the archive JSON (+ optional 2025 rows) into Game objects."""
    games = []
    for season, data in (archive or {}).get("seasons", {}).items():
        try:
            year = int(season)
        except ValueError:
            continue
        for school in data.get("schools", []):
            for g in school.get("games", []):
                score = _parse_score(g.get("score"))
                if not score:
                    continue  # cancelled / no score recorded
                pf, pa = score
                phase = g.get("phase") or "Regular Season"
                games.append(Game(
                    season=year,
                    date=_parse_date(g.get("game_date")),
                    school=school["school"],
                    opponent=g.get("opponent") or "",
                    pf=pf, pa=pa,
                    result=_clean_result(g.get("result"), pf, pa),
                    phase=phase,
                    week=_week_number(g.get("week")),
                    home_away=g.get("home_away") or "",
                    is_district=bool(g.get("is_district")),
                    playoff=phase != "Regular Season",
                    forfeit=bool(g.get("forfeit")),
                ))
    for g in season_2025_rows or []:
        score = _parse_score(g.get("score"))
        if not score:
            continue
        pf, pa = score
        games.append(Game(
            season=int(g.get("season") or 2025),
            date=_parse_date(g.get("game_date")),
            school=g.get("school") or "",
            opponent=g.get("opponent") or "",
            pf=pf, pa=pa,
            result=_clean_result(g.get("win_loss"), pf, pa),
            phase="Regular Season",
            week=_week_number(g.get("week")),
            home_away=g.get("home_away") or "",
            is_district="(d)" in str(g.get("district") or ""),
            playoff=False,
            forfeit=False,
        ))
    games.sort(key=lambda game: game.sort_key)
    return games


# ──────────────────────────────────────────────────────────────────────────
# TEAM NAME MATCHING
# ──────────────────────────────────────────────────────────────────────────

# Single-word school names that are also everyday words. These only count
# as a team when the fan capitalizes them ("Many", not "how many").
COMMON_WORD_SCHOOLS = {
    "many", "delta", "liberty", "grant", "pine", "phoenix", "block", "iota",
    "jackson", "carroll", "madison", "independence", "assumption", "sunset",
    "redeemer", "episcopal", "lutheran", "parkway", "homer", "baker",
    "walker", "clinton", "fisher", "capitol", "franklin", "montgomery",
    "gilbert", "central", "union", "northeast", "northwest", "northside",
    "southside", "lakeside", "lakeview", "riverdale", "ridgewood",
}

# Words that are never part of a team name — keeps the typo matcher from
# turning question words into schools.
QUESTION_WORDS = set("""
a an the and or of in on at to for from by with vs versus v against since
before after between during last time times when what who how many much most
least ever has have had has been was were is are be being did does do done
put up scored score scores scoring points point pts gave give given giving
allowed allow allowing surrendered hung hang drop dropped beat beaten beats
lost lose loses losing loss losses won win wins winning record records
season seasons year years game games history all alltime biggest largest
worst best longest streak streaks row straight shut shutout shutouts out
undefeated unbeaten perfect playoff playoffs district home road away
regular them their they team teams school high football state title titles
championship championships series head between played play plays matchup
times margin blowout blowouts win's against them's get got getting more
than over under least fewest any which their there it its this that these
""".split())

GENERIC_SUFFIXES = {
    "christian", "academy", "catholic", "high", "school", "charter",
    "magnet", "prep", "preparatory", "episcopal", "community", "baptist",
}
GENERIC_PREFIXES = {"archbishop", "bishop", "captain"}

WORD_NUMBERS = {
    "ten": 10, "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50,
    "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90, "hundred": 100,
}


def _norm(text):
    text = str(text or "").lower()
    text = text.replace("&", " and ").replace("’", "'")
    text = re.sub(r"'s\b", "", text)
    text = re.sub(r"\bsaint\b", "st", text)
    text = re.sub(r"\bmount\b", "mt", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split())


class TeamIndex:
    """Maps normalized name variants -> list of official school names."""

    def __init__(self, school_names, opponent_names=()):
        self.schools = sorted(set(school_names))
        self.variants = {}          # tuple(tokens) -> set(official names)
        self.school_set = set(self.schools)
        for name in self.schools:
            for variant in self._variants(name):
                self.variants.setdefault(variant, set()).add(name)
        # Opponents we have no schedule for (out-of-state, etc.) can still
        # be matched as the OTHER team in a head-to-head question.
        for name in set(opponent_names) - self.school_set:
            key = tuple(_norm(name).split())
            if key and key not in self.variants:
                self.variants[key] = {name}
        self.max_len = max((len(k) for k in self.variants), default=1)

    @staticmethod
    def _variants(name):
        out = set()
        full = _norm(name)
        out.add(tuple(full.split()))
        # "Woodlawn - Shrev." -> "woodlawn" (ambiguity handled later)
        if " - " in name:
            base = _norm(name.split(" - ")[0])
            if base:
                out.add(tuple(base.split()))
        # "C.E. Byrd" -> "ce byrd" and "byrd"
        no_dots = _norm(name.replace(".", ""))
        out.add(tuple(no_dots.split()))
        # "John Curtis Christian" -> "john curtis", "Evangel Christian
        # Academy" -> "evangel"
        words = full.split()
        while len(words) > 1 and words[-1] in GENERIC_SUFFIXES:
            words = words[:-1]
            if len(" ".join(words)) >= 4:
                out.add(tuple(words))
        words = full.split()
        if len(words) > 1 and words[0] in GENERIC_PREFIXES:
            out.add(tuple(words[1:]))
        initials = re.match(r"^(?:[A-Z]\.\s?)+\s*(.+)$", name)
        if initials and " - " not in name:
            rest = _norm(initials.group(1))
            if rest and len(rest) > 3:
                out.add(tuple(rest.split()))
        return {v for v in out if v}

    def find(self, question):
        """Return list of matches in question order.

        Each match: {"names": [official...], "text": str, "start": i}
        (more than one name = ambiguous, e.g. "Woodlawn").
        """
        raw_tokens = re.sub(r"[^A-Za-z0-9.&'’\-\s]", " ", question).split()
        tokens, original = [], []
        for raw in raw_tokens:
            for piece in _norm(raw).split():
                tokens.append(piece)
                original.append(raw)
        used = [False] * len(tokens)
        found = []

        # 1) exact matches, longest first
        for length in range(self.max_len, 0, -1):
            for start in range(0, len(tokens) - length + 1):
                if any(used[start:start + length]):
                    continue
                key = tuple(tokens[start:start + length])
                names = self.variants.get(key)
                if not names:
                    continue
                if length == 1 and (key[0] in COMMON_WORD_SCHOOLS
                                    or key[0] in QUESTION_WORDS):
                    if not original[start][:1].isupper():
                        continue
                for i in range(start, start + length):
                    used[i] = True
                found.append({"names": sorted(names),
                              "text": " ".join(key), "start": start})

        # 2) typo-tolerant matches on what's left
        choices = {" ".join(k): v for k, v in self.variants.items()}
        for length in range(min(3, self.max_len), 0, -1):
            for start in range(0, len(tokens) - length + 1):
                window = tokens[start:start + length]
                if any(used[start:start + length]):
                    continue
                if any(t in QUESTION_WORDS or t.isdigit() for t in window):
                    continue
                text = " ".join(window)
                if len(text) < 5:
                    continue
                pool = [c for c in choices if len(c.split()) == length]
                best = difflib.get_close_matches(text, pool, n=1, cutoff=0.84)
                if not best:
                    continue
                names = choices[best[0]]
                if length == 1 and best[0] in COMMON_WORD_SCHOOLS:
                    continue
                for i in range(start, start + length):
                    used[i] = True
                found.append({"names": sorted(names), "text": best[0],
                              "start": start, "fuzzy": True})

        found.sort(key=lambda m: m["start"])
        # Prefer real schools over opponent-only names when ambiguous.
        for match in found:
            schools = [n for n in match["names"] if n in self.school_set]
            if schools:
                match["names"] = schools
        return found


# ──────────────────────────────────────────────────────────────────────────
# FILTERS & NUMBERS
# ──────────────────────────────────────────────────────────────────────────

def _extract_filters(q):
    """Season range, home/away, district, playoffs. Returns (filters, q')."""
    f = {"min_season": None, "max_season": None, "where": None,
         "district": False, "playoffs": None, "labels": []}

    m = re.search(r"\bbetween (19\d\d|20\d\d) and (19\d\d|20\d\d)\b", q)
    if m:
        a, b = sorted((int(m.group(1)), int(m.group(2))))
        f["min_season"], f["max_season"] = a, b
        f["labels"].append(f"{a}–{b}")
        q = q.replace(m.group(0), " ")
    m = re.search(r"\b(since|after) (19\d\d|20\d\d)\b", q)
    if m:
        year = int(m.group(2))
        f["min_season"] = year if m.group(1) == "since" else year + 1
        f["labels"].append(f"since {f['min_season']}")
        q = q.replace(m.group(0), " ")
    m = re.search(r"\bbefore (19\d\d|20\d\d)\b", q)
    if m:
        f["max_season"] = int(m.group(1)) - 1
        f["labels"].append(f"before {m.group(1)}")
        q = q.replace(m.group(0), " ")
    m = re.search(r"\b(?:in|during|for|of)?\s*(?:the )?(19\d\d|20\d\d)\b", q)
    if m and f["min_season"] is None and f["max_season"] is None:
        year = int(m.group(1))
        f["min_season"] = f["max_season"] = year
        f["labels"].append(str(year))
        q = q.replace(m.group(0), " ")

    if re.search(r"\b(at home|home game|home games|at their place)\b", q):
        f["where"] = "H"
        f["labels"].append("home games")
    elif re.search(r"\b(on the road|road game|road games|away)\b", q):
        f["where"] = "A"
        f["labels"].append("road games")
    if re.search(r"\bdistrict\b", q):
        f["district"] = True
        f["labels"].append("district games")
    if re.search(r"\bplayoffs?\b|\bpost ?season\b", q):
        f["playoffs"] = True
        f["labels"].append("playoffs")
    elif re.search(r"\bregular season\b", q):
        f["playoffs"] = False
        f["labels"].append("regular season")
    return f, q


def _apply_filters(games, f):
    out = []
    for g in games:
        if f["min_season"] is not None and g.season < f["min_season"]:
            continue
        if f["max_season"] is not None and g.season > f["max_season"]:
            continue
        if f["where"] and (g.home_away or "").upper() != f["where"]:
            continue
        if f["district"] and not g.is_district:
            continue
        if f["playoffs"] is True and not g.playoff:
            continue
        if f["playoffs"] is False and g.playoff:
            continue
        out.append(g)
    return out


def _extract_number(q):
    for match in re.finditer(r"\b(\d{1,3})\s*\+?\b", q):
        value = int(match.group(1))
        if 0 <= value <= 150:
            return value
    for word, value in WORD_NUMBERS.items():
        if re.search(rf"\b{word}\b", q):
            return value
    return None


# ──────────────────────────────────────────────────────────────────────────
# WORDING → INTENT
# ──────────────────────────────────────────────────────────────────────────

ALLOWED_WORDS = (r"put on|hung on|hang on|dropped on|drop on|gave up|give up|"
                 r"given up|giving up|allowed|allow|allowing|surrender|"
                 r"scored on|score on|scored against|let up|points against|"
                 r"on them|on em|hung|dropped|put up on|gotten scored on")
SCORED_WORDS = (r"scored|score|put up|puts up|dropped|drop|hung|hang|"
                r"points for|hit|reached|tallied")
PASSIVE = r"\b(was|were|got|been|get|gets|getting|be)\b"


def _has(q, pattern):
    return re.search(rf"\b(?:{pattern})\b", q) is not None


def detect_intent(q, team_count, number):
    """Return an intent name from normalized question text."""
    last = _has(q, r"last time|last|most recent|latest|when")

    if _has(q, r"shut ?out|shutouts|blanked|held scoreless|goose ?egg"):
        if re.search(PASSIVE + r".{0,15}\b(shut ?out|blanked)", q) or \
                _has(q, r"held scoreless|failed to score|didn t score"):
            return "shutouts_against"
        return "shutouts_for"
    if _has(q, r"streak|in a row|straight|consecutive"):
        if _has(q, r"los|losing|loss|losses|lost"):
            return "losing_streak"
        return "winning_streak"
    if _has(q, r"undefeated|unbeaten|perfect season|perfect regular season"):
        return "undefeated"
    if _has(q, r"winless|no wins|0 10|0 and 10|didn t win a game"):
        return "winless"

    if team_count >= 2:
        if number is not None and _has(q, ALLOWED_WORDS) and not \
                _has(q, r"scored|put up"):
            return "h2h_allowed_n"
        if number is not None:
            return "h2h_scored_n"
        if _has(q, r"beat|beaten|won|win|defeat|defeated"):
            return "h2h_last_win"
        if _has(q, r"lost|lose|loss"):
            return "h2h_last_loss"
        return "head_to_head"

    if _has(q, r"biggest win|largest win|biggest blowout|largest margin|"
               r"biggest margin|worst beating they gave|most lopsided win"):
        return "biggest_win"
    if _has(q, r"worst loss|biggest loss|worst defeat|largest loss|"
               r"worst beating|most lopsided loss"):
        return "worst_loss"
    if _has(q, r"most|highest|record for") and _has(q, r"point|points|pts"):
        if _has(q, ALLOWED_WORDS):
            return "most_allowed"
        return "most_scored"
    if _has(q, r"most points|highest scoring"):
        return "most_scored"

    if number is not None:
        if _has(q, ALLOWED_WORDS):
            return "allowed_n"
        if _has(q, SCORED_WORDS + r"|points|pts"):
            return "scored_n"

    if _has(q, r"best season|most wins"):
        return "best_season"
    if _has(q, r"worst season|most losses"):
        return "worst_season"
    if last and _has(q, r"lost|lose|loss|losses"):
        return "last_loss"
    if last and _has(q, r"won|win|wins|victory"):
        return "last_win"
    if _has(q, r"record|how many wins|how many games|all time|alltime|"
               r"wins|won"):
        return "record"
    return None


# ──────────────────────────────────────────────────────────────────────────
# ANSWERS
# ──────────────────────────────────────────────────────────────────────────

def _line(g):
    """'Oct 11, 2019: Neville 52, West Monroe 28'."""
    if g.pf >= g.pa:
        score = f"{g.school} {g.pf}, {g.opponent} {g.pa}"
    else:
        score = f"{g.opponent} {g.pa}, {g.school} {g.pf}"
    date = _fmt_date(g.date) or f"{g.season} season"
    extra = f" ({g.phase})" if g.playoff else ""
    return f"{date}: {score}{extra}"


def _record(games):
    w = sum(1 for g in games if g.result == "W")
    l = sum(1 for g in games if g.result == "L")
    t = sum(1 for g in games if g.result == "T")
    return w, l, t


def _fmt_record(w, l, t):
    return f"{w}-{l}" + (f"-{t}" if t else "")


def _opp_matches(g, names):
    target = {_norm(n) for n in names}
    return _norm(g.opponent) in target


def _season_records(games):
    seasons = {}
    for g in games:
        seasons.setdefault(g.season, []).append(g)
    return {s: _record(gs) for s, gs in seasons.items()}


def _streak(games, want):
    best, run, best_span, start = 0, 0, None, None
    for g in games:
        if g.result == want:
            if run == 0:
                start = g
            run += 1
            if run > best:
                best, best_span = run, (start, g)
        else:
            run = 0
    return best, best_span


class Answerer:
    def __init__(self, games):
        self.games = games
        self.by_school = {}
        opponents = set()
        for g in games:
            self.by_school.setdefault(g.school, []).append(g)
            opponents.add(g.opponent)
        self.index = TeamIndex(self.by_school.keys(), opponents)
        seasons = sorted({g.season for g in games})
        self.first_season = seasons[0] if seasons else None
        self.last_season = seasons[-1] if seasons else None

    # ── public ──
    def answer(self, question):
        question = str(question or "").strip()[:300]
        base = {"question": question, "matched": False, "rows": [],
                "coverage": self.coverage()}
        if len(question) < 3:
            return {**base, "answer": "Type a question about Louisiana high "
                    "school football history.", "examples": EXAMPLES}

        teams = self.index.find(question)
        ambiguous = [t for t in teams if len(t["names"]) > 1]
        if ambiguous:
            names = ambiguous[0]["names"]
            return {**base, "status": "ambiguous", "choices": names,
                    "answer": f"Which one did you mean: "
                              f"{', '.join(names[:-1])} or {names[-1]}?"}

        q = _norm(question)
        for t in teams:  # strip team names so they can't trigger keywords
            q = re.sub(rf"\b{re.escape(t['text'])}\b", " ", q, count=1)
        filters, q = _extract_filters(q)
        number = _extract_number(q)
        intent = detect_intent(q, len(teams), number)

        if not teams:
            return {**base, "status": "no_team", "intent": intent,
                    "answer": "I couldn't tell which school you're asking "
                              "about. Try including the school name, like "
                              "the examples below.",
                    "examples": EXAMPLES}
        if intent is None:
            return {**base, "status": "no_template",
                    "teams": [t["names"][0] for t in teams],
                    "answer": "I found the school, but I don't know how to "
                              "answer that kind of question yet. Try asking "
                              "it like one of these:",
                    "examples": EXAMPLES}

        team = teams[0]["names"][0]
        other = teams[1]["names"] if len(teams) > 1 else None
        if team not in self.by_school:
            # first name is opponent-only; swap if possible
            if other and other[0] in self.by_school:
                team, other = other[0], [team]
            else:
                return {**base, "status": "no_data",
                        "answer": f"I don't have game results for {team}."}
        games = _apply_filters(self.by_school[team], filters)
        if other:
            games = [g for g in games if _opp_matches(g, other)]

        handler = getattr(self, f"_a_{intent}")
        text, rows = handler(team, other[0] if other else None, games,
                             number)
        scope = f" ({', '.join(filters['labels'])})" if filters["labels"] \
            else ""
        return {**base, "matched": True, "status": "ok", "intent": intent,
                "teams": [team] + ([other[0]] if other else []),
                "filters": filters["labels"],
                "interpreted_as": INTENT_LABELS[intent].format(
                    team=team, other=other[0] if other else "",
                    n=number) + scope,
                "answer": text,
                "rows": [g.as_row() for g in rows][:25]}

    def coverage(self):
        if self.first_season is None:
            return "No football history loaded."
        return (f"Football, {self.first_season}–{self.last_season} "
                f"({self.last_season} regular season only)")

    # ── templates ──
    def _a_allowed_n(self, team, other, games, n):
        hits = [g for g in games if g.pa >= n]
        if not hits:
            return (f"No opponent has scored {n} or more on {team} in our "
                    f"records ({self.coverage()}).", [])
        last = hits[-1]
        return (f"The last time {team} gave up {n}+ points was {_line(last)}. "
                f"It has happened {len(hits)} time"
                f"{'s' if len(hits) != 1 else ''} in our records.",
                list(reversed(hits)))

    def _a_scored_n(self, team, other, games, n):
        hits = [g for g in games if g.pf >= n]
        if not hits:
            return (f"{team} hasn't scored {n} or more in a game in our "
                    f"records ({self.coverage()}).", [])
        last = hits[-1]
        return (f"The last time {team} scored {n}+ points was {_line(last)}. "
                f"It has happened {len(hits)} time"
                f"{'s' if len(hits) != 1 else ''} in our records.",
                list(reversed(hits)))

    def _a_h2h_scored_n(self, team, other, games, n):
        hits = [g for g in games if g.pf >= n]
        if not hits:
            return (f"{team} hasn't scored {n}+ against {other} in our "
                    "records.", [])
        return (f"The last time {team} scored {n}+ against {other} was "
                f"{_line(hits[-1])}. ({len(hits)} time"
                f"{'s' if len(hits) != 1 else ''} total.)",
                list(reversed(hits)))

    def _a_h2h_allowed_n(self, team, other, games, n):
        hits = [g for g in games if g.pa >= n]
        if not hits:
            return (f"{other} hasn't scored {n}+ against {team} in our "
                    "records.", [])
        return (f"The last time {other} scored {n}+ against {team} was "
                f"{_line(hits[-1])}. ({len(hits)} time"
                f"{'s' if len(hits) != 1 else ''} total.)",
                list(reversed(hits)))

    def _no_meetings(self, team, other):
        return (f"I don't have any games between {team} and {other} in our "
                f"records that match ({self.coverage()}).", [])

    def _a_head_to_head(self, team, other, games, n):
        if not games:
            return self._no_meetings(team, other)
        w, l, t = _record(games)
        return (f"{team} is {_fmt_record(w, l, t)} against {other} in our "
                f"records ({len(games)} game{'s' if len(games) != 1 else ''}"
                f"). Most recent: {_line(games[-1])}.",
                list(reversed(games)))

    def _a_h2h_last_win(self, team, other, games, n):
        if not games:
            return self._no_meetings(team, other)
        wins = [g for g in games if g.result == "W"]
        if not wins:
            return (f"{team} hasn't beaten {other} in our records "
                    f"({len(games)} meetings).", list(reversed(games)))
        return (f"The last time {team} beat {other} was {_line(wins[-1])}. "
                f"All-time: {_fmt_record(*_record(games))}.",
                list(reversed(games)))

    def _a_h2h_last_loss(self, team, other, games, n):
        if not games:
            return self._no_meetings(team, other)
        losses = [g for g in games if g.result == "L"]
        if not losses:
            return (f"{team} hasn't lost to {other} in our records "
                    f"({len(games)} meetings).", list(reversed(games)))
        return (f"The last time {team} lost to {other} was "
                f"{_line(losses[-1])}. All-time: "
                f"{_fmt_record(*_record(games))}.", list(reversed(games)))

    def _a_shutouts_for(self, team, other, games, n):
        hits = [g for g in games if g.pa == 0]
        if not hits:
            return f"{team} has no shutouts in our records.", []
        return (f"{team} has {len(hits)} shutout"
                f"{'s' if len(hits) != 1 else ''} in our records. The most "
                f"recent: {_line(hits[-1])}.", list(reversed(hits)))

    def _a_shutouts_against(self, team, other, games, n):
        hits = [g for g in games if g.pf == 0]
        if not hits:
            return f"{team} hasn't been shut out in our records.", []
        return (f"{team} has been shut out {len(hits)} times in our records. "
                f"The most recent: {_line(hits[-1])}.", list(reversed(hits)))

    def _streak_answer(self, team, games, want, word):
        best, span = _streak(games, want)
        if not best:
            return f"{team} has no {word} in our records.", []
        first, last = span
        rows = [g for g in games
                if first.sort_key <= g.sort_key <= last.sort_key]
        start = _fmt_date(first.date) or str(first.season)
        end = _fmt_date(last.date) or str(last.season)
        return (f"{team}'s longest {word} is {best} games, from {start} to "
                f"{end}.", rows)

    def _a_winning_streak(self, team, other, games, n):
        return self._streak_answer(team, games, "W", "winning streak")

    def _a_losing_streak(self, team, other, games, n):
        return self._streak_answer(team, games, "L", "losing streak")

    def _season_list(self, team, games, test, word):
        recs = _season_records(games)
        hits = sorted(s for s, r in recs.items() if test(r))
        if not hits:
            return f"{team} has no {word} seasons in our records.", []
        last = hits[-1]
        listed = ", ".join(f"{s} ({_fmt_record(*recs[s])})"
                           for s in reversed(hits))
        rows = [g for g in games if g.season == last]
        return (f"{team}'s last {word} season was {last} "
                f"({_fmt_record(*recs[last])}). All {word} seasons: "
                f"{listed}.", rows)

    def _a_undefeated(self, team, other, games, n):
        return self._season_list(
            team, games, lambda r: r[1] == 0 and r[0] >= 5, "undefeated")

    def _a_winless(self, team, other, games, n):
        return self._season_list(
            team, games, lambda r: r[0] == 0 and r[1] >= 5, "winless")

    def _a_best_season(self, team, other, games, n):
        recs = _season_records(games)
        if not recs:
            return f"No seasons found for {team}.", []
        best = max(recs, key=lambda s: (recs[s][0], -recs[s][1], s))
        return (f"{team}'s best season by wins was {best} "
                f"({_fmt_record(*recs[best])}).",
                [g for g in games if g.season == best])

    def _a_worst_season(self, team, other, games, n):
        recs = _season_records(games)
        if not recs:
            return f"No seasons found for {team}.", []
        worst = max(recs, key=lambda s: (recs[s][1], -recs[s][0], s))
        return (f"{team}'s worst season by losses was {worst} "
                f"({_fmt_record(*recs[worst])}).",
                [g for g in games if g.season == worst])

    def _a_biggest_win(self, team, other, games, n):
        wins = [g for g in games if g.result == "W" and g.pf > g.pa]
        if not wins:
            return f"No wins found for {team}.", []
        top = sorted(wins, key=lambda g: (g.pf - g.pa, g.sort_key),
                     reverse=True)[:10]
        g = top[0]
        return (f"{team}'s biggest win in our records was by "
                f"{g.pf - g.pa} points: {_line(g)}.", top)

    def _a_worst_loss(self, team, other, games, n):
        losses = [g for g in games if g.result == "L" and g.pa > g.pf]
        if not losses:
            return f"No losses found for {team}.", []
        top = sorted(losses, key=lambda g: (g.pa - g.pf, g.sort_key),
                     reverse=True)[:10]
        g = top[0]
        return (f"{team}'s worst loss in our records was by "
                f"{g.pa - g.pf} points: {_line(g)}.", top)

    def _a_most_scored(self, team, other, games, n):
        if not games:
            return f"No games found for {team}.", []
        top = sorted(games, key=lambda g: (g.pf, g.sort_key),
                     reverse=True)[:10]
        return (f"The most points {team} has scored in a game is "
                f"{top[0].pf}: {_line(top[0])}.", top)

    def _a_most_allowed(self, team, other, games, n):
        if not games:
            return f"No games found for {team}.", []
        top = sorted(games, key=lambda g: (g.pa, g.sort_key),
                     reverse=True)[:10]
        return (f"The most points {team} has given up in a game is "
                f"{top[0].pa}: {_line(top[0])}.", top)

    def _a_last_win(self, team, other, games, n):
        wins = [g for g in games if g.result == "W"]
        if not wins:
            return f"No wins found for {team}.", []
        return (f"{team}'s most recent win: {_line(wins[-1])}.",
                list(reversed(wins))[:10])

    def _a_last_loss(self, team, other, games, n):
        losses = [g for g in games if g.result == "L"]
        if not losses:
            return f"No losses found for {team}.", []
        return (f"{team}'s most recent loss: {_line(losses[-1])}.",
                list(reversed(losses))[:10])

    def _a_record(self, team, other, games, n):
        if not games:
            return f"No games found for {team} for that.", []
        w, l, t = _record(games)
        seasons = sorted({g.season for g in games})
        span = (str(seasons[0]) if len(seasons) == 1
                else f"{seasons[0]}–{seasons[-1]}")
        return (f"{team} is {_fmt_record(w, l, t)} ({span}).",
                list(reversed(games)))


INTENT_LABELS = {
    "allowed_n": "{team} games where the opponent scored {n}+",
    "scored_n": "{team} games scoring {n}+",
    "h2h_scored_n": "{team} scoring {n}+ vs {other}",
    "h2h_allowed_n": "{other} scoring {n}+ vs {team}",
    "head_to_head": "{team} vs {other}, all meetings",
    "h2h_last_win": "{team} wins over {other}",
    "h2h_last_loss": "{team} losses to {other}",
    "shutouts_for": "{team} shutouts (opponent held to 0)",
    "shutouts_against": "{team} held to 0 points",
    "winning_streak": "{team} longest winning streak",
    "losing_streak": "{team} longest losing streak",
    "undefeated": "{team} undefeated seasons",
    "winless": "{team} winless seasons",
    "best_season": "{team} best season by wins",
    "worst_season": "{team} worst season by losses",
    "biggest_win": "{team} biggest wins by margin",
    "worst_loss": "{team} worst losses by margin",
    "most_scored": "{team} most points in a game",
    "most_allowed": "{team} most points given up in a game",
    "last_win": "{team} most recent win",
    "last_loss": "{team} most recent loss",
    "record": "{team} record",
}


# ──────────────────────────────────────────────────────────────────────────
# LOADING (cached, thread-safe)
# ──────────────────────────────────────────────────────────────────────────

_ANSWERER = None
_LOCK = threading.Lock()


def get_answerer():
    global _ANSWERER
    if _ANSWERER is None:
        with _LOCK:
            if _ANSWERER is None:
                archive = {}
                if os.path.exists(ARCHIVE_PATH):
                    with gzip.open(ARCHIVE_PATH, "rt", encoding="utf-8") as fh:
                        archive = json.load(fh)
                rows = []
                if os.path.exists(SEASON_2025_PATH):
                    with open(SEASON_2025_PATH, encoding="utf-8") as fh:
                        rows = json.load(fh)
                _ANSWERER = Answerer(build_games(archive, rows))
    return _ANSWERER


def answer(question):
    return get_answerer().answer(question)
