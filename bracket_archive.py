"""Past-season LHSAA playoff brackets (basketball, soccer, baseball, softball, volleyball).

Reads the official bracket pages on lhsaaonline.org (MainBracket32.aspx and,
for Class B/C, MainBracket32Print_2.aspx). Every page tags its slots with the
same element ids (Team1Game{n}, Team2Game{n}, T1G{n}R score, T1G{n}S seed),
so one parser covers all of them. Rounds are worked out from the bracket
itself: a game's round = how many bracket slots its loser occupied (byes
count), so 16- and 32-team brackets both come out right.

Output is the shape the site's bracket trees (WordPress snippet #91) already
draw from: {"schools": [{"school", "bracket", "games": [{opponent, phase,
result, score, home_away, ...}]}]}. Stored on the persistent disk as
/data/winter_archives/brackets_{sport}.json.gz. Display only.

    GET /api/brackets/boys_basketball/build?seasons=2014-2025
    GET /api/brackets/boys_basketball/seasons
    GET /api/brackets/boys_basketball?season=2020
"""

from __future__ import annotations

import gzip
import json
import os
import re
import threading
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import winter_archive

BASE = "https://www.lhsaaonline.org/"
# sport -> (LHSAA bracket sport code, bracket style)
#   class:    5A-1A classes (+ Select Division I-V from 2016-17) through 2021-22, then
#             Non-Select / Select divisions; Class B and C print pages when they exist
#   division: plain Division I-V (volleyball, soccer)
SPORTS = {
    "boys_basketball": (2, "class"), "girls_basketball": (3, "class"),
    "baseball": (4, "class"), "softball": (5, "class"), "volleyball": (6, "division"),
    "boys_soccer": (17, "division"), "girls_soccer": (18, "division"),
}
SPORT_CODES = {k: v[0] for k, v in SPORTS.items()}
FIRST_SEASON = 2013  # oldest LHSAA bracket pages; missing years (e.g. 2016, spring 2020) just come back empty
ROMAN = ["I", "II", "III", "IV", "V"]
CLASSES = ["5A", "4A", "3A", "2A", "1A"]
P = "ctl00_ContentPlaceHolder1_"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

_LOCK = threading.Lock()
STATE = {"status": "idle", "sport": None, "seasons": [], "progress": {},
         "started_at": None, "finished_at": None, "error": None}
_CACHE = {}


# ── which brackets a season has ───────────────────────────────

def bracket_pages(season, kind="class"):
    """[(label, path, expect)] to try for one season. Pages LHSAA doesn't have come back
    empty and are skipped; `expect` is checked against the page's own heading."""
    season = int(season)
    pages = []
    if kind == "division":
        for d in ROMAN:
            pages.append((f"Division {d}", f"MainBracket32.aspx?d={d}&s={{s}}&y={season}&select=0", ("div", d, None)))
        return pages
    if season <= 2022:
        for cls in CLASSES:
            pages.append((cls, f"MainBracket32.aspx?d={cls}&s={{s}}&y={season}", ("cls", cls, None)))
        if season >= 2017:
            # Select divisions began in 2016-17 (the select flag is ignored then).
            for d in ROMAN:
                pages.append((f"Select Division {d}", f"MainBracket32.aspx?d={d}&s={{s}}&y={season}&select=1", ("div", d, None)))
    else:
        for d in ROMAN:
            pages.append((f"Non-Select Division {d}", f"MainBracket32.aspx?d={d}&s={{s}}&y={season}&select=0", ("div", d, "Non-Select")))
        for d in ROMAN:
            pages.append((f"Select Division {d}", f"MainBracket32.aspx?d={d}&s={{s}}&y={season}&select=1", ("div", d, "Select")))
    for cls in ("B", "C"):
        pages.append((f"Class {cls}", f"MainBracket32Print_2.aspx?d={cls}&s={{s}}&y={season}", ("cls", cls, None)))
    return pages


def page_heading(html):
    """'Division V (Non-Select)' / 'Class 5A' from '... Playoff Bracket - <here> BI-DISTRICT'."""
    text = re.sub(r"<[^>]+>", " ", html[:200000])
    text = re.sub(r"\s+", " ", text.replace("&nbsp;", " "))
    m = re.search(r"Playoff Bracket\s*-\s*(.{1,60}?)\s*(?:BI-DISTRICT|FIRST ROUND|ROUND|REGIONAL|\*\s*Denotes|$)", text, re.I)
    return m.group(1).strip() if m else ""


def heading_matches(expect, heading):
    if not heading or not expect:
        return True
    kind, value, select = expect
    if kind == "cls":
        ok = re.search(r"(?:Class\s+)?" + re.escape(value) + r"\b", heading) is not None
    else:
        ok = re.search(r"Division\s+" + value + r"(?![IV])", heading) is not None
    if ok and select and re.search(r"\((?:Non-)?Select\)", heading):
        ok = f"({select})" in heading
    return ok


# ── parsing ───────────────────────────────────────────────────

def _txt(soup, el_id):
    el = soup.find(id=P + el_id)
    if el is None:
        return None
    return re.sub(r"\s+", " ", el.get_text(" ").replace("\xa0", " ")).strip()


def _name(raw):
    raw = (raw or "").strip()
    return re.sub(r"\s*\*\s*$", "", raw).strip(), raw.endswith("*")


def _score(raw):
    raw = (raw or "").strip()
    return int(raw) if raw.isdigit() else None


def parse_bracket(html):
    """Raw slots: [{g, a, a_home, a_score, b, b_home, b_score}] (empty games dropped)."""
    if "not available" in html[:20000].lower() and "Team1Game" not in html:
        return []
    soup = BeautifulSoup(html, "lxml")
    out = []
    for g in range(1, 70):
        a_raw, b_raw = _txt(soup, f"Team1Game{g}"), _txt(soup, f"Team2Game{g}")
        if a_raw is None and b_raw is None:
            continue
        a, a_home = _name(a_raw)
        b, b_home = _name(b_raw)
        if not a and not b:
            continue
        out.append({"g": g, "a": a, "a_home": a_home, "a_score": _score(_txt(soup, f"T1G{g}R")),
                    "b": b, "b_home": b_home, "b_score": _score(_txt(soup, f"T2G{g}R"))})
    return out


def _is_bye(name):
    return not name or name.strip().lower() == "bye"


def bracket_games(slots):
    """Real games with round numbers and names, from one bracket's slots."""
    appear = {}
    for s in slots:
        for team in (s["a"], s["b"]):
            if not _is_bye(team):
                appear[team] = appear.get(team, 0) + 1
    games = []
    for s in slots:
        a, b = s["a"], s["b"]
        if _is_bye(a) or _is_bye(b):
            continue
        sa, sb = s["a_score"], s["b_score"]
        if sa is not None and sb is not None and sa != sb:
            winner = a if sa > sb else b
        elif appear.get(a, 0) != appear.get(b, 0):
            winner = a if appear.get(a, 0) > appear.get(b, 0) else b
        else:
            winner = ""
        loser = b if winner == a else a if winner == b else ""
        rnd = appear.get(loser, appear.get(a, 0)) if loser else max(appear.get(a, 0), appear.get(b, 0))
        games.append({**s, "winner": winner, "round": rnd})
    last = max((g["round"] for g in games), default=0)
    for g in games:
        g["phase"] = round_name(g["round"], last)
    return games


def round_name(rnd, last):
    if rnd >= last:
        return "State Championship"
    if rnd == last - 1:
        return "Semifinals"
    if rnd == last - 2:
        return "Quarterfinals"
    if rnd <= 1:
        return "Bi-District"
    return "Regional"


def season_schools(brackets):
    """{label: games} -> [{school, bracket, games}] in the site's bracket-data shape."""
    schools = {}
    for label, games in brackets.items():
        for g in sorted(games, key=lambda x: (x["round"], x["g"])):
            for me, opp, my_score, opp_score, home, opp_home in (
                (g["a"], g["b"], g["a_score"], g["b_score"], g["a_home"], g["b_home"]),
                (g["b"], g["a"], g["b_score"], g["a_score"], g["b_home"], g["a_home"]),
            ):
                key = (label, me)
                rec = schools.setdefault(key, {"school": me, "division": "", "bracket": label,
                                               "class_": "", "games": []})
                result = "W" if g["winner"] == me else "L" if g["winner"] == opp else ""
                score = f"{my_score}-{opp_score}" if my_score is not None and opp_score is not None else ""
                rec["games"].append({
                    "opponent": opp, "phase": g["phase"], "week": 0, "result": result,
                    "score": score, "home_away": "H" if home else "A" if opp_home else "",
                    "game_date": "",
                })
    return list(schools.values())


# ── storage ───────────────────────────────────────────────────

def archive_path(sport):
    return os.path.join(winter_archive.archive_dir(), f"brackets_{sport}.json.gz")


def load(sport):
    path = archive_path(sport)
    if not os.path.exists(path):
        return {"sport": sport, "seasons": {}}
    mtime = os.path.getmtime(path)
    cached = _CACHE.get(path)
    if cached and cached[0] == mtime:
        return cached[1]
    with gzip.open(path, "rt", encoding="utf-8") as source:
        data = json.load(source)
    data.setdefault("seasons", {})
    _CACHE[path] = (mtime, data)
    return data


def save_season(sport, season, season_data):
    data = load(sport)
    data = {"sport": sport, "seasons": dict(data.get("seasons", {}))}
    data["seasons"][str(season)] = season_data
    data["updated_at"] = datetime.now().isoformat(timespec="seconds")
    os.makedirs(winter_archive.archive_dir(), exist_ok=True)
    path = archive_path(sport)
    tmp = path + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8") as out:
        json.dump(data, out, separators=(",", ":"))
    os.replace(tmp, path)
    _CACHE.pop(path, None)


def season_data(sport, season):
    return load(sport).get("seasons", {}).get(str(season))


# ── building ──────────────────────────────────────────────────

def fetch(path, session=None, attempts=3):
    http = session or requests
    last = None
    for attempt in range(attempts):
        try:
            resp = http.get(BASE + path, headers=HEADERS, timeout=90)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last = exc
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"{path}: {last}")


def build_season(sport, season, pause=0.5, fetcher=None):
    code, kind = SPORTS[sport]
    session = requests.Session()
    get = fetcher or (lambda path: fetch(path, session=session))
    brackets, champions, seen = {}, {}, set()
    for label, path, expect in bracket_pages(season, kind):
        try:
            html = get(path.format(s=code))
        except Exception as exc:  # one bad page shouldn't sink the season
            STATE["progress"][str(season)] = f"{label}: {exc}"
            missing = STATE.setdefault("page_errors", [])
            missing.append(f"{sport} {season} {label}")
            continue
        games = bracket_games(parse_bracket(html)) if heading_matches(expect, page_heading(html)) else []
        sig = tuple(sorted((g["a"], g["b"]) for g in games))
        if games and sig in seen:
            games = []  # LHSAA served another bracket's page for a division it doesn't have
        if games:
            seen.add(sig)
            brackets[label] = games
            final = [g for g in games if g["phase"] == "State Championship"]
            if final and final[0]["winner"]:
                champions[label] = final[0]["winner"]
        STATE["progress"][str(season)] = f"{label}: {len(games)} games"
        if pause:
            time.sleep(pause)
    if not brackets:
        raise RuntimeError(f"LHSAA has no {sport} brackets for {season}")
    out = {
        "season": str(season), "source": "LHSAA brackets",
        "built_at": datetime.now().isoformat(timespec="seconds"),
        "brackets": list(brackets), "champions": champions,
        "games": sum(len(g) for g in brackets.values()),
        "schools": season_schools(brackets),
    }
    save_season(sport, season, out)
    STATE["progress"][str(season)] = f"done: {len(brackets)} brackets, {out['games']} games"
    return out


def parse_seasons(text, current_season):
    seasons = set()
    for part in str(text or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = (int(x) for x in part.split("-", 1))
            seasons.update(range(min(a, b), max(a, b) + 1))
        else:
            seasons.add(int(part))
    return sorted(s for s in seasons if FIRST_SEASON <= s < int(current_season))


def start_build(sport, seasons):
    if sport not in SPORT_CODES:
        raise ValueError("Unsupported sport")
    if not _LOCK.acquire(blocking=False):
        return False

    def run():
        STATE.update({"status": "running", "sport": sport, "seasons": seasons, "progress": {},
                      "error": None, "started_at": datetime.now().isoformat(timespec="seconds"),
                      "finished_at": None})
        errors = []
        try:
            for season in seasons:
                try:
                    build_season(sport, season)
                except Exception as exc:
                    errors.append(f"{season}: {exc}")
                    STATE["progress"][str(season)] = f"failed: {exc}"
            STATE["status"] = "completed" if not errors else "completed_with_errors"
            STATE["error"] = "; ".join(errors) or None
        finally:
            STATE["finished_at"] = datetime.now().isoformat(timespec="seconds")
            _LOCK.release()

    threading.Thread(target=run, daemon=True).start()
    return True


def summary(sport):
    data = load(sport)
    return {
        "sport": sport,
        "seasons": {
            s: {"brackets": info.get("brackets", []), "games": info.get("games", 0),
                "champions": info.get("champions", {}), "built_at": info.get("built_at")}
            for s, info in sorted(data.get("seasons", {}).items())
        },
        "job": STATE if STATE.get("sport") in (None, sport) else {"status": "busy"},
    }
