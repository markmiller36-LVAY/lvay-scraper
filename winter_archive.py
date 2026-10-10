"""Past-season schedule archive (soccer, basketball, volleyball, baseball, softball).

LHSAA keeps every boys/girls soccer schedule back to 2014-15 on
lhsaaonline.org, but only the current season goes through the ratings
pipeline.  This module pulls a finished season district by district (the
report refuses an "All" search and its division filter returns nothing),
turns it into the same school/games shape the schedules feed already serves,
and stores it as one gzip JSON file per sport on the persistent disk.

Archive seasons are display-only: records come straight from LHSAA's
results, no power ratings are calculated, and the live database tables are
never touched.

Usage on Render (background thread, see server.py):
    GET /api/archive/winter/boys_soccer/build?seasons=2015-2025
    GET /api/archive/winter/boys_soccer
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

REPORT_URL = "https://www.lhsaaonline.org/pr/{path}/admin/ReportSchedule.asp"

SOURCES = {
    "boys_soccer": {
        "path": "sopr", "params": {"p": "1", "so": "1"},
        "referer": "https://www.lhsaaonline.org/pr/sopr/admin/SearchboyssoccerSchedule.asp",
    },
    "girls_soccer": {
        "path": "sopr", "params": {"p": "1", "so": "2"},
        "referer": "https://www.lhsaaonline.org/pr/sopr/admin/SearchgirlssoccerSchedule.asp",
    },
    # Basketball's district filter takes "N - CLASS" values that change every
    # alignment, so the report is pulled one class at a time instead (about
    # 1,000-2,000 rows per class, well under the report's buffer limit).
    "boys_basketball": {
        "path": "bbpr", "params": {"p": "1", "bb": "1"},
        "referer": "https://www.lhsaaonline.org/pr/bbpr/admin/SearchBoysBasketballSchedule.asp",
        "split": "class", "first_season": 2014,  # 2013-14 is the oldest season LHSAA lists
    },
    "girls_basketball": {
        "path": "bbpr", "params": {"p": "1", "bb": "2"},
        "referer": "https://www.lhsaaonline.org/pr/bbpr/admin/SearchGirlsBasketballSchedule.asp",
        "split": "class", "first_season": 2014,
    },
    # Volleyball (Oct 10): LHSAA lists 2013-14 on; season key = the fall year
    # ("2013" = 2013-14), matching the live feed.  Pulled one division at a time.
    "volleyball": {
        "path": "vbpr", "params": {"p": "1"},
        "referer": "https://www.lhsaaonline.org/pr/vbpr/admin/SearchVolleyballSchedule.asp",
        "split": "class", "parts": ["I", "II", "III", "IV", "V"], "group": "division",
        "year_field": "y", "first_season": 2013,
    },
    # Baseball/softball share one report (bb=1 / bb=2) and LHSAA remembers the
    # last search page opened in the session, so the search page is opened first.
    # LHSAA lists 2020-21 on; season key = the spring year.
    "baseball": {
        "path": "bpr", "params": {"p": "1", "bb": "1"},
        "referer": "https://www.lhsaaonline.org/pr/bpr/admin/SearchBaseballSchedule.asp",
        "split": "class", "year_field": "y", "first_season": 2021,
        # LHSAA's baseball report shows only the winning margin ("16-0" = won
        # by 16), never the real score, so archived baseball keeps the margin
        # and leaves the score blank (no fake runs for/against).
        "margin_scores": True,
    },
    "softball": {
        "path": "sbpr", "params": {"p": "1", "bb": "2"},
        "referer": "https://www.lhsaaonline.org/pr/sbpr/admin/SearchSoftballSchedule.asp",
        "split": "class", "year_field": "y", "first_season": 2021,
    },
}

ROMAN = ["I", "II", "III", "IV", "V"]

# LHSAA soccer districts run 1-9 in every season checked (2015, 2026); the
# extra numbers are cheap empty requests that guard against a season with more.
DISTRICTS = range(1, 16)
FIRST_SEASON = 2015  # soccer: 2014-15 is the oldest season LHSAA lists


def first_season(sport):
    return int(SOURCES.get(sport, {}).get("first_season", FIRST_SEASON))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/x-www-form-urlencoded",
}

CLASS_ORDER = ["5A", "4A", "3A", "2A", "1A", "B", "C"]

_LOCK = threading.Lock()
STATE = {"status": "idle", "sport": None, "seasons": [], "progress": {},
         "started_at": None, "finished_at": None, "error": None}
_CACHE = {}


# ── storage ───────────────────────────────────────────────────

def archive_dir():
    default = os.path.join(
        os.path.dirname(os.environ.get("DB_PATH", "/data/lvay_v2.db")),
        "winter_archives",
    )
    return os.environ.get("WINTER_ARCHIVE_DIR", default)


def archive_path(sport):
    return os.path.join(archive_dir(), f"{sport}.json.gz")


def load_archive(sport):
    """Return {"sport", "seasons": {season: {...}}}; cached until the file changes."""
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
    data = load_archive(sport)
    data = {"sport": sport, "seasons": dict(data.get("seasons", {}))}
    data["seasons"][str(season)] = season_data
    data["updated_at"] = datetime.now().isoformat(timespec="seconds")
    os.makedirs(archive_dir(), exist_ok=True)
    path = archive_path(sport)
    tmp = path + ".tmp"
    with gzip.open(tmp, "wt", encoding="utf-8") as out:
        json.dump(data, out, separators=(",", ":"))
    os.replace(tmp, path)
    _CACHE.pop(path, None)
    return data


def archive_season(sport, season):
    return load_archive(sport).get("seasons", {}).get(str(season))


# ── parsing ───────────────────────────────────────────────────

def _clean(text):
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def split_district_class(value):
    """'8-5A' -> ('8', '5A'); 'B' or '' -> ('', 'B'/'')."""
    value = _clean(value).upper()
    match = re.match(r"^(\d+)\s*-\s*([1-5]A|B|C|I{1,3}|IV|V)$", value)
    if match:
        return match.group(1), match.group(2)
    if value in CLASS_ORDER or value in ROMAN:
        return "", value
    return "", ""


def normalize_result(value):
    value = _clean(value).upper()
    return value[:1] if value[:1] in ("W", "L", "T") else ""


def _parse_when(value):
    """'11/18/2014 4:00:00 PM Tue' -> ('11/18/2014', '4:00 PM')."""
    match = re.match(
        r"^(\d{1,2}/\d{1,2}/\d{4})(?:\s+(\d{1,2}):(\d{2})(?::\d{2})?\s*([AP]M))?",
        _clean(value), re.IGNORECASE,
    )
    if not match:
        return _clean(value), ""
    time_text = ""
    if match.group(2):
        time_text = f"{int(match.group(2))}:{match.group(3)} {match.group(4).upper()}"
        if time_text == "12:00 AM":
            time_text = ""
    return match.group(1), time_text


def parse_report(html):
    """Parse one LHSAA ReportSchedule page into game rows (one per school-game)."""
    soup = BeautifulSoup(html, "lxml")
    games = []
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td", recursive=False)
        if len(cells) not in (12, 13):
            continue
        t = [_clean(c.get_text(" ")) for c in cells]
        if len(t) == 12:  # volleyball/baseball/softball reports have no OT column
            t.insert(11, "")
        if not re.fullmatch(r"\d+\.", t[0]) or not t[1]:
            continue
        district, class_ = split_district_class(t[2])
        opp_district, opp_class = split_district_class(t[5])
        game_date, game_time = _parse_when(t[3])
        kind = t[6].upper()
        games.append({
            "school": t[1],
            "district": district,
            "class_": class_,
            "game_date": game_date,
            "game_time": game_time,
            "opponent": t[4],
            "opp_district": opp_district,
            "opp_class": opp_class,
            "is_district": kind == "D",
            "is_tournament": kind == "T",
            "tournament_host": t[7],
            "match_num": t[8],
            "home_away": t[9].upper()[:1],
            "result": normalize_result(t[10]),
            "result_raw": t[10],
            "overtime": t[11].lower().startswith("y"),
            "score": t[12],
        })
    return games


# ── building ──────────────────────────────────────────────────

def _record(wins, losses, ties):
    return f"{wins}-{losses}" + (f"-{ties}" if ties else "")


def build_season(sport, season, rows):
    """Group parsed rows into the schedules-feed shape for one season."""
    by_division = SOURCES.get(sport, {}).get("group") == "division"
    by_school = {}
    seen = set()
    for row in rows:
        key = (row["school"].casefold(), row["game_date"], row["game_time"],
               row["opponent"].casefold(), row["match_num"], row["score"])
        if key in seen:
            continue
        seen.add(key)
        school = by_school.setdefault(row["school"], {
            "school": row["school"], "class_": row["class_"],
            "district": row["district"], "games": [],
        })
        if not school["class_"] and row["class_"]:
            school["class_"], school["district"] = row["class_"], row["district"]
        school["games"].append({
            "game_date": row["game_date"],
            "game_time": row["game_time"],
            "opponent": row["opponent"],
            "opp_class": row["opp_class"],
            "opp_district": row["opp_district"],
            "opp_division": row["opp_class"] if by_division else "",
            "home_away": row["home_away"],
            "result": row["result"],
            "score": row["score"],
            "overtime": row["overtime"],
            "is_district": row["is_district"],
            "is_tournament": row["is_tournament"],
            "tournament_host": row["tournament_host"],
            "match_num": row["match_num"],
            "total_pts": None,
        })

    records = {}
    for school in by_school.values():
        wins = sum(g["result"] == "W" for g in school["games"])
        losses = sum(g["result"] == "L" for g in school["games"])
        ties = sum(g["result"] == "T" for g in school["games"])
        records[school["school"].casefold()] = (wins, losses, ties)
        division = ""
        if by_division and school["class_"] in ROMAN:
            division = f"Division {school['class_']}"
        school.update({
            "sport": sport, "season": str(season), "division": division,
            "track": "", "power_rating": None, "rank": None,
            "wins": wins, "losses": losses, "ties": ties,
            "games_played": wins + losses + ties,
            "record": _record(wins, losses, ties),
        })

    if SOURCES.get(sport, {}).get("margin_scores"):
        for school in by_school.values():
            for game in school["games"]:
                match = re.match(r"^\s*(\d+)\s*-\s*0\s*$", game["score"] or "")
                game["margin"] = int(match.group(1)) if match else None
                game["score"] = ""

    if by_division:  # the division is not a class; keep class_ blank like the live feed's archive rows
        for school in by_school.values():
            school["class_"] = ""
            for game in school["games"]:
                game["opp_class"] = ""

    for school in by_school.values():
        school["games"].sort(key=_game_sort_key)
        for game in school["games"]:
            opp = records.get(game["opponent"].casefold())
            if opp:
                game["opp_wins"], game["opp_losses"], game["opp_ties"] = opp
                game["opp_record"] = _record(*opp)
            else:
                game["opp_wins"] = game["opp_losses"] = game["opp_ties"] = None
                game["opp_record"] = ""

    order = ROMAN if by_division else CLASS_ORDER

    def school_key(s):
        dist = int(s["district"]) if s["district"].isdigit() else 99
        cls = s["class_"]
        return (order.index(cls) if cls in order else 99, dist, s["school"].casefold())

    schools = sorted(by_school.values(), key=school_key)
    return {
        "season": str(season),
        "status": "final",
        "source": "LHSAA schedule archive",
        "count": len(schools),
        "games": sum(len(s["games"]) for s in schools),
        "built_at": datetime.now().isoformat(timespec="seconds"),
        "schools": schools,
    }


def _game_sort_key(game):
    try:
        date = datetime.strptime(game["game_date"], "%m/%d/%Y")
    except ValueError:
        date = datetime.max
    try:
        clock = datetime.strptime(game.get("game_time") or "", "%I:%M %p").time()
    except ValueError:
        clock = datetime.min.time()
    try:
        number = int(game.get("match_num") or 0)
    except ValueError:
        number = 0
    return (date, number, clock)


# ── fetching ──────────────────────────────────────────────────

def fetch_district(sport, season, district, session=None, attempts=3, classification=""):
    source = SOURCES[sport]
    year_field = source.get("year_field", "yr")
    payload = {year_field: str(season), "resultdate": "", "n": "", "h": "",
               "d": classification, "f": str(district or ""), "s": "",
               "paging": "", "n1": "", "d1": classification}
    if year_field == "y":
        payload["y1"] = str(season)
    headers = dict(HEADERS, Referer=source["referer"])
    http = session or requests
    last_error = None
    for attempt in range(attempts):
        try:
            if source.get("year_field") == "y":
                # Opens this sport's search page first: LHSAA's baseball and
                # softball reports read the sport from the session.
                http.get(source["referer"], headers={"User-Agent": HEADERS["User-Agent"]}, timeout=60)
            resp = http.post(REPORT_URL.format(path=source["path"]),
                             params=source["params"], data=payload,
                             headers=headers, timeout=90)
            resp.raise_for_status()
            if "Response Buffer Limit Exceeded" in resp.text:
                raise RuntimeError("LHSAA buffer limit exceeded")
            return resp.text
        except Exception as exc:  # network hiccups: retry with backoff
            last_error = exc
            time.sleep(3 * (attempt + 1))
    raise RuntimeError(f"{sport} {season} {classification or 'district'} {district}: {last_error}")


def build_from_lhsaa(sport, season, pause=1.0):
    session = requests.Session()
    rows, per_district = [], {}
    by_class = SOURCES[sport].get("split") == "class"
    parts = SOURCES[sport].get("parts", CLASS_ORDER) if by_class else DISTRICTS
    for part in parts:
        if by_class:
            html = fetch_district(sport, season, "", session=session, classification=part)
        else:
            html = fetch_district(sport, season, part, session=session)
        found = parse_report(html)
        per_district[str(part)] = len(found)
        rows.extend(found)
        label = "class" if by_class else "district"
        STATE["progress"][str(season)] = f"{label} {part}: {len(rows)} rows"
        time.sleep(pause)
    if not rows:
        raise RuntimeError(f"LHSAA returned no {sport} games for {season}")
    season_data = build_season(sport, season, rows)
    season_data["rows_by_district"] = per_district
    save_season(sport, season, season_data)
    STATE["progress"][str(season)] = (
        f"done: {season_data['count']} schools, {season_data['games']} games"
    )
    return season_data


def parse_seasons(text, current_season, sport=None):
    """'2015-2025' or '2015,2018' -> sorted list of finished seasons."""
    seasons = set()
    for part in str(text or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = (int(x) for x in part.split("-", 1))
            seasons.update(range(min(start, end), max(start, end) + 1))
        else:
            seasons.add(int(part))
    first = first_season(sport) if sport else FIRST_SEASON
    return sorted(s for s in seasons if first <= s < int(current_season))


def pending_path():
    return os.path.join(archive_dir(), "pending.json")


def _read_pending():
    try:
        with open(pending_path(), encoding="utf-8") as source:
            data = json.load(source)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_pending(data):
    os.makedirs(archive_dir(), exist_ok=True)
    data = {k: v for k, v in data.items() if v.get("seasons")}
    if not data:
        try:
            os.remove(pending_path())
        except OSError:
            pass
        return
    tmp = pending_path() + ".tmp"
    with open(tmp, "w", encoding="utf-8") as out:
        json.dump(data, out)
    os.replace(tmp, pending_path())


def _set_pending(sport, seasons, attempts=0):
    data = _read_pending()
    data[sport] = {"seasons": [int(s) for s in seasons], "attempts": attempts}
    _write_pending(data)


def _done_pending(sport, season):
    data = _read_pending()
    entry = data.get(sport)
    if entry:
        entry["seasons"] = [s for s in entry.get("seasons", []) if int(s) != int(season)]
        _write_pending(data)


MAX_RESUMES = 5


def start_build(sport, seasons, attempts=0):
    """Pull seasons in a background thread.

    The to-do list is saved on the persistent disk first, so a worker restart
    mid-build does not lose it: resume_pending() picks it up on the next boot.
    """
    if sport not in SOURCES:
        raise ValueError("Unsupported sport")
    if not _LOCK.acquire(blocking=False):
        # Busy with another sport: queue this one; resume_pending() starts it
        # when the running build finishes.
        try:
            data = _read_pending()
            if sport not in data:
                _set_pending(sport, seasons, attempts)
                return None
        except OSError:
            pass
        return False
    try:
        _set_pending(sport, seasons, attempts)
    except OSError:
        pass

    def run():
        STATE.update({"status": "running", "sport": sport, "seasons": seasons,
                      "progress": {}, "error": None, "resumes": attempts,
                      "started_at": datetime.now().isoformat(timespec="seconds"),
                      "finished_at": None})
        errors = []
        try:
            for season in seasons:
                try:
                    build_from_lhsaa(sport, season)
                except Exception as exc:
                    errors.append(f"{season}: {exc}")
                    STATE["progress"][str(season)] = f"failed: {exc}"
                try:
                    _done_pending(sport, season)
                except OSError:
                    pass
            STATE["status"] = "completed" if not errors else "completed_with_errors"
            STATE["error"] = "; ".join(errors) or None
        finally:
            STATE["finished_at"] = datetime.now().isoformat(timespec="seconds")
            _LOCK.release()
        resume_pending()  # another sport may be waiting

    threading.Thread(target=run, daemon=True).start()
    return True


def resume_pending():
    """Restart an unfinished build saved on disk (after a deploy or worker restart)."""
    data = _read_pending()
    for sport, entry in sorted(data.items()):
        seasons = [int(s) for s in entry.get("seasons", [])]
        attempts = int(entry.get("attempts", 0)) + 1
        if sport not in SOURCES or not seasons:
            continue
        if attempts > MAX_RESUMES:
            data.pop(sport, None)
            _write_pending(data)
            STATE.update({"status": "gave_up", "sport": sport,
                          "error": f"stopped after {MAX_RESUMES} restarts; left: {seasons}"})
            continue
        return start_build(sport, seasons, attempts)
    return False


def core_name(name):
    """Same school-name key the team pages use (norm + drop high/school/academy...)."""
    text = re.sub(r"[^a-z0-9]+", " ", str(name or "").lower().replace("&", " and ")).strip()
    text = re.sub(r"\b(high|school|hs|the|academy|of)\b", " ", text)
    text = re.sub(r"\bsaint\b", "st", text)
    return re.sub(r"\s+", " ", text).strip()


def school_history(sport, name):
    """Every archived season for one school: [{season, class_, district, record..., games}]."""
    key = core_name(name)
    out = []
    if not key:
        return out
    for season, info in sorted(load_archive(sport).get("seasons", {}).items()):
        for school in info.get("schools", []):
            if core_name(school.get("school")) == key:
                out.append(dict(school, season=str(season)))
                break
    return out


def summary(sport):
    data = load_archive(sport)
    return {
        "sport": sport,
        "updated_at": data.get("updated_at"),
        "seasons": {
            season: {
                "schools": info.get("count", 0),
                "games": info.get("games", 0),
                "built_at": info.get("built_at"),
                "rows_by_district": info.get("rows_by_district", {}),
            }
            for season, info in sorted(data.get("seasons", {}).items())
        },
        "job": STATE if STATE.get("sport") in (None, sport) else {"status": "busy"},
    }
