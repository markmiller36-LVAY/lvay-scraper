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
FIRST_SEASON = 2012  # oldest LHSAA bracket pages (volleyball has fall 2012; other sports start 2013); missing years (e.g. 2016, spring 2020) just come back empty
ROMAN = ["I", "II", "III", "IV", "V"]
CLASSES = ["5A", "4A", "3A", "2A", "1A"]
P = "ctl00_ContentPlaceHolder1_"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# Seasons LHSAA only posted as bracket PDFs (no MainBracket32 pages). Transcribed into
# bracket_pdf_data/ and built from there instead of lhsaaonline.org.
PDF_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bracket_pdf_data")
PDF_SEASONS = {"volleyball": {2010: "volleyball_2010_2011.txt", 2011: "volleyball_2010_2011.txt",
                              2015: "volleyball_2015.psv"}}


def _add_pdf_file(sport, season, fname):
    files = PDF_SEASONS.setdefault(sport, {}).get(season)
    files = [files] if isinstance(files, str) else list(files or [])
    if fname not in files:
        files.append(fname)
    # compact .txt first, then .psv files, whose sections replace or patch brackets
    PDF_SEASONS[sport][season] = sorted(files, key=lambda f: (f.endswith(".psv"), f))


def _scan_pdf_files():
    """Add {sport}_pdf.txt files ("#YYYY|Label" sections) to PDF_SEASONS."""
    if not os.path.isdir(PDF_DIR):
        return
    for fname in sorted(os.listdir(PDF_DIR)):
        p = re.match(r"^([a-z_]+)_(\d{4})\.psv$", fname)
        if p:
            _add_pdf_file(p.group(1), int(p.group(2)), fname)
            continue
        m = re.match(r"^([a-z_]+)_pdf\d*\.txt$", fname)
        if not m:
            continue
        with open(os.path.join(PDF_DIR, fname), encoding="utf-8") as src:
            for line in src:
                h = re.match(r"^#(\d{4})\|", line)
                if h:
                    _add_pdf_file(m.group(1), int(h.group(1)), fname)


_scan_pdf_files()

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
        ok = re.search(r"(?:Class\s+)?" + re.escape(value) + r"\b", heading, re.I) is not None
    else:
        ok = re.search(r"Division\s+" + value + r"(?![IViv])", heading, re.I) is not None
    if ok and select and re.search(r"\((?:Non-)?Select\)", heading, re.I):
        ok = f"({select})".lower() in heading.lower()
    return ok


# ── parsing ───────────────────────────────────────────────────

def _txt(soup, el_id):
    el = soup.find(id=P + el_id)
    if el is None:
        return None
    return re.sub(r"\s+", " ", el.get_text(" ").replace("\xa0", " ")).strip()


def clean_name(name):
    """Drop LHSAA's trailing markers (* home team, ^ and similar) from a team name."""
    return re.sub(r"[\s*^#+†]+$", "", str(name or "")).strip()


def _name(raw):
    raw = (raw or "").strip()
    return clean_name(raw), "*" in raw[-4:]


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
    appear, last_game = {}, {}
    for s in slots:
        for team in (s["a"], s["b"]):
            if not _is_bye(team):
                appear[team] = appear.get(team, 0) + 1
                last_game[team] = max(last_game.get(team, 0), s["g"])
    games = []
    for s in slots:
        a, b = s["a"], s["b"]
        if _is_bye(a) or _is_bye(b):
            continue
        sa, sb = s["a_score"], s["b_score"]
        # Who shows up in a later game is the winner, even when LHSAA's score is typed backwards.
        a_on, b_on = last_game.get(a, 0) > s["g"], last_game.get(b, 0) > s["g"]
        if a_on != b_on:
            winner = a if a_on else b
            if sa is not None and sb is not None and (sa > sb) != a_on and sa != sb:
                s = {**s, "a_score": sb, "b_score": sa, "score_flipped": True}
        elif sa is not None and sb is not None and sa != sb:
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
    data = load(sport).get("seasons", {}).get(str(season))
    if not data:
        return data
    # Seasons pulled before clean_name existed may still carry LHSAA markers ("Parkway ^").
    schools = []
    for school in data.get("schools", []):
        school = dict(school, school=clean_name(school.get("school")))
        school["games"] = [dict(g, opponent=clean_name(g.get("opponent"))) for g in school.get("games", [])]
        schools.append(school)
    champions = {k: clean_name(v) for k, v in (data.get("champions") or {}).items()}
    return dict(data, schools=schools, champions=champions)


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


def _slot(g, a, a_sets, b, b_sets, a_home, b_home):
    bye = lambda n: n.strip().lower() in ("", "b", "bye")
    a, b = ("Bye" if bye(a) else a), ("Bye" if bye(b) else b)
    num = lambda v: int(v) if str(v).strip().isdigit() else None
    return {"g": int(g), "a": a, "b": b, "a_score": num(a_sets), "b_score": num(b_sets),
            "a_home": bool(a_home), "b_home": bool(b_home)}


def pdf_slots(sport, season):
    """{"Division I": [slots]} for a season transcribed from LHSAA bracket PDFs.

    A season can come from several files: the compact .txt first, then .psv files.
    A "#Label" section in a .psv replaces that bracket; "#PATCH Label" replaces
    only the slots with the same game number."""
    files = (PDF_SEASONS.get(sport) or {}).get(int(season))
    if not files:
        return {}
    files = [files] if isinstance(files, str) else files
    out = {}
    for fname in files:
        brackets, patches = _pdf_file_slots(fname, season)
        out.update(brackets)
        for label, slots in patches.items():
            by_g = {s["g"]: s for s in out.get(label, [])}
            for slot in slots:
                by_g[slot["g"]] = slot
            out[label] = [by_g[g] for g in sorted(by_g)]
    return out


def _pdf_file_slots(fname, season):
    out, patches, label = {}, {}, None
    with open(os.path.join(PDF_DIR, fname), encoding="utf-8") as src:
        lines = src.read().splitlines()
    if fname.endswith(".psv"):
        # g|seedA|A|setsA|seedB|B|setsB ; lower seed is home
        target = out
        for line in lines:
            if line.startswith("#") and not line.startswith("# ") and len(line) > 1:
                head = line[1:].strip()
                target = patches if head.startswith("PATCH ") else out
                label = re.sub(r"^Class ([1-5]A)$", r"\1", re.sub(r"^PATCH\s+", "", head))
                target[label] = []
                continue
            if not line.strip() or line.startswith("#"):
                continue
            g, sa, a, xa, sb, b, xb = line.split("|")
            ha = b != "Bye" and a != "Bye" and int(sa) < int(sb)
            hb = b != "Bye" and a != "Bye" and int(sb) < int(sa)
            target[label].append(_slot(g, a, xa, b, xb, ha, hb))
        return out, patches
    # compact: "N name|name|..." then "#YYYY-D" (volleyball) or "#YYYY|Label" blocks
    # of "g a sets b sets home" ("-" = blank, B = bye, home 1 = a, 2 = b)
    names = []
    for line in lines:
        if line.startswith("N "):
            names = line[2:].split("|")
            continue
        m = re.match(r"^#(\d{4})-(\d)$", line)
        if m:
            label = f"Division {ROMAN[int(m.group(2)) - 1]}" if int(m.group(1)) == int(season) else None
            if label:
                out[label] = []
            continue
        m = re.match(r"^#(\d{4})\|(.+)$", line)
        if m:
            label = m.group(2).strip() if int(m.group(1)) == int(season) else None
            if label:
                label = re.sub(r"^Class ([1-5]A)$", r"\1", label)
                out[label] = []
            continue
        if label is None or not line.strip() or line.startswith("#"):
            continue
        g, a, xa, b, xb, home = (line.split(" ") + [""] * 6)[:6]
        name = lambda i: "Bye" if i in ("B", "-", "") else names[int(i)]
        out[label].append(_slot(g, name(a), xa, name(b), xb, int(home or 0) & 1, int(home or 0) & 2))
    return out, patches


def build_season(sport, season, pause=0.5, fetcher=None):
    if int(season) in (PDF_SEASONS.get(sport) or {}):
        return build_pdf_season(sport, season)
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


def first_season(sport=None):
    pdf = PDF_SEASONS.get(sport) or ()
    return min([FIRST_SEASON, *pdf])


def build_pdf_season(sport, season):
    brackets, champions = {}, {}
    known = known_names(sport)
    by_key = {_key(n): n for n in known}
    known_set, unmatched, renamed = set(known), set(), {}
    for label, slots in pdf_slots(sport, season).items():
        for s in slots:
            for side in ("a", "b"):
                if not _is_bye(s[side]):
                    raw = s[side]
                    s[side] = canonical_name(raw, known, by_key)
                    if s[side] != raw:
                        renamed[raw] = s[side]
                    if known_set and s[side] not in known_set:
                        unmatched.add(s[side])
        games = bracket_games(slots)
        if not games:
            continue
        brackets[label] = games
        final = [g for g in games if g["phase"] == "State Championship"]
        if final and final[0]["winner"]:
            champions[label] = final[0]["winner"]
    if not brackets:
        raise RuntimeError(f"No transcribed {sport} bracket PDF for {season}")
    out = {
        "season": str(season), "source": "LHSAA bracket PDF",
        "built_at": datetime.now().isoformat(timespec="seconds"),
        "brackets": list(brackets), "champions": champions,
        "games": sum(len(g) for g in brackets.values()),
        "schools": season_schools(brackets),
        "renamed": renamed, "unmatched": sorted(unmatched),
    }
    save_season(sport, season, out)
    STATE["progress"][str(season)] = f"done: {len(brackets)} brackets, {out['games']} games (PDF)"
    return out


# ── name matching for PDF seasons ──

# PDF spellings that the automatic matcher can't resolve (or would resolve wrongly).
NAME_FIX = {
    'Ouachita': 'Ouachita Parish', 'Carver': 'George Washington Carver', 'Carver, GW': 'George Washington Carver',
    'Higgins': 'L. W. Higgins', 'L.W. Higgins': 'L. W. Higgins', 'Reed': 'Sarah T. Reed',
    'St. Paul': "St. Paul's", 'Sacred Heart, VP': 'Sacred Heart V.P.', 'Sacred Heart VP': 'Sacred Heart V.P.',
    'Sacred Heart, V.P.': 'Sacred Heart V.P.', 'Sacred Heart, NO': 'Acad. of Sacred Heart - N.O.', 'Acad. Sac. Hrt, NO': 'Acad. of Sacred Heart - N.O.',
    'Sacred Hrt., NO': 'Acad. of Sacred Heart - N.O.', 'Cen. Jíville': 'Central - Jonesville', 'Central Jíville': 'Central - Jonesville',
    'Central Jonesville': 'Central - Jonesville', 'MP Country Day': 'Metairie Park Country Day', 'St. Joseph – Plauc.': "St. Joseph's - Plaucheville",
    "St. Joseph's – PL": "St. Joseph's - Plaucheville", 'Reserve Christ.': 'Reserve Christian', 'Reserve Chrs.': 'Reserve Christian',
    'Excelsior Christ.': 'Excelsior Christian', 'Bethel Christ.': 'Bethel Christian', 'Bethel Chrs.': 'Bethel Christian',
    'Bethany': 'Bethany Christian', 'Bethany Chrs.': 'Bethany Christian', 'Booth. Venice': 'Boothville-Venice',
    'Boothville Venice': 'Boothville-Venice', 'Booth-Venice': 'Boothville-Venice', 'La. School Deaf': 'La. School for the Deaf',
    'La Sch. Deaf': 'La. School for the Deaf', 'LA School For Deaf': 'La. School for the Deaf', 'Red. Seton': 'Redeemer-Seton',
    'Redeemer Seton': 'Redeemer-Seton', 'Jones – Hodge': 'Jonesboro-Hodge', 'Hamilton Christ.': 'Hamilton Christian',
    'Assembly Christ.': 'Assembly Christian', 'Episcopal, BR': 'Episcopal', 'Karr': 'Edna Karr',
    'Byrd': 'C.E. Byrd', 'Rummel': 'Archbishop Rummel', 'Shaw': 'Archbishop Shaw',
    'Loyola College Prep': 'Loyola Prep', 'Xavier': 'Xavier Prep', 'Famerville': 'Farmerville',
    'Mt. Herman': 'Mt. Hermon', 'B.T. Washington-SH': 'Booker T. Washington - Shr.', 'B.T. Wash. SH': 'Booker T. Washington - Shr.',
    'Hosanna': 'Hosanna Christian',
    "Lafayet t e": "Lafayette", "French Set t lement": "French Settlement", "Oak dale": "Oakdale",
    "H.L Bourgeois": "H.L. Bourgeois", "l e Platte": "Ville Platte", "Hackberrry": "Hackberry",
    "Harrsisonburg": "Harrisonburg", "Crecsent City": "Crescent City", "Glen Okas": "Glen Oaks",
    "Delhi": "Delhi", "Delhi High": "Delhi", "Episcopal, B.R.": "Episcopal", "Episcopal B.R.": "Episcopal",
    "W.L. Cohen": "Cohen College Prep", "Newman": "Isidore Newman", "McGehee": "Louise McGehee",
    "Sacred Heart, N.O.": "Acad. of Sacred Heart - N.O.", "Sacred Heart, G.C.": "Acad. of Sacred Heart - G.C.",
    "Catholic, N.I.": "Catholic - N.I.", "Catholic, NI": "Catholic - N.I.", "Catholic N.I.": "Catholic - N.I.",
    "Catholic, P.C.": "Catholic - P.C.", "Catholic P.C.": "Catholic - P.C.", "Country Day": "Metairie Park Country Day",
    "Evangel Christaian": "Evangel Christian", "St. Pauls's": "St. Paul's", "Evangel": "Evangel Christian",
    "Menard": "Holy Savior Menard", "St. Edmund's": "St. Edmund", "St. Michael's": "St. Michael the Archangel",
    "St. Michael": "St. Michael the Archangel", "Acad. S. H. (N.O.)": "Acad. of Sacred Heart - N.O.",
    "Hannan": "Archbishop Hannan", "Arch. Hannan": "Archbishop Hannan", "Mt. Carmel Acad.": "Mt. Carmel",
    "O. P. Walker": "O.P. Walker", "Walker, O.P.": "O.P. Walker", "St. Charles Cath.": "St. Charles Catholic",
    "St. Joseph's": "St. Joseph's Academy", "St. Joe's Academy": "St. Joseph's Academy", "St. Martin's": "St. Martin's Episcopal",
    "St. Mary's Acad.": "St. Mary's Academy", "Delhi High Charter": "Delhi Charter", "Assembly Chris.": "Assembly Christian",
    "Assembly Chrs.": "Assembly Christian", "Cohen, W.L.": "Cohen College Prep", "Ellender": "A.J. Ellender",
    "Ehret, John": "John Ehret", "Lusher": "Lusher Charter", "Westminster": "Westminster Christian",
    "Westminster Chris.": "Westminster Christian", "Westminster Academy": "Westminster Christian",
    "Washington, B.T.": "Booker T. Washington", "Booker T. Wash.": "Booker T. Washington",
    "Landry/Walker": "Lord Beaconsfield Landry", "Byrd C.E.": "C.E. Byrd", "Chapelle": "Archbishop Chapelle",
    "Our Lady": "Academy of Our Lady", "North Vermillion": "North Vermilion", "Catholic": "Catholic - B.R.",
    "Catholic B.R.": "Catholic - B.R.", "Central": "Central - B.R.", "Peabody Magnet": "Peabody",
    "Episcopal - B.R.": "Episcopal", "Woodlawn, Shrev.": "Woodlawn - Shrev.", "Northwood, Shreve": "Northwood - Shrev.",
    "Northwood": "Northwood - Shrev.", "Capitol for Girls": "Capitol",
}


def _pdf_clean(name):
    name = re.sub(r"(\w)\u00ed(s)\b", r"\1'\2", name)  # "St. Mary\u00eds" (mis-encoded apostrophe)
    name = re.sub(r"(?i)\s+inn\.?$|\s*\(forfeit\)$", "", name)
    name = re.sub(r"(\w) s\b", r"\1's", name)  # "St. Joseph s" -> "St. Joseph's"
    name = re.sub(r"\s+(SO|PK)$", "", name)
    name = re.sub(r"\s*\((\d+\s*in\.?|\d+in)\)", "", name)
    name = re.sub(r"\s*\{[^}]*\}|-Closed$", "", name)
    name = re.sub(r"\s*\(BR\)$", " - B.R.", name)
    name = re.sub(r"\s*\(SH\)$", " - Shrev.", name)
    return re.sub(r"\s+", " ", name).strip()


def _key(name):
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def _toks(name):
    return [t.replace("'", "") for t in re.split(r"[\s,.\-]+", (name or "").lower().replace("’", "'")) if t]


def _subseq(short, full):
    if not short or not full or short[0] != full[0]:
        return False
    i = 0
    for ch in full:
        if i < len(short) and ch == short[i]:
            i += 1
    return i == len(short)


def abbrev_match(short, full):
    a, b = _toks(short), _toks(full)
    if not a or len(a) > len(b) or not _subseq(a[0], b[0]):
        return False
    j = 1
    for t in a[1:]:
        while j < len(b) and not _subseq(t, b[j]):
            j += 1
        if j >= len(b):
            return False
        j += 1
    return True


def known_names(sport):
    names = set()
    for info in load(sport).get("seasons", {}).values():
        if "PDF" in str(info.get("source") or ""):
            continue
        for school in info.get("schools", []):
            names.add(clean_name(school.get("school")))
    return sorted(n for n in names if n)


def canonical_name(name, known, by_key=None):
    """Map a PDF spelling onto the name the archive already uses for that school."""
    name = _pdf_clean(re.sub(r"\s+", " ", (name or "").replace("’", "'")).strip())
    by_key = by_key or {_key(n): n for n in known}
    if name in NAME_FIX:
        return by_key.get(_key(NAME_FIX[name]), NAME_FIX[name])
    hit = by_key.get(_key(name))
    if hit:
        return hit
    flip = re.match(r"^([^,]+),\s*([^,]+)$", name)  # "Curtis, John" -> "John Curtis"
    if flip and _key(flip.group(2)) not in {"br", "no", "ni", "pc", "gc", "vp", "shrev", "shreve", "la"}:
        hit = by_key.get(_key(flip.group(2) + " " + flip.group(1)))
        if hit:
            return hit
        cands = [n for n in known if abbrev_match(flip.group(2) + " " + flip.group(1), n)]
        if len(cands) == 1:
            return cands[0]
    alt = re.sub(r",\s*", " - ", name)  # "Woodlawn, B.R." -> "Woodlawn - B.R."
    hit = by_key.get(_key(alt))
    if hit:
        return hit
    cands = [n for n in known if abbrev_match(name, n)]
    if len(cands) == 1:
        return cands[0]
    return name


def parse_seasons(text, current_season, sport=None):
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
    return sorted(s for s in seasons if first_season(sport) <= s < int(current_season))


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
                "champions": {k: clean_name(v) for k, v in (info.get("champions") or {}).items()},
                "built_at": info.get("built_at")}
            for s, info in sorted(data.get("seasons", {}).items())
        },
        "job": STATE if STATE.get("sport") in (None, sport) else {"status": "busy"},
    }
