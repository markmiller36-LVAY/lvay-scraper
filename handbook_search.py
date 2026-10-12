"""
LHSAA Handbook search for Hey Y'all (no AI).

The 2026-2027 LHSAA Handbook (188 pages, LHSAA's FlippingBook) is split into
numbered rules (e.g. "14.12.2", "8.1.1") and searched with simple keyword
scoring. Answers show the rule number, section, handbook page, a short
excerpt, and a link to that page of the official handbook.

Data: lhsaa_handbook_2026_27.json.gz (built by build_from_pages()).
"""

from __future__ import annotations

import gzip
import json
import math
import os
import re
import threading
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(HERE, "lhsaa_handbook_2026_27.json.gz")
EDITION = "2026–2027 LHSAA Handbook"
VIEWER = "https://online.flippingbook.com/view/611190912/"
PDF_PAGE_OFFSET = 12   # printed page 1 = viewer page 13

# Printed-page ranges from the handbook's table of contents.
SECTIONS = [
    (1, 18, "Constitution"), (19, 37, "Eligibility"),
    (38, 42, "Recruiting"), (43, 48, "Administration"),
    (49, 53, "Sports Seasons"), (54, 65, "Penalty Code"),
    (66, 76, "Athletic Contests"), (77, 77, "Awards"),
    (78, 84, "Contest Officiating"),
    (85, 85, "Athletics and Instructional Time"),
    (86, 93, "Baseball"), (94, 100, "Basketball"), (101, 103, "Bowling"),
    (104, 106, "Cross Country"), (107, 114, "Football"),
    (115, 121, "Golf"), (122, 124, "Gymnastics"),
    (125, 127, "Powerlifting"), (128, 132, "Soccer"),
    (133, 138, "Softball"), (139, 140, "Swimming"), (141, 144, "Tennis"),
    (145, 148, "Track and Field (Indoor)"),
    (149, 162, "Track and Field (Outdoor)"), (163, 166, "Volleyball"),
    (167, 171, "Wrestling"), (172, 173, "Glossary"),
    (174, 999, "Position Statements"),
]
SPORTS = {
    "football": "Football", "baseball": "Baseball",
    "basketball": "Basketball", "softball": "Softball",
    "soccer": "Soccer", "volleyball": "Volleyball", "golf": "Golf",
    "tennis": "Tennis", "wrestling": "Wrestling", "bowling": "Bowling",
    "swimming": "Swimming", "powerlifting": "Powerlifting",
    "gymnastics": "Gymnastics", "cross country": "Cross Country",
    "track": "Track and Field (Outdoor)",
}

SPORT_WORDS = {"football", "baseball", "basketball", "softball", "soccer",
               "volleyball"}

STOP = set("""
a an the and or of in on at to for from by with is are was were be been
being do does did how what when where which who whom why can could would
should shall will may might must i me my we our you your they them their
it its this that these those there here about into than then so if not no
any all some much many more most other such only own same too very just
lhsaa louisiana high school rule rules bylaw bylaws handbook hey yall
say says said tell know get got have has had player players student
students team teams game games kid kids athlete athletes happen happens
there
""".split())

SYNONYMS = {
    "transfer": ["transfer", "transferring", "transfers", "change",
                 "schools"],
    "eligible": ["eligible", "eligibility", "ineligible"],
    "eligibility": ["eligible", "eligibility"],
    "select": ["select", "nonselect"],
    "nonselect": ["nonselect", "select"],
    "power": ["power", "rating", "ratings"],
    "rating": ["power", "rating", "ratings"],
    "ratings": ["power", "rating", "ratings"],
    "ejected": ["ejection", "ejected", "disqualified", "unsportsmanlike"],
    "ejection": ["ejection", "ejected", "disqualified"],
    "overtime": ["overtime", "tiebreaker", "tie"],
    "tiebreaker": ["tie", "tied", "tiebreaking", "breaking"],
    "playoffs": ["playoff", "playoffs", "postseason", "bracket", "qualify"],
    "playoff": ["playoff", "playoffs", "postseason", "bracket", "qualify"],
    "classification": ["classification", "classified", "class",
                       "enrollment"],
    "reclassification": ["classification", "classified", "enrollment"],
    "age": ["age", "birthday", "nineteen", "19"],
    "semesters": ["semesters", "semester", "consecutive", "eight"],
    "recruit": ["recruit", "recruiting", "undue", "influence"],
    "recruiting": ["recruit", "recruiting", "undue", "influence"],
    "jamboree": ["jamboree", "scrimmage"],
    "scrimmage": ["jamboree", "scrimmage"],
    "mercy": ["mercy", "running", "clock", "point", "differential"],
    "old": ["age", "birthday", "19th"],
    "older": ["age", "birthday", "19th"],
    "years": ["semesters", "age"],
    "define": ["mean"],
    "definition": ["mean"],
    "bonus": ["larger", "classification", "points", "add"],
    "calculate": ["determine", "determined", "determining"],
    "calculated": ["determine", "determined", "determining"],
    "figured": ["determine", "determined", "determining"],
    "semester": ["semesters", "consecutive", "length", "eligibility"],
}


def _syn_table():
    out = {}
    for key, words in SYNONYMS.items():
        out.setdefault(_stem(key), set()).update(words)
    return out


def _section_for(printed):
    for lo, hi, name in SECTIONS:
        if lo <= printed <= hi:
            return name
    return ""


def _stem(word):
    for suf in ("ings", "ing", "ies", "ed", "es", "s"):
        if len(word) > 4 and word.endswith(suf):
            return word[: -len(suf)] + ("y" if suf == "ies" else "")
    return word


def _tokens(text):
    text = text.lower().replace("non-select", "nonselect").replace(
        "non select", "nonselect").replace("’", "'")
    words = re.findall(r"[a-z0-9]+", text)
    return [_stem(w) for w in words if w not in STOP and len(w) > 1]


_SYN = _syn_table()


# ── build ───────────────────────────────────────────────────────────────

HEADER_RE = re.compile(
    r"^.{0,90}?2026-2027 LHSAA Handbook\s*\|\s*[A-Za-z ()&]+?\s+(\d{1,3})\b"
    r"(?:\s+[A-Z][A-Z &()]+(?=\s))?")
RULE_RE = re.compile(r"(?:(?<=\s)|^)(\d{1,2}(?:\.\d{1,2}){1,4})\s+(?=[A-Z(\"“])")


GLOSSARY_RE = re.compile(
    r"(?:(?<=[.:]\s)|^)([A-Z][A-Za-z0-9\-/’' ]{1,48}?)\s+shall mean\b")


def build_from_pages(pages):
    """pages: [{"p": viewer page number, "t": text}] -> list of chunks."""
    chunks = []
    headings = {}   # rule number -> heading text ("14.12" -> "DETERMINING…")
    for page in pages:
        text = re.sub(r"\s+", " ", page.get("t") or "").strip()
        viewer_page = int(page["p"])
        printed = viewer_page - PDF_PAGE_OFFSET
        if printed < 1 or not text:
            continue
        section = _section_for(printed)
        m = HEADER_RE.match(text)
        if m:
            text = text[m.end():].strip()
        if section == "Glossary":
            terms = list(GLOSSARY_RE.finditer(text))
            for i, tm in enumerate(terms):
                end = terms[i + 1].start() if i + 1 < len(terms) else len(text)
                term = tm.group(1).strip()
                chunks.append({"page": viewer_page, "printed": printed,
                               "section": section, "rule": "",
                               "title": f"Glossary: {term}",
                               "text": text[tm.start():end].strip()})
            if terms:
                continue
        starts = [(mm.start(), mm.group(1)) for mm in RULE_RE.finditer(text)]
        if not starts or starts[0][0] > 0:
            starts.insert(0, (0, ""))
        for i, (pos, rule) in enumerate(starts):
            end = starts[i + 1][0] if i + 1 < len(starts) else len(text)
            body = text[pos:end].strip()
            if len(body) < 25:
                continue
            # A heading like "14.12 DETERMINING POWER RATING" (all caps).
            hm = re.match(r"^\d[\d.]*\s+([A-Z][A-Z0-9 ,&/()\-–’':]{3,80}?)"
                          r"(?=\s+(?:\d+\.|[A-Z][a-z]|\d|\(|-|–|$)|$)", body)
            if hm and rule:
                headings[rule] = hm.group(1).strip(" -–:")
            if len(body) < 25 or (hm and len(body) - hm.end() < 8):
                continue   # heading only; its title is carried to sub-rules
            title = ""
            parts = rule.split(".")
            for k in range(len(parts), 0, -1):
                h = headings.get(".".join(parts[:k]))
                if h:
                    title = h
                    break
            chunks.append({"page": viewer_page, "printed": printed,
                           "section": section, "rule": rule,
                           "title": title, "text": body})
    return chunks


# ── search ──────────────────────────────────────────────────────────────

class Handbook:
    def __init__(self, chunks):
        self.chunks = chunks
        self.toks = [Counter(_tokens(c["title"] + " " + c["title"] + " " +
                                     c["text"])) for c in chunks]
        df = Counter()
        for t in self.toks:
            df.update(t.keys())
        n = max(len(chunks), 1)
        self.idf = {w: math.log(1 + n / c) for w, c in df.items()}
        self.avg = sum(sum(t.values()) for t in self.toks) / n

    def search(self, question, limit=3):
        q = question.lower()
        sport = None
        for key, name in SPORTS.items():
            if re.search(rf"\b{key}\b", q):
                sport = name
                break
        words = [w for w in _tokens(q) if w not in SPORT_WORDS]
        groups = {}          # core word -> its synonyms (for coverage)
        expanded = []
        for w in words:
            expanded.append((w, 1.0))
            groups[w] = {w}
            for syn in _SYN.get(w, []):
                s = _stem(syn)
                if s != w:
                    expanded.append((s, 0.7))
                    groups[w].add(s)
        defining = re.search(r"\b(what (is|are|does)|mean|meaning|define|"
                             r"definition)\b", q)
        if not expanded:
            return []
        scored = []
        for i, (c, tf) in enumerate(zip(self.chunks, self.toks)):
            length = sum(tf.values()) or 1
            score, hit = 0.0, set()
            for w, weight in expanded:
                f = tf.get(w, 0)
                if not f:
                    continue
                hit.add(w)
                score += weight * self.idf.get(w, 0) * (f * 2.2) / (
                    f + 1.2 * (0.25 + 0.75 * length / self.avg))
            if not score:
                continue
            covered = sum(1 for g in groups.values() if g & hit)
            score *= 0.4 + covered / max(len(groups), 1)
            if c["title"]:
                tt = set(_tokens(c["title"]))
                if any(g & tt for g in groups.values()):
                    score *= 1.4
            if sport and c["section"] == sport:
                score *= 1.8
            elif c["section"] in SPORTS.values() and (
                    sport or c["section"] != "Football"):
                score *= 0.35   # another sport's rules
            if defining and c["section"] == "Glossary":
                score *= 2.5
            scored.append((score, i))
        scored.sort(reverse=True)
        out, seen = [], set()
        for score, i in scored:
            c = self.chunks[i]
            key = (c["page"], c["rule"])
            if key in seen:
                continue
            seen.add(key)
            out.append({**self._view(c, words), "score": round(score, 2)})
            if len(out) >= limit:
                break
        return out

    def _view(self, c, words):
        text = c["text"]
        # excerpt: ~45 words around the first matched word
        low = text.lower()
        pos = min([low.find(w) for w in words if low.find(w) >= 0] or [0])
        start = max(0, low.rfind(" ", 0, max(0, pos - 120)) + 1) \
            if pos > 160 else 0
        excerpt = " ".join(text[start:].split()[:45])
        if start > 0:
            excerpt = "…" + excerpt
        if len(text[start:].split()) > 45:
            excerpt += "…"
        return {"rule": c["rule"], "title": c["title"].title()
                if c["title"].isupper() else c["title"],
                "section": c["section"], "page": c["printed"],
                "link": f"{VIEWER}{c['page']}/", "excerpt": excerpt}


HANDBOOK_TRIGGERS = re.compile(
    r"\b(rules?|bylaws?|handbook|constitution|allowed|legal|illegal|"
    r"eligib\w*|ineligib\w*|transfer\w*|penalt\w*|eject\w*|forfeit\w*|"
    r"recruit\w*|mercy|running clock|overtime|jamboree|scrimmage|practice|"
    r"how (is|are) (the )?power ratings? (calculated|figured|computed|"
    r"determined)|power ratings? (formula|work|calculated|points)|"
    r"tie ?break\w*|seed\w*|age limit|semesters?|redshirt|physical|"
    r"concussion|heat|sportsmanship|officials?|uniform|max(imum)? "
    r"(number|games)|how many games|out of state|bonus points)\b")

_HB = None
_LOCK = threading.Lock()


def get_handbook():
    global _HB
    if _HB is None:
        with _LOCK:
            if _HB is None:
                chunks = []
                if os.path.exists(DATA_PATH):
                    with gzip.open(DATA_PATH, "rt", encoding="utf-8") as fh:
                        chunks = json.load(fh)
                _HB = Handbook(chunks)
    return _HB
