"""Locked LHSAA class equivalents for specific football out-of-state opponents.

The OOS bonus is +2 per class step the OOS opponent sits above the Louisiana
school (power_rating_engine, football ``oos_bonus``).  The class comes from the
"LHSAA Class" column of the OOS Team Registry sheet.  These schools were
entered with the wrong class, which produced the wrong bonus, so the class is
locked here:

* the OOS verifier writes it into the registry sheet on every run (so a
  hand edit can't quietly put the old value back), and imports it to the DB;
* the ratings run applies it when it reads ``oos_opponents``, so a stale DB
  row can't use the old value either.

Verified bonuses (2026), per Mark:
  Isidore Newman (2A) vs St. John's School, TX     -> +2  (3A)
  Isidore Newman (2A) vs Santa Fe Christian, CA    -> +0  (2A)
  Iowa (3A)           vs Vidor High School, TX     -> +2  (4A)
  Mansfield (2A)      vs Magnolia High School, AR  -> +2  (3A)
"""

from __future__ import annotations

import re

OOS_CLASS_CORRECTIONS = {
    "2026": {
        "St. John's School, TX": {
            "class": "3A",
            "note": "Class locked to 3A: Newman (2A) earns +2.",
        },
        "Santa Fe Christian School, CA": {
            "class": "2A",
            "note": "Class locked to 2A: Newman (2A) earns +0.",
        },
        "Vidor High School, TX": {
            "class": "4A",
            "note": "Class locked to 4A: Iowa (3A) earns +2.",
        },
        "Magnolia High School, AR": {
            "class": "3A",
            "note": "Class locked to 3A: Mansfield (2A) earns +2.",
        },
    },
}

_STATE_SUFFIX = re.compile(r",\s*[A-Za-z]{2}\.?$")
_FILLER = re.compile(r"\b(high school|school|hs)\b")


def oos_key(name) -> str:
    """Loose key: case, punctuation, ', ST' suffix and 'High School' ignored."""
    s = str(name or "").strip().replace("’", "'")
    s = _STATE_SUFFIX.sub("", s).casefold()
    s = s.replace("saint ", "st. ")
    s = _FILLER.sub(" ", s)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def oos_class_correction(season, opponent):
    corrections = OOS_CLASS_CORRECTIONS.get(str(season), {})
    key = oos_key(opponent)
    if not key:
        return None
    for name, info in corrections.items():
        if oos_key(name) == key:
            return info
    return None
