"""LHSAA football seeding tiebreakers (Handbook 2026-27, Bylaw 14.11.2-14.11.3).

Teams in the same division with an identical power rating are ordered:

Two teams tied (14.11.2):
  1. Head to head: if they played, the winner is seeded higher.
  2. Strength factor (14.13.1), higher first.
  3. a. Best won-lost % against teams in the school's same classification
     b. Best won-lost % in district play
     c. Best won-lost % against all teams played

Three or more teams tied (14.11.3):
  1. Strength factor, higher first.
  2. If two teams are still tied on strength factor and played each other,
     the winner is seeded higher.
  3. Then a/b/c as above.

Mark, Nov 7, 2026: Sam Houston and Sulphur were both 9.90 in Non-Select I.
Sulphur beat Sam Houston 55-54 in Week 4, so Sulphur must be seeded higher
even though Sam Houston has the higher strength factor.
"""

from functools import cmp_to_key

WIN = {"W", "W(F)"}
LOSS = {"L", "L(F)", "L(DF)", "DF"}
TIE = {"T", "TIE"}


def _norm_result(value):
    text = str(value or "").strip().upper()
    if text in WIN:
        return "W"
    if text in LOSS:
        return "L"
    if text in TIE:
        return "T"
    return ""


def build_game_index(game_rows):
    """game_rows: iterable of dicts with school, opponent, result, is_district."""
    index = {}
    for row in game_rows:
        result = _norm_result(row.get("result"))
        if not result:
            continue
        index.setdefault(row.get("school"), []).append({
            "opponent": row.get("opponent"),
            "result": result,
            "is_district": bool(row.get("is_district")),
        })
    return index


def _pct(games):
    if not games:
        return -1.0  # no games of that kind sorts below any real record
    wins = sum(1.0 for g in games if g["result"] == "W")
    wins += sum(0.5 for g in games if g["result"] == "T")
    return wins / len(games)


def _head_to_head(a, b, games):
    """+1 if a beat b, -1 if b beat a, 0 if they didn't play or split/tied."""
    score = 0
    for g in games.get(a, []):
        if g["opponent"] == b:
            if g["result"] == "W":
                score += 1
            elif g["result"] == "L":
                score -= 1
    return (score > 0) - (score < 0)


def _record_keys(team, games, class_of):
    played = games.get(team["school"], [])
    own_class = class_of.get(team["school"], "")
    same_class = [g for g in played if own_class and class_of.get(g["opponent"]) == own_class]
    district = [g for g in played if g["is_district"]]
    return (_pct(same_class), _pct(district), _pct(played))


def _sf(team):
    return float(team.get("strength_factor") or 0)


def _order_tied(tied, games, class_of):
    """Order teams that share an identical power rating."""
    if len(tied) == 2:
        a, b = tied

        def two(x, y):
            h = _head_to_head(x["school"], y["school"], games)
            if h:
                return -h
            if _sf(x) != _sf(y):
                return -1 if _sf(x) > _sf(y) else 1
            kx, ky = _record_keys(x, games, class_of), _record_keys(y, games, class_of)
            if kx != ky:
                return -1 if kx > ky else 1
            return -1 if x["school"] < y["school"] else 1

        return sorted(tied, key=cmp_to_key(two))

    # Three or more: strength factor first.
    by_sf = {}
    for team in tied:
        by_sf.setdefault(_sf(team), []).append(team)
    ordered = []
    for sf in sorted(by_sf, reverse=True):
        group = by_sf[sf]
        if len(group) == 2 and _head_to_head(group[0]["school"], group[1]["school"], games):
            h = _head_to_head(group[0]["school"], group[1]["school"], games)
            ordered.extend(group if h > 0 else [group[1], group[0]])
            continue
        ordered.extend(sorted(
            group,
            key=lambda t: (tuple(-v for v in _record_keys(t, games, class_of)), t["school"]),
        ))
    return ordered


def seed_division(teams, games, class_of):
    """Return teams (one division, eligible only) in LHSAA seed order."""
    by_rating = {}
    for team in teams:
        by_rating.setdefault(round(float(team.get("power_rating") or 0), 2), []).append(team)
    ordered = []
    for rating in sorted(by_rating, reverse=True):
        group = by_rating[rating]
        ordered.extend(group if len(group) == 1 else _order_tied(group, games, class_of))
    return ordered


def tiebreak_labels(teams, games):
    """For teams that share an identical rating, say how each was placed:
    "h2h" (head to head), "sf" (strength factor) or "record" (W-L criteria).
    Teams not in a tie are left out.
    """
    by_rating = {}
    for team in teams:
        by_rating.setdefault(round(float(team.get("power_rating") or 0), 2), []).append(team)
    labels = {}
    for group in by_rating.values():
        if len(group) < 2:
            continue
        if len(group) == 2:
            a, b = group
            if _head_to_head(a["school"], b["school"], games):
                method = "h2h"
            elif _sf(a) != _sf(b):
                method = "sf"
            else:
                method = "record"
            labels[a["school"]] = labels[b["school"]] = method
            continue
        by_sf = {}
        for team in group:
            by_sf.setdefault(_sf(team), []).append(team)
        for same in by_sf.values():
            if len(same) == 1:
                method = "sf"
            elif len(same) == 2 and _head_to_head(same[0]["school"], same[1]["school"], games):
                method = "h2h"
            else:
                method = "record"
            for team in same:
                labels[team["school"]] = method
    return labels
