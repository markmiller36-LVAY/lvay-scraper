"""
LVAY social posts: score, standings and power-rating graphics for
Facebook and Instagram.

Daily plan (Central time, football season):
  Sat  Friday night finals: a "Big Games" post and a by-class roundup
  Sun  Power Ratings, top 10 in every division
  Mon  District standings, 5A    Tue 4A    Wed 3A    Thu 2A    Fri 1A

Every post is built as 1080x1350 JPEG slides, saved on the persistent disk,
and emailed for approval.  Nothing is published until someone presses
"Approve & post" on the review page (or SOCIAL_AUTO_APPROVE=true).

Environment (Render web service):
  SOCIAL_APPROVER_EMAIL   who gets the review email (default lvaypipeline@gmail.com)
  SOCIAL_AUTO_APPROVE     "true" = publish without review (phase 2)
  SOCIAL_PUBLIC_BASE      public URL of this service (images + review page)
  SOCIAL_DIR              where slides are saved (default /data/social)
  META_PAGE_ID            Facebook Page id
  META_PAGE_TOKEN         long-lived Page access token
  META_IG_USER_ID         Instagram professional account id linked to the Page
  META_GRAPH_VERSION      default v21.0
  RESEND_API_KEY / REPORT_EMAIL_FROM   reused from the pipeline report email
"""

import hashlib
import hmac
import html
import json
import os
import re
import time
from collections import Counter, OrderedDict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import Blueprint, abort, jsonify, request, send_file

CENTRAL = ZoneInfo("America/Chicago")
HERE = os.path.dirname(os.path.abspath(__file__))
SITE = "https://louisianavsallyall.com"

CLASS_ORDER = ["5A", "4A", "3A", "2A", "1A", "B", "C"]
STANDINGS_DAY = {0: "5A", 1: "4A", 2: "3A", 3: "2A", 4: "1A"}
HASHTAGS = "#LVAY #LAPreps #LHSAA #LouisianaFootball #FridayNightLights"
IG_MAX_SLIDES = 10


def env(name, default=""):
    return os.environ.get(name, default).strip()


def social_dir():
    return env("SOCIAL_DIR", "/data/social")


def public_base():
    return env("SOCIAL_PUBLIC_BASE", "https://lvay-scraper.onrender.com").rstrip("/")


def approver_email():
    return env("SOCIAL_APPROVER_EMAIL", "lvaypipeline@gmail.com")


# ── DATA HELPERS ─────────────────────────────────────────────

ROMAN = {"1": "I", "2": "II", "3": "III", "4": "IV", "5": "V"}


def division_label(division):
    """'Non-Select Division 1' / 'Non-Select Division I' -> 'Non-Select Division I'."""
    text = str(division or "").strip()
    return re.sub(r"(\d)$", lambda m: ROMAN.get(m.group(1), m.group(1)), text)


def division_short(division):
    """'Non-Select Division II' -> 'NS II', 'Select Division I' -> 'S I'."""
    label = division_label(division)
    match = re.match(r"(Non-Select|Select)\s+Division\s+([IVX]+)", label, re.I)
    if not match:
        return label
    track = "NS" if match.group(1).lower().startswith("non") else "S"
    return f"{track} {match.group(2).upper()}"


def division_sort_key(division):
    label = division_label(division)
    order = ["I", "II", "III", "IV", "V"]
    match = re.match(r"(Non-Select|Select)\s+Division\s+([IVX]+)", label, re.I)
    if not match:
        return (9, 9, label)
    track = 0 if match.group(1).lower().startswith("non") else 1
    num = order.index(match.group(2).upper()) if match.group(2).upper() in order else 9
    return (track, num, label)


def class_sort_key(class_name):
    text = str(class_name or "").upper()
    return CLASS_ORDER.index(text) if text in CLASS_ORDER else 99


def parse_date(value):
    text = str(value or "").strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def parse_score(score, result):
    """Return (team_points, opponent_points) consistent with the W/L.

    Scores are stored team-first, but about 1 in 150 rows is flipped, so the
    W/L decides which number belongs to whom.
    """
    numbers = re.findall(r"\d+", str(score or ""))
    if len(numbers) < 2:
        return None
    a, b = int(numbers[0]), int(numbers[1])
    outcome = str(result or "").strip().upper()[:1]
    if outcome == "W" and a < b:
        a, b = b, a
    elif outcome == "L" and a > b:
        a, b = b, a
    return a, b


def is_forfeit(game):
    if str(game.get("forfeit") or "").strip().lower() in ("yes", "y", "1", "true"):
        return True
    return "(f)" in str(game.get("result") or "").lower()


def record_text(wins, losses, ties=0):
    text = f"{int(wins or 0)}-{int(losses or 0)}"
    if int(ties or 0):
        text += f"-{int(ties)}"
    return text


def division_ranks(rankings):
    """school -> (rank within its division, division label)."""
    grouped = {}
    for row in rankings:
        grouped.setdefault(division_label(row.get("division")), []).append(row)
    ranks = {}
    for label, rows in grouped.items():
        rows.sort(key=lambda r: (-(r.get("power_rating") or 0), -(r.get("strength_factor") or 0)))
        for index, row in enumerate(rows, 1):
            ranks[row["school"]] = (index, label)
    return ranks


def collect_finals(schools, date_from, date_to, rankings=None):
    """One entry per game (deduped across both schools' schedules)."""
    ranks = division_ranks(rankings or [])
    info = {s["school"]: s for s in schools}
    games = OrderedDict()
    for school in schools:
        for game in school.get("games") or []:
            when = parse_date(game.get("game_date"))
            if not when or when < date_from or when > date_to:
                continue
            if str(game.get("phase") or "Regular Season") != "Regular Season":
                continue
            result = str(game.get("result") or "").strip().upper()[:1]
            if result not in ("W", "L", "T") or is_forfeit(game):
                continue
            points = parse_score(game.get("score"), game.get("result"))
            if not points:
                continue
            team, opponent = school["school"], str(game.get("opponent") or "").strip()
            key = (str(game.get("week")), tuple(sorted([team.casefold(), opponent.casefold()])))
            if key in games:
                continue
            venue = str(game.get("home_away") or "").upper()
            if venue == "A":
                home, away, home_pts, away_pts = opponent, team, points[1], points[0]
            else:
                home, away, home_pts, away_pts = team, opponent, points[0], points[1]
            if home_pts >= away_pts:
                winner, loser, w_pts, l_pts = home, away, home_pts, away_pts
            else:
                winner, loser, w_pts, l_pts = away, home, away_pts, home_pts
            home_info = info.get(home) or {}
            away_info = info.get(away) or {}
            games[key] = {
                "week": game.get("week"),
                "date": when,
                "home": home, "away": away,
                "winner": winner, "loser": loser,
                "winner_pts": w_pts, "loser_pts": l_pts,
                "tie": result == "T" or w_pts == l_pts,
                "district": bool(game.get("is_district")),
                "class": home_info.get("class_") or away_info.get("class_") or "",
                "winner_record": (info.get(winner) or {}).get("record", ""),
                "loser_record": (info.get(loser) or {}).get("record", ""),
                "winner_rank": ranks.get(winner),
                "loser_rank": ranks.get(loser),
                "out_of_state": bool(game.get("out_of_state")),
            }
    return list(games.values())


def pick_big_games(finals, top=10, limit=9):
    """Games where both teams sit in the top `top` of their division."""
    big = []
    for game in finals:
        wr, lr = game["winner_rank"], game["loser_rank"]
        if wr and lr and wr[0] <= top and lr[0] <= top:
            margin = abs(game["winner_pts"] - game["loser_pts"])
            big.append((wr[0] + lr[0], margin, game))
    big.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in big[:limit]]


def district_standings(schools, class_name):
    """District tables for one class, sorted like the site's Standings page."""
    tables = {}
    for school in schools:
        if str(school.get("class_") or "").upper() != class_name.upper():
            continue
        ow = ol = ot = dw = dl = dt = pf = pa = 0
        for game in school.get("games") or []:
            if str(game.get("phase") or "Regular Season") != "Regular Season":
                continue
            result = str(game.get("result") or "").strip().upper()[:1]
            if result not in ("W", "L", "T"):
                continue
            if result == "W": ow += 1
            elif result == "L": ol += 1
            else: ot += 1
            if game.get("is_district"):
                if result == "W": dw += 1
                elif result == "L": dl += 1
                else: dt += 1
            points = parse_score(game.get("score"), game.get("result"))
            if points and not is_forfeit(game):
                pf += points[0]
                pa += points[1]
        dg, og = dw + dl + dt, ow + ol + ot
        tables.setdefault(str(school.get("district") or "?"), []).append({
            "team": school["school"],
            "district": record_text(dw, dl, dt), "overall": record_text(ow, ol, ot),
            "pf": pf, "pa": pa,
            "_key": (-((dw + .5 * dt) / dg if dg else 0), -dw,
                     -((ow + .5 * ot) / og if og else 0), -(pf - pa),
                     school["school"].casefold()),
        })
    output = []
    for district in sorted(tables, key=lambda d: (int(d) if d.isdigit() else 999, d)):
        rows = sorted(tables[district], key=lambda r: r["_key"])
        for index, row in enumerate(rows, 1):
            row["rank"] = index
            row.pop("_key", None)
        output.append({"district": district, "class": class_name, "teams": rows})
    return output


def ratings_by_division(rankings, top=10):
    grouped = {}
    for row in rankings:
        grouped.setdefault(division_label(row.get("division")), []).append(row)
    output = []
    for label in sorted(grouped, key=division_sort_key):
        rows = sorted(grouped[label], key=lambda r: (-(r.get("power_rating") or 0),
                                                     -(r.get("strength_factor") or 0)))
        output.append({"division": label, "teams": rows[:top]})
    return output


# ── POST PLANNING ────────────────────────────────────────────

class Feed:
    """Reads the same JSON the website uses (in-process, no HTTP)."""

    def __init__(self, season=None, client=None):
        self.season = season
        self.client = client
        self._schedules = None
        self._rankings = None

    def _get(self, path):
        if self.client is None:
            from server import app
            self.client = app.test_client()
        token = env("PIPELINE_TOKEN")
        response = self.client.get(path, headers={"X-Pipeline-Token": token} if token else {})
        data = response.get_json(silent=True) or {}
        if response.status_code != 200 or "error" in data:
            raise RuntimeError(f"{path} failed: {data.get('error') or response.status_code}")
        return data

    def schedules(self):
        if self._schedules is None:
            q = f"?season={self.season}" if self.season else ""
            self._schedules = self._get("/api/schedules/football" + q)
        return self._schedules

    def rankings(self):
        if self._rankings is None:
            q = f"?season={self.season}" if self.season else ""
            self._rankings = self._get("/api/rankings/football" + q)
        return self._rankings


def plan_for(day):
    """What gets posted on a given Central date."""
    weekday = day.weekday()
    if weekday == 5:
        return ["finals-big", "finals-roundup"]
    if weekday == 6:
        return ["ratings"]
    if weekday in STANDINGS_DAY:
        return ["standings-" + STANDINGS_DAY[weekday]]
    return []


def in_football_season(day):
    return day.month in (8, 9, 10, 11) or (day.month == 12 and day.day <= 15)


def fmt_day(day):
    return day.strftime("%b %-d") if os.name != "nt" else day.strftime("%b %d")


def build_post(kind, day, feed, outroot=None):
    """Render one post.  Returns a dict or None when there is nothing to post."""
    from social_graphics import (big_games_slides, ratings_slides, roundup_slides,
                                 standings_slides)
    outroot = outroot or social_dir()
    schedules = feed.schedules()
    season = str(schedules.get("season") or feed.season or "")
    schools = schedules.get("schools") or []
    rankings = (feed.rankings().get("rankings") or []) if kind != "finals-roundup-nr" else []

    if kind.startswith("finals"):
        date_to = day
        date_from = day - timedelta(days=3)
        finals = collect_finals(schools, date_from, date_to, rankings)
        if not finals:
            return None
        week = Counter(g["week"] for g in finals).most_common(1)[0][0]
        key = f"football-{season}-w{week}-{kind}"
        outdir = os.path.join(outroot, key)
        os.makedirs(outdir, exist_ok=True)
        link = f"{SITE}/football/scores/?week={week}"
        if kind == "finals-big":
            games = pick_big_games(finals)
            if not games:
                return None
            slides = big_games_slides(games, week, outdir)
            lines = [f"🏈 BIG GAMES · Week {week} finals", ""]
            for g in games:
                wr = f"#{g['winner_rank'][0]} " if g["winner_rank"] else ""
                lr = f"#{g['loser_rank'][0]} " if g["loser_rank"] else ""
                lines.append(f"{wr}{g['winner']} {g['winner_pts']}, {lr}{g['loser']} {g['loser_pts']}")
            lines += ["", "Rankings = LVAY power rating rank in each team's division.",
                      "Every score, standings and power ratings: louisianavsallyall.com (link in bio)",
                      "", HASHTAGS]
            title = f"Big Games · Week {week}"
        else:
            slides = roundup_slides(finals, week, outdir)
            if len(slides) > IG_MAX_SLIDES:
                slides = slides[:IG_MAX_SLIDES]
            lines = [f"🏈 FRIDAY NIGHT FINALS · Week {week}", "",
                     f"{len(finals)} final scores from across Louisiana, by class. Swipe →", "",
                     "Find your team's score, standings and power rating at louisianavsallyall.com (link in bio)",
                     "", HASHTAGS]
            title = f"Friday Night Finals · Week {week}"
        return {"key": key, "kind": kind, "season": season, "title": title,
                "caption": "\n".join(lines), "link": link, "slides": slides}

    if kind == "ratings":
        if not rankings:
            return None
        key = f"football-{season}-ratings-{day.isoformat()}"
        outdir = os.path.join(outroot, key)
        os.makedirs(outdir, exist_ok=True)
        divisions = ratings_by_division(rankings)
        slides = ratings_slides(divisions, outdir, fmt_day(day))
        lines = [f"📊 LVAY POWER RATINGS · {fmt_day(day)}", "",
                 "Top 10 in every LHSAA division. Swipe →", ""]
        for division in divisions:
            if division["teams"]:
                lines.append(f"{division_short(division['division'])}: #1 {division['teams'][0]['school']}")
        lines += ["", "Full ratings for every team + 'if the playoffs started today' brackets: louisianavsallyall.com (link in bio)",
                  "", HASHTAGS]
        return {"key": key, "kind": kind, "season": season, "title": f"Power Ratings · {fmt_day(day)}",
                "caption": "\n".join(lines), "link": f"{SITE}/football/power-ratings/", "slides": slides}

    if kind.startswith("standings-"):
        class_name = kind.split("-", 1)[1]
        tables = district_standings(schools, class_name)
        if not tables or not any(t["teams"] for t in tables):
            return None
        key = f"football-{season}-standings-{class_name}-{day.isoformat()}"
        outdir = os.path.join(outroot, key)
        os.makedirs(outdir, exist_ok=True)
        slides = standings_slides(tables, class_name, outdir)
        leaders = [f"District {t['district']}-{class_name}: {t['teams'][0]['team']} ({t['teams'][0]['district']})"
                   for t in tables if t["teams"]]
        lines = [f"📋 CLASS {class_name} DISTRICT STANDINGS · {fmt_day(day)}", "", "District leaders:"]
        lines += leaders
        lines += ["", "Every district, sortable stats and 10 years of standings: louisianavsallyall.com (link in bio)",
                  "", HASHTAGS]
        return {"key": key, "kind": kind, "season": season,
                "title": f"Class {class_name} District Standings · {fmt_day(day)}",
                "caption": "\n".join(lines), "link": f"{SITE}/football/standings/", "slides": slides}
    raise ValueError(f"Unknown post kind: {kind}")


# ── STORAGE ──────────────────────────────────────────────────

def db():
    from server import get_db
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS social_posts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            post_key     TEXT UNIQUE NOT NULL,
            kind         TEXT,
            season       TEXT,
            title        TEXT,
            caption      TEXT,
            link         TEXT,
            slides       TEXT,
            status       TEXT NOT NULL DEFAULT 'pending',
            created_at   TEXT,
            decided_at   TEXT,
            posted_at    TEXT,
            fb_post_id   TEXT,
            ig_media_id  TEXT,
            error        TEXT
        )
    """)
    return conn


def save_post(post):
    conn = db()
    try:
        existing = conn.execute("SELECT * FROM social_posts WHERE post_key=?", (post["key"],)).fetchone()
        if existing:
            return dict(existing), False
        rel = [os.path.relpath(p, social_dir()) for p in post["slides"]]
        conn.execute("""
            INSERT INTO social_posts (post_key, kind, season, title, caption, link, slides,
                                      status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
        """, (post["key"], post["kind"], post["season"], post["title"], post["caption"],
              post["link"], json.dumps(rel), datetime.now(CENTRAL).isoformat()))
        conn.commit()
        row = conn.execute("SELECT * FROM social_posts WHERE post_key=?", (post["key"],)).fetchone()
        return dict(row), True
    finally:
        conn.close()


def get_post(post_id):
    conn = db()
    try:
        row = conn.execute("SELECT * FROM social_posts WHERE id=?", (post_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def update_post(post_id, **fields):
    conn = db()
    try:
        sets = ", ".join(f"{k}=?" for k in fields)
        conn.execute(f"UPDATE social_posts SET {sets} WHERE id=?", (*fields.values(), post_id))
        conn.commit()
    finally:
        conn.close()


def slide_urls(post):
    return [f"{public_base()}/social/img/{rel}" for rel in json.loads(post["slides"] or "[]")]


def review_token(post_id):
    secret = env("SOCIAL_SECRET") or env("PIPELINE_TOKEN") or "lvay-dev"
    return hmac.new(secret.encode(), f"social-review:{post_id}".encode(), hashlib.sha256).hexdigest()[:32]


def review_url(post_id):
    return f"{public_base()}/social/review/{post_id}?t={review_token(post_id)}"


# ── EMAIL ────────────────────────────────────────────────────

EMAIL_SUBJECT = "Social Media Graphics Review"


def review_email(post, image_urls, link):
    """(subject, html) for the approval email."""
    subject = f"{EMAIL_SUBJECT}: {post['title']}"
    slides = "".join(
        f'<a href="{html.escape(link)}"><img src="{html.escape(u)}" width="300" alt="Slide {n}" '
        f'style="width:300px;max-width:100%;margin:0 10px 12px 0;border:0;display:inline-block"></a>'
        for n, u in enumerate(image_urls, 1)
    )
    count = len(image_urls)
    body = f"""
<div style="font-family:Arial,Helvetica,sans-serif;max-width:660px;color:#111">
  <div style="background:#0e0e0e;color:#fff;padding:16px 20px">
    <div style="font-size:13px;letter-spacing:.08em;color:#00b2b0;font-weight:bold">{EMAIL_SUBJECT.upper()}</div>
    <div style="font-size:24px;font-weight:bold;margin-top:4px">{html.escape(post['title'])}</div>
    <div style="font-size:13px;color:#bbb;margin-top:4px">{count} slide{'s' if count != 1 else ''} · Facebook + Instagram</div>
  </div>
  <p style="margin:20px 0">
    <a href="{html.escape(link)}" style="background:#008584;color:#fff;padding:13px 24px;text-decoration:none;font-weight:bold;font-size:16px;display:inline-block">Review &amp; approve</a>
  </p>
  <p style="color:#555;font-size:13px;margin:0 0 18px">Nothing posts until you press <b>Approve &amp; post</b> on the review page.
     You can edit the caption there, or skip this one.</p>
  <div>{slides}</div>
  <p style="font-size:13px;color:#555;margin:14px 0 6px"><b>Caption</b></p>
  <pre style="white-space:pre-wrap;background:#f3f5f5;padding:12px;font-size:13px;font-family:Arial,Helvetica,sans-serif;margin:0">{html.escape(post['caption'])}</pre>
</div>"""
    return subject, body


def email_for_review(post):
    import requests
    api_key = env("RESEND_API_KEY")
    if not api_key:
        print("[SOCIAL] Review email skipped: RESEND_API_KEY not set")
        return False
    subject, body = review_email(post, slide_urls(post), review_url(post["id"]))
    response = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"from": env("REPORT_EMAIL_FROM", "LVAY Pipeline <onboarding@resend.dev>"),
              "to": [approver_email()], "subject": subject, "html": body},
        timeout=30,
    )
    response.raise_for_status()
    print(f"[SOCIAL] Review email sent to {approver_email()} for post {post['id']}")
    return True


# ── META (FACEBOOK + INSTAGRAM) ──────────────────────────────

def meta_ready():
    return bool(env("META_PAGE_TOKEN") and (env("META_PAGE_ID") or env("META_IG_USER_ID")))


def graph(method, path, **params):
    import requests
    version = env("META_GRAPH_VERSION", "v21.0")
    params["access_token"] = env("META_PAGE_TOKEN")
    url = f"https://graph.facebook.com/{version}/{path}"
    response = requests.request(method, url, data=params if method == "POST" else None,
                                params=params if method != "POST" else None, timeout=60)
    data = response.json() if response.content else {}
    if response.status_code >= 400 or "error" in data:
        message = (data.get("error") or {}).get("message") or response.text[:300]
        raise RuntimeError(f"Meta {path}: {message}")
    return data


def post_to_facebook(post):
    page = env("META_PAGE_ID")
    urls = slide_urls(post)
    message = post["caption"].replace("(link in bio)", "").replace("  ", " ") + f"\n\n{post['link']}"
    if len(urls) == 1:
        return graph("POST", f"{page}/photos", url=urls[0], caption=message).get("post_id")
    media = [graph("POST", f"{page}/photos", url=u, published="false")["id"] for u in urls]
    fields = {f"attached_media[{i}]": json.dumps({"media_fbid": m}) for i, m in enumerate(media)}
    return graph("POST", f"{page}/feed", message=message, **fields).get("id")


def _wait_ig(container_id, tries=30):
    for _ in range(tries):
        status = graph("GET", container_id, fields="status_code").get("status_code")
        if status == "FINISHED":
            return
        if status == "ERROR":
            raise RuntimeError(f"Instagram could not process {container_id}")
        time.sleep(3)
    raise RuntimeError("Instagram took too long to process the images")


def post_to_instagram(post):
    ig = env("META_IG_USER_ID")
    urls = slide_urls(post)[:IG_MAX_SLIDES]
    caption = post["caption"]
    if len(urls) == 1:
        container = graph("POST", f"{ig}/media", image_url=urls[0], caption=caption)["id"]
    else:
        children = []
        for u in urls:
            child = graph("POST", f"{ig}/media", image_url=u, is_carousel_item="true")["id"]
            children.append(child)
        for child in children:
            _wait_ig(child)
        container = graph("POST", f"{ig}/media", media_type="CAROUSEL",
                          children=",".join(children), caption=caption)["id"]
    _wait_ig(container)
    return graph("POST", f"{ig}/media_publish", creation_id=container).get("id")


def publish(post_id):
    post = get_post(post_id)
    if not post:
        raise ValueError("post not found")
    if post["status"] == "posted":
        return post
    if not meta_ready():
        update_post(post_id, status="approved", decided_at=datetime.now(CENTRAL).isoformat(),
                    error="Approved, but Facebook/Instagram aren't connected yet (META_* settings).")
        return get_post(post_id)
    errors, fb_id, ig_id = [], post.get("fb_post_id"), post.get("ig_media_id")
    if env("META_PAGE_ID") and not fb_id:
        try:
            fb_id = post_to_facebook(post)
        except Exception as exc:
            errors.append(f"Facebook: {exc}")
    if env("META_IG_USER_ID") and not ig_id:
        try:
            ig_id = post_to_instagram(post)
        except Exception as exc:
            errors.append(f"Instagram: {exc}")
    update_post(post_id, status="posted" if not errors else "error",
                posted_at=datetime.now(CENTRAL).isoformat(), fb_post_id=fb_id, ig_media_id=ig_id,
                error="; ".join(errors) or None)
    return get_post(post_id)


# ── DAILY RUN ────────────────────────────────────────────────

def run_for_day(day=None, kinds=None, feed=None, send_email=True):
    day = day or datetime.now(CENTRAL).date()
    kinds = kinds or (plan_for(day) if in_football_season(day) else [])
    feed = feed or Feed()
    results = []
    for kind in kinds:
        try:
            post = build_post(kind, day, feed)
        except Exception as exc:
            results.append({"kind": kind, "status": "error", "error": str(exc)})
            continue
        if not post:
            results.append({"kind": kind, "status": "nothing to post"})
            continue
        row, created = save_post(post)
        entry = {"kind": kind, "id": row["id"], "key": row["post_key"], "status": row["status"],
                 "slides": len(post["slides"]), "new": created}
        if created:
            if env("SOCIAL_AUTO_APPROVE").lower() == "true":
                entry["status"] = publish(row["id"])["status"]
            elif send_email:
                try:
                    email_for_review(row)
                except Exception as exc:
                    entry["email_error"] = str(exc)
        results.append(entry)
    return {"date": day.isoformat(), "posts": results}


# ── ROUTES ───────────────────────────────────────────────────

social_bp = Blueprint("social", __name__)


@social_bp.route("/api/social/run", methods=["POST"])
def social_run():
    """Protected (PIPELINE_TOKEN). Optional ?date=YYYY-MM-DD&kind=a,b&email=0"""
    day = None
    if request.args.get("date"):
        day = datetime.strptime(request.args["date"], "%Y-%m-%d").date()
    kinds = [k for k in (request.args.get("kind") or "").split(",") if k] or None
    return jsonify(run_for_day(day, kinds, send_email=request.args.get("email") != "0"))


@social_bp.route("/api/social/posts")
def social_posts():
    conn = db()
    try:
        rows = conn.execute("""SELECT id, post_key, kind, title, status, created_at, posted_at, error
                               FROM social_posts ORDER BY id DESC LIMIT 50""").fetchall()
    finally:
        conn.close()
    return jsonify({"posts": [dict(r) for r in rows]})


@social_bp.route("/social/img/<path:rel>")
def social_image(rel):
    root = os.path.realpath(social_dir())
    path = os.path.realpath(os.path.join(root, rel))
    if not path.startswith(root + os.sep) or not path.endswith(".jpg") or not os.path.exists(path):
        abort(404)
    response = send_file(path, mimetype="image/jpeg", max_age=86400)
    response.headers["X-Robots-Tag"] = "noindex"
    return response


REVIEW_STYLE = """
<style>body{font-family:Arial,sans-serif;background:#f3f5f5;margin:0;color:#111}
.wrap{max-width:980px;margin:0 auto;padding:20px}
h1{font-size:24px;margin:4px 0 2px}.k{color:#008584;font-weight:bold;font-size:13px;letter-spacing:.06em}
.st{display:inline-block;padding:3px 10px;border-radius:12px;background:#e2e8e8;font-size:13px;margin:6px 0}
.slides{display:flex;gap:10px;overflow-x:auto;padding:10px 0}
.slides img{height:420px;border-radius:8px;box-shadow:0 2px 8px #0002}
textarea{width:100%;height:220px;font:14px/1.4 Arial;padding:10px;border-radius:8px;border:1px solid #ccc;box-sizing:border-box}
.btns{display:flex;gap:10px;margin:14px 0;flex-wrap:wrap}
button{font-size:17px;font-weight:bold;padding:13px 24px;border-radius:26px;border:0;cursor:pointer}
.go{background:#008584;color:#fff}.skip{background:#fff;border:1px solid #bbb;color:#444}
.err{background:#fde8e8;color:#8a1c1c;padding:10px;border-radius:8px}
@media(max-width:600px){.slides img{height:300px}}</style>"""


@social_bp.route("/social/review/<int:post_id>", methods=["GET", "POST"])
def social_review(post_id):
    token = request.values.get("t", "")
    if not hmac.compare_digest(token, review_token(post_id)):
        abort(404)
    post = get_post(post_id)
    if not post:
        abort(404)
    note = ""
    if request.method == "POST" and post["status"] in ("pending", "approved", "error"):
        action = request.form.get("action")
        caption = (request.form.get("caption") or post["caption"]).replace("\r\n", "\n")
        if action == "skip":
            update_post(post_id, status="skipped", decided_at=datetime.now(CENTRAL).isoformat())
            note = "Skipped. Nothing was posted."
        elif action == "approve":
            update_post(post_id, caption=caption, decided_at=datetime.now(CENTRAL).isoformat())
            post = publish(post_id)
            note = ("Posted!" if post["status"] == "posted"
                    else (post.get("error") or "Approved."))
        post = get_post(post_id)
    images = "".join(f'<img src="{html.escape(u)}" alt="slide">' for u in slide_urls(post))
    open_form = post["status"] in ("pending", "approved", "error")
    form = ""
    if open_form:
        form = f"""<form method="post"><input type="hidden" name="t" value="{html.escape(token)}">
<p><b>Caption</b> (edit if you like)</p>
<textarea name="caption">{html.escape(post['caption'])}</textarea>
<div class="btns"><button class="go" name="action" value="approve">Approve &amp; post</button>
<button class="skip" name="action" value="skip">Skip this one</button></div></form>"""
    else:
        form = f"<pre style='white-space:pre-wrap'>{html.escape(post['caption'])}</pre>"
    err = f'<p class="err">{html.escape(post["error"])}</p>' if post.get("error") else ""
    note_html = f"<p><b>{html.escape(note)}</b></p>" if note else ""
    page = f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex">
<title>Review: {html.escape(post['title'])}</title>{REVIEW_STYLE}</head><body><div class="wrap">
<div class="k">LVAY SOCIAL · FACEBOOK + INSTAGRAM</div><h1>{html.escape(post['title'])}</h1>
<span class="st">Status: {html.escape(post['status'])}</span>{note_html}{err}
<div class="slides">{images}</div>{form}</div></body></html>"""
    return page
