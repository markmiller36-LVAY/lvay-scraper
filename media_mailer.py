"""Saturday-morning Power Ratings graphics for news outlets and radio shows.

After the Saturday 5am football/volleyball pipeline run, LVAY draws one PNG
per edition in media_list.json (a region's local teams, or the statewide top
10 in each division) and emails it to that outlet through Resend.

Safety:
  - MEDIA_EMAIL_LIVE must be "true" before anything goes to an outlet.
    Until then every edition goes only to REPORT_EMAIL_TO as a preview.
  - Each outlet gets at most one email per Saturday (media_sends table).
  - Recipients come only from media_list.json in this repo.
"""

from __future__ import annotations

import base64
import html
import io
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from PIL import Image, ImageDraw, ImageFont

HERE = Path(__file__).resolve().parent
MEDIA_LIST_PATH = Path(os.environ.get("MEDIA_LIST_PATH", HERE / "media_list.json"))
FONT_DIR = HERE / "assets" / "fonts"
DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")
CENTRAL = ZoneInfo("America/Chicago")
SITE = "louisianavsallyall.com"

# LVAY brand colors.
TEAL = (0, 133, 132)
BLACK = (17, 21, 24)
INK = (17, 17, 17)
GREY = (170, 178, 178)
PAGE = (243, 243, 241)
ROW_A = (246, 246, 246)
ROW_B = (255, 255, 255)
SUBBAR = (228, 238, 237)

FOOTBALL_ORDER = [
    "Non-Select Division I", "Non-Select Division II",
    "Non-Select Division III", "Non-Select Division IV",
    "Select Division I", "Select Division II",
    "Select Division III", "Select Division IV",
]
VOLLEYBALL_ORDER = ["Division I", "Division II", "Division III", "Division IV", "Division V"]
SPORT_LABEL = {"football": "Football", "volleyball": "Volleyball"}
RATINGS_PAGE = {
    "football": f"{SITE}/football/power-ratings/",
    "volleyball": f"{SITE}/volleyball/power-ratings/",
}

# Shorter names for the graphic; the database keeps the official spelling.
DISPLAY_NAMES = {
    "Booker T. Washington - N.O.": "Booker T. Washington (New Orleans)",
    "Northwood - Lena": "Northwood-Lena",
    "Northwood - Shrev.": "Northwood (Shreveport)",
    "Woodlawn - Shrev.": "Woodlawn (Shreveport)",
    "D'Arbonne Woods Charter": "D'Arbonne Woods",
    "Lincoln Preparatory School": "Lincoln Prep",
    "Providence Classical Academy": "Providence Classical",
    "Word of God Academy": "Word of God",
    "Magnolia School of Excellence": "Magnolia Excellence",
    "Kenner Discovery Health Science": "Kenner Discovery",
    "Patrick Taylor - Science/Tech.": "Patrick Taylor",
    "Lafayette Renaissance Charter Academy": "Lafayette Renaissance",
    "Acadiana Renaissance Charter Academy": "Acadiana Renaissance",
    "St. Helena College & Career Acad.": "St. Helena",
}
MONTHS = ["JAN.", "FEB.", "MARCH", "APRIL", "MAY", "JUNE", "JULY",
          "AUG.", "SEPT.", "OCT.", "NOV.", "DEC."]


# ── Data ─────────────────────────────────────────────────────────────────

def load_media_list(path=None):
    with open(path or MEDIA_LIST_PATH, encoding="utf-8") as handle:
        return json.load(handle)


def load_rankings(sport):
    """The same rows the site's Power Ratings page shows (via the API view)."""
    from server import app

    response = app.test_client().get(f"/api/rankings/{sport}")
    data = response.get_json() or {}
    if response.status_code != 200 or "rankings" not in data:
        raise RuntimeError(f"{sport} rankings unavailable: {data.get('error')}")
    rows = []
    for row in data["rankings"]:
        # Schools LHSAA lists as not playing in the playoffs have no division
        # rank. Region graphics still show them (PR with a dash for rank);
        # the statewide top 10 leaves them out.
        out = row.get("playoff_eligible") is False
        rank = None if out else (row.get("division_rank") or row.get("div_rank"))
        if (not rank and not out) or not row.get("division"):
            continue
        rows.append({
            "school": row["school"],
            "division": row["division"],
            "rank": int(rank) if rank else None,
            "pr": float(row.get("power_rating") or 0),
            "calculated_at": row.get("calculated_at"),
            "cls": "" if (row.get("class_") or "").lower() in ("", "unknown") else row["class_"],
            "record": _record(row),
            "sf": row.get("strength_factor"),
        })
    return data.get("season"), rows


def expand_specs(specs):
    """{"sport": ..., "all_divisions": true} becomes one division edition per
    division (football: Non-Select I-IV, Select I-IV; volleyball: I-V)."""
    out = []
    for spec in specs:
        if spec.get("all_divisions"):
            order = FOOTBALL_ORDER if spec["sport"] == "football" else VOLLEYBALL_ORDER
            out.extend({"sport": spec["sport"], "division": d} for d in order)
        else:
            out.append(spec)
    return out


def _record(row):
    """W-L (W-L-T when there are ties), the same record the ratings page shows."""
    if row.get("wins") is None and row.get("losses") is None:
        return ""
    rec = f"{int(row.get('wins') or 0)}-{int(row.get('losses') or 0)}"
    if row.get("ties"):
        rec += f"-{int(row['ties'])}"
    return rec


def build_edition(sport, rows, season, edition, regions):
    """Group rows into the division blocks one graphic shows."""
    order = FOOTBALL_ORDER if sport == "football" else VOLLEYBALL_ORDER
    if edition.get("division"):
        # One division's full list, split into side-by-side columns.
        division = edition["division"]
        teams = sorted((r for r in rows if r["division"] == division),
                       key=lambda r: (r["rank"] is None, r["rank"] or 0, -r["pr"]))
        n = len(teams)
        parts = 1 if n <= 14 else (2 if n <= 46 else 3)
        size = -(-n // parts) if n else 0
        blocks = []
        for p in range(parts):
            chunk = teams[p * size:(p + 1) * size]
            if not chunk:
                continue
            first = chunk[0]["rank"] or "\u2014"
            last = chunk[-1]["rank"] or "\u2014"
            label = division if parts == 1 else f"{division}  {first}\u2013{last}"
            blocks.append((label, chunk))
        stamps = [r["calculated_at"] for r in rows if r.get("calculated_at")]
        updated = _parse_time(max(stamps)) if stamps else datetime.now(CENTRAL)
        return {
            "sport": sport, "season": str(season or "")[:4], "area": division.upper(),
            "subtitle": "EVERY TEAM IN THE DIVISION  \u2022  RANKED BY POWER RATING",
            "blocks": blocks, "updated": updated, "missing": [], "kind": "division",
        }
    if edition.get("top10"):
        picked = [r for r in rows if r["rank"] and r["rank"] <= 10]
        area, subtitle, missing = "STATEWIDE", "TOP 10 IN EACH DIVISION", []
    else:
        region = regions[edition["region"]]
        wanted = set(region["schools"])
        picked = [r for r in rows if r["school"] in wanted]
        missing = sorted(wanted - {r["school"] for r in picked})
        area, subtitle = region["title"].upper(), "LOCAL TEAMS"
    subtitle += (" •  RANKED WITHIN EACH SELECT / NON-SELECT DIVISION"
                 if sport == "football" else " •  RANKED WITHIN EACH DIVISION")
    blocks = []
    for division in order:
        teams = sorted((r for r in picked if r["division"] == division),
                       key=lambda r: (r["rank"] is None, r["rank"] or 0, -r["pr"]))
        if teams:
            blocks.append((division, teams))
    stamps = [r["calculated_at"] for r in rows if r.get("calculated_at")]
    updated = _parse_time(max(stamps)) if stamps else datetime.now(CENTRAL)
    return {
        "sport": sport, "season": str(season or "")[:4], "area": area,
        "subtitle": subtitle, "blocks": blocks, "updated": updated,
        "missing": missing,
    }


def _parse_time(value):
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(CENTRAL)
    if moment.tzinfo is None:
        return moment.replace(tzinfo=CENTRAL)  # Render runs with TZ=America/Chicago
    return moment.astimezone(CENTRAL)


# ── Drawing ──────────────────────────────────────────────────────────────

class _Fonts:
    def __init__(self, scale):
        self.scale = scale
        self._cache = {}

    def slab(self, size):
        return self._get("AlfaSlabOne-Regular.ttf", size, None)

    def sans(self, size, weight="Medium"):
        return self._get("Oswald-Variable.ttf", size, weight)

    def _get(self, name, size, weight):
        key = (name, size, weight)
        if key not in self._cache:
            font = ImageFont.truetype(str(FONT_DIR / name), int(size * self.scale))
            if weight:
                font.set_variation_by_name(weight)
            self._cache[key] = font
        return self._cache[key]


LOGO_URL = os.environ.get(
    "MEDIA_LOGO_URL",
    f"https://{SITE}/wp-content/uploads/2025/01/WHITELVAYCLEAR.png",
)
_LOGO = None


def _logo():
    """The white LVAY script logo, cropped to its edges (None if unreachable)."""
    global _LOGO
    if _LOGO is not None:
        return _LOGO
    cache = Path(os.environ.get("MEDIA_CACHE_DIR", "/data/media_cache")) / "lvay-logo.png"
    raw = None
    try:
        import requests

        response = requests.get(LOGO_URL, timeout=30)
        response.raise_for_status()
        raw = response.content
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_bytes(raw)
    except Exception as exc:
        print(f"[MEDIA] Logo download failed ({exc}); using a saved copy")
        if cache.exists():
            raw = cache.read_bytes()
    if raw is None:
        bundled = HERE / "assets" / "lvay-logo-white-mask.png"
        if not bundled.exists():
            return None
        _LOGO = Image.open(bundled).convert("L")
        return _LOGO
    alpha = Image.open(io.BytesIO(raw)).convert("RGBA").getchannel("A")
    bounds = alpha.getbbox()
    _LOGO = alpha.crop(bounds) if bounds else alpha
    return _LOGO


def _updated_label(moment):
    hour = moment.strftime("%I").lstrip("0")
    return f"{MONTHS[moment.month - 1]} {moment.day} • {hour}:{moment.strftime('%M %p')}"


OUTPUT_WIDTH = 2160  # 4:5 feed post, 2160 x 2700


def render_png(edition, scale=None):
    """Draw the graphic in LVAY's teal/black style and return PNG bytes.

    Always a 4:5 feed post (2160 x 2700). The division cards stretch or
    tighten to fill the space, and a crowded edition moves to three columns.
    """
    W, PAD, GAP = 950, 28, 18
    H = W * 5 / 4
    if scale is None:
        scale = OUTPUT_WIDTH / W
    HEADER_H, BAR_H, SUB_H, FOOT_H = 262, 7, 43, 96
    f = _Fonts(scale)
    body_top = HEADER_H + BAR_H + SUB_H + 18
    room = H - FOOT_H - 20 - body_top

    # Card proportions; three columns use slimmer headers and gaps.
    BASE = {2: (42, 20, 10), 3: (32, 12, 4)}

    def layout(ncols, k):
        hb, gb, pb = BASE[ncols]
        head, row, card_gap = hb * k, 31 * k, gb * k
        columns, heights = [[] for _ in range(ncols)], [0] * ncols
        for block in edition["blocks"]:
            target = heights.index(min(heights))
            columns[target].append(block)
            heights[target] += head + row * len(block[1]) + pb * k + card_gap
        return columns, max(heights or [0]) - card_gap

    ncols = 2
    if layout(2, 1)[1] > room:
        ncols = 3
    natural = layout(ncols, 1)[1] or 1
    k = max(0.7, min(1.9, room / natural))
    columns, used = layout(ncols, k)
    while used > room and k > 0.5:
        k -= 0.02
        columns, used = layout(ncols, k)
    HB, GB, PB = BASE[ncols]
    HEAD, ROW, CARD_GAP, CARD_PAD = HB * k, 31 * k, GB * k, PB * k
    if ncols == 3:
        PAD, GAP = 20, 12
    COL = (W - 2 * PAD - GAP * (ncols - 1)) / ncols
    H = W * 5 / 4

    s = scale
    img = Image.new("RGB", (round(W * s), round(H * s)), PAGE)
    d = ImageDraw.Draw(img)

    def box(x0, y0, x1, y1, fill, **kw):
        d.rectangle([x0 * s, y0 * s, x1 * s, y1 * s], fill=fill, **kw)

    def text(x, y, value, font, fill, anchor="la"):
        d.text((x * s, y * s), value, font=font, fill=fill, anchor=anchor)

    def fit(value, size, max_width, make):
        while size > 12 and d.textlength(value, font=make(size)) > max_width * s:
            size -= 1
        return make(size)

    # Header: the LVAY script logo, the title, and where to find LVAY.
    WHITE = (255, 255, 255)
    box(0, 0, W, HEADER_H, BLACK)
    logo = _logo()
    if logo is not None:
        ratio = min(170 / logo.width, 160 / logo.height)  # ~20% under the full panel
        mark = logo.resize((int(logo.width * ratio * s), int(logo.height * ratio * s)), Image.LANCZOS)
        left = int((132 - logo.width * ratio / 2) * s)
        top = int((131 - logo.height * ratio / 2) * s)
        img.paste(WHITE, (left, top, left + mark.width, top + mark.height), mark)
    else:
        for i, word in enumerate(("LOUISIANA", "VS", "ALL Y\u2019ALL")):
            text(132, 70 + i * 48, word, f.slab(30), WHITE if i != 1 else TEAL, "mm")
    box(256, 32, 258, 230, (60, 66, 70))
    title_left, title_right = 282, 700
    # Title: season + sport, "POWER RATINGS", then the area in big teal so
    # each outlet sees at a glance that the graphic is about its own teams.
    top_line = f"{edition['season']} LHSAA {SPORT_LABEL[edition['sport']].upper()}"
    text(title_left, 30, top_line, fit(top_line, 28, title_right - title_left, f.slab), WHITE)
    text(title_left, 66, "POWER RATINGS", fit("POWER RATINGS", 40, title_right - title_left, f.slab), WHITE)
    area_title = "STATEWIDE TOP 10" if edition["area"] == "STATEWIDE" else edition["area"]
    text(title_left, 112, area_title, fit(area_title, 40, title_right - title_left, f.slab), TEAL)
    text(title_left, 176, "LOUISIANAVSALLYALL.COM", f.sans(19, "SemiBold"), WHITE)
    text(title_left, 204, "@LAvsAllYall", f.sans(19, "SemiBold"), TEAL)
    handle_end = title_left + d.textlength("@LAvsAllYall", font=f.sans(19, "SemiBold")) / s
    text(handle_end + 12, 207, "FACEBOOK  \u00b7  INSTAGRAM  \u00b7  X  \u00b7  YOUTUBE",
         f.sans(13, "Medium"), GREY)
    d.rounded_rectangle([716 * s, 34 * s, 920 * s, 134 * s], radius=round(8 * s),
                        fill=(16, 32, 33), outline=TEAL, width=round(2 * s))
    text(736, 48, "UNOFFICIAL", f.sans(19, "SemiBold"), TEAL)
    text(736, 78, "UPDATED", f.sans(13, "SemiBold"), GREY)
    text(736, 100, _updated_label(edition["updated"]), f.sans(17, "SemiBold"), WHITE)
    box(0, HEADER_H, W, HEADER_H + BAR_H, TEAL)
    box(0, HEADER_H + BAR_H, W, HEADER_H + BAR_H + SUB_H, SUBBAR)
    text(34, HEADER_H + BAR_H + SUB_H / 2, edition["subtitle"], f.sans(18, "SemiBold"), INK, "lm")

    # Division cards: team (class in grey) | RANK | REC | PR | SF, like the
    # site's Power Ratings tables (volleyball has no SF column).
    if ncols == 2:
        fs, inset = min(k * 0.9, 1.6), 18
    else:
        fs, inset = min(k * 0.95, 1.45), 14
    cols = [("RANK", 44), ("REC", 50), ("PR", 54)]
    if edition["sport"] == "football":
        cols.append(("SF", 46))
    span = sum(w for _, w in cols) + 8
    g = min(fs, max(0.7, (COL - inset - 150) / span))  # number columns never squeeze names below ~150
    centers, right = {}, COL - 8 * g
    for label, w in reversed(cols):
        centers[label] = right - w * g / 2
        right -= w * g
    name_end = right - 6
    for index, blocks in enumerate(columns):
        x = PAD + index * (COL + GAP)
        y = body_top
        for division, teams in blocks:
            card_h = HEAD + ROW * len(teams) + CARD_PAD
            box(x + 2, y + 2, x + COL + 2, y + card_h + 2, (222, 222, 220))
            box(x, y, x + COL, y + card_h, ROW_B)
            box(x, y, x + COL, y + HEAD, BLACK)
            label = division.upper()
            if ncols == 3 or edition["sport"] == "football":
                label = label.replace("DIVISION", "DIV.")
            text(x + inset, y + HEAD / 2, label, fit(label, 20 * fs, name_end - inset - 10, f.slab), (255, 255, 255), "lm")
            for col_label, _ in cols:
                text(x + centers[col_label], y + HEAD / 2, col_label, f.sans(11.5 * g, "SemiBold"), GREY, "mm")
            for i, team in enumerate(teams):
                top = y + HEAD + i * ROW
                box(x, top, x + COL, top + ROW, ROW_A if i % 2 == 0 else ROW_B)
                mid = top + ROW / 2
                name = DISPLAY_NAMES.get(team["school"], team["school"])
                cls = team.get("cls") or ""
                cls_font = f.sans(12.5 * fs, "Medium")
                cls_w = (d.textlength(cls, font=cls_font) / s + 6) if cls else 0
                name_font = fit(name, 18.5 * fs, name_end - inset - cls_w, lambda z: f.sans(z, "Medium"))
                text(x + inset, mid, name, name_font, INK, "lm")
                cls_x = x + inset + d.textlength(name, font=name_font) / s + 6
                if cls and cls_x + cls_w - 6 <= x + name_end:
                    text(cls_x, mid + 1, cls, cls_font, GREY, "lm")
                if team["rank"]:
                    text(x + centers["RANK"], mid, f"#{team['rank']}", f.sans(18 * g, "Bold"), TEAL, "mm")
                else:
                    text(x + centers["RANK"], mid, "\u2014", f.sans(18 * g, "Bold"), GREY, "mm")
                text(x + centers["REC"], mid, team.get("record") or "", f.sans(16 * g, "Medium"), INK, "mm")
                text(x + centers["PR"], mid, f"{team['pr']:.2f}", f.sans(18 * g, "Bold"), INK, "mm")
                if "SF" in centers:
                    sf = team.get("sf")
                    text(x + centers["SF"], mid, f"{float(sf):.2f}" if sf is not None else "", f.sans(15 * g, "Medium"), GREY, "mm")
            box(x, y, x + 6, y + card_h, TEAL)
            y += card_h + CARD_GAP
    if not edition["blocks"]:
        text(W / 2, body_top + 30, "No rated teams yet this season", f.sans(18), INK, "mm")

    # Footer.
    foot = H - FOOT_H
    box(0, foot, W, H, BLACK)
    box(0, foot, W, foot + 5, TEAL)
    text(34, foot + 26, "SOURCE: LOUISIANA VS ALL Y’ALL", f.slab(17), (255, 255, 255))
    text(34, foot + 58, RATINGS_PAGE[edition["sport"]] + "   \u2022   @LAvsAllYall",
         f.sans(15, "Regular"), TEAL)
    legend = "PR = POWER RATING  \u2022  REC = RECORD"
    if edition["sport"] == "football":
        legend += "  \u2022  SF = STRENGTH FACTOR"
    text(W - 34, foot + 28, legend, f.sans(12, "SemiBold"), GREY, "ra")
    rank_note = "Division rank shown"
    if any(t["rank"] is None for _, teams in edition["blocks"] for t in teams):
        rank_note += "  \u2022  \u2014 = not in playoffs"
    text(W - 34, foot + 52, rank_note, f.sans(12, "Regular"), GREY, "ra")

    out = io.BytesIO()
    img.save(out, format="PNG", optimize=True)
    return out.getvalue()


# ── Email ────────────────────────────────────────────────────────────────

def _file_name(edition):
    area = "statewide-top-10" if edition["area"] == "STATEWIDE" else edition["area"].lower().replace(" ", "-")
    return f"lvay-{edition['sport']}-power-ratings-{area}-{edition['updated']:%Y-%m-%d}.png"


def _email_body(recipient, editions, preview_for=None):
    items = "".join(
        f"<li><b>{SPORT_LABEL[e['sport']]}</b> — "
        f"{'Top 10 in every division' if e['area'] == 'STATEWIDE' else (html.escape(e['area'].title()) + ' (every team)' if e.get('kind') == 'division' else html.escape(e['area'].title()) + ' teams')}"
        f" (<a href=\"https://{RATINGS_PAGE[e['sport']]}\">full ratings</a>)</li>"
        for e in editions
    )
    note = ""
    if preview_for:
        note = (f"<p style=\"background:#fdf3f1;border:1px solid #B5432B;padding:8px\">"
                f"<b>Preview only.</b> In live mode this goes to {html.escape(preview_for)}.")
        missing = sorted({m for e in editions for m in e["missing"]})
        if missing:
            note += f"<br>Not in this week's ratings: {html.escape(', '.join(missing))}"
        note += "</p>"
    return (
        f"{note}<p>Good morning,</p>"
        f"<p>Here are this week's Louisiana vs All Y'all power ratings, updated after "
        f"Friday night's games. The graphic is attached and free to use on air or in print "
        f"with credit to Louisiana vs All Y'all.</p><ul>{items}</ul>"
        f"<p>These are LVAY's own calculations using the LHSAA formula; only the LHSAA's "
        f"official ratings decide playoff seeding.</p>"
        f"<p>— Mark Miller, Louisiana vs All Y'all<br>"
        f"<a href=\"https://{SITE}\">{SITE}</a></p>"
        f"<p style=\"color:#888;font-size:12px\">Reply STOP and we'll take you off the list.</p>"
    )


def _send(to, subject, body, attachments):
    import requests

    api_key = os.environ.get("RESEND_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("RESEND_API_KEY not configured")
    sender = os.environ.get(
        "MEDIA_EMAIL_FROM",
        "Louisiana vs All Y'all <ratings@updates.louisianavsallyall.com>",
    )
    payload = {"from": sender, "to": to, "subject": subject, "html": body,
               "attachments": attachments}
    reply_to = os.environ.get("MEDIA_EMAIL_REPLY_TO", "").strip()
    if reply_to:
        payload["reply_to"] = reply_to
    response = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json=payload, timeout=60,
    )
    response.raise_for_status()


DAY_NAMES = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


def _already_sent(conn, day, recipient_id):
    conn.execute("""CREATE TABLE IF NOT EXISTS media_sends (
        day TEXT, recipient TEXT, sent_at TEXT, live INTEGER,
        PRIMARY KEY (day, recipient))""")
    return conn.execute("SELECT 1 FROM media_sends WHERE day=? AND recipient=?",
                        (day, recipient_id)).fetchone() is not None


def _internal_body(editions):
    missing = sorted({m for e in editions for m in e["missing"]})
    note = (f"<p>Not in today's ratings: {html.escape(', '.join(missing))}</p>"
            if missing else "")
    return (f"<p>LVAY Power Ratings graphic, updated "
            f"{html.escape(_updated_label(editions[0]['updated']))}.</p>{note}")


def send_media_emails(now=None, force_preview=False, only=None):
    """Draw and send the editions due today. Returns a summary.

    A recipient's "days" (default Saturday) say when it gets an email.
    "internal" recipients (LVAY's own inboxes) always get the real email;
    outlets only do once MEDIA_EMAIL_LIVE=true, otherwise Mark gets a preview.
    """
    now = (now or datetime.now(CENTRAL)).astimezone(CENTRAL)
    outlets_live = (os.environ.get("MEDIA_EMAIL_LIVE", "false").lower() == "true"
                    and not force_preview)
    preview_to = [a.strip() for a in os.environ.get(
        "REPORT_EMAIL_TO", "markmiller36@gmail.com").split(",") if a.strip()][:1]
    config = load_media_list()
    rankings, summary = {}, []
    today, weekday = now.date().isoformat(), DAY_NAMES[now.weekday()]
    conn = sqlite3.connect(DB_PATH)
    try:
        for recipient in config["recipients"]:
            rid = recipient["id"]
            if only:
                if rid not in only:
                    continue
            elif not recipient.get("active") or weekday not in recipient.get("days", ["saturday"]):
                continue
            live = recipient.get("internal") or outlets_live
            if live and not only and _already_sent(conn, today, rid):
                summary.append(f"{rid}: already sent {today}")
                continue
            editions = []
            for spec in expand_specs(recipient["editions"]):
                sport = spec["sport"]
                if sport not in rankings:
                    rankings[sport] = load_rankings(sport)
                season, rows = rankings[sport]
                editions.append(build_edition(sport, rows, season, spec, config["regions"]))
            editions = [e for e in editions if e["blocks"]]
            if not editions:
                summary.append(f"{rid}: nothing rated yet")
                continue
            attachments = [{"filename": _file_name(e),
                            "content": base64.b64encode(render_png(e)).decode()}
                           for e in editions]
            sports = " & ".join(dict.fromkeys(SPORT_LABEL[e["sport"]] for e in editions))
            day = editions[0]["updated"]
            subject = f"LVAY {sports} Power Ratings \u2014 {day:%b} {day.day}"
            if live:
                body = (_internal_body(editions) if recipient.get("internal")
                        else _email_body(recipient, editions))
                _send([recipient["email"]], subject, body, attachments)
                conn.execute("INSERT OR REPLACE INTO media_sends VALUES (?,?,?,1)",
                             (today, rid, now.isoformat()))
                conn.commit()
                summary.append(f"{rid}: sent to {recipient['email']}")
            else:
                target = f"{recipient['name']} <{recipient['email']}>"
                _send(preview_to, "[PREVIEW] " + subject,
                      _email_body(recipient, editions, preview_for=target), attachments)
                summary.append(f"{rid}: preview sent to {preview_to[0]}")
    finally:
        conn.close()
    for line in summary:
        print(f"[MEDIA] {line}")
    return summary


def is_media_slot(now, sports):
    """True right after the 5am football run on Saturday or Sunday."""
    now = now.astimezone(CENTRAL)
    return (DAY_NAMES[now.weekday()] in ("saturday", "sunday") and 5 <= now.hour < 9
            and "football" in (sports or []) and 8 <= now.month <= 12)
