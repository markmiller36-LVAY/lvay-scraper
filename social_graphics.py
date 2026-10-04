"""
Slide artwork for LVAY social posts (1080x1350 JPEG).

House style, matching LVAY's own graphics: flat black, the hand-lettered
"Louisiana vs. All Y'all" script logo (top-left, plus a faint watermark),
big condensed caps with a teal drop shadow, real school logos on white tiles
and a school-color stripe beside each team.  No gradients, no rounded cards.
"""

import hashlib
import io
import os
import re

from PIL import Image, ImageDraw, ImageFont

HERE = os.path.dirname(os.path.abspath(__file__))
FONT_DIR = os.path.join(HERE, "assets", "fonts")
BRAND_MASK = os.path.join(HERE, "assets", "lvay-script-logo-mask.png")
SITE = "https://louisianavsallyall.com"
LOGO_INDEX_URL = SITE + "/wp-content/uploads/2026/09/bayou-power-board-logo-urls-2026.json"
BRAND_LOGO_URL = SITE + "/wp-content/uploads/2025/01/WHITELVAYCLEAR.png"

W, H = 1080, 1350
PAD = 54
BLACK = (14, 14, 14)
WHITE = (255, 255, 255)
TEAL = (0, 133, 132)
TEAL_LIGHT = (0, 178, 176)
GOLD = (242, 182, 50)
GREY = (150, 150, 150)
DIM = (96, 96, 96)
RULE = (44, 44, 44)
FOOTER_H = 92

FONT_FILES = {
    "anton": "Anton-Regular.ttf",
    "xb": "BarlowCondensed-ExtraBold.ttf",
    "xbi": "BarlowCondensed-ExtraBoldItalic.ttf",
    "b": "BarlowCondensed-Bold.ttf",
    "sb": "BarlowCondensed-SemiBold.ttf",
}
_FONTS = {}


def font(kind, size):
    key = (kind, int(size))
    if key not in _FONTS:
        _FONTS[key] = ImageFont.truetype(os.path.join(FONT_DIR, FONT_FILES[kind]), int(size))
    return _FONTS[key]


def text_w(draw, text, fnt):
    box = draw.textbbox((0, 0), text, font=fnt)
    return box[2] - box[0]


def fit(draw, text, kind, size, max_width, min_size=16):
    """Shrink, then trim with an ellipsis, so text fits max_width."""
    size = int(size)
    while size > min_size and text_w(draw, text, font(kind, size)) > max_width:
        size -= 1
    fnt = font(kind, size)
    if text_w(draw, text, fnt) <= max_width:
        return text, fnt
    while len(text) > 3 and text_w(draw, text + "…", fnt) > max_width:
        text = text[:-1]
    return text.rstrip() + "…", fnt


def text_at(draw, xy, text, fnt, fill, anchor="ls"):
    draw.text(xy, text, font=fnt, fill=fill, anchor=anchor)


def shadow_text(draw, xy, text, fnt, fill=WHITE, shadow=TEAL, offset=5, anchor="ls"):
    x, y = xy
    draw.text((x + offset, y + offset), text, font=fnt, fill=shadow, anchor=anchor)
    draw.text((x, y), text, font=fnt, fill=fill, anchor=anchor)


def caps(name):
    return str(name or "").upper()


# ── LOGOS ────────────────────────────────────────────────────

def _cache_dir():
    from social_poster import social_dir
    path = os.path.join(social_dir(), "logo-cache")
    os.makedirs(path, exist_ok=True)
    return path


def _download(url):
    import requests
    path = os.path.join(_cache_dir(), hashlib.md5(url.encode()).hexdigest() + ".png")
    if not os.path.exists(path):
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        Image.open(io.BytesIO(response.content)).convert("RGBA").save(path)
    return Image.open(path).convert("RGBA")


class Logos:
    """School logos + colors from the site's logo index, and the LVAY script logo."""

    def __init__(self):
        self.index = None
        self.tiles = {}
        self.local = {}   # name -> local image path (previews/tests)
        self._brand = None

    def load_index(self):
        if self.index is None:
            self.index = {}
            try:
                import requests
                response = requests.get(LOGO_INDEX_URL, timeout=15)
                if response.ok:
                    self.index = {k.casefold(): v for k, v in response.json().items()}
            except Exception as exc:
                print(f"[SOCIAL] Logo index unavailable: {exc}")
        return self.index

    def entry(self, school):
        value = self.load_index().get(str(school).casefold())
        return value if isinstance(value, dict) else {}

    def color(self, school):
        value = str(self.entry(school).get("color") or "")
        if re.match(r"^#[0-9a-fA-F]{6}$", value):
            rgb = tuple(int(value[i:i + 2], 16) for i in (1, 3, 5))
            # white/very light school colors disappear on the tile edge; use teal
            if sum(rgb) > 680:
                return TEAL
            lum = .2126 * rgb[0] + .7152 * rgb[1] + .0722 * rgb[2]
            if lum < 60:  # dark colors disappear on the black background
                return tuple(int(v + (255 - v) * .4) for v in rgb)
            return rgb
        return TEAL

    def tile(self, school, size):
        """Logo on a white square tile; initials in school color if no logo."""
        key = (str(school).casefold(), size)
        if key in self.tiles:
            return self.tiles[key]
        tile = Image.new("RGB", (size, size), WHITE)
        art = None
        try:
            if school in self.local:
                art = Image.open(self.local[school]).convert("RGBA")
            elif self.entry(school).get("logo"):
                art = _download(self.entry(school)["logo"])
        except Exception as exc:
            print(f"[SOCIAL] Logo failed for {school}: {exc}")
        if art is not None:
            inner = int(size * .9)
            art.thumbnail((inner, inner), Image.LANCZOS)
            if art.width < inner and art.height < inner:  # upscale tiny logos
                scale = inner / max(art.width, art.height)
                art = art.resize((max(1, int(art.width * scale)), max(1, int(art.height * scale))),
                                 Image.LANCZOS)
            tile.paste(art, ((size - art.width) // 2, (size - art.height) // 2), art)
        else:
            d = ImageDraw.Draw(tile)
            words = [w for w in re.split(r"[\s\-./]+", str(school)) if w and w[0].isalnum()]
            letters = "".join(w[0] for w in words[:2]).upper() or "?"
            d.rectangle((0, 0, size, size), fill=self.color(school))
            d.text((size / 2, size / 2), letters, font=font("anton", size * .5), fill=WHITE, anchor="mm")
        self.tiles[key] = tile
        return tile

    def brand_mask(self):
        """'L' mask of the white script logo (bigger original when reachable)."""
        if self._brand is None:
            mask = None
            if not self.local:
                try:
                    art = _download(BRAND_LOGO_URL)
                    alpha = art.split()[-1]
                    mask = alpha.crop(alpha.getbbox())
                except Exception as exc:
                    print(f"[SOCIAL] Using built-in LVAY logo: {exc}")
            if mask is None:
                mask = Image.open(BRAND_MASK).convert("L")
            self._brand = mask
        return self._brand

    def brand(self, width, color=WHITE, opacity=255):
        mask = self.brand_mask()
        height = int(mask.height * width / mask.width)
        mask = mask.resize((width, height), Image.LANCZOS)
        if opacity < 255:
            mask = mask.point(lambda v: v * opacity // 255)
        layer = Image.new("RGBA", (width, height), color + (0,))
        layer.putalpha(mask)
        return layer


LOGOS = Logos()


# ── FRAME ────────────────────────────────────────────────────

def canvas():
    image = Image.new("RGB", (W, H), BLACK)
    mark = LOGOS.brand(1250, WHITE, 13).rotate(-9, expand=True, resample=Image.BICUBIC)
    image.paste(mark, ((W - mark.width) // 2 + 60, H - mark.height + 40), mark)
    return image, ImageDraw.Draw(image)


def header(image, draw, kicker, title, big=True):
    """Script logo top-left, kicker + title to its right.  Returns content top y."""
    logo_w = 230 if big else 168
    logo = LOGOS.brand(logo_w)
    image.paste(logo, (PAD - 6, 34), logo)
    x = PAD + logo_w + 26
    avail = W - PAD - x
    text_at(draw, (x, 92 if big else 80), caps(kicker), font("sb", 34 if big else 30), TEAL_LIGHT)
    line, fnt = fit(draw, caps(title), "anton", 104 if big else 80, avail - 8, 40)
    shadow_text(draw, (x, (206 if big else 166)), line, fnt, offset=6 if big else 5)
    bottom = 34 + logo.height + 22
    draw.rectangle((PAD, bottom, W - PAD, bottom + 4), fill=WHITE)
    return bottom + 28


def footer(draw, cta, page=None, pages=None):
    top = H - FOOTER_H
    draw.rectangle((0, top, W, H), fill=TEAL)
    text_at(draw, (PAD, top + 63), "LOUISIANAVSALLYALL.COM", font("anton", 46), WHITE)
    right = W - PAD
    if page and pages and pages > 1:
        counter = f"{page}/{pages}"
        text_at(draw, (right, top + 60), counter, font("sb", 34), (210, 240, 239), "rs")
        right -= text_w(draw, counter, font("sb", 34)) + 24
    text_at(draw, (right, top + 60), caps(cta) + "  ›", font("xb", 36), WHITE, "rs")


def save(image, path):
    image.save(path, "JPEG", quality=92, optimize=True, progressive=True, subsampling=0)


def _path(outdir, n):
    return os.path.join(outdir, f"{n:02d}.jpg")


# ── BIG GAMES ────────────────────────────────────────────────

def big_games_slides(games, week, outdir):
    from social_poster import division_short
    per = 3
    pages = max(1, (len(games) + per - 1) // per)
    paths = []
    for page in range(pages):
        image, draw = canvas()
        y = header(image, draw, f"Week {week} · Friday Night", "Big Games" if page == 0 else "More Big Games",
                   big=page == 0)
        chunk = games[page * per:(page + 1) * per]
        block = (H - FOOTER_H - 20 - y) // per
        for game in chunk:
            label = "DISTRICT" if game["district"] else "NON-DISTRICT"
            if game["class"]:
                label += f" · CLASS {game['class']}"
            text_at(draw, (PAD, y + 26), label, font("sb", 28), GREY)
            row_h = (block - 58) // 2
            for index, side in enumerate(("winner", "loser")):
                top = y + 40 + index * row_h
                won = side == "winner" or game["tie"]
                team = game[side]
                draw.rectangle((PAD, top + 6, PAD + 12, top + row_h - 6), fill=LOGOS.color(team))
                tile_size = row_h - 16
                image.paste(LOGOS.tile(team, tile_size), (PAD + 24, top + 8))
                nx = PAD + 24 + tile_size + 24
                score = str(game[f"{side}_pts"])
                sf = font("anton", int(row_h * .78))
                sw = text_w(draw, score, sf)
                name, nf = fit(draw, caps(team), "anton", row_h * .40, W - PAD - sw - 40 - nx, 24)
                text_at(draw, (nx, top + row_h * .52), name, nf, WHITE if won else GREY)
                rank = game[f"{side}_rank"]
                bits = []
                if rank:
                    bits.append(f"#{rank[0]} {division_short(rank[1])}")
                if game.get(f"{side}_record"):
                    bits.append(game[f"{side}_record"])
                text_at(draw, (nx, top + row_h * .52 + 42), "  ·  ".join(bits),
                        font("sb", 30), TEAL_LIGHT if won else DIM)
                text_at(draw, (W - PAD, top + row_h * .5), score, sf,
                        WHITE if won else DIM, "rm")
            y += block
            draw.rectangle((PAD, y - 10, W - PAD, y - 8), fill=RULE)
        footer(draw, "All scores", page + 1, pages)
        paths.append(_path(outdir, page + 1))
        save(image, paths[-1])
    return paths


# ── FRIDAY NIGHT FINALS (BY CLASS) ───────────────────────────

ROWS_PER_COLUMN = 15


def _columns(finals):
    from social_poster import class_sort_key
    by_class = {}
    for game in finals:
        by_class.setdefault(game["class"] or "Other", []).append(game)
    columns, column = [], []
    for class_name in sorted(by_class, key=class_sort_key):
        games = sorted(by_class[class_name], key=lambda g: g["winner"].casefold())
        if len(column) >= ROWS_PER_COLUMN - 1:
            columns.append(column)
            column = []
        column.append(("header", class_name, False))
        for game in games:
            if len(column) >= ROWS_PER_COLUMN:
                columns.append(column)
                column = [("header", class_name, True)]
            column.append(("game", game))
    if column:
        columns.append(column)
    return columns


def roundup_slides(finals, week, outdir):
    from collections import Counter
    columns = _columns(finals)
    pages = [columns[i:i + 2] for i in range(0, len(columns), 2)]
    total = len(pages) + 1
    paths = []

    # cover
    image, draw = canvas()
    y = header(image, draw, f"Week {week} · LHSAA Football", "Friday Night Finals")
    shadow_text(draw, (PAD - 4, y + 300), str(len(finals)), font("anton", 330), WHITE, TEAL, 10)
    text_at(draw, (PAD, y + 370), "FINAL SCORES FROM ACROSS LOUISIANA", font("xb", 50), WHITE)
    text_at(draw, (PAD, y + 420), "EVERY ONE, BY CLASS. SWIPE  ›", font("sb", 38), TEAL_LIGHT)
    counts = Counter(g["class"] or "Other" for g in finals)
    from social_poster import class_sort_key
    cy = y + 480
    for class_name in sorted(counts, key=class_sort_key):
        draw.rectangle((PAD, cy, W - PAD, cy + 2), fill=RULE)
        text_at(draw, (PAD, cy + 66), f"CLASS {class_name}", font("anton", 50), WHITE)
        text_at(draw, (W - PAD, cy + 66), f"{counts[class_name]} GAMES", font("xb", 44), GREY, "rs")
        cy += 84
        if cy > H - FOOTER_H - 90:
            break
    footer(draw, "All scores", 1, total)
    paths.append(_path(outdir, 1))
    save(image, paths[-1])

    col_w = (W - PAD * 2 - 36) // 2
    for number, page in enumerate(pages, 2):
        image, draw = canvas()
        classes = []
        for col in page:
            for item in col:
                if item[0] == "header" and item[1] not in classes:
                    classes.append(item[1])
        y0 = header(image, draw, f"Week {week} · Friday Night Finals",
                    "Class " + " & ".join(classes), big=False)
        row_h = (H - FOOTER_H - 16 - y0) // ROWS_PER_COLUMN
        for c, col in enumerate(page):
            x = PAD + c * (col_w + 36)
            y = y0
            for item in col:
                if item[0] == "header":
                    draw.rectangle((x, y + 8, x + col_w, y + row_h - 8), fill=TEAL)
                    label = f"CLASS {item[1]}" + ("  (CONT.)" if item[2] else "")
                    text_at(draw, (x + 14, y + row_h / 2 + 2), label, font("anton", row_h * .52), WHITE, "lm")
                else:
                    score_row(draw, item[1], x, y, col_w, row_h)
                y += row_h
        footer(draw, "All scores", number, total)
        paths.append(_path(outdir, number))
        save(image, paths[-1])
    return paths


def score_row(draw, game, x, y, w, h):
    draw.rectangle((x, y + 6, x + 6, y + h - 6), fill=LOGOS.color(game["winner"]))
    half = (h - 8) / 2
    for index, side in enumerate(("winner", "loser")):
        won = side == "winner" or game["tie"]
        base = y + 4 + half * (index + 1) - 6
        score = str(game[f"{side}_pts"])
        sf = font("anton", half * .92)
        sw = text_w(draw, score, sf)
        name, nf = fit(draw, caps(game[side]), "xb" if won else "b", half * .86, w - sw - 40, 14)
        text_at(draw, (x + 18, base), name, nf, WHITE if won else GREY)
        text_at(draw, (x + w, base), score, sf, WHITE if won else DIM, "rs")
    draw.rectangle((x + 18, y + h - 2, x + w, y + h - 1), fill=RULE)


# ── POWER RATINGS ────────────────────────────────────────────

def ratings_slides(divisions, outdir, updated):
    from social_poster import division_short, record_text
    total = len(divisions) + 1
    paths = []

    image, draw = canvas()
    y = header(image, draw, f"LVAY Football · Updated {updated}", "Power Ratings")
    text_at(draw, (PAD, y + 40), "THE #1 TEAM IN EVERY LHSAA DIVISION. SWIPE FOR THE TOP 10  ›",
            fit(draw, "THE #1 TEAM IN EVERY LHSAA DIVISION. SWIPE FOR THE TOP 10  ›", "sb", 34, W - 2 * PAD)[1],
            TEAL_LIGHT)
    y += 70
    row_h = min(112, (H - FOOTER_H - 20 - y) // max(1, len(divisions)))
    for division in divisions:
        leader = division["teams"][0] if division["teams"] else None
        text_at(draw, (PAD, y + row_h * .66), division_short(division["division"]), font("anton", row_h * .5), GOLD)
        if leader:
            tile = row_h - 22
            image.paste(LOGOS.tile(leader["school"], tile), (PAD + 170, y + 11))
            name, nf = fit(draw, caps(leader["school"]), "anton", row_h * .44, W - PAD - (PAD + 200 + tile) - 150)
            text_at(draw, (PAD + 194 + tile, y + row_h * .64), name, nf, WHITE)
            text_at(draw, (W - PAD, y + row_h * .64), f"{float(leader.get('power_rating') or 0):.2f}",
                    font("anton", row_h * .42), WHITE, "rs")
        y += row_h
        draw.rectangle((PAD, y - 2, W - PAD, y), fill=RULE)
    footer(draw, "Full ratings", 1, total)
    paths.append(_path(outdir, 1))
    save(image, paths[-1])

    for number, division in enumerate(divisions, 2):
        image, draw = canvas()
        y = header(image, draw, "Power Ratings · Top 10", division["division"], big=False)
        cols = [("#", PAD), ("TEAM", PAD + 190), ("CLASS", 700), ("REC", 800), ("RATING", W - PAD)]
        for label, x in cols:
            text_at(draw, (x, y + 20), label, font("sb", 28), GREY, "rs" if label == "RATING" else "ls")
        y += 36
        row_h = (H - FOOTER_H - 16 - y) // 10
        for index, team in enumerate(division["teams"], 1):
            mid = y + row_h / 2
            draw.rectangle((PAD + 76, y + 10, PAD + 84, y + row_h - 10), fill=LOGOS.color(team["school"]))
            rank_font = font("anton", row_h * .62)
            if index == 1:
                shadow_text(draw, (PAD, mid + row_h * .22), "1", rank_font, GOLD, TEAL, 4)
            else:
                text_at(draw, (PAD, mid + row_h * .22), str(index), rank_font, WHITE)
            tile = row_h - 20
            image.paste(LOGOS.tile(team["school"], tile), (PAD + 96, y + 10))
            name, nf = fit(draw, caps(team["school"]), "anton", row_h * .40, 690 - (PAD + 112 + tile), 20)
            text_at(draw, (PAD + 112 + tile, mid + row_h * .15), name, nf, WHITE)
            text_at(draw, (700, mid + row_h * .15), str(team.get("class_") or ""), font("xb", row_h * .4), GREY)
            text_at(draw, (800, mid + row_h * .15),
                    record_text(team.get("wins"), team.get("losses"), team.get("ties")),
                    font("xb", row_h * .4), WHITE)
            text_at(draw, (W - PAD, mid + row_h * .17), f"{float(team.get('power_rating') or 0):.2f}",
                    font("anton", row_h * .46), GOLD if index == 1 else WHITE, "rs")
            y += row_h
            draw.rectangle((PAD, y - 1, W - PAD, y), fill=RULE)
        footer(draw, "Full ratings", number, total)
        paths.append(_path(outdir, number))
        save(image, paths[-1])
    return paths


# ── DISTRICT STANDINGS ───────────────────────────────────────

def standings_slides(tables, class_name, outdir, max_slides=10):
    groups, current, used, capacity = [], [], 0, 22
    for table in tables:
        need = len(table["teams"]) + 2
        if current and used + need > capacity:
            groups.append(current)
            current, used = [], 0
        current.append(table)
        used += need
    if current:
        groups.append(current)
    groups = groups[:max_slides]
    paths = []
    for number, group in enumerate(groups, 1):
        image, draw = canvas()
        y = header(image, draw, f"Class {class_name} · LHSAA Football",
                   "District Standings" if number == 1 else f"Class {class_name} Standings",
                   big=number == 1)
        rows = sum(len(t["teams"]) + 2 for t in group)
        row_h = min(56, (H - FOOTER_H - 16 - y) // max(1, rows))
        cols = [("DIST", 640), ("OVERALL", 770), ("PF", 900), ("PA", W - PAD)]
        for table in group:
            draw.rectangle((PAD, y + 4, W - PAD, y + row_h - 2), fill=TEAL)
            text_at(draw, (PAD + 14, y + row_h / 2 + 2), f"DISTRICT {table['district']}-{class_name}",
                    font("anton", row_h * .58), WHITE, "lm")
            y += row_h
            for label, x in cols:
                text_at(draw, (x, y + row_h * .7), label, font("sb", row_h * .48), GREY,
                        "rs" if label == "PA" else "ls")
            y += row_h
            for team in table["teams"]:
                base = y + row_h * .74
                lead = team["rank"] == 1
                draw.rectangle((PAD + 44, y + 9, PAD + 50, y + row_h - 9), fill=LOGOS.color(team["team"]))
                f = font("xb", row_h * .62)
                text_at(draw, (PAD, base), str(team["rank"]), font("anton", row_h * .6), GOLD if lead else WHITE)
                name, nf = fit(draw, caps(team["team"]), "xb", row_h * .62, 640 - (PAD + 64) - 20, 14)
                text_at(draw, (PAD + 64, base), name, nf, WHITE)
                text_at(draw, (640, base), team["district"], font("anton", row_h * .58), GOLD if lead else WHITE)
                text_at(draw, (770, base), team["overall"], f, GREY)
                text_at(draw, (900, base), str(team["pf"]), f, DIM)
                text_at(draw, (W - PAD, base), str(team["pa"]), f, DIM, "rs")
                y += row_h
                draw.rectangle((PAD, y - 1, W - PAD, y), fill=RULE)
            y += 10
        footer(draw, "Standings/Stats", number, len(groups))
        paths.append(_path(outdir, number))
        save(image, paths[-1])
    return paths
