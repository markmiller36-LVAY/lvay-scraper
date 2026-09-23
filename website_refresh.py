"""Refresh WordPress after exports, then check anonymous football ratings."""
import os
import time
from decimal import Decimal

import requests
from bs4 import BeautifulSoup

SITE = "https://louisianavsallyall.com"
API = "https://lvay-scraper.onrender.com"


def verify_football(html, rankings):
    """Compare every published school, record, GP, rating and strength factor."""
    published_divisions = {f"{track} Division {division}"
                           for track in ("Select", "Non-Select")
                           for division in ("I", "II", "III", "IV")}
    rankings = [r for r in rankings if r.get("division") in published_divisions]
    actual = {}
    for row in BeautifulSoup(html, "html.parser").select("table.lvay-rtbl tbody tr"):
        cells = [c.get_text(" ", strip=True) for c in row.select("td")]
        if len(cells) != 7:
            continue
        school = cells[1]
        if school in actual:
            raise ValueError("Duplicate school in public ratings")
        actual[school] = cells
    if not rankings or set(actual) != {r["school"] for r in rankings}:
        raise ValueError("Public ratings school list differs from current ratings")
    for row in rankings:
        cells = actual[row["school"]]
        record = f"{row['wins']}-{row['losses']}"
        if row.get("ties"):
            record += f"-{row['ties']}"
        if (cells[2] != row["class_"] or cells[3] != record
                or int(cells[4]) != row["games_played"]
                or Decimal(cells[5]) != Decimal(f"{row['power_rating']:.2f}")
                or Decimal(cells[6]) != Decimal(f"{row['strength_factor']:.2f}")):
            raise ValueError("Public ratings contain stale values")
    return len(rankings)


def refresh_website(active_sports):
    if os.getenv("WEBSITE_REFRESH_ENABLED", "false").lower() != "true":
        return "Not enabled; website freshness was not checked."
    username = os.getenv("WORDPRESS_USERNAME", "").strip()
    password = os.getenv("WORDPRESS_APP_PASSWORD", "").strip()
    if not username or not password:
        raise RuntimeError("Website refresh enabled but WordPress credentials are missing")

    # Snapshot expected results before purging. No authenticated response is used
    # as evidence of public freshness, and credentials never go to the read URLs.
    rankings = None
    if "football" in active_sports:
        from scraper import resolve_season_year
        season = int(resolve_season_year("football"))
        response = requests.get(f"{API}/api/rankings/football",
                                params={"season": season}, timeout=45)
        response.raise_for_status()
        payload = response.json()
        if payload.get("season") != season or not payload.get("rankings"):
            raise RuntimeError("No current football ratings available for website verification")
        rankings = payload["rankings"]

    try:
        response = requests.post(
            f"{SITE}/wp-json/lvay/v1/refresh-cache",
            auth=(username, password), json={}, timeout=90,
            allow_redirects=False,
        )
        if response.status_code != 200 or response.json().get("purge_requested") is not True:
            raise RuntimeError("WordPress did not acknowledge the cache refresh")
    except requests.RequestException:
        # Do not echo request objects, authentication data or remote response bodies.
        raise RuntimeError("WordPress cache refresh request failed") from None

    if rankings is None:
        return "Cache refresh requested; no football ratings verification applicable."
    # The plugin also queues hosting-cache work. Probe the ordinary public URL,
    # without cookies or cache-busting parameters, until it actually catches up.
    for attempt in range(13):
        try:
            page = requests.get(f"{SITE}/power-rankings/", timeout=45)
            page.raise_for_status()
            count = verify_football(page.text, rankings)
            omitted = len(rankings) - count
            return (f"Cache refreshed; all {count} public football ratings verified. "
                    f"{omitted} unclassified backend schools are outside the website division tables.")
        except (requests.RequestException, ValueError, ArithmeticError):
            if attempt == 12:
                break
            time.sleep(15)
    raise RuntimeError("Website cache refresh requested, but public football ratings remain stale or unavailable")
