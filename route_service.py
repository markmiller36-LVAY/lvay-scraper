"""
LVAY - Matchup driving routes
=============================
Driving route between two schools for the Playoff HQ Matchup Map.

GET /api/route?a=<lat>,<lng>&b=<lat>,<lng>
  -> {"status": "ok", "mi": 212.4, "min": 198, "via": ["I-49", "I-10"],
      "pts": [[lat, lng], ...]}

Routes come from the public OSRM server (OpenStreetMap data) and are cached
forever in SQLite, so each school pair is only ever looked up once. OSRM is
called at most a few times a second, and a request that cannot get a turn
quickly returns "busy" instead of tying up a web thread. Nov 3, 2026.
"""

import json
import os
import re
import sqlite3
import threading
import time

import requests
from flask import Blueprint, jsonify, request

bp = Blueprint("route_service", __name__)

OSRM_URL = os.environ.get(
    "OSRM_URL", "https://router.project-osrm.org/route/v1/driving/")
DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")
METERS_PER_MILE = 1609.344
_MIN_GAP_SECONDS = 0.4
_LOCK = threading.Lock()
_LAST_CALL = [0.0]

# Louisiana plus a margin, so nobody uses this as a free routing API.
_LAT_RANGE = (28.5, 33.6)
_LNG_RANGE = (-94.6, -88.5)


def _parse_point(text):
    try:
        lat, lng = (float(x) for x in (text or "").split(","))
    except ValueError:
        return None
    if not (_LAT_RANGE[0] <= lat <= _LAT_RANGE[1]):
        return None
    if not (_LNG_RANGE[0] <= lng <= _LNG_RANGE[1]):
        return None
    return round(lat, 5), round(lng, 5)


def _cache_key(a, b):
    return "%.5f,%.5f|%.5f,%.5f" % (a[0], a[1], b[0], b[1])


def _db():
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS route_cache ("
        "k TEXT PRIMARY KEY, body TEXT, fetched_at TEXT)")
    return conn


def _cache_get(key):
    try:
        conn = _db()
        row = conn.execute(
            "SELECT body FROM route_cache WHERE k = ?", (key,)).fetchone()
        conn.close()
        return json.loads(row[0]) if row else None
    except Exception:
        return None


def _cache_put(key, body):
    try:
        conn = _db()
        conn.execute(
            "INSERT OR REPLACE INTO route_cache (k, body, fetched_at) "
            "VALUES (?, ?, datetime('now'))", (key, json.dumps(body)))
        conn.commit()
        conn.close()
    except Exception:
        pass


_REF_RE = re.compile(r"^(I|US|LA|TX|MS|AR)\s*-?\s*(\d+[A-Z]?)$", re.I)


def _clean_ref(ref):
    m = _REF_RE.match(ref.strip())
    if not m:
        return None
    return "%s-%s" % (m.group(1).upper(), m.group(2).upper())


def highway_summary(steps, min_miles=6.0):
    """Ordered list of the highways that carry real distance, e.g.
    ["I-49", "I-10"]. Interstates/US routes count from 6 mi, state routes
    from 12 mi, so short connectors don't clutter the summary."""
    totals = {}
    order = []
    for step in steps:
        miles = (step.get("distance") or 0) / METERS_PER_MILE
        refs = [_clean_ref(r) for r in (step.get("ref") or "").split(";")]
        refs = [r for r in refs if r]
        if not refs:
            continue
        # Prefer the interstate when a road carries several numbers.
        refs.sort(key=lambda r: 0 if r.startswith("I-") else
                  1 if r.startswith("US-") else 2)
        ref = refs[0]
        totals[ref] = totals.get(ref, 0) + miles
        if not order or order[-1] != ref:
            order.append(ref)
    out = []
    for ref in order:
        need = min_miles if ref[:2] in ("I-", "US") else min_miles * 2
        if totals.get(ref, 0) >= need and (not out or out[-1] != ref):
            if ref not in out:
                out.append(ref)
    return out


def _thin(coords, step=1):
    pts = [[round(c[1], 4), round(c[0], 4)] for c in coords[::step]]
    last = [round(coords[-1][1], 4), round(coords[-1][0], 4)]
    if pts[-1] != last:
        pts.append(last)
    return pts


def fetch_route(a, b):
    url = "%s%f,%f;%f,%f" % (OSRM_URL, a[1], a[0], b[1], b[0])
    resp = requests.get(url, params={
        "overview": "simplified", "geometries": "geojson",
        "steps": "true"}, timeout=10,
        headers={"User-Agent": "LVAY-PlayoffHQ/1.0 (louisianavsallyall.com)"})
    data = resp.json()
    if data.get("code") != "Ok" or not data.get("routes"):
        return None
    route = data["routes"][0]
    steps = [s for leg in route.get("legs", []) for s in leg.get("steps", [])]
    coords = route["geometry"]["coordinates"]
    return {
        "mi": round(route["distance"] / METERS_PER_MILE, 1),
        "min": round(route["duration"] / 60),
        "via": highway_summary(steps),
        "pts": _thin(coords, 1 if len(coords) < 400 else 2),
    }


@bp.route("/api/route")
def api_route():
    a = _parse_point(request.args.get("a"))
    b = _parse_point(request.args.get("b"))
    if not a or not b:
        return jsonify({"status": "error",
                        "message": "a and b must be lat,lng in Louisiana"}), 400
    key = _cache_key(a, b)
    cached = _cache_get(key)
    if cached:
        resp = jsonify(dict(cached, status="ok"))
        resp.headers["Cache-Control"] = "public, max-age=604800"
        return resp
    if not _LOCK.acquire(timeout=3):
        return jsonify({"status": "busy"}), 503
    try:
        wait = _MIN_GAP_SECONDS - (time.time() - _LAST_CALL[0])
        if wait > 0:
            time.sleep(wait)
        _LAST_CALL[0] = time.time()
        try:
            body = fetch_route(a, b)
        except Exception:
            body = None
    finally:
        _LOCK.release()
    if not body:
        return jsonify({"status": "unavailable"}), 502
    _cache_put(key, body)
    resp = jsonify(dict(body, status="ok"))
    resp.headers["Cache-Control"] = "public, max-age=604800"
    return resp
