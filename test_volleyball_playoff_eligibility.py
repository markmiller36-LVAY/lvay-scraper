import sqlite3

from volleyball_playoff_eligibility import (
    PLAYOFF_FIELD, annotate_rankings, not_playing_keys, parity_report, parse_report,
    update_playoff_eligibility,
)


def _table(rows):
    body = "".join(
        f'<tr><td>{i}</td><td class="low-games"><a href="#">{name}</a></td>'
        f'<td>{power:.2f}</td><td>{w}</td><td>{l}</td><td>3</td></tr>'
        for i, (name, power, w, l) in enumerate(rows, 1)
    )
    return f'<table border="1"><tbody><tr><th>#</th><th>School</th></tr>{body}</tbody></table>'


REPORT = (
    '<div>Division I</div>' + _table([("Mandeville", 17.49, 21, 2), ("Mt. Carmel", 14.66, 14, 2)])
    + '<span><b>Schools not Participating in the Playoff</b></span><div>Division I</div>'
    + _table([("Huntington", 4.56, 5, 5)])
    + '<div>Division II</div>' + _table([("St. Thomas More", 15.33, 19, 3)])
    + '<div>Division II</div><b>There are No Schools excluded from the Playoff.</b>'
    + '<div>Division IV</div>' + _table([("Catholic - N.I.", 14.66, 18, 2)])
    + '<div>Division V</div>' + _table([("Westminster Christian", 18.14, 21, 0)])
    + '<span>Schools not playing in the Playoff</span><div>Division V</div>'
    + _table([("Thrive Academy", 1.90, 0, 8)])
)


def _db(tmp_path):
    db = str(tmp_path / "vb.db")
    conn = sqlite3.connect(db)
    conn.execute("""CREATE TABLE volleyball_rankings (sport TEXT, season TEXT, school TEXT, division TEXT,
        power_rating REAL, wins INT, losses INT, games_played INT, div_rank INT)""")
    rows = [("Mandeville", "Division I", 17.49, 21, 2, 1), ("Mt. Carmel", "Division I", 14.70, 14, 2, 2),
            ("Huntington", "Division I", 4.56, 5, 5, 3), ("St. Thomas More", "Division II", 15.33, 19, 3, 1),
            ("Catholic - N.I.", "Division IV", 14.66, 18, 2, 1),
            ("Westminster Christian", "Division V", 18.14, 21, 0, 1), ("Thrive Academy", "Division V", 1.90, 0, 8, 2)]
    conn.executemany("INSERT INTO volleyball_rankings VALUES ('volleyball','2026',?,?,?,?,?,?,?)",
                     [(s, d, p, w, l, w + l, r) for s, d, p, w, l, r in rows])
    conn.commit()
    return db, conn


def test_parse_splits_not_playing_and_ignores_no_exclusions_notice():
    details = []
    parsed = parse_report(REPORT, details)
    assert parsed["ranked"]["I"] == ["Mandeville", "Mt. Carmel"]
    assert parsed["not_playing"] == {"I": ["Huntington"], "V": ["Thrive Academy"]}
    assert "II" not in parsed["not_playing"]
    mandeville = next(d for d in details if d["school"] == "Mandeville")
    assert (mandeville["power"], mandeville["wins"], mandeville["losses"]) == (17.49, 21, 2)


def test_update_annotate_and_parity(tmp_path):
    db, conn = _db(tmp_path)
    assert update_playoff_eligibility("2026", db_path=db, fetch=lambda url: REPORT)["updated"]
    assert set(not_playing_keys(conn, "2026")) == {not_playing_keys.__globals__["school_key"](n)
                                                   for n in ("Huntington", "Thrive Academy")}
    rows = [{"school": s, "division": d, "div_rank": r} for s, d, r in
            conn.execute("SELECT school, division, div_rank FROM volleyball_rankings")]
    annotate_rankings(rows, conn, "2026")
    by = {r["school"]: r for r in rows}
    assert by["Huntington"]["playoff_eligible"] is False and by["Huntington"]["division_rank"] is None
    assert by["Mt. Carmel"]["division_rank"] == 2 and by["Mt. Carmel"]["in_playoff_field"]
    assert PLAYOFF_FIELD == 32

    report = parity_report(conn, "2026")
    assert report["available"] and report["mismatch_count"] == 1
    assert report["mismatches"][0]["school"] == "Mt. Carmel"
    assert report["mismatches"][0]["problems"] == ["power"]

    # A failed read keeps the last good list.
    def broken(url):
        raise RuntimeError("LHSAA down")
    assert not update_playoff_eligibility("2026", db_path=db, fetch=broken)["updated"]
    assert len(not_playing_keys(conn, "2026")) == 2
