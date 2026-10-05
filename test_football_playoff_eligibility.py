import sqlite3

from football_playoff_eligibility import ineligible_schools, parse_report, update_playoff_eligibility


def _table(rows):
    body = "".join(
        f'<tr><td width="2%">{i}</td><td class="low-games"><font size="2"><a href="#">{name}</a></font></td>'
        f'<td>10.00</td><td>5.00</td><td>3</td><td>2</td><td>7</td></tr>'
        for i, name in enumerate(rows, 1)
    )
    return ('<div> <table border="1"><tbody><tr bgcolor="LightYellow"><th>#</th><th>School</th></tr>'
            f'{body}</tbody></table> </div>')


# Mirrors the LHSAA report markup (Oct 2026): ranked table per division, then an
# optional "Schools not playing in the Playoff" heading + table for that division.
SELECT_HTML = (
    '<span>Football Select Schools Power Rating Report by Division</span>'
    '<div style="font-family:Arial;font-size:12pt;">Division I</div>' + _table(["Archbishop Rummel", "Edna Karr"]) +
    '<br><span style="margin-top:11px;"><b><font face="Arial" size="4">Schools not playing in the Playoff</font></b></span>'
    '<span><div style="font-family:Arial;font-size:12pt;">Division I</div><br></span><br class="page">' + _table(["Ben Franklin"]) +
    '<div>Division II</div>' + _table(["Madison Prep"]) +
    '<div>Division IV</div>' + _table(["Ouachita Christian", "Thrive Academy"]) +
    '<span><b>Schools not playing in the Playoff</b></span><span><div>Division IV</div></span>' +
    _table(["Ascension Christian", "Acadiana Christian", "Highland Baptist"])
)
NONSELECT_HTML = (
    '<div>Division I</div>' + _table(["Ruston"]) +
    '<div>Division IV</div>' + _table(["Oberlin"]) +
    '<span>Schools not playing in the Playoff</span><div>Division IV</div>' + _table(["Gueydan", "Plain Dealing"])
)


def test_parse_report_splits_not_playing_sections():
    parsed = parse_report(SELECT_HTML)
    assert parsed["ranked"]["I"] == ["Archbishop Rummel", "Edna Karr"]
    assert parsed["ranked"]["II"] == ["Madison Prep"]
    assert parsed["not_playing"]["I"] == ["Ben Franklin"]
    assert parsed["not_playing"]["IV"] == ["Ascension Christian", "Acadiana Christian", "Highland Baptist"]
    assert "Ben Franklin" not in parsed["ranked"]["I"]


def test_update_stores_lhsaa_list_and_keeps_it_on_failure(tmp_path):
    db = str(tmp_path / "e.db")
    pages = {"FBSelectbyDivisionPR": SELECT_HTML, "FBNonselectByDivisionPR": NONSELECT_HTML}
    fetch = lambda url: next(v for k, v in pages.items() if k in url)
    result = update_playoff_eligibility("2026", db_path=db, fetch=fetch)
    assert result["updated"]
    conn = sqlite3.connect(db)
    out = ineligible_schools(conn, "2026")
    assert {"Ben Franklin", "Ascension Christian", "Acadiana Christian", "Highland Baptist", "Gueydan", "Plain Dealing"} <= out
    assert "Ruston" not in out and "Edna Karr" not in out

    def broken(url):
        raise RuntimeError("LHSAA down")
    assert not update_playoff_eligibility("2026", db_path=db, fetch=broken)["updated"]
    assert ineligible_schools(conn, "2026") == out  # last good list kept


def test_fallback_uses_alignment_when_nothing_stored(tmp_path):
    conn = sqlite3.connect(str(tmp_path / "empty.db"))
    out = ineligible_schools(conn, "2026")
    assert {"Ben Franklin", "Morris Jeff", "Highland Baptist", "Pine Prairie"} <= out
