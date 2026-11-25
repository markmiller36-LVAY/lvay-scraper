from winter_parity import parse_report

BB = """<p>Date: 11/25/2026 6:01:02 AM</p><p>Girls Basketball Power Ranking Report By Class</p>
<p>Class B</p><table><tr><td>#</td><td>School</td><td>PR</td><td>W</td><td>L</td><td>District</td></tr>
<tr><td>1.</td><td>Stanley</td><td>41.13</td><td>5</td><td>1</td><td>4-B</td></tr></table>
<p>Schools not playing in the Playoff</p><table><tr><td>1</td><td>Tensas</td><td>0.00</td><td>0</td><td>2</td><td>2-B</td></tr></table>"""

SOC = """<p>Boys Soccer Power Ranking Report By Division</p><p>Division II</p>
<table><tr><td>1.</td><td>Holy Cross</td><td>11.65</td><td>18</td><td>3</td><td>2</td><td>7-II</td></tr></table>
<p>Schools not playing in the Playoff</p><p>There are no Schools excluded from the Playoff.</p><table></table>"""


def test_basketball_rows():
    rows, meta = parse_report(BB)
    assert meta["as_of"].startswith("11/25/2026")
    assert rows[0]["school"] == "Stanley" and rows[0]["power"] == 41.13
    assert rows[0]["wins"] == 5 and rows[0]["losses"] == 1 and rows[0]["ties"] is None
    assert rows[0]["group"] == "Class B" and not rows[0]["not_playing"]
    assert rows[1]["not_playing"]


def test_soccer_ties():
    rows, meta = parse_report(SOC)
    assert "Boys Soccer" in meta["title"]
    assert rows[0]["ties"] == 2 and rows[0]["district"] == "7-II"
    assert rows[0]["group"] == "Division II"
