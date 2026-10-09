import os
import tempfile
import unittest

os.environ.setdefault("DB_PATH", os.path.join(tempfile.gettempdir(), "lvay_route_tests.db"))
import bracket_archive as ba

P = "ctl00_ContentPlaceHolder1_"


def slot(g, a, sa, b, sb, a_home=True):
    star = "*" if a_home else ""
    def sc(v):
        return "" if v is None else str(v)
    return (f'<tr><td><span id="{P}T1G{g}S">1</span></td><td><span id="{P}Team1Game{g}"><a>{a}</a>{star}</span>&nbsp;</td>'
            f'<td><strong><span id="{P}T1G{g}R">{sc(sa)}</span>&nbsp;</strong></td></tr>'
            f'<tr><td><span id="{P}T2G{g}S">8</span></td><td><span id="{P}Team2Game{g}"><a>{b}</a></span>&nbsp;</td>'
            f'<td><strong><span id="{P}T2G{g}R">{sc(sb)}</span>&nbsp;</strong></td></tr>')


def page(slots):
    return "<html><body>2020 LHSAA Boys' Basketball Playoff Bracket<table>" + "".join(slots) + "</table></body></html>"


# 8-team bracket: games 1-4 first round (one bye), 5-6 semis, 7 final.
EIGHT = page([
    slot(1, "Alpha", None, "Bye", None),
    slot(2, "Bravo", 50, "Charlie", 40),
    slot(3, "Delta", 61, "Echo", 70),
    slot(4, "Foxtrot", 55, "Golf", 44),
    slot(5, "Alpha", 60, "Bravo", 58),
    slot(6, "Echo", 49, "Foxtrot", 52, a_home=False),
    slot(7, "Alpha", 66, "Foxtrot", 61),
])


class BracketTests(unittest.TestCase):
    def test_rounds_from_bracket_shape(self):
        games = ba.bracket_games(ba.parse_bracket(EIGHT))
        by = {(g["a"], g["b"]): g for g in games}
        self.assertEqual(len(games), 6)  # the bye is not a game
        self.assertEqual(by[("Bravo", "Charlie")]["phase"], "Quarterfinals")
        self.assertEqual(by[("Alpha", "Bravo")]["phase"], "Semifinals")
        self.assertEqual(by[("Alpha", "Foxtrot")]["phase"], "State Championship")
        self.assertEqual(by[("Alpha", "Foxtrot")]["winner"], "Alpha")

    def test_school_records_match_site_shape(self):
        games = ba.bracket_games(ba.parse_bracket(EIGHT))
        schools = {s["school"]: s for s in ba.season_schools({"Select Division I": games})}
        alpha = schools["Alpha"]
        self.assertEqual(alpha["bracket"], "Select Division I")
        self.assertEqual([g["phase"] for g in alpha["games"]], ["Semifinals", "State Championship"])
        self.assertEqual(alpha["games"][1]["score"], "66-61")
        self.assertEqual(alpha["games"][1]["result"], "W")
        self.assertEqual(schools["Foxtrot"]["games"][-1]["result"], "L")
        self.assertEqual(schools["Foxtrot"]["games"][-1]["score"], "61-66")
        self.assertEqual(schools["Echo"]["games"][1]["home_away"], "")
        self.assertEqual(schools["Bravo"]["games"][1]["home_away"], "A")

    def test_32_team_names(self):
        self.assertEqual(ba.round_name(1, 5), "Bi-District")
        self.assertEqual(ba.round_name(2, 5), "Regional")
        self.assertEqual(ba.round_name(3, 5), "Quarterfinals")
        self.assertEqual(ba.round_name(5, 5), "State Championship")

    def test_pages_by_era(self):
        labels = lambda y, kind="class": [p[0] for p in ba.bracket_pages(y, kind)]
        self.assertEqual(labels(2014), ["5A", "4A", "3A", "2A", "1A", "Class B", "Class C"])
        self.assertIn("Select Division IV", labels(2020))
        self.assertIn("5A", labels(2020))
        self.assertEqual(labels(2024)[0], "Non-Select Division I")
        self.assertNotIn("5A", labels(2024))
        self.assertIn("Non-Select Division V", labels(2023))
        self.assertIn("Select Division V", labels(2024))
        self.assertEqual(labels(2020, "division"), ["Division I", "Division II", "Division III", "Division IV", "Division V"])
        self.assertEqual(ba.parse_seasons("2012-2027", 2027), list(range(2013, 2027)))

    def test_heading_guard(self):
        self.assertTrue(ba.heading_matches(("div", "V", "Non-Select"), "Division V (Non-Select)"))
        self.assertFalse(ba.heading_matches(("div", "V", "Select"), "Division V (Non-Select)"))
        self.assertFalse(ba.heading_matches(("div", "I", None), "Division II"))
        self.assertTrue(ba.heading_matches(("cls", "5A", None), "Class 5A"))
        self.assertTrue(ba.heading_matches(("div", "II", None), "DIVISION II"))
        self.assertFalse(ba.heading_matches(("div", "I", None), "DIVISION II"))
        self.assertTrue(ba.heading_matches(("div", "V", "Select"), "DIVISION V (SELECT)"))
        self.assertTrue(ba.heading_matches(("div", "IV", None), ""))
        html = "<td>2019 LHSAA Baseball Playoff Bracket - Division IV (Select)</td><td>BI-DISTRICT - 5/1</td>"
        self.assertEqual(ba.page_heading(html), "Division IV (Select)")

    def test_build_season_and_route(self):
        with tempfile.TemporaryDirectory() as tmp:
            old = os.environ.get("WINTER_ARCHIVE_DIR")
            os.environ["WINTER_ARCHIVE_DIR"] = tmp
            try:
                fetched = []
                def fake(path):
                    fetched.append(path)
                    return EIGHT if "d=5A" in path else "<html>not available</html>"
                data = ba.build_season("boys_basketball", 2014, pause=0, fetcher=fake)
                self.assertEqual(data["brackets"], ["5A"])
                self.assertEqual(data["champions"], {"5A": "Alpha"})
                self.assertTrue(all("s=2&" in p for p in fetched))
                import server
                client = server.app.test_client()
                out = client.get("/api/brackets/boys_basketball?season=2014").get_json()
                self.assertEqual(len(out["schools"]), 7)
                self.assertEqual(client.get("/api/brackets/boys_basketball?season=2019").status_code, 404)
                self.assertEqual(client.get("/api/brackets/boys_basketball/build?seasons=2030").status_code, 400)
            finally:
                if old is None:
                    os.environ.pop("WINTER_ARCHIVE_DIR", None)
                else:
                    os.environ["WINTER_ARCHIVE_DIR"] = old


if __name__ == "__main__":
    unittest.main()
