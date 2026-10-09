import gzip
import json
import os
import tempfile
import unittest

os.environ.setdefault(
    "DB_PATH", os.path.join(tempfile.gettempdir(), "lvay_route_tests.db")
)
import winter_archive as wa

ROW = (
    '<tr> <td width="5" height="28" ><font face="Arial" size="2"> {n}.</font></td>'
    ' <td width="15%" nowrap ><font Face="Arial" color= > {school}<font>&nbsp;</td>'
    ' <td><p align="center"><font size="1" face="Arial">{dc}</font></td>'
    ' <td><p align="center"><font size="1" face="Verdana">{date}</font><br>'
    ' <font size="1" face="Verdana">Tue&nbsp;</font>&nbsp;</td>'
    ' <td nowrap>{opp}&nbsp;</td>'
    ' <td align="center"><font size="1">{odc}</font>&nbsp;</td>'
    ' <td align="center">{kind}</td>'
    ' <td align="center" nowrap>{host}</td>'
    ' <td align="center" nowrap> {gn}</td>'
    ' <td align="center">{ha}</td>'
    ' <td align="center"><font size="1" face="Verdana">{wl}&nbsp;</font></td>'
    ' <td align="center" nowrap><font size="1" face="Verdana">{ot}</font></td>'
    ' <td align="center" nowrap><font size="1">{score}&nbsp;</font></td> </tr>'
)


def page(rows):
    body = "".join(ROW.format(**r) for r in rows)
    return (
        '<html><body><table><tr><td><table><tr><td>Date: x</td></tr></table>'
        '</td></tr><tr><td><table>'
        '<tr><td>#</td><td>School</td><td>District-Division</td><td>Date</td>'
        '<td>Opponent</td><td>Opponent District-Division</td><td>District or Tournament</td>'
        '<td>Tournament Host</td><td>Game#</td><td>Home/ Away</td><td>Win/Loss</td>'
        '<td>OT</td><td>Score</td></tr>' + body + '</table></td></tr></table></body></html>'
    )


def row(n, school, dc, date, opp, odc, kind, wl, score, ha="H", gn="1", host="", ot="No"):
    return dict(n=n, school=school, dc=dc, date=date, opp=opp, odc=odc, kind=kind,
                host=host, gn=gn, ha=ha, wl=wl, ot=ot, score=score)


class ParseTests(unittest.TestCase):
    def test_parses_lhsaa_rows(self):
        html = page([
            row(1, "L. W. Higgins", "9-5A", "11/18/2014 4:00:00 PM", "West Jefferson", "8-5A", "T", "L", "1-5"),
            row(2, "L. W. Higgins", "9-5A", "11/25/2014 11:00:00 PM", "Archbishop Shaw", "9-4A", "", "W", "3-2", ha="A"),
            row(3, "L. W. Higgins", "9-5A", "12/2/2014 6:00:00 PM", "John Ehret", "9-5A", "D", "T", "1-1", ot="Yes"),
        ])
        rows = wa.parse_report(html)
        self.assertEqual(len(rows), 3)
        first = rows[0]
        self.assertEqual(first["school"], "L. W. Higgins")
        self.assertEqual(first["district"], "9")
        self.assertEqual(first["class_"], "5A")
        self.assertEqual(first["game_date"], "11/18/2014")
        self.assertEqual(first["game_time"], "4:00 PM")
        self.assertEqual(first["opponent"], "West Jefferson")
        self.assertEqual(first["opp_class"], "5A")
        self.assertEqual(first["result"], "L")
        self.assertEqual(first["score"], "1-5")
        self.assertTrue(first["is_tournament"])
        self.assertFalse(first["is_district"])
        self.assertTrue(rows[2]["is_district"])
        self.assertTrue(rows[2]["overtime"])
        self.assertEqual(rows[1]["home_away"], "A")

    def test_unplayed_and_forfeit_results(self):
        self.assertEqual(wa.normalize_result("W(f)"), "W")
        self.assertEqual(wa.normalize_result("l (F)"), "L")
        self.assertEqual(wa.normalize_result(""), "")
        self.assertEqual(wa.normalize_result("T"), "T")


class BuildTests(unittest.TestCase):
    def test_build_season_records_and_opponent_records(self):
        rows = wa.parse_report(page([
            row(1, "Jesuit", "8-5A", "11/18/2014 4:00:00 PM", "Brother Martin", "8-5A", "D", "W", "2-1"),
            row(2, "Jesuit", "8-5A", "11/20/2014 4:00:00 PM", "Houston (TX)", "", "", "T", "0-0"),
            row(3, "Brother Martin", "8-5A", "11/18/2014 4:00:00 PM", "Jesuit", "8-5A", "D", "L", "1-2"),
            row(4, "Brother Martin", "8-5A", "11/18/2014 4:00:00 PM", "Jesuit", "8-5A", "D", "L", "1-2"),
            row(5, "Brother Martin", "8-5A", "12/30/2014 4:00:00 PM", "Rummel", "8-5A", "", "", ""),
        ]))
        season = wa.build_season("boys_soccer", "2015", rows)
        self.assertEqual(season["count"], 2)
        schools = {s["school"]: s for s in season["schools"]}
        self.assertEqual(schools["Jesuit"]["record"], "1-0-1")
        self.assertEqual(schools["Jesuit"]["games_played"], 2)
        # Exact duplicate rows from overlapping district reports collapse.
        self.assertEqual(len(schools["Brother Martin"]["games"]), 2)
        self.assertEqual(schools["Brother Martin"]["record"], "0-1")
        jesuit_vs_bm = schools["Jesuit"]["games"][0]
        self.assertEqual(jesuit_vs_bm["opp_record"], "0-1")
        self.assertEqual(jesuit_vs_bm["opp_wins"], 0)
        self.assertEqual(schools["Jesuit"]["games"][1]["opp_record"], "")
        self.assertEqual(season["schools"][0]["division"], "")

    def test_save_and_load_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.environ["WINTER_ARCHIVE_DIR"] = tmp
            try:
                wa.save_season("boys_soccer", "2015", {"count": 0, "schools": []})
                wa.save_season("boys_soccer", "2016", {"count": 0, "schools": []})
                data = wa.load_archive("boys_soccer")
                self.assertEqual(sorted(data["seasons"]), ["2015", "2016"])
                with gzip.open(os.path.join(tmp, "boys_soccer.json.gz"), "rt") as f:
                    self.assertIn("2016", json.load(f)["seasons"])
            finally:
                del os.environ["WINTER_ARCHIVE_DIR"]


class BasketballSourceTests(unittest.TestCase):
    def test_basketball_seasons_start_at_2014(self):
        self.assertEqual(wa.parse_seasons("2012-2027", 2027, "boys_basketball")[0], 2014)
        self.assertEqual(wa.parse_seasons("2012-2027", 2027, "boys_soccer")[0], 2015)
        self.assertEqual(wa.parse_seasons("2012-2027", 2027, "boys_basketball")[-1], 2026)

    def test_basketball_pulls_one_class_at_a_time(self):
        calls = []

        def fake_fetch(sport, season, district, session=None, attempts=3, classification=""):
            calls.append((district, classification))
            if classification == "1A":
                return page([row(1, "Arcadia", "1-1A", "11/20/2013 6:00:00 PM", "Homer", "1-2A", "T", "W", "59-58")])
            return page([])

        with tempfile.TemporaryDirectory() as tmp:
            old_fetch, old_dir = wa.fetch_district, os.environ.get("WINTER_ARCHIVE_DIR")
            wa.fetch_district = fake_fetch
            os.environ["WINTER_ARCHIVE_DIR"] = tmp
            try:
                data = wa.build_from_lhsaa("boys_basketball", 2014, pause=0)
            finally:
                wa.fetch_district = old_fetch
                if old_dir is None:
                    os.environ.pop("WINTER_ARCHIVE_DIR", None)
                else:
                    os.environ["WINTER_ARCHIVE_DIR"] = old_dir
        self.assertEqual(calls, [("", c) for c in wa.CLASS_ORDER])
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["schools"][0]["record"], "1-0")
        self.assertEqual(data["rows_by_district"]["1A"], 1)


if __name__ == "__main__":
    unittest.main()


class ServerArchiveRouteTests(unittest.TestCase):
    """The schedules feed serves archive seasons only when no live ratings exist."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "t.db")
        conn = __import__("sqlite3").connect(self.db_path)
        conn.executescript("""
            CREATE TABLE power_rankings (sport TEXT, season TEXT, school TEXT,
              division TEXT, track TEXT, class_ TEXT, district TEXT,
              power_rating REAL, wins INT, losses INT, ties INT,
              games_played INT, rank INT);
            CREATE TABLE game_power_points (sport TEXT, season TEXT, school TEXT,
              week INT, opponent TEXT, result TEXT, score TEXT, opp_wins INT,
              opp_losses INT, opp_ties INT, opp_division TEXT, base_pts REAL,
              div_bonus REAL, opp_quality REAL, total_pts REAL, is_district INT,
              game_date TEXT, home_away TEXT);
            CREATE TABLE season_schools (sport TEXT, season TEXT, school TEXT,
              source TEXT, status TEXT);
            CREATE TABLE season_registry (sport TEXT, season TEXT, source TEXT,
              status TEXT, is_locked INT);
            CREATE TABLE games (sport TEXT, season TEXT, school TEXT);
            INSERT INTO power_rankings VALUES ('boys_soccer','2026','Jesuit',
              'Division I','', '5A','8',17.8,16,0,1,17,1);
        """)
        conn.commit()
        conn.close()
        os.environ["WINTER_ARCHIVE_DIR"] = self.tmp.name
        rows = wa.parse_report(page([
            row(1, "Jesuit", "8-5A", "11/18/2014 4:00:00 PM", "Brother Martin", "8-5A", "D", "W", "2-1"),
            row(2, "Brother Martin", "8-5A", "11/18/2014 4:00:00 PM", "Jesuit", "8-5A", "D", "L", "1-2"),
        ]))
        wa.save_season("boys_soccer", "2015", wa.build_season("boys_soccer", "2015", rows))
        import server
        self.server = server
        self._old = server.DB_PATH
        server.DB_PATH = self.db_path
        self.client = server.app.test_client()

    def tearDown(self):
        self.server.DB_PATH = self._old
        del os.environ["WINTER_ARCHIVE_DIR"]
        self.tmp.cleanup()

    def test_archive_season_is_served(self):
        data = self.client.get("/api/schedules/winter/boys_soccer?season=2015").get_json()
        self.assertTrue(data["archive"])
        self.assertEqual(data["count"], 2)
        jesuit = [s for s in data["schools"] if s["school"] == "Jesuit"][0]
        self.assertEqual(jesuit["record"], "1-0")
        self.assertEqual(jesuit["games"][0]["opponent"], "Brother Martin")

    def test_summary_and_school_filter(self):
        data = self.client.get(
            "/api/schedules/winter/boys_soccer?season=2015&summary=1&school=Jesuit"
        ).get_json()
        self.assertEqual([s["school"] for s in data["schools"]], ["Jesuit"])
        self.assertEqual(data["schools"][0]["games"], [])

    def test_live_season_still_comes_from_ratings(self):
        data = self.client.get("/api/schedules/winter/boys_soccer?season=2026&summary=1").get_json()
        self.assertNotIn("archive", data)
        self.assertEqual(data["schools"][0]["division"], "Division I")

    def test_seasons_list_includes_archive(self):
        data = self.client.get("/api/seasons/boys_soccer").get_json()
        seasons = {s["season"]: s for s in data["seasons"]}
        self.assertIn("2015", seasons)
        self.assertTrue(seasons["2015"]["archive"])

    def test_build_rejects_current_season(self):
        resp = self.client.get("/api/archive/winter/boys_soccer/build?seasons=2030")
        self.assertEqual(resp.status_code, 400)
