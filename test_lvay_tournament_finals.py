import sqlite3
import unittest

import lvay_tournament_finals as ltf


def make_db():
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE games (
            id INTEGER PRIMARY KEY AUTOINCREMENT, sport TEXT NOT NULL, school TEXT,
            game_date TEXT, opponent TEXT, home_away TEXT, win_loss TEXT, score TEXT,
            week TEXT, district TEXT, class_ TEXT, district_class TEXT,
            opponent_class TEXT, tournament TEXT, tournament_host TEXT,
            out_of_state TEXT, location TEXT, season TEXT, scraped_at TEXT,
            source TEXT,
            UNIQUE(sport, school, game_date, opponent, season)
        )
    """)
    rows = [
        ("Bossier", "11/17/2026 5:30:00 PM", "West Monroe", "A", "L", "1-2", "1", "1-III", "2-I"),
        ("Bossier", "12/15/2026 6:00:00 PM", "Captain Shreve", "H", "", "", "1", "1-III", "1-I"),
        ("Captain Shreve", "11/17/2026 6:00:00 PM", "Loyola Prep", "H", "L", "1-2", "1", "1-I", "1-III"),
        ("Captain Shreve", "12/15/2026 6:00:00 PM", "Bossier", "A", "", "", "1", "1-I", "1-III"),
        ("Ruston", "11/21/2026 10:30:00 AM", "Bossier", "N", "", "", "2", "2-II", "1-III"),
    ]
    for school, date, opp, ha, wl, score, dist, dc, oc in rows:
        conn.execute(
            "INSERT INTO games (sport, school, game_date, opponent, home_away, win_loss, "
            "score, district, district_class, opponent_class, season, source) "
            "VALUES ('boys_soccer',?,?,?,?,?,?,?,?,?,'2027','LHSAA')",
            (school, date, opp, ha, wl, score, dist, dc, oc),
        )
    return conn


FINAL = {"date": "2026-11-19", "time": "18:20", "a": "Bossier", "b": "Captain Shreve",
         "sa": 4, "sb": 1, "tournament": "Flyer Invitational", "host": "Loyola College Prep"}


def results(conn, school):
    return conn.execute(
        "SELECT opponent, game_date, win_loss, score, source, tournament FROM games "
        "WHERE school=? ORDER BY id", (school,)).fetchall()


class TournamentFinalsTests(unittest.TestCase):
    def test_inserts_both_sides_when_lhsaa_has_no_game(self):
        conn = make_db()
        st = ltf.apply_tournament_finals(conn, "boys_soccer", "2027", [FINAL])
        self.assertEqual(st["inserted"], 2)
        b = [r for r in results(conn, "Bossier") if r[4] == ltf.SOURCE_INSERT]
        c = [r for r in results(conn, "Captain Shreve") if r[4] == ltf.SOURCE_INSERT]
        self.assertEqual(b[0][:4], ("Captain Shreve", "11/19/2026 6:20:00 PM", "W", "4-1"))
        self.assertEqual(c[0][2:4], ("L", "1-4"))
        self.assertEqual(b[0][5], "Flyer Invitational")

    def test_rerun_is_idempotent(self):
        conn = make_db()
        ltf.apply_tournament_finals(conn, "boys_soccer", "2027", [FINAL])
        ltf.apply_tournament_finals(conn, "boys_soccer", "2027", [FINAL])
        n = conn.execute("SELECT COUNT(*) FROM games WHERE source=?", (ltf.SOURCE_INSERT,)).fetchone()[0]
        self.assertEqual(n, 2)

    def test_lhsaa_score_wins(self):
        conn = make_db()
        for school, opp, wl, sc in (("Bossier", "Captain Shreve", "W", "3-1"), ("Captain Shreve", "Bossier", "L", "1-3")):
            conn.execute("INSERT INTO games (sport, school, game_date, opponent, win_loss, score, season, source) "
                         "VALUES ('boys_soccer',?, '11/19/2026 7:00:00 PM', ?, ?, ?, '2027', 'LHSAA')", (school, opp, wl, sc))
        st = ltf.apply_tournament_finals(conn, "boys_soccer", "2027", [FINAL])
        self.assertEqual((st["inserted"], st["filled"], st["lhsaa"]), (0, 0, 2))

    def test_fills_unscored_lhsaa_row_then_lhsaa_overrides(self):
        conn = make_db()
        fin = dict(FINAL, date="2026-12-15", time="18:00")
        st = ltf.apply_tournament_finals(conn, "boys_soccer", "2027", [fin])
        self.assertEqual(st["filled"], 2)
        row = conn.execute("SELECT win_loss, score FROM games WHERE school='Bossier' AND game_date LIKE '12/15/2026%'").fetchone()
        self.assertEqual(row, ("W", "4-1"))
        # LHSAA posts a different score into the same row (scraper upsert)
        conn.execute("UPDATE games SET win_loss='W', score='2-0' WHERE school='Bossier' AND game_date LIKE '12/15/2026%'")
        ltf.apply_tournament_finals(conn, "boys_soccer", "2027", [fin])
        row = conn.execute("SELECT win_loss, score, source FROM games WHERE school='Bossier' AND game_date LIKE '12/15/2026%'").fetchone()
        self.assertEqual(row, ("W", "2-0", "LHSAA"))

    def test_removed_final_is_undone(self):
        conn = make_db()
        ltf.apply_tournament_finals(conn, "boys_soccer", "2027", [FINAL, dict(FINAL, date="2026-12-15", time="18:00")])
        ltf.apply_tournament_finals(conn, "boys_soccer", "2027", [])
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM games WHERE source LIKE 'LVAY%'").fetchone()[0], 0)
        row = conn.execute("SELECT win_loss, score FROM games WHERE school='Bossier' AND game_date LIKE '12/15/2026%'").fetchone()
        self.assertEqual(row, ("", ""))

    def test_site_down_keeps_last_rows(self):
        conn = make_db()
        ltf.apply_tournament_finals(conn, "boys_soccer", "2027", [FINAL])
        orig = ltf.fetch_finals
        ltf.fetch_finals = lambda sport: None
        try:
            ltf.apply_tournament_finals(conn, "boys_soccer", "2027")
        finally:
            ltf.fetch_finals = orig
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM games WHERE source=?", (ltf.SOURCE_INSERT,)).fetchone()[0], 2)

    def test_out_of_state_side_skipped(self):
        conn = make_db()
        st = ltf.apply_tournament_finals(conn, "boys_soccer", "2027",
                                         [dict(FINAL, b="Some Texas School")])
        self.assertEqual(st["inserted"], 0)

    def test_tie(self):
        conn = make_db()
        ltf.apply_tournament_finals(conn, "boys_soccer", "2027", [dict(FINAL, sa=2, sb=2)])
        r = conn.execute("SELECT win_loss, score FROM games WHERE school='Bossier' AND source=?", (ltf.SOURCE_INSERT,)).fetchone()
        self.assertEqual(r, ("T", "2-2"))


if __name__ == "__main__":
    unittest.main()
