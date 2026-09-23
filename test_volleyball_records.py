import sqlite3
import unittest
from unittest.mock import patch

from scraper_volleyball import ensure_tables
from volleyball_records import repair_oos_eligibility, schedule_record
from run_power_rankings_volleyball import calculate_school_pr
import server

class VolleyballRecordTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        ensure_tables(self.conn)
        rows = [
            ('LA Rival', 'I', 'W', 1),
            ('Other LA', 'II', 'L', 1),
            ('Named School - TX - UIL', '0-', 'W', 1),
            ('OUT OF STATE', None, 'L', 1),
            ('Future LA', 'III', '', 1),
            ('Excluded match', 'I', 'W', 0),
        ]
        for i, (opponent, division, result, eligible) in enumerate(rows):
            self.conn.execute('INSERT INTO volleyball_games (sport,season,school,school_division,school_district,game_date,opponent,opp_division,result,counts_for_pr,match_num) VALUES (?,?,?,?,?,?,?,?,?,?,?)', ('volleyball','2026','Example','I',1,f'2026-09-{i+1:02}',opponent,division,result,eligible,1))
        repair_oos_eligibility(self.conn, '2026')

    def tearDown(self):
        self.conn.close()

    def test_excluded_matches_remain_in_overall_but_not_pr(self):
        games = [dict(r) for r in self.conn.execute('SELECT * FROM volleyball_games')]
        overall = schedule_record(games)
        self.assertEqual((overall['wins'], overall['losses'], overall['games_played']), (3,2,5))
        eligible = [g for g in games if g['counts_for_pr']]
        stats = calculate_school_pr('Example', eligible, {'LA Rival': 7, 'Other LA': 9})
        self.assertEqual(stats, {'wins':1,'losses':1,'games_played':2,'power_rating':7.5})
        self.assertEqual(len(games), 6)

    def test_schedule_endpoint_uses_overall_record(self):
        self.conn.execute("INSERT INTO volleyball_rankings (sport,season,school,division,wins,losses,games_played,power_rating) VALUES ('volleyball','2026','Example','Division I',1,1,2,7.5)")
        # Endpoint owns connection closing; inspect its response afterward.
        with patch.object(server, 'get_db', return_value=self.conn), patch.object(server, 'available_season', return_value='2026'):
            response = server.app.test_client().get('/api/schedules/volleyball?season=2026')
        self.assertEqual(response.status_code, 200)
        team = response.json['schools'][0]
        self.assertEqual(team['record'], '3-2')
        self.assertEqual(team['pr_record'], '1-1')
        self.assertEqual(team['games_played'], 5)
        self.assertEqual(team['pr_games_played'], 2)
        self.assertEqual(len(team['games']), 6)

    def test_repair_preserves_existing_exclusions(self):
        repair_oos_eligibility(self.conn, '2026')
        flags = {r['opponent']: r['counts_for_pr'] for r in self.conn.execute('SELECT * FROM volleyball_games')}
        self.assertEqual(flags['Excluded match'], 0)
        self.assertEqual(flags['Named School - TX - UIL'], 0)
        self.assertEqual(flags['LA Rival'], 1)

class ClearwaterRepairTests(unittest.TestCase):
    def test_only_confirmed_stale_clearwater_copy_is_removed(self):
        conn = sqlite3.connect(':memory:')
        ensure_tables(conn)
        for school in ('Mt. Carmel', 'Another School'):
            for match_num in (1, 3):
                conn.execute("INSERT INTO volleyball_games (sport,season,school,game_date,opponent,match_num,result,score) VALUES ('volleyball','2026',?,'9/5/2026','Clearwater Central Catholic - FL - FHSAA',?,'W','25-17, 25-15')", (school,match_num))
        repair_oos_eligibility(conn, '2026')
        self.assertEqual(conn.execute("SELECT match_num FROM volleyball_games WHERE school='Mt. Carmel'").fetchall(), [(3,)])
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM volleyball_games WHERE school='Another School'").fetchone()[0], 2)
        conn.close()

if __name__ == '__main__':
    unittest.main()
