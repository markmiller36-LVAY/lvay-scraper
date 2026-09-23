import sqlite3
import pytest
from scraper_volleyball import ensure_tables, insert_games, parse_date
from volleyball_sync import normalize_result, reconcile_snapshot


def snapshot():
    return {d: [dict(school='School '+d, school_dd='1-'+d, division=d,
                    date_raw='9/1/2026Tue', opponent='Opponent', opp_dd='1-I',
                    match_num='1', home_away='H', dist_t='', tournament='',
                    win_loss='W(f)', score='25-0, 25-0, 25-0')]
            for d in ['I', 'II', 'III', 'IV', 'V']}


def database():
    conn = sqlite3.connect(':memory:')
    ensure_tables(conn)
    return conn


def sync(conn, source):
    with conn:
        rows, exclusions = reconcile_snapshot(conn, source, '2026')
        insert_games(conn, rows, '2026', commit=False)
    return exclusions


def test_forfeits_and_date_suffixes():
    assert normalize_result('W(f)') == 'W'
    assert normalize_result('L(f)') == 'L'
    assert normalize_result('Cancelled') == ''
    assert parse_date('9/12/2026Sat') == '2026-09-12'
    with pytest.raises(ValueError):
        normalize_result('Unknown completed result')


def test_renumbered_and_renamed_matches_are_archived_and_idempotent():
    c = database()
    source = snapshot()
    sync(c, source)
    source['I'][0]['match_num'] = '3'
    source['II'][0]['opponent'] = 'Corrected Opponent'
    sync(c, source)
    sync(c, source)
    assert c.execute('SELECT count(*) FROM volleyball_games').fetchone()[0] == 5
    assert c.execute('SELECT count(*) FROM volleyball_schedule_history').fetchone()[0] == 2
    assert c.execute("SELECT result FROM volleyball_games WHERE school='School I'").fetchone()[0] == 'W'


def test_real_same_day_rematches_survive_reused_source_number():
    c = database()
    source = snapshot()
    first = source['I'][0]
    first['date_raw'] = '9/1/2026 2:30:00 PMTue'
    source['I'].append(dict(first, date_raw='9/1/2026 6:30:00 PMTue', win_loss='L', score='0-25, 0-25'))
    sync(c, source)
    sync(c, source)
    assert c.execute("SELECT count(*) FROM volleyball_games WHERE school='School I'").fetchone()[0] == 2
    assert c.execute('SELECT count(*) FROM volleyball_schedule_history').fetchone()[0] == 0


def test_incomplete_conflicting_or_locked_snapshot_keeps_database():
    c = database()
    source = snapshot()
    sync(c, source)
    for bad in [dict(source, V=[]), dict(source, I=[dict(source['I'][0], school='Missing Original')])]:
        with pytest.raises(ValueError):
            sync(c, bad)
    conflicting = snapshot()
    conflicting['I'].append(dict(conflicting['I'][0], win_loss='L'))
    with pytest.raises(ValueError):
        sync(c, conflicting)
    c.execute('CREATE TABLE season_registry (sport TEXT,season TEXT,is_locked INTEGER)')
    c.execute("INSERT INTO season_registry VALUES ('volleyball','2026',1)")
    c.commit()
    with pytest.raises(ValueError):
        sync(c, source)
    assert c.execute('SELECT count(*) FROM volleyball_games').fetchone()[0] == 5


def test_failure_rolls_back_archived_removal():
    c = database()
    source = snapshot()
    sync(c, source)
    source['I'][0]['opponent'] = 'Changed'
    with pytest.raises(RuntimeError):
        with c:
            reconcile_snapshot(c, source, '2026')
            raise RuntimeError('Simulated insert failure')
    assert c.execute("SELECT opponent FROM volleyball_games WHERE school='School I'").fetchone()[0] == 'Opponent'
    assert c.execute('SELECT count(*) FROM volleyball_schedule_history').fetchone()[0] == 0
