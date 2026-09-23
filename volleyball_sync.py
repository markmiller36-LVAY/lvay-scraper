"""Validate and reconcile complete LHSAA volleyball schedule snapshots."""
import json
import re
from collections import defaultdict
from datetime import datetime


def normalize_result(value):
    value = str(value or '').strip().upper()
    if value in ('W', 'W(F)'):
        return 'W'
    if value in ('L', 'L(F)'):
        return 'L'
    if value in ('', 'CANCELLED', 'CANCELED', 'POSTPONED'):
        return ''
    raise ValueError(f'Unrecognized volleyball result: {value!r}')


def canonical_date(value):
    value = str(value or '')
    match = re.search(r'\d{1,2}/\d{1,2}/\d{4}', value)
    if match:
        return datetime.strptime(match[0], '%m/%d/%Y').strftime('%Y-%m-%d')
    return datetime.strptime(value[:10], '%Y-%m-%d').strftime('%Y-%m-%d')


def reconcile_snapshot(conn, snapshots, season):
    """Called inside the caller's transaction; retain removed rows for recovery.

    A game is identified by school, date, opponent and match number. Repeated
    report blocks collapse, but genuine numbered rematches remain distinct.
    Manual result overrides live separately and are never modified here.
    """
    if set(snapshots) != {'I', 'II', 'III', 'IV', 'V'} or not all(snapshots.values()):
        raise ValueError('A complete five-division snapshot is required')
    if conn.execute("SELECT 1 FROM sqlite_master WHERE name='season_registry'").fetchone():
        if conn.execute('SELECT 1 FROM season_registry WHERE sport=? AND season=? AND is_locked=1',
                        ('volleyball', str(season))).fetchone():
            raise ValueError('Cannot replace a locked volleyball season')
    # LHSAA can reuse match number 1 for two same-day matches at different
    # times. Keep both with deterministic internal numbers, never overwrite one.
    snapshots = {d: [dict(r) for r in rows] for d, rows in snapshots.items()}
    groups = defaultdict(list)
    for rows in snapshots.values():
        for row in rows:
            groups[(row['school'], canonical_date(row['date_raw']), row['opponent'],
                    int(row['match_num'] or 1))].append(row)
    for key, group in groups.items():
        outcomes = {(r['win_loss'], r['score']) for r in group}
        if len(outcomes) > 1:
            times = sorted({r['date_raw'] for r in group})
            if len(times) < len(outcomes):
                raise ValueError(f'Conflicting source results for {key}')
            for row in group:
                row['match_num'] = str(10000 + key[3] * 100 + times.index(row['date_raw']))
    incoming = {}
    schools = defaultdict(set)
    for division, rows in snapshots.items():
        for raw in rows:
            row = dict(raw)
            if row['division'] != division or not row['school'] or not row['opponent']:
                raise ValueError('Invalid volleyball source row')
            row['win_loss'] = normalize_result(row['win_loss'])
            day = canonical_date(row['date_raw'])
            if day[:4] != str(season):
                raise ValueError('Source date does not match volleyball season')
            number = int(row['match_num'] or 1)
            key = (row['school'], day, row['opponent'], number)
            if key in incoming and any(incoming[key][field] != row[field] for field in ('win_loss', 'score')):
                raise ValueError(f'Conflicting source rows for {key}')
            incoming[key] = row
            schools[row['school']].add(key)
    cursor = conn.execute('SELECT rowid AS archive_rowid, * FROM volleyball_games WHERE sport=? AND season=?',
                          ('volleyball', str(season)))
    columns = [c[0] for c in cursor.description]
    old = [dict(zip(columns, row)) for row in cursor.fetchall()]
    old_schools = defaultdict(set)
    for row in old:
        old_schools[row['school']].add((canonical_date(row['game_date']), row['opponent'], row['match_num']))
    for school, games in old_schools.items():
        if school not in schools or len(schools[school]) < len(games) * 0.5:
            raise ValueError(f'Incomplete snapshot for {school}; existing schedule retained')
    for school in old_schools:
        old_completed = sum(r['school'] == school and r['result'] in ('W', 'L') for r in old)
        new_completed = sum(r['school'] == school and r['win_loss'] in ('W', 'L') for r in incoming.values())
        if new_completed < old_completed * 0.75:
            raise ValueError(f'Incomplete completed results for {school}')
    conn.execute('CREATE TABLE IF NOT EXISTS volleyball_schedule_history ('
                 'id INTEGER PRIMARY KEY, season TEXT, archived_at TEXT DEFAULT CURRENT_TIMESTAMP, '
                 'reason TEXT, row_json TEXT)')
    exclusions = set()
    seen = set()
    for row in old:
        key = (row['school'], canonical_date(row['game_date']), row['opponent'], row['match_num'])
        if key in incoming and row['counts_for_pr'] == 0:
            exclusions.add(key)
        # ISO normalization also repairs old mixed-format date duplicates.
        if key not in incoming or row['game_date'] != key[1] or key in seen:
            conn.execute('INSERT INTO volleyball_schedule_history (season,reason,row_json) VALUES (?,?,?)',
                         (str(season), 'Absent or superseded in complete LHSAA snapshot', json.dumps(row)))
            conn.execute('DELETE FROM volleyball_games WHERE rowid=?', (row['archive_rowid'],))
        else:
            seen.add(key)
    return list(incoming.values()), exclusions

