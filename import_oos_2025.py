"""
import_oos_2025.py
Inserts the 2025 football OOS opponent records into the oos_opponents table.
Run via: /api/import/oos2025
"""

import sqlite3

DB_PATH = "/data/lvay_v2.db"

OOS_GAMES = [
    {'school': 'Ruston',            'week': 2, 'opponent': 'Cypress-Mauriceville, TX',                'div': 'NS1', 'cls': '5A', 'opp_wins': 7,  'opp_losses': 3},
    {'school': 'Ruston',            'week': 3, 'opponent': 'Whitehouse High School, TX',              'div': 'NS2', 'cls': '4A', 'opp_wins': 8,  'opp_losses': 2},
    {'school': 'Ruston',            'week': 5, 'opponent': 'Stephenville High School, TX',            'div': 'NS1', 'cls': '5A', 'opp_wins': 10, 'opp_losses': 0},
    {'school': 'Iowa',              'week': 2, 'opponent': 'Cypress-Mauriceville, TX',                'div': 'NS1', 'cls': '5A', 'opp_wins': 7,  'opp_losses': 3},
    {'school': 'Edna Karr',         'week': 1, 'opponent': 'IMG Academy, FL',                        'div': 'S1',  'cls': '5A', 'opp_wins': 10, 'opp_losses': 0},
    {'school': 'Northshore',        'week': 2, 'opponent': 'Katy High School, TX',                   'div': 'NS1', 'cls': '5A', 'opp_wins': 9,  'opp_losses': 1},
    {'school': 'Brother Martin',    'week': 1, 'opponent': 'Bishop Sullivan Catholic, VA',            'div': 'S1',  'cls': '5A', 'opp_wins': 8,  'opp_losses': 2},
    {'school': 'St. Thomas More',   'week': 2, 'opponent': 'Brentwood Academy, TN',                  'div': 'S1',  'cls': '4A', 'opp_wins': 9,  'opp_losses': 1},
    {'school': 'Catholic BR',       'week': 1, 'opponent': 'Thompson High School, AL',               'div': 'S1',  'cls': '5A', 'opp_wins': 10, 'opp_losses': 0},
    {'school': 'St. Augustine',     'week': 3, 'opponent': 'Trinity Christian, TX',                  'div': 'S2',  'cls': '3A', 'opp_wins': 7,  'opp_losses': 3},
    {'school': 'John Curtis',       'week': 2, 'opponent': 'Chandler High School, AZ',               'div': 'NS1', 'cls': '5A', 'opp_wins': 9,  'opp_losses': 1},
    {'school': 'West Monroe',       'week': 1, 'opponent': 'Denton Ryan, TX',                        'div': 'NS1', 'cls': '5A', 'opp_wins': 10, 'opp_losses': 0},
    {'school': 'Hahnville',         'week': 3, 'opponent': 'George Ranch, TX',                       'div': 'NS1', 'cls': '5A', 'opp_wins': 8,  'opp_losses': 2},
    {'school': 'Destrehan',         'week': 1, 'opponent': 'Katy Jordan, TX',                        'div': 'NS1', 'cls': '5A', 'opp_wins': 6,  'opp_losses': 4},
    {'school': 'Evangel Christian', 'week': 5, 'opponent': 'Madison-Ridgeland Academy, MS',          'div': 'S1',  'cls': '3A', 'opp_wins': 9,  'opp_losses': 1},
    {'school': 'North Webster',     'week': 5, 'opponent': 'Garrison High School, TX',               'div': 'NS4', 'cls': '1A', 'opp_wins': 7,  'opp_losses': 3},
    {'school': 'St. Charles',       'week': 7, 'opponent': 'Legacy the School of Sport Science, TX', 'div': 'S3',  'cls': '2A', 'opp_wins': 6,  'opp_losses': 4},
    {'school': 'Delhi',             'week': 9, 'opponent': 'Pensacola Catholic, FL',                 'div': 'NS2', 'cls': '4A', 'opp_wins': 9,  'opp_losses': 1},
    # St. Frederick WK8 — Cypress Christian TX — CORRECTED to S3 (was wrongly NS4)
    {'school': 'St. Frederick',     'week': 8, 'opponent': 'Cypress Christian School, TX',           'div': 'S3',  'cls': '2A', 'opp_wins': 9,  'opp_losses': 0},
    # Haynesville WK2 — Harmony Grove High School AR (NS3, 2A) — 3W 7L
    {'school': 'Haynesville',       'week': 2, 'opponent': 'Harmony Grove High School, AR',          'div': 'NS3', 'cls': '2A', 'opp_wins': 3,  'opp_losses': 7},
    # St. Edmund WK2 — Muenster Sacred Heart TX (NS4, 1A) — 2W 7L
    {'school': 'St. Edmund',        'week': 2, 'opponent': 'Muenster Sacred Heart, TX',              'div': 'NS4', 'cls': '1A', 'opp_wins': 2,  'opp_losses': 7},
]


def run():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS oos_opponents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sport TEXT NOT NULL DEFAULT 'football',
            season TEXT NOT NULL DEFAULT '2025',
            school TEXT NOT NULL,
            week INTEGER NOT NULL,
            opponent TEXT NOT NULL,
            division TEXT NOT NULL,
            class_ TEXT DEFAULT '',
            opp_wins INTEGER DEFAULT 0,
            opp_losses INTEGER DEFAULT 0
        )
    """)

    try:
        c.execute("ALTER TABLE oos_opponents ADD COLUMN class_ TEXT DEFAULT ''")
    except Exception:
        pass  # column already exists

    # Wipe and re-insert clean
    c.execute("DELETE FROM oos_opponents WHERE sport='football' AND season='2025'")
    for g in OOS_GAMES:
        c.execute("""
            INSERT INTO oos_opponents (sport, season, school, week, opponent, division, class_, opp_wins, opp_losses)
            VALUES ('football','2025',?,?,?,?,?,?,?)
        """, (g['school'], g['week'], g['opponent'], g['div'], g['cls'], g['opp_wins'], g['opp_losses']))

    conn.commit()
    print(f"Imported {len(OOS_GAMES)} OOS opponent records (2025 football)")
    conn.close()


if __name__ == "__main__":
    run()
