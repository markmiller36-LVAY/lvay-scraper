"""
import_oos_2025.py

Inserts the 2025 football OOS opponent records into the oos_opponents table.
Run via: /api/import/oos2025
"""

import sqlite3

DB_PATH = "/data/lvay_v2.db"

OOS_GAMES = [
    # ── ORIGINAL OOS GAMES ──────────────────────────────────────────────────
    {'school': 'Ruston',            'week': 2, 'opponent': 'Cabot High School, AR',                    'div': 'NS1', 'cls': '5A', 'opp_wins': 4,  'opp_losses': 6},
    {'school': 'Ruston',            'week': 3, 'opponent': 'Longview High School, TX',                 'div': 'NS1', 'cls': '5A', 'opp_wins': 6,  'opp_losses': 4},
    {'school': 'Ruston',            'week': 4, 'opponent': 'Midland-Legacy High School, TX',           'div': 'NS1', 'cls': '5A', 'opp_wins': 5,  'opp_losses': 5},
    {'school': 'Ruston',            'week': 5, 'opponent': 'Stephenville High School, TX',             'div': 'NS1', 'cls': '5A', 'opp_wins': 10, 'opp_losses': 0},

    # Iowa WK2 — corrected opp record from 7-3 to 5-5
    {'school': 'Iowa',              'week': 2, 'opponent': 'Cypress-Mauriceville, TX',                 'div': 'NS1', 'cls': '5A', 'opp_wins': 5,  'opp_losses': 5},

    {'school': 'Edna Karr',         'week': 1, 'opponent': 'IMG Academy, FL',                          'div': 'S1',  'cls': '5A', 'opp_wins': 10, 'opp_losses': 0},
    {'school': 'Brother Martin',    'week': 1, 'opponent': 'Bishop Sullivan Catholic, VA',             'div': 'S1',  'cls': '5A', 'opp_wins': 8,  'opp_losses': 2},
    {'school': 'St. Thomas More',   'week': 2, 'opponent': 'Brentwood Academy, TN',                    'div': 'S1',  'cls': '4A', 'opp_wins': 9,  'opp_losses': 1},
    {'school': 'Catholic - B.R.',   'week': 1, 'opponent': 'Thompson High School, AL',                 'div': 'S1',  'cls': '5A', 'opp_wins': 10, 'opp_losses': 0},
    {'school': 'St. Augustine',     'week': 3, 'opponent': 'Trinity Christian, TX',                    'div': 'S2',  'cls': '3A', 'opp_wins': 7,  'opp_losses': 3},
    {'school': 'John Curtis Christian', 'week': 2, 'opponent': 'Chandler High School, AZ',            'div': 'NS1', 'cls': '5A', 'opp_wins': 9,  'opp_losses': 1},
    {'school': 'West Monroe',       'week': 1, 'opponent': 'Denton Ryan, TX',                          'div': 'NS1', 'cls': '5A', 'opp_wins': 10, 'opp_losses': 0},
    {'school': 'Hahnville',         'week': 3, 'opponent': 'George Ranch, TX',                         'div': 'NS1', 'cls': '5A', 'opp_wins': 8,  'opp_losses': 2},
    {'school': 'Destrehan',         'week': 1, 'opponent': 'Katy Jordan, TX',                          'div': 'NS1', 'cls': '5A', 'opp_wins': 6,  'opp_losses': 4},
    {'school': 'Evangel Christian', 'week': 5, 'opponent': 'Madison-Ridgeland Academy, MS',            'div': 'S1',  'cls': '3A', 'opp_wins': 9,  'opp_losses': 1},
    {'school': 'North Webster',     'week': 5, 'opponent': 'Garrison High School, TX',                 'div': 'NS4', 'cls': '1A', 'opp_wins': 7,  'opp_losses': 3},
    {'school': 'St. Charles',       'week': 7, 'opponent': 'Legacy the School of Sport Science, TX',   'div': 'S3',  'cls': '2A', 'opp_wins': 6,  'opp_losses': 4},
    {'school': 'Delhi',             'week': 9, 'opponent': 'Pensacola Catholic, FL',                   'div': 'NS2', 'cls': '4A', 'opp_wins': 9,  'opp_losses': 1},

    # St. Frederick WK8 — corrected to S3
    {'school': 'St. Frederick',     'week': 8, 'opponent': 'Cypress Christian School, TX',             'div': 'S3',  'cls': '2A', 'opp_wins': 9,  'opp_losses': 0},

    # Haynesville WK2
    {'school': 'Haynesville',       'week': 2, 'opponent': 'Harmony Grove High School, AR',            'div': 'NS3', 'cls': '2A', 'opp_wins': 3,  'opp_losses': 7},

    # St. Edmund WK2
    {'school': 'St. Edmund',        'week': 2, 'opponent': 'Muenster Sacred Heart, TX',                'div': 'NS4', 'cls': '1A', 'opp_wins': 2,  'opp_losses': 7},

    # ── NEW OOS GAMES DISCOVERED FROM BILL'S EXCEL ──────────────────────────
    {'school': 'DeRidder',          'week': 3, 'opponent': 'Newton High School, TX',                   'div': 'NS4', 'cls': '3A', 'opp_wins': 9,  'opp_losses': 1},
    {'school': 'Neville',           'week': 3, 'opponent': 'Oak Grove, MS',                            'div': 'NS1', 'cls': '5A', 'opp_wins': 8,  'opp_losses': 3},
    {'school': 'Northshore',        'week': 1, 'opponent': 'Picayune Memorial, MS',                    'div': 'NS1', 'cls': '4A', 'opp_wins': 8,  'opp_losses': 2},
    {'school': 'North DeSoto',      'week': 2, 'opponent': 'Center High School, TX',                   'div': 'NS2', 'cls': '4A', 'opp_wins': 5,  'opp_losses': 5},
    {'school': 'Ouachita Parish',   'week': 4, 'opponent': 'Port Gibson, MS',                          'div': 'NS3', 'cls': '2A', 'opp_wins': 4,  'opp_losses': 6},
    {'school': 'West Monroe',       'week': 2, 'opponent': 'Pulaski Academy, AR',                      'div': 'S1',  'cls': '2A', 'opp_wins': 3,  'opp_losses': 7},

    # ── 10 MISSING OOS GAMES FROM FOOTBALL RUN ──────────────────────────────
    {'school': 'Brother Martin',        'week': 3, 'opponent': 'New Hope High, MS',                        'div': 'NS3', 'cls': '3A', 'opp_wins': 7,  'opp_losses': 3},
    {'school': 'Catholic - B.R.',       'week': 5, 'opponent': 'Madison-Ridgeland Academy, MS',           'div': 'S1',  'cls': '5A', 'opp_wins': 9,  'opp_losses': 1},
    {'school': 'Central Private',       'week': 4, 'opponent': 'Centreville Academy, MS',                 'div': 'NS4', 'cls': '2A', 'opp_wins': 10, 'opp_losses': 1},
    {'school': "D'Arbonne Woods Charter", 'week': 2, 'opponent': 'Bearden High School, AR',              'div': 'NS4', 'cls': '2A', 'opp_wins': 6,  'opp_losses': 4},
    {'school': 'Edna Karr',             'week': 2, 'opponent': 'American Heritage, FL',                   'div': 'S1',  'cls': '4A', 'opp_wins': 4,  'opp_losses': 5},
    {'school': 'Jesuit',                'week': 1, 'opponent': 'Jesuit College Prep School of Dallas, TX','div': 'NS1', 'cls': '5A', 'opp_wins': 5,  'opp_losses': 5},
    {'school': 'John Curtis Christian', 'week': 1, 'opponent': 'Cathedral High, CA',                      'div': 'NS1', 'cls': '5A', 'opp_wins': 5,  'opp_losses': 5},
    {'school': 'Oak Grove',             'week': 2, 'opponent': 'Crossett High School, AR',                'div': 'NS3', 'cls': '3A', 'opp_wins': 2,  'opp_losses': 8},
    {'school': 'Pope John Paul II',     'week': 1, 'opponent': 'Pearl River Central, MS',                 'div': 'NS1', 'cls': '4A', 'opp_wins': 6,  'opp_losses': 3},
    {'school': 'St. Louis Catholic',    'week': 3, 'opponent': 'Orangefield High School, TX',             'div': 'NS1', 'cls': '4A', 'opp_wins': 8,  'opp_losses': 2},
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
        pass

    c.execute("DELETE FROM oos_opponents WHERE sport='football' AND season='2025'")

    for g in OOS_GAMES:
        c.execute("""
            INSERT INTO oos_opponents (
                sport, season, school, week, opponent, division, class_, opp_wins, opp_losses
            )
            VALUES ('football', '2025', ?, ?, ?, ?, ?, ?, ?)
        """, (
            g['school'],
            g['week'],
            g['opponent'],
            g['div'],
            g['cls'],
            g['opp_wins'],
            g['opp_losses'],
        ))

    conn.commit()
    print(f"Imported {len(OOS_GAMES)} OOS opponent records (2025 football)")
    conn.close()


if __name__ == "__main__":
    run()
