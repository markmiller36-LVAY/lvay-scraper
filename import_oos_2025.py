"""
Import 2025 OOS opponent records into DB.
Run once via /api/import/oos2025
"""
import sqlite3
import os


DB_PATH = os.environ.get("DB_PATH", "/data/lvay_v2.db")

# Division label to full name mapping
DIV_MAP = {
    'NS1': 'Non-Select Division I',
    'NS2': 'Non-Select Division II',
    'NS3': 'Non-Select Division III',
    'NS4': 'Non-Select Division IV',
    'S1':  'Select Division I',
    'S2':  'Select Division II',
    'S3':  'Select Division III',
    'S4':  'Select Division IV',
}

OOS_DATA = [
    {'school': 'Jesuit', 'week': 1, 'opponent': 'Jesuit College Prep School of Dallas, TX', 'div': 'S1', 'opp_wins': 5, 'opp_losses': 5},
    {'school': 'John Curtis Christian', 'week': 1, 'opponent': 'Cathedral High, CA', 'div': 'NS2', 'opp_wins': 5, 'opp_losses': 5},
    {'school': 'Northshore', 'week': 1, 'opponent': 'Picayune Memorial, MS', 'div': 'NS1', 'opp_wins': 8, 'opp_losses': 2},
    {'school': 'Pope John Paul II', 'week': 1, 'opponent': 'Pearl River Central, MS', 'div': 'NS1', 'opp_wins': 6, 'opp_losses': 3},
    {'school': "D'Arbonne Woods Charter", 'week': 2, 'opponent': 'Bearden High School, AR', 'div': 'NS4', 'opp_wins': 6, 'opp_losses': 4},
    {'school': 'Edna Karr', 'week': 2, 'opponent': 'American Heritage, FL', 'div': 'NS1', 'opp_wins': 4, 'opp_losses': 5},
    {'school': 'Haynesville', 'week': 2, 'opponent': 'Harmony Grove High School, AR', 'div': 'NS3', 'opp_wins': 3, 'opp_losses': 7},
    {'school': 'Iowa', 'week': 2, 'opponent': 'Cypress-Mauriceville, TX', 'div': 'NS1', 'opp_wins': 5, 'opp_losses': 5},
    {'school': 'North DeSoto', 'week': 2, 'opponent': 'Center High School, TX', 'div': 'NS2', 'opp_wins': 5, 'opp_losses': 5},
    {'school': 'Oak Grove', 'week': 2, 'opponent': 'Crossett High School, AR', 'div': 'NS3', 'opp_wins': 2, 'opp_losses': 8},
    {'school': 'Ruston', 'week': 2, 'opponent': 'Cabot High School, AR', 'div': 'NS1', 'opp_wins': 4, 'opp_losses': 6},
    {'school': 'St. Edmund', 'week': 2, 'opponent': 'Muenster Sacred Heart, TX', 'div': 'NS4', 'opp_wins': 2, 'opp_losses': 7},
    {'school': 'West Monroe', 'week': 2, 'opponent': 'Pulaski Academy, AR', 'div': 'S1', 'opp_wins': 3, 'opp_losses': 7},
    {'school': 'Brother Martin', 'week': 3, 'opponent': 'New Hope High, MS', 'div': 'NS2', 'opp_wins': 7, 'opp_losses': 3},
    {'school': 'DeRidder', 'week': 3, 'opponent': 'Newton, TX', 'div': 'NS4', 'opp_wins': 9, 'opp_losses': 1},
    {'school': 'Neville', 'week': 3, 'opponent': 'Oak Grove, MS', 'div': 'NS1', 'opp_wins': 8, 'opp_losses': 3},
    {'school': 'Ruston', 'week': 3, 'opponent': 'Longview High School, TX', 'div': 'NS1', 'opp_wins': 6, 'opp_losses': 4},
    {'school': 'St. Augustine', 'week': 3, 'opponent': 'Legacy the School of Sport Science, TX', 'div': 'S3', 'opp_wins': 6, 'opp_losses': 4},
    {'school': 'St. Louis Catholic', 'week': 3, 'opponent': 'Orangefield High School, TX', 'div': 'NS2', 'opp_wins': 8, 'opp_losses': 2},
    {'school': 'Central Private', 'week': 4, 'opponent': 'Centreville Academy, MS', 'div': 'S3', 'opp_wins': 10, 'opp_losses': 1},
    {'school': 'Ouachita Parish', 'week': 4, 'opponent': 'Port Gibson, MS', 'div': 'NS3', 'opp_wins': 4, 'opp_losses': 6},
    {'school': 'Ruston', 'week': 4, 'opponent': 'Midland-Legacy High School, TX', 'div': 'NS1', 'opp_wins': 5, 'opp_losses': 5},
    {'school': 'Catholic - B.R.', 'week': 5, 'opponent': 'Madison-Ridgeland Academy, MS', 'div': 'S1', 'opp_wins': 9, 'opp_losses': 1},
    {'school': 'North Webster', 'week': 5, 'opponent': 'Garrison High School, TX', 'div': 'NS4', 'opp_wins': 7, 'opp_losses': 3},
    {'school': 'Ruston', 'week': 5, 'opponent': 'Stephenville High School, TX', 'div': 'NS1', 'opp_wins': 10, 'opp_losses': 0},
    {'school': 'St. Charles', 'week': 7, 'opponent': 'Legacy the School of Sport Science, TX', 'div': 'S3', 'opp_wins': 6, 'opp_losses': 4},
    {'school': 'Delhi', 'week': 9, 'opponent': 'Pensacola Catholic, FL', 'div': 'NS2', 'opp_wins': 9, 'opp_losses': 1},
]

def run():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Create OOS table
    c.execute("""
        CREATE TABLE IF NOT EXISTS oos_opponents (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sport        TEXT,
            season       TEXT,
            school       TEXT,
            week         INTEGER,
            opponent     TEXT,
            division     TEXT,
            opp_wins     INTEGER,
            opp_losses   INTEGER,
            UNIQUE(sport, season, school, week)
        )
    """)

    inserted = 0
    for r in OOS_DATA:
        division = DIV_MAP.get(r['div'], r['div'])
        c.execute("""
            INSERT OR REPLACE INTO oos_opponents
            (sport, season, school, week, opponent, division, opp_wins, opp_losses)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, ('football', '2025', r['school'], r['week'],
              r['opponent'], division, r['opp_wins'], r['opp_losses']))
        inserted += 1

    conn.commit()
    conn.close()
    print(f"Imported {inserted} OOS opponent records")
    return inserted

if __name__ == "__main__":
    run()
