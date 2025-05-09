import psycopg2
import csv
import os
from datetime import datetime

# Database connection parameters
conn = psycopg2.connect(
    dbname='nbafantasy',
    user='postgres',
    password='pineapple100',
    host='localhost',
    port='5432'
)

conn.autocommit = True


cur = conn.cursor()

# Create the table if it doesn't exist
create_table_query = '''
CREATE TABLE IF NOT EXISTS nba_top20_gamelogs (
    rk INTEGER,
    gcar INTEGER,
    gtm INTEGER,
    date DATE,
    team TEXT,
    opp TEXT,
    result TEXT,
    gs TEXT,
    mp TEXT,
    fg REAL,
    fga REAL,
    fg_pct REAL,
    three_p REAL,
    three_pa REAL,
    three_pct REAL,
    two_p REAL,
    two_pa REAL,
    two_pct REAL,
    efg_pct REAL,
    ft REAL,
    fta REAL,
    ft_pct REAL,
    orb REAL,
    drb REAL,
    trb REAL,
    ast REAL,
    stl REAL,
    blk REAL,
    tov REAL,
    pf REAL,
    pts REAL,
    gm_sc REAL,
    "+/-" REAL
);
'''
cur.execute(create_table_query)
conn.commit()

# Utility: convert to float or None
def float_or_none(value):
    if value in ('', None):
        return None
    try:
        return float(value)
    except ValueError:
        return None

# Utility: convert to int or None
def int_or_none(value):
    if value in ('', None):
        return None
    try:
        return int(value)
    except ValueError:
        return None

# Utility: convert to date or None
def date_or_none(value):
    if value in ('', None):
        return None
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError:
        return None

# Load all gamelog CSVs in current directory
for filename in os.listdir('.'):
    if '_gamelog_2025.csv' in filename.lower():
        print(f'Processing file: {filename}')
        with open(filename, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Skip repeated header rows or footer totals
                if row['Rk'] == '' or row['Rk'] == 'Rk':
                    continue
                try:
                    cur.execute('''
                        INSERT INTO nba_top20_gamelogs (
                            rk, gcar, gtm, date, team, opp, result, gs, mp, fg, fga, fg_pct,
                            three_p, three_pa, three_pct, two_p, two_pa, two_pct, efg_pct, 
                            ft, fta, ft_pct, orb, drb, trb, ast, stl, blk, tov, pf, pts, gm_sc, "+/-"
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s
                        );
                    ''', (
                        int_or_none(row.get('Rk')),
                        int_or_none(row.get('Gcar')),
                        int_or_none(row.get('Gtm')),
                        date_or_none(row.get('Date')),
                        row.get('Team'),
                        row.get('Opp'),
                        row.get('Result'),
                        row.get('GS'),
                        row.get('MP'),
                        float_or_none(row.get('FG')),
                        float_or_none(row.get('FGA')),
                        float_or_none(row.get('FG%')),
                        float_or_none(row.get('3P')),
                        float_or_none(row.get('3PA')),
                        float_or_none(row.get('3P%')),
                        float_or_none(row.get('2P')),
                        float_or_none(row.get('2PA')),
                        float_or_none(row.get('2P%')),
                        float_or_none(row.get('eFG%')),
                        float_or_none(row.get('FT')),
                        float_or_none(row.get('FTA')),
                        float_or_none(row.get('FT%')),
                        float_or_none(row.get('ORB')),
                        float_or_none(row.get('DRB')),
                        float_or_none(row.get('TRB')),
                        float_or_none(row.get('AST')),
                        float_or_none(row.get('STL')),
                        float_or_none(row.get('BLK')),
                        float_or_none(row.get('TOV')),
                        float_or_none(row.get('PF')),
                        float_or_none(row.get('PTS')),
                        float_or_none(row.get('GmSc')),
                        float_or_none(row.get('+/-'))
                    ))
                except Exception as e:
                    print(f"Error inserting row from {filename}: {row}. Error: {e}")

conn.commit()
cur.close()
conn.close()
print("Done loading all gamelogs.")
