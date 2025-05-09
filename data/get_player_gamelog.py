import os
import csv
import psycopg2

gamelog_dir = r'D:\DATA_ENGINEER\nba_fantasy_project\data\gamelogs'

conn = psycopg2.connect(
    dbname='nbafantasy',
    user='postgres',
    password='pineapple100',
    host='localhost',
    port='5432'
)
cur = conn.cursor()

row_count = 0

def sanitize_row(row):
    mapping = {
        'FG%': 'FG_pct',
        '3P': 'tp',
        '3PA': 'tpa',
        '3P%': 'tp_pct',
        '2P': 'twop',
        '2PA': 'twopa',
        '2P%': 'twop_pct',
        'eFG%': 'efg_pct',
        'FT%': 'FT_pct',
        '+/-': 'plus_minus',
        'GmSc': 'gmsc',
        'FG': 'FG',
        'FGA': 'FGA',
        'FT': 'FT',
        'FTA': 'FTA',
        'ORB': 'ORB',
        'DRB': 'DRB',
        'TRB': 'TRB',
        'AST': 'AST',
        'STL': 'STL',
        'BLK': 'BLK',
        'TOV': 'TOV',
        'PF': 'PF',
        'PTS': 'PTS',
        'MP': 'MP',
        'GS': 'GS',
        'Result': 'Result',
        'Opp': 'Opp',
        'Team': 'Team',
        'Date': 'Date',
        'Gtm': 'Gtm',
        'Gcar': 'Gcar',
        'Rk': 'Rk'
    }
    
    cleaned = {}
    for k, v in row.items():
        if k.strip() == '':  # skip empty headers
            continue
        key = mapping.get(k, k)
        cleaned[key] = v
    return cleaned

for filename in os.listdir(gamelog_dir):
    if '_gamelog_2025.csv' in filename.lower():
        filepath = os.path.join(gamelog_dir, filename)
        print(f"Processing file: {filename}")

        with open(filepath, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row['Rk'] == '' or row['Rk'] == 'Rk':
                    continue

                try:
                    cleaned_row = sanitize_row(row)
                    cur.execute("""
                        INSERT INTO nba_top20_gamelogs (
                            rk, gcar, gtm, date, team, opp, result, gs, mp, fg, fga,
                            fg_pct, tp, tpa, tp_pct, twop, twopa, twop_pct,
                            efg_pct, ft, fta, ft_pct, orb, drb, trb,
                            ast, stl, blk, tov, pf, pts, gmsc, plus_minus
                        ) VALUES (
                            %(Rk)s, %(Gcar)s, %(Gtm)s, %(Date)s, %(Team)s, %(Opp)s, %(Result)s,
                            %(GS)s, %(MP)s, %(FG)s, %(FGA)s, %(FG_pct)s, %(tp)s, %(tpa)s, %(tp_pct)s,
                            %(twop)s, %(twopa)s, %(twop_pct)s, %(efg_pct)s, %(FT)s, %(FTA)s, %(FT_pct)s,
                            %(ORB)s, %(DRB)s, %(TRB)s, %(AST)s, %(STL)s, %(BLK)s,
                            %(TOV)s, %(PF)s, %(PTS)s, %(gmsc)s, %(plus_minus)s
                        );
                    """, cleaned_row)
                    row_count += 1
                except Exception as e:
                    print(f"Error inserting row from {filename}: {cleaned_row}. Error: {e}")

conn.commit()
print(f"Done. Total rows inserted: {row_count}")
cur.close()
conn.close()
