import os
import pandas as pd
import psycopg2

# Database connection details
db_connection = psycopg2.connect(
    host="localhost",
    dbname="nbafantasy",
    user="postgres",
    password="pineapple100"
)

# Directory where the CSV files are located
folder_path = "D:/DATA_ENGINEER/nba_fantasy_project/data/gamelogs"

# Create table (once)
cursor = db_connection.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS nba_player_game_logs (
        game_id SERIAL PRIMARY KEY,
        player_name VARCHAR(100),
        game_date DATE,
        team VARCHAR(50),
        opponent VARCHAR(50),
        points INTEGER,
        assists INTEGER,
        steals INTEGER,
        blocks INTEGER,
        turnovers INTEGER,
        field_goals_made INTEGER,
        field_goals_attempted INTEGER,
        free_throws_made INTEGER,
        free_throws_attempted INTEGER,
        three_pointers_made INTEGER,
        rebounds INTEGER
    );
""")
db_connection.commit()
cursor.close()

# Function to load CSV files into the unified table
def load_csv_to_db(csv_file, player_name):
    df = pd.read_csv(csv_file)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=['Date', 'Team', 'Opp'])

    df.fillna({
        'PTS': 0, 'AST': 0, 'STL': 0, 'BLK': 0, 'TOV': 0,
        'FG': 0, 'FGA': 0, 'FT': 0, 'FTA': 0, '3P': 0, 'TRB': 0
    }, inplace=True)

    cursor = db_connection.cursor()

    insert_query = """
        INSERT INTO nba_player_game_logs (
            player_name, game_date, team, opponent, points, assists, steals, blocks,
            turnovers, field_goals_made, field_goals_attempted,
            free_throws_made, free_throws_attempted,
            three_pointers_made, rebounds
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            player_name, row['Date'], row['Team'], row['Opp'], row['PTS'], row['AST'], row['STL'],
            row['BLK'], row['TOV'], row['FG'], row['FGA'], row['FT'], row['FTA'], row['3P'], row['TRB']
        ))

    db_connection.commit()
    cursor.close()

# Loop through all CSVs
for file_name in os.listdir(folder_path):
    if file_name.endswith(".csv"):
        player_name = file_name.split("_")[0]
        file_path = os.path.join(folder_path, file_name)

        print(f"Loading data for {player_name}...")
        load_csv_to_db(file_path, player_name)
        print(f"Finished loading {player_name} data.")

db_connection.close()
