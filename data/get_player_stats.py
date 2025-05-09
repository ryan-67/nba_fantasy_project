import pandas as pd
import numpy as np

# Load the CSV file
df = pd.read_csv('nba_player_stats.csv')

# Print the columns in the CSV to verify they match
print(f"Columns in CSV: {df.columns.tolist()}")

# Clean NaN values and handle percentages (remove % and convert to float)
df = df.applymap(lambda x: None if isinstance(x, float) and np.isnan(x) else x)

# Handle percentage columns by removing '%' and converting to float (e.g., 'FG%' to 'FG_pct')
percentage_columns = ['FG%', '3P%', '2P%', 'eFG%', 'FT%', '3P%', '2P%']
for col in percentage_columns:
    if col in df.columns:
        df[col] = df[col].replace({'%': ''}, regex=True).astype(float) / 100  # Remove '%' and convert to float

# Group by player to select one row per player, prioritizing '2TM' team players
df = df.groupby('Player').apply(lambda x: x[x['Team'] == '2TM'] if '2TM' in x['Team'].values else x.head(1))

# Remove any duplicate rows
df = df.drop_duplicates()

# Display cleaned DataFrame
print("Data after removing duplicates:")
print(df.head())

# Example of the first row
first_row = df.iloc[0]
print(f"First row data: {tuple(first_row)}")
