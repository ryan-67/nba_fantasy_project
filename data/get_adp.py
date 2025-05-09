import pandas as pd

df = pd.read_csv("D:/DATA_ENGINEER/nba_fantasy_project/data/adp.csv")

# Replace all empty strings with NaN (null)
df.replace("", pd.NA, inplace=True)

# Save cleaned version
df.to_csv("D:/DATA_ENGINEER/nba_fantasy_project/data/adp_cleaned.csv", index=False)
