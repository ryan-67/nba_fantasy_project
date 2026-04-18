# NBA Fantasy Project

An NBA fantasy analytics project combining Python data pipelines, SQL analysis, and interactive Power BI dashboards to surface player value insights for fantasy decision-making.

## Features

- **Player stats pipeline** — fetches per-game stats from the NBA API using `nba_api`
- **Game log analysis** — pulls and loads individual game logs for granular trend analysis
- **ADP integration** — cleans and merges average draft position data for draft value analysis
- **SQL analytics** — query layer for aggregations, rankings, and custom metrics
- **Power BI dashboard** — interactive visuals for player comparison and fantasy scoring trends

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Python |
| Data source | NBA API (`nba_api`) |
| Database | SQLite |
| Analytics | SQL |
| Visualization | Power BI |

## Setup

```bash
python -m venv venv
venv\Scripts\activate
pip install nba_api pandas sqlite3
```

## Scripts

| File | Purpose |
|---|---|
| `get_player_stats.py` | Fetch season-level player stats |
| `get_player_gamelog.py` | Fetch per-game logs for individual players |
| `get_adp.py` | Fetch average draft position data |
| `load_player_gamelog.py` | Load game logs into SQLite |

## Usage

```bash
# Fetch and load player stats
python get_player_stats.py

# Load game logs for target players
python load_player_gamelog.py

# Open dashboard
# Open nba_fantasy_dashboard.pbix in Power BI Desktop
```

## Project Structure

```
nba_fantasy_project/
├── SQL/
│   ├── queries.sql             # Analytics queries
│   └── query1.sql              # Table creation and loading
├── dashboard/
│   └── nba_fantasy_dashboard.pbix  # Power BI dashboard
├── data/
│   ├── nba_player_stats.csv    # Cached player stats
│   ├── adp.csv                 # Raw ADP data
│   └── adp_cleaned.csv         # Cleaned ADP
├── get_player_stats.py
├── get_player_gamelog.py
├── get_adp.py
└── load_player_gamelog.py
```
