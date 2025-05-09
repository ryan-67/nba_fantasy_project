-- Drop the table if it already exists
DROP TABLE IF EXISTS nba_player_stats;

-- Create the table
CREATE TABLE nba_player_stats (
    rk INTEGER,
    player TEXT,
    age INTEGER,
    team TEXT,
    pos TEXT,
    g INTEGER,
    gs INTEGER,
    mp REAL,
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
    awards TEXT,
    player_additional TEXT
);

-- Load the CSV into the table
COPY nba_player_stats
FROM 'D:/DATA_ENGINEER/nba_fantasy_project/data/nba_player_stats.csv'
DELIMITER ',' 
CSV HEADER;

-- Create cleaned version with only one row per player
CREATE OR REPLACE VIEW nba_player_stats_clean AS
SELECT DISTINCT ON (player)
    *
FROM nba_player_stats
ORDER BY player, 
         CASE WHEN team = 'TOT' THEN 0 ELSE 1 END;

-- Update the type of 'rk' column if necessary
ALTER TABLE nba_player_stats
ALTER COLUMN rk TYPE BIGINT;

-- Add the fantasy_score column
ALTER TABLE nba_player_stats
ADD COLUMN fantasy_score REAL;

-- Update the fantasy_score column with the calculated values
UPDATE nba_player_stats
SET fantasy_score = 
    (COALESCE(fg, 0) * 2) +
    (COALESCE(fga, 0) * -1) +
    (COALESCE(ft, 0) * 1) +
    (COALESCE(fta, 0) * -1) +
    (COALESCE(three_p, 0) * 1) +
    (COALESCE(trb, 0) * 1) +
    (COALESCE(ast, 0) * 2) +
    (COALESCE(stl, 0) * 4) +
    (COALESCE(blk, 0) * 4) +
    (COALESCE(tov, 0) * -2) +
    (COALESCE(pts, 0) * 1);

-- Get the top 20 players by fantasy score using the cleaned view
WITH aggregated_player_stats AS (
    SELECT player,
           ROUND(AVG(fantasy_score)::numeric, 1) AS avg_fantasy_score
    FROM nba_player_stats_clean
    GROUP BY player
)
SELECT player, avg_fantasy_score
FROM aggregated_player_stats
ORDER BY avg_fantasy_score DESC
LIMIT 20;

-- Drop and recreate the game logs table
DROP TABLE IF EXISTS nba_player_game_logs;

CREATE TABLE nba_player_game_logs (
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

-- Get players' fantasy consistency (standard deviation)
SELECT
  player_name,
  ROUND(AVG(fantasy_score), 2) AS season_avg_fp,
  ROUND(STDDEV(fantasy_score), 2) AS fantasy_stddev,
  ROUND(MAX(fantasy_score), 2) AS best_fp,
  ROUND(MIN(fantasy_score), 2) AS worst_fp,
  COUNT(*) AS games_played
FROM (
  SELECT
    player_name,
    (
      2 * field_goals_made - 1 * field_goals_attempted +
      1 * free_throws_made - 1 * free_throws_attempted +
      1 * three_pointers_made +
      1 * rebounds +
      2 * assists +
      4 * steals +
      4 * blocks -
      2 * turnovers +
      1 * points
    ) AS fantasy_score
  FROM nba_player_game_logs
) AS scored_games
GROUP BY player_name
HAVING COUNT(*) > 5
ORDER BY fantasy_stddev ASC;

-- Get Top 10 fantasy performances from 2024-25 season
SELECT
  player_name,
  game_date,
  team,
  opponent,
  (
    2 * field_goals_made - field_goals_attempted +
    free_throws_made - free_throws_attempted +
    three_pointers_made +
    rebounds +
    2 * assists +
    4 * steals +
    4 * blocks -
    2 * turnovers +
    points
  ) AS fantasy_score
FROM nba_player_game_logs
ORDER BY fantasy_score DESC
LIMIT 10;

-- Compare back-to-backs vs. regular rest
WITH game_with_prev AS (
    SELECT
        player_name,
        game_date,
        field_goals_made,
        field_goals_attempted,
        free_throws_made,
        free_throws_attempted,
        three_pointers_made,
        rebounds,
        assists,
        steals,
        blocks,
        turnovers,
        points,
        LAG(game_date) OVER (PARTITION BY player_name ORDER BY game_date) AS prev_game
    FROM nba_player_game_logs
),
labeled_games AS (
    SELECT *,
        CASE 
            WHEN game_date - prev_game = 1 THEN 'Back-to-Back'
            ELSE 'Regular Rest'
        END AS rest_type,
        (
            2 * field_goals_made -
            field_goals_attempted +
            free_throws_made -
            free_throws_attempted +
            three_pointers_made +
            rebounds +
            2 * assists +
            4 * steals +
            4 * blocks -
            2 * turnovers +
            points
        ) AS fantasy_points
    FROM game_with_prev
)
SELECT 
    player_name,
    rest_type,
    COUNT(*) AS games_played,
    ROUND(AVG(fantasy_points), 2) AS avg_fantasy_points,
    ROUND(STDDEV(fantasy_points), 2) AS stddev_fantasy_points
FROM labeled_games
GROUP BY player_name, rest_type
ORDER BY player_name, rest_type;

-- Rename column for clarity
ALTER TABLE nba_player_stats
RENAME COLUMN player_additional TO player_id;

-- Create a mapping table for player_name → player_id
CREATE TABLE player_id_map (
    player_name TEXT PRIMARY KEY,
    player_id TEXT
);

INSERT INTO player_id_map (player_name, player_id) VALUES
('jokic', 'jokicni01'),
('gilgeous', 'gilgesh01'),
('antetokounmpo', 'antetgi01'),
('wembanyama', 'wembavi01'),
('doncic', 'doncilu01'),
('davis', 'davisan02'),
('james', 'jamesle01'),
('cunningham', 'cunnica01'),
('tatum', 'tatumja01'),
('haliburton', 'halibty01'),
('towns', 'townska01'),
('sabonis', 'sabondo01'),
('maxey', 'maxeyty01'),
('young', 'youngtr01'),
('durant', 'duranke01'),
('harden', 'hardeja01'),
('lillard', 'lillada01'),
('williamson', 'willizi01'),
('edwards', 'edwaran01'),
('johnson', 'johnsja05');

ALTER TABLE nba_player_game_logs ADD COLUMN player_id TEXT;

UPDATE nba_player_game_logs
SET player_id = (
    SELECT player_id
    FROM player_id_map
    WHERE player_id_map.player_name = nba_player_game_logs.player_name
);


-- Use cleaned table for final lookup
SELECT player, player_id, fantasy_score
FROM nba_player_stats_clean
ORDER BY fantasy_score DESC
LIMIT 20;

SELECT * FROM nba_player_stats ORDER BY fantasy_score DESC LIMIT 10;
