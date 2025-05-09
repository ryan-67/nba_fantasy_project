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
    player_id TEXT
);

COPY nba_player_stats
FROM 'D:/DATA_ENGINEER/nba_fantasy_project/data/nba_player_stats.csv'
DELIMITER ',' 
CSV HEADER;

-- Clean the table to remove duplicate rows
CREATE TABLE nba_player_stats_clean AS
SELECT rk, player, age, team, pos, g, gs, mp, fg, fga, fg_pct,
       three_p, three_pa, three_pct, two_p, two_pa, two_pct,
       efg_pct, ft, fta, ft_pct, orb, drb, trb, ast, stl,
       blk, tov, pf, pts, awards, player_id
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY player
               ORDER BY CASE WHEN team = '2TM' THEN 0 ELSE 1 END
           ) AS rn
    FROM nba_player_stats
    WHERE player != 'League Average'
) sub
WHERE rn = 1;

-- Add the fantasy_avg column
ALTER TABLE nba_player_stats_clean
ADD COLUMN fantasy_avg REAL;

-- Update the fantasy_avg column with the calculated values
UPDATE nba_player_stats_clean
SET fantasy_avg = 
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


CREATE TABLE adp (
    rank REAL,
    player TEXT,
    team TEXT,
    positions TEXT,
    yahoo REAL,
    espn REAL,
    avg REAL
);


COPY adp
FROM 'D:/DATA_ENGINEER/nba_fantasy_project/data/adp_cleaned.csv'
DELIMITER ','
CSV HEADER
NULL '';

SELECT * FROM adp;

DROP TABLE adp;

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


ALTER TABLE adp ADD COLUMN player_id TEXT;

-- Step 2: Update the player_id column based on the player_name using the player_id_map
UPDATE adp
SET player_id = s.player_id
FROM nba_player_stats s
WHERE adp.player = s.player;

ALTER TABLE nba_player_game_logs ADD COLUMN fantasy_score INTEGER;

UPDATE nba_player_game_logs
SET fantasy_score =
    (field_goals_made * 2) +
    (field_goals_attempted * -1) +
    (free_throws_made * 1) +
    (free_throws_attempted * -1) +
    (three_pointers_made * 1) +
    (rebounds * 1) +
    (assists * 2) +
    (steals * 4) +
    (blocks * 4) +
    (turnovers * -2) +
    (points * 1);

ALTER TABLE nba_player_game_logs
ADD COLUMN full_player_name TEXT;

UPDATE nba_player_game_logs gl
SET full_player_name = ns.player
FROM nba_player_stats ns
WHERE gl.player_id = ns.player_id;



SELECT * FROM nba_player_game_logs;
SELECT * FROM nba_player_stats_clean;
SELECT * FROM adp;