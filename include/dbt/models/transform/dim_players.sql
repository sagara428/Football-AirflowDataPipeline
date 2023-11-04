-- dim_players.sql

-- Create a dimension table for players
WITH players AS (
    SELECT
        ROW_NUMBER() OVER () AS player_id,
        scorer AS player_name
    FROM {{ source('international_football', 'raw_goalscorers') }}
)
SELECT * FROM players