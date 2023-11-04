-- dim_tournaments.sql

-- Create a dimension table for tournaments
WITH tournaments AS (
    SELECT
        ROW_NUMBER() OVER () AS tournament_id,
        tournament AS tournament_name
    FROM {{ source('international_football', 'raw_results') }}
)
SELECT * FROM tournaments