-- dim_teams.sql

-- Create a dimension table for teams
WITH teams AS (
    SELECT
        ROW_NUMBER() OVER () AS team_id,
        home_team AS team_name,
        country AS team_country
    FROM {{ source('international_football', 'raw_results') }}
    UNION
    SELECT
        ROW_NUMBER() OVER () AS team_id,
        away_team AS team_name,
        country AS team_country
    FROM {{ source('international_football', 'raw_results') }}
)
SELECT * FROM teams