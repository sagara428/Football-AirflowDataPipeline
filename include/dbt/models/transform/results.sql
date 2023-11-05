-- results.sql

-- Create results table
WITH results AS (
    SELECT
        date,
        home_team,
        away_team,
        home_score,
        away_score
    FROM {{ source('international_football', 'raw_results') }}
)
SELECT
    date,
    home_team,
    away_team,
    home_score,
    away_score,
    CASE WHEN home_score > away_score THEN home_team
         WHEN home_score < away_score THEN away_team
         ELSE 'Draw'
    END AS result,
    t1.team_id AS home_team_id,
    t2.team_id AS away_team_id
FROM results
JOIN {{ ref('team_stats') }} AS t1 ON results.home_team = t1.team_name
JOIN {{ ref('team_stats') }} AS t2 ON results.away_team = t2.team_name
