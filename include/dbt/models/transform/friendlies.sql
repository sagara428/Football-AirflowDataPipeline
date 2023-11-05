-- friendlies.sql

-- Create the friendlies table
WITH friendlies AS (
    SELECT
        DISTINCT date,
        home_team,
        away_team,
        home_score,
        away_score,
        tournament
    FROM {{ source('international_football', 'raw_results') }}
    WHERE tournament LIKE '%Friendly'
)
SELECT
    DISTINCT f.date,
    f.home_team,
    f.away_team,
    f.tournament,
    CASE WHEN home_score > away_score THEN home_team
         WHEN home_score < away_score THEN away_team
         ELSE 'Draw'
    END AS result,
    t1.team_id AS home_team_id,
    t2.team_id AS away_team_id,
    dt.tournament_id AS tournament_id
FROM friendlies AS f
JOIN {{ ref('team_stats') }} AS t1 ON f.home_team = t1.team_name
JOIN {{ ref('team_stats') }} AS t2 ON f.away_team = t2.team_name
JOIN {{ ref('tournament_eras') }} AS dt ON f.tournament = dt.tournament
