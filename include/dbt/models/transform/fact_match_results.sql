-- fact_match_results.sql

-- Create a fact table for match results
WITH match_results AS (
    SELECT
        ROW_NUMBER() OVER () AS match_result_id,
        m.match_id,
        m.match_date,
        m.tournament_name,
        m.match_city,
        m.match_country,
        m.match_neutral,
        t1.team_id AS home_team_id,
        t2.team_id AS away_team_id,
        r.home_score,
        r.away_score
    FROM {{ source('international_football', 'raw_results') }} r
    JOIN {{ ref('dim_matches') }} m ON r.date = m.match_date
    JOIN {{ ref('dim_teams') }} t1 ON r.home_team = t1.team_name
    JOIN {{ ref('dim_teams') }} t2 ON r.away_team = t2.team_name
)
SELECT * FROM match_results

