-- tournament_eras.sql

-- Create the tournament_eras.sql table
WITH tournament_eras AS (
    SELECT
        r.tournament,
        r.home_team AS team_name,
        CASE
            WHEN r.date >= '2000-01-01' THEN 'Modern Era'
            WHEN r.date >= '1990-01-01' THEN 'Late 20th Century'
            WHEN r.date >= '1980-01-01' THEN '1980s'
            ELSE 'Earlier'
        END AS era
    FROM {{ source('international_football', 'raw_results') }} AS r
)
SELECT
    RANK() OVER (ORDER BY tournament, team_name) AS tournament_id,
    tournament,
    team_name,
    STRING_AGG(DISTINCT era, ', ') AS eras
FROM tournament_eras
GROUP BY tournament, team_name
