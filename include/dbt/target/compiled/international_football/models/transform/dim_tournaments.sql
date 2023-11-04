-- Create the dim_tournaments table with references to dim_teams

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
    FROM `airflow-data-pipeline-404012`.`international_football`.`raw_results` AS r
)
SELECT
    RANK() OVER (ORDER BY tournament) AS tournament_id,
    tournament,
    team_name,
    STRING_AGG(DISTINCT era, ', ') AS eras
FROM tournament_eras
GROUP BY tournament, team_name