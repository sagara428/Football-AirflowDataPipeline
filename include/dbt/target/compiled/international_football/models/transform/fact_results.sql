-- Create the fact_results table with references to dim_teams

WITH results AS (
    SELECT
        date,
        home_team,
        away_team,
        home_score,
        away_score
    FROM `airflow-data-pipeline-404012`.`international_football`.`raw_results`
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
JOIN `airflow-data-pipeline-404012`.`international_football`.`dim_teams` AS t1 ON results.home_team = t1.team_name
JOIN `airflow-data-pipeline-404012`.`international_football`.`dim_teams` AS t2 ON results.away_team = t2.team_name