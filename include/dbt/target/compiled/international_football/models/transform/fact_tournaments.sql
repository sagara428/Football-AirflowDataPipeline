-- Create the fact_tournaments table with references to dim_teams

WITH tournament_results AS (
    SELECT
        date,
        home_team,
        away_team,
        CASE WHEN home_score > away_score THEN home_team
             WHEN home_score < away_score THEN away_team
             ELSE 'Draw'
        END AS result
    FROM `airflow-data-pipeline-404012`.`international_football`.`raw_results`
)
SELECT
    t.date,
    t.home_team,
    t.away_team,
    t.result,
    CASE WHEN t.result = t.home_team THEN 1 ELSE 0 END AS home_team_win,
    t1.team_id AS home_team_id,
    t2.team_id AS away_team_id
FROM tournament_results AS t
JOIN `airflow-data-pipeline-404012`.`international_football`.`dim_teams` AS t1 ON t.home_team = t1.team_name
JOIN `airflow-data-pipeline-404012`.`international_football`.`dim_teams` AS t2 ON t.away_team = t2.team_name