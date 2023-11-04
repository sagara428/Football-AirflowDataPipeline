
  
    

    create or replace table `airflow-data-pipeline-404012`.`international_football`.`fact_friendlies`
    
    

    OPTIONS()
    as (
      -- Create the fact_friendlies table with references to dim_teams and dim_tournaments

WITH friendlies AS (
    SELECT
        DISTINCT date,
        home_team,
        away_team,
        home_score,
        away_score,
        tournament
    FROM `airflow-data-pipeline-404012`.`international_football`.`raw_results`
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
JOIN `airflow-data-pipeline-404012`.`international_football`.`dim_teams` AS t1 ON f.home_team = t1.team_name
JOIN `airflow-data-pipeline-404012`.`international_football`.`dim_teams` AS t2 ON f.away_team = t2.team_name
JOIN `airflow-data-pipeline-404012`.`international_football`.`dim_tournaments` AS dt ON f.tournament = dt.tournament
    );
  