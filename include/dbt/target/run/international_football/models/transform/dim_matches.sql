
  
    

    create or replace table `airflow-data-pipeline-404012`.`international_football`.`dim_matches`
    
    

    OPTIONS()
    as (
      -- dim_matches.sql

-- Create a dimension table for matches
WITH matches AS (
    SELECT
        ROW_NUMBER() OVER () AS match_id,
        date AS match_date,
        home_team AS home_team_name,
        away_team AS away_team_name,
        tournament AS tournament_name,
        city AS match_city,
        country AS match_country,
        neutral AS match_neutral
    FROM `airflow-data-pipeline-404012`.`international_football`.`raw_results`
)
SELECT * FROM matches
    );
  