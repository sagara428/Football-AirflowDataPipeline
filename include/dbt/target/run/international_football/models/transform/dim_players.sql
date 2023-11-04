
  
    

    create or replace table `airflow-data-pipeline-404012`.`international_football`.`dim_players`
    
    

    OPTIONS()
    as (
      -- dim_players.sql

-- Create a dimension table for players
WITH players AS (
    SELECT
        ROW_NUMBER() OVER () AS player_id,
        scorer AS player_name
    FROM `airflow-data-pipeline-404012`.`international_football`.`raw_goalscorers`
)
SELECT * FROM players
    );
  