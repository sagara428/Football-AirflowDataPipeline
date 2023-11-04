
  
    

    create or replace table `airflow-data-pipeline-404012`.`international_football`.`dim_team_stats`
    
    

    OPTIONS()
    as (
      -- dim_team_stats.sql

-- Create dim_teams table with unique team names and team statistics
WITH team_stats AS (
    SELECT
        team AS team_name,
        SUM(CASE WHEN home_team = team THEN 1 ELSE 0 END) AS home_wins,
        SUM(CASE WHEN away_team = team THEN 1 ELSE 0 END) AS away_wins,
        SUM(CASE WHEN home_team = team THEN away_score ELSE home_score END) AS goals_scored,
        SUM(CASE WHEN away_team = team THEN away_score ELSE home_score END) AS goals_conceded
    FROM
        `airflow-data-pipeline-404012`.`international_football`.`raw_results`
    GROUP BY
        team_name
)

SELECT
    team_name,
    home_wins,
    away_wins,
    goals_scored,
    goals_conceded
FROM
    team_stats
    );
  