
  
    

    create or replace table `airflow-data-pipeline-404012`.`international_football`.`dim_teams`
    
    

    OPTIONS()
    as (
      -- Create the dim_teams table

WITH team_stats AS (
    SELECT
        home_team AS team_name,
        SUM(CASE WHEN home_score > away_score THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN home_score < away_score THEN 1 ELSE 0 END) AS losses,
        SUM(home_score) AS goals_scored,
        SUM(away_score) AS goals_conceded
    FROM `airflow-data-pipeline-404012`.`international_football`.`raw_results`
    GROUP BY home_team
    UNION ALL
    SELECT
        away_team AS team_name,
        SUM(CASE WHEN away_score > home_score THEN 1 ELSE 0 END) AS wins,
        SUM(CASE WHEN away_score < home_score THEN 1 ELSE 0 END) AS losses,
        SUM(away_score) AS goals_scored,
        SUM(home_score) AS goals_conceded
    FROM `airflow-data-pipeline-404012`.`international_football`.`raw_results`
    GROUP BY away_team
    UNION ALL
    SELECT
        home_team AS team_name,
        0 AS wins,
        0 AS losses,
        0 AS goals_scored,
        0 AS goals_conceded
    FROM `airflow-data-pipeline-404012`.`international_football`.`raw_shootouts`
    UNION ALL
    SELECT
        away_team AS team_name,
        0 AS wins,
        0 AS losses,
        0 AS goals_scored,
        0 AS goals_conceded
    FROM `airflow-data-pipeline-404012`.`international_football`.`raw_shootouts`
)
SELECT
    RANK() OVER (ORDER BY team_name) AS team_id,
    team_name,
    SUM(wins) AS total_wins,
    SUM(losses) AS total_losses,
    SUM(goals_scored) AS total_goals_scored,
    SUM(goals_conceded) AS total_goals_conceded
FROM team_stats
GROUP BY team_name
    );
  