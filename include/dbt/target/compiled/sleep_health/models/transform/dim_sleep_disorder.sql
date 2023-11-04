-- dim_sleep_disorder.sql

-- dbt model for dim_sleep_disorder


WITH raw_sleep_health AS (
  SELECT * FROM `airflow-data-pipeline-404012`.`sleep_health`.`raw_sleep_health`
)

SELECT
  id AS dim_sleep_disorder_id,
  Sleep_Disorder
FROM raw_sleep_health