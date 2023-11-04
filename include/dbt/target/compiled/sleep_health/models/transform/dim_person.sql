-- dim_person.sql

-- dbt model for dim_person


WITH raw_sleep_health AS (
  SELECT * FROM `airflow-data-pipeline-404012`.`sleep_health`.`raw_sleep_health`
)

SELECT
  id AS dim_person_id,
  Person_ID,
  Gender,
  Age,
  Occupation
FROM raw_sleep_health