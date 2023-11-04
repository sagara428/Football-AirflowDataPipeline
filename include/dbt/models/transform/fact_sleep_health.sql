-- fact_sleep_health.sql

-- fact_sleep_health.sql

-- dbt model for fact_sleep_health
{{ config(
  materialized='table',
  unique_key='fact_sleep_health_id'
) }}

WITH raw_sleep_health AS (
  SELECT * FROM {{ source('sleep_health', 'raw_sleep_health') }}
)

SELECT
  id AS fact_sleep_health_id,
  rsh.Person_ID,
  dbc.dim_bmi_category_id AS dim_bmi_category_id,
  dsd.dim_sleep_disorder_id AS dim_sleep_disorder_id,
  rsh.Sleep_Duration,
  rsh.Quality_of_Sleep,
  rsh.Physical_Activity_Level,
  rsh.Stress_Level,
  rsh.Blood_Pressure,
  rsh.Heart_Rate,
  rsh.Daily_Steps
FROM raw_sleep_health rsh
INNER JOIN {{ ref('dim_bmi_category') }} dbc ON rsh.BMI_Category = dbc.BMI_Category
INNER JOIN {{ ref('dim_sleep_disorder') }} dsd ON rsh.Sleep_Disorder = dsd.Sleep_Disorder
