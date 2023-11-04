-- dim_sleep_disorder.sql

-- dbt model for dim_sleep_disorder
{{ config(
  materialized='table',
  unique_key='dim_sleep_disorder_id'
) }}

WITH raw_sleep_health AS (
  SELECT * FROM {{ source('sleep_health', 'raw_sleep_health') }}
)

SELECT
  id AS dim_sleep_disorder_id,
  Sleep_Disorder
FROM raw_sleep_health

