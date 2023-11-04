-- dim_person.sql

-- dbt model for dim_person
{{ config(
  materialized='table',
  unique_key='Person_ID'
) }}

WITH raw_sleep_health AS (
  SELECT * FROM {{ source('sleep_health', 'raw_sleep_health') }}
)

SELECT
  Person_ID,
  Gender,
  Age,
  Occupation
FROM raw_sleep_health
