-- dim_bmi_category.sql

-- dbt model for dim_bmi_category
{{ config(
  materialized='table',
  unique_key='dim_bmi_category_id'
) }}

WITH raw_sleep_health AS (
  SELECT * FROM {{ source('sleep_health', 'raw_sleep_health') }}
)

-- Assuming you have an auto-incrementing primary key column, e.g., 'id'
SELECT
  id AS dim_bmi_category_id,
  BMI_Category
FROM raw_sleep_health
