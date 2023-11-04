
  
    

    create or replace table `airflow-data-pipeline-404012`.`sleep_health`.`dim_bmi_category`
    
    

    OPTIONS()
    as (
      -- dim_bmi_category.sql

-- dbt model for dim_bmi_category


WITH raw_sleep_health AS (
  SELECT * FROM `airflow-data-pipeline-404012`.`sleep_health`.`raw_sleep_health`
)

-- Assuming you have an auto-incrementing primary key column, e.g., 'id'
SELECT
  id AS dim_bmi_category_id,
  BMI_Category
FROM raw_sleep_health;
    );
  