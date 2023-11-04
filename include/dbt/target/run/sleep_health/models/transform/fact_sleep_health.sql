
  
    

    create or replace table `airflow-data-pipeline-404012`.`sleep_health`.`fact_sleep_health`
    
    

    OPTIONS()
    as (
      -- fact_sleep_health.sql

-- fact_sleep_health.sql

-- dbt model for fact_sleep_health


WITH raw_sleep_health AS (
  SELECT * FROM `airflow-data-pipeline-404012`.`sleep_health`.`raw_sleep_health`
)

SELECT
  md5(CONCAT(CAST(rsh.Person_ID AS STRING), rsh.Sleep_Disorder)) AS fact_sleep_health_id,
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
INNER JOIN `airflow-data-pipeline-404012`.`sleep_health`.`dim_bmi_category` dbc ON rsh.BMI_Category = dbc.BMI_Category
INNER JOIN `airflow-data-pipeline-404012`.`sleep_health`.`dim_sleep_disorder` dsd ON rsh.Sleep_Disorder = dsd.Sleep_Disorder
    );
  