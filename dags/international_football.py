from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models.baseoperator import chain
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

@dag(
    start_date=datetime(2023, 10, 22),
    schedule=None,
    catchup=False,
    tags=['international_football'],
)
def international_football():
    '''
    DAG for international football data pipelines.

    This DAG performs the following tasks:
    1. Moves CSV files from the local filesystem to Google Cloud Storage (GCS).
    2. Creates a BigQuery dataset and loads data from GCS into BigQuery tables.
    3. Runs data quality checks on the raw data.
    4. Runs a transformation using dbt.
    5. Runs data quality checks on the transformed data.

    '''
    # Dataset from local to GCS and BigQuery
    # Task 1: Dataset from local to GCS
    csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='csv_to_gcs',
        src='/usr/local/airflow/include/dataset/*.csv',
        dst='raw/',
        bucket='international_football_results',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    # Task 2: Dataset from GCS to BigQuery
    with TaskGroup(group_id='gcs_to_bigquery') as tg1:
        # Create empty dataset in BigQuery
        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='create_dataset',
            dataset_id='international_football',
            gcp_conn_id='gcp',
        )

        # Load data from GCS into BigQuery tables
        raw_goalscorers = aql.load_file(
            task_id='raw_goalscorers',
            input_file=File(
                'gs://international_football_results/raw/goalscorers.csv',
                conn_id='gcp',
                filetype=FileType.CSV,
            ),
            output_table=Table(
                name='raw_goalscorers',
                conn_id='gcp',
                metadata=Metadata(schema='international_football')
            ),
            use_native_support=True,
        )

        raw_results = aql.load_file(
            task_id='raw_results',
            input_file=File(
                'gs://international_football_results/raw/results.csv',
                conn_id='gcp',
                filetype=FileType.CSV,
            ),
            output_table=Table(
                name='raw_results',
                conn_id='gcp',
                metadata=Metadata(schema='international_football')
            ),
            use_native_support=True,
        )

        raw_shootouts = aql.load_file(
            task_id='raw_shootouts',
            input_file=File(
                'gs://international_football_results/raw/shootouts.csv',
                conn_id='gcp',
                filetype=FileType.CSV,
            ),
            output_table=Table(
                name='raw_shootouts',
                conn_id='gcp',
                metadata=Metadata(schema='international_football')
            ),
            use_native_support=True,
        )

        # Sub tasks dependencies
        create_dataset >> raw_goalscorers >> raw_shootouts >> raw_results

    # End task group definition

    # task 3: Check data quality of raw data in BigQuery
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    
    def check_load(scan_name='check_load', checks_subpath='sources'):
        '''
        Run data quality checks on the loaded data.

        Args:
            scan_name (str): The name of the scan.
            checks_subpath (str): The subpath for checks.

        Returns:
            str: The result of the data quality checks.
        '''
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    task_check_load = check_load()

    # task 4: transform raw data into dim tables, fact tables with dbt
    transform = BashOperator(
        task_id='transform',
        bash_command='''
            source /usr/local/airflow/dbt_venv/bin/activate &&
            cd /usr/local/airflow/include/dbt &&
            dbt deps &&
            dbt run --profiles-dir /usr/local/airflow/include/dbt/
        ''',
    )


    # task 5: data quality check of dim tables and fact tables
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        '''
        Run data quality checks on the transformed data.

        Args:
            scan_name (str): The name of the scan.
            checks_subpath (str): The subpath for checks.

        Returns:
            str: The result of the data quality checks.
        '''
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    task_check_transform = check_transform()

    # Set tasks dependencies
    chain(
        csv_to_gcs, tg1, task_check_load, transform, task_check_transform
    )

international_football()
