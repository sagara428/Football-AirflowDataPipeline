from airflow.decorators import dag, task, task_group
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

@dag(
    start_date = datetime(2023, 10, 22),
    schedule = None,
    catchup = False,
    tags = ['international_football'],

)

def international_football():
    # Dataset from local to GCS and BigQuery 
    csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'csv_to_gcs',
        src = '/usr/local/airflow/include/dataset/*.csv',
        dst = 'raw/',
        bucket = 'international_football_results',
        gcp_conn_id = 'gcp',
        mime_type = 'text/csv',
    )

    @task_group(group_id = 'gcs_to_bigquery')
    def tg1():
        
        create_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id = 'create_dataset',
            dataset_id = 'international_football',
            gcp_conn_id = 'gcp',
        )
        raw_goalscorers = aql.load_file(
            task_id = 'raw_goalscorers',
            input_file = File(
                'gs://international_football_results/raw/goalscorers.csv',
                conn_id = 'gcp',
                filetype = FileType.CSV,
            ),
            output_table = Table(
                name = 'raw_goalscorers',
                conn_id = 'gcp',
                metadata = Metadata(schema = 'international_football')
            ),
            use_native_support = True,
        )
        raw_results = aql.load_file(
            task_id = 'raw_results',
            input_file = File(
                'gs://international_football_results/raw/results.csv',
                conn_id = 'gcp',
                filetype = FileType.CSV,
            ),
            output_table = Table(
                name = 'raw_results',
                conn_id = 'gcp',
                metadata = Metadata(schema = 'international_football')
            ),
            use_native_support = True,
        )

        raw_shootouts = aql.load_file(
            task_id = 'raw_shootouts',
            input_file = File(
                'gs://international_football_results/raw/shootouts.csv',
                conn_id = 'gcp',
                filetype = FileType.CSV,
            ),
            output_table = Table(
                name = 'raw_shootouts',
                conn_id = 'gcp',
                metadata = Metadata(schema = 'international_football')
            ),
            use_native_support = True,
        )

        create_dataset >> raw_goalscorers >> raw_shootouts >> raw_results

    tg1()

    @task.external_python(python = '/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name = 'check_load', checks_subpath = 'sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
    check_load()

international_football()