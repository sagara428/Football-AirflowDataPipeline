from airflow.decorators import dag, task
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
    tags = ['sleep_health'],

)

def sleep_health():
    
    csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'csv_to_gcs',
        src = '/usr/local/airflow/include/dataset/sleep_health.csv',
        dst = 'raw/sleep_health.csv',
        bucket = 'sleep_health_lifestyle',
        gcp_conn_id = 'gcp',
        mime_type = 'text/csv',
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id = 'create_dataset',
        dataset_id = 'sleep_health',
        gcp_conn_id = 'gcp',
    )

    gcs_to_raw = aql.load_file(
        task_id = 'gcs_to_raw',
        input_file = File(
            'gs://sleep_health_lifestyle/raw/sleep_health.csv',
            conn_id = 'gcp',
            filetype = FileType.CSV,
        ),
        output_table = Table(
            name = 'raw_sleep_health',
            conn_id = 'gcp',
            metadata = Metadata(schema = 'sleep_health')
        ),
        use_native_support = True,
    )

    @task.external_python(python = '/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name = 'check_load', checks_subpath = 'sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
    check_load()
sleep_health()