from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pendulum

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id='load_gcs_to_bigquery_parallel_files',
    default_args=default_args,
    description='Load multiple files from GCS into BigQuery in parallel',
    schedule=None,
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
) as dag:

    # List all files in the target directory using GCSHook
    gcs_hook = GCSHook(gcp_conn_id='google_new')
    all_files = gcs_hook.list(bucket_name='migration-bucket2', prefix='your-target-directory/')

    # Dynamically create a task for each file
    for file_name in all_files:
        GCSToBigQueryOperator(
            task_id=f'load_file_{file_name.replace("/", "_")}',  # Create a unique task ID
            bucket='migration-bucket2',
            source_objects=[file_name],  # Load a single file
            destination_project_dataset_table='migration-444912.migration_dataset.users',
            source_format='CSV',
            skip_leading_rows=0,
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
            field_delimiter=',',
            allow_quoted_newlines=True,
            autodetect=False,
            gcp_conn_id='google_new',
            schema_fields=[
                {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                {'name': 'last_login', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            ],
        )
