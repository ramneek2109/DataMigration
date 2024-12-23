from airflow import DAG
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Define the DAG
dag_s3_to_gcs = DAG(
    dag_id="s3_to_gcs",  # Unique DAG ID
    schedule=None,  # Set to None for manual triggering
    start_date=days_ago(1),
    catchup=False,  # Do not run for past dates
)

# Function to list files from S3 for debugging
def list_s3_files():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    files = s3_hook.list_keys(bucket_name='migration35', prefix='')
    print("Files in S3 bucket:", files)
    return files

# List S3 files before transfer
list_files_task = PythonOperator(
    task_id='list_s3_files',
    python_callable=list_s3_files,
    dag=dag_s3_to_gcs,
)

# Task: Transfer all files from S3 to GCS
s3_to_gcs_task = S3ToGCSOperator(
    task_id="s3_to_gcs_transfer",  # Task ID
    bucket="migration35",  # Your S3 bucket name
    prefix="",  # No prefix, transfer all files
    dest_gcs="gs://migration-bucket2/your-target-directory/",  # GCS bucket path
    gcp_conn_id="google_new",  # Your GCS connection ID in Airflow
    aws_conn_id="aws_default",  # Your AWS connection ID in Airflow
    replace=True,  # Allow overwriting files for testing
    gzip=False,  # Disable gzip compression for testing
    dag=dag_s3_to_gcs,  # Attach the task to the DAG
)

# Set task dependencies
list_files_task >> s3_to_gcs_task
