from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import logging

# Define the DAG
dag_unload = DAG(
    dag_id="redshift_unload_to_s3",  # Unique DAG ID
    schedule_interval=None,  # Set to None for manual triggering
    start_date=days_ago(1),
    catchup=False,  # Do not run for past dates
)

# Define your UNLOAD query
unload_query = """
    UNLOAD ('SELECT * FROM users')  -- Replace with your actual query
    TO 's3://migration35/'  -- Replace with your S3 bucket and path
    CREDENTIALS 'aws_iam_role={iam_role_arn}'
    DELIMITER ','  -- Specify the delimiter for the data file
    ADDQUOTES  -- Enclose fields in quotes
    ALLOWOVERWRITE;  -- Allow overwriting existing files in S3
"""

# Create the task to execute the UNLOAD query on Redshift
unload_task = PostgresOperator(
    task_id="unload_redshift_to_s3",  # Task ID
    postgres_conn_id="redshift_new",  # Your Redshift connection ID configured in Airflow
    sql=unload_query,  # The SQL query to execute
    autocommit=True,  # Ensure autocommit is enabled for the UNLOAD operation
    dag=dag_unload,  # Attach the task to the DAG
)

# Set task dependencies (optional if you have other tasks in the DAG)
unload_task
