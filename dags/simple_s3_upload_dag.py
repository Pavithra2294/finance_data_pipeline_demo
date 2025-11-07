from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os

def upload_to_s3(**kwargs):
    local_file_path = "/opt/airflow/data/sample_financial_data_100_extended.csv"
    bucket_name = "finance-datapav"
    s3_key = "extract-raw/sample_financial_data_100_extended.csv"

    hook = S3Hook(aws_conn_id="aws_default")
    hook.load_file(
        filename=local_file_path,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )

with DAG(
    dag_id="simple_s3_upload_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "s3"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_file_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
    )
