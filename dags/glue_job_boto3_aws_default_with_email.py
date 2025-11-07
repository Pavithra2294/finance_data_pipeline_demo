"""
Airflow DAG: Run an AWS Glue job via boto3 using the Airflow 'aws_default' connection.
Waits for job completion, and sends email notifications on success or failure.
"""

from datetime import datetime, timedelta
import boto3
import time
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException


# ========= Configuration ==========
GLUE_JOB_NAME = "Finance_data.py"      # <-- change this
AWS_REGION = "eu-north-1" 
EMAIL_TO = "pavithraganesanuk@gmail.com"           # <-- change this

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}


# ========= Python callable ==========
def run_glue_job(**context):
    """
    Starts an AWS Glue job using credentials from the Airflow aws_default connection,
    waits for it to complete, and raises AirflowException if it fails.
    """
    # Get AWS credentials from Airflow connection
    aws_conn = BaseHook.get_connection("aws_default")

    session = boto3.Session(
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name=AWS_REGION,
    )
    glue_client = session.client("glue")

    print(f"Starting AWS Glue job: {GLUE_JOB_NAME}")
    response = glue_client.start_job_run(JobName=GLUE_JOB_NAME)
    job_run_id = response["JobRunId"]
    print(f"Glue job started with run ID: {job_run_id}")

    # Poll for job completion
    while True:
        job_status = glue_client.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)
        state = job_status["JobRun"]["JobRunState"]
        print(f"Current job state: {state}")

        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            break
        time.sleep(30)

    if state != "SUCCEEDED":
        raise AirflowException(f"Glue job failed with state: {state}")

    print(f"Glue job completed successfully: {job_run_id}")
    context["ti"].xcom_push(key="job_run_id", value=job_run_id)
    return job_run_id


# ========= DAG definition ==========
with DAG(
    dag_id="glue_job_boto3_aws_default_with_email",
    description="Start an AWS Glue job using boto3 + aws_default connection and send email notifications",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=None,   # Run manually or via trigger
    catchup=False,
    tags=["aws", "glue", "boto3"],
) as dag:

    start_glue_job = PythonOperator(
        task_id="start_glue_job",
        python_callable=run_glue_job,
        provide_context=True,
    )

    success_email = EmailOperator(
        task_id="send_success_email",
        to=EMAIL_TO,
        subject="AWS Glue Job Succeeded: {{ params.job_name }}",
        html_content=(
            "<h3>Glue Job Succeeded 🎉</h3>"
            "<b>Job Name:</b> {{ params.job_name }}<br>"
            "<b>Run ID:</b> {{ ti.xcom_pull(task_ids='start_glue_job', key='job_run_id') }}<br>"
            "<b>DAG:</b> {{ dag.dag_id }}<br>"
            "<b>Execution Date:</b> {{ ds }}<br>"
        ),
        params={"job_name": GLUE_JOB_NAME},
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    failure_email = EmailOperator(
        task_id="send_failure_email",
        to=EMAIL_TO,
        subject="AWS Glue Job FAILED: {{ params.job_name }}",
        html_content=(
            "<h3>Glue Job Failed ❌</h3>"
            "<b>Job Name:</b> {{ params.job_name }}<br>"
            "<b>DAG:</b> {{ dag.dag_id }}<br>"
            "<b>Execution Date:</b> {{ ds }}<br>"
            "Check AWS Glue logs in the AWS Console for more details."
        ),
        params={"job_name": GLUE_JOB_NAME},
        trigger_rule=TriggerRule.ONE_FAILED,  # runs only if upstream task fails
    )

    start_glue_job >> [success_email, failure_email]
