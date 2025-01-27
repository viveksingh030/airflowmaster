from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.jenkins.operators.jenkins_job_trigger import JenkinsJobTriggerOperator
from airflow.utils.trigger_rule import TriggerRule
import logging

# Jenkins connection ID in Airflow
JENKINS_CONNECTION_ID = 'jenkins_http'


def print_hello():
    """Sample function to demonstrate logging."""
    logging.info("Hello Jenkins job completed successfully!")


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 20),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
        'synthetic_id_endpoint_deploy_destroy',
        default_args=default_args,
        description='Deploy and destroy ML endpoint via Jenkins using Airflow',
        schedule_interval=None,
        catchup=False,
        tags=["qec", "team:qec"]
) as dag:
    # Start task
    start = BashOperator(
        task_id="start",
        bash_command='sleep 2'
    )

    # Trigger Jenkins job
    deploy_face_detection_endpoint = JenkinsJobTriggerOperator(
        task_id="deploy_face_detection_live",
        job_name="choice-pipeline",
        jenkins_connection_id=JENKINS_CONNECTION_ID,  # Use Jenkins connection ID
        parameters={
            "delay": "0sec",
            "deployedEnv": 'PROD',
            "team": "KYX",
        }
    )

    # End task
    print_end_task = PythonOperator(
        task_id='end_task',
        python_callable=print_hello,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    # Task dependencies
    start >> deploy_face_detection_endpoint >> print_end_task
