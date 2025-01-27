from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.jenkins.operators.jenkins_job_trigger import JenkinsJobTriggerOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook
import logging

JENKINS_CONNECTION_ID = 'jenkins_http'

def get_jenkins_connection():
    """Retrieve Jenkins connection details from Airflow connections."""
    connection = BaseHook.get_connection(JENKINS_CONNECTION_ID)
    host = connection.host.rstrip('/')
    port = connection.port or 8080
    return connection

def print_hello():
    logging.info("Hello Jenkins job completed successfully!")


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
    start = BashOperator(task_id="start",
                         bash_command='sleep 10')
    connection = get_jenkins_connection()

    deploy_face_detection_endpoint = JenkinsJobTriggerOperator(
        task_id="deploy_face_detection_live",
        job_name="choice-pipeline",
        jenkins_conn_id=connection.conn_id,  # Required when not using Airflow Connections
        jenkins_url=connection.host,
        username= connection.login,
        password=connection.password,
        parameters={
            "delay": "0sec",
            "deployedEnv": 'PROD',
            "team": "KYX",
        },
        jenkins_connection_id=JENKINS_CONNECTION_ID,
    )

    print_end_task = PythonOperator(
        task_id='end_task',
        python_callable=print_hello(),
        trigger_rule=TriggerRule.NONE_FAILED
    )
    start>>deploy_face_detection_endpoint>>print_end_task