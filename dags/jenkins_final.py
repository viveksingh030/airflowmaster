from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import base64
import time
import logging

# Helper functions
def get_jenkins_connection(jenkins_conn_id):
    """Retrieve Jenkins connection details from Airflow connections."""
    connection = BaseHook.get_connection(jenkins_conn_id)
    host = connection.host.rstrip('/')
    port = connection.port or 8080
    return connection

def trigger_jenkins_job(job_name, jenkins_conn_id='jenkins_http', **kwargs):
    """Trigger a Jenkins job and return the build number."""
    conn = get_jenkins_connection(jenkins_conn_id)
    url = f"{conn['host']}:{conn['port']}/job/{job_name}/build?delay=0sec"
    
    response = requests.post(url, headers=conn["auth_header"])
    if response.status_code == 201:
        logging.info(f"Triggered Jenkins job: {job_name}")
    else:
        raise Exception(f"Failed to trigger Jenkins job {job_name}: {response.text}")

    # Fetch the build number
    queue_url = f"{conn['host']}:{conn['port']}/job/{job_name}/lastBuild/api/json"
    response = requests.get(queue_url, headers=conn["auth_header"])
    if response.status_code == 200:
        build_number = response.json().get("number")
        if build_number:
            logging.info(f"Build number for job '{job_name}': {build_number}")
            return build_number
        else:
            raise Exception("Build number not found.")
    else:
        raise Exception(f"Failed to fetch build number: {response.text}")

def monitor_jenkins_job(job_name, build_number, jenkins_conn_id='jenkins_http', max_retries=20, wait_time=30, **kwargs):
    """Monitor a Jenkins job until it completes or fails."""
    conn = get_jenkins_connection(jenkins_conn_id)
    job_url = f"{conn['host']}:{conn['port']}/job/{job_name}/{build_number}/api/json"

    for attempt in range(max_retries):
        response = requests.get(job_url, headers=conn["auth_header"])
        if response.status_code == 200:
            data = response.json()
            result = data.get("result")
            if result == "SUCCESS":
                logging.info(f"Jenkins job '{job_name}' completed successfully!")
                return
            elif result == "FAILURE":
                raise Exception(f"Jenkins job '{job_name}' failed!")
            else:
                logging.info(f"Jenkins job '{job_name}' is running. Attempt {attempt + 1} of {max_retries}.")
        else:
            logging.warning(f"Failed to fetch job status (Attempt {attempt + 1}/{max_retries}): {response.text}")
        
        time.sleep(wait_time)

    raise TimeoutError(f"Jenkins job '{job_name}' did not complete after {max_retries} retries.")

def print_hello():
    logging.info("Hello! Jenkins job completed successfully!")

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 19),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'ml_endpoint_deployment_pipeline_final',
    default_args=default_args,
    description='Deploy and destroy ML endpoint via Jenkins using Airflow',
    schedule_interval=None,
    catchup=False,
    tags=['jenkins', 'ml', 'deployment']
) as dag:

    deploy_endpoint = PythonOperator(
        task_id='deploy_endpoint',
        python_callable=trigger_jenkins_job,
        op_kwargs={'job_name': 'envPipeline'},
    )

    monitor_deploy = PythonOperator(
        task_id='monitor_deploy',
        python_callable=monitor_jenkins_job,
        op_kwargs={
            'job_name': 'envPipeline',
            'build_number': "{{ ti.xcom_pull(task_ids='deploy_endpoint') }}"
        },
    )

    print_hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    # Set task dependencies
    deploy_endpoint >> monitor_deploy >> print_hello_task
