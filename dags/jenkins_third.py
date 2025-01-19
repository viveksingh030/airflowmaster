from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import base64
import time

def print_hello():
    print("Hello! Jenkins job completed successfully!")

def trigger_jenkins_job(job_name, **kwargs):
    """Trigger the Jenkins job via API and pass build number to next task."""
    jenkins_conn_id = 'jenkins_http'
    connection = BaseHook.get_connection(jenkins_conn_id)
    
    # Extract the host and credentials
    host = connection.host  # This should include the Jenkins base URL
    username = connection.login
    password = connection.password
    
    # Encode the username and password for Basic Auth
    auth_string = f'{username}:{password}'
    encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')
    
    # Replace this with your actual crumb if CSRF protection is enabled in Jenkins
    crumb = "ebdfc4a817b2f21a38b229550722f4f2baed6b528d8bba887073fe202eaa242e"
    
    headers = {
        "Authorization": f"Basic {encoded_auth}",
        "Jenkins-Crumb": crumb
    }
    
    # Construct the URL dynamically
    host = connection.host.rstrip('/')
    port = connection.port or 8080  # Default to 8080 if port is not specified
    url = f"{host}:{port}/job/{job_name}/build?delay=0sec"
    response = requests.post(url, headers=headers)
    
    if response.status_code == 201:
        print(f"Jenkins job '{job_name}' triggered successfully!")
    else:
        raise Exception(f"Failed to trigger Jenkins job '{job_name}': {response.text}")
    
    # Get the build number
    queue_url = f"{host}:{port}/job/{job_name}/lastBuild/api/json"
    response = requests.get(queue_url, headers=headers)
    if response.status_code == 200:
        build_number = response.json().get('number')
        if build_number:
            print(f"Build number for '{job_name}': {build_number}")
            return build_number
        else:
            raise Exception("Build number not found.")
    else:
        raise Exception(f"Failed to fetch build number: {response.text}")


def monitor_jenkins_job(job_name, build_number, **kwargs):
    """Monitor the status of the Jenkins job."""
    jenkins_conn_id = 'jenkins_http'
    connection = BaseHook.get_connection(jenkins_conn_id)
    host = connection.host.rstrip('/')
    port = connection.port or 8080  # Default to 8080 if port is not specified
    username = connection.login
    password = connection.password
    auth_string = f'{username}:{password}'
    encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')
    
    headers = {
        "Authorization": f"Basic {encoded_auth}"
    }
    
    job_url = f"{host}:{port}/job/{job_name}/{build_number}/api/json"
    max_retries = 3
    wait_time = 300  # Seconds between checks
    
    for _ in range(max_retries):
        response = requests.get(job_url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data.get('result') == 'SUCCESS':
                print(f"Jenkins job '{job_name}' completed successfully!")
                return
            elif data.get('result') == 'FAILURE':
                raise Exception(f"Jenkins job '{job_name}' failed!")
            else:
                print(f"Jenkins job '{job_name}' is still running. Retrying...")
        else:
            print(f"Failed to fetch job status: {response.text}")
        
        time.sleep(wait_time)
    
    raise TimeoutError(f"Jenkins job '{job_name}' did not complete within the maximum retries.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ml_endpoint_deployment_pipeline',
    default_args=default_args,
    description='Deploy and destroy ML endpoint via Jenkins using Airflow',
    schedule=None,
    catchup=False
)

deploy_endpoint = PythonOperator(
    task_id='deploy_endpoint',
    python_callable=trigger_jenkins_job,
    op_kwargs={'job_name': 'envPipeline'},  # Replace with your deploy job name
    dag=dag
)

monitor_deploy = PythonOperator(
    task_id='monitor_deploy',
    python_callable=monitor_jenkins_job,
    op_kwargs={
        'job_name': 'envPipeline',
        'build_number': "{{ ti.xcom_pull(task_ids='deploy_endpoint') }}"
    },
    dag=dag
)

# Print hello after Jenkins job completes
print_hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag
)

# Set task dependencies
deploy_endpoint >> monitor_deploy>>print_hello_task
