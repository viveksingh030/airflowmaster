from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'docker_test',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

docker_task = DockerOperator(
    task_id='docker_command',
    image='hello-world',
    container_name='test_container',
    api_version='auto',
    auto_remove=True,
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    dag=dag
)