from datetime import datetime
from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator

def my_evaluation():
    return False

@dag(
    start_date=datetime(2024, 12, 16),
    schedule='@daily',
    catchup=False,
)
def docker_dag():
    
    @task
    def task1():
        print("hello from task 1")

    task2=DockerOperator(task_id='docker_task',image='stock_image_final:v1.0.0',command='echo "Hello In Docker"',
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge")

    task1()>>task2
    
docker_dag()

