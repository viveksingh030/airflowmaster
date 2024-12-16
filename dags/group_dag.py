from airflow import DAG
from airflow.operators.bash import BashOperator
from groups.transform_task import transform_task
from groups.downloads_task import downloads_task

from datetime import datetime

with DAG('group_dag', start_date=datetime(2024, 12, 14),
         schedule_interval='@daily', catchup=False) as dag:
    downloads = downloads_task()
    transform = transform_task()

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    downloads >> check_files >> transform
