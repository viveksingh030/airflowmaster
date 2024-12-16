from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


def transform_task():
    with TaskGroup("transform", tooltip="Transform task") as group:
        download_a = BashOperator(
            task_id='transform_a',
            bash_command='sleep 10'
        )
        download_b = BashOperator(
            task_id='transform_b',
            bash_command='sleep 10'
        )
        download_c = BashOperator(
            task_id='transform_c',
            bash_command='sleep 10'
        )
        return group
