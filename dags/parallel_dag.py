from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime

with DAG(dag_id="parallel_dag",
         start_date=datetime(2024,12,14),
         schedule_interval="@daily",
         catchup=False):
    process_a=BashOperator(task_id="process_1",
                       bash_command="sleep 10")

    process_b=BashOperator(task_id="process_2",
                       bash_command="sleep 10")
    load_a=BashOperator(task_id="load_a",
                       bash_command="sleep 10")
    load_b=BashOperator(task_id="load_b",
                       bash_command="sleep 10")
    transform=BashOperator(task_id="transform",
                           queue="high_cpu",
                           bash_command="sleep 30")

    process_a>>load_a
    process_b>>load_b
    [load_a,load_b]>>transform
