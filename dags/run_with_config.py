from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_task(**context):
    # Access the config parameter
    config = context['dag_run'].conf
    print(f"Received config: {config}")

with DAG(
    'configurable_dag',
    start_date=datetime(2024, 1, 1),
    # Important: Set catchup=False if you don't want historical runs
    catchup=False
) as dag:
    
    task = PythonOperator(
        task_id='configurable_task',
        python_callable=my_task,
        # This makes the task able to receive runtime configuration
        provide_context=True
    )