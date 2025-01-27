from airflow.decorators import dag,task
from datetime import datetime
from airflow.operators.python import PythonOperator

@dag(dag_id='config_dag',schedule=None,start_date=datetime(2025,1,21),
     catchup=False)
def config_dag():
    @task
    def print_hello(**context):
        print("hello")
        custom_config = context['dag_run'].conf
        dataset_id = custom_config.get('dataset_id')
        print(f"dataset id {dataset_id}")
    
    print_hello()

config_dag()
