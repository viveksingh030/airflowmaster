from airflow.models.baseoperator import chain
from airflow.decorators import dag,task
from datetime import datetime

@dag(dag_id="chain_demo",start_date=datetime(2025,1,22),catchup=False,schedule_interval="@daily")
def chain_demo():
    @task
    def print_hello():
        print("Hello")
    @task
    def print_world():
        print("World")
    @task
    def print_welcome():
        print("Welcome")
    @task
    def print_namaste():
        print("Namastey")
    chain(print_hello(),print_world(),print_welcome(),print_namaste())
chain_demo()