from airflow.models.baseoperator import cross_downstream
from airflow.decorators import dag,task
from datetime import datetime

@dag(dag_id="cross_dependency",start_date=datetime(2025,1,22),catchup=False,schedule_interval="@daily")
def cross_dependency():
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
    cross_downstream([print_hello(),print_world()],[print_welcome(),print_namaste()])
cross_dependency()