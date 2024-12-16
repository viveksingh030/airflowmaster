from airflow.datasets import Dataset
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

my_file=Dataset('/tmp/my_file.txt')

with DAG('consumer',
         schedule=[my_file],
         start_date=datetime(2024,12,16),
         catchup=False):
    
    @task(inlets=[my_file])
    def read_dataset():
        with open(my_file.uri,'r') as f:
            print(f.read())
            f.close
    read_dataset()
    