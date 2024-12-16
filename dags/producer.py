from airflow.datasets import Dataset
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

my_file=Dataset('/tmp/my_file.txt')

with DAG('producer',
         schedule='@daily',
         start_date=datetime(2024,12,16),
         catchup=False):
    
    @task
    def update_dataset():
        with open(my_file.uri,'a+') as f:
            f.write('prodcuer updated this file')
        
    update_dataset()