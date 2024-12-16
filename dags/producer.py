from airflow.datasets import Dataset
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

my_file=Dataset('/tmp/my_file.txt')

with DAG('producer',
         schedule='@daily',
         start_date=datetime(2024,12,16),
         catchup=False):
    
    @task(outlets=[my_file])
    def update_dataset():
        with open(my_file.uri,'w+') as f:
            f.write('prodcuer updated this file')
            f.close
        
    update_dataset()